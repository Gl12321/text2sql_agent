from langgraph.graph import StateGraph, END
from typing import TypedDict
from src.core.logger import setup_logger

logger = setup_logger("LangGraph")

class AgentState(TypedDict):
    question: str
    tables: list[str]
    all_columns: list[str]
    db_id: str
    ddls_context: list[str]
    prompt: str
    query: str
    status: str
    error_query: str
    error_from_db: str
    retry_count: int
    result_from_db: str


class SQLAgentGraph:
    def __init__(self, retriever, prompt_manager, llm_wrapper, executor, corrector):
        self.retriever = retriever
        self.prompt_manager = prompt_manager
        self.llm_wrapper = llm_wrapper
        self.executor = executor
        self.corrector = corrector
        self.graph = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(AgentState)

        workflow.add_node("retrieve", self.retriever_node)
        workflow.add_node("prompt_manager", self.prompt_node)
        workflow.add_node("llm", self.llm_node)
        workflow.add_node("correction_loop", self.correction_loop)
        workflow.add_node("executor", self.executor_node)

        workflow.set_entry_point("retrieve")
        workflow.add_edge("retrieve", "prompt_manager")
        workflow.add_edge("prompt_manager", "llm")
        workflow.add_edge("llm", "executor")
        workflow.add_conditional_edges(
            "executor",
            self.self_correction_loop,
            {
                "need_correct": "correction_loop",
                "fatal_error": END,
                "is_ready": END,
            }
        )
        workflow.add_edge("correction_loop", "llm")

        return workflow.compile()

    def self_correction_loop(self, state: AgentState):
        if state["status"] == "success":
            return "is_ready"
        elif state["retry_count"] > 3:
            logger.error("Failed to generate a valid request")
            return "fatal_error"
        else:
            return "need_correct"

    async def retriever_node(self, state: AgentState):
        logger.info(f"start search by question: {state['question']}")

        question = state["question"]

        docs = await self.retriever.ainvoke(question)
        tables = []
        columns = []
        ddls_context = []
        db_id = None

        logger.info(f"retriever found tables: {tables}")

        for doc in docs:
            meta = doc.metadata
            tables.append(meta.get("table_name"))
            columns.extend((meta.get("column_names")).split(","))
            db_id = meta.get("db_id")

            ddls_context.append(doc.page_content)

        return {
            "tables": tables,
            "all_columns": list(set(columns)),
            "db_id": db_id,
            "ddls_context": ddls_context,
        }

    async def prompt_node(self, state: AgentState):
        question = state["question"]
        retrieved_ddls = state["ddls_context"]
        prompt = self.prompt_manager.build_sql_prompt(question, retrieved_ddls)

        logger.info(f"Created  prompt: {prompt}")

        return {"prompt": prompt}

    async def llm_node(self, state: AgentState):
        columns_for_grammar = state["all_columns"]
        tables_name_for_grammar = state["tables"]
        prompt = state["prompt"]

        logger.info("Getting started of llm")

        llm = self.llm_wrapper.get_chain(tables_name_for_grammar, columns_for_grammar)
        query = await llm.ainvoke(prompt)

        logger.info(f"Generated query: {query}")

        return {"query": query}

    async def executor_node(self, state: AgentState):
        query = state["query"]
        result_of_query = await self.executor.execute(query)

        if result_of_query["status"] == "success":
            return {
                "result_from_db": result_of_query["data"],
                "status": "success",
            }
        else:
            logger.error("An invalid request was generated. Start correction")

            return {
                "error_query": result_of_query["query"],
                "error_from_db": result_of_query["error"],
                "retry_count": state["retry_count"] + 1,
                "status": "error"
            }

    async def correction_loop(self, state: AgentState):
        logger.info(f"started correction loop, {state['retry_count']} attempt")

        context = state["ddls_context"]
        error_query = state["error_query"]
        error_from_db = state["error_from_db"]
        corrected_prompt = await self.corrector.correct(context, error_query, error_from_db)

        return {"prompt": corrected_prompt}

