from langgraph.graph import StateGraph, END
from langchain_core.documents import Document
from typing import TypedDict
import time

from src.core.logger import setup_logger


logger = setup_logger("LangGraph")

class AgentState(TypedDict):
    question: str
    requested_schemes: list[str]

    tables: list[str]
    raw_documents: list[Document]
    ddls_context: list[str]
    schemas_mapping: dict[str, dict[str, list[str]]]

    prompt: str
    query: str

    status: str
    error_query: str
    error_from_db: str
    retry_count: int

    result_from_db: str


class SQLAgentGraph:
    def __init__(self, retriever, reranker, prompt_manager, llm_wrapper, executor, corrector):
        self.retriever = retriever
        self.reranker = reranker
        self.prompt_manager = prompt_manager
        self.llm_wrapper = llm_wrapper
        self.executor = executor
        self.corrector = corrector
        self.graph = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(AgentState)

        workflow.add_node("retrieve", self.retriever_node)
        workflow.add_node("reranker", self.reranker_node)
        workflow.add_node("prompt_manager", self.prompt_node)
        workflow.add_node("llm", self.llm_node)
        workflow.add_node("correction_loop", self.correction_loop)
        workflow.add_node("executor", self.executor_node)

        workflow.set_entry_point("retrieve")
        workflow.add_edge("retrieve", "reranker")
        workflow.add_edge("reranker", "prompt_manager")
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

        docs = await self.retriever.ainvoke(state["question"])

        tables = []
        for doc in docs:
            table_name = doc.metadata.get("schema_id") + "." + doc.metadata.get("table_name")
            tables.append(table_name)

        return {
            "raw_documents": docs,
        }

    async def reranker_node(self, state: AgentState):
        question = state["question"]
        raw_documents = state["raw_documents"]

        relevant_documents = await self.reranker.rerank(question, raw_documents)
        tables = [doc.metadata.get("table_name") for doc in relevant_documents]

        ddls_context = []
        schemas_mapping = {}

        for doc in relevant_documents:
            meta = doc.metadata
            ddls_context.append(doc.page_content)
            schema_id = meta.get("schema_id")
            table_name = meta.get("table_name")
            cols = [col for col in (meta.get("column_names", "")).split(",")]

            if schema_id not in schemas_mapping:
                schemas_mapping[schema_id] = {}
            schemas_mapping[schema_id][table_name] = cols

        return {
            "tables": tables,
            "ddls_context": ddls_context,
            "schemas_mapping": schemas_mapping,
        }

    async def prompt_node(self, state: AgentState):
        question = state["question"]
        retrieved_ddls = state["ddls_context"]
        prompt = self.prompt_manager.build_sql_prompt(question, retrieved_ddls)
        
        logger.info(f"Prompt length in chars: {len(prompt)}")
        logger.info(f"Created  prompt: {prompt}")

        return {"prompt": prompt}

    async def llm_node(self, state: AgentState):
        schemas_mapping = state["schemas_mapping"]
        prompt = state["prompt"]

        logger.info("Getting started of llm")

        start = time.time()
        llm = self.llm_wrapper.get_chain(schemas_mapping)
        end = time.time()
        query = await llm.ainvoke(prompt)

        logger.info(f"Generated query: {query} \n execution time: {end - start}")

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
            
            raw_error = str(result_of_query["error"])
            clean_error = raw_error.split("[SQL:")[0].strip()

            return {
                "error_query": result_of_query["query"],
                "error_from_db": clean_error,
                "retry_count": state["retry_count"] + 1,
                "status": "error"
            }

    async def correction_loop(self, state: AgentState):
        logger.info(f"started correction loop, {state['retry_count']} attempt")

        ddls_context = state["ddls_context"]
        error_query = state["error_query"]
        error_msg = state["error_from_db"]
        question = state["question"]
        
        logger.info(f" Error from db: {state["error_from_db"]}")
        
        corrected_prompt = self.corrector.build_correction_prompt(question, ddls_context, error_msg)

        return {"prompt": corrected_prompt}

