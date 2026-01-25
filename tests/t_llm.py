from src.rag.retriver import TableRetriever
from src.llm.promts import PromptManager
from src.core.logger import setup_logger
from src.rag.embedder import TableEmbedder
from langchain_core.documents import Document
from src.core.config import get_settings
from src.llm.wrapper import LLMWrapper
from src.llm.graph import SQLAgentGraph
from src.agent.executor import SQLExecutor
from src.agent.corrector import SQLCorrector
import asyncio
import chromadb


logger = setup_logger("test")
config = get_settings()
test_db = "college_2"

async def t_graph():
    test_questions_1 = ["Find the names of all students in the Comp. Sci. department",
              "Find the names and salaries of instructors who earn more than 70000",
              "Count the number of courses in each department"]
    test_questions_2 = [
        "Find the titles of courses and the names of departments that offer them.",
        "List the names of students and the names of their advisors.",
        "Find the names of students who took courses in the 'Watson' building."
    ]
    test_questions_3 = [
        "Find the names of all students in the Comp. Sci. department",
        "Find the names of students who took courses in the 'Watson' building."
    ]

    prompt_manager = PromptManager()
    embedder = TableEmbedder()
    client = chromadb.PersistentClient(config.VECTOR_DB_PATH)
    collection = client.get_collection("tables")
    retriever = TableRetriever(collection=collection, embedder=embedder, db_id=test_db, top_k=4)
    executor = SQLExecutor()
    corrector = SQLCorrector()
    wrapper = LLMWrapper()

    agent = SQLAgentGraph(retriever, prompt_manager, wrapper, executor, corrector)

    for i, question in enumerate(test_questions_3):
        logger.info(f"query {i+1}: {question}")

        initial_state = {
            "question": question,
            "db_id": test_db,
            "retry_count": 0
        }

        final_state = await agent.graph.ainvoke(initial_state)
        logger.info(f"Question: {question}")
        logger.info(f"SQL: {final_state.get('query')}")
        logger.info(f"Status: {final_state.get('status')}")
        logger.info(f"Result: {final_state.get('result_from_db')}")

if __name__ == "__main__":
    asyncio.run(t_graph())
    
