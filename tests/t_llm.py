import asyncio
import chromadb
from src.rag.retriver import TableRetriever
from src.llm.promts import PromptManager
from src.core.logger import setup_logger
from src.rag.embedder import TableEmbedder
from src.rag.reranker import TableReranker
from src.core.config import get_settings
from src.llm.wrapper import LLMWrapper
from src.agent.graph import SQLAgentGraph
from src.agent.executor import SQLExecutor
from src.agent.corrector import SQLCorrector
from src.database.schema_parser import SchemaParser

logger = setup_logger("test")
config = get_settings()

async def t_graph():
    test_questions_1 = [
        "Find the names of all students in the Comp. Sci. department",
        "Find the names and salaries of instructors who earn more than 70000",
        "Count the number of courses in each department. In the collage_2 schema",
        "Show the names of the stations and the number of available bikes at each of them.",
    ]
    
    parser = SchemaParser()
    all_schemas = await parser.get_all_schemas()
    
    prompt_manager = PromptManager()
    embedder = TableEmbedder()
    client = chromadb.PersistentClient(config.VECTOR_DB_PATH)
    collection = client.get_collection("tables")
    retriever = TableRetriever(collection=collection, embedder=embedder, schemas_id=all_schemas, top_k=10)
    reranker = TableReranker()
    executor = SQLExecutor()
    corrector = SQLCorrector()
    wrapper = LLMWrapper()

    agent = SQLAgentGraph(retriever, reranker, prompt_manager, wrapper, executor, corrector)

    for i, question in enumerate(test_questions_1):
        logger.info(f"query {i+1}: {question}")

        initial_state = {
            "question": question,
            "retry_count": 0
        }

        final_state = await agent.graph.ainvoke(initial_state)
        
        res_query = final_state.get('query')
        res_status = final_state.get('status')
        res_db = final_state.get('result_from_db')
        res_err = final_state.get('error_from_db')

        logger.info(f"Question: {question}")
        logger.info(f"SQL: {res_query}")
        logger.info(f"Status: {res_status}")

        if res_status == "success":
            res_str = str(res_db)
            logger.info(f"Result : {res_str[:300]}...") 
        else:
            logger.error(f"Error Message: {res_err}")

if __name__ == "__main__":
    asyncio.run(t_graph())
