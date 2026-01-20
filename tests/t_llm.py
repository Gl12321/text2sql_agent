from src.rag.retriver import TableRetriever
from src.llm.promts import PromptManager
from src.core.logger import setup_logger
from src.rag.embedder import TableEmbedder
from langchain_core.documents import Document
from src.core.config import get_settings
from src.llm.wrapper import LLMWrapper
import chromadb


logger = setup_logger("test")
config = get_settings()
test_db = "college_2"
def t_llm():
    queries = ["Find the names of all students in the Comp. Sci. department",
              "Find the names and salaries of instructors who earn more than 70000",
              "Count the number of courses in each department"]
    prompt_manager = PromptManager()
    embedder = TableEmbedder()
    client = chromadb.PersistentClient(config.VECTOR_DB_PATH)
    collection = client.get_collection("tables")
    retriever = TableRetriever(collection=collection, embedder=embedder, db_id=test_db, top_k=4)
    wrapper = LLMWrapper()

    for i, query in enumerate(queries):
        logger.info(f"query {i+1}: {query}")

        results_of_retriver = retriever.invoke(query)
        tables = [doc.metadata.get("table_name") for doc in results_of_retriver]
        raw_cols = [doc.metadata.get("column_names", "") for doc in results_of_retriver]
        columns = list(set(c.strip() for s in raw_cols for c in s.split(",") if c))
        logger.info(f"found tables: {' '.join(tables)} columns: {columns}")

        prompt = prompt_manager.build_sql_prompt(query, results_of_retriver)
        logger.info(f"Full prompt:\n{prompt}")
        
        llm = wrapper.get_chain(tables=tables, columns=columns)
        sql_query = llm.invoke(prompt)
        logger.info(f"result of llm {sql_query}")
        
if __name__ == "__main__":
    t_llm()
    
