from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.core.logger import setup_logger
from contextlib import asynccontextmanager
from src.llm.wrapper import LLMWrapper
from src.rag.retriver import TableRetriever
from src.rag.embedder import TableEmbedder
from src.llm.promts import PromptManager
from src.agent.executor import SQLExecutor
from src.agent.corrector import SQLCorrector
from src.core.config import get_settings
import chromadb


settings = get_settings()
logger = setup_logger("api")
state = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    embedder = TableEmbedder()
    client = chromadb.PersistentClient("./data")
    collection = client.get_collection("tables")
    state["prompt_manager"] = PromptManager()
    state["llm_wrapper"] = LLMWrapper()

    state["retriever"] = TableRetriever(
        collection=collection,
        embedder=embedder,
        top_k=5,
        db_id="collage_2"
    )

    state["executor"] = SQLExecutor()
    state["corrector"] = SQLCorrector(state["llm_wrapper"])

    logger.info("all components initialized")
    yield
    state.clear()

app = FastAPI(title="Enterprise SQL Angent", lifespan=lifespan)

class QueryRequest(BaseModel):
    question: str

@app.post("/ask")
async def ask_sql(request: QueryRequest):
    try:
        docs = state["retriever"].get_docs(request.question)
        if not docs:
            raise HTTPException(status_code=404, detail="No relevant tables found in schema.")

        tables = [d.metadata["table_name"] for d in docs]
        nested_cols = [d.metadata.get("column_names").split(",") for d in docs]
        columns = list(set(col.strip() for sublist in nested_cols for col in sublist if col))

        promt = state["prompt_manager"].build_sql_prompt(request.question, docs)
        sql_chain = state["llm_wrapper"].get_chain(tables, columns)
        sql_query = await sql_chain.ainvoke(promt)

        result = await state["executor"].execute(sql_query)

        if result["status"] == "error":
            logger.warning(f"Execution failed. Error: {result['error']}. Starting correction")

            context_ddl = "\n\n".join([d.page_content for d in docs])

            corrected_sql = await state["corrector"].correct(
                context=context_ddl,
                failed_sql=sql_query,
                error_msg=result["error"],
                tables=tables,
                columns=columns
            )

            result = await state["executor"].execute(corrected_sql)
            sql_query = corrected_sql

        return {
            "question": request.question,
            "sql": sql_query,
            "status": result["status"],
            "data": result.get("data") if result["status"] == "success" else None,
            "error": result.get("error") if result["status"] == "error" else None
        }
    except Exception as e:
        logger.error(f"Request processing error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

