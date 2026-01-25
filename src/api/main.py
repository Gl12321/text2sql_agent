from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
import psutil
import os

from src.core.logger import setup_logger
from src.core.config import get_settings
from src.rag.embedder import TableEmbedder
from src.rag.retriver import TableRetriever
from src.llm.promts import PromptManager
from src.llm.wrapper import LLMWrapper
from src.agent.executor import SQLExecutor
from src.agent.corrector import SQLCorrector
from src.llm.graph import SQLAgentGraph


settings = get_settings()
logger = setup_logger("API")
state = {}

test_db = "collage_2" #убрать после добавления коллектора

@asynccontextmanager
async def lifespan(app: FastAPI):
    process = psutil.Process(os.getpid())
    memory_before_load = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory before load: {memory_before_load:.2f} MB")

    embedder = TableEmbedder()
    retriever = TableRetriever(                    #добавить коллекцию таблиц
        collection="table_collection",
        embedder=embedder,
        db_id=test_db,
        top_k=2
    )
    prompt_manager = PromptManager()
    llm_wrapper = LLMWrapper()
    executor = SQLExecutor()
    corrector = SQLCorrector()

    agent = SQLAgentGraph(retriever, prompt_manager, llm_wrapper, executor, corrector)
    state["pipline"] = agent

    memory_after_load = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory after load {memory_after_load} MB different:"
                f" {memory_after_load - memory_before_load}")
    yield
    state.clear()

app = FastAPI(title="Enterprise SQL Angent", lifespan=lifespan)

class QueryRequest(BaseModel):
    question: str

@app.post("/ask")
async def ask_sql(request: QueryRequest):
    try:
        agent = state["pipline"]
        initial_state = {
            "question": request.question,
            "db_id": test_db,
            "retry_count": 0
        }
        final_state = await agent.graph.ainvoke(initial_state)

        if final_state.get("status") == "success":
            return {
                "question": request.question,
                "status": "success",
                "sql": final_state.get("query"),
                "data": final_state.get("result_from_db")
            }
        else:
            return {
                "question": request.question,
                "status": "error",
                "error": final_state.get("error_from_db"),
                "failed_sql": final_state.get("error_query")
            }

    except Exception as e:
        logger.error(f"Request processing error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

