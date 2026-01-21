from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.core.logger import setup_logger
from src.core.config import get_settings
from contextlib import asynccontextmanager
from src.llm.chains import SQLAgentGraph
import psutil
import os


settings = get_settings()
logger = setup_logger("api")
state = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    process = psutil.Process(os.getpid())
    memory_before_load = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory before load: {memory_before_load:.2f} MB")

    agent = SQLAgentGraph()
    state["pipline"] = agent.get_pipeline()

    memory_after_load = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory after load {memory_after_load} MB}, different:"
                f" {memory_after_load - memory_before_load}")
    yield
    state.clear()

app = FastAPI(title="Enterprise SQL Angent", lifespan=lifespan)

class QueryRequest(BaseModel):
    question: str

@app.post("/ask")
async def ask_sql(request: QueryRequest):
    try:
        result = state["pipline"].invoke(request)

        return {
            "question": request.question,
            "status": result["status"],
            "data": result.get("data") if result["status"] == "success" else None,
            "error": result.get("error") if result["status"] == "error" else None
        }
    except Exception as e:
        logger.error(f"Request processing error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

