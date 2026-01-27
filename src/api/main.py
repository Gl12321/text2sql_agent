from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Union
import json
import asyncio
import logging
import psutil
import os

from src.core.logger import setup_logger, AsyncLogHandler
from src.core.config import get_settings
from src.database.migration import DatabaseMigration
from src.database.schema_parser import SchemaParser
from src.rag.cataloger import SchemaCataloger
from src.rag.embedder import TableEmbedder
from src.rag.retriver import TableRetriever
from src.rag.reranker import TableReranker
from src.agent.llm.prompts import PromptManager
from src.agent.llm.wrapper import LLMWrapper
from src.agent.executor import SQLExecutor
from src.agent.corrector import SQLCorrector
from src.agent.graph import SQLAgentGraph


settings = get_settings()
logger = setup_logger("API")

TOP_K = 10

@asynccontextmanager
async def lifespan(app: FastAPI):
    process = psutil.Process(os.getpid())
    memory_before_load = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory before load: {memory_before_load:.2f} MB")

    schema_migrator = DatabaseMigration()
    schema_manager = SchemaParser()
    schema_cataloger = SchemaCataloger()
    app.state.schema_migrator = schema_migrator
    app.state.schema_manager = schema_manager
    app.state.schema_cataloger = schema_cataloger
    tables_collection_obj = schema_cataloger.table_collection

    embedder = TableEmbedder()
    retriever = TableRetriever(
        collection=tables_collection_obj,
        embedder=embedder,
        schemas_id=[],
        top_k=TOP_K
    )
    reranker = TableReranker()
    prompt_manager = PromptManager()
    llm_wrapper = LLMWrapper()
    executor = SQLExecutor()
    corrector = SQLCorrector()

    agent = SQLAgentGraph(retriever, reranker, prompt_manager, llm_wrapper, executor, corrector)
    app.state.pipline = agent

    memory_after_load = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory after load {memory_after_load} MB different:"
                f" {memory_after_load - memory_before_load}")
    yield

app = FastAPI(title="Enterprise SQL Agent", lifespan=lifespan)

class QueryRequest(BaseModel):
    question: str
    schemas_for_search: Union[list[str], str]

@app.post("/load_schema")
async def load_schema(request: Request, files: list[UploadFile] = File(...)):
    storage_path = settings.SQLITE_PATH

    if not os.path.exists(storage_path):
        os.makedirs(storage_path, exist_ok=True)

    try:
        loaded_schemas = []
        for file in files:
            if not file.filename.endswith(('.db', '.sqlite', '.sqlite3')):
                continue

            target_path = os.path.join(storage_path, file.filename)

            with open(target_path, "wb") as buffer:
                while content := await file.read(1024 * 1024):
                    buffer.write(content)

            schema_id = os.path.splitext(file.filename)[0]

            if asyncio.iscoroutinefunction(request.app.state.schema_migrator.migrate_db):
                 await request.app.state.schema_migrator.migrate_db(schema_id, target_path)
            else:
                 request.app.state.schema_migrator.migrate_db(schema_id, target_path)
            loaded_schemas.append(schema_id)

            await request.app.state.schema_cataloger.index_schema(schema_id)
        return {"status": "success", "loaded_schemas": loaded_schemas}

    except Exception as e:
        logger.error(f"Error loading schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/drop_all_schemas")
async def drop_all_schemas(request: Request):
    await request.app.state.schema_manager.drop_all_schemas()
    await request.app.state.schema_cataloger.reset_store()

@app.post("/schema_show")
async def schema_show(request: Request):
    schemas = await request.app.state.schema_manager.get_all_schemas()
    return {"schemas": schemas}
    
@app.post("/question/stream")
async def ask_sql(request: Request, query_data: QueryRequest):
    log_queue = asyncio.Queue()
    handler = AsyncLogHandler(log_queue)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%H:%M:%S"))

    logging.getLogger().addHandler(handler)
    
    if query_data.schemas_for_search == "all":
        query_data.schemas_for_search = await request.app.state.schema_manager.get_all_schemas()

    initial_state = {
        "question": query_data.question,
        "requested_schemes": query_data.schemas_for_search,
        "retry_count": 0
    }
    agent = request.app.state.pipline

    async def stream_generator():
        try:
            task = asyncio.create_task(agent.graph.ainvoke(initial_state))

            while not task.done():
                try:
                    log_entry = await asyncio.wait_for(log_queue.get(), timeout=0.1)
                    yield json.dumps({"event": "log", "content": log_entry}) + "\n"
                except asyncio.TimeoutError:
                    continue
            
            final_state = await task
            result = {
                "event": "result",
                "content": {
                    "question": query_data.question,
                    "status": final_state["status"],
                    "data": final_state.get("result_from_db")
                }
            }
            yield json.dumps(result) + "\n"

        except Exception as e:
            logger.error(f"Error in stream: {e}")
            yield json.dumps({"event": "error", "content": str(e)}) + "\n"
        finally:
            logging.getLogger().removeHandler(handler)

    return StreamingResponse(stream_generator(), media_type="application/x-ndjson")
    
