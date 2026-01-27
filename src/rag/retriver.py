from typing import Any
import asyncio
from langchain_core.retrievers import BaseRetriever
from langchain_core.documents import Document
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from src.core.logger import setup_logger


logger = setup_logger("RETRIEVER")

class TableRetriever(BaseRetriever):
    collection: Any
    embedder: Any
    schemas_id: list[str]
    top_k: int

    def _get_relevant_documents(self, query: str, *, run_manager: CallbackManagerForRetrieverRun):
        raise NotImplementedError("Need use ainvoke()")

    async def _aget_relevant_documents(self, query: str, *, run_manager: CallbackManagerForRetrieverRun):
        embeddings = await self.embedder.get_embeddings([query])
        query_embedding = embeddings[0]
        filter_schemas = {"schema_id": {"$in": self.schemas_id}}

        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=self.top_k,
            where=filter_schemas,
        )
        logger.info(f"Found tables of retriever: {results['ids']}")
        
        documents = []

        for i in range(len(results["ids"][0])):
            doc = Document(
                page_content=results["documents"][0][i],
                metadata=results["metadatas"][0][i]
            )
            documents.append(doc)

        return documents
