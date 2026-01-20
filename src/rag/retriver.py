from typing import Any
from langchain_core.retrievers import BaseRetriever
from langchain_core.documents import Document
from langchain_core.callbacks import CallbackManagerForRetrieverRun

class TableRetriever(BaseRetriever):
    collection: Any
    embedder: Any
    db_id: str
    top_k: int = 2

    def _get_relevant_documents(self, query: str, *, run_manager: CallbackManagerForRetrieverRun):
        query_embedding = self.embedder.get_embeddings([query])[0]

        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=self.top_k,
            where={"db_id": self.db_id}
        )

        documents = []

        for i in range(len(results["ids"][0])):
            doc = Document(
                page_content=results["documents"][0][i],
                metadata=results["metadatas"][0][i]
            )
            documents.append(doc)

        return documents