from typing import Any
from langchain_core.retrievers import BaseRetriever
from langchain_core.documents import Document
from langchain_core.callbacks import CallbackManagerForRetrieverRun


class TableRetriever(BaseRetriever):
    collections: Any
    embedder: Any
    db_id: str
    top_k: int = 2

    def _get_relevant_documents(self, query: str, *, run_manager: CallbackManagerForRetrieverRun):
        query_embedding = self.embedder(query)

        filter = {"db_id": self.db_id}

        results = self.collection.query(
            query_embedding=[query_embedding],
            n_results=self.top_k,
            where=filter
        )

        documents = []
        for i in range(len(results["ids"][0])):
            doc = Document(
                page_content=results["ids"][0][i],
                metadata=results["metadata"][0][i]
            )
            documents.append(doc)

        return documents