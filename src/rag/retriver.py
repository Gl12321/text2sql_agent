import chromadb
from typing import Any
from src.core.logger import setup_logger


logger = setup_logger("retriver")

class Retriver:
    def __init__(self, path = "/home/stanislav/Enterprice_SQL_agent/data"):
        self.client = chromadb.PersistentClient(path=path)
        self.table_collection = self.client.get_or_create_collection(
            name="tables",
            metadata={"hnsw:space": "cosine"}
        )
        # ДОПИСАТЬ ПОИСК ПО СХЕМАМ С РЕРАНКЕРОМ
        self.shema_collection = self.client.get_or_create_collection(
            name="schemes",
            metadata={"hnsw:space": "cosine"}
        )

    def add_tables(
            self,
            db_id, table_names: list[str],
            documents_of_ddl: list[str],
            embeddings: list[list[float]],
            metadatas: list[dict]
    ):
        ids = [f"{db_id}.{name}" for name in table_names]
        self.table_collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents_of_ddl,
            metadatas=metadatas
        )
        logger.info(f"Added tables of schema: {db_id}")

    def search_tables(self, query_embedding, db_id, top_k=5):
        search_filter = {"db_id": db_id}

        results = self.table_collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=search_filter
        )

        return results["documents"][0]
