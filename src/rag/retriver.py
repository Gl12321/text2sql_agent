import chromadb
from src.core.logger import setup_logger


logger = setup_logger("retriver")

class TableRetriver:
    def __init__(self, path = "../../data"):
        self.client = chromadb.PersistentClient(path=path)
        self.table_collection = self.client.get_or_create_collection(
            name="tables",
            metadata={"hnsw:space": "cosine"}
        )
        # ДОПИСАТЬ ПОИСК ПО СХЕМАМ
        self.shema_collection = self.client.get_or_create_collection(
            name="schemes",
            metadata={"hnsw:space": "cosine"}
        )

    def add_tables(self, db_id, tables_metadata: list[dict[str, any]], embeddings): pass
