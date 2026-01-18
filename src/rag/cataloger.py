import asyncio
import chromadb
from src.database.schema_parser import SchemaParser
from src.core.logger import setup_logger
from src.rag.embedder import TableEmbedder
from src.rag.serializer import TableSerializer

logger = setup_logger("Cataloger")


class SchemaCataloger:
    def __init__(self, db_path="/home/stanislav/Enterprice_SQL_agent/data"): #заменить абсолютный путь
        self.schema_parser = SchemaParser()
        self.table_embedder = TableEmbedder()
        self.table_serializer = TableSerializer()

        self.client = chromadb.PersistentClient(path=db_path)
        self.table_collection = self.client.get_or_create_collection(
            name="tables",
            metadata={"hnsw:space": "cosine"}
        )
        # ДОПИСАТЬ ПОИСК ПО СХЕМАМ С РЕРАНКЕРОМ
        self.shema_collection = self.client.get_or_create_collection(
            name="schemes",
            metadata={"hnsw:space": "cosine"}
        )

    def add_tables_to_store(
            self,
            db_id: str,
            table_names: list[str],
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
        logger.info(f"Successfully upserted {len(table_names)} tables to ChromaDB for: {db_id}")

    async def index_schema(self, db_id):
        metadata_dict = await self.schema_parser.get_ddl_of_schema(db_id)
        if not metadata_dict:
            return

        table_names = []
        documents_of_ddl = []
        metadatas = []

        for table_info in metadata_dict.values():
            name = table_info["table_name"]
            table_names.append(name)

            document = self.table_serializer(table_info)
            documents_of_ddl.append(document)

            metadatas.append({
                "db_id": db_id,
                "table_name": name
            })

        embeddings = self.table_embedder.get_embeddings(documents_of_ddl)

        self.add_tables_to_store(db_id, table_names, documents_of_ddl, embeddings, metadatas)
        logger.info(f"Successfully indexed {len(table_names)} tables for {db_id}")

    async def index_all_schemas(self):
        schemes = await self.schema_parser.get_all_schemas()
        for schema in schemes:
            await self.index_schema(schema)