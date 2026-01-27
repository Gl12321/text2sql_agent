import asyncio
import chromadb
import shutil
import os
from src.database.schema_parser import SchemaParser
from src.core.logger import setup_logger
from src.rag.embedder import TableEmbedder
from src.rag.serializer import TableSerializer
from src.core.config import get_settings


config = get_settings()
logger = setup_logger("CATALOGER")

class SchemaCataloger:
    def __init__(self, db_path=config.VECTOR_DB_PATH):
        self.schema_parser = SchemaParser()
        self.table_embedder = TableEmbedder()
        self.table_serializer = TableSerializer()

        self.client = chromadb.PersistentClient(path=db_path)
        self.table_collection = self.client.get_or_create_collection(
            name="tables",
            metadata={"hnsw:space": "cosine"}
        )

    def add_tables_to_store(
            self,schema_id, table_names, documents_of_ddl, embeddings, metadatas
    ):
        ids = [f"{schema_id}.{name}" for name in table_names]
        self.table_collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents_of_ddl,
            metadatas=metadatas,
        )
        logger.info(f"Successfully upserted {len(table_names)} tables to ChromaDB for: {schema_id}")

    async def index_schema(self, schema_id):
        metadata_dict = await self.schema_parser.get_info_of_schema(schema_id)
        if not metadata_dict:
            return

        table_names = []
        documents_of_ddl = []
        serialized_tables = []
        metadatas = []

        for table_info in metadata_dict.values():
            name = table_info["table_name"]
            column_names = [col["name"] for col in table_info["columns"]]
            table_names.append(name)

            document = await self.schema_parser.get_ddl(schema_id, name)
            documents_of_ddl.append(document)
            serialized_table = self.table_serializer(table_info)
            serialized_tables.append(serialized_table)

            metadatas.append({
                "schema_id": schema_id,
                "table_name": name,
                "column_names": ",".join(column_names),
                "serialized_table": serialized_table
            })

        embeddings = await self.table_embedder.get_embeddings(serialized_tables)
        self.add_tables_to_store(schema_id, table_names, documents_of_ddl, embeddings, metadatas)

        logger.info(f"Successfully indexed {len(table_names)} tables for {schema_id}")

    async def index_all_schemas(self):
        schemes = await self.schema_parser.get_all_schemas()
        for schema in schemes:
            await self.index_schema(schema)

    async def reset_store(self):
        try:
            self.client.delete_collection(name="tables")
        except:
            pass
        
        self.table_collection = self.client.get_or_create_collection(
            name="tables",
            metadata={"hnsw:space": "cosine"}
        )
        logger.info("Vector store reset complete.")