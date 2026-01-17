import asyncio
from src.database.schema_parser import SchemaParser
from src.core.logger import setup_logger
from src.rag.embedder import TableEmbedder
from src.rag.serializer import TableSerializer
from src.rag.retriver import Retriver

logger = setup_logger("Cataloger")


class SchemaCataloger:
    def __init__(self):
        self.schema_parser = SchemaParser()
        self.retriver = Retriver()
        self.table_embedder = TableEmbedder()
        self.table_serializer = TableSerializer()

    async def index_schema(self, db_id):
        metadata_dict = await self.schema_parser.get_ddl_of_schema(db_id)
        if not metadata_dict:
            return

        table_names = []
        documents_of_ddl = []

        for table_info in metadata_dict.values():
            table_names.append(table_info["table_name"])

            document = self.table_serializer(table_info)
            documents_of_ddl.append(document)

        embeddings = self.table_embedder.get_embeddings(documents_of_ddl)
        metadatas = [{"db_id": db_id} for _ in table_names]

        self.retriver.add_tables(db_id, table_names, documents_of_ddl, embeddings, metadatas)
        logger.info(f"Successfully indexed {len(table_names)} tables for {db_id}")

    async def index_all_schemas(self):
        schemes = await self.schema_parser.get_all_schemas()
        for schema in schemes:
            await self.index_schema(schema)