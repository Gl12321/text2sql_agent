from sqlalchemy import inspect, text
from src.database.postgres_client import async_db
from src.core.logger import setup_logger

logger = setup_logger("schema_parser")


class SchemaParser:
    def __init__(self):
        self.db_manager = async_db

    async def get_all_schemas(self) -> list[str]:
        async with self.db_manager.engine.connect() as conn:
            query = text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'public')
                  AND schema_name NOT LIKE 'pg_%'
            """)
            result = await conn.execute(query)
            return [row[0] for row in result]

    async def get_ddl_of_schema(self, schema_name: str) -> dict:

        def get_sync_info(connection):
            inspector = inspect(connection)
            tables = inspector.get_table_names(schema=schema_name)

            schema_metadata = {}
            for table in tables:
                table_info = {}
                table_info["schema_name"] = schema_name
                table_info["table_name"] = table
                table_info["columns"] = inspector.get_columns(table, schema=schema_name)

                pk_info = inspector.get_pk_constraint(table, schema=schema_name)
                table_info["primary_keys"] = pk_info.get('constrained_columns', [])

                table_info["foreign_keys"] = inspector.get_foreign_keys(table, schema=schema_name)

                schema_metadata[table] = table_info

            return schema_metadata

        async with self.db_manager.engine.connect() as conn:
            return await conn.run_sync(get_sync_info)


if __name__ == "__main__":
    import asyncio
    async def main():
        parser = SchemaParser()
        schemas = await parser.get_all_schemas()
        logger.info(f"Available schemas: {schemas}")

        if schemas:
            test_schema = schemas[0]
            logger.info(f"Analyzing schema: {test_schema}")
            ddl = await parser.get_ddl_of_schema(test_schema)

            print(f"Structure for {test_schema}: {ddl}")


    asyncio.run(main())
