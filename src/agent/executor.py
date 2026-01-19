from sqlalchemy import text
from src.database.postgres_client import async_db

class SQLExecutor:
    def __init__(self):
        self.engine = async_db().engine

    async def execute(self, sql_query):
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text(sql_query))

                data = [dict(row) for row in result.mapping().all()]

                return {
                    "status": "success",
                    "data": data,
                    "query": sql_query
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "query": sql_query
            }