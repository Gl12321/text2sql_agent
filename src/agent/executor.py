from sqlalchemy import text
from src.database.postgres_client import async_db
from src.core.logger import setup_logger


logger = setup_logger("executer")

class SQLExecutor:
    def __init__(self):
        self.engine = async_db.engine

    async def execute(self, sql_query: str):
        try:
            clean_query = sql_query.strip().split(';')[0] + ';'

            async with self.engine.connect() as conn:
                result = await conn.execute(text(clean_query))
                data = [dict(row) for row in result.mappings().all()]
                await conn.commit()

                return {
                    "status": "success",
                    "data": data,
                    "query": clean_query
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "query": sql_query
            }
