from src.database.schema_parser import SchemaParser
from src.database.migration import DatabaseMigration
from src.core.logger import setup_logger
from src.core.config import get_settings
import asyncio
import os


logger = setup_logger("TEST_MIGRATION")
settings = get_settings()

async def test_migration():
    test_db = "bike_1"
    test_db_path = os.path.join(settings.SQLITE_PATH, test_db) + ".sqlite"

    migrator = DatabaseMigration()
    parser = SchemaParser()

    await parser.drop_all_schemas()
    migrator.migrate_db(test_db, test_db_path)
    schema = await parser.get_info_of_schema(test_db)

    logger.info(f"Schema {test_db}: {schema}")

if __name__ == "__main__":
    asyncio.run(test_migration())


