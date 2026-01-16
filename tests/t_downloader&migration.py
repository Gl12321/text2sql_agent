import os
from src.database.downloader import DatasetProvider
from src.database.migration import DatabaseMigration
from src.core.config import Settings
from sqlalchemy import text


def main():
    provider = DatasetProvider()
    provider.ensure_data_ready()

    settings = Settings()
    migrator = DatabaseMigration(settings, provider)

    test_dbs = ["college_2", "bike_1", "department_management"]

    for db_id in test_dbs:
        migrator.migrate_db(db_id)

    with migrator.pg_engine.connect() as conn:
        for db_id in test_dbs:
            schema = db_id.lower()
            query = text(f"""
                SELECT count(*) FROM information_schema.tables 
                WHERE table_schema = '{schema}'
            """)
            count = conn.execute(query).scalar()

if __name__ == "__main__":
    main()