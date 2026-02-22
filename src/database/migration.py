import sqlite3
import pandas as pd
import os

from sqlalchemy import create_engine, text, MetaData, select
from sqlalchemy.schema import AddConstraint, ForeignKeyConstraint
from src.core.logger import setup_logger
from src.core.config import get_settings

settings = get_settings()
logger = setup_logger("SCHEMA_MIGRATION")


class DatabaseMigration:
    def __init__(self):
        self.settings = settings

    def migrate_db(self, db_id, sql_path):
        sqlite_engine = create_engine(f"sqlite:///{sql_path}")
        pg_engine = create_engine(self.settings.db_url_sync)
        schema = db_id.lower()

        metadata = MetaData()
        metadata.reflect(bind=sqlite_engine)

        for table in metadata.tables.values():
            table.schema = schema
            for fk in table.foreign_keys:
                fk.parent.type = fk.column.type

        with pg_engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        metadata.create_all(bind=pg_engine)
        self._transfer_data(metadata, schema, sqlite_engine, pg_engine)
        self._fix_sequences(schema, metadata, sqlite_engine, pg_engine)

    def remove_sqlite(self, sql_path):
        os.remove(sql_path)

    def _transfer_data(self, metadata, schema, sqlite_engine, pg_engine):
        with sqlite_engine.connect() as s_conn, pg_engine.connect() as pg_conn:
            for table in metadata.sorted_tables:
                real_schema = table.schema
                table.schema = None
                select_stmt = select(table)
                result = s_conn.execution_options(stream_results=True).execute(select_stmt)
                table.schema = real_schema

                while True:
                    batch = result.fetchmany(2000)
                    if not batch:
                        break

                    data = [dict(row._mapping) for row in batch]
                    pg_conn.execute(table.insert(), data)

                pg_conn.commit()

        logger.info(f"{schema} migrated")

    def _fix_sequences(self, schema, metadata, sqlite_engine, pg_engine):
        with pg_engine.connect() as conn:
            for table in metadata.tables.values():
                if not table.primary_key.columns:
                    continue
                pk_col = list(table.primary_key.columns)[0].name
                seq_sql = f"""
                    SELECT setval(pg_get_serial_sequence('{schema}."{table.name}"', '{pk_col}'), 
                    (SELECT COALESCE(MAX("{pk_col}"), 1) FROM {schema}."{table.name}"));
                """
                conn.execute(text(seq_sql))
            conn.commit()