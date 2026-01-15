import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from src.core.logger import setup_logger
logger = setup_logger("migration")

class DatabaseMigration:
    def __init__(self, settings, sql_lite_path):
        self.settings = settings
        self.sqlite_path = sql_lite_path
        self.pg_engine = create_engine(self.settings.db_url_sunc)

    def migrate_db(self, db_id):
        schema = db_id.lower()

        with self.pg_engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        with sqlite3.connect(self.sqlite_path) as sqlite_conn:
            cursor = sqlite_conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
            tables = [t[0] for t in cursor.fetchall() if t[0] != 'sqlite_sequence']

            for table in tables:
                df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
                df.columns = [c.lower() for c in df.columns]
                df.to_sql(name=table.lower(), con=self.pg_engine, if_exists="replace")
                logger.info(f"Migrated {table}")

            self._apply_constraints(schema, tables, cursor)

    def _apply_constraints(self, schema, tables, sqlite_cursor):
        with self.pg_engine.connect() as pg_conn:
            for table in tables:
                t_low = table.lower()

                sqlite_cursor.execute(f"PRAGMA table_info('{table}')")
                pks = [row[1].lower() for row in sqlite_cursor.fetchall() if row[5]>0]
                if pks:
                    pg_conn.execute(text(f'ALTER TABLE {schema}."{t_low}" ADD PRIMARY KEY ({", ".join(pks)})'))

                sqlite_cursor.execute(f"PRAGMA foreign_key_list('{table}')")
                for fk in sqlite_cursor.fetchall():
                    target_t, from_c, to_c = fk[2].lower(), fk[3].lower(), fk[4].lower()
                    pg_conn.execute(text(f"""
                    ALTER TABLE {schema}."{t_low}"
                    ADD CONSTRAINT fk_{t_low}_{from_c}
                    FOREIGN KEY ({from_c}) REFERENCES {schema}."{target_t}"({to_c})
                """))
            pg_conn.commit()
            logger.info(f"fk and pks recovery for {schema}")