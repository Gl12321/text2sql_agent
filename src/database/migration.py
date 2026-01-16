import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from src.core.logger import setup_logger
logger = setup_logger("migration")

class DatabaseMigration:
    def __init__(self, settings, provider):
        self.settings = settings
        self.provider = provider
        self.pg_engine = create_engine(self.settings.db_url_sync)

    def migrate_db(self, db_id):
        schema = db_id.lower()

        with self.pg_engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        with sqlite3.connect(self.provider.get_sql_path(db_id)) as sqlite_conn:
            cursor = sqlite_conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
            tables = [t[0] for t in cursor.fetchall() if t[0] != 'sqlite_sequence']

            for table in tables:
                df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
                df.columns = [c.lower() for c in df.columns]
                df.to_sql(
                    name=table.lower(),
                    con=self.pg_engine,
                    schema=schema,
                    if_exists="replace",
                    index=False
                )
                logger.info(f"Migrated {table}")

            self._apply_constraints(schema, tables, cursor)

    def _apply_constraints(self, schema, tables, sqlite_cursor):
        with self.pg_engine.connect() as pg_conn:
            for table in tables:
                t_low = table.lower()

                sqlite_cursor.execute(f"PRAGMA table_info('{table}')")
                pks = [row[1].lower() for row in sqlite_cursor.fetchall() if row[5] > 0]
                if pks:
                    pg_conn.execute(text(f'ALTER TABLE {schema}."{t_low}" ADD PRIMARY KEY ({", ".join(pks)})'))

            pg_conn.commit()

            for table in tables:
                t_low = table.lower()
                sqlite_cursor.execute(f"PRAGMA foreign_key_list('{table}')")
                fks = sqlite_cursor.fetchall()

                from collections import defaultdict
                grouped_fks = defaultdict(lambda: {"table": "", "from": [], "to": []})

                for fk in fks:
                    fk_id, target_t = fk[0], fk[2].lower()
                    grouped_fks[fk_id]["table"] = target_t
                    grouped_fks[fk_id]["from"].append(fk[3].lower())
                    grouped_fks[fk_id]["to"].append(fk[4].lower())

                for fk_id, data in grouped_fks.items():
                    from_cols = ", ".join(data["from"])
                    to_cols = ", ".join(data["to"])
                    target_t = data["table"]

                    pg_conn.execute(text(f"""
                        ALTER TABLE {schema}."{t_low}"
                        ADD CONSTRAINT fk_{t_low}_{target_t}_{fk_id}
                        FOREIGN KEY ({from_cols}) 
                        REFERENCES {schema}."{target_t}"({to_cols})
                    """))

            pg_conn.commit()
            logger.info(f"FK and PK recovery for {schema} completed")