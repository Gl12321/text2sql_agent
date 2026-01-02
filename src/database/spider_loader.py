import os
import sqlite3
import zipfile
import gdown
import pandas as pd
from sqlalchemy import create_engine, text
from src.core.config import get_settings
from src.core.logger import setup_logger

logger = setup_logger("spider_etl")
settings = get_settings()


class SpiderETL:
    def __init__(self):
        self.settings = settings
        self.raw_data_dir = os.path.expanduser("~/.cache/raw_spider")
        self.zip_path = os.path.join(self.raw_data_dir, "spider_raw.zip")
        self.db_dir = os.path.join(self.raw_data_dir, "database")
        self.pg_engine = create_engine(self.settings.db_url_sync)

    def _download_file(self):
        if os.path.exists(self.zip_path):
            return

        os.makedirs(self.raw_data_dir, exist_ok=True)
        file_id = "1TqleXec_OykOYFREKKtschzY29dUcVAQ"

        logger.info("Downloading file via gdown...")
        gdown.download(id=file_id, output=self.zip_path, quiet=False)

    def extract_zip(self):
        if not os.path.exists(self.db_dir):
            logger.info("Extracting archive...")
            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.raw_data_dir)

    def migrate_database(self, db_id: str):
        base_search = os.path.join(self.raw_data_dir, "spider") if os.path.exists(
            os.path.join(self.raw_data_dir, "spider")) else self.raw_data_dir
        sqlite_file = os.path.join(base_search, "database", db_id, f"{db_id}.sqlite")

        if not os.path.exists(sqlite_file):
            logger.error(f"DB [{db_id}] NOT found at {sqlite_file}")
            return

        logger.info(f"--- Migrating: {db_id} ---")
        with self.pg_engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {db_id.lower()}"))
            conn.commit()

        with sqlite3.connect(sqlite_file) as sqlite_conn:
            cursor = sqlite_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [t[0] for t in cursor.fetchall() if t[0] != 'sqlite_sequence']

            for table in tables:
                df = pd.read_sql_query(f'SELECT * FROM "{table}"', sqlite_conn)
                df.columns = [c.lower() for c in df.columns]
                df.to_sql(name=table.lower(), con=self.pg_engine, schema=db_id.lower(), if_exists="replace",
                          index=False)
                logger.info(f"  Table '{table}' OK")

    def run(self, dbs):
        self._download_file()
        self.extract_zip()
        for db in dbs:
            self.migrate_database(db)


if __name__ == "__main__":
    etl = SpiderETL()
    etl.run(["college_2", "f1_stats", "world_1", "musical_1"])