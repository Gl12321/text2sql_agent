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

        logger.info("Downloading file via gdown")
        gdown.download(id=file_id, output=self.zip_path, quiet=False)

    def extract_zip(self):
        if not os.path.exists(self.db_dir):
            logger.info("Extracting archive")
            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.raw_data_dir)

    def migrate_database(self, db_id: str):



    def run(self, dbs):
        # self._download_file()
        # self.extract_zip()
        for db in dbs:
            self.migrate_database(db)


if __name__ == "__main__":
    etl = SpiderETL()
    etl.run(["college_2", "f1_stats", "world_1", "musical_1"])