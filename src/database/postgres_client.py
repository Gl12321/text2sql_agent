from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from src.core.config import get_settings
from src.core.logger import setup_logger


logger = setup_logger("postgres_client")
settings = get_settings()

class PostgresClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PostgresClient, cls).__new__(cls)

            cls._instance.engine = create_async_engine(
                settings.db_url_async,
                echo=False,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20
            )

            cls._instance.session_factory = async_sessionmaker(
                bind=cls._instance.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            logger.info("Async PostgreSQL Engine initialized.")

        return cls._instance

    def get_session_marker(self):
        return self.session_factory

async_db = PostgresClient()