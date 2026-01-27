import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from pathlib import Path

BASE_DIR = Path(__file__).parents[2]
ENV_FILE = os.path.join(BASE_DIR, ".env")

class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str = "db"
    DB_PORT: int = 5432
    DB_NAME: str

    VECTOR_DB_PATH: str = str(BASE_DIR / "data"/ "chromadb")
    SQLITE_PATH: str = str(BASE_DIR / "data" / "sqlite_storage")

    MODELS: dict = {
        "llm_1": {
            "local_path": str(BASE_DIR / "models" / "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf"),
            "repo_id": "MaziyarPanahi/Meta-Llama-3-8B-Instruct-GGUF",
            "filename": "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
        },
        "llm_2": {
            "repo_id": "SandLogicTechnologies/Llama-3-Sqlcoder-8B-GGUF",
            "filename": "llama-3-sqlcoder-8b.Q4_K_M.gguf",
            "params": {
                "model_path": str(BASE_DIR / "models" / "llama-3-sqlcoder-8b.Q4_K_M.gguf"),
                "temperature": 0.0,
                "max_tokens": 512,
                "n_ctx": 8192,
                "n_batch": 1024,
                "verbose": False,
                "n_gpu_layers": 0,
                "n_threads": 5,
                "n_threads_batch": 5,
            }
        },
        "reranker": {
            "repo_id": "BAAI/bge-reranker-base",
            "cache_path": str(BASE_DIR / "models" / "reranker")
        },
        "embedder": {
            "repo_id": "BAAI/bge-m3",
            "cache_path": str(BASE_DIR / "models" / "embedder")
        }
    }


    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding='utf-8',
        extra="ignore"
    )

    @property
    def db_url_async(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def db_url_sync(self) -> str:
        return f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
