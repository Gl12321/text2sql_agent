from sentence_transformers import SentenceTransformer
from src.core.logger import setup_logger
from src.core.config import get_settings


logger = setup_logger("embedder")
settings = get_settings()


class TableEmbedder:
    def __init__(self):
        embedder_config = settings.MODELS["embedder"]
        model_name = embedder_config["repo_id"]
        cache_path = embedder_config["cache_path"]

        self.device = "cpu"

        self.model = SentenceTransformer(
            model_name,
            device=self.device,
            cache_folder=cache_path
        )
        logger.info("Model loaded")

    def get_embeddings(self, serialized_tables):
        embeddings = self.model.encode(
            serialized_tables,
            normalize_embeddings=True,
            show_progress_bar=False
        )

        return embeddings