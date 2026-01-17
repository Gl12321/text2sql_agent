import torch
from sentence_transformers import SentenceTransformer
from src.core.logger import setup_logger
import transformers


logger = setup_logger("embedder")

class TableEmbedder:
    def __init__(self, model_name="BAAI/bge-m3"):
        self.device = "cpu"
        logger.info(f"Start loading {model_name}")
        self.model = SentenceTransformer(model_name, device=self.device)
        logger.info("Model loaded")

    def get_embeddings(self, ddls: list[str]):
        embeddings = self.model.encode(
            ddls,
            normalize_embeddings=True,
            show_progress_bar=False
        )

        return embeddings

