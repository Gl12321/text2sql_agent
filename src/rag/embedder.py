import torch
from sentence_transformers import SentenceTransformer
from src.core.logger import setup_logger


logger = setup_logger("embedder")

class TableEmbedder:
    def __init__(self, model_name="BAAI/bge-m3"):
        self.device = "cpu"
        self.model = SentenceTransformer(model_name, device=self.device)

    def get_embeddngs(self, ddl: str):
        embeddings = self.model.encode(
            ddl,
            normalize_embeddings=True,
            show_progress_bar=False
        )

        return embeddings

