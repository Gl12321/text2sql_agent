from transformers import AutoModelForSequenceClassification, AutoTokenizer
from langchain_core.documents import Document
import asyncio
import torch

from src.core.config import get_settings
from src.core.logger import setup_logger


logger = setup_logger("RERANKER")
settings = get_settings()

class TableReranker:
    def __init__(self, threshold: float = 0.03):
        self.threshold = threshold
        self.model_name = settings.MODELS["reranker"]["repo_id"]
        self.cache_path = settings.MODELS["reranker"]["cache_path"]

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.model_name,
            cache_dir=self.cache_path
        )
        self.model = AutoModelForSequenceClassification.from_pretrained(
            self.model_name,
            cache_dir=self.cache_path
        )
        logger.info("Model loaded")

        self.model.eval()
        self.device = "cpu"
        self.model.to(self.device)

    async def rerank(self, question, raw_documents: list[Document]):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_rerank, question, raw_documents)
        
    def _sync_rerank(self, question, raw_documents):
        base_question = "Does this database table contain the answer to the question?"
        pairs = [[question, "".join([base_question, doc.metadata.get("serialized_table")])] for doc in raw_documents]

        with torch.no_grad():
            inputs = self.tokenizer(pairs, padding=True, truncation=True, max_length=512,
                                    return_tensors='pt').to(self.device)

            logits = self.model(**inputs).logits.view(-1)
            scores = torch.sigmoid(logits).cpu().tolist()
            logger.info(f"scors: {sorted(scores, reverse=True)}")
            
            relevant_docs = [
                    doc for doc, score in zip(raw_documents, scores)
                    if score >= self.threshold
                ]
            
            tables = [f"{doc.metadata.get('schema_id')}.{doc.metadata.get('table_name')}"
                for doc in relevant_docs]
            
            logger.info(f"Remaining tables: {tables}")
            
            return relevant_docs
