import os
from huggingface_hub import hf_hub_download
from langchain_community.llms import LlamaCpp
from src.core.logger import setup_logger
from src.llm.grammar import SQLGrammarBuilder
from src.core.config import get_settings

logger = setup_logger("wrapper")

class LLMWrapper:
    def __init__(self):
        self.model_path = get_settings().MODEL_PATH
        model_dir = os.path.dirname(self.model_path)
        model_filename = os.path.basename(self.model_path)

        if not os.path.exists(self.model_path):
            logger.info(f"Downloading model to {self.model_path}")
            hf_hub_download(
                repo_id="MaziyarPanahi/Meta-Llama-3-8B-Instruct-GGUF",
                filename="Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
                local_dir=model_dir,
                local_dir_use_symlinks=False
            )

        self.base_params = {
            "model_path": self.model_path,
            "temperature": 0.0,
            "max_tokens": 512,
            "n_ctx": 2048,
            "n_batch": 1024,
            "verbose": False,
            "n_gpu_layers": 0,
            "n_threads": 5
        }

    def get_chain(self, tables, columns):
        grammar_text = SQLGrammarBuilder.build(tables, columns)
        
        return LlamaCpp(
            **self.base_params,
            grammar=grammar_text 
        )
