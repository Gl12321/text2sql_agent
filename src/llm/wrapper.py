import os
from huggingface_hub import hf_hub_download
from langchain_community.llms import LlamaCpp
from src.core.logger import setup_logger
from src.llm.grammar import SQLGrammarBuilder
from src.core.config import get_settings
from contextlib import redirect_stdout, redirect_stderr
import os 


os.environ["OMP_NUM_THREADS"] = "5"
os.environ["OPENBLAS_NUM_THREADS"] = "5"
os.environ["MKL_NUM_THREADS"] = "5"
os.environ["VECLIB_MAXIMUM_THREADS"] = "5"
os.environ["NUMEXPR_NUM_THREADS"] = "5"

logger = setup_logger("wrapper")

class LLMWrapper:
    def __init__(self):
        self.model_config = get_settings().MODELS["llm_2"]
        self.model_path = self.model_config["local_path"]
        model_dir = os.path.dirname(self.model_path)

        if not os.path.exists(self.model_path):
            logger.info(f"Downloading model to {self.model_path}")
            hf_hub_download(
                repo_id=self.model_config["repo_id"],
                filename=self.model_config["filename"],
                local_dir=model_dir,
                local_dir_use_symlinks=False
            )

        self.base_params = {
            "model_path": self.model_path,
            "temperature": 0.0,
            "max_tokens": 512,
            "n_ctx": 8192,
            "n_batch": 1024,
            "verbose": False,
            "n_gpu_layers": 0,
            "n_threads": 5,
            "n_threads_batch": 5,
        }

    def get_chain(self, schema_mapping):
        grammar_text = SQLGrammarBuilder.build(schema_mapping)
        # logger.info(f"grammar: {grammar_text}")
        
        with open(os.devnull, 'w') as fnull:
            with redirect_stdout(fnull), redirect_stderr(fnull):
                llm = LlamaCpp(
                    **self.base_params,
                    grammar=grammar_text
                )
                
        return llm


if __name__ == "__main__":
    llm = LLMWrapper()