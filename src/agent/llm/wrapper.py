from huggingface_hub import hf_hub_download
from langchain_community.llms import LlamaCpp
from src.core.logger import setup_logger
from src.agent.llm.grammar import SQLGrammarBuilder
from src.core.config import get_settings
from contextlib import redirect_stdout, redirect_stderr
import os 


logger = setup_logger("WRAPPER")

class LLMWrapper:
    def __init__(self):
        self.model_config = get_settings().MODELS["llm_2"]
        self.model_path = self.model_config["params"]["model_path"]
        model_dir = os.path.dirname(self.model_path)

        if not os.path.exists(self.model_path):
            logger.info(f"Downloading model to {self.model_path}")
            hf_hub_download(
                repo_id=self.model_config["repo_id"],
                filename=self.model_config["filename"],
                local_dir=model_dir,
                local_dir_use_symlinks=False
            )

    def get_chain(self, schema_mapping):
        grammar_text = SQLGrammarBuilder.build(schema_mapping)

        with open(os.devnull, 'w') as fnull:
            with redirect_stdout(fnull), redirect_stderr(fnull):
                llm = LlamaCpp(
                    **self.model_config["params"],
                    grammar=grammar_text
                )
                
        return llm


if __name__ == "__main__":
    llm = LLMWrapper()