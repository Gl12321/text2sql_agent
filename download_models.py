import os
from huggingface_hub import hf_hub_download, snapshot_download
from src.core.config import get_settings


def main():
    settings = get_settings()

    llm_conf = settings.MODELS["llm_2"]
    llm_path = llm_conf["params"]["model_path"]
    if not os.path.exists(llm_path):
        print("LLM: Downloading")
        hf_hub_download(
            repo_id=llm_conf["repo_id"],
            filename=llm_conf["filename"],
            local_dir=os.path.dirname(llm_path),
            local_dir_use_symlinks=False
        )
    else:
        print("LLM already downloaded")

    emb_conf = settings.MODELS["embedder"]
    if not os.path.exists(os.path.join(emb_conf["cache_path"], "config.json")):
        print("Embedder: Downloading")
        snapshot_download(
            repo_id=emb_conf["repo_id"],
            local_dir=emb_conf["cache_path"],
            local_dir_use_symlinks=False
        )
    else:
        print("Embedder already downloaded")

    rer_conf = settings.MODELS["reranker"]
    if not os.path.exists(os.path.join(rer_conf["cache_path"], "config.json")):
        print("Reranker: Downloading")
        snapshot_download(
            repo_id=rer_conf["repo_id"],
            local_dir=rer_conf["cache_path"],
            local_dir_use_symlinks=False
        )
    else:
        print("Reranker already downloaded")


if __name__ == "__main__":
    main()