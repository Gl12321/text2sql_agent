from typing import Any
from src.llm.wrapper import LLMWrapper
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from langchain_core.output_parsers import StrOutputParser
from langchain_core.documents import Document


def _parse_retrieval_output(docs: list[Document]):
    tables = [d.metadata.get("table_name") for d in docs]

    nested_cols = [d.metadata.get("column_names", "").split(",") for d in docs]
    columns = list(set(col for sublist in nested_cols for col in sublist if col))

    return {
        "docs": docs,
        "tables": tables,
        "columns": columns
    }


def create_sql_chain(llm_wrapper, retriever, prompt_manager):
    retrieval_chain = (
            retriever
            | RunnableLambda(_parse_retrieval_output)
    )

    llm_wrapper = LLMWrapper()
    full_chain = (
            {
                "data": retrieval_chain,
                "question": RunnablePassthrough()
            }
            | RunnableLambda(lambda x: {
        "prompt": prompt_manager.build_sql_prompt(x["question"], x["data"]["docs"]),
        "tables": x["data"]["tables"],
        "columns": x["data"]["columns"]
    })
            | RunnableLambda(lambda x: llm_wrapper.get_chain(
        tables=x["tables"],
        columns=x["columns"]
    ).invoke(x["prompt"]))
            | StrOutputParser()
    )

    return full_chain