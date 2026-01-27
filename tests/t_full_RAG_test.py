import asyncio
from src.database.schema_parser import SchemaParser
from src.rag.cataloger import SchemaCataloger
from src.rag.retriver import TableRetriever
from src.rag.reranker import TableReranker
from src.rag.embedder import TableEmbedder
from src.core.logger import setup_logger

logger = setup_logger("RAG_TEST")


async def rag_test():
    parser = SchemaParser()
    embedder = TableEmbedder()
    cataloger = SchemaCataloger()
    reranker = TableReranker()

    all_schemas = await parser.get_all_schemas()
    if not all_schemas:
        return

    await cataloger.index_all_schemas()

    test_query = "Show the names of the stations and the number of available bikes at each of them"
    logger.info(f"Check semantic search. Question: {test_query}")

    retriever = TableRetriever(
        collection=cataloger.table_collection,
        embedder=embedder,
        schemas_id=all_schemas,
        top_k=10
    )

    retriever_results = await retriever.ainvoke(test_query)
    if retriever_results:
        for i, doc in enumerate(retriever_results):
            logger.info(f"Top-{i + 1} result snippet of retriever: \n table: {doc.metadata["table_name"]}"
                        f" \n Serialized_table: \n{doc.metadata['serialized_table']}")

    reranker_results = await reranker.rerank(test_query, retriever_results)
    if reranker_results:
        for i, doc in enumerate(reranker_results):
            logger.info(f"Top-{i + 1} result snippet of retriever: \n table: {doc.metadata["table_name"]}"
                        f" \n Serialized_table: \n{doc.metadata['serialized_table']}")

if __name__ == "__main__":
    asyncio.run(rag_test())