import asyncio
from src.database.schema_parser import SchemaParser
from src.rag.cataloger import SchemaCataloger
from src.rag.retriver import TableRetriever
from src.rag.embedder import TableEmbedder
from src.core.logger import setup_logger

logger = setup_logger("RAG_Test")


async def rag_test():
    parser = SchemaParser()
    cataloger = SchemaCataloger()
    embedder = TableEmbedder()

    logger.info("Check data migration")

    all_schemas = await parser.get_all_schemas()
    if not all_schemas:
        return

    logger.info("Check semantic search")

    test_query = "Show the names of the stations and the number of available bikes at each of them"

    retriever = TableRetriever(
        collection=cataloger.table_collection,
        embedder=embedder,
        schemas_id=all_schemas,
        top_k=5
    )

    search_results = retriever.invoke(test_query)

    if search_results:
        for i, doc in enumerate(search_results):
            logger.info(f"Top-{i + 1} result snippet: \n table: {doc.metadata["table_name"]} \n {doc.page_content}")


if __name__ == "__main__":
    asyncio.run(rag_test())