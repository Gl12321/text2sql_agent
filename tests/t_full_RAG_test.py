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

    await cataloger.index_all_schemas()
    all_schemas = await parser.get_all_schemas()

    if not all_schemas:
        return

    postgres_stats = {}
    for schema in all_schemas:
        tables = await parser.get_ddl_of_schema(schema)
        postgres_stats[schema] = list(tables.keys())

    for schema, tables_list in postgres_stats.items():
        results = cataloger.table_collection.get(where={"db_id": schema})
        actual_count = len(results['ids'])
        expected_tables = len(tables_list)

        if actual_count != expected_tables:
            logger.info(f"Amount tables between Chroma and Postgres don't match in schema {schema}")
        else:
            logger.info(f"All tables match in schema {schema}")

    logger.info("Check semantic search")

    test_db = "college_2"
    test_query = "how match buildings in department and classrooms"

    retriever = TableRetriever(
        collection=cataloger.table_collection,
        embedder=embedder,
        db_id=test_db,
        top_k=2
    )

    search_results = retriever.invoke(test_query)

    if search_results:
        logger.info(f"Search for '{test_db}' returned {len(search_results)} documents")
        for i, doc in enumerate(search_results):
            logger.info(f"Top-{i + 1} result snippet: {doc.page_content[:150]}...")
    else:
        logger.error(f"Search returned NO results for schema '{test_db}'")


if __name__ == "__main__":
    asyncio.run(rag_test())