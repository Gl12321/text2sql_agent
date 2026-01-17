import asyncio
from src.database.schema_parser import SchemaParser
from src.rag.cataloger import SchemaCataloger
from src.rag.retriver import Retriver
from src.rag.embedder import TableEmbedder
from src.core.logger import setup_logger

logger = setup_logger("RAG_Test")

async def rag_test():
    parser = SchemaParser()
    cataloger = SchemaCataloger()
    retriver = Retriver()
    embedder = TableEmbedder()

    logger.info("Check data migration")

    all_schemas = await parser.get_all_schemas()
    if not all_schemas:
        return

    postgres_stats = {}
    for schema in all_schemas:
        tables = await parser.get_ddl_of_schema(schema)

        postgres_stats[schema] = list(tables.keys())

    await cataloger.index_all_schemas()

    for schema, tables_list in postgres_stats.items():
        results = retriver.table_collection.get(where={"db_id": schema})
        actual_count = len(results['ids'])
        # Сравнение через длину сохраненного списка
        expected_tables = len(tables_list)

        if actual_count != expected_tables:
            logger.info(f"Amount tables between Chroma and Postgres don't match in schema {schema}")
        else:
            logger.info(f"All tables match in schema {schema}")

    logger.info("Check semantic search")

    test_db = "college_2"

    logger.info(f"tables of schema {test_db}: {postgres_stats.get(test_db, [])}")
    
    test_query = "how match buildings in department and classrooms"
    query_vector = embedder.get_embeddings([test_query])[0]

    search_results = retriver.search_tables(
        query_embedding=query_vector,
        db_id=test_db,
        top_k=2
    )

    if search_results:
        logger.info(f"Search for '{test_db}' returned {len(search_results)} documents")
        for i, doc in enumerate(search_results):
            logger.info(f"Top-{i+1} result snippet: {doc[:150]}...")
    else:
        logger.error(f"Search returned NO results for schema '{test_db}'")

if __name__ == "__main__":
    asyncio.run(rag_test())
