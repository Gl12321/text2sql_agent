import asyncio
from src.database.schema_parser import SchemaParser
from src.rag.serializer import TableSerializer


async def test_flow():
    parser = SchemaParser()
    serializer = TableSerializer()
    schemas = await parser.get_all_schemas()

    target_schema = schemas[1]
    ddl_of_schema = await parser.get_ddl_of_schema(target_schema)

    data = []
    for table_name, table_meta in ddl_of_schema.items():
        table_meta["schema"] = target_schema

        data.append(serializer(table_meta))

    return data

if __name__ == "__main__":
    print(asyncio.run(test_flow()))