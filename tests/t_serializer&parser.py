import asyncio
from src.database.schema_parcer import SchemaParser
from src.rag.serializer import TableSerializer


async def test_flow():
    parser = SchemaParser()
    serializer = TableSerializer()
    schemas = await parser.get_all_schemas()

    ddl_of_schema = await parser.get_ddl_of_schema(schemas[1])

    data = []
    for table_name in ddl_of_schema.keys():
        data.append(serializer(ddl_of_schema[table_name]))

    return data


if __name__ == "__main__":
    data = asyncio.run(test_flow())
    print(data)