from typing import Any
from langchain_core.output_parsers import StrOutputParser
from src.core.logger import setup_logger


class SQLCorrector:
    def __init__(self):
        self.template = """### System:
You are a PostgreSQL expert. Your previous SQL query failed.
Correct it using the context. Output only SQL.

### Context:
{context}

### Failed SQL:
{failed_sql}

### Error Message:
{error_msg} 

### SQL:"""

    async def correct(self, context, failed_sql, error_msg):
        prompt = self.template.format(
            context=context,
            failed_sql=failed_sql,
            error_msg=error_msg
        )

        return prompt
