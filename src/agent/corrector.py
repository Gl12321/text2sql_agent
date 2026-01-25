from typing import Any
from langchain_core.output_parsers import StrOutputParser
from src.core.logger import setup_logger


class SQLCorrector:
    def __init__(self):
        self.template = """### System:
You are a PostgreSQL expert. Your previous SQL query failed.
Correct it using the context. Let me remind you that you must display only the information that is written to you in the question. This is incredibly important! When writing a query, you must ensure that the columns you select correspond to their tables. This is very important! Output only SQL.

### Original Question:
{question}

### Context:
{context}

### Error Message (NEVER REPEAT THIS):
{error_msg} 


### SQL:"""

    async def correct(self, context, failed_sql, error_msg, question):
        prompt = self.template.format(
            context=context,
           # failed_sql=failed_sql,
            error_msg=error_msg,
            question=question,
        )

        return prompt
