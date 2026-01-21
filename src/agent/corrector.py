from typing import Any
from langchain_core.output_parsers import StrOutputParser
from src.core.logger import setup_logger

logger = setup_logger("corrector")

class SQLCorrector:
    def __init__(self, llm_wrapper: Any):
        self.llm_wrapper = llm_wrapper
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

    async def correct(self, context, failed_sql, error_msg, tables, columns):
        logger.info("Starting SQL self-correction...")
        logger.debug(f"Failed SQL: {failed_sql}")
        logger.debug(f"Error from DB: {error_msg}")

        prompt = self.template.format(
            context=context,
            failed_sql=failed_sql,
            error_msg=error_msg
        )

        chain = self.llm_wrapper.get_chain(tables=tables, columns=columns) | StrOutputParser()
        
        corrected_sql = await chain.ainvoke(prompt)
        
        logger.info("Successfully generated corrected SQL")
        logger.debug(f"New SQL: {corrected_sql}")
        
        return corrected_sql
