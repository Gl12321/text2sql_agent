from typing import Any
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

CORRECTION_PROMPT = """### System:
You are a PostgreSQL expert. Your previous SQL query failed with an error.
Correct the SQL query based on the error message and the provided context.
Output only the corrected SQL query.

### Context:
{{ context }}

### Failed SQL:
{{ failed_sql }}

### Error Message:
{{ error_msg }}

### Corrected SQL:
"""


class SQLCorrector:
    def __init__(self, llm_wrapper: Any):
        self.llm_wrapper = llm_wrapper
        self.template = CORRECTION_PROMPT

    async def correct(self, context, failed_sql, error_msg, tables, columns):
        prompt = self.template.replace("{{ context }}", context) \
            .replace("{{ failed_sql }}", failed_sql) \
            .replace("{{ error_msg }}", error_msg)

        correction_chain = self.llm_wrapper.get_chain(tables=tables, columns=columns) | StrOutputParser()

        return await correction_chain.ainvoke(prompt)