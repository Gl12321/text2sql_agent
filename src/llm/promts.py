from jinja2 import Template
from langchain_core.documents import Document

SQL_PROMPT_TEMPLATE = """### System:
You are a PostgreSQL expert. Use the provided database schema to write a SQL query.
Output only the SQL query, without explanations. DO NOT use columns that are not explicitly listed in the schema for a given table. It is incredibly important to display only the data specified in the question, and absolutely nothing extra!!!  When writing a sql query, you must ensure that the columns you select correspond to their tables and schems. This is very important!!!

### Context:
{% for doc in documents %}
{{ doc }}
---
{% endfor %}

### Question:
{{ question }}

### SQL:
"""

class PromptManager:
    def __init__(self):
        self.template = Template(SQL_PROMPT_TEMPLATE)

    def build_sql_prompt(self, question: str, retrieved_ddls: list[str]):
        formatted_docs = [ddl for ddl in retrieved_ddls]

        return self.template.render(
            question=question,
            documents=formatted_docs
        )
