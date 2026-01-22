from jinja2 import Template
from langchain_core.documents import Document

SQL_PROMPT_TEMPLATE = """### System:
You are a PostgreSQL expert. Use the provided database schema to write a SQL query.
Output only the SQL query, without explanations.

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