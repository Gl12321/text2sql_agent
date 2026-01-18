from jinja2 import Template
from typing import List
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
{{ query }}

### SQL:
"""

class PromptManager:
    def __init__(self):
        self.template = Template(SQL_PROMPT_TEMPLATE)

    def build_sql_prompt(self, query: str, retrieved_docs: List[Document]):
        formatted_docs = [doc.page_content for doc in retrieved_docs]

        return self.template.render(
            query=query,
            documents=formatted_docs
        )