from jinja2 import Template
from langchain_core.documents import Document

SQL_PROMPT_TEMPLATE = """Generate a SQL query to answer this question: {{ question }}
Output only the SQL query, without explanations. Ensure columns correspond to tables.

DDL statements:
{% for doc in documents %}
{{ doc }}
{% endfor %}<|eot_id|><|start_header_id|>assistant<|end_header_id|>
The following SQL query best answers the question {{ question }}:
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