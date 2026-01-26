from jinja2 import Template

CORRECTOR_PROMPT_TEMPLATE = """### System:
You are a PostgreSQL expert. Your previous SQL query failed. Do not use SELF-JOINs or join a table to itself; if all columns are in one table, use a single SELECT without any JOIN statements.
Correct it using the context. Output only the SQL query, without explanations.

### Original Question: {{ question }}

### Context (DDL):
{% for doc in documents %}
{{ doc }}
{% endfor %}

### Error Message: {{ error_msg }}

<|eot_id|><|start_header_id|>assistant<|end_header_id|>
The corrected SQL query is:
"""

class SQLCorrector:
    def __init__(self):
        self.template = Template(CORRECTOR_PROMPT_TEMPLATE)

    def build_correction_prompt(self, question, ddls_context, error_msg):
        return self.template.render(
            question=question,
            documents=ddls_context,
            error_msg=error_msg
        )
