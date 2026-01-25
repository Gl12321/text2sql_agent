from jinja2 import Template

CORRECTOR_PROMPT_TEMPLATE = """### System:
You are a PostgreSQL expert. Your previous SQL query failed. 
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

    def build_correction_prompt(self, question, retrieved_ddls, error_msg):
        return self.template.render(
            question=question,
            documents=retrieved_ddls,
            error_msg=error_msg
        )