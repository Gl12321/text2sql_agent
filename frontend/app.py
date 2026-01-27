import streamlit as st
import pandas as pd
import requests
import json
import os

st.set_page_config(page_title="SQL Terminal", layout="wide")
API_URL = os.getenv("API_URL", "http://api:8000")

if "full_logs" not in st.session_state:
    st.session_state.full_logs = "> System initialized."
if "last_df" not in st.session_state:
    st.session_state.last_df = None

with st.sidebar:
    st.header("Data Source")
    uploaded_files = st.file_uploader("Upload SQLite DB", type=["sqlite", "db"], accept_multiple_files=True)

    if st.button("Load schemas"):
        if uploaded_files:
            files_payload = [("files", (f.name, f.getvalue())) for f in uploaded_files]
            r = requests.post(f"{API_URL}/load_schema", files=files_payload)
            if r.status_code == 200:
                st.session_state.full_logs += "\n> Files uploaded to host\n"
            else:
                st.session_state.full_logs += "\n> Upload failed\n"
            st.rerun()

    if st.button("Show schemas"):
        r = requests.post(f"{API_URL}/schema_show")
        if r.status_code == 200:
            schemas = "\n".join(r.json().get("schemas", []))
            st.session_state.full_logs += f"[SCHEMAS]: {schemas} "
        st.rerun()

    if st.button("Clear History"):
        st.session_state.full_logs = "> Logs cleared."
        st.session_state.last_df = None
        st.rerun()

    if st.button("Drop all schemas"):
        r = requests.post(f"{API_URL}/drop_all_schemas")
        if r.status_code == 200:
            st.session_state.full_logs += f"\nAll schemas dropped\n"
        st.rerun()

st.subheader("Process Monitor")
log_container = st.container(height=300, border=True)
with log_container:
    log_terminal = st.empty()
    log_terminal.code(st.session_state.full_logs, language="text")

st.subheader("Data Result")
if st.session_state.last_df is not None:
    st.dataframe(st.session_state.last_df, use_container_width=True)

if question := st.chat_input("Write question to DB"):
    st.session_state.full_logs += f"\n> User Query: {question}\n"
    st.session_state.last_df = None

    payload = {"question": question, "schemas_for_search": "all"}
    try:
        with requests.post(f"{API_URL}/question/stream", json=payload, stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if line:
                    chunk = json.loads(line.decode("utf-8"))
                    event = chunk.get("event")
                    content = chunk.get("content")

                    if event == "log":
                        st.session_state.full_logs += f"[TASK] {content}\n"
                    elif event == "result":
                        if content.get('data'):
                            st.session_state.last_df = pd.DataFrame(content['data'])
                        st.session_state.full_logs += f"> Finished with status: {content.get('status')}\n"

                    log_terminal.code(st.session_state.full_logs, language="text")

            st.rerun()

    except Exception as e:
        error_msg = f"[ERROR] {str(e)}"
        st.session_state.full_logs += f"{error_msg}\n"
        log_terminal.code(st.session_state.full_logs, language="text")
        st.error(error_msg)