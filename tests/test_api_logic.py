import pytest
import json
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock
from src.api.main import app


client = TestClient(app)


def test_ask_sql_streaming_logic():
    mock_agent = MagicMock()

    mock_agent.graph.ainvoke = AsyncMock(return_value={
        "status": "success",
        "result_from_db": [{"name": "Mocked Student", "salary": 50000}]
    })

    app.state.pipline = mock_agent
    app.state.schema_manager = AsyncMock()
    app.state.schema_manager.get_all_schemas.return_value = ["test_schema"]

    payload = {
        "question": "Show me students",
        "schemas_for_search": "all"
    }
    response = client.post("/question/stream", json=payload)

    assert response.status_code == 200

    lines = list(response.iter_lines())
    assert len(lines) > 0

    results = [json.loads(line) for line in lines if line]

    final_event = results[-1]
    assert final_event["event"] == "result"
    assert final_event["content"]["status"] == "success"
    assert final_event["content"]["data"][0]["name"] == "Mocked Student"

if "__main__" == __name__:
    test_ask_sql_streaming_logic()