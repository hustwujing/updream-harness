from pathlib import Path
import json
import sys

from fastapi.testclient import TestClient

import sandbox_runtime.app as runtime_app


def test_runtime_configuration_freezes_turn_harness_and_model(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    app = runtime_app.create_app()

    with TestClient(app) as client:
        response = client.post("/v1/configure", json={
            "capability_token": "x" * 32,
            "gateway_url": "http://gateway:28600",
            "harness": "deepseek",
            "model": "deepseek-v4.1",
            "proxy_url": "http://172.31.0.1:28601",
        })

    assert response.status_code == 200
    assert runtime_app.os.environ["HARNESS_KIND"] == "deepseek"
    assert runtime_app.os.environ["HARNESS_MODEL"] == "deepseek-v4.1"
    assert runtime_app.os.environ["HTTPS_PROXY"].startswith("http://" + "x" * 32 + ":@")
    assert "172.31.0.1" in runtime_app.os.environ["NO_PROXY"]
    assert (tmp_path / "run" / "capability-token").read_text() == "x" * 32


def test_runtime_stream_accepts_bounded_large_native_json_lines(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    payload = json.dumps({"type": "assistant_delta", "text": "x" * 100_000})
    emitter = tmp_path / "emit.py"
    emitter.write_text("print(" + repr(payload) + ")\n")
    monkeypatch.setenv("HARNESS_COMMAND_JSON", json.dumps({
        "codex": [sys.executable, str(emitter)],
        "claude": [sys.executable, str(emitter)],
        "deepseek": [sys.executable, str(emitter)],
    }))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        configured = client.post("/v1/configure", json={"capability_token": "x" * 32,
            "gateway_url": "http://gateway:28600", "harness": "claude", "model": "model",
            "proxy_url": "http://172.31.0.1:28601"})
        response = client.post("/v1/turns", json={"message": "hello", "reasoning_effort": "high"})
    assert configured.status_code == 200 and response.status_code == 200
    rows = [json.loads(line) for line in response.text.splitlines()]
    assert len(rows[0]["payload"]["text"]) == 100_000
    assert rows[-1]["type"] == "result" and rows[-1]["result"]["status"] == "completed"
