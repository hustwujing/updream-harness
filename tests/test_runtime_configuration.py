from pathlib import Path
import json
import sys
import threading
import time

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


def test_claude_live_stream_accepts_appended_input_before_first_result(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "stream.py"
    runner.write_text("""import json,sys,time
print(json.dumps({'type':'system','subtype':'init','session_id':'native-live'}),flush=True)
for index,line in enumerate(sys.stdin,1):
    time.sleep(.2)
    print(json.dumps({'type':'stream_event','event':{'type':'content_block_delta','delta':{'type':'text_delta','text':f'reply-{index}'}}}),flush=True)
    print(json.dumps({'type':'result','session_id':'native-live','usage':{}}),flush=True)
""")
    monkeypatch.setenv("HARNESS_COMMAND_JSON", json.dumps({name: [sys.executable, str(runner)]
        for name in ("codex", "claude", "deepseek")}))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"claude","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        result = {}
        worker = threading.Thread(target=lambda: result.setdefault("response", client.post("/v1/turns",
            json={"message":"first","reasoning_effort":"high"})))
        worker.start(); time.sleep(.08)
        appended = client.post("/v1/inputs", json={"message":"second","reasoning_effort":"high"})
        worker.join(timeout=5)
    assert appended.status_code == 200 and appended.json()["mode"] == "native_stream"
    rows = [json.loads(line) for line in result["response"].text.splitlines()]
    assert [row["payload"]["text"] for row in rows if row.get("kind") == "text_delta"] == ["reply-1", "reply-2"]
    assert rows[-1]["type"] == "result" and rows[-1]["result"]["native_thread_id"] == "native-live"
