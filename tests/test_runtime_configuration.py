from pathlib import Path
import json
import sys
import threading
import time
import asyncio
import io
import tarfile

from fastapi.testclient import TestClient

import sandbox_runtime.app as runtime_app
from sandbox_runtime.adapters import HarnessCommand


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


async def test_runtime_finishes_and_replays_events_without_an_http_subscriber(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    emitter = tmp_path / "background.py"
    emitter.write_text("import json,time\ntime.sleep(.05)\nprint(json.dumps({'type':'assistant_delta','text':'background-finished'}),flush=True)\n")
    monkeypatch.setenv("HARNESS_COMMAND_JSON", json.dumps({name: [sys.executable, str(emitter)]
        for name in ("codex", "claude", "deepseek")}))
    app = runtime_app.create_app()
    configure = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/v1/configure")
    run_turn = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/v1/turns")
    replay = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/v1/executions/{execution_id}/events")
    await configure(runtime_app.Configure(capability_token="x" * 32, gateway_url="http://gateway:28600",
        harness="codex", model="model", proxy_url="http://172.31.0.1:28601"))
    response = await run_turn(runtime_app.TurnRequest(message="hello", execution_id="background-one"))
    for _ in range(100):
        execution = app.state.execution
        if execution and execution.result is not None:
            break
        await asyncio.sleep(.01)
    assert app.state.execution.result["status"] == "completed"
    replayed = await replay("background-one", after=0)
    rows = []
    async for chunk in replayed.body_iterator:
        text = chunk.decode() if isinstance(chunk, bytes) else chunk
        rows.extend(json.loads(line) for line in text.splitlines() if line)
    assert any(row.get("payload", {}).get("text") == "background-finished" for row in rows)
    assert rows[-1]["type"] == "result"
    await response.body_iterator.aclose()


def test_runtime_preserves_native_harness_failure_status(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "failed.py"
    runner.write_text("""import json,sys
for line in sys.stdin:
    print(json.dumps({'type':'result','is_error':True,'subtype':'error_during_execution','result':'native failure'}),flush=True)
""")
    monkeypatch.setenv("HARNESS_COMMAND_JSON", json.dumps({name: [sys.executable, str(runner)]
        for name in ("codex", "claude", "deepseek")}))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"claude","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        response = client.post("/v1/turns", json={"message":"first","reasoning_effort":"high"})
    rows = [json.loads(line) for line in response.text.splitlines()]
    assert rows[-1]["result"]["status"] == "failed"
    assert rows[-1]["result"]["error_code"] == "harness_failed"
    assert rows[-1]["result"]["error_detail"] == "native failure"


def test_runtime_materializes_only_managed_project_file_namespaces(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        response = client.put("/v1/project-files/file", params={"path":"assets/reference.txt"},
            content=b"persistent")
        assert response.status_code == 200
        assert (tmp_path / "workspace/project/assets/reference.txt").read_bytes() == b"persistent"
        for path in ("../escape", "state/private", "artifacts/../../escape"):
            assert client.put("/v1/project-files/file", params={"path":path}, content=b"x").status_code == 400
        assert client.post("/v1/project-files/reset").status_code == 200
        assert not (tmp_path / "workspace/project/assets/reference.txt").exists()


def test_checkpoint_excludes_object_backed_managed_project_files(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    app = runtime_app.create_app()
    project = tmp_path / "workspace/project"
    (project / "src").mkdir(parents=True); (project / "src/app.py").write_text("print('kept')")
    (project / "assets").mkdir(); (project / "assets/large.bin").write_bytes(b"asset")
    (project / "artifacts").mkdir(); (project / "artifacts/result.bin").write_bytes(b"artifact")
    with TestClient(app) as client:
        response = client.get("/v1/checkpoint")
    assert response.status_code == 200
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as archive:
        names = set(archive.getnames())
    assert "project/src/app.py" in names
    assert not any(name.startswith("project/assets") or name.startswith("project/artifacts") for name in names)


def test_runtime_delivers_native_interaction_response_to_control_harness(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "interaction.py"
    runner.write_text("""import json,sys
first=json.loads(sys.stdin.readline())
print(json.dumps({'type':'harness.interaction.requested','id':'native-approval','interaction_kind':'approval','prompt':'Allow command?'}),flush=True)
response=json.loads(sys.stdin.readline())
print(json.dumps({'type':'assistant_delta','text':response['response']['action']}),flush=True)
print(json.dumps({'type':'harness.input.completed','input_ids':[first['id']],'thread_id':'native'}),flush=True)
""")
    monkeypatch.setattr(runtime_app, "command", lambda *args, **kwargs: HarnessCommand(
        [sys.executable, str(runner)], {}, streaming_input=True, control_input=True, batched_completion=True))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"codex","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        result = {}
        worker = threading.Thread(target=lambda: result.setdefault("response", client.post("/v1/turns",
            json={"message":"first","execution_id":"interaction-execution","input_id":"input-one"})))
        worker.start()
        for _ in range(100):
            execution = app.state.execution
            if execution and any(row.get("kind") == "waiting_user" for row in execution.events):
                break
            time.sleep(.01)
        response = client.post("/v1/interactions/native-approval", json={"action":"approve","answers":{}})
        worker.join(timeout=5)
    assert response.status_code == 200
    rows = [json.loads(line) for line in result["response"].text.splitlines()]
    assert any(row.get("kind") == "waiting_user" for row in rows)
    assert any(row.get("payload", {}).get("text") == "approve" for row in rows)
    assert rows[-1]["result"]["status"] == "completed"


def test_runtime_waits_for_supervised_background_job_and_captures_status(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "background_harness.py"
    runner.write_text("import json,sys,time\nvalue=json.loads(sys.stdin.readline());time.sleep(.2)\nprint(json.dumps({'type':'harness.input.completed','input_ids':[value['id']],'thread_id':'native'}),flush=True)\n")
    worker = tmp_path / "background_worker.py"
    worker.write_text("import pathlib,time\ntime.sleep(.35)\npathlib.Path('" + str(tmp_path / "workspace/outputs/background.json") + "').write_text('{\"done\":true}')\nprint('BACKGROUND_DONE',flush=True)\n")
    monkeypatch.setattr(runtime_app, "command", lambda *args, **kwargs: HarnessCommand(
        [sys.executable, str(runner)], {}, streaming_input=True, control_input=True, batched_completion=True))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"codex","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        result = {}; started = time.monotonic()
        turn_worker = threading.Thread(target=lambda: result.setdefault("response", client.post("/v1/turns",
            json={"message":"first","execution_id":"background-execution","input_id":"input-one"})))
        turn_worker.start()
        for _ in range(100):
            if app.state.execution:
                break
            time.sleep(.01)
        response = client.post("/v1/background-jobs", json={"name":"render",
            "argv":[sys.executable,str(worker)], "cwd":str(tmp_path / "workspace/project")})
        assert response.status_code == 200
        turn_worker.join(timeout=5)
        jobs = client.get("/v1/background-jobs").json()["items"]
    assert time.monotonic() - started >= .3
    assert jobs[0]["status"] == "completed" and "BACKGROUND_DONE" in jobs[0]["log_tail"]
    rows = [json.loads(line) for line in result["response"].text.splitlines()]
    assert any(row.get("payload", {}).get("background_job_id") for row in rows)
    assert rows[-1]["result"]["status"] == "completed"


def test_runtime_generic_interaction_blocks_any_harness_until_user_response(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "generic_interaction_harness.py"
    runner.write_text("import json,sys,time\nvalue=json.loads(sys.stdin.readline());time.sleep(.8)\nprint(json.dumps({'type':'harness.input.completed','input_ids':[value['id']],'thread_id':'native'}),flush=True)\n")
    monkeypatch.setattr(runtime_app, "command", lambda *args, **kwargs: HarnessCommand(
        [sys.executable, str(runner)], {}, streaming_input=True, control_input=True, batched_completion=True))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"deepseek","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        turn_result = {}; interaction_result = {}
        turn_worker = threading.Thread(target=lambda: turn_result.setdefault("response", client.post("/v1/turns",
            json={"message":"first","execution_id":"generic-interaction","input_id":"input-one"})))
        turn_worker.start()
        for _ in range(100):
            if app.state.execution:
                break
            time.sleep(.01)
        interaction_worker = threading.Thread(target=lambda: interaction_result.setdefault("response",
            client.post("/v1/native-interactions", json={"kind":"question","prompt":"Pick",
                "questions":[{"id":"format","question":"Format?"}],"options":[]})))
        interaction_worker.start()
        identity = ""
        for _ in range(100):
            rows = app.state.execution.events
            pending = next((row for row in rows if row.get("kind") == "waiting_user"), None)
            if pending:
                identity = pending["payload"]["interaction_id"]; break
            time.sleep(.01)
        assert identity.startswith("runtime-")
        response = client.post(f"/v1/interactions/{identity}",
            json={"action":"answer","answers":{"format":"mp4"}})
        interaction_worker.join(timeout=5); turn_worker.join(timeout=5)
    assert response.status_code == 200
    assert interaction_result["response"].json()["answers"] == {"format":"mp4"}
    assert turn_result["response"].status_code == 200
