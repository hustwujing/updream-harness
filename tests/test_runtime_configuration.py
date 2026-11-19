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


def test_runtime_guidance_has_network_and_browser_fallbacks():
    assert "do not keep retrying the same site" in runtime_app.RUNTIME_GUIDANCE
    assert "sandbox-browser-screenshot URL OUTPUT" in runtime_app.RUNTIME_GUIDANCE
    assert "chromium" in runtime_app.RUNTIME_GUIDANCE and "Playwright scripts" in runtime_app.RUNTIME_GUIDANCE
    assert "ImageMagick (`convert` and `identify`)" in runtime_app.RUNTIME_GUIDANCE
    assert "current Turn's publication directory" in runtime_app.RUNTIME_GUIDANCE
    assert "--replaces FAILED_JOB_ID" in runtime_app.RUNTIME_GUIDANCE
    assert "Build and patch files under `/workspace/project/work`" in runtime_app.RUNTIME_GUIDANCE
    assert "do not use an absolute `/workspace/outputs` path" in runtime_app.RUNTIME_GUIDANCE


def test_background_failure_requires_successful_explicit_replacement():
    failed = runtime_app.BackgroundJob(id="a" * 32, name="render", argv=["false"], cwd="/workspace/project",
        execution_id="execution", status="failed", returncode=1)
    unrelated = runtime_app.BackgroundJob(id="b" * 32, name="render-2", argv=["true"], cwd="/workspace/project",
        execution_id="execution", status="completed", returncode=0)
    replacement = runtime_app.BackgroundJob(id="c" * 32, name="render-3", argv=["true"], cwd="/workspace/project",
        execution_id="execution", replaces=failed.id, status="completed", returncode=0)
    assert runtime_app.unresolved_background_jobs([failed, unrelated]) == [failed]
    assert runtime_app.unresolved_background_jobs([failed, unrelated, replacement]) == []


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
    assert runtime_app.os.environ["HOME"] == str(tmp_path / "workspace" / "state" / "home")
    assert runtime_app.os.environ["npm_config_registry"] == "https://registry.npmmirror.com"
    assert runtime_app.os.environ["HTTPS_PROXY"].startswith("http://" + "x" * 32 + ":@")
    assert "172.31.0.1" in runtime_app.os.environ["NO_PROXY"]
    assert (tmp_path / "run" / "capability-token").read_text() == "x" * 32


def test_harness_commands_preload_the_playwright_proxy_bridge(monkeypatch, tmp_path):
    token=tmp_path/'token';token.write_text('x'*32)
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway:28600')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    for harness in ('codex','claude','deepseek'):
        configured=__import__('sandbox_runtime.adapters',fromlist=['command']).command(harness,'model')
        assert '--require=/usr/local/lib/sandbox-playwright-proxy.cjs' in configured.environment['NODE_OPTIONS']
        if harness=='claude':assert configured.environment['CLAUDE_CONFIG_DIR']=='/workspace/state/claude'


def test_runtime_stream_accepts_bounded_large_native_json_lines(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    payload = json.dumps({"type": "assistant_delta", "text": "x" * 100_000})
    emitter = tmp_path / "emit.py"
    emitter.write_text("print(" + repr(payload) + ")\nprint('{\"type\":\"result\"}')\n")
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
        appended = client.post("/v1/inputs", json={"message":"second","reasoning_effort":"high",
            "execution_id":"legacy", "input_id":"append-one"})
        replayed = client.post("/v1/inputs", json={"message":"second","reasoning_effort":"high",
            "execution_id":"legacy", "input_id":"append-one"})
        worker.join(timeout=5)
    assert appended.status_code == 200 and appended.json()["mode"] == "native_stream"
    assert replayed.status_code == 200 and replayed.json()["replayed"] is True
    rows = [json.loads(line) for line in result["response"].text.splitlines()]
    assert [row["payload"]["text"] for row in rows if row.get("kind") == "text_delta"] == ["reply-1", "reply-2"]
    assert rows[-1]["type"] == "result" and rows[-1]["result"]["native_thread_id"] == "native-live"


def test_stream_snapshot_deduplication_resets_for_each_appended_input(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "snapshots.py"
    runner.write_text("""import json,sys,time
print(json.dumps({'type':'thread.started','thread_id':'native-live'}),flush=True)
for index,line in enumerate(sys.stdin,1):
    time.sleep(.2)
    command=json.loads(line)
    if index == 1:
        print(json.dumps({'type':'assistant_delta','text':'streamed-1'}),flush=True)
        print(json.dumps({'type':'assistant_delta','text':'streamed-1','snapshot':True}),flush=True)
    else:
        print(json.dumps({'type':'assistant_delta','text':'fallback-2','snapshot':True}),flush=True)
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
        appended = client.post("/v1/inputs", json={"message":"second","reasoning_effort":"high",
            "execution_id":"legacy", "input_id":"append-two"})
        worker.join(timeout=5)
    assert appended.status_code == 200
    rows = [json.loads(line) for line in result["response"].text.splitlines()]
    assert [row["payload"]["text"] for row in rows if row.get("kind") == "text_delta"] == [
        "streamed-1", "fallback-2",
    ]


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


def test_new_execution_clears_previous_turn_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "output.py"
    runner.write_text("""import json,pathlib,sys
state=pathlib.Path('""" + str(tmp_path / "workspace/state/count") + """')
number=int(state.read_text())+1 if state.exists() else 1
state.parent.mkdir(parents=True,exist_ok=True);state.write_text(str(number))
out=pathlib.Path('""" + str(tmp_path / "workspace/outputs") + """');out.mkdir(parents=True,exist_ok=True)
(out/f'artifact-{number}.txt').write_text(str(number))
sys.stdin.read();print(json.dumps({'type':'result','result':'done'}),flush=True)
""")
    monkeypatch.setenv("HARNESS_COMMAND_JSON", json.dumps({name: [sys.executable, str(runner)]
        for name in ("codex", "claude", "deepseek")}))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"codex","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        first = client.post("/v1/turns", json={"message":"first","execution_id":"first"})
        assert json.loads(first.text.splitlines()[-1])["result"]["status"] == "completed"
        assert [item["path"] for item in client.get("/v1/artifacts").json()["items"]] == ["artifact-1.txt"]
        second = client.post("/v1/turns", json={"message":"second","execution_id":"second"})
        assert json.loads(second.text.splitlines()[-1])["result"]["status"] == "completed"
        assert [item["path"] for item in client.get("/v1/artifacts").json()["items"]] == ["artifact-2.txt"]


def test_runtime_promotes_only_current_execution_project_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    legacy = tmp_path / "workspace/project/outputs"
    legacy.mkdir(parents=True); (legacy / "older.txt").write_text("older")
    runner = tmp_path / "legacy-output.py"
    runner.write_text("""import json,pathlib,sys
state=pathlib.Path('""" + str(tmp_path / "workspace/state/legacy-count") + """')
number=int(state.read_text())+1 if state.exists() else 1
state.parent.mkdir(parents=True,exist_ok=True);state.write_text(str(number))
root=pathlib.Path('""" + str(legacy) + """');root.mkdir(parents=True,exist_ok=True)
(root/f'current-{number}.txt').write_text(str(number))
sys.stdin.read();print(json.dumps({'type':'assistant_delta','text':'done'}),flush=True)
""")
    monkeypatch.setenv("HARNESS_COMMAND_JSON", json.dumps({name: [sys.executable, str(runner)]
        for name in ("codex", "claude", "deepseek")}))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"codex","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        first = client.post("/v1/turns", json={"message":"first","execution_id":"legacy-first"})
        assert json.loads(first.text.splitlines()[-1])["result"]["status"] == "completed"
        first_outputs = client.get("/v1/artifacts").json()
        assert first_outputs["batch_id"] == "legacy-first"
        assert [item["path"] for item in first_outputs["items"]] == ["current-1.txt"]
        second = client.post("/v1/turns", json={"message":"second","execution_id":"legacy-second"})
        assert json.loads(second.text.splitlines()[-1])["result"]["status"] == "completed"
        second_outputs = client.get("/v1/artifacts").json()
        assert second_outputs["batch_id"] == "legacy-second"
        assert [item["path"] for item in second_outputs["items"]] == ["current-2.txt"]


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
        manifest = client.get("/v1/project-files/manifest").json()
        assert manifest["truncated"] is False
        assert manifest["items"] == [{"path":"assets/reference.txt","size":10,
            "sha256":__import__('hashlib').sha256(b"persistent").hexdigest(),"mime":"text/plain"}]
        assert client.get("/v1/project-files/file",params={"path":"assets/reference.txt"}).content==b"persistent"
        preserved=client.post("/v1/project-files/preserve",json={
            "paths":["assets/reference.txt"],"destination":"work/recovered-managed/test"}).json()
        assert preserved["items"][0]["path"]=="work/recovered-managed/test/assets/reference.txt"
        assert (tmp_path/"workspace/project/work/recovered-managed/test/assets/reference.txt").read_bytes()==b"persistent"
        for path in ("../escape", "state/private", "artifacts/../../escape"):
            assert client.put("/v1/project-files/file", params={"path":path}, content=b"x").status_code == 400
        assert client.post("/v1/project-files/reset").status_code == 200
        assert not (tmp_path / "workspace/project/assets/reference.txt").exists()


def test_runtime_rejects_dotdot_attachment_name(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    with TestClient(runtime_app.create_app()) as client:
        assert client.put("/v1/attachments/file", params={"name":".."}, content=b"x").status_code == 400


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
    (project / "outputs").mkdir(); (project / "outputs/final.mp4").write_bytes(b"video")
    with TestClient(app) as client:
        response = client.get("/v1/checkpoint")
    assert response.status_code == 200
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as archive:
        names = set(archive.getnames())
    assert "project/src/app.py" in names
    assert not any(name.startswith("project/assets") or name.startswith("project/artifacts")
        or name.startswith("project/outputs") for name in names)


def test_checkpoint_excludes_runtime_links_and_restore_ignores_existing_links(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    app = runtime_app.create_app(); state = tmp_path / "workspace/state/codex/tmp"
    state.mkdir(parents=True); (state / "target").write_text("kept"); (state / "runtime-link").symlink_to("/opt/codex")
    for harness,filename in (("claude","transcript.jsonl"),("dsh","compacted-session.jsonl")):
        target=tmp_path/"workspace/state"/harness/filename;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(harness+"-native-state")
    with TestClient(app) as client:
        exported = client.get("/v1/checkpoint")
        assert exported.status_code == 200
        with tarfile.open(fileobj=io.BytesIO(exported.content), mode="r:") as archive:
            assert "state/codex/tmp/target" in archive.getnames()
            assert "state/codex/tmp/runtime-link" not in archive.getnames()
            assert "state/claude/transcript.jsonl" in archive.getnames()
            assert "state/dsh/compacted-session.jsonl" in archive.getnames()

        crafted = io.BytesIO()
        with tarfile.open(fileobj=crafted, mode="w") as archive:
            payload = b"restored"
            member = tarfile.TarInfo("state/restored.txt"); member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))
            link = tarfile.TarInfo("state/ignored-link"); link.type = tarfile.SYMTYPE; link.linkname = "/opt/runtime"
            archive.addfile(link)
        restored = client.put("/v1/checkpoint", content=crafted.getvalue())
    assert restored.status_code == 200
    assert (tmp_path / "workspace/state/restored.txt").read_text() == "restored"
    assert not (tmp_path / "workspace/state/ignored-link").exists()


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
        interaction_id = next(row["payload"]["interaction_id"] for row in execution.events if row.get("kind") == "waiting_user")
        response = client.post(f"/v1/interactions/{interaction_id}", json={"action":"approve","answers":{}})
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
        replay = client.post(f"/v1/interactions/{identity}",
            json={"action":"answer","answers":{"format":"mp4"}})
        conflict = client.post(f"/v1/interactions/{identity}",
            json={"action":"answer","answers":{"format":"webm"}})
        interaction_worker.join(timeout=5); turn_worker.join(timeout=5)
    assert response.status_code == 200
    assert replay.status_code == 200 and replay.json()["replayed"] is True
    assert conflict.status_code == 409
    assert interaction_result["response"].json()["answers"] == {"format":"mp4"}
    assert turn_result["response"].status_code == 200


def test_runtime_fails_when_model_never_emits_a_substantive_event(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "silent_model.py"
    runner.write_text("import json,time\nprint(json.dumps({'type':'system','subtype':'init','session_id':'silent'}),flush=True)\ntime.sleep(5)\n")
    monkeypatch.setattr(runtime_app, "command", lambda *args, **kwargs: HarnessCommand(
        [sys.executable, str(runner)], {}, streaming_input=True, batched_completion=True))
    app = runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"claude","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        app.state.model_start_timeout_seconds = .05
        response = client.post("/v1/turns", json={"message":"first","execution_id":"silent-execution"})
    rows = [json.loads(line) for line in response.text.splitlines()]
    assert rows[-1]["result"]["status"] == "failed"
    assert rows[-1]["result"]["error_code"] == "model_response_timeout"


def test_runtime_stream_emits_transport_heartbeat_during_long_silence(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace" / "outputs")
    monkeypatch.setattr(runtime_app, "PERSISTENT", ("project", "state"))
    runner = tmp_path / "slow_tool.py"
    runner.write_text("import json,time\nprint(json.dumps({'type':'tool_started','name':'slow'}),flush=True)\ntime.sleep(.15)\nprint(json.dumps({'type':'harness.input.completed','input_ids':['initial'],'thread_id':'native'}),flush=True)\n")
    monkeypatch.setattr(runtime_app, "command", lambda *args, **kwargs: HarnessCommand(
        [sys.executable, str(runner)], {}, streaming_input=True, control_input=True, batched_completion=True))
    app = runtime_app.create_app(); app.state.stream_heartbeat_seconds = .03
    with TestClient(app) as client:
        assert client.post("/v1/configure", json={"capability_token":"x"*32,"gateway_url":"http://gateway:28600",
            "harness":"claude","model":"model","proxy_url":"http://172.31.0.1:28601"}).status_code == 200
        response = client.post("/v1/turns", json={"message":"first","execution_id":"heartbeat-execution"})
    rows = [json.loads(line) for line in response.text.splitlines()]
    assert sum(row.get("type") == "heartbeat" for row in rows) >= 2
    assert rows[-1]["type"] == "result" and rows[-1]["result"]["status"] == "completed"


def test_artifact_export_rejects_links_and_non_regular_files(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace'/'outputs')
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    app=runtime_app.create_app();outputs=tmp_path/'workspace'/'outputs'
    (outputs/'real.txt').write_text('safe')
    (outputs/'alias.txt').symlink_to(outputs/'real.txt')
    import os
    os.mkfifo(outputs/'pipe')
    with TestClient(app) as client:
        assert client.get('/v1/artifacts/file',params={'path':'alias.txt'}).status_code==404
        assert client.get('/v1/artifacts/file',params={'path':'pipe'}).status_code==404
        assert [r['path'] for r in client.get('/v1/artifacts').json()['items']]==['real.txt']
