import hashlib
import json
from pathlib import Path
import subprocess
import sys
import os
import signal
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient
from fastapi.routing import APIWebSocketRoute

from sandbox_bootstrap import supervisor as bootstrap
from sandbox_bootstrap import main as bootstrap_main


ROOT = Path(__file__).resolve().parents[1]


def test_bootstrap_configures_runtime_console_observability(tmp_path,monkeypatch):
    calls=[]
    monkeypatch.setattr(bootstrap_main,'configure_bootstrap_logging',
        lambda service:calls.append(service))
    monkeypatch.setattr(bootstrap_main.uvicorn,'run',lambda *args,**kwargs:None)

    bootstrap_main.run()

    assert calls==['runtime-bootstrap']


def test_bootstrap_import_does_not_require_runtime_code_bundle():
    code="""
import builtins
original=builtins.__import__
def guarded(name,*args,**kwargs):
    if name == 'sandbox_harness' or name.startswith('sandbox_harness.'):
        raise ModuleNotFoundError(name)
    return original(name,*args,**kwargs)
builtins.__import__=guarded
import sandbox_bootstrap.main
"""
    result=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True)
    assert result.returncode==0,result.stderr


def test_bootstrap_logging_emits_fixed_payload_free_schema():
    import io
    from sandbox_bootstrap.observability import configure_bootstrap_logging
    stream=io.StringIO()

    configure_bootstrap_logging('runtime-bootstrap',stream=stream)

    row=json.loads(stream.getvalue())
    assert row['schema_version']==1 and row['service']=='runtime-bootstrap'
    assert row['event']=='service.started' and row['level']=='INFO'
    assert row['timestamp'].endswith('+00:00')
    assert {key:row[key] for key in ('sandbox_id','thread_id','turn_id','job_id','execution_id')}=={
        'sandbox_id':'','thread_id':'','turn_id':'','job_id':'','execution_id':''}


def test_bootstrap_emits_ready_once_after_health_is_observable(tmp_path,monkeypatch):
    monkeypatch.setenv("SANDBOX_RUNTIME_STATE_DIR",str(tmp_path/"run"))
    calls=[]
    monkeypatch.setattr(bootstrap,"bootstrap_event",
        lambda event,**fields:calls.append((event,fields)))
    app=bootstrap.create_app()

    with TestClient(app) as client:
        assert client.get("/health").status_code==200
        assert client.get("/health").status_code==200

    assert calls==[("service.ready",{"service":"runtime-bootstrap"})]


async def test_bootstrap_relays_runtime_websocket_to_child(monkeypatch):
    app=bootstrap.create_app()
    async def healthy():return {"status":"ok"}
    monkeypatch.setattr(app.state.supervisor,"child_health",healthy)
    calls=[]
    class Upstream:
        sent=[]
        def __aiter__(self):return self
        async def __anext__(self):
            if getattr(self,"done",False):raise StopAsyncIteration
            self.done=True;return b"runtime-output"
        async def send(self,value):self.sent.append(value)
    upstream=Upstream()
    class Connection:
        async def __aenter__(self):return upstream
        async def __aexit__(self,*args):return False
    def connect(url,**kwargs):calls.append((url,kwargs));return Connection()
    monkeypatch.setattr(bootstrap,"websocket_connect",connect)
    class Browser:
        headers={"x-runtime-auth":"token","connection":"upgrade",
            "sec-websocket-key":"secret"}
        query_params="since=7"
        accepted=False;outputs=[]
        async def accept(self):self.accepted=True
        async def receive(self):return {"type":"websocket.disconnect"}
        async def send_bytes(self,value):self.outputs.append(value)
        async def send_text(self,value):self.outputs.append(value)
        async def close(self,code):self.close_code=code
    browser=Browser()
    route=next(item for item in app.routes
        if isinstance(item,APIWebSocketRoute) and item.path=="/{path:path}")

    await route.endpoint(browser,"v1/pty/session/ws")

    assert browser.accepted and browser.outputs==[b"runtime-output"]
    assert calls[0][0]=="ws://127.0.0.1:18080/v1/pty/session/ws?since=7"
    assert calls[0][1]["additional_headers"]=={"x-runtime-auth":"token"}
    assert calls[0][1]["proxy"] is None


def test_runtime_state_dir_defaults_to_private_current_uid_temp_directory(tmp_path, monkeypatch):
    temporary = tmp_path / "tmp"
    temporary.mkdir()
    monkeypatch.delenv("SANDBOX_RUNTIME_STATE_DIR", raising=False)
    monkeypatch.setenv("TMPDIR", str(temporary))

    state = bootstrap.runtime_state_dir()

    assert state == temporary / f"sandbox-harness-runtime-{os.geteuid()}"
    assert state.is_dir()
    assert state.stat().st_mode & 0o777 == 0o700


def test_runtime_state_dir_honors_explicit_writable_directory(tmp_path, monkeypatch):
    configured = tmp_path / "configured-runtime-state"
    monkeypatch.setenv("SANDBOX_RUNTIME_STATE_DIR", str(configured))

    state = bootstrap.runtime_state_dir()

    assert state == configured
    assert state.is_dir()
    assert state.stat().st_mode & 0o777 == 0o700


def test_runtime_cli_dispatch_uses_configured_state_directory(tmp_path):
    state = tmp_path / "state"
    command = state / "code-releases/current/bin/sandbox-media"
    command.parent.mkdir(parents=True)
    command.write_text("#!/bin/sh\necho configured-runtime-state\n")
    command.chmod(0o755)
    launcher = tmp_path / "sandbox-media"
    launcher.symlink_to(ROOT / "runtime/runtime_cli_dispatch.sh")

    result = subprocess.run(["/bin/sh", str(launcher)], capture_output=True, text=True,
        env={**os.environ, "SANDBOX_RUNTIME_STATE_DIR": str(state)})

    assert result.returncode == 0
    assert result.stdout.strip() == "configured-runtime-state"


def build(tmp_path: Path) -> tuple[bytes, bytes, str, Path]:
    key = Ed25519PrivateKey.generate(); private = tmp_path / "private.pem"; public = tmp_path / "public.pem"
    tmp_path.mkdir(parents=True, exist_ok=True)
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    public.write_bytes(key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo))
    output = subprocess.check_output([sys.executable, str(ROOT / "runtime/build_runtime_code_bundle.py"),
        "--output-dir", str(tmp_path / "bundles"), "--signing-key", str(private)], text=True)
    value = json.loads(output)
    return Path(value["path"]).read_bytes(), Path(value["signature_path"]).read_bytes(), value["sha256"], public


def test_code_bundle_is_signed_content_addressed_and_contains_cli_modules(tmp_path, monkeypatch):
    data, signature, digest, public = build(tmp_path)
    monkeypatch.setenv("SANDBOX_RUNTIME_STATE_DIR", str(tmp_path / "run"))
    monkeypatch.setattr(bootstrap, "PUBLIC_KEY", public)
    monkeypatch.setattr(bootstrap, "DEPENDENCY_LOCK", ROOT / "runtime/requirements-runtime.txt")
    bootstrap.verify_signature(data, signature)
    value = bootstrap.activate_code_bundle(data, digest)
    root = Path(value["root"])
    assert digest == hashlib.sha256(data).hexdigest()
    assert (root / "site-packages/sandbox_runtime/app.py").is_file()
    assert (root / "site-packages/sandbox_harness/common/observability/schema.py").is_file()
    assert (root / "site-packages/sandbox_harness/common/observability/diagnostics.py").is_file()
    assert (root / "site-packages/editppt/cli.py").is_file()
    assert (root / "bin/sandbox-media").stat().st_mode & 0o111
    assert (root / "bin/sandbox-job").stat().st_mode & 0o111
    assert (root / "bin/sandbox-artifact").stat().st_mode & 0o111
    assert (root / "bin/sandbox-review").stat().st_mode & 0o111
    assert (tmp_path / "run/code-releases/current").resolve() == root


def test_bootstrap_rejects_bad_signature_then_starts_and_proxies_runtime(tmp_path, monkeypatch):
    data, signature, digest, public = build(tmp_path)
    monkeypatch.setenv("SANDBOX_RUNTIME_STATE_DIR", str(tmp_path / "run"))
    monkeypatch.setattr(bootstrap, "PUBLIC_KEY", public)
    monkeypatch.setattr(bootstrap, "DEPENDENCY_LOCK", ROOT / "runtime/requirements-runtime.txt")
    app = bootstrap.create_app()
    with TestClient(app) as client:
        rejected = client.put("/bootstrap/code-bundle", params={"sha256": digest}, content=data,
            headers={"X-Runtime-Bundle-Signature": "00"})
        installed = client.put("/bootstrap/code-bundle", params={"sha256": digest}, content=data,
            headers={"X-Runtime-Bundle-Signature": signature.hex()})
        health = client.get("/health")
        child_status = client.get("/v1/runtime-bundle")
        first_pid = app.state.supervisor.process.pid
        os.killpg(first_pid, signal.SIGKILL)
        for _ in range(40):
            time.sleep(.05)
            if (app.state.supervisor.process and app.state.supervisor.process.pid != first_pid
                    and client.get("/health").json().get("status") == "ok"):
                break
        restarted_pid = app.state.supervisor.process.pid
    assert rejected.status_code == 400
    assert installed.status_code == 200 and installed.json()["sha256"] == digest
    assert health.status_code == 200 and health.json()["status"] == "ok"
    assert health.json()["runtime_code_bundle_digest"] == digest
    assert child_status.status_code == 200 and child_status.json()["sha256"] == ""
    assert restarted_pid != first_pid
