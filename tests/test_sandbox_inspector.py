from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.app import create_app
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness, ThreadCreate, TurnCreate, TurnState, now
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.sandbox_inspector import SandboxInspector, TerminalTicketCodec
from sandbox_harness.store import Store
from sandbox_runtime import app as runtime_app


@pytest.mark.asyncio
async def test_sandbox_control_is_owner_scoped_and_blocks_active_turn(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "control.db")); await store.initialize()
    try:
        owner = "a" * 64; profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Control"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="run", idempotency_key="sandbox-control-turn"), profile, owner)
        with pytest.raises(RuntimeError, match="active_turn"):
            await store.begin_sandbox_control(thread.id, owner, "sandbox-one", "nonce",
                now() + timedelta(hours=1))
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        control = await store.begin_sandbox_control(thread.id, owner, "sandbox-one", "nonce",
            now() + timedelta(hours=1))
        assert control["state"] == "user_controlled" and control["generation"] == 1
        assert await store.active_sandbox_control_ids() == {"sandbox-one"}
        control = await store.set_sandbox_terminal(thread.id, owner, "sandbox-one", "pty-one")
        assert control["pty_session_id"] == "pty-one"
        grant = await store.create_sandbox_port_grant(thread.id, owner, "sandbox-one", 3000,
            now() + timedelta(minutes=10))
        assert await store.get_sandbox_port_grant(grant["id"], "b" * 64) is None
        assert (await store.get_sandbox_port_grant(grant["id"], owner))["port"] == 3000
        await store.add_sandbox_audit(thread.id, owner, "sandbox-one", "terminal.created")
        assert (await store.list_sandbox_audit(thread.id, owner))[0]["action"] == "terminal.created"
        await store.update_thread_checkpoint_idle(thread.id, owner, "sha256:" + "1" * 64)
        assert (await store.get_thread(thread.id, owner)).checkpoint_ref == "sha256:" + "1" * 64
        from sandbox_harness.store import SandboxControlRow
        async with store.connections.begin() as db:
            row=await db.get(SandboxControlRow,thread.id,with_for_update=True)
            row.expires_at=now()-timedelta(seconds=1)
        assert await store.active_sandbox_control_ids() == {"sandbox-one"}
        assert (await store.expired_sandbox_controls())[0]["sandbox_id"] == "sandbox-one"
        await store.finish_sandbox_control(thread.id, owner)
        assert await store.get_sandbox_control(thread.id, owner) is None
        assert await store.get_sandbox_port_grant(grant["id"], owner) is None
    finally:
        await store.close()


class FakeObjects:
    def __init__(self): self.values = {}
    def put(self, data):
        digest = __import__('hashlib').sha256(data).hexdigest(); value = "sha256:" + digest
        self.values[value] = data; return value, digest
    def get(self, value, *, maximum): return self.values[value]


class FakeRuntime:
    def __init__(self): self.files = {"project/readme.txt": "hello"}; self.injected = 0; self.fail_checkpoint=False
    async def restore(self, sandbox, archive): self.restored = archive
    async def reset_project_files(self, sandbox): return None
    async def put_project_file(self, sandbox, path, data, mime): return None
    async def inject(self, sandbox, **kwargs): self.injected += 1
    async def inspect_overview(self, sandbox): return {"execution":"idle","cpu_percent":1,
        "cpu_count":4,"memory_used":10,"memory_total":100,"disk_used":20,"disk_total":200,"process_count":3}
    async def inspect_files(self, sandbox, path, depth): return {"root":path,"path":path,"items":[],"truncated":False}
    async def inspect_file(self, sandbox, path): return self.files[path].encode(), "text/plain"
    async def write_inspect_file(self, sandbox, path, data, mime): self.files[path]=data.decode();return {"path":path,"size":len(data)}
    async def inspect_processes(self, sandbox): return {"items":[{"pid":42,"name":"server"}]}
    async def stop_inspect_process(self, sandbox, pid): return {"pid":pid,"stopped":True}
    async def inspect_ports(self, sandbox): return {"items":[{"port":3000,"pid":42,"process":"server"}]}
    async def checkpoint(self, sandbox):
        if self.fail_checkpoint: raise httpx.ConnectError("runtime lost")
        return b"checkpoint"


class FakeBroker:
    def __init__(self): self.active_sandboxes={};self.cancelled=[]
    async def cancel(self, turn_id): self.cancelled.append(turn_id)


@pytest.mark.asyncio
async def test_inspector_restore_takeover_terminal_port_and_release(tmp_path, monkeypatch):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "inspector.db")); await store.initialize()
    backend = FakeSandboxBackend(); pool = SandboxPool(backend,
        {harness:"test@sha256:"+"1"*64 for harness in Harness},
        {harness:0 for harness in Harness}, max_sandboxes=4)
    runtime = FakeRuntime(); objects = FakeObjects(); broker = FakeBroker()
    settings = SimpleNamespace(max_checkpoint_bytes=1024*1024,max_artifact_bytes=1024*1024,
        session_keepalive_seconds=60,sandbox_control_ttl_seconds=3600,
        sandbox_terminal_ticket_seconds=60,sandbox_port_grant_ttl_seconds=3600,
        sandbox_inspect_max_file_bytes=1024*1024,gateway_signing_key="g"*32,
        session_signing_key="s"*32,gateway_url="http://gateway.test",
        public_proxy_url="http://proxy.test",capability_public_request_limit=100,
        gateway_control_key="c"*32)
    inspector = SandboxInspector(settings,store,objects,pool,runtime,broker)
    try:
        owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title="Inspector"),profile,owner)
        sandbox=await inspector.restore(thread.id,owner)
        assert (await inspector.status(thread.id,owner))["state"]=="live"
        control=await inspector.takeover(thread.id,owner,network_access="public")
        assert control["state"]=="user_controlled" and runtime.injected==1
        await inspector.write_file(thread.id,owner,"project/readme.txt",b"changed","text/plain")
        assert (await inspector.file(thread.id,owner,"project/readme.txt"))[0]==b"changed"
        assert (await inspector.processes(thread.id,owner))["items"][0]["pid"]==42
        assert (await inspector.stop_process(thread.id,owner,42))["stopped"]
        grant=await inspector.create_port_grant(thread.id,owner,3000)
        assert grant["port"]==3000
        async def execd(endpoint,headers,method,path,**kwargs):
            request=httpx.Request(method,endpoint+path)
            if method=="POST": return httpx.Response(201,json={"session_id":"pty-one"},request=request)
            return httpx.Response(200,json={"running":True,"output_offset":0},request=request)
        monkeypatch.setattr(inspector,"_execd_request",execd)
        monkeypatch.setattr(inspector,"_revoke",lambda *args,**kwargs: __import__('asyncio').sleep(0))
        monkeypatch.setattr(inspector,"delete_terminal",lambda *args,**kwargs: __import__('asyncio').sleep(0))
        ticket=await inspector.terminal_ticket(thread.id,owner)
        assert inspector.tickets.verify(ticket["ticket"])["pty_session_id"]=="pty-one"
        result=await inspector.release(thread.id,owner)
        assert result=={"released":True,"checkpointed":True}
        assert (await store.get_thread(thread.id,owner)).checkpoint_ref in objects.values
        assert await store.get_sandbox_control(thread.id,owner) is None
        await inspector.takeover(thread.id,owner,network_access="restricted")
        runtime.fail_checkpoint=True
        assert await inspector.release(thread.id,owner)=={"released":True,"checkpointed":False}
        assert await store.get_sandbox_control(thread.id,owner) is None
    finally:
        await pool.close(); await store.close()


def test_terminal_ticket_rejects_tamper_and_expiry(monkeypatch):
    codec=TerminalTicketCodec("secret")
    value=codec.issue(owner_id="a"*64,thread_id="b"*32,sandbox_id="sandbox",
        pty_session_id="pty",ttl_seconds=10)
    assert codec.verify(value)["thread_id"]=="b"*32
    with pytest.raises(ValueError):codec.verify(value+"x")
    monkeypatch.setattr("sandbox_harness.sandbox_inspector.time.time",lambda:10_000)
    with pytest.raises(ValueError,match="expired"):codec.verify(codec.issue(owner_id="a"*64,
        thread_id="b"*32,sandbox_id="sandbox",pty_session_id="pty",ttl_seconds=-1))


def test_nginx_preserves_websocket_upgrade_headers():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    for name in ("nginx.conf","nginx-frpc.conf"):
        value=(root/"deploy"/name).read_text()
        assert "proxy_set_header Upgrade $http_upgrade;" in value
        assert "proxy_set_header Connection $http_connection;" in value


def test_runtime_inspector_file_boundaries(tmp_path, monkeypatch):
    workspace=tmp_path/"workspace";project=workspace/"project";inbox=workspace/"inbox";outputs=workspace/"outputs";run=tmp_path/"run"
    for value in (project,inbox,outputs,workspace/"state",workspace/"tmp",run):value.mkdir(parents=True,exist_ok=True)
    (project/"readme.txt").write_text("hello")
    monkeypatch.setattr(runtime_app,"WORKSPACE",workspace);monkeypatch.setattr(runtime_app,"OUTPUTS",outputs);monkeypatch.setattr(runtime_app,"RUN",run)
    monkeypatch.setattr(runtime_app,"INSPECT_ROOTS",{"project":project,"inbox":inbox,"outputs":outputs})
    app=runtime_app.create_app()
    with TestClient(app) as client:
        overview=client.get("/v1/inspect/overview");assert overview.status_code==200
        files=client.get("/v1/inspect/files",params={"path":"project","depth":2}).json()
        assert files["items"][0]["path"]=="project/readme.txt"
        assert client.get("/v1/inspect/file",params={"path":"project/readme.txt"}).text=="hello"
        assert client.put("/v1/inspect/file",params={"path":"project/readme.txt"},content=b"changed").status_code==200
        assert (project/"readme.txt").read_text()=="changed"
        assert client.put("/v1/inspect/file",params={"path":"inbox/no.txt"},content=b"x").status_code==403
        (project/"escape").symlink_to(tmp_path)
        assert client.get("/v1/inspect/files",params={"path":"project/escape"}).status_code==400


def test_sandbox_api_status_is_thread_owner_scoped(tmp_path):
    settings=Settings(data_root=tmp_path,database_url="sqlite+aiosqlite:///"+str(tmp_path/"api.db"),
        redis_url="",backend="fake",object_backend="local",model_catalog_discovery=False,
        session_signing_key="sandbox-inspector-api-secret")
    with TestClient(create_app(settings)) as first, TestClient(create_app(settings)) as second:
        thread=first.post("/api/threads",json={"harness":"codex","model":"gpt-5.6",
            "reasoning_level":"standard","title":"Inspector owner"}).json()
        value=first.get(f"/api/threads/{thread['id']}/sandbox")
        assert value.status_code==200 and value.json()["state"]=="stopped"
        assert second.get(f"/api/threads/{thread['id']}/sandbox").status_code==404
