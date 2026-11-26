from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.services.api.app import (create_app, sandbox_preview_target)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate, TurnState, now)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
from sandbox_harness.common.execution.sandbox_inspector import (SandboxInspector, TerminalTicketCodec, websocket_endpoint_headers)
from test_support.legacy_store import (SandboxControlRow, Store)
from sqlalchemy import update
from sandbox_harness.common.sandbox.runtime import (StagedRuntimeObject)
from sandbox_runtime import app as runtime_app


def handle(identity):
    return {"provider":"fake","sandbox_id":identity,"allocation_id":"allocation-"+identity,
        "profile_key":"a"*63,"revision":"revision-1"}


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
            await store.begin_sandbox_control(thread.id, owner, "sandbox-one",handle("sandbox-one"),"nonce",
                now() + timedelta(hours=1))
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        control = await store.begin_sandbox_control(thread.id, owner, "sandbox-one",handle("sandbox-one"),"nonce",
            now() + timedelta(hours=1))
        assert control["state"] == "user_controlled" and control["generation"] == 1
        assert await store.active_sandbox_control_ids() == {"sandbox-one"}
        control = await store.set_sandbox_terminal(thread.id, owner, "sandbox-one", "pty-one")
        assert control["pty_session_id"] == "pty-one"
        grant = await store.create_sandbox_port_grant(thread.id, owner, "sandbox-one",
            handle("sandbox-one"),3000,
            now() + timedelta(minutes=10))
        assert await store.get_sandbox_port_grant(grant["id"], "b" * 64) is None
        assert (await store.get_sandbox_port_grant(grant["id"], owner))["port"] == 3000
        await store.add_sandbox_audit(thread.id, owner, "sandbox-one", "terminal.created")
        assert (await store.list_sandbox_audit(thread.id, owner))[0]["action"] == "terminal.created"
        await store.update_thread_checkpoint_idle(thread.id, owner, "sha256:" + "1" * 64)
        assert (await store.get_thread(thread.id, owner)).checkpoint_ref == "sha256:" + "1" * 64
        from test_support.legacy_store import (SandboxControlRow)
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
    def put_file(self,path,*,digest,size):return self.put(path.read_bytes())
    def size(self,value,*,maximum):return len(self.values[value])
    def iter_bytes(self,value,*,maximum,chunk_size):yield self.values[value]


class FakeRuntime:
    def __init__(self):
        self.files = {"project/readme.txt": "hello"}; self.injected = 0; self.fail_checkpoint=False
        self.manifest={"items":[],"truncated":False};self.preserved=[];self.materialized=[];self.legacy_manifest=False
        self.terminals={};self.terminal_calls=[]
    async def restore(self, sandbox, archive, *, size):
        self.restored=b''.join([chunk async for chunk in archive])
    async def reset_project_files(self, sandbox): self.materialized=[]
    async def put_project_file(self, sandbox, path, data, mime): self.materialized.append((path,data,mime))
    async def put_project_file_stream(self,sandbox,path,content,*,size,mime):
        data=b"".join([chunk async for chunk in content]);self.materialized.append((path,data,mime))
        return {"bytes":len(data),"sha256":__import__('hashlib').sha256(data).hexdigest()}
    async def inject(self, sandbox, **kwargs): self.injected += 1
    async def prepare(self, sandbox): pass
    async def inspect_overview(self, sandbox): return {"execution":"idle","cpu_percent":1,
        "cpu_count":4,"memory_used":10,"memory_total":100,"disk_used":20,"disk_total":200,"process_count":3}
    async def inspect_files(self, sandbox, path, depth): return {"root":path,"path":path,"items":[],"truncated":False}
    async def inspect_file(self, sandbox, path): return self.files[path].encode(), "text/plain"
    async def write_inspect_file(self, sandbox, path, data, mime): self.files[path]=data.decode();return {"path":path,"size":len(data)}
    async def inspect_processes(self, sandbox): return {"items":[{"pid":42,"name":"server"}]}
    async def stop_inspect_process(self, sandbox, pid): return {"pid":pid,"stopped":True}
    async def inspect_ports(self, sandbox): return {"items":[{"port":3000,"pid":42,"process":"server"}]}
    async def create_terminal(self,sandbox,*,cwd,command):
        self.terminal_calls.append(("create",sandbox.id,cwd,command))
        self.terminals["pty-one"]={"session_id":"pty-one","running":True}
        return self.terminals["pty-one"]
    async def terminal_status(self,sandbox,session_id):
        self.terminal_calls.append(("status",sandbox.id,session_id))
        return self.terminals.get(session_id)
    async def delete_terminal(self,sandbox,session_id):
        self.terminal_calls.append(("delete",sandbox.id,session_id))
        return self.terminals.pop(session_id,None) is not None
    async def terminal_endpoint(self,sandbox,session_id,*,since):
        self.terminal_calls.append(("endpoint",sandbox.id,session_id,since))
        return f"wss://runtime/v1/pty/{session_id}/ws?since={since}",{"X-Runtime":"yes"}
    async def project_file_manifest(self,sandbox):
        if self.legacy_manifest:
            request=httpx.Request("GET","http://runtime/v1/project-files/manifest")
            response=httpx.Response(404,request=request)
            raise httpx.HTTPStatusError("legacy",request=request,response=response)
        return self.manifest
    async def managed_project_file(self,sandbox,path,*,maximum):
        data=self.files["project/"+path].encode()
        target=__import__('pathlib').Path.cwd()/'.data'/'tmp'/('managed-'+__import__('uuid').uuid4().hex)
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
        return StagedRuntimeObject(target,len(data),__import__('hashlib').sha256(data).hexdigest())
    async def preserve_project_files(self,sandbox,paths,destination):
        self.preserved.append((paths,destination));return {"items":paths,"destination":destination}
    async def checkpoint(self, sandbox, *, maximum):
        if self.fail_checkpoint: raise httpx.ConnectError("runtime lost")
        target=__import__('pathlib').Path.cwd()/'.data'/'tmp'/'inspector-checkpoint'
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b'checkpoint')
        return StagedRuntimeObject(target,10,__import__('hashlib').sha256(b'checkpoint').hexdigest())


class FakeBroker:
    def __init__(self): self.active_sandboxes={};self.cancelled=[]
    async def cancel(self, turn_id): self.cancelled.append(turn_id)


def inspector_settings():
    return SimpleNamespace(max_checkpoint_bytes=1024*1024,max_artifact_bytes=8*1024*1024,
        session_keepalive_seconds=60,sandbox_control_ttl_seconds=120,sandbox_terminal_ticket_seconds=60,
        sandbox_port_grant_ttl_seconds=3600,sandbox_inspect_max_file_bytes=1024*1024,
        gateway_signing_key="g"*32,session_signing_key="s"*32,gateway_url="http://gateway.test",
        public_proxy_url="http://proxy.test",capability_public_request_limit=100,gateway_control_key="c"*32)


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
        with pytest.raises(RuntimeError,match="managed_project_file_read_only"):
            await inspector.write_file(thread.id,owner,"project/assets/input.txt",b"x","text/plain")
        assert (await inspector.processes(thread.id,owner))["items"][0]["pid"]==42
        assert (await inspector.stop_process(thread.id,owner,42))["stopped"]
        grant=await inspector.create_port_grant(thread.id,owner,3000)
        assert grant["port"]==3000
        monkeypatch.setattr(inspector,"_revoke",lambda *args,**kwargs: __import__('asyncio').sleep(0))
        ticket=await inspector.terminal_ticket(thread.id,owner)
        assert inspector.tickets.verify(ticket["ticket"])["pty_session_id"]=="pty-one"
        assert runtime.terminal_calls[0][0]=="create"
        runtime.terminals["pty-one"]["running"]=False
        renewed=await inspector.terminal_ticket(thread.id,owner)
        assert inspector.tickets.verify(renewed["ticket"])["pty_session_id"]=="pty-one"
        assert [call[0] for call in runtime.terminal_calls]==[
            "create","status","delete","create"]
        connections=[]
        ssl_context=object()
        monkeypatch.setattr(httpx,"create_ssl_context",lambda **kwargs:ssl_context)
        def connect(url,**kwargs):
            connections.append((url,kwargs));return "runtime-websocket"
        monkeypatch.setattr("sandbox_harness.common.execution.sandbox_inspector.websocket_connect",connect)
        assert await inspector.terminal_connection(renewed["ticket"],owner,12)=="runtime-websocket"
        assert runtime.terminal_calls[-1]==("endpoint",sandbox.id,"pty-one",12)
        assert connections[0][0]=="wss://runtime/v1/pty/pty-one/ws?since=12"
        assert connections[0][1]["additional_headers"]=={"X-Runtime":"yes"}
        assert connections[0][1]["ssl"] is ssl_context
        result=await inspector.release(thread.id,owner)
        assert result["released"] and result["checkpointed"] and result["managed_changes"]["count"]==0
        assert (await store.get_thread(thread.id,owner)).checkpoint_ref in objects.values
        assert await store.get_sandbox_control(thread.id,owner) is None
        await inspector.takeover(thread.id,owner,network_access="restricted")
        runtime.fail_checkpoint=True;runtime.legacy_manifest=True
        with pytest.raises(RuntimeError,match="checkpoint_retryable"):
            await inspector.release(thread.id,owner)
        assert (await store.get_sandbox_control(thread.id,owner))["release_phase"]=="managed_resolved"
        runtime.fail_checkpoint=False
        recovered=await inspector.release(thread.id,owner)
        assert recovered["released"] and recovered["checkpointed"]
        assert await store.get_sandbox_control(thread.id,owner) is None
    finally:
        await pool.close(); await store.close()


@pytest.mark.asyncio
async def test_inspector_restore_records_durable_sandbox_ownership(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    store = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="inspector-owned")
    await store.initialize()
    backend = FakeSandboxBackend(namespace="inspector-owned")
    pool = SandboxPool(backend, {harness:"test@sha256:"+"1"*64 for harness in Harness},
        {harness:0 for harness in Harness}, max_sandboxes=2, ownership=store.ownership)
    inspector = SandboxInspector(inspector_settings(), store, FakeObjects(), pool, FakeRuntime(), FakeBroker())
    try:
        owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title="Owned Inspector"),profile,owner)
        sandbox=await inspector.restore(thread.id,owner)
        identity=store.ownership.identity(sandbox.handle.provider,sandbox.handle.allocation_id)
        record=await store.ownership.get(identity)
        assert record["state"]=="RETAINED" and record["retained_until"] is not None
        assert record["thread_id"]==thread.id
        await pool.release(sandbox,keep_attached=False)
        assert (await store.ownership.get(identity))["state"]=="RELEASED"
    finally:
        await pool.close();await store.close()


@pytest.mark.asyncio
async def test_inspector_requires_managed_change_decision_and_can_preserve_work(tmp_path,monkeypatch):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"managed.db"));await store.initialize()
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:"test@sha256:"+"1"*64 for h in Harness},
        {h:0 for h in Harness},max_sandboxes=2);runtime=FakeRuntime();objects=FakeObjects();broker=FakeBroker()
    settings=SimpleNamespace(max_checkpoint_bytes=1024*1024,max_artifact_bytes=1024*1024,
        session_keepalive_seconds=60,sandbox_control_ttl_seconds=3600,sandbox_terminal_ticket_seconds=60,
        sandbox_port_grant_ttl_seconds=3600,sandbox_inspect_max_file_bytes=1024*1024,
        gateway_signing_key="g"*32,session_signing_key="s"*32,gateway_url="http://gateway.test",
        public_proxy_url="http://proxy.test",capability_public_request_limit=100,gateway_control_key="c"*32)
    inspector=SandboxInspector(settings,store,objects,pool,runtime,broker)
    monkeypatch.setattr(inspector,"_revoke",lambda *args,**kwargs: __import__('asyncio').sleep(0))
    try:
        owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title="Managed"),profile,owner)
        object_ref,digest=objects.put(b"original")
        await store.register_project_file(thread.id,path="assets/input.txt",source_type="upload",
            source_turn_id="",sha256=digest,size=8,mime="text/plain",object_ref=object_ref)
        await inspector.takeover(thread.id,owner,network_access="restricted")
        runtime.files["project/assets/input.txt"]="changed"
        runtime.manifest={"items":[{"path":"assets/input.txt","sha256":__import__('hashlib').sha256(
            b"changed").hexdigest(),"size":7,"mime":"text/plain"}],"truncated":False}
        changes=await inspector.managed_changes(thread.id,owner)
        assert changes["dirty"] and changes["items"][0]["kind"]=="modified"
        with pytest.raises(RuntimeError,match="managed_files_modified"):
            await inspector.release(thread.id,owner)
        assert (await store.get_sandbox_control(thread.id,owner))["state"]=="user_controlled"
        result=await inspector.release(thread.id,owner,managed_action="preserve_work")
        assert result["managed_changes"]["action"]=="preserve_work"
        assert runtime.preserved[0][0]==["assets/input.txt"]
        assert runtime.materialized[0][1]==b"original"
        await inspector.takeover(thread.id,owner,network_access="restricted")
        runtime.files["project/assets/input.txt"]="official-v2"
        runtime.manifest={"items":[{"path":"assets/input.txt","sha256":__import__('hashlib').sha256(
            b"official-v2").hexdigest(),"size":11,"mime":"text/plain"}],"truncated":False}
        versioned=await inspector.release(thread.id,owner,managed_action="save_versions")
        assert versioned["managed_changes"]["action"]=="save_versions"
        current=(await store.current_project_files(thread.id,owner))[0]
        assert current.version==2 and current.sha256==__import__('hashlib').sha256(b"official-v2").hexdigest()
        assert objects.get(current.object_ref,maximum=100)==b"official-v2"
    finally:
        await pool.close();await store.close()


def test_terminal_ticket_rejects_tamper_and_expiry(monkeypatch):
    codec=TerminalTicketCodec("secret")
    value=codec.issue(owner_id="a"*64,thread_id="b"*32,sandbox_id="sandbox",
        pty_session_id="pty",ttl_seconds=10)
    assert codec.verify(value)["thread_id"]=="b"*32
    with pytest.raises(ValueError):codec.verify(value+"x")
    monkeypatch.setattr("sandbox_harness.common.execution.sandbox_inspector.time.time",lambda:10_000)
    with pytest.raises(ValueError,match="expired"):codec.verify(codec.issue(owner_id="a"*64,
        thread_id="b"*32,sandbox_id="sandbox",pty_session_id="pty",ttl_seconds=-1))


def test_websocket_endpoint_headers_preserve_auth_without_handshake_conflicts():
    value=websocket_endpoint_headers({"OPEN-SANDBOX-API-KEY":"secret","Accept":"*/*",
        "Connection":"keep-alive","Upgrade":"websocket","Host":"proxy","Sec-WebSocket-Key":"bad"})
    assert value=={"OPEN-SANDBOX-API-KEY":"secret","Accept":"*/*"}


def test_sandbox_preview_target_keeps_server_proxy_prefix_and_query():
    assert sandbox_preview_target("http://proxy/sandboxes/s1/ports/3000", "/assets/app.js", "v=1")==(
        "http://proxy/sandboxes/s1/ports/3000/assets/app.js?v=1")


def test_nginx_preserves_websocket_upgrade_headers():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    for name in ("nginx-ali.conf","nginx-frpc-ali-shared-location.conf"):
        value=(root/"deploy"/name).read_text()
        assert "proxy_set_header Upgrade $http_upgrade;" in value
        assert "proxy_set_header Connection $http_connection;" in value


def test_runtime_inspector_file_boundaries(tmp_path, monkeypatch):
    workspace=tmp_path/"workspace";project=workspace/"project";inbox=workspace/"inbox";outputs=workspace/"outputs";run=tmp_path/"run"
    for value in (project,inbox,outputs,workspace/"state",workspace/"tmp",run):value.mkdir(parents=True,exist_ok=True)
    (project/"readme.txt").write_text("hello")
    (project/"nested").mkdir();(project/"nested/child.txt").write_text("child")
    monkeypatch.setattr(runtime_app,"WORKSPACE",workspace);monkeypatch.setattr(runtime_app,"OUTPUTS",outputs);monkeypatch.setattr(runtime_app,"RUN",run)
    monkeypatch.setattr(runtime_app,"INSPECT_ROOTS",{"project":project,"inbox":inbox,"outputs":outputs})
    app=runtime_app.create_app()
    with TestClient(app) as client:
        overview=client.get("/v1/inspect/overview");assert overview.status_code==200
        assert overview.json()["disk_total"]==0 and overview.json()["disk_limited"] is False
        assert overview.json()["disk_used"]>=5
        files=client.get("/v1/inspect/files",params={"path":"project","depth":2}).json()
        assert {item["path"] for item in files["items"]}=={"project/nested","project/readme.txt"}
        assert [item["path"] for item in client.get("/v1/inspect/files",
            params={"path":"project/nested","depth":1}).json()["items"]]==["project/nested/child.txt"]
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
        released=first.post(f"/api/threads/{thread['id']}/sandbox/release")
        assert released.status_code==200 and released.json()=={"released":True,"checkpointed":False}
        assert second.get(f"/api/threads/{thread['id']}/sandbox").status_code==404


async def review17_environment(tmp_path,name):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/(name+".db")));await store.initialize()
    backend=FakeSandboxBackend();pool=SandboxPool(backend,
        {h:"test@sha256:"+"1"*64 for h in Harness},{h:0 for h in Harness},max_sandboxes=2)
    runtime=FakeRuntime();objects=FakeObjects();broker=FakeBroker()
    inspector=SandboxInspector(inspector_settings(),store,objects,pool,runtime,broker)
    owner="a"*64;profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title=name),profile,owner)
    return store,pool,runtime,objects,inspector,owner,thread


async def test_inspector_materializes_managed_files_as_streams(tmp_path):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,"stream")
    data=b"a"*(1024*1024)+b"b"*(1024*1024)+b"tail";reference,digest=objects.put(data)
    await store.register_project_file(thread.id,path="assets/large.bin",source_type="upload",
        source_turn_id="",sha256=digest,size=len(data),mime="application/octet-stream",object_ref=reference)
    def forbidden(*args,**kwargs):raise AssertionError("full object buffering is forbidden")
    objects.get=forbidden
    objects.iter_bytes=lambda value,**kwargs:(chunk for chunk in (data[:1024*1024],data[1024*1024:]))
    try:
        await inspector.takeover(thread.id,owner,network_access="restricted")
        assert runtime.materialized==[("assets/large.bin",data,"application/octet-stream")]
    finally:await pool.close();await store.close()


async def test_new_takeover_activates_runtime_before_restoring_workspace(tmp_path):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(
        tmp_path,"takeover-order")
    runtime.active=False;events=[]
    original_inject=runtime.inject;original_reset=runtime.reset_project_files

    async def inject(*args,**kwargs):
        events.append('inject');runtime.active=True
        return await original_inject(*args,**kwargs)

    async def reset(*args,**kwargs):
        assert runtime.active
        events.append('restore')
        return await original_reset(*args,**kwargs)

    runtime.inject=inject;runtime.reset_project_files=reset
    try:
        await inspector.takeover(thread.id,owner,network_access='restricted')
        assert events[:2]==['inject','restore']
    finally:await pool.close();await store.close()


async def test_control_activity_renews_lease_and_expired_status_is_not_active(tmp_path):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,"lease")
    try:
        await inspector.takeover(thread.id,owner,network_access="restricted")
        before=(await store.get_sandbox_control(thread.id,owner))["expires_at"]
        await inspector.status(thread.id,owner);after=(await store.get_sandbox_control(thread.id,owner))["expires_at"]
        assert after>=before
        async with store.connections.begin() as db:
            await db.execute(update(SandboxControlRow).where(SandboxControlRow.thread_id==thread.id)
                .values(expires_at=now()-timedelta(seconds=1)))
        status=await inspector.status(thread.id,owner)
        assert status["control_state"]=="expired" and status["capabilities"]["terminal"] is False
    finally:await pool.close();await store.close()


async def test_releasing_control_is_immediately_reconciled_after_revoke_failure(tmp_path,monkeypatch):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,"release")
    calls=0
    async def flaky(*args):
        nonlocal calls;calls+=1
        if calls==1:raise httpx.ConnectError("gateway unavailable")
    monkeypatch.setattr(inspector,"_revoke",flaky)
    try:
        await inspector.takeover(thread.id,owner,network_access="restricted")
        with pytest.raises(httpx.ConnectError):await inspector.release(thread.id,owner,managed_action="discard")
        control=await store.get_sandbox_control(thread.id,owner)
        assert control["state"]=="releasing" and control["release_phase"]=="checkpointed"
        assert any(item["thread_id"]==thread.id for item in await store.expired_sandbox_controls())
        await inspector.release(thread.id,owner,expired=True)
        assert await store.get_sandbox_control(thread.id,owner) is None and calls==2
    finally:await pool.close();await store.close()


async def test_checkpoint_transport_failure_stays_retryable_and_retains_sandbox(tmp_path,monkeypatch):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,"checkpoint-retry")
    revokes=0
    async def revoke(*args):
        nonlocal revokes;revokes+=1
    monkeypatch.setattr(inspector,"_revoke",revoke)
    try:
        control=await inspector.takeover(thread.id,owner,network_access="restricted")
        runtime.fail_checkpoint=True
        with pytest.raises(RuntimeError,match="checkpoint_retryable"):
            await inspector.release(thread.id,owner,managed_action="discard")
        pending=await store.get_sandbox_control(thread.id,owner)
        assert pending["release_phase"]=="managed_resolved" and revokes==0
        assert (await pool.get_attached(thread.id)).id==control["sandbox_id"]
        assert (await store.get_thread(thread.id,owner)).checkpoint_ref==""
        runtime.fail_checkpoint=False
        result=await inspector.release(thread.id,owner,managed_action="discard")
        assert result["checkpointed"] is True and revokes==1
        checkpoint=(await store.get_thread(thread.id,owner)).checkpoint_ref
        assert objects.get(checkpoint,maximum=1024)==b"checkpoint"
    finally:await pool.close();await store.close()


async def test_missing_sandbox_requires_explicit_checkpoint_loss_confirmation(tmp_path,monkeypatch):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,"checkpoint-missing")
    revokes=0;revoke_routes=[]
    async def revoke(*args):
        nonlocal revokes;revokes+=1;revoke_routes.append(args[-1])
    monkeypatch.setattr(inspector,"_revoke",revoke)
    try:
        control=await inspector.takeover(thread.id,owner,network_access="restricted")
        sandbox=await pool.get_attached(thread.id);await pool.release(sandbox,keep_attached=False)
        with pytest.raises(RuntimeError,match="checkpoint_unavailable"):
            await inspector.release(thread.id,owner,managed_action="discard")
        pending=await store.get_sandbox_control(thread.id,owner)
        assert pending["release_phase"]=="checkpoint_unavailable" and revokes==0
        result=await inspector.release(thread.id,owner,managed_action="discard",
            allow_checkpoint_loss=True)
        assert result["checkpointed"] is False and result["checkpoint_unavailable"] is True
        assert revokes==1 and await store.get_sandbox_control(thread.id,owner) is None
        assert revoke_routes==[pool.backend.runtime_route(sandbox).gateway_url]
    finally:await pool.close();await store.close()


async def test_port_grant_rejects_reused_sandbox_and_revokes_grant(tmp_path):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,"grant")
    try:
        sandbox=await pool.acquire(thread_id=thread.id,turn_id="inspect-"+thread.id,
            harness=Harness.CODEX,environment={})
        grant=await store.create_sandbox_port_grant(thread.id,owner,sandbox.id,
            sandbox.handle.to_mapping(),3000,
            now()+timedelta(minutes=5))
        sandbox.thread_id="other-thread";sandbox.metadata["sandbox-harness.io/thread"]="other-thread"
        with pytest.raises(KeyError,match="preview_expired"):
            await inspector.port_endpoint(grant["id"],owner)
        assert await store.get_sandbox_port_grant(grant["id"],owner) is None
    finally:await pool.close();await store.close()


async def test_observer_restore_prepares_runtime_before_managed_files_without_grant(tmp_path):
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,'observer-prepare')
    events=[];runtime.active=False;original=runtime.reset_project_files
    async def prepare(sandbox):events.append('prepare');runtime.active=True
    async def reset(sandbox):
        assert runtime.active,'runtime_bundle_not_prepared'
        events.append('managed-reset');await original(sandbox)
    runtime.prepare=prepare;runtime.reset_project_files=reset
    try:
        await inspector.restore(thread.id,owner)
        assert events==['prepare','managed-reset']
        assert runtime.injected==0
        assert await store.get_sandbox_control(thread.id,owner) is None
    finally:await pool.close();await store.close()


@pytest.mark.parametrize('cancelled',[False,True])
async def test_observer_prepare_failure_retires_its_new_sandbox(tmp_path,cancelled):
    import asyncio
    store,pool,runtime,objects,inspector,owner,thread=await review17_environment(tmp_path,'prepare-failure')
    error=asyncio.CancelledError if cancelled else RuntimeError
    async def prepare(sandbox):raise error('controlled preparation stop')
    runtime.prepare=prepare
    try:
        with pytest.raises(error):await inspector.restore(thread.id,owner)
        assert not await pool.managed_sandbox_ids()
        assert not runtime.materialized
    finally:await pool.close();await store.close()
