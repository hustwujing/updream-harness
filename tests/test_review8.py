import asyncio
import hashlib
import io
import json
import sqlite3
import tarfile

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.requests import Request

import sandbox_runtime.app as runtime_app
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.execution.turn_broker import (Broker)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Attachment, Harness, ItemType, ThreadCreate, TurnCreate, TurnInputCreate, RuntimeResult, TurnState)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient, RuntimeTransferError)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend, Sandbox)
from test_support.legacy_store import (Store)
from test_support.checkpoints import (checkpoint_bytes)


class CountingStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.read_count = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk


class InterruptedStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"partial-"
        raise httpx.ReadError("controlled stream interruption")


async def test_runtime_checkpoint_download_retries_from_zero_after_stream_interruption(
        tmp_path,monkeypatch):
    waits=[];calls=0;payload=b"complete-checkpoint"
    async def no_wait(seconds):waits.append(seconds)
    monkeypatch.setattr('sandbox_harness.common.sandbox.runtime.asyncio.sleep',no_wait)
    def respond(request):
        nonlocal calls;calls+=1
        return (httpx.Response(200,stream=InterruptedStream()) if calls==1 else
            httpx.Response(200,headers={"Content-Length":str(len(payload))},
                stream=CountingStream([payload])))
    runtime=RuntimeClient(transport=httpx.MockTransport(respond),staging_root=tmp_path/"stage")
    sandbox=Sandbox(id="sandbox",harness=Harness.CODEX,image="image",
        runtime_url="http://runtime",metadata={})

    staged=await runtime.checkpoint(sandbox,maximum=1024)
    try:
        assert calls==2 and waits==[1]
        assert staged.path.read_bytes()==payload
        assert staged.sha256==hashlib.sha256(payload).hexdigest()
        assert list((tmp_path/"stage").glob(".runtime-download-*"))==[staged.path]
    finally:staged.remove()


async def test_runtime_checkpoint_stream_stops_at_limit_and_removes_stage(tmp_path):
    stream = CountingStream([b"a" * 8, b"b" * 8, b"must-not-be-read"])

    def respond(request):
        return httpx.Response(200, stream=stream)

    runtime = RuntimeClient(transport=httpx.MockTransport(respond), staging_root=tmp_path / "stage")
    sandbox = Sandbox(id="sandbox", harness=Harness.CODEX, image="image",
        runtime_url="http://runtime", metadata={})
    with pytest.raises(RuntimeTransferError, match="checkpoint_too_large"):
        await runtime.checkpoint(sandbox, maximum=12)
    assert stream.read_count == 2
    assert list((tmp_path / "stage").rglob(".runtime-download-*")) == []


async def test_runtime_artifact_stream_persists_multichunk_without_bytes_result(tmp_path):
    chunks = [b"first-", b"second-", b"third"]
    runtime = RuntimeClient(transport=httpx.MockTransport(lambda request:
        httpx.Response(200, headers={"Content-Length": str(sum(map(len, chunks)))},
            stream=CountingStream(chunks))), staging_root=tmp_path / "stage")
    sandbox = Sandbox(id="sandbox", harness=Harness.CODEX, image="image",
        runtime_url="http://runtime", metadata={})
    staged = await runtime.artifact(sandbox, "result.bin", maximum=1024)
    try:
        assert not isinstance(staged, bytes)
        assert staged.size == sum(map(len, chunks))
        assert staged.sha256 == hashlib.sha256(b"".join(chunks)).hexdigest()
        assert staged.path.read_bytes() == b"".join(chunks)
    finally:
        staged.remove()


async def test_oversized_chunked_checkpoint_creates_no_binding_or_temp_file(tmp_path):
    stream = CountingStream([b"a" * 8, b"b" * 8, b"must-not-be-read"])
    runtime = RuntimeClient(transport=httpx.MockTransport(lambda request:
        httpx.Response(200, stream=stream)), staging_root=tmp_path / "stage")
    settings = Settings(backend="fake", data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "broker.db"),
        max_checkpoint_bytes=12, gateway_url="http://127.0.0.1:9")
    store = Store(settings.database_url); await store.initialize()
    pool = SandboxPool(FakeSandboxBackend(), {h: "" for h in Harness}, {h: 0 for h in Harness})
    broker = Broker(settings, store, ObjectStore(tmp_path / "objects"), pool, runtime, HarnessCatalog())
    try:
        profile = broker.catalog.resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Bounded"), profile, "owner")
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="save", idempotency_key="bounded-checkpoint"), profile, "owner")
        claimed = await store.claim_turn(turn.id, broker.instance_id, 30)
        broker.leases[turn.id] = claimed.recovery_generation
        sandbox = Sandbox(id="sandbox", harness=Harness.CODEX, image="image",
            runtime_url="http://runtime", metadata={})
        with pytest.raises(RuntimeTransferError, match="checkpoint_too_large"):
            await broker._save_workspace(sandbox, thread.id, turn.id, "native")
        assert (await store.get_thread(thread.id)).checkpoint_ref == ""
        assert await store.artifacts(turn.id) == []
        assert list((tmp_path / "stage").rglob(".runtime-download-*")) == []
    finally:
        await pool.close(); await store.close()


async def test_runtime_artifact_manifest_fails_closed_when_truncated():
    runtime = RuntimeClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,
        json={"batch_id":"current", "items":[], "truncated":True})))
    sandbox = Sandbox(id="sandbox", harness=Harness.CODEX, image="image",
        runtime_url="http://runtime", metadata={})
    with pytest.raises(RuntimeTransferError, match="artifact_count_limit_exceeded"):
        await runtime.artifacts(sandbox, "current")


async def test_runtime_artifact_manifest_translates_real_count_limit_response():
    runtime = RuntimeClient(transport=httpx.MockTransport(lambda request:httpx.Response(413,
        json={"detail":"artifact_count_limit_exceeded"})))
    sandbox = Sandbox(id="sandbox", harness=Harness.CODEX, image="image",
        runtime_url="http://runtime", metadata={})
    with pytest.raises(RuntimeTransferError, match="artifact_count_limit_exceeded") as captured:
        await runtime.artifacts(sandbox, "current")
    assert captured.value.error_code == "artifact_count_limit_exceeded"


async def test_artifact_count_limit_is_public_turn_failure(tmp_path):
    class Runtime:
        async def inject(self, *args, **kwargs): pass
        async def reset_project_files(self, *args): pass
        async def run(self, *args, **kwargs):
            yield RuntimeResult(status="completed", native_thread_id="native")
        async def checkpoint(self, sandbox, *, maximum):
            path = tmp_path / ("checkpoint-" + __import__('uuid').uuid4().hex)
            data=checkpoint_bytes();path.write_bytes(data)
            from sandbox_harness.common.sandbox.runtime import (StagedRuntimeObject)
            return StagedRuntimeObject(path,len(data),hashlib.sha256(data).hexdigest())
        async def artifacts(self, *args):
            raise RuntimeTransferError("artifact_count_limit_exceeded")
        async def cancel(self, *args): return {"cancelled": True}

    settings = Settings(backend="fake", data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "public-count.db"),
        gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    pool = SandboxPool(FakeSandboxBackend(), {h: "" for h in Harness}, {h: 0 for h in Harness})
    broker = Broker(settings, store, ObjectStore(tmp_path / "objects"), pool, Runtime(), HarnessCatalog())
    try:
        profile = broker.catalog.resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Count limit"), profile, "owner")
        turn = await broker.submit(thread.id,
            TurnCreate(message="many files", idempotency_key="public-count-limit"))
        for _ in range(300):
            current = await store.get_turn(turn.id)
            if current.state == TurnState.FAILED:
                break
            await __import__('asyncio').sleep(.01)
        assert current.state == TurnState.FAILED
        error = next(item for item in await store.items_after(thread.id) if item.type == ItemType.ERROR)
        assert error.payload["code"] == "artifact_count_limit_exceeded"
        assert "50000" in error.payload["detail"]
    finally:
        await broker.close(); await pool.close(); await store.close()


def test_runtime_artifact_endpoint_rejects_count_over_limit(tmp_path, monkeypatch):
    outputs = tmp_path / "outputs"; outputs.mkdir()
    marker = tmp_path / "run" / "output-batch"; marker.parent.mkdir(); marker.write_text("turn")
    for name in ("a.txt", "b.txt", "c.txt"):
        (outputs / name).write_text(name)
    monkeypatch.setattr(runtime_app, "OUTPUTS", outputs)
    monkeypatch.setattr(runtime_app, "OUTPUT_BATCH_MARKER", marker)
    monkeypatch.setattr(runtime_app, "MAX_OUTPUT_ARTIFACTS", 2)
    response = TestClient(runtime_app.create_app()).get("/v1/artifacts")
    assert response.status_code == 413
    assert response.json()["detail"] == "artifact_count_limit_exceeded"


def test_runtime_output_batch_identity_survives_publication_cleanup(tmp_path, monkeypatch):
    outputs=tmp_path/'outputs';outputs.mkdir();(outputs/'old.txt').write_text('old')
    marker=tmp_path/'run'/'output-batch'
    monkeypatch.setattr(runtime_app,'OUTPUTS',outputs)
    monkeypatch.setattr(runtime_app,'OUTPUT_BATCH_MARKER',marker)
    app=runtime_app.create_app();runtime_app.reset_outputs('turn-1')
    for path in outputs.iterdir():path.unlink()
    (outputs/'final.txt').write_text('final')
    response=TestClient(app).get('/v1/artifacts')
    assert response.status_code==200
    assert response.json()['batch_id']=='turn-1'
    assert [row['path'] for row in response.json()['items']]==['final.txt']


def test_runtime_artifact_count_exact_boundaries():
    runtime_app.require_output_artifact_count(49_999)
    runtime_app.require_output_artifact_count(50_000)
    with pytest.raises(Exception) as captured:
        runtime_app.require_output_artifact_count(50_001)
    assert captured.value.status_code == 413
    assert captured.value.detail == "artifact_count_limit_exceeded"


async def test_failed_save_marks_existing_turn_artifact_partial_and_retry_is_new_history(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "partial.db")); await store.initialize()
    try:
        owner = "a" * 64
        profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Partial"), profile, owner)
        first, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="first", idempotency_key="partial-first-turn"), profile, owner)
        first_lease = await store.claim_turn(first.id, "worker", 30)
        artifact, _, _ = await store.publish_artifact(thread.id, first.id, path="result.txt",
            sha256="1" * 64, size=1, mime="text/plain", object_ref="sha256:" + "1" * 64,
            partial=False, lease_owner="worker", recovery_generation=first_lease.recovery_generation)
        await store.mark_turn_artifacts_partial(first.id, lease_owner="worker",
            recovery_generation=first_lease.recovery_generation)
        first_item = next(item for item in await store.items_after(thread.id)
            if item.type == ItemType.ARTIFACT)
        assert first_item.payload["artifact_id"] == artifact.id
        assert first_item.payload["partial"] is True

        await store.set_turn_state(first.id, TurnState.FAILED, lease_owner="worker",
            recovery_generation=first_lease.recovery_generation)
        await store.release_turn_lease(first.id, "worker", first_lease.recovery_generation)
        second, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="retry", idempotency_key="partial-retry-turn"), profile, owner)
        second_lease = await store.claim_turn(second.id, "worker", 30)
        retry, _, created = await store.publish_artifact(thread.id, second.id, path="result.txt",
            sha256="1" * 64, size=1, mime="text/plain", object_ref="sha256:" + "1" * 64,
            partial=False, lease_owner="worker", recovery_generation=second_lease.recovery_generation)
        items = [item for item in await store.items_after(thread.id) if item.type == ItemType.ARTIFACT]
        assert created and retry.id != artifact.id
        assert [item.payload["partial"] for item in items] == [True, False]
    finally:
        await store.close()


async def test_initialize_migrates_existing_artifacts_partial_column_idempotently(tmp_path):
    database = tmp_path / "artifact-upgrade.db"
    url = "sqlite+aiosqlite:///" + str(database)
    store = Store(url); await store.initialize()
    artifact = await store.add_artifact("turn-before-upgrade", path="kept.txt", sha256="3" * 64,
        size=4, mime="text/plain", object_ref="sha256:" + "3" * 64)
    await store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE artifacts DROP COLUMN partial")
    for _ in range(2):
        reopened = Store(url); await reopened.initialize(); await reopened.close()
    with sqlite3.connect(database) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(artifacts)")}
        row = connection.execute("SELECT id, partial FROM artifacts WHERE id = ?", (artifact.id,)).fetchone()
    assert "partial" in columns
    assert row == (artifact.id, 0)


async def test_concurrent_initialize_migrates_artifacts_partial_once(tmp_path):
    database = tmp_path / "artifact-upgrade-race.db"
    url = "sqlite+aiosqlite:///" + str(database)
    seed = Store(url); await seed.initialize(); await seed.close()
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE artifacts DROP COLUMN partial")
    stores = [Store(url) for _ in range(4)]
    try:
        await asyncio.gather(*(store.initialize() for store in stores))
    finally:
        await asyncio.gather(*(store.close() for store in stores))
    with sqlite3.connect(database) as connection:
        columns = [row[1] for row in connection.execute("PRAGMA table_info(artifacts)")]
    assert columns.count("partial") == 1


def checkpoint_tar() -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        payload = b"restored"
        member = tarfile.TarInfo("project/restored.txt"); member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    return output.getvalue()


def chunked_request(path: str, chunks: list[bytes]):
    delivered = 0

    async def receive():
        nonlocal delivered
        if delivered >= len(chunks):
            return {"type":"http.request", "body":b"", "more_body":False}
        body = chunks[delivered]; delivered += 1
        return {"type":"http.request", "body":body, "more_body":delivered < len(chunks)}

    request = Request({"type":"http", "method":"PUT", "path":path,
        "query_string":b"", "headers":[]}, receive)
    return request, lambda: delivered


async def test_runtime_checkpoint_restore_streams_and_stops_over_limit(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"; workspace.mkdir()
    run = tmp_path / "run"; run.mkdir()
    monkeypatch.setattr(runtime_app, "WORKSPACE", workspace)
    monkeypatch.setattr(runtime_app, "RUN", run)
    monkeypatch.setattr(runtime_app, "MAX_CHECKPOINT_BYTES", 12)
    app = runtime_app.create_app()
    endpoint = next(route.endpoint for route in app.routes
        if getattr(route, "path", None) == "/v1/checkpoint" and "PUT" in route.methods)
    request, delivered = chunked_request("/v1/checkpoint", [b"a" * 8, b"b" * 8, b"unread"])
    with pytest.raises(Exception) as captured:
        await endpoint(request)
    assert captured.value.status_code == 413
    assert captured.value.detail == "checkpoint_too_large"
    assert delivered() == 2
    assert list(run.glob(".checkpoint-restore-*")) == []


async def test_runtime_checkpoint_restore_accepts_multichunk_without_request_body(tmp_path, monkeypatch):
    archive = checkpoint_tar(); workspace = tmp_path / "workspace"; workspace.mkdir()
    run = tmp_path / "run"; run.mkdir()
    monkeypatch.setattr(runtime_app, "WORKSPACE", workspace)
    monkeypatch.setattr(runtime_app, "RUN", run)
    monkeypatch.setattr(runtime_app, "MAX_CHECKPOINT_BYTES", len(archive) + 1)
    monkeypatch.setattr(Request, "body", lambda self: (_ for _ in ()).throw(
        AssertionError("request.body must not be called")))
    app = runtime_app.create_app()
    endpoint = next(route.endpoint for route in app.routes
        if getattr(route, "path", None) == "/v1/checkpoint" and "PUT" in route.methods)
    thirds = max(1, len(archive) // 3)
    request, delivered = chunked_request("/v1/checkpoint",
        [archive[:thirds], archive[thirds:2*thirds], archive[2*thirds:]])
    assert await endpoint(request) == {"restored": True}
    assert delivered() == 3
    assert (workspace / "project" / "restored.txt").read_bytes() == b"restored"
    assert list(run.glob(".checkpoint-restore-*")) == []


async def test_runtime_project_file_accepts_proxy_stream_without_content_length(tmp_path,monkeypatch):
    workspace=tmp_path/"workspace";workspace.mkdir();monkeypatch.setattr(runtime_app,"WORKSPACE",workspace)
    app=runtime_app.create_app();endpoint=next(route.endpoint for route in app.routes
        if getattr(route,"path",None)=="/v1/project-files/file" and "PUT" in route.methods)
    request,delivered=chunked_request("/v1/project-files/file",[b"first-",b"second"])
    result=await endpoint(request,path="assets/reference.bin",size=12)
    assert result["bytes"]==12 and delivered()==2
    assert (workspace/"project/assets/reference.bin").read_bytes()==b"first-second"
    short,_=chunked_request("/v1/project-files/file",[b"short"])
    with pytest.raises(Exception) as captured:await endpoint(short,path="assets/short.bin",size=6)
    assert captured.value.status_code==400 and captured.value.detail=="Project file length mismatch"
    assert not (workspace/"project/assets/short.bin").exists()


@pytest.mark.parametrize("model", [TurnCreate, TurnInputCreate])
def test_turn_inputs_reject_duplicate_canonical_attachment_names(model):
    first = Attachment(name="same.txt", object_ref="sha256:first", sha256="1" * 64,
        size=1, mime="text/plain")
    second = Attachment(name="same.txt", object_ref="sha256:second", sha256="2" * 64,
        size=1, mime="text/plain")
    with pytest.raises(ValidationError, match="Duplicate attachment filename"):
        model(message="hello", idempotency_key="duplicate-attachment", attachments=[first, second])
