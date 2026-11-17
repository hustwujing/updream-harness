import hashlib
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import sandbox_runtime.app as runtime_app
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.broker import Broker
from sandbox_harness.config import Settings
from sandbox_harness.domain import (
    Attachment,
    Harness,
    ItemType,
    ThreadCreate,
    TurnCreate,
    TurnInputCreate,
    TurnState,
)
from sandbox_harness.runtime import RuntimeClient, RuntimeTransferError
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox import FakeSandboxBackend, Sandbox
from sandbox_harness.store import Store


class CountingStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.read_count = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk


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


def test_runtime_artifact_endpoint_rejects_count_over_limit(tmp_path, monkeypatch):
    outputs = tmp_path / "outputs"; outputs.mkdir()
    (outputs / ".sandbox-output-batch").write_text("turn")
    for name in ("a.txt", "b.txt", "c.txt"):
        (outputs / name).write_text(name)
    monkeypatch.setattr(runtime_app, "OUTPUTS", outputs)
    monkeypatch.setattr(runtime_app, "MAX_OUTPUT_ARTIFACTS", 2)
    response = TestClient(runtime_app.create_app()).get("/v1/artifacts")
    assert response.status_code == 413
    assert response.json()["detail"] == "artifact_count_limit_exceeded"


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


@pytest.mark.parametrize("model", [TurnCreate, TurnInputCreate])
def test_turn_inputs_reject_duplicate_canonical_attachment_names(model):
    first = Attachment(name="same.txt", object_ref="sha256:first", sha256="1" * 64,
        size=1, mime="text/plain")
    second = Attachment(name="same.txt", object_ref="sha256:second", sha256="2" * 64,
        size=1, mime="text/plain")
    with pytest.raises(ValidationError, match="Duplicate attachment filename"):
        model(message="hello", idempotency_key="duplicate-attachment", attachments=[first, second])
