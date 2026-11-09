import asyncio

from sandbox_harness.broker import Broker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import EventKind, Harness, RuntimeEvent, RuntimeResult, ThreadCreate, TurnCreate, TurnState
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.store import Store


class Runtime:
    async def inject(self, sandbox, **kwargs): pass
    async def restore(self, sandbox, archive): pass
    async def put_attachment(self, sandbox, name, data, mime): pass
    async def run(self, sandbox, **kwargs):
        yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text": "done"})
        yield RuntimeEvent(kind=EventKind.TOOL_FINISHED, payload={"name": "write", "result": "ok"})
        yield RuntimeResult(status="completed", native_thread_id="native-1")
    async def checkpoint(self, sandbox): return b"checkpoint"
    async def artifacts(self, sandbox): return [{"path": "result.txt", "mime": "text/plain"}]
    async def artifact(self, sandbox, path): return b"result"
    async def cancel(self, sandbox): pass


async def test_broker_runs_harness_and_persists_transcript_checkpoint_and_artifact(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "broker.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, Runtime(), catalog)
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "deep")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="deep", title="Test"), profile, "a" * 64)
        turn = await broker.submit(thread.id, TurnCreate(message="make a file", idempotency_key="turn-key-1"))
        for _ in range(100):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED}:
                break
            await asyncio.sleep(.02)
        assert current.state == TurnState.COMPLETED
        assert (await store.get_thread(thread.id)).checkpoint_ref.startswith("sha256:")
        assert [item.type.value for item in await store.items_after(thread.id)] == ["user_message", "agent_message", "tool", "artifact"]
        assert (await store.artifacts(turn.id))[0].path == "result.txt"
        assert not backend.items
    finally:
        await pool.close(); await store.close()
