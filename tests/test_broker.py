import asyncio

from sandbox_harness.broker import Broker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import EventKind, Harness, RuntimeEvent, RuntimeResult, ThreadCreate, TurnCreate, TurnInputCreate, TurnState
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


class FailingRuntime(Runtime):
    async def run(self, sandbox, **kwargs):
        yield RuntimeResult(status="failed", error_code="harness_exit", error_detail="native process exited")


class AppendedInputRuntime(Runtime):
    def __init__(self):
        self.release = asyncio.Event()
        self.messages = []

    async def run(self, sandbox, **kwargs):
        self.messages.append(kwargs["message"])
        if len(self.messages) == 1:
            await self.release.wait()
        yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text": "done " + kwargs["message"]})
        yield RuntimeResult(status="completed", native_thread_id="native-appended")


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


async def test_broker_persists_a_user_visible_item_for_terminal_failure(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "failed.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, FailingRuntime(), catalog)
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Failure"), profile, "a" * 64)
        turn = await broker.submit(thread.id, TurnCreate(message="fail", idempotency_key="failed-turn-key"))
        for _ in range(100):
            current = await store.get_turn(turn.id)
            if current.state == TurnState.FAILED:
                break
            await asyncio.sleep(.02)
        errors = [item for item in await store.items_after(thread.id) if item.type.value == "error"]
        assert current.state == TurnState.FAILED
        assert len(errors) == 1 and errors[0].status == "failed"
        assert "Harness 执行失败" in errors[0].summary
    finally:
        await pool.close(); await store.close()


async def test_broker_rejects_switching_harness_inside_a_conversation(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "fixed.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9")
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, Runtime(), catalog)
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Fixed"), profile, "a" * 64)
        try:
            await broker.submit(thread.id, TurnCreate(message="switch", idempotency_key="fixed-switch-key",
                harness=Harness.CODEX, model="gpt-5.6", reasoning_level="standard"))
        except ValueError as exc:
            assert "fixed" in str(exc)
        else:
            raise AssertionError("Harness switch was accepted")
    finally:
        await pool.close(); await store.close()


async def test_broker_drains_appended_inputs_in_same_sandbox_session(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "append.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    runtime = AppendedInputRuntime(); catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    owner = "a" * 64
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Append"), profile, owner)
        turn = await broker.submit(thread.id, TurnCreate(message="first", idempotency_key="append-first-key"))
        for _ in range(100):
            if (await store.get_turn(turn.id)).state == TurnState.RUNNING:
                break
            await asyncio.sleep(.01)
        appended, _ = await store.create_turn_input(turn.id,
            TurnInputCreate(message="second", idempotency_key="append-second-key"), owner)
        runtime.release.set()
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.01)
        assert current.state == TurnState.COMPLETED
        assert runtime.messages == ["first", "second"]
        assert (await store.list_turn_inputs(turn.id))[0].status == "applied"
        assert len({item.sandbox_id for item in [current]}) == 1
    finally:
        await pool.close(); await store.close()
