import asyncio
from datetime import timedelta
import httpx
import pytest

from sandbox_harness.broker import Broker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import EventKind, Harness, InteractionResponse, RuntimeEvent, RuntimeResult, ThreadCreate, TurnCreate, TurnInputCreate, TurnState, now
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.runtime import RuntimeStreamInterrupted
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.store import Store


class Runtime:
    def __init__(self): self.project_files=[]
    async def inject(self, sandbox, **kwargs): pass
    async def restore(self, sandbox, archive): pass
    async def put_attachment(self, sandbox, name, data, mime): pass
    async def put_project_file(self, sandbox, path, data, mime): self.project_files.append((path,data,mime))
    async def reset_project_files(self, sandbox): pass
    async def run(self, sandbox, **kwargs):
        yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text": "done"})
        yield RuntimeEvent(kind=EventKind.TOOL_FINISHED, payload={"name": "write", "result": "ok"})
        yield RuntimeResult(status="completed", native_thread_id="native-1")
    async def checkpoint(self, sandbox): return b"checkpoint"
    async def artifacts(self, sandbox, expected_batch_id): return [{"path": "result.txt", "mime": "text/plain"}]
    async def artifact(self, sandbox, path): return b"result"
    async def cancel(self, sandbox): return {"cancelled": True, "native_thread_id": "native-1"}


class PausableRuntime(Runtime):
    def __init__(self):
        super().__init__(); self.started = asyncio.Event(); self.cancelled = 0

    async def run(self, sandbox, **kwargs):
        self.started.set()
        await asyncio.Event().wait()
        yield RuntimeResult(status="completed", native_thread_id="native-pause")

    async def cancel(self, sandbox):
        self.cancelled += 1
        return {"cancelled": True, "native_thread_id": "native-pause",
            "execution_id": "paused-execution"}


class SlowPauseRuntime(PausableRuntime):
    def __init__(self):
        super().__init__(); self.pause_started = asyncio.Event()

    async def cancel(self, sandbox):
        self.cancelled += 1; self.pause_started.set(); await asyncio.sleep(.1)
        return {"cancelled": True, "native_thread_id": "native-pause",
            "execution_id": "paused-execution"}


class FailingRuntime(Runtime):
    async def run(self, sandbox, **kwargs):
        yield RuntimeResult(status="failed", error_code="harness_exit", error_detail="native process exited")


async def test_broker_close_preserves_sandbox_that_was_active_before_task_cancellation(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "close.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9")
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    broker = Broker(settings, store, objects, pool, Runtime(), HarnessCatalog())
    sandbox = await pool.acquire(thread_id="thread", turn_id="turn", harness=Harness.CLAUDE, environment={})
    broker.active_sandboxes["turn"] = sandbox
    try:
        preserve = await broker.close()
        await pool.close(preserve_sandbox_ids=preserve)
        assert preserve == {sandbox.id}
        assert sandbox.id in backend.items
    finally:
        await backend.delete(sandbox)
        await store.close()


class InterruptedRuntime(Runtime):
    async def run(self, sandbox, **kwargs):
        yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text": "partial"})
        raise RuntimeStreamInterrupted("runtime endpoint temporarily unavailable")


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


class LiveInputRuntime(AppendedInputRuntime):
    def __init__(self):
        super().__init__(); self.live_inputs = []; self.live_input_ids = []

    async def append_input(self, sandbox, **kwargs):
        self.live_inputs.append(kwargs["message"]); self.live_input_ids.append(kwargs["input_id"])
        self.release.set(); return "native_stream"

    async def run(self, sandbox, **kwargs):
        async for value in super().run(sandbox, **kwargs):
            if isinstance(value, RuntimeResult):
                value = value.model_copy(update={"input_ids": list(self.live_input_ids)})
            yield value


class RecoverableRuntime(Runtime):
    async def execution_status(self, sandbox, execution_id):
        return {"execution_id": execution_id, "state": "completed", "last_sequence": 2,
            "result": {"status": "completed", "native_thread_id": "native-recovered",
                "error_code": "", "error_detail": "", "input_ids": []}}

    async def resume(self, sandbox, execution_id, after_sequence):
        if after_sequence < 1:
            yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text": "recovered"}, runtime_sequence=1)
        if after_sequence < 2:
            yield RuntimeResult(status="completed", native_thread_id="native-recovered", runtime_sequence=2)


class MissingExecutionRuntime(Runtime):
    async def execution_status(self, sandbox, execution_id): return None


class InteractionRuntime(Runtime):
    def __init__(self):
        self.release = asyncio.Event(); self.responses = []

    async def run(self, sandbox, **kwargs):
        yield RuntimeEvent(kind=EventKind.WAITING_USER, payload={"interaction_id":"native-question",
            "interaction_kind":"question", "prompt":"Choose a format", "questions":[{"id":"format"}]})
        await self.release.wait()
        yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text":"continued"})
        yield RuntimeResult(status="completed", native_thread_id="native-interaction")

    async def respond_interaction(self, sandbox, interaction_id, response):
        self.responses.append((interaction_id, response)); self.release.set(); return True


class FailingInteractionRuntime(InteractionRuntime):
    async def respond_interaction(self, sandbox, interaction_id, response):
        raise httpx.ConnectError("runtime connection reset")


async def test_user_sandbox_control_blocks_turn_archive_and_delete(tmp_path):
    settings=Settings(data_root=tmp_path,database_url="sqlite+aiosqlite:///"+str(tmp_path/"control-block.db"),
        backend="fake",gateway_signing_key="k"*32,gateway_url="http://127.0.0.1:9")
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/"objects")
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:"" for h in Harness},{h:0 for h in Harness})
    catalog=HarnessCatalog();broker=Broker(settings,store,objects,pool,Runtime(),catalog)
    try:
        owner="a"*64;profile=catalog.resolve("codex","gpt-5.6","standard")
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level="standard",title="Controlled"),profile,owner)
        await store.begin_sandbox_control(thread.id,owner,"sandbox-control","nonce",
            now()+timedelta(hours=1))
        with pytest.raises(RuntimeError,match="sandbox_takeover_active"):
            await broker.submit(thread.id,TurnCreate(message="new",idempotency_key="blocked-turn"))
        with pytest.raises(RuntimeError,match="sandbox_takeover_active"):
            await broker.set_thread_archived(thread.id,owner,archived=True)
        with pytest.raises(RuntimeError,match="sandbox_takeover_active"):
            await broker.delete_thread(thread.id,owner)
    finally:
        await broker.close();await pool.close();await store.close()


async def test_terminal_turn_cannot_be_cancelled(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "cancel.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9")
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); runtime=Runtime(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    try:
        profile = catalog.resolve("codex", "gpt-5.6", "standard"); owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Terminal"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="done", idempotency_key="terminal-cancel-key"), profile, owner)
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        with pytest.raises(RuntimeError, match="already_terminal"):
            await broker.cancel(turn.id)
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_broker_pause_quiesces_and_checkpoints_primary_harness(tmp_path):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "pause-primary.db"),
        backend="fake", gateway_signing_key="k" * 32,
        gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); runtime = PausableRuntime()
    broker = Broker(settings, store, objects, pool, runtime, catalog)
    owner = "a" * 64
    try:
        profile = catalog.resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX,
            model=profile.model, reasoning_level="standard", title="Pause primary"),
            profile, owner)
        turn = await broker.submit(thread.id, TurnCreate(message="long task",
            idempotency_key="pause-primary-turn"))
        await asyncio.wait_for(runtime.started.wait(), 3)
        control = await broker.pause_thread(thread.id, owner)
        assert control.state.value == "PAUSING"
        for _ in range(300):
            current = await store.get_turn(turn.id)
            control = await store.get_thread_control(thread.id)
            if current.state == TurnState.PAUSED and control.state.value == "PAUSED":
                break
            await asyncio.sleep(.01)
        saved = await store.get_thread(thread.id)
        participant = await store.get_pause_participant(thread.id,
            "primary_harness", turn.id, control.generation)
        assert current.state == TurnState.PAUSED
        assert current.error_code == "paused"
        assert control.state.value == "PAUSED" and not control.degraded
        assert participant.checkpoint_ref == saved.checkpoint_ref
        assert saved.checkpoint_ref.startswith("sha256:")
        assert saved.native_thread_id == "native-pause"
        assert runtime.cancelled == 1 and not backend.items
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_broker_restart_finishes_committed_pause_without_resuming_business_execution(tmp_path):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "pause-restart.db"),
        backend="fake", gateway_signing_key="k" * 32,
        gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0,
        recovery_scan_seconds=300)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); runtime = PausableRuntime(); owner = "a" * 64
    profile = catalog.resolve("codex", "gpt-5.6", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX,
        model=profile.model, reasoning_level="standard", title="Pause restart"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id, TurnCreate(message="long task",
        idempotency_key="pause-restart-turn"), profile, owner)
    claimed = await store.claim_turn(turn.id, "dead-broker", 30)
    execution_id = turn.id + ":" + turn.id
    await store.start_runtime_execution(turn.id, execution_id, "dead-broker",
        claimed.recovery_generation)
    sandbox = await backend.create(Harness.CODEX, "", {
        "sandbox-harness.io/turn": turn.id,
        "sandbox-harness.io/thread": thread.id,
        "sandbox-harness.io/harness": Harness.CODEX.value,
    }, {})
    await store.set_turn_state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id,
        lease_owner="dead-broker", recovery_generation=claimed.recovery_generation)
    control = await store.request_thread_pause(thread.id, owner)
    await store.release_turn_lease(turn.id, "dead-broker", claimed.recovery_generation)
    broker = Broker(settings, store, objects, pool, runtime, catalog)
    try:
        await broker.start()
        for _ in range(300):
            current = await store.get_turn(turn.id)
            control = await store.get_thread_control(thread.id)
            if (current.state == TurnState.PAUSED and control.state.value == "PAUSED"
                    and turn.id not in broker.tasks and sandbox.id not in backend.items):
                break
            await asyncio.sleep(.01)
        assert current.state == TurnState.PAUSED
        assert control.generation == 1 and control.state.value == "PAUSED"
        assert runtime.cancelled == 1
        assert (await store.get_thread(thread.id)).native_thread_id == "native-pause"
        assert sandbox.id not in backend.items
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_broker_shutdown_does_not_cancel_pause_checkpoint_a_second_time(tmp_path):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "pause-shutdown.db"),
        backend="fake", gateway_signing_key="k" * 32,
        gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); runtime = SlowPauseRuntime(); owner = "a" * 64
    broker = Broker(settings, store, objects, pool, runtime, catalog)
    try:
        profile = catalog.resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX,
            model=profile.model, reasoning_level="standard", title="Pause shutdown"),
            profile, owner)
        turn = await broker.submit(thread.id, TurnCreate(message="long task",
            idempotency_key="pause-shutdown-turn"))
        await asyncio.wait_for(runtime.started.wait(), 3)
        await broker.pause_thread(thread.id, owner)
        await asyncio.wait_for(runtime.pause_started.wait(), 3)
        await broker.close()
        current = await store.get_turn(turn.id)
        control = await store.get_thread_control(thread.id)
        assert current.state == TurnState.PAUSED
        assert control.state.value == "PAUSED" and not control.degraded
        assert (await store.get_thread(thread.id)).native_thread_id == "native-pause"
    finally:
        await pool.close(); await store.close()


async def test_turn_heartbeat_renews_active_sandbox_expiration(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "heartbeat.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        turn_heartbeat_seconds=2, turn_lease_seconds=10)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); runtime=Runtime(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    try:
        profile = catalog.resolve("codex", "gpt-5.6", "standard"); owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Heartbeat"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="long task", idempotency_key="heartbeat-turn-key"), profile, owner)
        claimed = await store.claim_turn(turn.id, broker.instance_id, settings.turn_lease_seconds)
        sandbox = await backend.create(Harness.CODEX, "", {"sandbox-harness.io/turn": turn.id}, {})
        broker.active_sandboxes[turn.id] = sandbox
        heartbeat = asyncio.create_task(broker._heartbeat(turn.id, claimed.recovery_generation))
        for _ in range(50):
            if backend.renewed:
                break
            await asyncio.sleep(0.1)
        heartbeat.cancel(); await asyncio.gather(heartbeat, return_exceptions=True)
        assert backend.renewed
    finally:
        await pool.close(); await store.close()


async def test_broker_runs_harness_and_persists_transcript_checkpoint_and_artifact(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "broker.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); runtime=Runtime(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "deep")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="deep", title="Test"), profile, "a" * 64)
        object_ref,digest=objects.put(b"input-v1")
        source,_=await store.register_project_file(thread.id,path="assets/input.txt",source_type="upload",
            source_turn_id="",sha256=digest,size=8,mime="text/plain",object_ref=object_ref)
        turn = await broker.submit(thread.id, TurnCreate(message="make a file", idempotency_key="turn-key-1"))
        for _ in range(100):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.02)
        assert current.state == TurnState.COMPLETED
        assert (await store.get_thread(thread.id)).checkpoint_ref.startswith("sha256:")
        assert [item.type.value for item in await store.items_after(thread.id)] == ["user_message", "agent_message", "tool", "artifact"]
        assert (await store.artifacts(turn.id))[0].path == "result.txt"
        files = await store.current_project_files(thread.id)
        assert [(item.path, item.source_type) for item in files] == [
            ("artifacts/result.txt", "artifact"),("assets/input.txt", "upload")]
        snapshot=await store.turn_project_files(turn.id)
        assert len(snapshot)==1 and snapshot[0].project_file_id==source.id and snapshot[0].version==1
        assert runtime.project_files==[("assets/input.txt",b"input-v1","text/plain")]
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
            if current.state == TurnState.FAILED and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.02)
        errors = [item for item in await store.items_after(thread.id) if item.type.value == "error"]
        assert current.state == TurnState.FAILED
        assert len(errors) == 1 and errors[0].status == "failed"
        assert "执行进程异常退出" in errors[0].summary
        artifacts = await store.artifacts(turn.id)
        assert artifacts and objects.get(artifacts[0].object_ref, maximum=100) == b"result"
        exported = [item for item in await store.items_after(thread.id) if item.type.value == "artifact"]
        assert exported[0].payload["partial"] is True
        assert (await store.get_thread(thread.id)).checkpoint_ref
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
        recorded = (await store.list_turn_inputs(turn.id))[0]
        assert recorded.status == "applied" and recorded.delivery_mode == "deferred_safe_point"
        assert len({item.sandbox_id for item in [current]}) == 1
    finally:
        await pool.close(); await store.close()


async def test_broker_delivers_live_input_and_marks_it_applied_at_turn_boundary(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "live.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    runtime = LiveInputRuntime(); catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    owner = "a" * 64
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Live"), profile, owner)
        turn = await broker.submit(thread.id, TurnCreate(message="first", idempotency_key="live-first-key"))
        for _ in range(100):
            if (await store.get_turn(turn.id)).state == TurnState.RUNNING: break
            await asyncio.sleep(.01)
        appended, _ = await store.create_turn_input(turn.id,
            TurnInputCreate(message="steer", idempotency_key="live-steer-key"), owner)
        assert await broker.try_deliver_input(appended)
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks: break
            await asyncio.sleep(.01)
        assert current.state == TurnState.COMPLETED and runtime.live_inputs == ["steer"]
        recorded = (await store.list_turn_inputs(turn.id))[0]
        assert recorded.status == "applied" and recorded.delivery_mode == "native_stream"
    finally:
        await pool.close(); await store.close()


async def test_broker_reattaches_expired_running_turn_and_replays_runtime_journal(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "recover.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0, recovery_scan_seconds=300)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); owner = "a" * 64
    profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
        reasoning_level="standard", title="Recover"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id,
        TurnCreate(message="survive restart", idempotency_key="recover-running-key"), profile, owner)
    old = await store.claim_turn(turn.id, "dead-broker", 30)
    execution_id = turn.id + ":" + turn.id
    await store.start_runtime_execution(turn.id, execution_id, "dead-broker", old.recovery_generation)
    sandbox = await backend.create(Harness.CLAUDE, "", {
        "sandbox-harness.io/turn": turn.id,
        "sandbox-harness.io/thread": thread.id,
        "sandbox-harness.io/harness": Harness.CLAUDE.value,
    }, {})
    await store.set_turn_state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id,
        lease_owner="dead-broker", recovery_generation=old.recovery_generation)
    await store.release_turn_lease(turn.id, "dead-broker", old.recovery_generation)
    broker = Broker(settings, store, objects, pool, RecoverableRuntime(), catalog)
    try:
        await broker.start()
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.01)
        assert current.state == TurnState.COMPLETED
        assert current.runtime_sequence == 2
        assert "recovered" in [item.summary for item in await store.items_after(thread.id)]
        assert any(event.payload.get("code") == "turn_runtime_reattached"
            for event in await store.events_after(turn.id, 0))
        assert sandbox.id not in backend.items
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_primary_execution_failure_survives_cleanup_failure_and_saves_partial_workspace(tmp_path, monkeypatch):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "primary-failure.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize()
    objects = ObjectStore(tmp_path / "objects"); backend = FakeSandboxBackend()
    pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, InterruptedRuntime(), catalog)
    original_delete = backend.delete

    async def fail_delete(sandbox):
        raise ConnectionError("sandbox control plane unavailable")

    monkeypatch.setattr(backend, "delete", fail_delete)
    try:
        profile = catalog.resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Preserve primary failure"), profile, "a" * 64)
        turn = await broker.submit(thread.id,
            TurnCreate(message="run", idempotency_key="primary-failure-turn"))
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state == TurnState.FAILED and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.02)
        events = await store.events_after(turn.id, 0)
        errors = [item for item in await store.items_after(thread.id) if item.type.value == "error"]
        assert current.error_code == "RuntimeStreamInterrupted"
        assert len(errors) == 1 and "连接中断" in errors[0].summary
        assert any(event.payload.get("code") == "cleanup_secondary_failed" for event in events)
        assert not any(event.payload.get("code") == "cleanup_failed" for event in events)
        assert (await store.get_thread(thread.id)).checkpoint_ref
        assert (await store.artifacts(turn.id))[0].path == "result.txt"
    finally:
        monkeypatch.setattr(backend, "delete", original_delete)
        await pool.retry_deletions()
        await broker.close(); await pool.close(); await store.close()


async def test_broker_does_not_replay_turn_when_runtime_execution_is_lost(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "lost.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9", recovery_scan_seconds=300)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); owner = "a" * 64
    profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
        reasoning_level="standard", title="Lost"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id,
        TurnCreate(message="must not replay", idempotency_key="lost-runtime-key"), profile, owner)
    old = await store.claim_turn(turn.id, "dead", 30)
    await store.start_runtime_execution(turn.id, turn.id + ":initial", "dead", old.recovery_generation)
    sandbox = await backend.create(Harness.CLAUDE, "", {"sandbox-harness.io/turn":turn.id,
        "sandbox-harness.io/thread":thread.id, "sandbox-harness.io/harness":"claude"}, {})
    await store.set_turn_state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id,
        lease_owner="dead", recovery_generation=old.recovery_generation)
    await store.release_turn_lease(turn.id, "dead", old.recovery_generation)
    broker = Broker(settings, store, objects, pool, MissingExecutionRuntime(), catalog)
    try:
        await broker.start(); current = await store.get_turn(turn.id)
        assert current.state == TurnState.FAILED
        assert current.error_code == "turn_interrupted_during_harness_execution"
        assert sandbox.id not in backend.items
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_broker_finishes_cleanup_after_sandbox_was_deleted_by_previous_process(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "cleanup.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        recovery_scan_seconds=300)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); owner = "a" * 64
    profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
        reasoning_level="standard", title="Cleanup"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id,
        TurnCreate(message="finish cleanup", idempotency_key="cleanup-recovery-key"), profile, owner)
    old = await store.claim_turn(turn.id, "dead-broker", 30)
    await store.begin_turn_cleanup(turn.id, TurnState.COMPLETED, error_code="", error_detail="",
        owner="dead-broker", generation=old.recovery_generation)
    await store.release_turn_lease(turn.id, "dead-broker", old.recovery_generation)
    broker = Broker(settings, store, objects, pool, RecoverableRuntime(), catalog)
    try:
        await broker.start()
        current = await store.get_turn(turn.id)
        assert current.state == TurnState.COMPLETED and current.pending_final_state == ""
        assert any(event.payload.get("code") == "turn_cleanup_recovered"
            for event in await store.events_after(turn.id, 0))
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_broker_persists_native_question_and_resumes_the_same_runtime(tmp_path):
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "interaction.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    runtime = InteractionRuntime(); catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    owner = "a" * 64
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Interaction"), profile, owner)
        turn = await broker.submit(thread.id, TurnCreate(message="ask", idempotency_key="interaction-turn-key"))
        for _ in range(100):
            if (await store.get_turn(turn.id)).state == TurnState.AWAITING_USER:
                break
            await asyncio.sleep(.01)
        interactions = await store.list_interactions(thread.id, owner)
        assert len(interactions) == 1 and interactions[0].status == "pending"
        response = InteractionResponse(idempotency_key="interaction-response-key",
            action="answer", answers={"format":"mp4"})
        resolved = await broker.respond_interaction(interactions[0].id, owner, response)
        repeated = await broker.respond_interaction(interactions[0].id, owner, response)
        assert resolved.status == repeated.status == "responded"
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.01)
        assert current.state == TurnState.COMPLETED
        assert runtime.responses == [("native-question", {"action":"answer", "answers":{"format":"mp4"}})]
        approval = next(item for item in await store.items_after(thread.id) if item.type.value == "approval")
        assert approval.status == "completed" and approval.payload["response"]["answers"]["format"] == "mp4"
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_interaction_transport_failure_is_retryable_and_returns_domain_error(tmp_path):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "interaction-failure.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    runtime = FailingInteractionRuntime(); catalog = HarnessCatalog()
    broker = Broker(settings, store, objects, pool, runtime, catalog)
    owner = "a" * 64
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Interaction failure"), profile, owner)
        turn = await broker.submit(thread.id,
            TurnCreate(message="ask", idempotency_key="interaction-failure-turn"))
        for _ in range(100):
            if (await store.get_turn(turn.id)).state == TurnState.AWAITING_USER:
                break
            await asyncio.sleep(.01)
        interaction = (await store.list_interactions(thread.id, owner))[0]
        response = InteractionResponse(idempotency_key="interaction-failure-response",
            action="answer", answers={"format":"mp4"})
        with pytest.raises(RuntimeError, match="interaction_delivery_failed"):
            await broker.respond_interaction(interaction.id, owner, response)
        current = (await store.list_interactions(thread.id, owner))[0]
        assert current.status == "pending" and current.response_key == ""
    finally:
        runtime.release.set()
        await broker.close(); await pool.close(); await store.close()


async def test_stale_broker_cleanup_does_not_publish_a_false_failure(tmp_path, monkeypatch):
    settings = Settings(data_root=tmp_path,
        database_url="sqlite+aiosqlite:///" + str(tmp_path / "stale-cleanup.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, Runtime(), catalog)

    async def stale_cleanup(*args, **kwargs):
        raise RuntimeError("stale_turn_lease")

    monkeypatch.setattr(store, "begin_turn_cleanup", stale_cleanup)
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Stale cleanup"), profile, "a" * 64)
        turn = await broker.submit(thread.id,
            TurnCreate(message="run", idempotency_key="stale-cleanup-turn"))
        for _ in range(200):
            if turn.id not in broker.tasks:
                break
            await asyncio.sleep(.01)
        items = await store.items_after(thread.id)
        events = await store.events_after(turn.id, 0)
        assert not any(item.type.value == "error" for item in items)
        assert not any(event.payload.get("code") == "cleanup_failed" for event in events)
        assert turn.id not in broker.leases and turn.id not in broker.active_sandboxes
    finally:
        for sandbox in await backend.list_namespace():
            await backend.delete(sandbox)
        await broker.close(); await pool.close(); await store.close()
