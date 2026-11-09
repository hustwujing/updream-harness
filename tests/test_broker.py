import asyncio

from sandbox_harness.broker import Broker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import EventKind, Harness, InteractionResponse, RuntimeEvent, RuntimeResult, ThreadCreate, TurnCreate, TurnInputCreate, TurnState
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.store import Store


class Runtime:
    async def inject(self, sandbox, **kwargs): pass
    async def restore(self, sandbox, archive): pass
    async def put_attachment(self, sandbox, name, data, mime): pass
    async def put_project_file(self, sandbox, path, data, mime): pass
    async def reset_project_files(self, sandbox): pass
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


class LiveInputRuntime(AppendedInputRuntime):
    def __init__(self):
        super().__init__(); self.live_inputs = []

    async def append_input(self, sandbox, **kwargs):
        self.live_inputs.append(kwargs["message"]); self.release.set(); return "native_stream"


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
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.02)
        assert current.state == TurnState.COMPLETED
        assert (await store.get_thread(thread.id)).checkpoint_ref.startswith("sha256:")
        assert [item.type.value for item in await store.items_after(thread.id)] == ["user_message", "agent_message", "tool", "artifact"]
        assert (await store.artifacts(turn.id))[0].path == "result.txt"
        files = await store.current_project_files(thread.id)
        assert [(item.path, item.source_type) for item in files] == [("artifacts/result.txt", "artifact")]
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
