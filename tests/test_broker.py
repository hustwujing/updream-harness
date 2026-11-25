import asyncio
import hashlib
import json
from datetime import timedelta
from pathlib import Path
import httpx
import pytest
from types import SimpleNamespace

from sandbox_harness.broker import Broker,reconcile_native_model_error
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import EventKind, Harness, InteractionResponse, RuntimeEvent, RuntimeResult, ThreadCreate, TurnCreate, TurnInputCreate, TurnState, now
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.runtime import RuntimeStreamInterrupted, StagedRuntimeObject
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.sandbox_contracts import SandboxCreateRequest,SandboxHandle
from sandbox_harness.sandbox_factory import SandboxProfileResolver
from sandbox_harness.store import Store


class Runtime:
    def __init__(self): self.project_files=[]
    async def inject(self, sandbox, **kwargs): pass
    async def restore(self, sandbox, archive, *, size):
        self.restored=b''.join([chunk async for chunk in archive])
    async def put_attachment(self, sandbox, name, data, mime): pass
    async def put_project_file(self, sandbox, path, data, mime): self.project_files.append((path,data,mime))
    async def reset_project_files(self, sandbox): pass
    async def run(self, sandbox, **kwargs):
        yield RuntimeEvent(kind=EventKind.TEXT_DELTA, payload={"text": "done"})
        yield RuntimeEvent(kind=EventKind.TOOL_FINISHED, payload={"name": "write", "result": "ok"})
        yield RuntimeResult(status="completed", native_thread_id="native-1")
    @staticmethod
    def stage(data):
        root=Path.cwd()/'.data'/'tmp'/'test-runtime';root.mkdir(parents=True,exist_ok=True)
        path=root/(__import__('uuid').uuid4().hex);path.write_bytes(data)
        return StagedRuntimeObject(path,len(data),hashlib.sha256(data).hexdigest())
    async def checkpoint(self, sandbox, *, maximum): return self.stage(b"checkpoint")
    async def artifacts(self, sandbox, expected_batch_id): return [{"path": "result.txt", "mime": "text/plain",
        "size":6,"sha256":hashlib.sha256(b"result").hexdigest()}]
    async def artifact(self, sandbox, path, *, maximum): return self.stage(b"result")
    async def cancel(self, sandbox): return {"cancelled": True, "native_thread_id": "native-1"}


def sandbox_handle_json(sandbox):
    return json.dumps(sandbox.handle.to_mapping(),sort_keys=True,separators=(",",":"))


async def test_orphan_reaper_keeps_provisioning_job_sandbox():
    sandbox=SimpleNamespace(id='sandbox-job',handle=SandboxHandle(provider='fake',
        sandbox_id='sandbox-job',allocation_id='job-live',profile_key='a'*63,
        revision='revision-1'),metadata={'sandbox-harness.io/turn':'job-live'})
    class Backend:
        deleted=[]
        async def delete(self,value):self.deleted.append(value.id)
    backend=Backend()
    pool=SimpleNamespace(backend=backend,retry_deletions=lambda:None)
    async def retry():return None
    async def empty():return set()
    async def owned():return [sandbox]
    async def managed_keys():return set()
    pool.retry_deletions=retry;pool.managed_sandbox_ids=empty;pool.managed_sandbox_keys=managed_keys
    pool.sandbox_key=lambda value:(value.handle.provider,value.handle.sandbox_id);pool.list_owned=owned
    store=SimpleNamespace(nonterminal_turn_sandbox_ids=empty,
        active_sandbox_control_ids=empty,
        active_job_sandbox_ids=empty,nonterminal_job_ids=lambda:None)
    async def live_jobs():return {'job-live'}
    async def job_sandboxes():return {sandbox.id}
    store.nonterminal_job_ids=live_jobs;store.active_job_sandbox_ids=job_sandboxes
    async def active_keys():return {('fake',sandbox.id)}
    store.active_sandbox_keys=active_keys
    broker=SimpleNamespace(pool=pool,store=store,active_sandboxes={})
    await Broker.cleanup_startup_orphans(broker)
    assert backend.deleted==[]


async def test_orphan_reaper_keeps_durable_turn_sandbox_when_provider_metadata_is_stale():
    sandbox=SimpleNamespace(id="durable-sandbox",handle=SandboxHandle(provider="fake",
        sandbox_id="durable-sandbox",allocation_id="turn",profile_key="a"*63,
        revision="revision-1"),metadata={
        "sandbox-harness.io/turn":"original-turn"})
    class Backend:
        deleted=[]
        async def delete(self,value):self.deleted.append(value.id)
    backend=Backend()
    async def retry():return None
    async def empty():return set()
    async def active_sandbox():return {sandbox.id}
    async def owned():return [sandbox]
    async def managed_keys():return set()
    async def active_keys():return {("fake",sandbox.id)}
    pool=SimpleNamespace(backend=backend,retry_deletions=retry,managed_sandbox_ids=empty,
        managed_sandbox_keys=managed_keys,
        sandbox_key=lambda value:(value.handle.provider,value.handle.sandbox_id),list_owned=owned)
    store=SimpleNamespace(nonterminal_turn_sandbox_ids=active_sandbox,active_sandbox_control_ids=empty,
        active_job_sandbox_ids=empty,nonterminal_job_ids=empty,active_sandbox_keys=active_keys)
    broker=SimpleNamespace(pool=pool,store=store,active_sandboxes={})
    await Broker.cleanup_startup_orphans(broker)
    assert backend.deleted==[]


async def test_turn_inventory_uses_durable_id_when_provider_turn_metadata_is_stale():
    sandbox=SimpleNamespace(id="durable-sandbox")
    handle=SandboxHandle(provider="fake",sandbox_id=sandbox.id,allocation_id="current-turn",
        profile_key="a"*63,revision="revision-1")
    class Backend:
        async def get(self,value):
            assert value==handle
            return sandbox
    broker=Broker.__new__(Broker)
    broker.pool=SimpleNamespace(backend=Backend())
    rows=await broker._turn_inventory(SimpleNamespace(id="current-turn",sandbox_id=sandbox.id,
        sandbox_handle_json=json.dumps(handle.to_mapping())))
    assert rows==[sandbox]


async def test_turn_inventory_rejects_nonterminal_sandbox_without_handle():
    broker=Broker.__new__(Broker);broker.pool=SimpleNamespace(backend=SimpleNamespace())
    with pytest.raises(RuntimeError,match="handle_missing"):
        await broker._turn_inventory(SimpleNamespace(id="current-turn",sandbox_id="sandbox" ,
            sandbox_handle_json=""))


async def test_runtime_diagnostics_are_incremental_and_report_counter_deltas(monkeypatch):
    import logging
    import sandbox_harness.broker as broker_module
    import sandbox_harness.telemetry_ingest as ingest
    snapshots=[
        {'instance_id':'runtime-1','retained_from':1,'last_sequence':1,
            'records':[{'sequence':1}],'stats':{'dropped_records':4,'export_failures':1}},
        {'instance_id':'runtime-1','retained_from':1,'last_sequence':2,
            'records':[{'sequence':2}],'stats':{'dropped_records':4,'export_failures':1}},
    ]
    class RuntimeDiagnostics:
        def __init__(self):self.after=[]
        async def diagnostics(self,sandbox,after):
            self.after.append(after);return snapshots.pop(0)
    runtime=RuntimeDiagnostics();events=[]
    monkeypatch.setattr(broker_module,'telemetry',lambda:object())
    monkeypatch.setattr(broker_module,'event',lambda name,**fields:events.append((name,fields)))
    monkeypatch.setattr(ingest,'import_snapshot',lambda target,snapshot,sandbox:len(snapshot['records']))
    broker=Broker.__new__(Broker);broker.runtime=runtime;broker._runtime_diagnostics={}
    sandbox=SimpleNamespace(id='sandbox-1',turn_id='turn',thread_id='thread',metadata={})
    await broker._collect_runtime_logs(sandbox);await broker._collect_runtime_logs(sandbox)
    assert runtime.after==[0,1]
    collected=[fields for name,fields in events if name=='runtime.logs.collected']
    assert collected[0]['dropped_count']==4 and collected[0]['export_failures']==1
    assert collected[1]['dropped_count']==0 and collected[1]['export_failures']==0
    assert collected[1]['level']==logging.INFO
    assert collected[1]['reason']=='runtime_turn_delta'


async def test_runtime_diagnostics_handle_instance_reset_rotation_and_bound(monkeypatch):
    import sandbox_harness.broker as broker_module
    import sandbox_harness.telemetry_ingest as ingest
    class RuntimeDiagnostics:
        def __init__(self):self.after=[];self.snapshots=[]
        async def diagnostics(self,sandbox,after):
            self.after.append(after);return self.snapshots.pop(0)
    runtime=RuntimeDiagnostics();events=[]
    monkeypatch.setattr(broker_module,'telemetry',lambda:object())
    monkeypatch.setattr(broker_module,'event',lambda name,**fields:events.append((name,fields)))
    monkeypatch.setattr(ingest,'import_snapshot',lambda target,snapshot,sandbox:len(snapshot['records']))
    broker=Broker.__new__(Broker);broker.runtime=runtime;broker._runtime_diagnostics={}
    sandbox=SimpleNamespace(id='sandbox-reset',turn_id='turn',thread_id='thread',metadata={})
    runtime.snapshots=[{'instance_id':'runtime-1','retained_from':1,'last_sequence':9,
        'records':[{'sequence':9}],'stats':{'disk_errors':7}}]
    await broker._collect_runtime_logs(sandbox)
    runtime.snapshots=[
        {'instance_id':'runtime-2','retained_from':1,'last_sequence':2,'records':[],
            'stats':{'disk_errors':2}},
        {'instance_id':'runtime-2','retained_from':1,'last_sequence':2,
            'records':[{'sequence':1},{'sequence':2}],'stats':{'disk_errors':2}},
    ]
    await broker._collect_runtime_logs(sandbox)
    assert runtime.after[-2:]==[9,0]
    assert broker._runtime_diagnostics['sandbox-reset'].cursor==2
    runtime.snapshots=[{'instance_id':'runtime-2','retained_from':5,'last_sequence':5,
        'records':[{'sequence':5}],'stats':{'disk_errors':1}}]
    await broker._collect_runtime_logs(sandbox)
    assert broker._runtime_diagnostics['sandbox-reset'].cursor==5
    last=[fields for name,fields in events if name=='runtime.logs.collected'][-1]
    assert last['disk_errors']==1

    for index in range(513):
        runtime.snapshots=[{'instance_id':f'instance-{index}','retained_from':1,
            'last_sequence':0,'records':[],'stats':{}}]
        await broker._collect_runtime_logs(SimpleNamespace(id=f'sandbox-{index}',turn_id='',
            thread_id='',metadata={}))
    assert len(broker._runtime_diagnostics)==512
    assert 'sandbox-0' not in broker._runtime_diagnostics


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


async def test_generic_native_429_recovers_local_budget_identity_only():
    class StoreStub:
        async def latest_model_activity_error(self,turn_id):return 'model_token_budget_exhausted'
    store=StoreStub()
    assert await reconcile_native_model_error(store,'turn','model_rate_limited','last status 429') == (
        'model_token_budget_exhausted','model_token_budget_exhausted')
    assert await reconcile_native_model_error(store,'turn','model_service_overloaded','upstream 503') == (
        'model_service_overloaded','upstream 503')


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
        await pool.close(preserve_sandbox_keys=preserve)
        assert preserve == {pool.sandbox_key(sandbox)}
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


class UnknownInteractionRuntime(InteractionRuntime):
    async def respond_interaction(self,sandbox,interaction_id,response):return None


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
        await store.begin_sandbox_control(thread.id,owner,"sandbox-control",{
            "provider":"fake","sandbox_id":"sandbox-control","allocation_id":"control",
            "profile_key":"a"*63,"revision":"revision-1"},"nonce",
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


async def test_native_input_completion_receipt_survives_later_input_failure(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'input-receipts.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9')
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    catalog=HarnessCatalog();broker=Broker(settings,store,objects,pool,Runtime(),catalog);owner='a'*64
    try:
        profile=catalog.resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Input receipts'),profile,owner)
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='start',
            idempotency_key='input-receipts-turn'),profile,owner)
        claim=await store.claim_turn(turn.id,broker.instance_id,30);broker.leases[turn.id]=claim.recovery_generation
        execution_id=turn.id+':'+turn.id
        await store.start_runtime_execution(turn.id,execution_id,broker.instance_id,claim.recovery_generation)
        first,_=await store.create_turn_input(turn.id,TurnInputCreate(message='first',
            idempotency_key='input-receipt-first'),owner)
        second,_=await store.create_turn_input(turn.id,TurnInputCreate(message='second',
            idempotency_key='input-receipt-second'),owner)
        await broker._record_runtime_event(thread.id,turn.id,RuntimeEvent(
            kind=EventKind.STEP_FINISHED,payload={'name':'Input completed',
                'input_completed':True,'input_ids':[first.id],'is_error':True},
            runtime_sequence=1),execution_id)
        assert (await store.get_turn_input(first.id)).status=='applied'
        await store.set_input_status(first.id,'failed',execution_id=execution_id,
            lease_owner=broker.instance_id,recovery_generation=claim.recovery_generation)
        await store.set_input_status(second.id,'failed',execution_id=execution_id,
            lease_owner=broker.instance_id,recovery_generation=claim.recovery_generation)
        assert (await store.get_turn_input(first.id)).status=='applied'
        assert (await store.get_turn_input(second.id)).status=='failed'
    finally:
        await broker.close();await pool.close();await store.close()


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
        sandbox_handle_json=sandbox_handle_json(sandbox),lease_owner="dead-broker",
        recovery_generation=claimed.recovery_generation)
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
    from sandbox_harness.domain import Attachment
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "append.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    class InspectingDeferredInput(AppendedInputRuntime):
        async def run(self,sandbox,**kwargs):
            if self.messages:
                inputs=await store.turn_input_artifacts(turn.id,owner)
                assert len(inputs)==1 and inputs[0]['project_path']=='inbox/deferred.png'
            async for value in super().run(sandbox,**kwargs):yield value
    runtime = InspectingDeferredInput();runtime.project_files=[]
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, runtime, catalog)
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
        reference,digest=objects.put(b'deferred image')
        attachment=Attachment(name='deferred.png',object_ref=reference,sha256=digest,
            size=len(b'deferred image'),mime='image/png')
        appended, _ = await store.create_turn_input(turn.id,
            TurnInputCreate(message="second", idempotency_key="append-second-key",
                attachments=[attachment]), owner)
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
    from sandbox_harness.domain import Attachment
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "live.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9", session_keepalive_seconds=0)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    backend = FakeSandboxBackend(); pool = SandboxPool(backend, {h: "" for h in Harness}, {h: 0 for h in Harness})
    class InspectingLiveInput(LiveInputRuntime):
        async def append_input(self,sandbox,**kwargs):
            inputs=await store.turn_input_artifacts(turn.id,owner)
            assert len(inputs)==1 and inputs[0]['project_path']=='inbox/replacement.png'
            assert inputs[0]['artifact'].sha256==attachment.sha256
            assert (await store.get_turn_input(kwargs['input_id'])).status=='pending'
            self.bound_artifact_id=inputs[0]['artifact'].id
            return await super().append_input(sandbox,**kwargs)
    runtime = InspectingLiveInput();runtime.project_files=[]
    catalog = HarnessCatalog(); broker = Broker(settings, store, objects, pool, runtime, catalog)
    owner = "a" * 64
    try:
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Live"), profile, owner)
        turn = await broker.submit(thread.id, TurnCreate(message="first", idempotency_key="live-first-key"))
        for _ in range(100):
            if (await store.get_turn(turn.id)).state == TurnState.RUNNING: break
            await asyncio.sleep(.01)
        reference,digest=objects.put(b'replacement image')
        attachment=Attachment(name='replacement.png',object_ref=reference,sha256=digest,
            size=len(b'replacement image'),mime='image/png')
        appended, _ = await store.create_turn_input(turn.id,
            TurnInputCreate(message="steer", idempotency_key="live-steer-key",
                attachments=[attachment]), owner)
        assert await broker.try_deliver_input(appended)
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks: break
            await asyncio.sleep(.01)
        assert current.state == TurnState.COMPLETED and runtime.live_inputs == ["steer"]
        recorded = (await store.list_turn_inputs(turn.id))[0]
        assert recorded.status == "applied" and recorded.delivery_mode == "native_stream"
        inputs=await store.turn_input_artifacts(turn.id,owner)
        assert len(inputs)==1 and inputs[0]['artifact'].id==runtime.bound_artifact_id
    finally:
        runtime.release.set()
        await broker.close()
        await pool.close(); await store.close()


@pytest.mark.parametrize('recovery_mode',['journal','completed_receipt'])
async def test_broker_reattaches_expired_running_turn_and_replays_runtime_journal(tmp_path,recovery_mode):
    from sandbox_harness.profiles import ProviderRelease
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "recover.db"),
        backend="fake", gateway_signing_key="k" * 32, gateway_url="http://127.0.0.1:9",
        session_keepalive_seconds=0, recovery_scan_seconds=300)
    store = Store(settings.database_url); await store.initialize(); objects = ObjectStore(tmp_path / "objects")
    class DurableIdBackend(FakeSandboxBackend):
        def __init__(self):super().__init__();self.get_calls=[]
        async def get(self,sandbox_id):self.get_calls.append(sandbox_id);return await super().get(sandbox_id)
        async def find(self,metadata):
            raise AssertionError("durable sandbox ID recovery must not query provider metadata")
    frozen_image="registry/runtime@sha256:"+"1"*64
    frozen_revision='immutable-release-before-restart'
    frozen_bundle="2"*64;frozen_code="3"*64
    frozen_resolver=SandboxProfileResolver(provider="fake",
        runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest=frozen_bundle,
        runtime_code_bundle_digest=frozen_code)
    current_resolver=SandboxProfileResolver(provider="fake",
        runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="4"*64,
        runtime_code_bundle_digest="5"*64,releases={Harness.CLAUDE:ProviderRelease(
            ref=frozen_image,revision='different-current-release',artifact_digest='1'*64)})
    backend = DurableIdBackend(); pool = SandboxPool(backend,
        {h:frozen_image for h in Harness},{h:0 for h in Harness},profile_resolver=current_resolver)
    catalog = HarnessCatalog(); owner = "a" * 64
    profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
        reasoning_level="standard", title="Recover"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id,
        TurnCreate(message="survive restart", idempotency_key="recover-running-key"), profile, owner,
        runtime_image_digest=frozen_image,runtime_bundle_digest=frozen_bundle,
        runtime_code_bundle_digest=frozen_code)
    old = await store.claim_turn(turn.id, "dead-broker", 30)
    execution_id = turn.id + ":" + turn.id
    await store.start_runtime_execution(turn.id, execution_id, "dead-broker", old.recovery_generation)
    sandbox = await backend.create(SandboxCreateRequest(harness=Harness.CLAUDE,
        profile=frozen_resolver.resolve(Harness.CLAUDE,frozen_image,revision=frozen_revision),environment={},
        ttl_seconds=3600,allocation_id=turn.id))
    await store.set_turn_state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id,
        sandbox_handle_json=sandbox_handle_json(sandbox),lease_owner="dead-broker",
        recovery_generation=old.recovery_generation)
    persisted=await store.get_turn(turn.id)
    assert SandboxHandle.from_mapping(json.loads(persisted.sandbox_handle_json))==sandbox.handle
    appended,_=await store.create_turn_input(turn.id,
        TurnInputCreate(message='keep this completed input',idempotency_key='recovered-input-key'),owner)
    await store.set_input_status(appended.id,'delivered','native_stream',execution_id=execution_id,
        lease_owner='dead-broker',recovery_generation=old.recovery_generation)
    if recovery_mode=='completed_receipt':
        await store.update_runtime_sequence(turn.id,2,'dead-broker',old.recovery_generation)
    await store.release_turn_lease(turn.id, "dead-broker", old.recovery_generation)
    class InputReceiptRuntime(RecoverableRuntime):
        run_calls=0;resume_calls=0
        async def execution_status(self,sandbox,identity):
            value=await super().execution_status(sandbox,identity)
            value['result']['input_ids']=[appended.id]
            return value
        async def resume(self,sandbox,identity,after_sequence):
            self.resume_calls+=1
            assert identity==execution_id
            async for value in super().resume(sandbox,identity,after_sequence):
                yield (value.model_copy(update={'input_ids':[appended.id]})
                    if isinstance(value,RuntimeResult) else value)
        async def run(self,*args,**kwargs):
            self.run_calls+=1
            raise AssertionError('Recovery must not resubmit an accepted execution')
            yield
    runtime=InputReceiptRuntime()
    broker = Broker(settings, store, objects, pool, runtime, catalog)
    try:
        await broker.start()
        for _ in range(200):
            current = await store.get_turn(turn.id)
            if current.state in {TurnState.COMPLETED, TurnState.FAILED} and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.01)
        assert current.state == TurnState.COMPLETED
        assert current.runtime_sequence == 2
        if recovery_mode=='journal':
            assert "recovered" in [item.summary for item in await store.items_after(thread.id)]
        assert (await store.get_turn_input(appended.id)).status=='applied'
        assert current.runtime_execution_id==execution_id and runtime.run_calls==0
        assert runtime.resume_calls==(1 if recovery_mode=='journal' else 0)
        assert any(event.payload.get("code") == "turn_runtime_reattached"
            for event in await store.events_after(turn.id, 0))
        assert backend.get_calls and {value.sandbox_id for value in backend.get_calls}=={sandbox.id}
        assert sandbox.id not in backend.items
    finally:
        await broker.close(); await pool.close(); await store.close()


async def test_recovery_waits_for_transient_runtime_health_then_reattaches(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'health-recover.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9',
        session_keepalive_seconds=0,recovery_scan_seconds=300)
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    catalog=HarnessCatalog();profile=catalog.resolve('codex','gpt-5.6','standard');owner='a'*64
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title='Health recovery'),profile,owner)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='recover',
        idempotency_key='health-recover-turn'),profile,owner)
    old=await store.claim_turn(turn.id,'dead',30);execution_id=turn.id+':'+turn.id
    await store.start_runtime_execution(turn.id,execution_id,'dead',old.recovery_generation)
    sandbox=await backend.create(Harness.CODEX,'',{'sandbox-harness.io/turn':turn.id,
        'sandbox-harness.io/thread':thread.id,'sandbox-harness.io/harness':'codex'}, {})
    sandbox.health_state='unhealthy';sandbox.health_error='runtime_health_unavailable'
    await store.set_turn_state(turn.id,TurnState.RUNNING,sandbox_id=sandbox.id,
        sandbox_handle_json=sandbox_handle_json(sandbox),lease_owner='dead',
        recovery_generation=old.recovery_generation)
    await store.release_turn_lease(turn.id,'dead',old.recovery_generation)
    broker=Broker(settings,store,objects,pool,RecoverableRuntime(),catalog)
    try:
        await broker.reconcile()
        assert (await store.get_turn(turn.id)).state==TurnState.RUNNING
        assert sandbox.id in backend.items
        assert any(value.payload.get('code')=='turn_runtime_not_ready'
            for value in await store.events_after(turn.id,0))
        sandbox.health_state='ready';sandbox.health_error=''
        await broker.reconcile()
        for _ in range(200):
            current=await store.get_turn(turn.id)
            if current.state==TurnState.COMPLETED and turn.id not in broker.tasks:break
            await asyncio.sleep(.01)
        assert current.state==TurnState.COMPLETED and sandbox.id not in backend.items
    finally:await broker.close();await pool.close();await store.close()


async def test_reconciliation_lease_loss_cancels_stalled_external_work_before_takeover(tmp_path):
    from datetime import timedelta
    from sqlalchemy import update
    from sandbox_harness.store import TurnRow
    class Backend(FakeSandboxBackend):
        entered=asyncio.Event();calls=0;blocked_task=None;old_task_deleted=False
        async def get(self,sandbox_id):
            self.calls+=1
            if self.calls==1:
                self.blocked_task=asyncio.current_task();self.entered.set()
                await asyncio.Event().wait()
            return await super().get(sandbox_id)
        async def delete(self,sandbox):
            if asyncio.current_task() is self.blocked_task:self.old_task_deleted=True
            await super().delete(sandbox)
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'reconcile-fence.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9',
        session_keepalive_seconds=0,recovery_scan_seconds=300).model_copy(
            update={'turn_heartbeat_seconds':.02,'turn_lease_seconds':1})
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=Backend();old_pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    new_pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    catalog=HarnessCatalog();profile=catalog.resolve('codex','gpt-5.6','standard');owner='a'*64
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title='Reconcile fence'),profile,owner)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='recover',
        idempotency_key='reconcile-fence-turn'),profile,owner)
    seed_claim=await store.claim_turn(turn.id,'dead',1);execution_id=turn.id+':'+turn.id
    await store.start_runtime_execution(turn.id,execution_id,'dead',seed_claim.recovery_generation)
    sandbox=await backend.create(Harness.CODEX,'',{'sandbox-harness.io/turn':turn.id,
        'sandbox-harness.io/thread':thread.id,'sandbox-harness.io/harness':'codex'}, {})
    await store.set_turn_state(turn.id,TurnState.RUNNING,sandbox_id=sandbox.id,
        sandbox_handle_json=sandbox_handle_json(sandbox),lease_owner='dead',
        recovery_generation=seed_claim.recovery_generation)
    await store.release_turn_lease(turn.id,'dead',seed_claim.recovery_generation)
    old=Broker(settings,store,objects,old_pool,RecoverableRuntime(),catalog)
    new=Broker(settings,store,objects,new_pool,RecoverableRuntime(),catalog)
    try:
        stalled=asyncio.create_task(old.reconcile());await backend.entered.wait()
        async with store.connections.begin() as db:
            await db.execute(update(TurnRow).where(TurnRow.id==turn.id).values(
                lease_expires_at=now()-timedelta(seconds=1)))
        await new.reconcile();await asyncio.wait_for(stalled,2)
        for _ in range(200):
            current=await store.get_turn(turn.id)
            if current.state==TurnState.COMPLETED and turn.id not in new.tasks:break
            await asyncio.sleep(.01)
        assert current.state==TurnState.COMPLETED
        assert not backend.old_task_deleted
        assert sandbox.id not in backend.items
        assert turn.id not in old.reconciliation_tasks and turn.id not in old.leases
    finally:
        await old.close();await new.close();await old_pool.close();await new_pool.close();await store.close()


async def test_graceful_shutdown_releases_stalled_reconciliation_lease_immediately(tmp_path):
    class Backend(FakeSandboxBackend):
        entered=asyncio.Event();calls=0
        async def get(self,sandbox_id):
            self.calls+=1
            if self.calls==1:self.entered.set();await asyncio.Event().wait()
            return await super().get(sandbox_id)
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'reconcile-shutdown.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9',
        session_keepalive_seconds=0,recovery_scan_seconds=300,turn_lease_seconds=30)
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=Backend();old_pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    new_pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    catalog=HarnessCatalog();profile=catalog.resolve('codex','gpt-5.6','standard');owner='a'*64
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title='Reconcile shutdown'),profile,owner)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='recover',
        idempotency_key='reconcile-shutdown-turn'),profile,owner)
    seed_claim=await store.claim_turn(turn.id,'dead',30);execution_id=turn.id+':'+turn.id
    await store.start_runtime_execution(turn.id,execution_id,'dead',seed_claim.recovery_generation)
    sandbox=await backend.create(Harness.CODEX,'',{'sandbox-harness.io/turn':turn.id,
        'sandbox-harness.io/thread':thread.id,'sandbox-harness.io/harness':'codex'}, {})
    await store.set_turn_state(turn.id,TurnState.RUNNING,sandbox_id=sandbox.id,
        sandbox_handle_json=sandbox_handle_json(sandbox),lease_owner='dead',
        recovery_generation=seed_claim.recovery_generation)
    await store.release_turn_lease(turn.id,'dead',seed_claim.recovery_generation)
    old=Broker(settings,store,objects,old_pool,RecoverableRuntime(),catalog)
    new=Broker(settings,store,objects,new_pool,RecoverableRuntime(),catalog)
    try:
        stalled=asyncio.create_task(old.reconcile());await backend.entered.wait()
        await old.close();await asyncio.wait_for(stalled,1)
        released=await store.get_turn(turn.id)
        assert released.lease_owner=='' and released.lease_expires_at is None
        await new.reconcile()
        for _ in range(200):
            current=await store.get_turn(turn.id)
            if current.state==TurnState.COMPLETED and turn.id not in new.tasks:break
            await asyncio.sleep(.01)
        assert current.state==TurnState.COMPLETED and sandbox.id not in backend.items
    finally:
        await old.close();await new.close();await old_pool.close();await new_pool.close();await store.close()


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
            if current.state == TurnState.CLEANING and turn.id not in broker.tasks:
                break
            await asyncio.sleep(.02)
        events = await store.events_after(turn.id, 0)
        assert current.pending_final_state==TurnState.FAILED.value
        assert current.error_code == "RuntimeStreamInterrupted"
        assert any(event.payload.get("code") == "cleanup_pending" for event in events)
        assert not any(event.payload.get("code") == "cleanup_failed" for event in events)
        assert (await store.get_thread(thread.id)).checkpoint_ref
        assert (await store.artifacts(turn.id))[0].path == "result.txt"
        monkeypatch.setattr(backend, "delete", original_delete)
        await pool.retry_deletions();await broker.reconcile()
        current=await store.get_turn(turn.id)
        errors = [item for item in await store.items_after(thread.id) if item.type.value == "error"]
        assert current.state==TurnState.FAILED and current.error_code=="RuntimeStreamInterrupted"
        assert len(errors)==1 and "连接中断" in errors[0].summary
    finally:
        monkeypatch.setattr(backend, "delete", original_delete)
        await pool.retry_deletions()
        await broker.close(); await pool.close(); await store.close()


async def test_success_cleanup_failure_keeps_durable_completed_intent_until_recovery(tmp_path,monkeypatch):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'success-cleanup.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9',
        session_keepalive_seconds=0)
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    broker=Broker(settings,store,objects,pool,Runtime(),HarnessCatalog());original=backend.delete
    async def fail_delete(sandbox):raise ConnectionError('delete unavailable')
    monkeypatch.setattr(backend,'delete',fail_delete)
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Success cleanup'),profile,'a'*64)
        turn=await broker.submit(thread.id,TurnCreate(message='complete',
            idempotency_key='success-cleanup-turn'))
        for _ in range(200):
            current=await store.get_turn(turn.id)
            if current.state==TurnState.CLEANING and turn.id not in broker.tasks:break
            await asyncio.sleep(.01)
        assert current.pending_final_state==TurnState.COMPLETED.value
        assert any(value.payload.get('code')=='cleanup_pending'
            for value in await store.events_after(turn.id,0))
        monkeypatch.setattr(backend,'delete',original)
        await pool.retry_deletions();await broker.reconcile();current=await store.get_turn(turn.id)
        assert current.state==TurnState.COMPLETED and current.error_code==''
    finally:
        monkeypatch.setattr(backend,'delete',original)
        await broker.close();await pool.close();await store.close()


async def test_cleanup_retry_retires_stale_same_pool_attachment_and_releases_capacity(tmp_path):
    class Backend(FakeSandboxBackend):
        failed=False;fail_id=''
        async def delete(self,sandbox):
            if sandbox.id==self.fail_id and not self.failed:
                self.failed=True;raise RuntimeError('first duplicate delete failed')
            await super().delete(sandbox)
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'cleanup-capacity.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9',
        session_keepalive_seconds=0,recovery_scan_seconds=300)
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=Backend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness},max_sandboxes=1)
    catalog=HarnessCatalog();profile=catalog.resolve('codex','gpt-5.6','standard');owner='a'*64
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title='Cleanup capacity'),profile,owner)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='done',
        idempotency_key='cleanup-capacity-turn'),profile,owner)
    main=await pool.acquire(thread_id=thread.id,turn_id=turn.id,harness=Harness.CODEX,environment={})
    claim=await store.claim_turn(turn.id,'dead',30)
    await store.set_turn_state(turn.id,TurnState.STARTING,sandbox_id=main.id,
        sandbox_handle_json=sandbox_handle_json(main),lease_owner='dead',
        recovery_generation=claim.recovery_generation)
    await store.begin_turn_cleanup(turn.id,TurnState.COMPLETED,error_code='',error_detail='',
        owner='dead',generation=claim.recovery_generation)
    await store.release_turn_lease(turn.id,'dead',claim.recovery_generation)
    backend.fail_id=main.id
    broker=Broker(settings,store,objects,pool,Runtime(),catalog)
    try:
        await broker.reconcile()
        assert (await store.get_turn(turn.id)).state==TurnState.CLEANING
        assert main.id in await pool.managed_sandbox_ids()
        await broker.reconcile()
        assert (await store.get_turn(turn.id)).state==TurnState.COMPLETED
        assert not backend.items and not await pool.managed_sandbox_ids()
        replacement=await asyncio.wait_for(pool.acquire(thread_id='other',turn_id='other',
            harness=Harness.CODEX,environment={}),1)
        await pool.release(replacement,keep_attached=False)
    finally:await broker.close();await pool.close();await store.close()


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
        sandbox_handle_json=sandbox_handle_json(sandbox),lease_owner="dead",
        recovery_generation=old.recovery_generation)
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


async def test_interaction_unknown_delivery_never_returns_to_pending_or_false_responded(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'interaction-unknown.db'),
        backend='fake',gateway_signing_key='k'*32,gateway_url='http://127.0.0.1:9',
        session_keepalive_seconds=0)
    store=Store(settings.database_url);await store.initialize();objects=ObjectStore(tmp_path/'objects')
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    runtime=UnknownInteractionRuntime();catalog=HarnessCatalog();broker=Broker(
        settings,store,objects,pool,runtime,catalog);owner='a'*64
    try:
        profile=catalog.resolve('claude','claude-4.6-sonnet','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CLAUDE,model=profile.model,
            reasoning_level='standard',title='Interaction unknown'),profile,owner)
        turn=await broker.submit(thread.id,TurnCreate(message='ask',
            idempotency_key='interaction-unknown-turn'))
        for _ in range(100):
            if (await store.get_turn(turn.id)).state==TurnState.AWAITING_USER:break
            await asyncio.sleep(.01)
        interaction=(await store.list_interactions(thread.id,owner))[0]
        response=InteractionResponse(idempotency_key='interaction-unknown-response',
            action='answer',answers={'format':'mp4'})
        with pytest.raises(RuntimeError,match='interaction_delivery_unknown'):
            await broker.respond_interaction(interaction.id,owner,response)
        current=(await store.list_interactions(thread.id,owner))[0]
        assert current.status=='responding'
        assert current.response_key=='interaction-unknown-response'
        assert current.response['answers']['format']=='mp4'
        approval=next(item for item in await store.items_after(thread.id)
            if item.type.value=='approval')
        assert approval.status=='in_progress' and 'response' not in approval.payload
        execution_id=(await store.get_turn(turn.id)).runtime_execution_id
        await broker._record_runtime_event(thread.id,turn.id,RuntimeEvent(
            kind=EventKind.STEP_FINISHED,payload={"name":"Interaction applied",
                "interaction_applied":True,
                "interaction_id":interaction.runtime_interaction_id},runtime_sequence=999),
            execution_id)
        reconciled=(await store.list_interactions(thread.id,owner))[0]
        assert reconciled.status=='responded'
        approval=next(item for item in await store.items_after(thread.id)
            if item.type.value=='approval')
        assert approval.status=='completed' and approval.payload['response']['answers']['format']=='mp4'
    finally:
        runtime.release.set();await broker.close();await pool.close();await store.close()


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
        for sandbox in await pool.list_owned():
            await backend.delete(sandbox)
        await broker.close(); await pool.close(); await store.close()


@pytest.mark.parametrize('native_code',['other','harness_failed'])
async def test_local_model_ingress_error_uses_durable_evidence(native_code):
    class StoreStub:
        async def latest_model_activity_error(self,turn_id):return 'model_request_too_large'
    assert await reconcile_native_model_error(StoreStub(),'turn',native_code,'native failed')==(
        'model_request_too_large','model_request_too_large')


async def test_genuine_upstream_error_is_not_rewritten_as_local_ingress_failure():
    class StoreStub:
        async def latest_model_activity_error(self,turn_id):return 'model_request_too_large'
    assert await reconcile_native_model_error(StoreStub(),'turn','model_service_overloaded','upstream 503')==(
        'model_service_overloaded','upstream 503')


async def test_orphan_reaper_keeps_late_visible_durable_allocation_without_handle():
    sandbox=SimpleNamespace(id='late-sandbox',handle=SandboxHandle(provider='fake',
        sandbox_id='late-sandbox',allocation_id='pending-allocation',profile_key='a'*63,
        revision='revision'),metadata={})
    deleted=[]
    async def empty():return set()
    async def retry():pass
    async def owned():return [sandbox]
    async def remove(value):deleted.append(value.id)
    async def allocations():return {('fake','pending-allocation')}
    pool=SimpleNamespace(backend=SimpleNamespace(delete=remove),retry_deletions=retry,
        managed_sandbox_keys=empty,list_owned=owned,
        sandbox_key=lambda value:(value.handle.provider,value.handle.sandbox_id))
    store=SimpleNamespace(active_sandbox_keys=empty,active_sandbox_allocations=allocations)
    await Broker.cleanup_startup_orphans(SimpleNamespace(pool=pool,store=store,active_sandboxes={}))
    assert deleted==[]
