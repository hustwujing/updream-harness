import asyncio
import pytest
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness, ThreadCreate, TurnCreate, TurnState
from sandbox_harness.store import Store

async def seed(store):
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,title='review'),profile,'owner')
    first,_,_=await store.create_turn(thread.id,TurnCreate(message='first',idempotency_key='first-review-key'),profile,'owner')
    second,_,_=await store.create_turn(thread.id,TurnCreate(message='second',idempotency_key='second-review-key',delivery_mode='queue'),profile,'owner')
    return thread,first,second

async def test_queued_turn_cannot_be_claimed_by_another_broker(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'queue.db'));await store.initialize()
    try:
        thread,first,second=await seed(store)
        assert await store.claim_turn(first.id,'one',30)
        assert await store.claim_turn(second.id,'two',30) is None
        assert [t.id for t in await store.recoverable_turns()]==[]
        await store.set_turn_state(first.id,TurnState.COMPLETED)
        assert [t.id for t in await store.recoverable_turns()]==[second.id]
        assert await store.claim_turn(second.id,'two',30)
    finally:await store.close()

async def test_parallel_sqlite_lease_claim_has_one_winner(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'race.db'));await store.initialize()
    try:
        _,first,_=await seed(store)
        claims=await asyncio.gather(*(store.claim_turn(first.id,f'worker-{i}',30) for i in range(12)))
        assert sum(c is not None for c in claims)==1
    finally:await store.close()

async def test_old_checkpoint_writer_is_fenced(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'checkpoint.db'));await store.initialize()
    try:
        thread,first,_=await seed(store)
        old=await store.claim_turn(first.id,'old',30)
        await store.release_turn_lease(first.id,'old',old.recovery_generation)
        new=await store.claim_turn(first.id,'new',30)
        await store.update_thread_checkpoint(thread.id,'sha256:new','native-new',turn_id=first.id,
            lease_owner='new',recovery_generation=new.recovery_generation)
        with pytest.raises(RuntimeError,match='stale_turn_lease'):
            await store.update_thread_checkpoint(thread.id,'sha256:old','native-old',turn_id=first.id,
                lease_owner='old',recovery_generation=old.recovery_generation)
        assert (await store.get_thread(thread.id)).checkpoint_ref=='sha256:new'
    finally:await store.close()

async def test_runtime_transcript_and_receipt_are_atomic_and_idempotent(tmp_path,monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession
    from sandbox_harness.domain import RuntimeEvent,EventKind
    from sandbox_harness.store import EventRow
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'events.db'));await store.initialize()
    try:
        thread,turn,_=await seed(store)
        value=RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':'exactly once'},runtime_sequence=1)
        original=AsyncSession.add
        def fail_receipt(self,row,*args,**kwargs):
            if isinstance(row,EventRow):raise RuntimeError('crash before receipt')
            return original(self,row,*args,**kwargs)
        with monkeypatch.context() as patch:
            patch.setattr(AsyncSession,'add',fail_receipt)
            with pytest.raises(RuntimeError,match='crash before receipt'):
                await store.persist_runtime_event(thread.id,turn.id,value,'execution')
        assert not any(i.type=='agent_message' for i in await store.items_after(thread.id))
        await store.persist_runtime_event(thread.id,turn.id,value,'execution')
        await store.persist_runtime_event(thread.id,turn.id,value,'execution')
        messages=[i.summary for i in await store.items_after(thread.id) if i.type=='agent_message']
        assert messages==['exactly once']
        assert len(await store.events_after(turn.id,0))==1
    finally:await store.close()

async def test_late_delivery_error_cannot_undo_a_completed_input(tmp_path):
    from sandbox_harness.domain import TurnInputCreate
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'input.db'));await store.initialize()
    try:
        _,turn,_=await seed(store)
        inp,_=await store.create_turn_input(turn.id,TurnInputCreate(message='more',idempotency_key='input-race-key'),'owner')
        await store.set_input_status(inp.id,'applied')
        await store.set_input_status(inp.id,'unknown','delivery_unconfirmed')
        assert (await store.get_turn_input(inp.id)).status=='applied'
    finally:await store.close()

async def test_slow_cleanup_keeps_heartbeat_running(tmp_path,monkeypatch):
    from sandbox_harness.broker import Broker
    from sandbox_harness.config import Settings
    from sandbox_harness.objects import ObjectStore
    from sandbox_harness.pool import SandboxPool
    from sandbox_harness.sandbox import FakeSandboxBackend
    from test_broker import Runtime
    class SlowDelete(FakeSandboxBackend):
        deleting=False
        async def delete(self,sandbox):
            self.deleting=True
            await asyncio.sleep(.15)
            await super().delete(sandbox)
            self.deleting=False
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'slow.db'));await store.initialize()
    backend=SlowDelete();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    settings=Settings(backend='fake',data_root=tmp_path,gateway_url='http://127.0.0.1:9').model_copy(update={'turn_heartbeat_seconds':.02})
    broker=Broker(settings,store,ObjectStore(tmp_path/'objects'),pool,Runtime(),HarnessCatalog())
    renewed=[];original=store.renew_turn_lease
    async def renew(*args):
        if backend.deleting:renewed.append(True)
        return await original(*args)
    monkeypatch.setattr(store,'renew_turn_lease',renew)
    try:
        _,first,_=await seed(store)
        broker._schedule(first.id)
        for _ in range(300):
            if (await store.get_turn(first.id)).state==TurnState.COMPLETED:break
            await asyncio.sleep(.01)
        assert renewed,'heartbeat must run during backend deletion'
        assert (await store.get_turn(first.id)).state==TurnState.COMPLETED
    finally:await broker.close();await pool.close();await store.close()

async def test_old_worker_does_not_cancel_runtime_after_lease_takeover(tmp_path):
    from datetime import timedelta
    from sandbox_harness.domain import now
    from sandbox_harness.store import TurnRow
    from sandbox_harness.broker import Broker
    from sandbox_harness.config import Settings
    from sandbox_harness.objects import ObjectStore
    from sandbox_harness.pool import SandboxPool
    from sandbox_harness.sandbox import FakeSandboxBackend
    from test_broker import AppendedInputRuntime
    class Runtime(AppendedInputRuntime):
        cancelled=0
        async def cancel(self,sandbox):self.cancelled+=1
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'old-worker.db'));await store.initialize()
    backend=FakeSandboxBackend();pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    runtime=Runtime()
    settings=Settings(backend='fake',data_root=tmp_path,gateway_url='http://127.0.0.1:9').model_copy(update={'turn_heartbeat_seconds':100})
    broker=Broker(settings,store,ObjectStore(tmp_path/'objects'),pool,runtime,HarnessCatalog())
    try:
        _,turn,_=await seed(store);broker._schedule(turn.id)
        for _ in range(100):
            if runtime.messages:break
            await asyncio.sleep(.01)
        async with store.connections.begin() as db:
            row=await db.get(TurnRow,turn.id);row.lease_expires_at=now()-timedelta(seconds=1)
        assert await store.claim_turn(turn.id,'new-owner',30)
        task=broker.tasks[turn.id];task.cancel();await task
        assert runtime.cancelled==0
        current=await store.get_turn(turn.id)
        assert current.lease_owner=='new-owner' and current.state==TurnState.RUNNING
        assert backend.items and not broker.tasks and not broker.leases
    finally:
        runtime.release.set()
        await broker.close();await pool.close();await store.close()

async def test_old_artifact_publisher_is_fenced(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'artifact.db'));await store.initialize()
    try:
        thread,turn,_=await seed(store)
        old=await store.claim_turn(turn.id,'old',30)
        await store.release_turn_lease(turn.id,'old',old.recovery_generation)
        await store.claim_turn(turn.id,'new',30)
        with pytest.raises(RuntimeError,match='stale_turn_lease'):
            await store.add_artifact(turn.id,path='file.png',sha256='a'*64,size=1,mime='image/png',object_ref='sha256:'+'a'*64,
                lease_owner='old',recovery_generation=old.recovery_generation)
        with pytest.raises(RuntimeError,match='stale_turn_lease'):
            await store.register_project_file(thread.id,path='artifacts/file.png',source_type='artifact',source_turn_id=turn.id,
                sha256='a'*64,size=1,mime='image/png',object_ref='sha256:'+'a'*64,lease_owner='old',recovery_generation=old.recovery_generation)
        assert not await store.artifacts(turn.id)
        assert not await store.current_project_files(thread.id)
    finally:await store.close()


def test_input_lost_receipt_is_unknown_not_a_new_execution(tmp_path):
    import httpx
    from fastapi.testclient import TestClient
    from sandbox_harness.app import create_app
    from sandbox_harness.config import Settings
    app=create_app(Settings(backend='fake',database_url='sqlite+aiosqlite:///'+str(tmp_path/'lost-reply.db'),
        redis_url='',data_root=tmp_path,object_backend='local',model_catalog_discovery=False))
    app.state.broker._schedule=lambda turn_id:None
    async def lost(value):raise httpx.ReadTimeout('reply lost after possible delivery')
    app.state.broker.try_deliver_input=lost
    with TestClient(app) as c:
        thread=c.post('/api/threads',json={'harness':'codex','model':'gpt-5.6','title':'test'}).json()
        turn=c.post(f"/api/threads/{thread['id']}/turns",json={'message':'first','idempotency_key':'first-request-key'}).json()
        body={'message':'more','idempotency_key':'unknown-input-key'}
        reply=c.post(f"/api/turns/{turn['id']}/inputs",json=body)
        assert reply.status_code==200 and reply.json()['status']=='unknown'
        again=c.post(f"/api/turns/{turn['id']}/inputs",json=body)
        assert again.json()['id']==reply.json()['id']
        assert len(c.get(f"/api/turns/{turn['id']}/inputs").json())==1
