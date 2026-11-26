import asyncio
import json
import pytest
from sandbox_harness.services.api.broker import (Broker)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, TurnState, TurnInputCreate)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
from sandbox_harness.common.db.store import (Store)
from test_second_review import seed
from test_broker import AppendedInputRuntime


async def setup(tmp_path):
    settings = Settings(backend='fake', data_root=tmp_path, gateway_url='http://127.0.0.1:9',
        database_url='sqlite+aiosqlite:///'+str(tmp_path/'db')).model_copy(update={'turn_heartbeat_seconds':.02})
    store=Store(settings.database_url); await store.initialize()
    backend=FakeSandboxBackend()
    pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    class Runtime(AppendedInputRuntime):
        cancelled=0
        async def cancel(self, sandbox): self.cancelled+=1
    runtime=Runtime()
    brokers=[Broker(settings,store,ObjectStore(tmp_path/'objects'),pool,runtime,HarnessCatalog()) for _ in range(2)]
    return store,pool,runtime,brokers


async def wait_state(store, turn_id, state):
    async with asyncio.timeout(8):
        while (await store.get_turn(turn_id)).state != state:
            await asyncio.sleep(.01)


async def test_foreign_broker_stop_reaches_owner_and_blocks_new_input(tmp_path):
    store,pool,runtime,(owner,receiver)=await setup(tmp_path)
    try:
        thread,first,second=await seed(store)
        await store.request_turn_cancel(second.id)
        owner._schedule(first.id)
        await wait_state(store,first.id,TurnState.RUNNING)
        sandbox_id=(await store.get_turn(first.id)).sandbox_id
        await receiver.cancel(first.id)
        saved=await store.get_turn(first.id)
        assert saved.cancel_requested
        with pytest.raises(RuntimeError,match='turn_not_accepting_input'):
            await store.create_turn_input(first.id,TurnInputCreate(message='late',idempotency_key='late-input-key'),'owner')
        await wait_state(store,first.id,TurnState.CANCELLED)
        assert runtime.cancelled == 1
        assert sandbox_id not in pool.backend.items
    finally:
        await owner.close();await receiver.close();await pool.close();await store.close()


async def test_cancel_survives_broker_restart_without_replaying_harness(tmp_path):
    store,pool,runtime,(old,new)=await setup(tmp_path)
    try:
        _,first,second=await seed(store)
        await store.request_turn_cancel(second.id)
        claim=await store.claim_turn(first.id,old.instance_id,30)
        sandbox=await pool.acquire(thread_id=first.thread_id,turn_id=first.id,harness=first.harness,environment={})
        await store.set_turn_state(first.id,TurnState.RUNNING,sandbox_id=sandbox.id,
            sandbox_handle_json=json.dumps(sandbox.handle.to_mapping()),
            lease_owner=old.instance_id,recovery_generation=claim.recovery_generation)
        await store.request_turn_cancel(first.id)
        await store.release_turn_lease(first.id,old.instance_id,claim.recovery_generation)
        await new.reconcile()
        await wait_state(store,first.id,TurnState.CANCELLED)
        assert runtime.messages == [] and runtime.cancelled == 1
        assert sandbox.id not in pool.backend.items
    finally:
        await old.close();await new.close();await pool.close();await store.close()


async def test_cancel_restart_uses_exact_id_and_orphan_reaper_deletes_unowned_sandbox(tmp_path):
    settings=Settings(backend='fake',data_root=tmp_path,gateway_url='http://127.0.0.1:9',
        database_url='sqlite+aiosqlite:///'+str(tmp_path/'fresh-pool.db')).model_copy(
            update={'turn_heartbeat_seconds':.02})
    store=Store(settings.database_url);await store.initialize();backend=FakeSandboxBackend()
    old_pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
    class Runtime(AppendedInputRuntime):
        cancelled=0
        async def cancel(self,sandbox):self.cancelled+=1;return {'cancelled':True}
    runtime=Runtime();old=Broker(settings,store,ObjectStore(tmp_path/'objects'),old_pool,runtime,HarnessCatalog())
    try:
        _,first,_=await seed(store)
        claim=await store.claim_turn(first.id,old.instance_id,30)
        await store.set_turn_state(first.id,TurnState.RUNNING,
            lease_owner=old.instance_id,recovery_generation=claim.recovery_generation)
        main=await old_pool.acquire(thread_id=first.thread_id,turn_id=first.id,
            harness=first.harness,environment={})
        await store.set_turn_state(first.id,TurnState.RUNNING,sandbox_id=main.id,
            sandbox_handle_json=json.dumps(main.handle.to_mapping()),
            lease_owner=old.instance_id,recovery_generation=claim.recovery_generation)
        duplicate=await backend.create(first.harness,'',{
            'sandbox-harness.io/thread':first.thread_id,
            'sandbox-harness.io/turn':first.id,
            'sandbox-harness.io/harness':first.harness.value}, {})
        duplicate.health_state='failed';duplicate.health_error='opensandbox_failed'
        await store.request_turn_cancel(first.id)
        await store.release_turn_lease(first.id,old.instance_id,claim.recovery_generation)
        await old_pool.close(preserve_sandbox_ids={main.id,duplicate.id})
        new_pool=SandboxPool(backend,{h:'' for h in Harness},{h:0 for h in Harness})
        replacement=Broker(settings,store,ObjectStore(tmp_path/'objects'),new_pool,runtime,HarnessCatalog())
        try:
            await replacement.reconcile();await wait_state(store,first.id,TurnState.CANCELLED)
            assert 'first' not in runtime.messages and runtime.cancelled==1
            assert main.id not in backend.items and duplicate.id in backend.items
            assert not await new_pool.managed_sandbox_ids()
            await replacement.cleanup_startup_orphans()
            assert duplicate.id not in backend.items
        finally:
            await replacement.close();await new_pool.close()
    finally:
        await old.close();await store.close()


async def test_queued_stop_is_atomic_with_claim_and_completion_cannot_override_stop(tmp_path):
    store,pool,runtime,(one,two)=await setup(tmp_path)
    try:
        _,first,second=await seed(store)
        pending,_=await store.create_turn_input(second.id,TurnInputCreate(message='extra',idempotency_key='queue-input-key'),'owner')
        assert (await store.request_turn_cancel(second.id)).state == TurnState.CANCELLED
        assert (await store.get_turn_input(pending.id)).status=='failed'
        assert await store.claim_turn(second.id,'worker',30) is None
        claim=await store.claim_turn(first.id,'worker',30)
        await store.request_turn_cancel(first.id)
        done=await store.set_turn_state(first.id,TurnState.COMPLETED,lease_owner='worker',recovery_generation=claim.recovery_generation)
        assert done.state == TurnState.CANCELLED
        failed=await store.set_turn_state(first.id,TurnState.FAILED,error_code='cleanup_failed')
        assert failed.state == TurnState.FAILED and failed.error_code=='cleanup_failed'
        with pytest.raises(RuntimeError,match='turn_already_terminal'):
            await store.request_turn_cancel(first.id)
    finally:
        await one.close();await two.close();await pool.close();await store.close()


async def test_heartbeat_cancellation_finishes_inflight_database_read(tmp_path,monkeypatch):
    import aiosqlite
    store,pool,_,(owner,other)=await setup(tmp_path)
    entered=asyncio.Event();release=asyncio.Event();task=None
    execute=aiosqlite.Cursor.execute
    async def blocked(cursor,sql,parameters=None):
        result=await execute(cursor,sql,parameters)
        if sql.lstrip().startswith('SELECT') and 'FROM turns' in sql:
            entered.set();await release.wait()
        return result
    try:
        _,turn,_=await seed(store)
        claim=await store.claim_turn(turn.id,owner.instance_id,30)
        monkeypatch.setattr(aiosqlite.Cursor,'execute',blocked)
        task=asyncio.create_task(owner._heartbeat(turn.id,claim.recovery_generation))
        await asyncio.wait_for(entered.wait(),2)
        task.cancel();await asyncio.sleep(.02)
        assert not task.done(), 'Cancellation cut through an active database read'
        task.cancel();await asyncio.sleep(.02)
        assert not task.done(), 'Repeated cancellation cut through the protected read'
        release.set()
        with pytest.raises(asyncio.CancelledError):await task
        await store.release_turn_lease(turn.id,owner.instance_id,claim.recovery_generation)
    finally:
        release.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await owner.close();await other.close();await pool.close();await store.close()


@pytest.mark.parametrize('signal',['heartbeat','api_cancel'])
async def test_cancel_handler_is_not_cancelled_again(tmp_path,monkeypatch,signal):
    store,pool,_,(owner,other)=await setup(tmp_path)
    entered=asyncio.Event();release=asyncio.Event();task=None
    owns=store.owns_turn_lease
    async def blocked(*args,**kwargs):
        value=await owns(*args,**kwargs)
        entered.set();await release.wait()
        return value
    try:
        _,turn,second=await seed(store)
        await store.request_turn_cancel(second.id)
        claim=await store.claim_turn(turn.id,'previous-worker',30)
        await store.request_turn_cancel(turn.id)
        await store.release_turn_lease(turn.id,'previous-worker',claim.recovery_generation)
        monkeypatch.setattr(store,'owns_turn_lease',blocked)
        owner._schedule(turn.id);task=owner.tasks[turn.id]
        await asyncio.wait_for(entered.wait(),2)
        if signal=='api_cancel':await owner.cancel(turn.id)
        await asyncio.sleep(.08)
        assert task.cancelling()==0, 'Heartbeat interrupted an existing cancellation handler'
        release.set();await task
        assert (await store.get_turn(turn.id)).state==TurnState.CANCELLED
    finally:
        release.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await owner.close();await other.close();await pool.close();await store.close()


@pytest.mark.parametrize('stage',['claim_turn','get_turn','get_thread'])
async def test_cancelled_startup_settles_database_and_releases_claim(tmp_path,monkeypatch,stage):
    store,pool,runtime,(owner,other)=await setup(tmp_path)
    entered=asyncio.Event();release=asyncio.Event();task=None
    original=getattr(store,stage)
    async def blocked(*args,**kwargs):
        result=await original(*args,**kwargs)
        entered.set();await release.wait()
        return result
    try:
        _,turn,_=await seed(store)
        monkeypatch.setattr(store,stage,blocked)
        owner._schedule(turn.id);task=owner.tasks[turn.id]
        await asyncio.wait_for(entered.wait(),2)
        task.cancel();await asyncio.sleep(.02)
        assert not task.done(), 'Startup database call was abandoned'
        release.set()
        with pytest.raises(asyncio.CancelledError):await task
        assert not owner.leases and not owner.heartbeats and not owner.tasks
        assert not await store.owns_turn_lease(turn.id,owner.instance_id,1)
        assert runtime.messages==[]
    finally:
        release.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await owner.close();await other.close();await pool.close();await store.close()


async def test_repeated_cancel_during_startup_release_forgets_completed_task(tmp_path,monkeypatch):
    store,pool,_,(owner,other)=await setup(tmp_path)
    reading=asyncio.Event();finish_read=asyncio.Event()
    releasing=asyncio.Event();finish_release=asyncio.Event();task=None
    read=store.get_turn;release=store.release_turn_lease
    async def blocked_read(*args,**kwargs):
        value=await read(*args,**kwargs);reading.set();await finish_read.wait();return value
    async def blocked_release(*args,**kwargs):
        await release(*args,**kwargs);releasing.set();await finish_release.wait()
    try:
        _,turn,_=await seed(store)
        monkeypatch.setattr(store,'get_turn',blocked_read)
        monkeypatch.setattr(store,'release_turn_lease',blocked_release)
        owner._schedule(turn.id);task=owner.tasks[turn.id]
        await asyncio.wait_for(reading.wait(),2)
        task.cancel();finish_read.set()
        await asyncio.wait_for(releasing.wait(),2)
        task.cancel();await asyncio.sleep(.02);finish_release.set()
        with pytest.raises(asyncio.CancelledError):await task
        assert not owner.tasks and not owner.leases and not owner.heartbeats
    finally:
        finish_read.set();finish_release.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await owner.close();await other.close();await pool.close();await store.close()
