import asyncio
import pytest
from sandbox_harness.broker import Broker
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness, TurnState, TurnInputCreate
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.store import Store
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
