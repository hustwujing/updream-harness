import asyncio
from dataclasses import asdict, replace
from datetime import timedelta

import pytest

from test_allocation_dispatch import fixture
from sandbox_harness.allocation_dispatch import AllocationClient, AllocationService
from sandbox_harness.domain import now,Harness
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.sandbox_contracts import SandboxCreateOutcomeUnknown
from sandbox_harness.store import SandboxAllocationRow


async def issued_allocation(store, context, request, provider):
    from sandbox_harness.harness_job import HarnessJobAdapter
    from sandbox_harness.scheduler import Scheduler
    request=replace(request,allocation_id=context.job.id+'-g'+str(context.generation))
    reservation=await Scheduler(store).reserve(context.job.id,context.generation,
        HarnessJobAdapter._resources(context))
    client=AllocationClient(store,provider)
    identity=await client.enqueue(context,request)
    receipt={'allocation_dispatch_id':identity,'allocation_phase':'allocating',
        'allocation_id':request.allocation_id,'allocation_provider':provider.provider,
        'allocation_namespace':provider.namespace,'allocation_profile':asdict(request.profile),
        'reservation_id':reservation.id,'reservation_generation':context.generation,
        'node_id':reservation.node_id,'node_generation':reservation.node_generation}
    await store.record_job_submission_allocation(context.job.id,context.generation,
        external_execution_id=context.job.id+':'+str(context.generation),receipt=receipt)
    async with store.connections.begin() as db:
        row=await db.get(SandboxAllocationRow,identity)
        row.state='UNKNOWN';row.issued_at=now()-timedelta(seconds=91)
    return identity,receipt,reservation,request


async def test_uncertain_creation_has_a_bounded_wait_without_reissuing(tmp_path, monkeypatch):
    class Provider(FakeSandboxBackend):
        calls = 0
        async def create(self, request):
            self.calls += 1
            raise SandboxCreateOutcomeUnknown('response unavailable', provider=self.provider)

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider, poll_seconds=.01)
    try:
        await client.enqueue(context, request)
        await AllocationService(store, provider).run_once()
        future = now()+timedelta(seconds=91)
        monkeypatch.setattr('sandbox_harness.allocation_dispatch.now', lambda: future)
        with pytest.raises(RuntimeError, match='allocation_recovery_required'):
            await asyncio.wait_for(client.create(context, request), .5)
        assert provider.calls == 1
        assert (await client.snapshot(context, request))['state'] == 'UNKNOWN'
    finally:
        await store.close()


async def test_waiting_for_dispatch_does_not_start_uncertain_creation_deadline(tmp_path, monkeypatch):
    store, context, request = await fixture(tmp_path)
    provider = FakeSandboxBackend()
    client = AllocationClient(store, provider, poll_seconds=.01)
    task = None
    try:
        await client.enqueue(context, request)
        future = now()+timedelta(hours=1)
        monkeypatch.setattr('sandbox_harness.allocation_dispatch.now', lambda: future)
        task = asyncio.create_task(client.create(context, request))
        await asyncio.sleep(.05)
        assert not task.done()
        assert (await client.snapshot(context, request))['state'] == 'PENDING'
        assert not provider.items
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await store.close()


async def test_new_allocations_use_short_provisioning_ttl(tmp_path):
    class Provider(FakeSandboxBackend):
        ttl = None
        async def create(self, request):
            self.ttl = request.ttl_seconds
            return await super().create(request)

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider)
    try:
        request = replace(request, ttl_seconds=3600)
        await client.enqueue(context, request)
        await AllocationService(store, provider).run_once()
        assert provider.ttl == 300
        value = await client.snapshot(context, request)
        assert value['state'] == 'READY'
    finally:
        await store.close()


async def test_replacement_fences_old_attempt_and_keeps_original_cleanup_and_capacity(tmp_path):
    from sandbox_harness.store import JobRow, ResourceReservationRow
    store,context,request=await fixture(tmp_path)
    provider=FakeSandboxBackend()
    try:
        identity,receipt,reservation,request=await issued_allocation(store,context,request,provider)
        assert await store.replace_unstarted_allocation(context.job.id,'job-worker',context.generation,identity)=='queued'
        cleanup=await store.get_job_cleanup(context.job.id)
        assert cleanup['generation']==context.generation and cleanup['state']=='pending'
        import json
        from sqlalchemy import select
        from sandbox_harness.store import JobCleanupRow
        async with store.connections() as db:
            original=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==context.job.id))).one()
            assert json.loads(original.acceptance_receipt_json)==receipt
            assert (await db.get(JobRow,context.job.id)).allocation_replacements==1
            assert (await db.get(ResourceReservationRow,reservation.id)).state=='reserved'
        with pytest.raises(RuntimeError,match='stale_job_generation'):
            await store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id='late-runtime',receipt={**receipt,'allocation_phase':'runtime_submission'})
        newer=await store.claim_job(context.job.id,'replacement','node',30)
        assert newer.retry_kind=='allocation_replacement'
        assert await store.unresolved_job_submission(context.job.id,newer.generation) is None
    finally:await store.close()


async def test_replacement_budget_is_durable_and_runtime_submission_is_never_replaceable(tmp_path):
    store,context,request=await fixture(tmp_path)
    provider=FakeSandboxBackend()
    try:
        identity,receipt,_,_=await issued_allocation(store,context,request,provider)
        await store.record_job_submission_allocation(context.job.id,context.generation,
            external_execution_id='runtime',receipt={**receipt,'allocation_phase':'runtime_submission'})
        with pytest.raises(RuntimeError,match='allocation_not_replaceable'):
            await store.replace_unstarted_allocation(context.job.id,'job-worker',context.generation,identity)
        assert await store.get_job_cleanup(context.job.id) is None
        # Restore the pre-submission fixture; production never reverses this boundary.
        await store.record_job_submission_allocation(context.job.id,context.generation,
            external_execution_id='unstarted',receipt=receipt)
        assert await store.replace_unstarted_allocation(context.job.id,'job-worker',context.generation,identity)=='queued'
        newer=await store.claim_job(context.job.id,'replacement','node',30)
        await store.mark_job_submission_started(context.job.id,'replacement',newer.generation)
        context=replace(context,job=await store.get_job(context.job.id),generation=newer.generation)
        second,_,_,_=await issued_allocation(store,context,request,provider)
        assert await store.replace_unstarted_allocation(context.job.id,'replacement',newer.generation,second)=='exhausted'
        from sqlalchemy import select
        from sandbox_harness.store import JobCleanupRow, JobRow
        async with store.connections() as db:
            assert (await db.get(JobRow,context.job.id)).allocation_replacements==1
            assert len((await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==context.job.id))).all())==2
        # Crash after the exhaustion decision but before terminal publication.
        await store.release_job_lease(context.job.id,'replacement',newer.generation)
        recovered=await store.claim_job(context.job.id,'recovered','node',30)
        assert recovered.retry_kind=='allocation_recovery_exhausted'
        from sandbox_harness.job_worker import JobWorker
        from sandbox_harness.job_contracts import JobState
        await JobWorker(store,{},node_id='node',worker_id='recovered').process(recovered)
        assert (await store.get_job(context.job.id)).state==JobState.FAILED
        assert (await store.get_job(context.job.id)).result['error_code']=='allocation_recovery_exhausted'
    finally:await store.close()


@pytest.mark.parametrize('capacity',['available','local_full','node_full','provider_full'])
async def test_worker_replacement_requires_extra_capacity_and_keeps_late_cleanup_isolated(tmp_path,capacity):
    from types import SimpleNamespace
    from sqlalchemy import select
    from sandbox_harness.config import Settings
    from sandbox_harness.harness_job import HarnessJobAdapter,SandboxChildBackend
    from sandbox_harness.job_contracts import JobState
    from sandbox_harness.job_worker import JobWorker
    from sandbox_harness.objects import ObjectStore
    from sandbox_harness.pool import SandboxPool
    from sandbox_harness.scheduler import Scheduler,NodeInventory
    from sandbox_harness.sandbox_contracts import SandboxCapacityUnavailable
    from sandbox_harness.store import JobAttemptRow,ResourceReservationRow

    class Provider(FakeSandboxBackend):
        calls=0
        requests=[]
        async def create(self,request):
            self.calls+=1;self.requests.append(request)
            if self.calls==1:raise SandboxCreateOutcomeUnknown('response lost',provider=self.provider)
            if capacity=='provider_full':raise SandboxCapacityUnavailable('quota',provider=self.provider)
            return await super().create(request)

    class Runtime:
        starts=0
        completed=False
        async def inject(self,*args,**kwargs):pass
        async def start_execution(self,*args,**kwargs):
            self.starts+=1;return {'accepted':True}
        async def execution_status(self,*args,**kwargs):
            return {'result':{'status':'completed'}} if self.completed else {'result':None}
        async def artifacts(self,*args,**kwargs):return []

    store,context,request=await fixture(tmp_path)
    provider=Provider();runtime=Runtime();scheduler=Scheduler(store)
    if capacity=='node_full':
        await scheduler.register_node(NodeInventory(node_id='node',generation=1,cpu_total=1,
            memory_mb_total=16000,disk_mb_total=100000,load=0,
            profiles=[{'runtime_profile_id':'general-cpu','harness':'codex','image_digest':'b'*64,'warm_slots':1}]))
    client=AllocationClient(store,provider,poll_seconds=.01,recovery_seconds=.1)
    pool=SandboxPool(provider,{Harness.CODEX:'image'}, {Harness.CODEX:0},
        max_sandboxes=1 if capacity=='local_full' else 2)
    async def token(*args):return 'test',SimpleNamespace(nonce='')
    child=SandboxChildBackend(pool=pool,runtime=runtime,store=store,objects=ObjectStore(tmp_path/'objects'),
        settings=Settings(backend='fake',data_root=tmp_path,runtime_image_codex='image',
            job_allocation_dispatcher_enabled=True),token_factory=token,allocation_client=client)
    child._ensure_collector=lambda *args:None
    worker=JobWorker(store,{'child-harness':HarnessJobAdapter(scheduler,child)},node_id='node',worker_id='job-worker')
    stop=asyncio.Event();allocator=AllocationService(store,provider)
    runner=asyncio.create_task(allocator.run(stop,concurrency=2));collector=None
    try:
        async with store.connections() as db:
            first=store.job_attempt_view((await db.scalars(select(JobAttemptRow).where(
                JobAttemptRow.job_id==context.job.id))).one())
        await asyncio.wait_for(worker.process(first),5)
        assert (await store.get_job(context.job.id)).state==JobState.QUEUED
        assert runtime.starts==0 and pool._user_slots._value==(0 if capacity=='local_full' else 1)
        second=await store.claim_job(context.job.id,'job-worker','node',30)
        await asyncio.wait_for(worker.process(second),5)
        if capacity!='available':
            result=await store.get_job(context.job.id)
            assert result.state==JobState.FAILED
            assert result.result['error_code']=='allocation_replacement_capacity_unavailable'
            assert runtime.starts==0
            assert provider.calls==(2 if capacity=='provider_full' else 1)
        else:
            assert (await store.get_job(context.job.id)).state==JobState.WAITING_EXTERNAL
            assert provider.calls==2 and runtime.starts==1
            current=child.sessions[context.job.id]
            assert current.id in provider.renewed
            assert (current.expires_at-now()).total_seconds()>300
            collected=asyncio.Event();collector=asyncio.create_task(collected.wait())
            child.collectors[context.job.id]=collector
            late=await FakeSandboxBackend.create(provider,provider.requests[0])
            async with store.connections.begin() as db:
                row=(await db.scalars(select(SandboxAllocationRow).where(
                    SandboxAllocationRow.job_id==context.job.id,SandboxAllocationRow.job_generation==1))).one()
                row.reconcile_after=now()-timedelta(seconds=1)
            await allocator.reconcile_once()
            await worker.run_cleanup_once()
            assert late.id not in provider.items and current.id in provider.items
            assert child.sessions[context.job.id].handle==current.handle
            assert child.collectors[context.job.id] is collector and not collector.done()
            assert pool._user_slots._value==1
            collected.set();await collector
            runtime.completed=True
            third=await store.claim_job(context.job.id,'job-worker','node',30)
            await asyncio.wait_for(worker.process(third),5)
            assert (await store.get_job(context.job.id)).state==JobState.SUCCEEDED
            assert runtime.starts==1 and provider.calls==2 and not provider.items
        async with store.connections() as db:
            reservations=(await db.scalars(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id==context.job.id))).all()
            assert sum(row.state!='released' for row in reservations)==(0 if capacity=='available' else 1)
    finally:
        if collector:
            collector.cancel();await asyncio.gather(collector,return_exceptions=True)
        stop.set();await asyncio.gather(runner,return_exceptions=True)
        await pool.close();await store.close()


async def test_worker_loss_during_wait_keeps_original_generation_cleanup(tmp_path):
    from sandbox_harness.store import JobRow,JobCleanupRow
    from sqlalchemy import select
    store,context,request=await fixture(tmp_path)
    try:
        identity,receipt,_,_=await issued_allocation(store,context,request,FakeSandboxBackend())
        await store.release_job_lease(context.job.id,'job-worker',context.generation)
        claimed=await store.claim_job(context.job.id,'new-worker','node',30)
        assert await store.replace_unstarted_allocation(context.job.id,'new-worker',claimed.generation,identity)=='queued'
        async with store.connections() as db:
            cleanup=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==context.job.id))).one()
            assert cleanup.generation==context.generation
            assert (await db.get(JobRow,context.job.id)).allocation_replacements==1
        next_attempt=await store.claim_job(context.job.id,'third-worker','node',30)
        assert next_attempt.retry_kind=='allocation_replacement'
        assert await store.unresolved_job_submission(context.job.id,next_attempt.generation) is None
    finally:await store.close()


@pytest.mark.parametrize('winner',['ready','cancel'])
async def test_ready_receipt_or_user_cancellation_prevents_replacement(tmp_path,winner):
    import json
    from sandbox_harness.store import JobRow
    store,context,request=await fixture(tmp_path)
    provider=FakeSandboxBackend()
    try:
        identity,_,_,issued=await issued_allocation(store,context,request,provider)
        if winner=='ready':
            sandbox=await provider.create(issued)
            async with store.connections.begin() as db:
                row=await db.get(SandboxAllocationRow,identity)
                row.state='READY';row.handle_json=json.dumps(sandbox.handle.to_mapping())
        else:await store.request_job_cancel(context.job.id,'owner')
        outcome=await store.replace_unstarted_allocation(context.job.id,'job-worker',context.generation,identity)
        assert outcome==('observe' if winner=='ready' else 'cancelled')
        async with store.connections() as db:
            assert (await db.get(JobRow,context.job.id)).allocation_replacements==0
    finally:await store.close()


async def test_recovery_policy_is_frozen_and_capacity_rejection_resets_issued_clock(tmp_path):
    store,context,request=await fixture(tmp_path)
    provider=FakeSandboxBackend()
    client=AllocationClient(store,provider,recovery_seconds=60,provision_ttl_seconds=120)
    try:
        identity=await client.enqueue(context,request)
        changed=AllocationClient(store,provider,recovery_seconds=90,provision_ttl_seconds=300)
        await changed.enqueue(context,request)
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity)
            row.state='REJECTED';row.issued_at=now()-timedelta(seconds=100)
        await changed.enqueue(context,request)
        value=await changed.snapshot(context,request)
        assert value['state']=='PENDING' and value['issued_at'] is None
        assert value['recovery_policy']=={'recovery_seconds':60,'provision_ttl_seconds':120,'max_replacements':1}
    finally:await store.close()


async def test_renewal_failure_keeps_ready_handle_for_cleanup(tmp_path):
    from sandbox_harness.sandbox_contracts import SandboxProviderUnavailable
    class Provider(FakeSandboxBackend):
        async def renew(self,*args,**kwargs):
            raise SandboxProviderUnavailable('renewal unavailable',provider=self.provider)
    store,context,request=await fixture(tmp_path)
    provider=Provider();client=AllocationClient(store,provider)
    try:
        await client.enqueue(context,request)
        await AllocationService(store,provider).run_once()
        with pytest.raises(SandboxCreateOutcomeUnknown):await client.create(context,request)
        value=await client.snapshot(context,request)
        assert value['state']=='READY' and value['handle']['sandbox_id'] in provider.items
        assert len(provider.items)==1
    finally:await store.close()


async def test_old_schema_upgrade_keeps_legacy_unknown_and_does_not_invent_recovery_deadline(tmp_path,monkeypatch):
    from sqlalchemy import text
    from sandbox_harness.store import JobRow
    store,context,request=await fixture(tmp_path)
    provider=FakeSandboxBackend();client=AllocationClient(store,provider,poll_seconds=.01)
    task=None
    try:
        identity,_,_,request=await issued_allocation(store,context,request,provider)
        async with store.engine.begin() as db:
            await db.execute(text('ALTER TABLE sandbox_allocations DROP COLUMN issued_at'))
            await db.execute(text('ALTER TABLE sandbox_allocations DROP COLUMN recovery_policy_json'))
            await db.execute(text('ALTER TABLE jobs DROP COLUMN allocation_replacements'))
        await store.initialize();await store.initialize()
        future=now()+timedelta(hours=2)
        monkeypatch.setattr('sandbox_harness.allocation_dispatch.now',lambda:future)
        task=asyncio.create_task(client.create(context,request))
        await asyncio.sleep(.05)
        assert not task.done()
        value=await client.snapshot(context,request)
        assert value['state']=='UNKNOWN' and value['issued_at'] is None and value['recovery_policy']=={}
        async with store.connections() as db:
            assert (await db.get(JobRow,context.job.id)).allocation_replacements==0
    finally:
        if task:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        await store.close()
