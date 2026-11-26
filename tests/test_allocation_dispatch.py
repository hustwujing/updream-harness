import asyncio
import pytest
from dataclasses import replace

from test_harness_job import setup
from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, JobExecutionContext)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateRequest, SandboxRateLimited)


async def fixture(tmp_path):
    store,_,_,job=await setup(tmp_path,AdapterStatus(False))
    attempt=await store.claim_job(job.id,'job-worker','node',30)
    await store.mark_job_submission_started(job.id,'job-worker',attempt.generation)
    context=JobExecutionContext(job=await store.get_job(job.id),request=await store.get_job_request(job.id),
        generation=attempt.generation)
    request=SandboxCreateRequest(harness=Harness.CODEX,
        profile=FakeSandboxBackend.legacy_profile(Harness.CODEX,'image'),environment={},
        ttl_seconds=300,allocation_id=job.id)
    return store,context,request


async def test_waiter_loss_does_not_cancel_provider_create_or_duplicate_on_recovery(tmp_path):
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    class Provider(FakeSandboxBackend):
        entered=asyncio.Event();finish=asyncio.Event();calls=0
        async def create(self,request):
            self.calls+=1;self.entered.set();await self.finish.wait()
            return await super().create(request)
    store,context,request=await fixture(tmp_path);backend=Provider()
    client=AllocationClient(store,backend,poll_seconds=.01)
    service=AllocationService(store,backend,owner='allocator')
    waiter=asyncio.create_task(client.create(context,request));task=None
    try:
        for _ in range(100):
            if await client.snapshot(context,request):break
            await asyncio.sleep(.01)
        assert await store.active_sandbox_allocations()=={(backend.provider,request.allocation_id)}
        task=asyncio.create_task(service.run_once());await backend.entered.wait()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):await waiter
        await store.release_job_lease(context.job.id,'job-worker',context.generation)
        newer=await store.claim_job(context.job.id,'replacement','node',30)
        context=replace(context,job=await store.get_job(context.job.id),generation=newer.generation)
        backend.finish.set();await task
        sandbox=await client.create(context,request)
        assert backend.calls==1 and sandbox.handle.allocation_id==request.allocation_id
        assert (backend.provider,sandbox.id) in await store.active_sandbox_keys()
    finally:
        backend.finish.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await store.close()


async def test_stale_waiter_cannot_enqueue_after_replacement(tmp_path):
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    store,context,request=await fixture(tmp_path);backend=FakeSandboxBackend()
    client=AllocationClient(store,backend,poll_seconds=.01)
    try:
        await store.release_job_lease(context.job.id,'job-worker',context.generation)
        await store.claim_job(context.job.id,'replacement','node',30)
        with pytest.raises(RuntimeError,match='stale_job_generation'):
            await client.create(context,request)
        assert await AllocationService(store,backend,owner='allocator').run_once()==0
        assert not backend.items
    finally:await store.close()


async def test_definitive_capacity_rejection_is_durable_and_retryable(tmp_path):
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    class Provider(FakeSandboxBackend):
        calls=0
        async def create(self,request):
            self.calls+=1
            if self.calls==1:raise SandboxRateLimited('quota',provider=self.provider)
            return await super().create(request)
    store,context,request=await fixture(tmp_path);backend=Provider()
    client=AllocationClient(store,backend,poll_seconds=.01);service=AllocationService(store,backend,owner='allocator')
    try:
        for rejected in (True,False):
            waiter=asyncio.create_task(client.create(context,request))
            for _ in range(100):
                current=await client.snapshot(context,request)
                if current and current['state']=='PENDING':break
                await asyncio.sleep(.01)
            await service.run_once()
            if rejected:
                with pytest.raises(SandboxRateLimited):await waiter
            else:assert (await waiter).handle.allocation_id==request.allocation_id
        assert backend.calls==2
    finally:await store.close()


async def test_allocator_loss_keeps_unknown_and_never_reissues_create(tmp_path):
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    from sandbox_harness.common.db.store import (SandboxAllocationRow)
    from sandbox_harness.common.contracts.domain import (now)
    from datetime import timedelta
    class Provider(FakeSandboxBackend):
        calls=0
        async def create(self,request):self.calls+=1;return await super().create(request)
    store,context,request=await fixture(tmp_path);backend=Provider();client=AllocationClient(store,backend)
    try:
        identity=await client.enqueue(context,request)
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity)
            row.state='SENDING';row.claim_owner='lost-service';row.claim_expires_at=now()-timedelta(seconds=1)
        newer=AllocationService(store,backend,owner='replacement')
        assert await newer.run_once()==0
        assert (await client.snapshot(context,request))['state']=='UNKNOWN' and backend.calls==0
        # A late original create becomes visible. Reconciliation adopts it.
        sandbox=await backend.create(request)
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity)
            row.reconcile_after=now()-timedelta(seconds=1)
        assert await newer.run_once()==0
        assert (await client.snapshot(context,request))['handle']==sandbox.handle.to_mapping()
        assert backend.calls==1
    finally:await store.close()


@pytest.mark.parametrize('user_cancel',[False,True])
@pytest.mark.parametrize('reject',[False,True])
async def test_late_response_cleans_cancelled_waiter_and_original_reservation(tmp_path,user_cancel,reject):
    from types import SimpleNamespace
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    from sandbox_harness.common.configuration.config import (Settings)
    from sandbox_harness.services.job_worker.media_render_adapter import (MediaRenderOperationAdapter)
    from sandbox_harness.common.storage.objects import (ObjectStore)
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    from sandbox_harness.common.scheduling.scheduler import (Scheduler)
    from sandbox_harness.common.execution.job_adapters import (SubmissionInterrupted)
    from sandbox_harness.common.contracts.job_contracts import (JobState)
    from sandbox_harness.common.execution.job_worker import (JobWorker)
    class Provider(FakeSandboxBackend):
        entered=asyncio.Event();finish=asyncio.Event();calls=0
        async def create(self,request):
            self.calls+=1;self.entered.set();await self.finish.wait()
            if reject:raise SandboxRateLimited('quota',provider=self.provider)
            return await super().create(request)
    store,context,request=await fixture(tmp_path);backend=Provider();scheduler=Scheduler(store)
    client=AllocationClient(store,backend,poll_seconds=.01)
    pool=SandboxPool(backend,{Harness.CODEX:'image'},{Harness.CODEX:0},max_sandboxes=1)
    adapter=MediaRenderOperationAdapter(scheduler=scheduler,pool=pool,runtime=None,store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path,
            runtime_image_codex='image',job_allocation_dispatcher_enabled=True),token_factory=None,
        allocation_client=client)
    request=replace(request,allocation_id=context.job.id+'-g'+str(context.generation))
    worker=JobWorker(store,{'child-harness':adapter},node_id='node',worker_id='job-worker')
    if user_cancel:
        from sandbox_harness.common.db.store import (JobAttemptRow)
        from sqlalchemy import select
        async with store.connections() as db:
            attempt=(await db.scalars(select(JobAttemptRow).where(JobAttemptRow.job_id==context.job.id))).one()
            attempt=store.job_attempt_view(attempt)
        waiter=asyncio.create_task(worker.process(attempt,continuous=True))
    else:waiter=asyncio.create_task(adapter.submit(context))
    service_task=None
    try:
        for _ in range(200):
            if await client.snapshot(context,request):break
            await asyncio.sleep(.01)
        service_task=asyncio.create_task(AllocationService(store,backend,owner='allocator').run_once())
        await asyncio.wait_for(backend.entered.wait(),3)
        if user_cancel:
            await store.request_job_cancel(context.job.id,'owner')
            await asyncio.wait_for(asyncio.shield(waiter),2)
            assert (await store.get_job(context.job.id)).state==JobState.CANCELLED
            assert not backend.finish.is_set() and pool._pending_allocations
        else:
            waiter.cancel()
            with pytest.raises(SubmissionInterrupted):await waiter
            await store.transition_job(context.job.id,JobState.FAILED,expected_generation=context.generation,
                result={'error_code':'child_allocation_timeout'})
        backend.finish.set();await service_task
        await worker.run_cleanup_once()
        assert (await store.get_job_cleanup(context.job.id))['state']=='completed'
        assert await scheduler.active_reservation(context.job.id) is None
        assert not pool._pending_allocations and not backend.items and backend.calls==1
    finally:
        backend.finish.set()
        if service_task:await asyncio.gather(service_task,return_exceptions=True)
        if not waiter.done():waiter.cancel();await asyncio.gather(waiter,return_exceptions=True)
        await pool.close();await store.close()


@pytest.mark.parametrize('boundary',['poll','ready_lookup'])
async def test_observation_failure_after_enqueue_preserves_allocation_ownership(tmp_path,boundary):
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown, SandboxProviderUnavailable)
    store,context,request=await fixture(tmp_path);backend=FakeSandboxBackend()
    client=AllocationClient(store,backend,poll_seconds=.01)
    try:
        await client.enqueue(context,request)
        read=client._snapshot_identity;get=backend.get
        if boundary=='ready_lookup':
            await AllocationService(store,backend).run_once()
            async def unavailable(*args):raise SandboxProviderUnavailable('transient',provider=backend.provider)
            backend.get=unavailable
        else:
            async def unavailable(*args):raise OSError('observation unavailable')
            client._snapshot_identity=unavailable
        with pytest.raises(SandboxCreateOutcomeUnknown):await client.create(context,request)
        client._snapshot_identity=read;backend.get=get
        value=await client.snapshot(context,request)
        assert value['state']==('READY' if boundary=='ready_lookup' else 'PENDING')
        await AllocationService(store,backend).run_once()
        assert (await client.create(context,request)).handle.allocation_id==request.allocation_id
        assert len(backend.items)==1
    finally:await store.close()


@pytest.mark.parametrize('response_lost',[False,True])
@pytest.mark.parametrize('delete_failures',[0,2])
async def test_child_receipt_keeps_dispatch_identity_through_cleanup_and_new_attempt(tmp_path,response_lost,delete_failures):
    from types import SimpleNamespace
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
    from sandbox_harness.common.configuration.config import (Settings)
    from sandbox_harness.services.job_worker.harness_job import (HarnessJobAdapter, SandboxChildBackend)
    from sandbox_harness.common.execution.job_adapters import (SubmissionOutcomeUnknown)
    from sandbox_harness.common.storage.objects import (ObjectStore)
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    from sandbox_harness.common.scheduling.scheduler import (Scheduler)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxProviderUnavailable)
    class Provider(FakeSandboxBackend):
        deletes=0
        async def delete(self,sandbox):
            self.deletes+=1
            if self.deletes<=delete_failures:raise SandboxProviderUnavailable('delete unavailable',provider=self.provider)
            await super().delete(sandbox)
    store,context,request=await fixture(tmp_path);provider=Provider()
    client=AllocationClient(store,provider,poll_seconds=.01);service=AllocationService(store,provider)
    pool=SandboxPool(provider,{Harness.CODEX:'image'},{Harness.CODEX:0},max_sandboxes=1)
    class Runtime:
        starts=0
        async def inject(self,*args,**kwargs):pass
        async def start_execution(self,*args,**kwargs):
            self.starts+=1
            if response_lost:raise ConnectionError('response lost')
            return {'accepted':True}
    async def token(*args):return 'token',SimpleNamespace(nonce='')
    runtime=Runtime();backend=SandboxChildBackend(pool=pool,runtime=runtime,store=store,
        objects=ObjectStore(tmp_path/'objects'),settings=Settings(backend='fake',data_root=tmp_path,
            runtime_image_codex='image',job_allocation_dispatcher_enabled=True),token_factory=token,
        allocation_client=client)
    backend._ensure_collector=lambda *args:None
    adapter=HarnessJobAdapter(Scheduler(store),backend)
    async def submit(current):
        task=asyncio.create_task(adapter.submit(current))
        for _ in range(200):
            await service.run_once()
            if task.done():break
            await asyncio.sleep(.01)
        try:
            value=await asyncio.wait_for(task,2)
            return value.external_execution_id,value.acceptance_receipt
        except SubmissionOutcomeUnknown as exc:
            assert response_lost
            return exc.external_execution_id,exc.acceptance_receipt
    try:
        external,receipt=await submit(context)
        original_request=replace(request,allocation_id=receipt['allocation_id'])
        assert receipt['allocation_dispatch_id'] and receipt['allocation_phase']=='runtime_submission'
        old_id=receipt['sandbox_id']
        for _ in range(delete_failures):
            with pytest.raises(SandboxProviderUnavailable):
                await adapter.cleanup(replace(context,previous_acceptance_receipt=receipt),external)
            assert (await client.snapshot(context,original_request))['state']=='READY'
            assert old_id in provider.items and await adapter.scheduler.active_reservation(context.job.id)
        await adapter.cleanup(replace(context,previous_acceptance_receipt=receipt),external)
        assert (await client.snapshot(context,original_request))['state']=='RELEASED'
        assert not provider.items
        # An explicit new attempt after confirmed retirement may allocate again.
        await store.release_job_lease(context.job.id,'job-worker',context.generation)
        newer=await store.claim_job(context.job.id,'replacement','node',30)
        await store.mark_job_submission_started(context.job.id,'replacement',newer.generation)
        current=replace(context,job=await store.get_job(context.job.id),generation=newer.generation)
        external,second=await submit(current)
        assert second['sandbox_id']!=old_id and runtime.starts==2
        await adapter.cleanup(replace(current,previous_acceptance_receipt=second),external)
        assert (await client.snapshot(current,replace(request,allocation_id=second['allocation_id'])))['state']=='RELEASED'
        assert (await client.snapshot(context,original_request))['state']=='RELEASED'
    finally:await pool.close();await store.close()


async def test_retiring_old_generation_does_not_release_new_pending_allocation():
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    from sandbox_harness.common.contracts.sandbox_contracts import (SandboxProviderUnavailable)
    class Provider(FakeSandboxBackend):
        creates=0;deletes=0;entered=asyncio.Event();finish=asyncio.Event()
        async def create(self,request):
            self.creates+=1
            if self.creates==2:self.entered.set();await self.finish.wait()
            return await super().create(request)
        async def delete(self,sandbox):
            self.deletes+=1
            if self.deletes==1:raise SandboxProviderUnavailable('delete unavailable',provider=self.provider)
            await super().delete(sandbox)
    provider=Provider();pool=SandboxPool(provider,{Harness.CODEX:'image'},{Harness.CODEX:0},max_sandboxes=2)
    task=None
    try:
        first=await pool.acquire(thread_id='job',turn_id='job',harness=Harness.CODEX,
            environment={},allocation_id='job-g1',retain_unresolved_allocation=True)
        with pytest.raises(SandboxProviderUnavailable):await pool.retire(first)
        task=asyncio.create_task(pool.acquire(thread_id='job',turn_id='job',harness=Harness.CODEX,
            environment={},allocation_id='job-g2',retain_unresolved_allocation=True))
        await asyncio.wait_for(provider.entered.wait(),2)
        await pool.retire(first)
        assert await pool.owns_allocation(provider.provider,'job-g2')
        assert pool._user_slots._value==1
        provider.finish.set();second=await task
        assert second.id!=first.id and second.handle.allocation_id=='job-g2'
    finally:
        provider.finish.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await pool.close()


async def test_explicit_replacement_allocation_never_reuses_previous_attachment():
    from sandbox_harness.common.sandbox.pool import (SandboxPool)
    provider=FakeSandboxBackend();pool=SandboxPool(provider,{Harness.CODEX:'image'},
        {Harness.CODEX:0},max_sandboxes=1)
    try:
        first=await pool.acquire(thread_id='job',turn_id='job',harness=Harness.CODEX,
            environment={},allocation_id='job-g1',retain_unresolved_allocation=True)
        replacement=await pool.acquire(thread_id='job',turn_id='job',harness=Harness.CODEX,
            environment={},allocation_id='job-g3',retain_unresolved_allocation=True)
        assert replacement.id!=first.id
        assert replacement.handle.allocation_id=='job-g3'
        assert first.id not in provider.items
    finally:await pool.close()
