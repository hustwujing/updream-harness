import asyncio
import os
import pytest

from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient, AllocationService)
from sandbox_harness.common.contracts.domain import (Harness, identifier)
from sandbox_harness.common.execution.job_adapters import (JobExecutionContext)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobRequest)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateRequest)
from test_support.legacy_store import (Store)

URL=os.environ.get('PARALLEL_TEST_POSTGRES_URL','')
pytestmark=pytest.mark.skipif(not URL,reason='PARALLEL_TEST_POSTGRES_URL is not configured')


async def test_concurrent_enqueues_and_allocators_issue_one_provider_create():
    store=Store(URL);await store.initialize();others=[Store(URL),Store(URL)]
    class Provider(FakeSandboxBackend):
        calls=0
        async def create(self,request):
            self.calls+=1;await asyncio.sleep(.05)
            return await super().create(request)
    backend=Provider();identity=identifier()
    request=JobRequest(local_job_id='allocation',kind='harness',adapter='child-harness',
        descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key='allocation-'+identity)
    try:
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=identifier(),
            origin_turn_id=identifier(),idempotency_key='group-'+identity,jobs=[request]))
        job=jobs[0];attempt=await store.claim_job(job.id,'worker','node',30)
        await store.mark_job_submission_started(job.id,'worker',attempt.generation)
        context=JobExecutionContext(job=await store.get_job(job.id),request=request,generation=attempt.generation)
        creation=SandboxCreateRequest(harness=Harness.CODEX,
            profile=backend.legacy_profile(Harness.CODEX,'image'),environment={},ttl_seconds=300,
            allocation_id=job.id)
        identities=await asyncio.gather(*(AllocationClient(s,backend).enqueue(context,creation) for s in others))
        assert len(set(identities))==1
        await asyncio.gather(*(AllocationService(s,backend,owner='allocator-'+str(i)).run_once()
            for i,s in enumerate(others)))
        assert backend.calls==1 and len(backend.items)==1
        assert (await AllocationClient(store,backend).snapshot(context,creation))['state']=='READY'
    finally:
        await asyncio.gather(*(s.close() for s in others));await store.close()


async def allocation_fixture(store, backend):
    identity=identifier()
    request=JobRequest(local_job_id='allocation',kind='harness',adapter='child-harness',
        descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key='allocation-'+identity)
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=identifier(),
        origin_turn_id=identifier(),idempotency_key='group-'+identity,jobs=[request]))
    job=jobs[0];attempt=await store.claim_job(job.id,'worker','node',30)
    await store.mark_job_submission_started(job.id,'worker',attempt.generation)
    context=JobExecutionContext(job=await store.get_job(job.id),request=request,generation=attempt.generation)
    creation=SandboxCreateRequest(harness=Harness.CODEX,
        profile=backend.legacy_profile(Harness.CODEX,'image'),environment={},ttl_seconds=300,
        allocation_id=job.id)
    return context,creation


async def test_concurrent_observers_claim_each_allocation_once():
    from test_support.legacy_store import (SandboxAllocationRow)
    store=Store(URL);await store.initialize();other=Store(URL)
    class Provider(FakeSandboxBackend):
        reads=0
        entered=asyncio.Event();finish=asyncio.Event()
        async def reconcile_create(self,scope,allocation_id):
            self.reads+=1;self.entered.set();await self.finish.wait()
            return None
    backend=Provider(namespace='pg-reconcile-'+identifier())
    task=None
    try:
        context,request=await allocation_fixture(store,backend)
        identity=await AllocationClient(store,backend).enqueue(context,request)
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity);row.state='UNKNOWN'
        first=AllocationService(store,backend);second=AllocationService(other,backend)
        task=asyncio.create_task(first.reconcile_once(limit=1))
        await asyncio.wait_for(backend.entered.wait(),2)
        assert await second.reconcile_once(limit=1)==0
        backend.finish.set();assert await task==1
        assert backend.reads==1
    finally:
        backend.finish.set()
        if task:await asyncio.gather(task,return_exceptions=True)
        await other.close();await store.close()


async def test_postgres_upgrade_preserves_unknown_allocation_and_original_claim():
    from sqlalchemy import text
    from test_support.legacy_store import (SandboxAllocationRow)
    store=Store(URL);await store.initialize()
    backend=FakeSandboxBackend(namespace='pg-upgrade-'+identifier())
    try:
        context,request=await allocation_fixture(store,backend)
        client=AllocationClient(store,backend)
        identity=await client.enqueue(context,request)
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity)
            row.state='UNKNOWN';row.claim_owner='original-create-owner'
        async with store.engine.begin() as db:
            for column in ('reconcile_after','reconcile_attempts','reconcile_token','reconcile_expires_at',
                           'issued_at','recovery_policy_json'):
                await db.execute(text('ALTER TABLE sandbox_allocations DROP COLUMN '+column))
            await db.execute(text('ALTER TABLE jobs DROP COLUMN allocation_replacements'))
        await asyncio.gather(store.initialize(),store.initialize())
        async with store.connections() as db:
            row=await db.get(SandboxAllocationRow,identity)
            assert row.state=='UNKNOWN' and row.claim_owner=='original-create-owner'
            assert row.reconcile_attempts==0 and row.reconcile_token==''
            assert row.issued_at is None and row.recovery_policy_json=='{}'
        assert await AllocationService(store,backend).reconcile_once()==1
        assert (await client.snapshot(context,request))['state']=='UNKNOWN'
    finally:await store.close()


@pytest.mark.parametrize('replacement_first',[False,True])
async def test_runtime_submission_and_allocation_replacement_are_mutually_exclusive(replacement_first):
    from datetime import timedelta
    from sandbox_harness.common.contracts.domain import (now)
    from test_support.legacy_store import (SandboxAllocationRow, JobRow)
    store=Store(URL);await store.initialize();other=Store(URL)
    backend=FakeSandboxBackend(namespace='pg-replacement-'+identifier())
    try:
        context,request=await allocation_fixture(store,backend)
        identity=await AllocationClient(store,backend).enqueue(context,request)
        receipt={'allocation_phase':'allocating','allocation_dispatch_id':identity}
        await store.record_job_submission_allocation(context.job.id,context.generation,
            external_execution_id='original-runtime',receipt=receipt)
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity)
            row.state='UNKNOWN';row.issued_at=now()-timedelta(seconds=91)
        replacement=store.replace_unstarted_allocation(context.job.id,'worker',context.generation,identity)
        submission=other.record_job_submission_allocation(context.job.id,context.generation,
            external_execution_id='original-runtime',receipt={**receipt,'allocation_phase':'runtime_submission'})
        calls=[replacement,submission] if replacement_first else [submission,replacement]
        values=await asyncio.gather(*calls,return_exceptions=True)
        replaced,submitted=values if replacement_first else reversed(values)
        if replaced=='queued':
            assert isinstance(submitted,RuntimeError) and str(submitted)=='stale_job_generation'
            async with store.connections() as db:
                assert (await db.get(JobRow,context.job.id)).allocation_replacements==1
        else:
            assert isinstance(replaced,RuntimeError) and str(replaced)=='allocation_not_replaceable'
            assert submitted is None
            async with store.connections() as db:
                assert (await db.get(JobRow,context.job.id)).allocation_replacements==0
    finally:await other.close();await store.close()
