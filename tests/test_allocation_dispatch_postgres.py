import asyncio
import os
import pytest

from sandbox_harness.allocation_dispatch import AllocationClient,AllocationService
from sandbox_harness.domain import Harness,identifier
from sandbox_harness.job_adapters import JobExecutionContext
from sandbox_harness.job_contracts import JobGroupCreate,JobRequest
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.sandbox_contracts import SandboxCreateRequest
from sandbox_harness.store import Store

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
