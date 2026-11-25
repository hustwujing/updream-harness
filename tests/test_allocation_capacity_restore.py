import asyncio
import json
from dataclasses import replace
import pytest
from test_allocation_dispatch import fixture
from test_allocation_recovery import issued_allocation
from sandbox_harness.domain import Harness
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.store import SandboxAllocationRow,JobCleanupRow
from sqlalchemy import select

async def test_lowered_limit_debt_adoption_failed_delete_and_release(tmp_path):
    class Backend(FakeSandboxBackend):
        fail=False
        async def delete(self, sandbox):
            if self.fail:raise RuntimeError('test_delete_failure')
            await super().delete(sandbox)
    store,context,request=await fixture(tmp_path)
    backend=Backend()
    pool=SandboxPool(backend,{Harness.CODEX:'image'},{Harness.CODEX:0},max_sandboxes=1)
    sandboxes=[await backend.create(replace(request,allocation_id=f'owned-{i}')) for i in range(3)]
    inventory=[dict(provider=s.handle.provider,allocation_id=s.handle.allocation_id,profile_key=s.compatibility_key) for s in sandboxes]
    try:
        await pool.restore_allocation_capacity(inventory)
        await pool.restore_allocation_capacity(inventory)
        assert pool._capacity_debt==2 and pool._user_slots._value==0
        first=await asyncio.wait_for(pool.adopt(sandboxes[0],thread_id='thread',turn_id='turn',expected_profile=request.profile),1)
        assert pool._capacity_debt==2 and pool._user_slots._value==0
        backend.fail=True
        with pytest.raises(RuntimeError,match="test_delete_failure"):
            await pool.retire(first)
        await pool.restore_allocation_capacity(inventory)
        assert pool._capacity_debt==2 and pool._user_slots._value==0
        backend.fail=False
        await pool.retire(first)
        assert pool._capacity_debt==1 and pool._user_slots._value==0
        await pool.retire(sandboxes[1])
        assert pool._capacity_debt==0 and pool._user_slots._value==0
        await pool.retire(sandboxes[2])
        await pool.retire(sandboxes[2])
        assert pool._capacity_debt==0 and pool._user_slots._value==1
    finally:
        backend.fail=False
        await pool.close();await store.close()

@pytest.mark.parametrize('phase',['allocating','runtime_submission'])
async def test_capacity_projection_released_exclusion_and_legacy_cleanup(tmp_path,phase):
    store,context,request=await fixture(tmp_path)
    backend=FakeSandboxBackend()
    try:
        identity,receipt,reservation,request=await issued_allocation(store,context,request,backend)
        sandbox=await backend.create(request)
        receipt={**receipt,'allocation_phase':phase,'sandbox_id':sandbox.id,'sandbox_handle':sandbox.handle.to_mapping()}
        await store.record_job_submission_allocation(context.job.id,context.generation,external_execution_id='runtime',receipt=receipt)
        if phase=='runtime_submission':
            await store.record_job_acceptance(context.job.id,'job-worker',context.generation,external_execution_id='runtime',receipt=receipt)
        await store.enqueue_job_cleanup(context.job.id,context.generation)
        assert len(await store.job_sandbox_capacity())==1
        async with store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity)
            row.state='RELEASED'
        assert await store.job_sandbox_capacity()==[]
        # Existing deployments can have receipts but no allocation ledger.
        async with store.connections.begin() as db:
            await db.delete(await db.get(SandboxAllocationRow,identity))
        assert len(await store.job_sandbox_capacity())==1
        async with store.connections.begin() as db:
            cleanup=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==context.job.id))).one()
            cleanup.state='completed';cleanup.phase='completed'
        assert await store.job_sandbox_capacity()==[]
    finally:await store.close()
