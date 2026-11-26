import asyncio
import os
from datetime import timedelta

import pytest

from sandbox_harness.common.contracts.domain import (identifier, now)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobGroupDraftCreate, JobKind, JobRequest, JobState)
from sandbox_harness.common.db.store import (Store, PauseParticipantRow)


POSTGRES_URL = os.environ.get("PARALLEL_TEST_POSTGRES_URL", "")
pytestmark = pytest.mark.skipif(not POSTGRES_URL, reason="PARALLEL_TEST_POSTGRES_URL is not configured")


def request(suffix: str) -> JobRequest:
    return JobRequest(
        local_job_id="one", kind=JobKind.OPERATION, adapter="test.operation",
        descriptor_id="test.operation", descriptor_version="1", descriptor_digest="a" * 64,
        idempotency_key=f"postgres-job-{suffix}", parameters={},
    )


async def test_concurrent_confirm_and_terminalization_have_one_winner():
    store = Store(POSTGRES_URL)
    await store.initialize()
    workers = [Store(POSTGRES_URL) for _ in range(8)]
    try:
        suffix = identifier()
        draft, _ = await store.create_job_group_draft("postgres-owner", JobGroupDraftCreate(
            thread_id=identifier(), origin_turn_id=identifier(),
            idempotency_key=f"postgres-draft-{suffix}", jobs=[request(suffix)],
            expires_at=now() + timedelta(minutes=5), requires_confirmation=True,
        ))
        confirmations = await asyncio.gather(*[
            worker.confirm_job_group_draft(draft.id, "postgres-owner") for worker in workers
        ])
        group_ids = {group.id for group, _ in confirmations}
        job_ids = {jobs[0].id for _, jobs in confirmations}
        assert len(group_ids) == len(job_ids) == 1
        job_id = next(iter(job_ids))
        await asyncio.gather(*[
            worker.transition_job(job_id, JobState.SUCCEEDED,
                expected_generation=0, result={"ok": True}) for worker in workers
        ])
        assert len([value for value in await store.list_job_outboxes()
            if value.group_id == next(iter(group_ids))]) == 1
    finally:
        await asyncio.gather(*(worker.close() for worker in workers))
        await store.close()


async def test_recovery_claims_exclude_superseded_acceptance_and_have_one_owner():
    store=Store(POSTGRES_URL);await store.initialize();workers=[Store(POSTGRES_URL) for _ in range(2)]
    identity=identifier();adapter='recovery-'+identity
    try:
        _,jobs=await store.create_job_group('postgres-owner',JobGroupCreate(
            thread_id=identifier(),origin_turn_id=identifier(),idempotency_key='recovery-'+identity,
            jobs=[request(identity+'-'+str(i)).model_copy(update={'local_job_id':str(i),'adapter':adapter})
                for i in range(3)]))
        for job in jobs[:2]:
            attempt=await store.claim_job(job.id,'old','node',30)
            await store.record_job_acceptance(job.id,'old',attempt.generation,
                external_execution_id='external-'+job.id,receipt={})
            await store.defer_accepted_job(job.id,'old',attempt.generation)
        async with store.connections.begin() as db:
            db.add(PauseParticipantRow(id=identifier(),owner_id='postgres-owner',
                thread_id=jobs[1].thread_id,generation=1,kind='child_harness',
                participant_id=jobs[1].id,state='RESUMED',created_at=now(),updated_at=now()))
        claimed=await asyncio.gather(*(worker.claim_available_jobs('recovery-'+str(i),'node',30,
            limit=3,adapters={adapter},accepted_only=True) for i,worker in enumerate(workers)))
        assert [row.job_id for rows in claimed for row in rows]==[jobs[0].id]
        assert (await store.get_job(jobs[2].id)).generation==0
    finally:
        await asyncio.gather(*(worker.close() for worker in workers));await store.close()
