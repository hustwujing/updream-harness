import asyncio
import os
from datetime import timedelta

import pytest

from sandbox_harness.domain import identifier, now
from sandbox_harness.job_contracts import JobGroupDraftCreate, JobKind, JobRequest, JobState
from sandbox_harness.store import Store


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
