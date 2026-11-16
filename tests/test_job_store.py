import asyncio
from datetime import timedelta

import pytest

from sandbox_harness.domain import identifier, now
from sandbox_harness.job_contracts import (
    JobGroupCreate,
    JobGroupDraftCreate,
    JobGroupDraftState,
    JobGroupState,
    JobKind,
    JobRequest,
    JobState,
)
from sandbox_harness.store import Store
from sandbox_harness.store import JOB_DATABASE_LOCK_ORDER


def request(local_job_id: str) -> JobRequest:
    return JobRequest(
        local_job_id=local_job_id,
        kind=JobKind.OPERATION,
        adapter="test.operation",
        descriptor_id="test.operation",
        descriptor_version="1",
        descriptor_digest="a" * 64,
        idempotency_key=f"job-{local_job_id}-key",
        parameters={"value": local_job_id},
    )


async def make_store(tmp_path) -> Store:
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "jobs.db"))
    await store.initialize()
    return store


def test_job_database_lock_order_is_explicit_and_stable():
    assert JOB_DATABASE_LOCK_ORDER == (
        "job_group_draft", "authorization_snapshot", "budget_account", "budget_reservation", "job_group",
        "job", "job_attempt", "job_lease", "job_artifact", "job_outbox",
    )


async def test_draft_confirm_is_atomic_idempotent_and_group_is_immutable(tmp_path):
    store = await make_store(tmp_path)
    try:
        draft_value = JobGroupDraftCreate(
            thread_id="a" * 32,
            origin_turn_id="b" * 32,
            idempotency_key="draft-confirm-key",
            jobs=[request("one"), request("two")],
            expires_at=now() + timedelta(minutes=5),
            requires_confirmation=True,
        )
        draft, created = await store.create_job_group_draft("owner", draft_value)
        assert created and draft.state == JobGroupDraftState.AWAITING_CONFIRMATION
        repeated, created = await store.create_job_group_draft("owner", draft_value)
        assert not created and repeated.id == draft.id
        group, jobs = await store.confirm_job_group_draft(draft.id, "owner")
        assert group.state == JobGroupState.COMMITTED
        assert [job.local_job_id for job in jobs] == ["one", "two"]
        assert all(job.state == JobState.CREATED for job in jobs)
        same_group, same_jobs = await store.confirm_job_group_draft(draft.id, "owner")
        assert same_group.id == group.id and [job.id for job in same_jobs] == [job.id for job in jobs]
        with pytest.raises(RuntimeError, match="draft_not_editable"):
            await store.reject_job_group_draft(draft.id, "owner")
    finally:
        await store.close()


async def test_draft_reject_and_expire_create_no_jobs(tmp_path):
    store = await make_store(tmp_path)
    try:
        rejected, _ = await store.create_job_group_draft("owner", JobGroupDraftCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="draft-reject-key", jobs=[request("reject")],
            expires_at=now() + timedelta(minutes=5), requires_confirmation=True,
        ))
        rejected = await store.reject_job_group_draft(rejected.id, "owner")
        assert rejected.state == JobGroupDraftState.REJECTED
        expired, _ = await store.create_job_group_draft("owner", JobGroupDraftCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="draft-expire-key", jobs=[request("expire")],
            expires_at=now() - timedelta(seconds=1), requires_confirmation=True,
        ))
        assert await store.expire_job_group_drafts() == 1
        assert (await store.get_job_group_draft(expired.id, "owner")).state == JobGroupDraftState.EXPIRED
        assert await store.list_jobs_for_thread("a" * 32) == []
    finally:
        await store.close()


async def test_confirming_expired_draft_persists_expired_state(tmp_path):
    store = await make_store(tmp_path)
    try:
        draft, _ = await store.create_job_group_draft("owner", JobGroupDraftCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="expired-confirm-key", jobs=[request("expired-confirm")],
            expires_at=now() - timedelta(seconds=1), requires_confirmation=True,
        ))
        with pytest.raises(RuntimeError, match="draft_expired"):
            await store.confirm_job_group_draft(draft.id, "owner")
        assert (await store.get_job_group_draft(
            draft.id, "owner")).state == JobGroupDraftState.EXPIRED
    finally:
        await store.close()


async def test_all_terminal_projection_creates_exactly_one_outbox(tmp_path):
    store = await make_store(tmp_path)
    try:
        group, jobs = await store.create_job_group("owner", JobGroupCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="direct-group-key", jobs=[request("one"), request("two")],
        ))
        await store.transition_job(jobs[0].id, JobState.SUCCEEDED, expected_generation=0,
            result={"ok": True})
        assert (await store.get_job_group(group.id)).state == JobGroupState.RUNNING
        assert await store.get_job_outbox(group.id) is None
        await asyncio.gather(
            store.transition_job(jobs[1].id, JobState.FAILED, expected_generation=0,
                result={"error": "provider_failed"}),
            store.transition_job(jobs[1].id, JobState.FAILED, expected_generation=0,
                result={"error": "provider_failed"}),
        )
        terminal = await store.get_job_group(group.id)
        outbox = await store.get_job_outbox(group.id)
        assert terminal.state == JobGroupState.TERMINAL
        assert outbox is not None and outbox.group_id == group.id and outbox.state == "ready"
        assert len(await store.list_job_outboxes()) == 1
    finally:
        await store.close()


async def test_job_attempt_generation_fences_stale_worker(tmp_path):
    store = await make_store(tmp_path)
    try:
        _, jobs = await store.create_job_group("owner", JobGroupCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="attempt-group-key", jobs=[request("attempt")],
        ))
        first = await store.claim_job(jobs[0].id, "worker-a", "node-a", 30)
        await store.release_job_lease(jobs[0].id, "worker-a", first.generation)
        second = await store.claim_job(jobs[0].id, "worker-b", "node-b", 30)
        assert second.generation > first.generation
        with pytest.raises(RuntimeError, match="stale_job_generation"):
            await store.record_job_acceptance(jobs[0].id, "worker-a", first.generation,
                external_execution_id="old-handle", receipt={"accepted": True})
        accepted = await store.record_job_acceptance(jobs[0].id, "worker-b", second.generation,
            external_execution_id="new-handle", receipt={"accepted": True})
        assert accepted.external_execution_id == "new-handle"
    finally:
        await store.close()


async def test_only_one_worker_can_hold_a_live_job_lease(tmp_path):
    store = await make_store(tmp_path)
    try:
        _, jobs = await store.create_job_group("owner", JobGroupCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="claim-race-group", jobs=[request("claim-race")],
        ))
        results = await asyncio.gather(
            store.claim_job(jobs[0].id, "worker-a", "node-a", 30),
            store.claim_job(jobs[0].id, "worker-b", "node-b", 30),
            return_exceptions=True,
        )
        assert sum(not isinstance(value, Exception) for value in results) == 1
        loser = next(value for value in results if isinstance(value, Exception))
        assert str(loser) == "job_already_leased"
    finally:
        await store.close()


async def test_replacement_and_reconciliation_lineage_are_persisted(tmp_path):
    store = await make_store(tmp_path)
    try:
        original = request("original")
        _, jobs = await store.create_job_group("owner", JobGroupCreate(
            thread_id="a" * 32, origin_turn_id="b" * 32,
            idempotency_key="lineage-original", jobs=[original],
        ))
        replacement = request("replacement").model_copy(update={"replaces_job_id": jobs[0].id})
        _, replacement_jobs = await store.create_job_group("owner", JobGroupCreate(
            thread_id="a" * 32, origin_turn_id="c" * 32,
            idempotency_key="lineage-replacement", jobs=[replacement],
        ))
        assert replacement_jobs[0].replaces_job_id == jobs[0].id
    finally:
        await store.close()
