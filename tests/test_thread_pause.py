import pytest

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, PauseParticipantState, ThreadControlState, ThreadCreate, TurnCreate, TurnState)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, AdapterSubmission, BaseJobAdapter)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from sandbox_harness.common.db.store import (Store)


def request(name: str, kind: JobKind) -> JobRequest:
    return JobRequest(local_job_id=name, kind=kind, adapter={
        JobKind.HARNESS: "child-harness",
        JobKind.PROVIDER: "image.generate",
        JobKind.OPERATION: "media-render",
    }[kind], descriptor_id=name, descriptor_version="1", descriptor_digest="a" * 64,
        idempotency_key="pause-" + name + "-request", parameters={})


async def setup(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "pause.db"))
    await store.initialize()
    profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model="gpt-5.6",
        reasoning_level="standard", title="Pause"), profile, "owner")
    turn, _, _ = await store.create_turn(thread.id, TurnCreate(message="run",
        idempotency_key="pause-origin-turn"), profile, "owner")
    return store, profile, thread, turn


async def test_pause_is_idempotent_and_only_harness_jobs_become_participants(tmp_path):
    store, profile, thread, turn = await setup(tmp_path)
    try:
        _, jobs = await store.create_job_group("owner", JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id, idempotency_key="pause-mixed-group",
            jobs=(request("agent", JobKind.HARNESS), request("provider", JobKind.PROVIDER),
                request("operation", JobKind.OPERATION))))
        first = await store.request_thread_pause(thread.id, "owner")
        second = await store.request_thread_pause(thread.id, "owner")
        assert first.operation_id == second.operation_id
        assert first.generation == second.generation == 1
        assert first.state == ThreadControlState.PAUSED
        participants = await store.list_pause_participants(thread.id, first.generation)
        assert {(value.kind, value.participant_id) for value in participants} == {
            ("primary_harness", turn.id), ("child_harness", jobs[0].id)}
        assert (await store.get_job(jobs[0].id)).state == JobState.PAUSED
        assert (await store.get_job(jobs[1].id)).state == JobState.CREATED
        assert (await store.get_job(jobs[2].id)).state == JobState.CREATED
    finally:
        await store.close()


async def test_resume_requeues_only_paused_harness_and_unfences_claims(tmp_path):
    store, profile, thread, turn = await setup(tmp_path)
    try:
        _, jobs = await store.create_job_group("owner", JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id, idempotency_key="pause-resume-group",
            jobs=(request("agent", JobKind.HARNESS), request("provider", JobKind.PROVIDER))))
        control = await store.request_thread_pause(thread.id, "owner")
        await store.complete_pause_participant(thread.id, control.generation,
            "primary_harness", turn.id, checkpoint_ref="sha256:" + "b" * 64,
            native_thread_id="native-primary")
        assert (await store.get_thread_control(thread.id)).state == ThreadControlState.PAUSED
        assert await store.claim_available_jobs("worker", "node", 30, limit=4) == []
        resumed = await store.resume_thread(thread.id, "owner")
        assert resumed.state == ThreadControlState.RUNNING
        assert (await store.get_job(jobs[0].id)).state == JobState.QUEUED
        assert (await store.get_job(jobs[1].id)).state == JobState.CREATED
        participants = await store.list_pause_participants(thread.id, control.generation)
        assert all(value.state == PauseParticipantState.RESUMED for value in participants)
        continued, _, _ = await store.create_turn(thread.id, TurnCreate(message="继续",
            idempotency_key="pause-resume-context"), profile, "owner")
        assert continued.message == "继续"
        assert "[pause_resume_context]" in continued.runtime_message
        assert '"previous_user_objective":"run"' in continued.runtime_message
        claimed = await store.claim_available_jobs("worker", "node", 30, limit=4)
        assert {value.job_id for value in claimed} == {jobs[0].id, jobs[1].id}
    finally:
        await store.close()


async def test_paused_thread_holds_ready_continuation_and_new_turn(tmp_path):
    store, profile, thread, turn = await setup(tmp_path)
    try:
        group, jobs = await store.create_job_group("owner", JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id, idempotency_key="pause-terminal-group",
            jobs=(request("operation", JobKind.OPERATION),)))
        await store.transition_job(jobs[0].id, JobState.SUCCEEDED, expected_generation=0,
            result={"ok": True})
        control = await store.request_thread_pause(thread.id, "owner")
        await store.complete_pause_participant(thread.id, control.generation,
            "primary_harness", turn.id, checkpoint_ref="sha256:" + "c" * 64)
        assert (await store.get_job_outbox(group.id)).state == "ready"
        assert await store.claim_job_outbox_batch("dispatcher") == []
        with pytest.raises(RuntimeError, match="thread_paused"):
            await store.create_turn(thread.id, TurnCreate(message="continue",
                idempotency_key="paused-new-turn"), profile, "owner")
        await store.resume_thread(thread.id, "owner")
        assert len(await store.claim_job_outbox_batch("dispatcher")) == 1
    finally:
        await store.close()


async def test_terminal_primary_during_pause_settles_participant_and_control(tmp_path):
    store,_,thread,turn=await setup(tmp_path)
    try:
        await store.set_turn_state(turn.id,TurnState.RUNNING)
        control=await store.request_thread_pause(thread.id,'owner')
        participant=await store.get_pause_participant(thread.id,'primary_harness',turn.id,
            control.generation)
        assert participant.state==PauseParticipantState.REQUESTED
        await store.set_turn_state(turn.id,TurnState.FAILED,
            error_code='infrastructure_capacity_wait',error_detail='capacity unavailable')
        await store.repair_thread_heads()
        participant=await store.get_pause_participant(thread.id,'primary_harness',turn.id,
            control.generation)
        current=await store.get_thread_control(thread.id)
        assert participant.state==PauseParticipantState.FAILED
        assert participant.detail=={'terminal_state':'FAILED'}
        assert current.state==ThreadControlState.PAUSED and current.degraded
    finally:await store.close()


class PausableAdapter(BaseJobAdapter):
    def __init__(self):
        self.submits = 0; self.pauses = 0; self.cleanups = 0

    async def submit(self, context):
        self.submits += 1
        return AdapterSubmission("execution-" + str(self.submits), {"accepted": True})

    async def status(self, context, external_execution_id):
        return AdapterStatus(False)

    async def pause(self, context, external_execution_id):
        self.pauses += 1
        return {"checkpoint_ref": "sha256:" + "d" * 64,
            "native_thread_id": "native-child", "external_execution_id": external_execution_id}

    async def cleanup(self, context, external_execution_id):
        self.cleanups += 1


async def test_running_child_harness_checkpoints_and_resubmits_from_pause_snapshot(tmp_path):
    store, _, thread, turn = await setup(tmp_path)
    adapter = PausableAdapter()
    worker = JobWorker(store, {"child-harness": adapter}, node_id="node", worker_id="worker")
    try:
        _, jobs = await store.create_job_group("owner", JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id, idempotency_key="pause-running-child-group",
            jobs=(request("agent", JobKind.HARNESS),)))
        assert await worker.run_once() == 1
        assert (await store.get_job(jobs[0].id)).state == JobState.WAITING_EXTERNAL
        control = await store.request_thread_pause(thread.id, "owner")
        assert (await store.get_job(jobs[0].id)).state == JobState.PAUSING
        assert await worker.run_once() == 1
        paused = await store.get_pause_participant(thread.id, "child_harness",
            jobs[0].id, control.generation)
        assert paused.state == PauseParticipantState.PAUSED
        assert paused.checkpoint_ref == "sha256:" + "d" * 64
        assert paused.native_thread_id == "native-child"
        assert (await store.get_job(jobs[0].id)).state == JobState.PAUSED
        await store.complete_pause_participant(thread.id, control.generation,
            "primary_harness", turn.id, checkpoint_ref="sha256:" + "e" * 64)
        await store.resume_thread(thread.id, "owner")
        assert await worker.run_once() == 1
        assert adapter.submits == 2 and adapter.pauses == 1 and adapter.cleanups == 1
    finally:
        await store.close()


async def test_accepted_provider_can_reconcile_while_paused_but_unsubmitted_provider_cannot(tmp_path):
    store, _, thread, turn = await setup(tmp_path)
    try:
        _, jobs = await store.create_job_group("owner", JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id, idempotency_key="pause-provider-group",
            jobs=(request("accepted", JobKind.PROVIDER),
                request("not-submitted", JobKind.PROVIDER))))
        attempt = await store.claim_job(jobs[0].id, "seed", "node", 30)
        await store.record_job_acceptance(jobs[0].id, "seed", attempt.generation,
            external_execution_id="provider-task", receipt={"provider_id": "provider-task"})
        await store.defer_accepted_job(jobs[0].id, "seed", attempt.generation)
        await store.request_thread_pause(thread.id, "owner")
        claimed = await store.claim_available_jobs("reconciler", "node", 30, limit=4)
        assert [value.job_id for value in claimed] == [jobs[0].id]
        assert (await store.get_job(jobs[1].id)).generation == 0
    finally:
        await store.close()
