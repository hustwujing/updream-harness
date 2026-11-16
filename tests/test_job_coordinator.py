from datetime import timedelta

import pytest

from sandbox_harness.domain import identifier, now
from sandbox_harness.job_catalog import JobCatalog, JobCatalogPayload
from sandbox_harness.job_contracts import JobGroupCreate, JobKind, JobRequest, JobState
from sandbox_harness.job_coordinator import JobCoordinator, JobGroupDraftUpdate
from sandbox_harness.store import Store


def catalog() -> JobCatalog:
    payload=JobCatalogPayload.model_validate({"schema_version":1,"operations":[{
        "id":"free.compute","version":"1","digest":"a"*64,"execution_mode":"durable",
        "timeout_seconds":300,"parameter_schema":{"type":"object","properties":{},"additionalProperties":False},
        "input_roles":[],"output_roles":["result"],"required_capabilities":[],"network_origins":[],
        "resources":{"cpu":1,"memory_mb":128,"gpu":0},"side_effects":False,"billable":False,
        "creates_artifacts":True,"retry_policy":{"max_pre_accept_attempts":2},
        "cancellation_policy":{"supported":True},"reconciliation_policy":{"supported":True}
    },{
        "id":"inline.inspect","version":"1","digest":"b"*64,"execution_mode":"inline",
        "timeout_seconds":5,"parameter_schema":{"type":"object","properties":{},"additionalProperties":False},
        "input_roles":[],"output_roles":[],"required_capabilities":[],"network_origins":[],
        "resources":{"cpu":1,"memory_mb":128,"gpu":0},"side_effects":False,"billable":False,
        "creates_artifacts":False,"retry_policy":{"max_pre_accept_attempts":1},
        "cancellation_policy":{"supported":False},"reconciliation_policy":{"supported":False}
    }],"capabilities":[{
        "id":"image.generate","version":"1","digest":"c"*64,"operations":["submit","status"],
        "parameter_schema":{"type":"object","properties":{"prompt":{"type":"string"}},"additionalProperties":False},
        "input_roles":[],"output_roles":["images"],"network_origins":["image-provider"],
        "resources":{"cpu":1,"memory_mb":128,"gpu":0},"billable":True,"side_effects":True,
        "retry_policy":{"max_pre_accept_attempts":2},"cancellation_policy":{"supported":True},
        "reconciliation_policy":{"supported":True}
    }]})
    return JobCatalog(payload,"d"*64)


def job(local_id: str, descriptor: str = "free.compute") -> JobRequest:
    kind=JobKind.PROVIDER if descriptor=="image.generate" else JobKind.OPERATION
    digest={"free.compute":"a"*64,"inline.inspect":"b"*64,"image.generate":"c"*64}[descriptor]
    return JobRequest(local_job_id=local_id,kind=kind,adapter=descriptor,
        descriptor_id=descriptor,descriptor_version="1",descriptor_digest=digest,
        idempotency_key=f"job-{local_id}-key",parameters={"prompt":"one"} if kind==JobKind.PROVIDER else {})


async def setup(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/'coordinator.db'));await store.initialize()
    return store,JobCoordinator(store,catalog(),draft_ttl_seconds=300)


async def test_free_durable_group_auto_commits_but_inline_operation_is_rejected(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        value=JobGroupCreate(thread_id="a"*32,origin_turn_id="b"*32,
            idempotency_key="free-group-key",jobs=[job("free")])
        prepared=await coordinator.prepare_group("owner",value)
        assert prepared.mode=="committed" and prepared.group.member_count==1
        assert prepared.jobs[0].state==JobState.CREATED
        with pytest.raises(ValueError,match="inline Operation"):
            await coordinator.prepare_group("owner",JobGroupCreate(
                thread_id="a"*32,origin_turn_id="b"*32,
                idempotency_key="inline-group-key",jobs=[job("inline","inline.inspect")]))
    finally:await store.close()


async def test_one_paid_member_holds_complete_mixed_batch_until_single_confirmation(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        value=JobGroupCreate(thread_id="a"*32,origin_turn_id="b"*32,
            idempotency_key="mixed-group-key",jobs=[job("free"),job("paid","image.generate")])
        prepared=await coordinator.prepare_group("owner",value)
        assert prepared.mode=="draft" and prepared.draft.requires_confirmation
        assert await store.list_jobs_for_thread("a"*32)==[]
        confirmed=await coordinator.confirm_group(prepared.draft.id,"owner")
        assert confirmed.group.member_count==2
        assert {value.local_job_id for value in confirmed.jobs}=={"free","paid"}
    finally:await store.close()


async def test_edit_remove_uses_optimistic_digest_and_confirmation_freezes_result(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        prepared=await coordinator.prepare_group("owner",JobGroupCreate(
            thread_id="a"*32,origin_turn_id="b"*32,idempotency_key="editable-group-key",
            jobs=[job("free"),job("paid","image.generate")]))
        edited=await coordinator.edit_group(prepared.draft.id,"owner",JobGroupDraftUpdate(
            expected_digest=prepared.draft.request_digest,jobs=(job("paid","image.generate"),)))
        assert edited.request_digest!=prepared.draft.request_digest
        with pytest.raises(RuntimeError,match="draft_digest_conflict"):
            await coordinator.edit_group(edited.id,"owner",JobGroupDraftUpdate(
                expected_digest=prepared.draft.request_digest,jobs=(job("free"),)))
        confirmed=await coordinator.confirm_group(edited.id,"owner")
        assert [value.local_job_id for value in confirmed.jobs]==["paid"]
        with pytest.raises(RuntimeError,match="draft_not_editable"):
            await coordinator.edit_group(edited.id,"owner",JobGroupDraftUpdate(
                expected_digest=edited.request_digest,jobs=(job("free"),)))
    finally:await store.close()


async def test_reject_expire_and_cancel_scopes_are_truthful(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        rejected=await coordinator.prepare_group("owner",JobGroupCreate(
            thread_id="a"*32,origin_turn_id="b"*32,idempotency_key="reject-group-key",
            jobs=[job("paid","image.generate")]))
        assert (await coordinator.reject_group(rejected.draft.id,"owner")).state.value=="REJECTED"
        assert await store.list_jobs_for_thread("a"*32)==[]
        committed=await coordinator.prepare_group("owner",JobGroupCreate(
            thread_id="a"*32,origin_turn_id="b"*32,idempotency_key="cancel-group-key",
            jobs=[job("free-one"),job("free-two")]))
        cancelled=await coordinator.cancel_job(committed.jobs[0].id,"owner")
        assert cancelled.cancel_requested and cancelled.state==JobState.CANCELLING
        group_jobs=await coordinator.cancel_group(committed.group.id,"owner")
        assert all(value.cancel_requested for value in group_jobs)
    finally:await store.close()
