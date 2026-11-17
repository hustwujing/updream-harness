from datetime import timedelta

import pytest
from pydantic import ValidationError

from sandbox_harness.domain import identifier, now
from sandbox_harness.job_contracts import (
    HARD_JOB_GROUP_LIMIT,
    JobArtifactOutput,
    JobGroupCreate,
    JobGroupDraftCreate,
    JobGroupDraftState,
    JobGroupState,
    JobKind,
    JobRequest,
    JobState,
    OutputDisposition,
    TERMINAL_JOB_STATES,
)


def request(local_job_id: str = "render") -> JobRequest:
    return JobRequest(
        local_job_id=local_job_id,
        kind=JobKind.OPERATION,
        adapter="media.render",
        descriptor_id="media.render",
        descriptor_version="1",
        descriptor_digest="a" * 64,
        idempotency_key=f"job-{local_job_id}-key",
        parameters={"fps": 24},
        expected_outputs=[
            {"artifact_role": "preview", "output_disposition": "candidate"},
        ],
    )


def test_complete_job_and_group_state_contracts():
    assert {value.value for value in JobState} == {
        "CREATED", "QUEUED", "RUNNING", "WAITING_EXTERNAL", "CANCELLING",
        "PAUSING", "PAUSED", "SUCCEEDED", "FAILED", "CANCELLED", "BLOCKED", "UNKNOWN",
    }
    assert TERMINAL_JOB_STATES == {
        JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED,
        JobState.BLOCKED, JobState.UNKNOWN,
    }
    assert {value.value for value in JobGroupDraftState} == {
        "DRAFT", "AWAITING_CONFIRMATION", "REJECTED", "EXPIRED", "CONFIRMED",
    }
    assert {value.value for value in JobGroupState} == {
        "COMMITTED", "RUNNING", "TERMINAL", "CONTINUED",
    }


def test_request_digest_is_canonical_and_request_is_frozen():
    first = request()
    second = request().model_copy(update={"parameters": {"fps": 24}})
    assert first.request_digest == second.request_digest
    with pytest.raises(ValidationError):
        first.parameters = {"fps": 30}
    changed = request().model_copy(update={"parameters": {"fps": 30}})
    assert changed.request_digest != first.request_digest


def test_group_has_default_and_hard_member_bounds_and_no_dependencies():
    jobs = [request(f"job-{index}") for index in range(33)]
    group = JobGroupCreate(
        thread_id="a" * 32,
        origin_turn_id="b" * 32,
        idempotency_key="group-create-key",
        jobs=jobs,
    )
    with pytest.raises(ValueError, match="configured_job_group_limit"):
        group.validate_member_limit()
    group.validate_member_limit(33)
    with pytest.raises(ValidationError):
        JobGroupCreate(
            thread_id="a" * 32,
            origin_turn_id="b" * 32,
            idempotency_key="group-hard-limit",
            jobs=[request(f"hard-{index}") for index in range(HARD_JOB_GROUP_LIMIT + 1)],
        )
    with pytest.raises(ValidationError):
        JobRequest.model_validate({**request().model_dump(), "dependencies": ["another-job"]})


def test_replacement_and_reconciliation_are_explicit_and_mutually_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        request().model_copy(update={
            "replaces_job_id": identifier(), "reconciles_job_id": identifier(),
        }).model_validate(
            request().model_dump() | {
                "replaces_job_id": identifier(), "reconciles_job_id": identifier(),
            }
        )


def test_artifact_role_is_independent_from_output_disposition():
    output = JobArtifactOutput(
        artifact_id=identifier(),
        artifact_role="evidence_report",
        item_key="report-1",
        position=0,
        output_disposition=OutputDisposition.WORKSPACE,
        sha256="b" * 64,
        size=12,
        mime="application/pdf",
        object_ref="objects/report.pdf",
    )
    assert output.artifact_role == "evidence_report"
    assert output.output_disposition == OutputDisposition.WORKSPACE


def test_draft_digest_covers_jobs_and_expiry():
    expiry = now() + timedelta(minutes=15)
    draft = JobGroupDraftCreate(
        thread_id="a" * 32,
        origin_turn_id="b" * 32,
        idempotency_key="draft-create-key",
        jobs=[request()],
        expires_at=expiry,
        requires_confirmation=True,
    )
    assert len(draft.request_digest) == 64
    assert draft.request_digest != draft.model_copy(
        update={"expires_at": expiry + timedelta(seconds=1)}
    ).request_digest
