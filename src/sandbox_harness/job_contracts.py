from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


DEFAULT_JOB_GROUP_LIMIT = 32
HARD_JOB_GROUP_LIMIT = 100


class JobState(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING_EXTERNAL = "WAITING_EXTERNAL"
    CANCELLING = "CANCELLING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


TERMINAL_JOB_STATES = {
    JobState.SUCCEEDED,
    JobState.FAILED,
    JobState.CANCELLED,
    JobState.BLOCKED,
    JobState.UNKNOWN,
}


class JobGroupDraftState(StrEnum):
    DRAFT = "DRAFT"
    AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CONFIRMED = "CONFIRMED"


class JobGroupState(StrEnum):
    COMMITTED = "COMMITTED"
    RUNNING = "RUNNING"
    TERMINAL = "TERMINAL"
    CONTINUED = "CONTINUED"


class JobKind(StrEnum):
    PROVIDER = "provider"
    OPERATION = "operation"
    HARNESS = "harness"
    SITE = "site"
    DOCUMENT = "document"


class OutputDisposition(StrEnum):
    CANDIDATE = "candidate"
    WORKSPACE = "workspace"
    DELIVERY = "delivery"


class JobArtifactInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    artifact_role: str = Field(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$")


class JobExpectedOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_role: str = Field(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$")
    output_disposition: OutputDisposition = OutputDisposition.CANDIDATE
    min_items: int = Field(default=0, ge=0, le=1000)
    max_items: int = Field(default=1, ge=1, le=1000)
    mime_patterns: tuple[str, ...] = ()
    max_item_bytes: int = Field(default=512 * 1024 * 1024, ge=1)
    max_total_bytes: int = Field(default=2 * 1024 * 1024 * 1024, ge=1)
    validators: tuple[str, ...] = ()
    requires_review: bool = False

    @model_validator(mode="after")
    def valid_cardinality(self):
        if self.min_items > self.max_items:
            raise ValueError("min_items cannot exceed max_items")
        return self


def _canonical_digest(value: dict[str, Any]) -> str:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    local_job_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    kind: JobKind
    adapter: str = Field(min_length=1, max_length=160)
    descriptor_id: str = Field(min_length=1, max_length=160)
    descriptor_version: str = Field(min_length=1, max_length=64)
    descriptor_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    idempotency_key: str = Field(min_length=8, max_length=160)
    parameters: dict[str, Any] = Field(default_factory=dict)
    input_artifacts: tuple[JobArtifactInput, ...] = ()
    expected_outputs: tuple[JobExpectedOutput, ...] = ()
    child_profile_id: str = Field(default="", max_length=100)
    runtime_profile_id: str = Field(default="", max_length=100)
    skill_ids: tuple[str, ...] = ()
    resource_requirements: dict[str, Any] = Field(default_factory=dict)
    network_origins: tuple[str, ...] = ()
    budget_requirements: dict[str, int] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
    retry_policy: dict[str, Any] = Field(default_factory=dict)
    cancellation_policy: dict[str, Any] = Field(default_factory=dict)
    reconciliation_policy: dict[str, Any] = Field(default_factory=dict)
    replaces_job_id: str = Field(default="", pattern=r"^$|^[0-9a-f]{32}$")
    reconciles_job_id: str = Field(default="", pattern=r"^$|^[0-9a-f]{32}$")

    @model_validator(mode="after")
    def valid_lineage(self):
        if self.replaces_job_id and self.reconciles_job_id:
            raise ValueError("replaces_job_id and reconciles_job_id are mutually exclusive")
        roles = [output.artifact_role for output in self.expected_outputs]
        if len(roles) != len(set(roles)):
            raise ValueError("expected output artifact roles must be unique")
        for label, policy in (("cancellation",self.cancellation_policy),
                              ("reconciliation",self.reconciliation_policy)):
            if policy and not isinstance(policy.get("supported"),bool):
                raise ValueError(label+" policy requires boolean supported")
        return self

    @property
    def request_digest(self) -> str:
        return _canonical_digest(self.model_dump(mode="json"))


class JobGroupCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    thread_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    origin_turn_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    idempotency_key: str = Field(min_length=8, max_length=160)
    jobs: tuple[JobRequest, ...] = Field(min_length=1, max_length=HARD_JOB_GROUP_LIMIT)
    authorization_snapshot: dict[str, Any] = Field(default_factory=dict)
    suppress_continuation: bool = False

    @model_validator(mode="after")
    def unique_members(self):
        local_ids = [job.local_job_id for job in self.jobs]
        if len(local_ids) != len(set(local_ids)):
            raise ValueError("local_job_id values must be unique within a group")
        keys = [job.idempotency_key for job in self.jobs]
        if len(keys) != len(set(keys)):
            raise ValueError("job idempotency keys must be unique within a group")
        return self

    def validate_member_limit(self, configured_limit: int = DEFAULT_JOB_GROUP_LIMIT) -> None:
        if configured_limit < 1 or configured_limit > HARD_JOB_GROUP_LIMIT:
            raise ValueError("invalid configured_job_group_limit")
        if len(self.jobs) > configured_limit:
            raise ValueError("configured_job_group_limit exceeded")

    @property
    def request_digest(self) -> str:
        return _canonical_digest(self.model_dump(mode="json"))


class JobGroupDraftCreate(JobGroupCreate):
    expires_at: datetime
    requires_confirmation: bool = True
    authorization_gaps: tuple[str, ...] = ()

    @model_validator(mode="after")
    def timezone_aware_expiry(self):
        if self.expires_at.tzinfo is None:
            raise ValueError("expires_at must be timezone-aware")
        return self


class JobResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    state: JobState
    data: dict[str, Any] = Field(default_factory=dict)
    error_code: str = Field(default="", max_length=160)
    error_detail: str = ""

    @model_validator(mode="after")
    def terminal_only(self):
        if self.state not in TERMINAL_JOB_STATES:
            raise ValueError("JobResult requires a terminal state")
        return self


class JobArtifactOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    artifact_role: str = Field(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$")
    item_key: str = Field(min_length=1, max_length=160)
    filename: str = Field(default="artifact.bin", min_length=1, max_length=255)
    position: int = Field(ge=0)
    output_disposition: OutputDisposition
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    mime: str = Field(min_length=1, max_length=200)
    object_ref: str = Field(min_length=1, max_length=1000)


class JobAttemptReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    job_id: str
    generation: int = Field(ge=1)
    worker_id: str
    node_id: str
    executor_kind: str
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    accepted_at: datetime | None = None
    external_execution_id: str = ""
    acceptance_receipt: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime
    finished_at: datetime | None = None
    outcome: str = ""
    error_code: str = ""
    error_detail: str = ""


class JobGroupDraftView(BaseModel):
    id: str
    owner_id: str
    thread_id: str
    origin_turn_id: str
    idempotency_key: str
    request_digest: str
    state: JobGroupDraftState
    requires_confirmation: bool
    authorization_gaps: tuple[str, ...]
    expires_at: datetime
    confirmed_group_id: str = ""
    created_at: datetime
    updated_at: datetime


class JobGroupView(BaseModel):
    id: str
    owner_id: str
    thread_id: str
    origin_turn_id: str
    draft_id: str = ""
    idempotency_key: str
    request_digest: str
    state: JobGroupState
    member_count: int
    suppress_continuation: bool = False
    terminal_at: datetime | None = None
    continued_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class JobView(BaseModel):
    id: str
    group_id: str
    local_job_id: str
    owner_id: str
    thread_id: str
    origin_turn_id: str
    kind: JobKind
    adapter: str
    idempotency_key: str
    request_digest: str
    state: JobState
    generation: int = 0
    cancel_requested: bool = False
    replaces_job_id: str = ""
    reconciles_job_id: str = ""
    result: dict[str, Any] = Field(default_factory=dict)
    terminal_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class JobOutboxView(BaseModel):
    id: str
    group_id: str
    thread_id: str
    state: str
    result_digest: str
    continuation_turn_id: str = ""
    created_at: datetime
    updated_at: datetime


class BudgetAccountView(BaseModel):
    id: str
    owner_id: str
    thread_id: str
    budget_key: str
    limit: int
    reserved: int
    consumed: int
    created_at: datetime
    updated_at: datetime


class BudgetReservationView(BaseModel):
    id: str
    account_id: str
    group_id: str
    authorization_snapshot_id: str
    budget_key: str
    reserved: int
    consumed: int
    released: int
    state: str
    created_at: datetime
    updated_at: datetime


class BudgetLedgerView(BaseModel):
    id: str
    reservation_id: str
    group_id: str
    job_id: str
    action: str
    amount: int
    idempotency_key: str
    detail: dict[str, Any]
    created_at: datetime
