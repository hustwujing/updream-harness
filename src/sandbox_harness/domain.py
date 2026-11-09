from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


def now() -> datetime:
    return datetime.now(timezone.utc)


def identifier() -> str:
    return uuid4().hex


class Harness(StrEnum):
    CODEX = "codex"
    CLAUDE = "claude"
    DEEPSEEK = "deepseek"


class TurnState(StrEnum):
    QUEUED = "QUEUED"
    RESTORING = "RESTORING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    AWAITING_USER = "AWAITING_USER"
    DRAINING_INPUTS = "DRAINING_INPUTS"
    CHECKPOINTING = "CHECKPOINTING"
    WAITING_USER = "WAITING_USER"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    CLEANING = "CLEANING"


TERMINAL = {TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED}


class EventKind(StrEnum):
    TEXT_DELTA = "text_delta"
    STEP_STARTED = "step_started"
    STEP_PROGRESS = "step_progress"
    STEP_FINISHED = "step_finished"
    TOOL_STARTED = "tool_started"
    TOOL_OUTPUT = "tool_output"
    TOOL_FINISHED = "tool_finished"
    ARTIFACT_CREATED = "artifact_created"
    WARNING = "warning"
    ERROR = "error"
    WAITING_USER = "waiting_user"
    DONE = "done"
    STATE = "state"


class ItemType(StrEnum):
    USER_MESSAGE = "user_message"
    AGENT_MESSAGE = "agent_message"
    REASONING = "reasoning"
    PLAN = "plan"
    COMMAND = "command"
    TOOL = "tool"
    FILE_CHANGE = "file_change"
    ARTIFACT = "artifact"
    APPROVAL = "approval"
    WARNING = "warning"
    ERROR = "error"


class Item(BaseModel):
    id: str
    thread_id: str
    turn_id: str
    sequence: int = Field(ge=1)
    type: ItemType
    status: str
    summary: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class ThreadCreate(BaseModel):
    harness: Harness
    model: str = Field(min_length=1, max_length=160)
    reasoning_level: str = Field(default="standard", pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    title: str = Field(default="New session", min_length=1, max_length=200)


class ThreadView(BaseModel):
    id: str
    harness: Harness
    model: str
    reasoning_level: str
    reasoning_effort: str
    title: str
    checkpoint_ref: str = ""
    native_thread_id: str = ""
    active_turn_id: str = ""
    created_at: datetime
    updated_at: datetime
    owner_id: str = Field(default="", exclude=True)


class Attachment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=255)
    object_ref: str = Field(min_length=1, max_length=1000)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    mime: str = Field(default="application/octet-stream", max_length=200)


class TurnCreate(BaseModel):
    message: str = Field(min_length=1, max_length=200_000)
    idempotency_key: str = Field(min_length=8, max_length=160)
    attachments: list[Attachment] = Field(default_factory=list, max_length=64)
    harness: Harness | None = None
    model: str | None = Field(default=None, min_length=1, max_length=160)
    reasoning_level: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    delivery_mode: Literal["start", "queue", "steer"] = "start"


class TurnInputCreate(BaseModel):
    message: str = Field(min_length=1, max_length=200_000)
    idempotency_key: str = Field(min_length=8, max_length=160)
    attachments: list[Attachment] = Field(default_factory=list, max_length=64)


class TurnInputView(BaseModel):
    id: str
    turn_id: str
    sequence: int = Field(ge=1)
    idempotency_key: str
    message: str
    attachments: list[Attachment]
    status: Literal["pending", "delivered", "applied", "failed"]
    delivery_mode: str = "pending"
    created_at: datetime
    updated_at: datetime


class TurnView(BaseModel):
    id: str
    thread_id: str
    idempotency_key: str
    message: str
    attachments: list[Attachment]
    harness: Harness
    model: str
    reasoning_level: str
    reasoning_effort: str
    delivery_mode: Literal["start", "queue", "steer"]
    state: TurnState
    sandbox_id: str = ""
    error_code: str = ""
    error_detail: str = ""
    lease_owner: str = ""
    lease_expires_at: datetime | None = None
    recovery_generation: int = 0
    runtime_sequence: int = 0
    runtime_execution_id: str = ""
    capability_nonce: str = ""
    pending_final_state: str = ""
    created_at: datetime
    updated_at: datetime


class Event(BaseModel):
    turn_id: str
    sequence: int = Field(ge=1)
    kind: EventKind
    item_id: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=now)


class Artifact(BaseModel):
    id: str
    turn_id: str
    path: str
    sha256: str
    size: int
    mime: str
    object_ref: str
    created_at: datetime


class ProjectFile(BaseModel):
    id: str
    thread_id: str
    path: str
    version: int = Field(ge=1)
    source_type: Literal["upload", "artifact"]
    source_turn_id: str = ""
    sha256: str
    size: int = Field(ge=0)
    mime: str
    object_ref: str = Field(exclude=True)
    deleted: bool = False
    created_at: datetime


class ProjectFileRename(BaseModel):
    path: str = Field(min_length=1, max_length=1000)


class Interaction(BaseModel):
    id: str
    thread_id: str
    turn_id: str
    runtime_execution_id: str
    runtime_interaction_id: str
    kind: Literal["approval", "question"]
    status: Literal["pending", "responding", "responded", "expired"]
    request: dict[str, Any] = Field(default_factory=dict)
    response: dict[str, Any] = Field(default_factory=dict)
    response_key: str = ""
    created_at: datetime
    updated_at: datetime


class InteractionResponse(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=160)
    action: Literal["approve", "reject", "answer"]
    answers: dict[str, str] = Field(default_factory=dict)


class RuntimeEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: EventKind
    payload: dict[str, Any] = Field(default_factory=dict)
    runtime_sequence: int = Field(default=0, ge=0)


class RuntimeResult(BaseModel):
    status: Literal["completed", "waiting_user", "failed", "cancelled"]
    native_thread_id: str = ""
    error_code: str = ""
    error_detail: str = ""
    input_ids: list[str] = Field(default_factory=list)
    runtime_sequence: int = Field(default=0, ge=0)


class CapabilityClaims(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[2] = 2
    thread_id: str
    turn_id: str
    sandbox_id: str
    harness: Harness
    model: str
    routes: list[str]
    max_requests: int = Field(ge=1)
    max_public_requests: int = Field(ge=1)
    token_budget: int = Field(ge=1)
    issued_at: int
    expires_at: int
    nonce: str

    @model_validator(mode="after")
    def valid_time(self):
        if self.expires_at <= self.issued_at:
            raise ValueError("Capability expiration must follow issuance")
        return self
