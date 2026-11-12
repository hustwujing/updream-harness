from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pathlib import Path
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
    ITEM_UPDATED = "item.updated"
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


class ThreadRead(BaseModel):
    turn_id: str = Field(pattern=r"^[0-9a-f]{32}$")


class AccountView(BaseModel):
    id: str
    owner_id: str = Field(exclude=True)
    username: str
    disabled: bool = False
    avatar_version: int = 0
    created_at: datetime
    updated_at: datetime


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class RegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=256)


class ChangeUsernameRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    current_password: str = Field(min_length=1, max_length=256)
    username: str = Field(min_length=3, max_length=64)


class ChangePasswordRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    current_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=8, max_length=256)


class UpdateAccountRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    current_password: str = Field(min_length=1, max_length=256)
    username: str | None = Field(default=None, min_length=3, max_length=64)
    new_password: str | None = Field(default=None, min_length=8, max_length=256)

    @model_validator(mode="after")
    def has_change(self):
        if self.username is None and self.new_password is None:
            raise ValueError("At least one profile change is required")
        return self


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
    last_read_turn_id: str = ""
    archived_at: datetime | None = None
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


def valid_attachment_name(value: str) -> bool:
    return bool(value and len(value) <= 255 and value not in {".", ".."} and Path(value).name == value
        and not any(character in value for character in '/\\"\r\n\0')
        and not any(ord(character) < 32 or ord(character) == 127 for character in value))


class TurnCreate(BaseModel):
    message: str = Field(min_length=1, max_length=200_000)
    idempotency_key: str = Field(min_length=8, max_length=160)
    attachments: list[Attachment] = Field(default_factory=list, max_length=64)
    harness: Harness | None = None
    model: str | None = Field(default=None, min_length=1, max_length=160)
    reasoning_level: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    delivery_mode: Literal["start", "queue", "steer"] = "start"

    @model_validator(mode="after")
    def safe_attachment_names(self):
        if any(not valid_attachment_name(item.name) for item in self.attachments):
            raise ValueError("Invalid attachment filename")
        return self


class TurnInputCreate(BaseModel):
    message: str = Field(min_length=1, max_length=200_000)
    idempotency_key: str = Field(min_length=8, max_length=160)
    attachments: list[Attachment] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def safe_attachment_names(self):
        if any(not valid_attachment_name(item.name) for item in self.attachments):
            raise ValueError("Invalid attachment filename")
        return self


class TurnInputView(BaseModel):
    id: str
    turn_id: str
    sequence: int = Field(ge=1)
    idempotency_key: str
    message: str
    attachments: list[Attachment]
    status: Literal["pending", "delivered", "unknown", "applied", "failed"]
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
    network_access: Literal["restricted", "public"] = "restricted"
    internal: bool = False
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
    submit_traceparent: str = ""
    last_traceparent: str = ""
    cancel_requested: bool = False
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
    traceparent: str = ""
    thread_id: str
    turn_id: str
    sandbox_id: str
    harness: Harness
    model: str
    routes: list[str]
    network_access: Literal["restricted", "public"] = "restricted"
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


class MediaDraftCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["image", "video", "audio", "model3d"]
    model: str = Field(min_length=1, max_length=160)
    generation_mode: Literal["text_to_image", "reference_image", "text_to_video", "first_frame",
        "first_last_frame", "multimodal_reference", "edit_video", "extend_video", "text_to_audio",
        "text_to_model", "image_to_model", "multiview_to_model", "texture_model", "decimate_model",
        "convert_model"] | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    references: list["MediaReferenceInput"] = Field(default_factory=list, max_length=15)
    idempotency_key: str = Field(min_length=8, max_length=160)
    turn_id: str = Field(default="", pattern=r"^$|^[0-9a-f]{32}$")
    output_role: Literal["candidate", "workspace", "delivery"] = "candidate"

    @model_validator(mode="after")
    def default_and_match_mode(self):
        defaults = {"image": "text_to_image", "video": "text_to_video", "audio": "text_to_audio",
            "model3d": "text_to_model"}
        if self.generation_mode is None:
            self.generation_mode = defaults[self.kind]
        if not self.generation_mode.startswith({"image": ("text_to_image", "reference_image"),
                "video": ("text_to_video", "first_frame", "first_last_frame", "multimodal_reference",
                    "edit_video", "extend_video"),
                "audio": ("text_to_audio",),
                "model3d": ("text_to_model", "image_to_model", "multiview_to_model", "texture_model",
                    "decimate_model", "convert_model")}[self.kind]):
            raise ValueError("Generation mode does not match media kind")
        return self


class MediaReferenceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_file_id: str = Field(default="", pattern=r"^$|^[0-9a-f]{32}$")
    project_path: str = Field(default="", max_length=1000)
    role: Literal["reference_image", "reference_video", "reference_audio", "first_frame", "last_frame",
        "front_view", "left_view", "back_view", "right_view"]
    usage: Literal["subject", "style", "composition", "action", "camera", "audio",
        "edit_source", "extend_source"] = "subject"

    @model_validator(mode="after")
    def exactly_one_selector(self):
        if bool(self.project_file_id) == bool(self.project_path):
            raise ValueError("Reference requires exactly one project file selector")
        return self


class MediaReferenceView(BaseModel):
    project_file_id: str
    path: str
    version: int = Field(ge=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=1)
    mime: str
    role: Literal["reference_image", "reference_video", "reference_audio", "first_frame", "last_frame",
        "front_view", "left_view", "back_view", "right_view"]
    usage: Literal["subject", "style", "composition", "action", "camera", "audio",
        "edit_source", "extend_source"]
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None
    fps: float | None = None
    codec: str = ""
    audio_codec: str = ""
    object_ref: str = Field(default="", exclude=True)


class MediaReferenceUploadCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=255)
    kind: Literal["image", "video", "audio"]
    mime: str = Field(min_length=1, max_length=200)
    size: int = Field(gt=0, le=200 * 1024 * 1024)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def safe_name_and_mime(self):
        if not valid_attachment_name(self.name):
            raise ValueError("Invalid media reference filename")
        if not self.mime.startswith(self.kind + "/"):
            raise ValueError("Media reference MIME does not match kind")
        return self


class MediaReferenceUploadView(BaseModel):
    id: str
    thread_id: str
    name: str
    kind: Literal["image", "video", "audio"]
    mime: str
    size: int
    sha256: str
    object_key: str
    public_url: str
    status: Literal["pending", "completed", "expired", "failed"]
    project_file_id: str = ""
    expires_at: datetime


class MediaDraftUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str | None = Field(default=None, min_length=1, max_length=160)
    generation_mode: Literal["text_to_image", "reference_image", "text_to_video", "first_frame",
        "first_last_frame", "multimodal_reference", "edit_video", "extend_video", "text_to_audio",
        "text_to_model", "image_to_model", "multiview_to_model", "texture_model", "decimate_model",
        "convert_model"] | None = None
    parameters: dict[str, Any] | None = None
    references: list[MediaReferenceInput] | None = Field(default=None, max_length=15)


class MediaDraftView(BaseModel):
    id: str
    thread_id: str
    kind: Literal["image", "video", "audio", "model3d"]
    model: str
    generation_mode: str
    parameters: dict[str, Any]
    references: list[MediaReferenceView] = Field(default_factory=list)
    status: Literal["awaiting_confirmation", "confirmed", "cancelled", "expired"]
    expires_at: datetime
    job_id: str = ""
    output_role: Literal["candidate", "workspace", "delivery"] = "candidate"


class MediaJobOutputView(BaseModel):
    id: str
    role: Literal["primary", "preview", "auxiliary"]
    filename: str
    mime: str
    size: int
    artifact_id: str = ""
    project_file_id: str = ""


class MediaJobView(BaseModel):
    id: str
    thread_id: str
    turn_id: str = ""
    kind: Literal["image", "video", "audio", "model3d"]
    model: str
    generation_mode: str = ""
    references: list[MediaReferenceView] = Field(default_factory=list)
    provider: str
    adapter: str
    execution_mode: Literal["sync", "async"]
    cancellation_mode: Literal["supported", "unsupported"]
    status: str
    phase: str = ""
    progress: int | None = None
    cancelable: bool = False
    cancel_reason: str = ""
    artifact_ids: list[str] = Field(default_factory=list)
    outputs: list[MediaJobOutputView] = Field(default_factory=list)
    credits_consumed: float | None = None
    output_role: Literal["candidate", "workspace", "delivery"] = "candidate"
    created_at: datetime
    finished_at: datetime | None = None
