"""Canonical ORM registry and pure persistence value helpers."""
from __future__ import annotations
import json
import hashlib
from datetime import (datetime, timezone)
from pathlib import (PurePosixPath)
from sqlalchemy import (BigInteger, Boolean, DateTime, Integer, String, Text, UniqueConstraint)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column)


class Base(DeclarativeBase):
    pass


def valid_project_file_path(path: str, source_type: str) -> bool:
    pure = PurePosixPath(path)
    expected_root = "assets" if source_type == "upload" else "artifacts" if source_type == "artifact" else ""
    return bool(expected_root and not pure.is_absolute() and len(pure.parts) >= 2
        and pure.parts[0] == expected_root and not any(part in {"", ".", ".."} for part in pure.parts))


def utc_datetime(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)


def canonical_digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


JOB_DATABASE_LOCK_ORDER = (
    "job_group_draft", "authorization_snapshot", "budget_account", "budget_reservation", "job_group",
    "job", "job_attempt", "job_lease", "job_artifact", "job_outbox",
)


SCHEMA_INITIALIZATION_LOCK_ID = 0x5348504152414C4C


class ThreadRow(Base):
    __tablename__ = "threads"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True, default="legacy-unowned", server_default="legacy-unowned")
    harness: Mapped[str] = mapped_column(String(20))
    model: Mapped[str] = mapped_column(String(160))
    reasoning_level: Mapped[str] = mapped_column(String(32))
    reasoning_effort: Mapped[str] = mapped_column(String(20))
    title: Mapped[str] = mapped_column(String(200))
    checkpoint_ref: Mapped[str] = mapped_column(Text, default="")
    native_thread_id: Mapped[str] = mapped_column(String(300), default="")
    active_turn_id: Mapped[str] = mapped_column(String(32), default="")
    last_read_turn_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    next_item_sequence: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ThreadControlRow(Base):
    __tablename__ = "thread_controls"
    thread_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(24), index=True)
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    operation_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    degraded: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    detail_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class PauseParticipantRow(Base):
    __tablename__ = "pause_participants"
    __table_args__ = (UniqueConstraint("thread_id", "generation", "kind", "participant_id"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    generation: Mapped[int] = mapped_column(Integer)
    kind: Mapped[str] = mapped_column(String(32))
    participant_id: Mapped[str] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(24), index=True)
    checkpoint_ref: Mapped[str] = mapped_column(Text, default="", server_default="")
    native_thread_id: Mapped[str] = mapped_column(String(300), default="", server_default="")
    external_execution_id: Mapped[str] = mapped_column(Text, default="", server_default="")
    detail_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TurnRow(Base):
    __tablename__ = "turns"
    __table_args__ = (UniqueConstraint("thread_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    message: Mapped[str] = mapped_column(Text)
    public_message: Mapped[str] = mapped_column(Text)
    attachments_json: Mapped[str] = mapped_column(Text, default="[]")
    harness: Mapped[str] = mapped_column(String(20))
    model: Mapped[str] = mapped_column(String(160))
    reasoning_level: Mapped[str] = mapped_column(String(32))
    reasoning_effort: Mapped[str] = mapped_column(String(20))
    delivery_mode: Mapped[str] = mapped_column(String(20))
    network_access: Mapped[str] = mapped_column(String(20), default="restricted", server_default="restricted")
    internal: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    state: Mapped[str] = mapped_column(String(24), index=True)
    sandbox_id: Mapped[str] = mapped_column(String(200), default="")
    sandbox_handle_json: Mapped[str] = mapped_column(Text, default="", server_default="")
    error_code: Mapped[str] = mapped_column(String(100), default="")
    error_detail: Mapped[str] = mapped_column(Text, default="")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    recovery_generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    runtime_sequence: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    runtime_execution_id: Mapped[str] = mapped_column(String(160), default="", server_default="")
    runtime_image_digest: Mapped[str] = mapped_column(Text, default="", server_default="")
    runtime_code_bundle_digest: Mapped[str] = mapped_column(String(64), default="", server_default="")
    runtime_bundle_digest: Mapped[str] = mapped_column(String(64), default="", server_default="")
    capability_nonce: Mapped[str] = mapped_column(String(64), default="", server_default="")
    submit_traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    last_traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    pending_final_state: Mapped[str] = mapped_column(String(24), default="", server_default="")
    project_files_frozen: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TurnOrderRow(Base):
    __tablename__ = "turn_order"
    __table_args__ = (UniqueConstraint("thread_id", "sequence"),)
    turn_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class EventRow(Base):
    __tablename__ = "events"
    turn_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(40))
    item_id: Mapped[str] = mapped_column(String(32), default="")
    payload_json: Mapped[str] = mapped_column(Text)
    source_execution_id: Mapped[str] = mapped_column(String(160), default="", server_default="")
    source_sequence: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TurnInputRow(Base):
    __tablename__ = "turn_inputs"
    __table_args__ = (UniqueConstraint("turn_id", "idempotency_key"), UniqueConstraint("turn_id", "sequence"))
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    turn_id: Mapped[str] = mapped_column(String(32), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    message: Mapped[str] = mapped_column(Text)
    attachments_json: Mapped[str] = mapped_column(Text, default="[]")
    status: Mapped[str] = mapped_column(String(20), index=True)
    delivery_mode: Mapped[str] = mapped_column(String(30), default="pending", server_default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ArtifactRow(Base):
    __tablename__ = "artifacts"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    turn_id: Mapped[str] = mapped_column(String(32), index=True)
    path: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    object_ref: Mapped[str] = mapped_column(Text)
    partial: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ItemRow(Base):
    __tablename__ = "items"
    __table_args__ = (UniqueConstraint("thread_id", "sequence"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    turn_id: Mapped[str] = mapped_column(String(32), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    type: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(30))
    summary: Mapped[str] = mapped_column(Text, default="")
    payload_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UploadRow(Base):
    __tablename__ = "uploads"
    owner_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    object_ref: Mapped[str] = mapped_column(Text, primary_key=True)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String(255))
    mime: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ProjectFileRow(Base):
    __tablename__ = "project_files"
    __table_args__ = (UniqueConstraint("thread_id", "path", "version"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    path: Mapped[str] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer)
    source_type: Mapped[str] = mapped_column(String(20))
    source_turn_id: Mapped[str] = mapped_column(String(32), default="")
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    object_ref: Mapped[str] = mapped_column(Text)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TurnProjectFileRow(Base):
    __tablename__ = "turn_project_files"
    __table_args__ = (UniqueConstraint("turn_id", "path"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    turn_id: Mapped[str] = mapped_column(String(32), index=True)
    project_file_id: Mapped[str] = mapped_column(String(32))
    path: Mapped[str] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer)
    source_type: Mapped[str] = mapped_column(String(20))
    source_turn_id: Mapped[str] = mapped_column(String(32), default="")
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    object_ref: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class InteractionRow(Base):
    __tablename__ = "interactions"
    __table_args__ = (UniqueConstraint("turn_id", "runtime_interaction_id"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    turn_id: Mapped[str] = mapped_column(String(32), index=True)
    runtime_execution_id: Mapped[str] = mapped_column(String(160))
    runtime_interaction_id: Mapped[str] = mapped_column(String(160))
    kind: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20), index=True)
    request_json: Mapped[str] = mapped_column(Text, default="{}")
    response_json: Mapped[str] = mapped_column(Text, default="{}")
    response_key: Mapped[str] = mapped_column(String(160), default="")
    response_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    response_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaDraftRow(Base):
    __tablename__ = "media_drafts"
    __table_args__ = (UniqueConstraint("thread_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    turn_id: Mapped[str] = mapped_column(String(32), default="", index=True)
    kind: Mapped[str] = mapped_column(String(16))
    model: Mapped[str] = mapped_column(String(160))
    generation_mode: Mapped[str] = mapped_column(String(40), default="", server_default="")
    parameters_json: Mapped[str] = mapped_column(Text, default="{}")
    references_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    idempotency_key: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    output_role: Mapped[str] = mapped_column(String(20), default="candidate", server_default="candidate")
    traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaReferenceUploadRow(Base):
    __tablename__ = "media_reference_uploads"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(255))
    kind: Mapped[str] = mapped_column(String(16))
    mime: Mapped[str] = mapped_column(String(200))
    size: Mapped[int] = mapped_column(Integer)
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    object_key: Mapped[str] = mapped_column(Text)
    public_url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), index=True)
    project_file_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaReferenceAssetRow(Base):
    __tablename__ = "media_reference_assets"
    sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16))
    mime: Mapped[str] = mapped_column(String(200))
    size: Mapped[int] = mapped_column(Integer)
    object_ref: Mapped[str] = mapped_column(Text)
    object_key: Mapped[str] = mapped_column(Text, unique=True)
    public_url: Mapped[str] = mapped_column(Text, unique=True)
    boss_status: Mapped[str] = mapped_column(String(20), default="pending", server_default="pending")
    ark_asset_id: Mapped[str] = mapped_column(String(160), default="", server_default="")
    ark_status: Mapped[str] = mapped_column(String(20), default="pending", server_default="pending")
    ark_error: Mapped[str] = mapped_column(String(200), default="", server_default="")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaVoiceCloneRow(Base):
    __tablename__ = "media_voice_clones"
    __table_args__ = (UniqueConstraint("owner_id", "sha256"), UniqueConstraint("voice_id"))
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    mime: Mapped[str] = mapped_column(String(200))
    size: Mapped[int] = mapped_column(Integer)
    object_ref: Mapped[str] = mapped_column(Text)
    voice_id: Mapped[str] = mapped_column(String(80))
    revision: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    provider_file_id: Mapped[str] = mapped_column(String(80), default="", server_default="")
    status: Mapped[str] = mapped_column(String(20), default="pending", server_default="pending")
    error_code: Mapped[str] = mapped_column(String(100), default="", server_default="")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaBatchRow(Base):
    __tablename__ = "media_batches"
    turn_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True, default="open", server_default="open")
    origin_finished: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    sealed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    continued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaJobRow(Base):
    __tablename__ = "media_jobs"
    __table_args__ = (UniqueConstraint("draft_id"), UniqueConstraint("thread_id", "idempotency_key"))
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    turn_id: Mapped[str] = mapped_column(String(32), default="", index=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True)
    source_job_id: Mapped[str | None] = mapped_column(String(32), nullable=True,
        unique=True, index=True)
    kind: Mapped[str] = mapped_column(String(16))
    model: Mapped[str] = mapped_column(String(160))
    generation_mode: Mapped[str] = mapped_column(String(40), default="", server_default="")
    provider: Mapped[str] = mapped_column(String(80))
    adapter: Mapped[str] = mapped_column(String(120))
    execution_mode: Mapped[str] = mapped_column(String(16))
    cancellation_mode: Mapped[str] = mapped_column(String(16))
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    status: Mapped[str] = mapped_column(String(32), index=True)
    phase: Mapped[str] = mapped_column(String(32), default="", server_default="")
    progress: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parameters_json: Mapped[str] = mapped_column(Text, default="{}")
    references_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    provider_job_id: Mapped[str] = mapped_column(String(200), default="", server_default="")
    provider_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    error_code: Mapped[str] = mapped_column(String(100), default="", server_default="")
    error_detail: Mapped[str] = mapped_column(Text, default="", server_default="")
    output_object_ref: Mapped[str] = mapped_column(Text, default="", server_default="")
    output_mime: Mapped[str] = mapped_column(String(200), default="", server_default="")
    output_size: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    credits_consumed: Mapped[str] = mapped_column(String(40), default="", server_default="")
    output_role: Mapped[str] = mapped_column(String(20), default="candidate", server_default="candidate")
    traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaJobOutputRow(Base):
    __tablename__ = "media_job_outputs"
    __table_args__ = (UniqueConstraint("job_id", "role"), UniqueConstraint("job_id", "filename"))
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    role: Mapped[str] = mapped_column(String(20))
    filename: Mapped[str] = mapped_column(String(255))
    object_ref: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    artifact_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    project_file_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaExecutionRow(Base):
    __tablename__ = "media_job_executions"
    job_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    provider_result_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    staged_outputs_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaOutboxRow(Base):
    __tablename__ = "media_job_outbox"
    __table_args__ = (UniqueConstraint("job_id", "event_type"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(20), index=True, default="pending", server_default="pending")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SiteProjectRow(Base):
    __tablename__ = "site_projects"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(120))
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(20), index=True, default="draft", server_default="draft")
    current_version_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    public_url: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SiteDraftRow(Base):
    __tablename__ = "site_drafts"
    __table_args__ = (UniqueConstraint("thread_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    turn_id: Mapped[str] = mapped_column(String(32), default="", index=True)
    project_id: Mapped[str] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(120))
    slug: Mapped[str] = mapped_column(String(64))
    archive_object_ref: Mapped[str] = mapped_column(Text)
    bundle_sha256: Mapped[str] = mapped_column(String(64))
    archive_size: Mapped[int] = mapped_column(Integer)
    file_count: Mapped[int] = mapped_column(Integer)
    total_size: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    request_digest: Mapped[str] = mapped_column(String(64), default="", server_default="")
    status: Mapped[str] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SiteDraftFileRow(Base):
    __tablename__ = "site_draft_files"
    __table_args__ = (UniqueConstraint("draft_id", "path"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True)
    path: Mapped[str] = mapped_column(String(300))
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    object_ref: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SiteDeployJobRow(Base):
    __tablename__ = "site_deploy_jobs"
    __table_args__ = (UniqueConstraint("draft_id"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    turn_id: Mapped[str] = mapped_column(String(32), default="", index=True)
    project_id: Mapped[str] = mapped_column(String(32), index=True)
    draft_id: Mapped[str] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(120))
    slug: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), index=True)
    phase: Mapped[str] = mapped_column(String(32), default="", server_default="")
    progress: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str] = mapped_column(String(100), default="", server_default="")
    error_detail: Mapped[str] = mapped_column(Text, default="", server_default="")
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SiteVersionRow(Base):
    __tablename__ = "site_versions"
    __table_args__ = (UniqueConstraint("project_id", "version"),
        UniqueConstraint("project_id", "bundle_sha256"))
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    project_id: Mapped[str] = mapped_column(String(32), index=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    version: Mapped[int] = mapped_column(Integer)
    bundle_sha256: Mapped[str] = mapped_column(String(64))
    file_count: Mapped[int] = mapped_column(Integer)
    total_size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SiteFileRow(Base):
    __tablename__ = "site_files"
    __table_args__ = (UniqueConstraint("version_id", "path"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    version_id: Mapped[str] = mapped_column(String(32), index=True)
    path: Mapped[str] = mapped_column(String(300))
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    object_ref: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SandboxControlRow(Base):
    __tablename__ = "sandbox_controls"
    thread_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    sandbox_id: Mapped[str] = mapped_column(String(200), index=True)
    sandbox_handle_json: Mapped[str] = mapped_column(Text, default="", server_default="")
    state: Mapped[str] = mapped_column(String(24), index=True)
    generation: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    capability_nonce: Mapped[str] = mapped_column(String(64), default="", server_default="")
    pty_session_id: Mapped[str] = mapped_column(String(160), default="", server_default="")
    release_phase: Mapped[str] = mapped_column(String(32), default="", server_default="")
    release_action: Mapped[str] = mapped_column(String(32), default="", server_default="")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SandboxPortGrantRow(Base):
    __tablename__ = "sandbox_port_grants"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    sandbox_id: Mapped[str] = mapped_column(String(200), index=True)
    sandbox_handle_json: Mapped[str] = mapped_column(Text, default="", server_default="")
    port: Mapped[int] = mapped_column(Integer)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SandboxAuditRow(Base):
    __tablename__ = "sandbox_audit_events"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    sandbox_id: Mapped[str] = mapped_column(String(200), default="", server_default="")
    action: Mapped[str] = mapped_column(String(80), index=True)
    detail_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UserRow(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(Text)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    avatar_object_ref: Mapped[str] = mapped_column(Text, default="", server_default="")
    avatar_mime: Mapped[str] = mapped_column(String(100), default="", server_default="")
    avatar_version: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AuthSessionRow(Base):
    __tablename__ = "auth_sessions"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class RegistrationIntentRow(Base):
    __tablename__ = "registration_intents"
    source_owner_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    username: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobGroupDraftRow(Base):
    __tablename__ = "job_group_drafts"
    __table_args__ = (UniqueConstraint("thread_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    origin_turn_id: Mapped[str] = mapped_column(String(32), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    request_digest: Mapped[str] = mapped_column(String(64))
    request_json: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(32), index=True)
    requires_confirmation: Mapped[bool] = mapped_column(Boolean)
    authorization_gaps_json: Mapped[str] = mapped_column(Text, default="[]")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    confirmed_group_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AuthorizationSnapshotRow(Base):
    __tablename__ = "authorization_snapshots"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    origin_turn_id: Mapped[str] = mapped_column(String(32))
    scope: Mapped[str] = mapped_column(String(24))
    version: Mapped[int] = mapped_column(Integer)
    snapshot_digest: Mapped[str] = mapped_column(String(64))
    snapshot_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobGroupRow(Base):
    __tablename__ = "job_groups"
    __table_args__ = (
        UniqueConstraint("thread_id", "idempotency_key"),
        UniqueConstraint("draft_id"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    origin_turn_id: Mapped[str] = mapped_column(String(32), index=True)
    draft_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    request_digest: Mapped[str] = mapped_column(String(64))
    authorization_snapshot_id: Mapped[str] = mapped_column(String(32))
    state: Mapped[str] = mapped_column(String(32), index=True)
    member_count: Mapped[int] = mapped_column(Integer)
    suppress_continuation: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    continued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobRow(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("group_id", "local_job_id"),
        UniqueConstraint("thread_id", "idempotency_key"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(32), index=True)
    local_job_id: Mapped[str] = mapped_column(String(100))
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    origin_turn_id: Mapped[str] = mapped_column(String(32), index=True)
    kind: Mapped[str] = mapped_column(String(24), index=True)
    adapter: Mapped[str] = mapped_column(String(160), index=True)
    descriptor_id: Mapped[str] = mapped_column(String(160))
    descriptor_version: Mapped[str] = mapped_column(String(64))
    descriptor_digest: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str] = mapped_column(String(160))
    request_digest: Mapped[str] = mapped_column(String(64))
    request_json: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(32), index=True)
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    allocation_replacements: Mapped[int] = mapped_column(Integer,default=0,server_default='0')
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    replaces_job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    reconciles_job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    result_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    result_digest: Mapped[str] = mapped_column(String(64), default="", server_default="")
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobAttemptRow(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (UniqueConstraint("job_id", "generation"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    generation: Mapped[int] = mapped_column(Integer)
    worker_id: Mapped[str] = mapped_column(String(64), index=True)
    node_id: Mapped[str] = mapped_column(String(160), default="", server_default="")
    executor_kind: Mapped[str] = mapped_column(String(32))
    retry_kind: Mapped[str] = mapped_column(String(32), default="initial", server_default="initial")
    request_digest: Mapped[str] = mapped_column(String(64))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acceptance_receipt_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    external_execution_id: Mapped[str] = mapped_column(Text, default="", server_default="")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str] = mapped_column(String(32), default="", server_default="")
    error_code: Mapped[str] = mapped_column(String(160), default="", server_default="")
    error_detail: Mapped[str] = mapped_column(Text, default="", server_default="")


class JobLeaseRow(Base):
    __tablename__ = "job_leases"
    job_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(32), unique=True)
    worker_id: Mapped[str] = mapped_column(String(64), index=True)
    generation: Mapped[int] = mapped_column(Integer)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class PlatformLeaderRow(Base):
    __tablename__ = "platform_leaders"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner: Mapped[str] = mapped_column(String(160), default="", server_default="")
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ProviderCircuitRow(Base):
    __tablename__ = "provider_circuits"
    id: Mapped[str] = mapped_column(String(200), primary_key=True)
    state: Mapped[str] = mapped_column(String(20), index=True, default="closed", server_default="closed")
    failure_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    opened_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    probe_owner: Mapped[str] = mapped_column(String(160), default="", server_default="")
    probe_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SandboxAllocationRow(Base):
    __tablename__ = 'sandbox_allocations'
    id: Mapped[str] = mapped_column(String(64),primary_key=True)
    namespace: Mapped[str] = mapped_column(String(64),index=True)
    provider: Mapped[str] = mapped_column(String(64))
    allocation_id: Mapped[str] = mapped_column(String(160))
    job_id: Mapped[str] = mapped_column(String(32),index=True)
    job_generation: Mapped[int] = mapped_column(Integer)
    request_json: Mapped[str] = mapped_column(Text)
    request_digest: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(24),index=True)
    handle_json: Mapped[str] = mapped_column(Text,default='{}',server_default='{}')
    error_code: Mapped[str] = mapped_column(String(80),default='',server_default='')
    claim_owner: Mapped[str] = mapped_column(String(64),default='',server_default='')
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    reconcile_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    reconcile_attempts: Mapped[int] = mapped_column(Integer,default=0,server_default='0')
    reconcile_token: Mapped[str] = mapped_column(String(64),default='',server_default='')
    reconcile_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True),nullable=True)
    recovery_policy_json: Mapped[str] = mapped_column(Text,default='{}',server_default='{}')
    traceparent: Mapped[str] = mapped_column(String(128),default='',server_default='')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobCleanupRow(Base):
    __tablename__ = "job_cleanups"
    __table_args__ = (UniqueConstraint("job_id","generation"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    adapter: Mapped[str] = mapped_column(String(160))
    generation: Mapped[int] = mapped_column(Integer)
    external_execution_id: Mapped[str] = mapped_column(Text)
    acceptance_receipt_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    state: Mapped[str] = mapped_column(String(24), default="pending", server_default="pending", index=True)
    phase: Mapped[str] = mapped_column(String(40), default="external_quiescence", server_default="external_quiescence")
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str] = mapped_column(Text, default="", server_default="")
    claim_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobArtifactRow(Base):
    __tablename__ = "job_artifacts"
    __table_args__ = (UniqueConstraint("job_id", "artifact_role", "item_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    artifact_id: Mapped[str] = mapped_column(String(32), unique=True)
    artifact_role: Mapped[str] = mapped_column(String(100))
    item_key: Mapped[str] = mapped_column(String(160))
    filename: Mapped[str] = mapped_column(String(255))
    position: Mapped[int] = mapped_column(Integer)
    requested_disposition: Mapped[str] = mapped_column(String(24))
    output_disposition: Mapped[str] = mapped_column(String(24))
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    mime: Mapped[str] = mapped_column(String(200))
    object_ref: Mapped[str] = mapped_column(Text)
    validation_receipts_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    review_receipts_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    state: Mapped[str] = mapped_column(String(24), default="committed", server_default="committed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReviewGateRow(Base):
    __tablename__ = "review_gates"
    reviewer_job_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    source_job_id: Mapped[str] = mapped_column(String(32), index=True)
    source_generation: Mapped[int] = mapped_column(Integer)
    artifact_role: Mapped[str] = mapped_column(String(100))
    artifact_ids_json: Mapped[str] = mapped_column(Text)
    artifact_digests_json: Mapped[str] = mapped_column(Text)
    required: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0")
    state: Mapped[str] = mapped_column(String(32), index=True, default="pending", server_default="pending")
    receipt_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    warning: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobOutboxRow(Base):
    __tablename__ = "job_outbox"
    __table_args__ = (UniqueConstraint("group_id"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(32), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(24), index=True)
    result_digest: Mapped[str] = mapped_column(String(64))
    result_json: Mapped[str] = mapped_column(Text)
    claim_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    claim_batch_digest: Mapped[str] = mapped_column(String(64), default="", server_default="")
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    continuation_turn_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ChildContinuationRow(Base):
    __tablename__ = "child_job_continuations"
    __table_args__ = (UniqueConstraint("group_id"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    parent_job_id: Mapped[str] = mapped_column(String(32), index=True)
    group_id: Mapped[str] = mapped_column(String(32), index=True)
    outbox_id: Mapped[str] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(24), index=True)
    result_json: Mapped[str] = mapped_column(Text)
    resume_execution_id: Mapped[str] = mapped_column(String(200), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UsageLedgerRow(Base):
    __tablename__ = "usage_ledger"
    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), default="", server_default="", index=True)
    turn_id: Mapped[str] = mapped_column(String(32), default="", server_default="", index=True)
    job_id: Mapped[str] = mapped_column(String(32), default="", server_default="", index=True)
    kind: Mapped[str] = mapped_column(String(40), index=True)
    amount: Mapped[int] = mapped_column(BigInteger)
    unit: Mapped[str] = mapped_column(String(24))
    source: Mapped[str] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class BudgetAccountRow(Base):
    __tablename__ = "budget_accounts"
    __table_args__ = (UniqueConstraint("owner_id", "thread_id", "budget_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    budget_key: Mapped[str] = mapped_column(String(160))
    limit: Mapped[int] = mapped_column(Integer)
    reserved: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    consumed: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class BudgetReservationRow(Base):
    __tablename__ = "budget_reservations"
    __table_args__ = (UniqueConstraint("group_id", "budget_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(32), index=True)
    group_id: Mapped[str] = mapped_column(String(32), index=True)
    authorization_snapshot_id: Mapped[str] = mapped_column(String(32), index=True)
    budget_key: Mapped[str] = mapped_column(String(160))
    reserved: Mapped[int] = mapped_column(Integer)
    consumed: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    released: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    state: Mapped[str] = mapped_column(String(24), default="reserved", server_default="reserved")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class BudgetLedgerRow(Base):
    __tablename__ = "budget_ledger"
    __table_args__ = (UniqueConstraint("reservation_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    reservation_id: Mapped[str] = mapped_column(String(32), index=True)
    group_id: Mapped[str] = mapped_column(String(32), index=True)
    job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    action: Mapped[str] = mapped_column(String(24))
    amount: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    detail_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class CapabilityLedgerRow(Base):
    __tablename__ = "capability_ledger"
    nonce: Mapped[str] = mapped_column(String(64), primary_key=True)
    claims_digest: Mapped[str] = mapped_column(String(64), default="", server_default="")
    parent_nonce: Mapped[str] = mapped_column(String(64), default="", server_default="", index=True)
    expires_at: Mapped[int] = mapped_column(Integer)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", index=True)
    max_requests: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_public_requests: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    request_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    public_request_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    token_budget: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    token_reserved: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    token_consumed: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ModelTokenReservationRow(Base):
    __tablename__ = "model_token_reservations"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    nonce: Mapped[str] = mapped_column(String(64), index=True)
    parent_nonce: Mapped[str] = mapped_column(String(64), default="", server_default="")
    requested_output: Mapped[int] = mapped_column(Integer)
    reserved: Mapped[int] = mapped_column(Integer)
    charged: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    state: Mapped[str] = mapped_column(String(20), default="reserved", server_default="reserved")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class NodeRow(Base):
    __tablename__ = "nodes"
    id: Mapped[str] = mapped_column(String(160), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(24), index=True)
    inventory_json: Mapped[str] = mapped_column(Text)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ResourceReservationRow(Base):
    __tablename__ = "resource_reservations"
    __table_args__ = (UniqueConstraint("job_id", "job_generation"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    job_generation: Mapped[int] = mapped_column(Integer)
    node_id: Mapped[str] = mapped_column(String(160), index=True)
    node_generation: Mapped[int] = mapped_column(Integer)
    child_profile_id: Mapped[str] = mapped_column(String(100))
    runtime_profile_id: Mapped[str] = mapped_column(String(100))
    harness: Mapped[str] = mapped_column(String(24))
    image_digest: Mapped[str] = mapped_column(String(64))
    image_ref: Mapped[str] = mapped_column(Text, default="", server_default="")
    cpu: Mapped[int] = mapped_column(Integer)
    memory_mb: Mapped[int] = mapped_column(Integer)
    disk_mb: Mapped[int] = mapped_column(Integer)
    gpu_ids_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    state: Mapped[str] = mapped_column(String(24), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class JobActivityRow(Base):
    __tablename__ = "job_activities"
    __table_args__ = (UniqueConstraint("job_id", "sequence"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    kind: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(24))
    summary: Mapped[str] = mapped_column(Text, default="", server_default="")
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class JobDisplayActivityRow(Base):
    __tablename__ = "job_display_activities"
    __table_args__ = (UniqueConstraint("job_id", "projection_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(32), index=True)
    projection_key: Mapped[str] = mapped_column(String(200))
    sequence: Mapped[int] = mapped_column(Integer)
    cursor_sequence: Mapped[int] = mapped_column(Integer, index=True)
    kind: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(24))
    summary: Mapped[str] = mapped_column(Text, default="", server_default="")
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    raw_count: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


OWNER_MIGRATION_ROWS = (
    ThreadRow, ThreadControlRow, PauseParticipantRow, UploadRow,
    MediaDraftRow, MediaReferenceUploadRow, MediaVoiceCloneRow, MediaBatchRow, MediaJobRow,
    SiteProjectRow, SiteDraftRow, SiteDeployJobRow,
    SandboxControlRow, SandboxPortGrantRow, SandboxAuditRow,
    JobGroupDraftRow, AuthorizationSnapshotRow, JobGroupRow, JobRow, BudgetAccountRow,
)


OWNER_MIGRATION_TABLES = frozenset(row.__tablename__ for row in OWNER_MIGRATION_ROWS)
