from __future__ import annotations

import json
import asyncio
import hashlib
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath

from sqlalchemy import and_, case, or_, Boolean, DateTime, Integer, String, Text, UniqueConstraint, delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .observability import observed
from .domain import AccountView, RuntimeEvent, Artifact, Event, EventKind, Harness, Interaction, Item, ItemType, ProjectFile, TurnProjectFileSnapshot, ThreadCreate, ThreadView, ThreadControlState, ThreadControlView, PauseParticipantState, PauseParticipantView, TurnCreate, TurnInputCreate, TurnInputView, TurnState, TurnView, TERMINAL, identifier, now, MediaDraftView, MediaJobView, MediaJobOutputView, MediaReferenceUploadView, SiteDeployJobView, SiteDraftView, SiteProjectView, SiteVersionView
from .catalog import ResolvedProfile
from .events import RedisEventBus
from .media_errors import media_error_guidance
from .job_contracts import (
    JobAttemptReceipt, JobGroupCreate, JobGroupDraftCreate, JobGroupDraftState,
    JobGroupDraftView, JobGroupState, JobGroupView, JobKind, JobOutboxView,
    JobState, JobView, JobRequest, TERMINAL_JOB_STATES, BudgetAccountView, BudgetLedgerView,
    BudgetReservationView, JobArtifactOutput, OutputDisposition,
)


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
    kind: Mapped[str] = mapped_column(String(16))
    model: Mapped[str] = mapped_column(String(160))
    generation_mode: Mapped[str] = mapped_column(String(40), default="", server_default="")
    provider: Mapped[str] = mapped_column(String(80))
    adapter: Mapped[str] = mapped_column(String(120))
    execution_mode: Mapped[str] = mapped_column(String(16))
    cancellation_mode: Mapped[str] = mapped_column(String(16))
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
    status: Mapped[str] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
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
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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
    state: Mapped[str] = mapped_column(String(24), index=True)
    generation: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    capability_nonce: Mapped[str] = mapped_column(String(64), default="", server_default="")
    pty_session_id: Mapped[str] = mapped_column(String(160), default="", server_default="")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SandboxPortGrantRow(Base):
    __tablename__ = "sandbox_port_grants"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    sandbox_id: Mapped[str] = mapped_column(String(200), index=True)
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


OWNER_MIGRATION_ROWS = (
    ThreadRow, ThreadControlRow, PauseParticipantRow, UploadRow,
    MediaDraftRow, MediaReferenceUploadRow, MediaVoiceCloneRow, MediaBatchRow, MediaJobRow,
    SiteProjectRow, SiteDraftRow, SiteDeployJobRow,
    SandboxControlRow, SandboxPortGrantRow, SandboxAuditRow,
    JobGroupDraftRow, AuthorizationSnapshotRow, JobGroupRow, JobRow, BudgetAccountRow,
)
OWNER_MIGRATION_TABLES = frozenset(row.__tablename__ for row in OWNER_MIGRATION_ROWS)


class Store:
    def __init__(self, database_url: str, redis_url: str = "", redis_prefix: str = "sandbox-harness:"):
        self.engine = create_async_engine(database_url)
        self.connections = async_sessionmaker(self.engine, expire_on_commit=False)
        self.events = RedisEventBus(redis_url, redis_prefix) if redis_url else None
        self.event_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.event_lock_users: dict[str, int] = defaultdict(int)
        self.event_lock_guard = asyncio.Lock()
        self.job_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.job_lock_users: dict[str, int] = defaultdict(int)
        self.job_lock_guard = asyncio.Lock()

    @asynccontextmanager
    async def event_lock(self, turn_id: str):
        async with self.event_lock_guard:
            lock = self.event_locks[turn_id]
            self.event_lock_users[turn_id] += 1
        try:
            async with lock:
                yield
        finally:
            async with self.event_lock_guard:
                self.event_lock_users[turn_id] -= 1
                if self.event_lock_users[turn_id] == 0:
                    self.event_lock_users.pop(turn_id, None)
                    self.event_locks.pop(turn_id, None)

    @asynccontextmanager
    async def job_lock(self, identity: str):
        """Serialize one identity locally; database row locks fence other processes."""
        async with self.job_lock_guard:
            lock = self.job_locks[identity]
            self.job_lock_users[identity] += 1
        try:
            async with lock:
                yield
        finally:
            async with self.job_lock_guard:
                self.job_lock_users[identity] -= 1
                if self.job_lock_users[identity] == 0:
                    self.job_lock_users.pop(identity, None)
                    self.job_locks.pop(identity, None)

    async def initialize(self) -> None:
        if self.events:
            await self.events.initialize()
        async with self.engine.begin() as connection:
            if connection.dialect.name == "postgresql":
                await connection.execute(text("SELECT pg_advisory_xact_lock(:lock_id)"),
                    {"lock_id": SCHEMA_INITIALIZATION_LOCK_ID})
            await connection.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        if self.events:
            await self.events.close()
        await self.engine.dispose()

    @staticmethod
    def job_group_draft_view(row: JobGroupDraftRow) -> JobGroupDraftView:
        return JobGroupDraftView(
            id=row.id, owner_id=row.owner_id, thread_id=row.thread_id,
            origin_turn_id=row.origin_turn_id, idempotency_key=row.idempotency_key,
            request_digest=row.request_digest, state=JobGroupDraftState(row.state),
            requires_confirmation=row.requires_confirmation,
            authorization_gaps=tuple(json.loads(row.authorization_gaps_json)),
            expires_at=utc_datetime(row.expires_at), confirmed_group_id=row.confirmed_group_id,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )

    @staticmethod
    def job_group_view(row: JobGroupRow) -> JobGroupView:
        return JobGroupView(
            id=row.id, owner_id=row.owner_id, thread_id=row.thread_id,
            origin_turn_id=row.origin_turn_id, draft_id=row.draft_id or "",
            idempotency_key=row.idempotency_key, request_digest=row.request_digest,
            state=JobGroupState(row.state), member_count=row.member_count,
            suppress_continuation=row.suppress_continuation,
            terminal_at=utc_datetime(row.terminal_at) if row.terminal_at else None,
            continued_at=utc_datetime(row.continued_at) if row.continued_at else None,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )

    @staticmethod
    def job_view(row: JobRow) -> JobView:
        return JobView(
            id=row.id, group_id=row.group_id, local_job_id=row.local_job_id,
            owner_id=row.owner_id, thread_id=row.thread_id, origin_turn_id=row.origin_turn_id,
            kind=JobKind(row.kind), adapter=row.adapter, idempotency_key=row.idempotency_key,
            request_digest=row.request_digest, state=JobState(row.state), generation=row.generation,
            cancel_requested=row.cancel_requested, replaces_job_id=row.replaces_job_id,
            reconciles_job_id=row.reconciles_job_id, result=json.loads(row.result_json),
            terminal_at=utc_datetime(row.terminal_at) if row.terminal_at else None,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )

    @staticmethod
    def job_attempt_view(row: JobAttemptRow) -> JobAttemptReceipt:
        return JobAttemptReceipt(
            id=row.id, job_id=row.job_id, generation=row.generation, worker_id=row.worker_id,
            node_id=row.node_id, executor_kind=row.executor_kind,
            request_digest=row.request_digest,
            accepted_at=utc_datetime(row.accepted_at) if row.accepted_at else None,
            external_execution_id=row.external_execution_id,
            acceptance_receipt=json.loads(row.acceptance_receipt_json),
            started_at=utc_datetime(row.started_at),
            finished_at=utc_datetime(row.finished_at) if row.finished_at else None,
            outcome=row.outcome, error_code=row.error_code, error_detail=row.error_detail,
        )

    @staticmethod
    def job_outbox_view(row: JobOutboxRow) -> JobOutboxView:
        return JobOutboxView(
            id=row.id, group_id=row.group_id, thread_id=row.thread_id, state=row.state,
            result_digest=row.result_digest, continuation_turn_id=row.continuation_turn_id,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )

    @staticmethod
    def budget_account_view(row: BudgetAccountRow) -> BudgetAccountView:
        return BudgetAccountView(
            id=row.id, owner_id=row.owner_id, thread_id=row.thread_id,
            budget_key=row.budget_key, limit=row.limit, reserved=row.reserved,
            consumed=row.consumed, created_at=utc_datetime(row.created_at),
            updated_at=utc_datetime(row.updated_at),
        )

    @staticmethod
    def budget_reservation_view(row: BudgetReservationRow) -> BudgetReservationView:
        return BudgetReservationView(
            id=row.id, account_id=row.account_id, group_id=row.group_id,
            authorization_snapshot_id=row.authorization_snapshot_id,
            budget_key=row.budget_key, reserved=row.reserved, consumed=row.consumed,
            released=row.released, state=row.state, created_at=utc_datetime(row.created_at),
            updated_at=utc_datetime(row.updated_at),
        )

    @staticmethod
    def budget_ledger_view(row: BudgetLedgerRow) -> BudgetLedgerView:
        return BudgetLedgerView(
            id=row.id, reservation_id=row.reservation_id, group_id=row.group_id,
            job_id=row.job_id, action=row.action, amount=row.amount,
            idempotency_key=row.idempotency_key, detail=json.loads(row.detail_json),
            created_at=utc_datetime(row.created_at),
        )

    async def get_thread_control(self, thread_id: str, owner_id: str | None = None) -> ThreadControlView:
        async with self.connections() as db:
            query = select(ThreadControlRow).where(ThreadControlRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadControlRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
            if row:
                return self.thread_control_view(row)
            thread = await db.get(ThreadRow, thread_id)
            if not thread or owner_id is not None and thread.owner_id != owner_id:
                raise KeyError("thread_not_found")
            timestamp = utc_datetime(thread.updated_at)
            return ThreadControlView(thread_id=thread_id, state=ThreadControlState.RUNNING,
                generation=0, updated_at=timestamp)

    async def list_pause_participants(self, thread_id: str, generation: int | None = None
                                      ) -> list[PauseParticipantView]:
        async with self.connections() as db:
            query = select(PauseParticipantRow).where(PauseParticipantRow.thread_id == thread_id)
            if generation is not None:
                query = query.where(PauseParticipantRow.generation == generation)
            rows = (await db.execute(query.order_by(PauseParticipantRow.created_at,
                PauseParticipantRow.id))).scalars().all()
            return [self.pause_participant_view(row) for row in rows]

    async def get_pause_participant(self, thread_id: str, kind: str, participant_id: str,
                                    generation: int | None = None) -> PauseParticipantView | None:
        async with self.connections() as db:
            query = select(PauseParticipantRow).where(PauseParticipantRow.thread_id == thread_id,
                PauseParticipantRow.kind == kind,
                PauseParticipantRow.participant_id == participant_id)
            if generation is not None:
                query = query.where(PauseParticipantRow.generation == generation)
            row = (await db.execute(query.order_by(
                PauseParticipantRow.generation.desc()).limit(1))).scalar_one_or_none()
            return self.pause_participant_view(row) if row else None

    async def request_thread_pause(self, thread_id: str, owner_id: str) -> ThreadControlView:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                    ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
                if not thread:
                    raise KeyError("thread_not_found")
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if control and control.state in {ThreadControlState.PAUSING.value,
                        ThreadControlState.PAUSED.value}:
                    return self.thread_control_view(control)
                if control and control.state == ThreadControlState.RESUMING.value:
                    raise RuntimeError("thread_resume_in_progress")
                timestamp = now()
                generation = (control.generation if control else 0) + 1
                if not control:
                    control = ThreadControlRow(thread_id=thread_id, owner_id=owner_id,
                        state=ThreadControlState.PAUSING.value, generation=generation,
                        operation_id=identifier(), degraded=False, detail_json="{}",
                        requested_at=timestamp, completed_at=None,
                        created_at=timestamp, updated_at=timestamp)
                    db.add(control)
                else:
                    control.state = ThreadControlState.PAUSING.value
                    control.generation = generation
                    control.operation_id = identifier()
                    control.degraded = False
                    control.detail_json = "{}"
                    control.requested_at = timestamp
                    control.completed_at = None
                    control.updated_at = timestamp
                participants = 0
                active = await db.get(TurnRow, thread.active_turn_id) if thread.active_turn_id else None
                if active and TurnState(active.state) not in TERMINAL:
                    state = (PauseParticipantState.PAUSED if active.state == TurnState.QUEUED.value
                        else PauseParticipantState.REQUESTED)
                    db.add(PauseParticipantRow(id=identifier(), owner_id=owner_id,
                        thread_id=thread_id, generation=generation, kind="primary_harness",
                        participant_id=active.id, state=state.value, checkpoint_ref="",
                        native_thread_id=thread.native_thread_id,
                        external_execution_id=active.runtime_execution_id, detail_json="{}",
                        created_at=timestamp, updated_at=timestamp))
                    participants += 1
                    if state == PauseParticipantState.PAUSED:
                        active.state = TurnState.PAUSED.value
                        active.error_code = "paused"
                        active.error_detail = "Turn paused before execution"
                        await self._finish_turn_records(db, active.id)
                jobs = list((await db.execute(select(JobRow).where(
                    JobRow.thread_id == thread_id, JobRow.kind == JobKind.HARNESS.value,
                    JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                ).with_for_update())).scalars().all())
                for job in jobs:
                    queued = JobState(job.state) in {JobState.CREATED, JobState.QUEUED}
                    state = PauseParticipantState.PAUSED if queued else PauseParticipantState.REQUESTED
                    acceptance = (await db.execute(select(JobAttemptRow).where(
                        JobAttemptRow.job_id == job.id,
                        JobAttemptRow.accepted_at.is_not(None),
                    ).order_by(JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
                    db.add(PauseParticipantRow(id=identifier(), owner_id=owner_id,
                        thread_id=thread_id, generation=generation, kind="child_harness",
                        participant_id=job.id, state=state.value, checkpoint_ref="",
                        native_thread_id="", external_execution_id=(
                            acceptance.external_execution_id if acceptance else ""),
                        detail_json="{}", created_at=timestamp, updated_at=timestamp))
                    job.state = (JobState.PAUSED if queued else JobState.PAUSING).value
                    job.updated_at = timestamp
                    participants += 1
                if not participants or all(row.state == PauseParticipantState.PAUSED.value
                        for row in await self._pause_rows_locked(db, thread_id, generation)):
                    control.state = ThreadControlState.PAUSED.value
                    control.completed_at = timestamp
                await db.flush()
                return self.thread_control_view(control)

    @staticmethod
    async def _pause_rows_locked(db, thread_id: str, generation: int) -> list[PauseParticipantRow]:
        return list((await db.execute(select(PauseParticipantRow).where(
            PauseParticipantRow.thread_id == thread_id,
            PauseParticipantRow.generation == generation).with_for_update())).scalars().all())

    async def complete_pause_participant(self, thread_id: str, generation: int, kind: str,
                                         participant_id: str, *, checkpoint_ref: str = "",
                                         native_thread_id: str = "",
                                         external_execution_id: str = "",
                                         detail: dict | None = None,
                                         failed: bool = False) -> PauseParticipantView:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if not control or control.generation != generation or control.state not in {
                        ThreadControlState.PAUSING.value, ThreadControlState.PAUSED.value}:
                    raise RuntimeError("stale_pause_generation")
                row = (await db.execute(select(PauseParticipantRow).where(
                    PauseParticipantRow.thread_id == thread_id,
                    PauseParticipantRow.generation == generation,
                    PauseParticipantRow.kind == kind,
                    PauseParticipantRow.participant_id == participant_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("pause_participant_not_found")
                timestamp = now()
                row.state = (PauseParticipantState.FAILED if failed
                    else PauseParticipantState.PAUSED).value
                row.checkpoint_ref = checkpoint_ref or row.checkpoint_ref
                row.native_thread_id = native_thread_id or row.native_thread_id
                row.external_execution_id = external_execution_id or row.external_execution_id
                row.detail_json = canonical_json(detail or {})
                row.updated_at = timestamp
                if kind == "child_harness":
                    job = await db.get(JobRow, participant_id, with_for_update=True)
                    if job and JobState(job.state) not in TERMINAL_JOB_STATES:
                        job.state = JobState.PAUSED.value
                        job.updated_at = timestamp
                        lease = await db.get(JobLeaseRow, participant_id, with_for_update=True)
                        if lease:
                            attempt = await db.get(JobAttemptRow, lease.attempt_id, with_for_update=True)
                            if attempt and not attempt.finished_at:
                                attempt.finished_at = timestamp
                                attempt.outcome = JobState.PAUSED.value
                            await db.delete(lease)
                rows = await self._pause_rows_locked(db, thread_id, generation)
                if all(value.state in {PauseParticipantState.PAUSED.value,
                        PauseParticipantState.FAILED.value} for value in rows):
                    control.state = ThreadControlState.PAUSED.value
                    control.degraded = any(value.state == PauseParticipantState.FAILED.value for value in rows)
                    control.detail_json = canonical_json({"participants": len(rows),
                        "failed": sum(value.state == PauseParticipantState.FAILED.value for value in rows)})
                    control.completed_at = timestamp
                    control.updated_at = timestamp
                await db.flush()
                return self.pause_participant_view(row)

    async def resume_thread(self, thread_id: str, owner_id: str) -> ThreadControlView:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                    ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
                if not thread:
                    raise KeyError("thread_not_found")
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if not control:
                    timestamp = now()
                    control = ThreadControlRow(thread_id=thread_id, owner_id=owner_id,
                        state=ThreadControlState.RUNNING.value, generation=0,
                        operation_id="", degraded=False, detail_json="{}",
                        requested_at=None, completed_at=timestamp,
                        created_at=timestamp, updated_at=timestamp)
                    db.add(control);await db.flush();return self.thread_control_view(control)
                if control.state == ThreadControlState.RUNNING.value:
                    return self.thread_control_view(control)
                if control.state == ThreadControlState.PAUSING.value:
                    raise RuntimeError("thread_pause_in_progress")
                control.state = ThreadControlState.RESUMING.value
                control.updated_at = now()
                rows = await self._pause_rows_locked(db, thread_id, control.generation)
                for row in rows:
                    if row.kind == "child_harness":
                        job = await db.get(JobRow, row.participant_id, with_for_update=True)
                        if job and job.state == JobState.PAUSED.value:
                            job.state = JobState.QUEUED.value
                            job.updated_at = now()
                    row.state = PauseParticipantState.RESUMED.value
                    row.updated_at = now()
                control.state = ThreadControlState.RUNNING.value
                control.detail_json = canonical_json({
                    "resume_context_required": True,
                    "degraded": control.degraded,
                    "generation": control.generation,
                })
                control.completed_at = now()
                control.updated_at = now()
                await db.flush()
                return self.thread_control_view(control)

    async def configure_budget(self, owner_id: str, thread_id: str, budget_key: str,
                               *, limit: int) -> BudgetAccountView:
        if limit < 0:
            raise ValueError("budget limit cannot be negative")
        identity = f"budget:{owner_id}:{thread_id}:{budget_key}"
        async with self.job_lock(identity):
            async with self.connections.begin() as db:
                row = (await db.execute(select(BudgetAccountRow).where(
                    BudgetAccountRow.owner_id == owner_id,
                    BudgetAccountRow.thread_id == thread_id,
                    BudgetAccountRow.budget_key == budget_key,
                ).with_for_update())).scalar_one_or_none()
                timestamp = now()
                if row:
                    if limit < row.consumed + row.reserved:
                        raise RuntimeError("budget_limit_below_committed_usage")
                    row.limit = limit
                    row.updated_at = timestamp
                else:
                    row = BudgetAccountRow(
                        id=identifier(), owner_id=owner_id, thread_id=thread_id,
                        budget_key=budget_key, limit=limit, reserved=0, consumed=0,
                        created_at=timestamp, updated_at=timestamp,
                    )
                    db.add(row)
                await db.flush()
                return self.budget_account_view(row)

    async def get_budget_account(self, account_id: str) -> BudgetAccountView | None:
        async with self.connections() as db:
            row = await db.get(BudgetAccountRow, account_id)
            return self.budget_account_view(row) if row else None

    async def reserve_budget(self, account_id: str, *, group_id: str,
                             authorization_snapshot_id: str, amount: int,
                             idempotency_key: str) -> BudgetReservationView:
        if amount < 1:
            raise ValueError("budget reservation amount must be positive")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                if not account:
                    raise KeyError("budget_account_not_found")
                existing = (await db.execute(select(BudgetReservationRow).where(
                    BudgetReservationRow.group_id == group_id,
                    BudgetReservationRow.budget_key == account.budget_key,
                ).with_for_update())).scalar_one_or_none()
                if existing:
                    ledger = (await db.execute(select(BudgetLedgerRow).where(
                        BudgetLedgerRow.reservation_id == existing.id,
                        BudgetLedgerRow.idempotency_key == idempotency_key,
                    ))).scalar_one_or_none()
                    if not ledger or existing.account_id != account_id or existing.reserved != amount:
                        raise RuntimeError("budget_reservation_idempotency_conflict")
                    return self.budget_reservation_view(existing)
                if account.limit - account.consumed - account.reserved < amount:
                    raise RuntimeError("budget_exhausted")
                timestamp = now()
                row = BudgetReservationRow(
                    id=identifier(), account_id=account.id, group_id=group_id,
                    authorization_snapshot_id=authorization_snapshot_id,
                    budget_key=account.budget_key, reserved=amount, consumed=0, released=0,
                    state="reserved", created_at=timestamp, updated_at=timestamp,
                )
                account.reserved += amount
                account.updated_at = timestamp
                db.add_all([row, BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=group_id, job_id="",
                    action="reserve", amount=amount, idempotency_key=idempotency_key,
                    detail_json="{}", created_at=timestamp,
                )])
                await db.flush()
                return self.budget_reservation_view(row)

    async def consume_budget(self, reservation_id: str, *, job_id: str, amount: int,
                             idempotency_key: str) -> BudgetReservationView:
        if amount < 1:
            raise ValueError("budget consumption amount must be positive")
        async with self.connections() as lookup:
            account_id = (await lookup.execute(select(BudgetReservationRow.account_id).where(
                BudgetReservationRow.id == reservation_id))).scalar_one_or_none()
        if not account_id:
            raise KeyError("budget_reservation_not_found")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                row = await db.get(BudgetReservationRow, reservation_id, with_for_update=True)
                existing = (await db.execute(select(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id == reservation_id,
                    BudgetLedgerRow.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if existing:
                    if existing.action != "consume" or existing.amount != amount or existing.job_id != job_id:
                        raise RuntimeError("budget_ledger_idempotency_conflict")
                    return self.budget_reservation_view(row)
                if row.consumed + row.released + amount > row.reserved:
                    raise RuntimeError("budget_reservation_exhausted")
                timestamp = now()
                row.consumed += amount
                row.state = "consumed" if row.consumed + row.released == row.reserved else "partial"
                row.updated_at = timestamp
                account.reserved -= amount
                account.consumed += amount
                account.updated_at = timestamp
                db.add(BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=row.group_id, job_id=job_id,
                    action="consume", amount=amount, idempotency_key=idempotency_key,
                    detail_json="{}", created_at=timestamp,
                ))
                await db.flush()
                return self.budget_reservation_view(row)

    async def release_budget(self, reservation_id: str, *,
                             idempotency_key: str) -> BudgetReservationView:
        async with self.connections() as lookup:
            account_id = (await lookup.execute(select(BudgetReservationRow.account_id).where(
                BudgetReservationRow.id == reservation_id))).scalar_one_or_none()
        if not account_id:
            raise KeyError("budget_reservation_not_found")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                row = await db.get(BudgetReservationRow, reservation_id, with_for_update=True)
                existing = (await db.execute(select(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id == reservation_id,
                    BudgetLedgerRow.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if existing:
                    if existing.action != "release":
                        raise RuntimeError("budget_ledger_idempotency_conflict")
                    return self.budget_reservation_view(row)
                amount = row.reserved - row.consumed - row.released
                timestamp = now()
                if amount==0:
                    row.state = "released" if row.consumed == 0 else "closed"
                    row.updated_at = timestamp
                    await db.flush()
                    return self.budget_reservation_view(row)
                row.released += amount
                row.state = "released" if row.consumed == 0 else "closed"
                row.updated_at = timestamp
                account.reserved -= amount
                account.updated_at = timestamp
                db.add(BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=row.group_id, job_id="",
                    action="release", amount=amount, idempotency_key=idempotency_key,
                    detail_json="{}", created_at=timestamp,
                ))
                await db.flush()
                return self.budget_reservation_view(row)

    async def adjust_budget(self, reservation_id: str, *, amount: int,
                            idempotency_key: str, detail: dict | None = None) -> BudgetReservationView:
        if amount == 0:
            raise ValueError("budget adjustment cannot be zero")
        async with self.connections() as lookup:
            account_id = (await lookup.execute(select(BudgetReservationRow.account_id).where(
                BudgetReservationRow.id == reservation_id))).scalar_one_or_none()
        if not account_id:
            raise KeyError("budget_reservation_not_found")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                row = await db.get(BudgetReservationRow, reservation_id, with_for_update=True)
                existing = (await db.execute(select(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id == reservation_id,
                    BudgetLedgerRow.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if existing:
                    if existing.action != "adjust" or existing.amount != amount:
                        raise RuntimeError("budget_ledger_idempotency_conflict")
                    return self.budget_reservation_view(row)
                if amount > 0 and account.consumed + account.reserved + amount > account.limit:
                    raise RuntimeError("budget_exhausted")
                if amount < 0 and row.consumed < -amount:
                    raise RuntimeError("budget_adjustment_exceeds_consumption")
                row.consumed += amount
                account.consumed += amount
                row.updated_at = account.updated_at = now()
                db.add(BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=row.group_id, job_id="",
                    action="adjust", amount=amount, idempotency_key=idempotency_key,
                    detail_json=canonical_json(detail or {}), created_at=now(),
                ))
                await db.flush()
                return self.budget_reservation_view(row)

    async def list_budget_ledger(self, reservation_id: str) -> list[BudgetLedgerView]:
        async with self.connections() as db:
            rows = (await db.execute(select(BudgetLedgerRow).where(
                BudgetLedgerRow.reservation_id == reservation_id).order_by(
                BudgetLedgerRow.created_at, BudgetLedgerRow.id))).scalars().all()
            return [self.budget_ledger_view(row) for row in rows]

    async def get_budget_reservations_for_group(self, group_id: str) -> list[BudgetReservationView]:
        async with self.connections() as db:
            rows = (await db.execute(select(BudgetReservationRow).where(
                BudgetReservationRow.group_id == group_id).order_by(
                BudgetReservationRow.budget_key))).scalars().all()
            return [self.budget_reservation_view(row) for row in rows]

    @staticmethod
    def job_activity_view(row: JobActivityRow)->dict:
        return {'id':row.id,'job_id':row.job_id,'sequence':row.sequence,'kind':row.kind,
            'status':row.status,'summary':row.summary,'payload':json.loads(row.payload_json),
            'created_at':utc_datetime(row.created_at),'updated_at':utc_datetime(row.updated_at)}

    async def append_job_activity(self, job_id: str, sequence: int, kind: str,
                                  payload: dict, *, status: str = "in_progress") -> dict:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id)
            if not job:raise KeyError('job_not_found')
            row=(await db.execute(select(JobActivityRow).where(JobActivityRow.job_id==job_id,
                JobActivityRow.sequence==sequence).with_for_update())).scalar_one_or_none()
            timestamp=now();summary=str(payload.get('name') or payload.get('text') or kind)[:1000]
            if row:
                if row.kind!=kind or json.loads(row.payload_json)!=payload:
                    raise RuntimeError('job_activity_sequence_conflict')
                return self.job_activity_view(row)
            row=JobActivityRow(id=identifier(),job_id=job_id,sequence=sequence,kind=kind,
                status=status,summary=summary,payload_json=canonical_json(payload),
                created_at=timestamp,updated_at=timestamp);db.add(row);await db.flush()
            return self.job_activity_view(row)

    async def list_job_activities(self,job_id:str,after:int=0,limit:int=500)->list[dict]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobActivityRow).where(JobActivityRow.job_id==job_id,
                JobActivityRow.sequence>after).order_by(JobActivityRow.sequence).limit(
                min(max(limit,1),1000)))).scalars().all()
            return [self.job_activity_view(row) for row in rows]

    async def create_job_group_draft(self, owner_id: str,
                                     value: JobGroupDraftCreate) -> tuple[JobGroupDraftView, bool]:
        value.validate_member_limit()
        lock_identity = f"draft-create:{value.thread_id}:{value.idempotency_key}"
        async with self.job_lock(lock_identity):
            async with self.connections.begin() as db:
                existing = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.thread_id == value.thread_id,
                    JobGroupDraftRow.idempotency_key == value.idempotency_key,
                ).with_for_update())).scalar_one_or_none()
                if existing:
                    if existing.owner_id != owner_id or existing.request_digest != value.request_digest:
                        raise RuntimeError("idempotency_conflict")
                    return self.job_group_draft_view(existing), False
                timestamp = now()
                row = JobGroupDraftRow(
                    id=identifier(), owner_id=owner_id, thread_id=value.thread_id,
                    origin_turn_id=value.origin_turn_id, idempotency_key=value.idempotency_key,
                    request_digest=value.request_digest, request_json=value.model_dump_json(),
                    state=(JobGroupDraftState.AWAITING_CONFIRMATION.value
                        if value.requires_confirmation else JobGroupDraftState.DRAFT.value),
                    requires_confirmation=value.requires_confirmation,
                    authorization_gaps_json=canonical_json(value.authorization_gaps),
                    expires_at=value.expires_at, confirmed_group_id="",
                    created_at=timestamp, updated_at=timestamp,
                )
                db.add(row)
                await db.flush()
                return self.job_group_draft_view(row), True

    async def get_job_group_draft(self, draft_id: str,
                                  owner_id: str | None = None) -> JobGroupDraftView | None:
        async with self.connections() as db:
            query = select(JobGroupDraftRow).where(JobGroupDraftRow.id == draft_id)
            if owner_id is not None:
                query = query.where(JobGroupDraftRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
            return self.job_group_draft_view(row) if row else None

    async def get_job_group_draft_request(self, draft_id: str,
                                          owner_id: str) -> JobGroupDraftCreate | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.id == draft_id,
                JobGroupDraftRow.owner_id == owner_id,
            ))).scalar_one_or_none()
            return JobGroupDraftCreate.model_validate_json(row.request_json) if row else None

    async def list_job_group_drafts(self, thread_id: str, owner_id: str) -> list[JobGroupDraftView]:
        async with self.connections() as db:
            rows = (await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.thread_id == thread_id,
                JobGroupDraftRow.owner_id == owner_id,
            ).order_by(JobGroupDraftRow.created_at.desc()))).scalars().all()
            return [self.job_group_draft_view(row) for row in rows]

    async def update_job_group_draft(self, draft_id: str, owner_id: str, *,
                                     expected_digest: str,
                                     value: JobGroupDraftCreate) -> JobGroupDraftView:
        value.validate_member_limit()
        async with self.job_lock(f"draft:{draft_id}"):
            async with self.connections.begin() as db:
                row = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.id == draft_id,
                    JobGroupDraftRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("job_group_draft_not_found")
                if row.state not in {JobGroupDraftState.DRAFT.value,
                                     JobGroupDraftState.AWAITING_CONFIRMATION.value}:
                    raise RuntimeError("draft_not_editable")
                if row.request_digest != expected_digest:
                    raise RuntimeError("draft_digest_conflict")
                if (row.thread_id != value.thread_id or row.origin_turn_id != value.origin_turn_id
                        or row.idempotency_key != value.idempotency_key):
                    raise RuntimeError("draft_identity_immutable")
                row.request_digest = value.request_digest
                row.request_json = value.model_dump_json()
                row.requires_confirmation = value.requires_confirmation
                row.authorization_gaps_json = canonical_json(value.authorization_gaps)
                row.expires_at = value.expires_at
                row.updated_at = now()
                await db.flush()
                return self.job_group_draft_view(row)

    async def reject_job_group_draft(self, draft_id: str, owner_id: str) -> JobGroupDraftView:
        async with self.job_lock(f"draft:{draft_id}"):
            async with self.connections.begin() as db:
                row = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.id == draft_id, JobGroupDraftRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("job_group_draft_not_found")
                if row.state not in {JobGroupDraftState.DRAFT.value,
                                     JobGroupDraftState.AWAITING_CONFIRMATION.value}:
                    raise RuntimeError("draft_not_editable")
                row.state = JobGroupDraftState.REJECTED.value
                row.updated_at = now()
                await db.flush()
                return self.job_group_draft_view(row)

    async def expire_job_group_drafts(self) -> int:
        async with self.connections.begin() as db:
            result = await db.execute(update(JobGroupDraftRow).where(
                JobGroupDraftRow.state.in_([
                    JobGroupDraftState.DRAFT.value,
                    JobGroupDraftState.AWAITING_CONFIRMATION.value,
                ]),
                JobGroupDraftRow.expires_at < now(),
            ).values(state=JobGroupDraftState.EXPIRED.value, updated_at=now()))
            return result.rowcount or 0

    async def _create_job_group_locked(self, db: AsyncSession, owner_id: str,
                                       value: JobGroupCreate, *, draft_id: str | None = None
                                       ) -> tuple[JobGroupRow, list[JobRow]]:
        value.validate_member_limit()
        existing = (await db.execute(select(JobGroupRow).where(
            JobGroupRow.thread_id == value.thread_id,
            JobGroupRow.idempotency_key == value.idempotency_key,
        ).with_for_update())).scalar_one_or_none()
        if existing:
            if existing.owner_id != owner_id or existing.request_digest != value.request_digest:
                raise RuntimeError("idempotency_conflict")
            jobs = (await db.execute(select(JobRow).where(
                JobRow.group_id == existing.id).order_by(JobRow.created_at, JobRow.local_job_id))).scalars().all()
            return existing, list(jobs)
        timestamp = now()
        snapshot_json = canonical_json(value.authorization_snapshot)
        snapshot = AuthorizationSnapshotRow(
            id=identifier(), owner_id=owner_id, thread_id=value.thread_id,
            origin_turn_id=value.origin_turn_id, scope="job_group",
            version=int(value.authorization_snapshot.get("version", 0)),
            snapshot_digest=canonical_digest(value.authorization_snapshot),
            snapshot_json=snapshot_json, created_at=timestamp,
        )
        group_id = identifier()
        reservations = value.authorization_snapshot.get("budget_reservations", [])
        if not isinstance(reservations, list):
            raise ValueError("budget_reservations must be a list")
        normalized_reservations = []
        for reservation in reservations:
            if (not isinstance(reservation, dict)
                    or set(reservation) != {"account_id", "amount"}
                    or not isinstance(reservation["account_id"], str)
                    or not isinstance(reservation["amount"], int)
                    or reservation["amount"] < 1):
                raise ValueError("invalid budget_reservation request")
            normalized_reservations.append(reservation)
        if len({row["account_id"] for row in normalized_reservations}) != len(normalized_reservations):
            raise ValueError("duplicate budget account reservation")
        for reservation in sorted(normalized_reservations, key=lambda row: row["account_id"]):
            account = await db.get(BudgetAccountRow, reservation["account_id"], with_for_update=True)
            if not account or account.owner_id != owner_id or account.thread_id != value.thread_id:
                raise RuntimeError("budget_account_outside_authorization_scope")
            amount = reservation["amount"]
            if account.limit - account.consumed - account.reserved < amount:
                raise RuntimeError("budget_exhausted")
            budget_reservation = BudgetReservationRow(
                id=identifier(), account_id=account.id, group_id=group_id,
                authorization_snapshot_id=snapshot.id, budget_key=account.budget_key,
                reserved=amount, consumed=0, released=0, state="reserved",
                created_at=timestamp, updated_at=timestamp,
            )
            account.reserved += amount
            account.updated_at = timestamp
            db.add_all([budget_reservation, BudgetLedgerRow(
                id=identifier(), reservation_id=budget_reservation.id, group_id=group_id,
                job_id="", action="reserve", amount=amount,
                idempotency_key=f"group-reserve:{group_id}:{account.budget_key}",
                detail_json="{}", created_at=timestamp,
            )])
        group = JobGroupRow(
            id=group_id, owner_id=owner_id, thread_id=value.thread_id,
            origin_turn_id=value.origin_turn_id, draft_id=draft_id,
            idempotency_key=value.idempotency_key, request_digest=value.request_digest,
            authorization_snapshot_id=snapshot.id, state=JobGroupState.COMMITTED.value,
            member_count=len(value.jobs), suppress_continuation=value.suppress_continuation,
            terminal_at=None, continued_at=None, created_at=timestamp, updated_at=timestamp,
        )
        db.add_all([snapshot, group])
        rows: list[JobRow] = []
        for request in value.jobs:
            row = JobRow(
                id=identifier(), group_id=group.id, local_job_id=request.local_job_id,
                owner_id=owner_id, thread_id=value.thread_id, origin_turn_id=value.origin_turn_id,
                kind=request.kind.value, adapter=request.adapter,
                descriptor_id=request.descriptor_id, descriptor_version=request.descriptor_version,
                descriptor_digest=request.descriptor_digest, idempotency_key=request.idempotency_key,
                request_digest=request.request_digest, request_json=request.model_dump_json(),
                state=JobState.CREATED.value, generation=0, cancel_requested=False,
                replaces_job_id=request.replaces_job_id, reconciles_job_id=request.reconciles_job_id,
                result_json="{}", result_digest="", terminal_at=None,
                created_at=timestamp, updated_at=timestamp,
            )
            db.add(row)
            rows.append(row)
        await db.flush()
        return group, rows

    async def create_job_group(self, owner_id: str,
                               value: JobGroupCreate) -> tuple[JobGroupView, list[JobView]]:
        lock_identity = f"group-create:{value.thread_id}:{value.idempotency_key}"
        async with self.job_lock(lock_identity):
            async with self.connections.begin() as db:
                group, jobs = await self._create_job_group_locked(db, owner_id, value)
                return self.job_group_view(group), [self.job_view(job) for job in jobs]

    async def confirm_job_group_draft(self, draft_id: str,
                                      owner_id: str) -> tuple[JobGroupView, list[JobView]]:
        expired = False
        async with self.job_lock(f"draft:{draft_id}"):
            async with self.connections.begin() as db:
                draft = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.id == draft_id,
                    JobGroupDraftRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not draft:
                    raise KeyError("job_group_draft_not_found")
                if draft.confirmed_group_id:
                    group = await db.get(JobGroupRow, draft.confirmed_group_id)
                    jobs = (await db.execute(select(JobRow).where(JobRow.group_id == group.id)
                        .order_by(JobRow.created_at, JobRow.local_job_id))).scalars().all()
                    return self.job_group_view(group), [self.job_view(job) for job in jobs]
                if draft.state not in {JobGroupDraftState.DRAFT.value,
                                       JobGroupDraftState.AWAITING_CONFIRMATION.value}:
                    raise RuntimeError("draft_not_editable")
                if utc_datetime(draft.expires_at) <= now():
                    draft.state = JobGroupDraftState.EXPIRED.value
                    draft.updated_at = now()
                    expired = True
                else:
                    draft_value = JobGroupDraftCreate.model_validate_json(draft.request_json)
                    group_value = JobGroupCreate.model_validate(draft_value.model_dump(exclude={
                        "expires_at", "requires_confirmation", "authorization_gaps",
                    }))
                    group, jobs = await self._create_job_group_locked(
                        db, owner_id, group_value, draft_id=draft.id)
                    draft.state = JobGroupDraftState.CONFIRMED.value
                    draft.confirmed_group_id = group.id
                    draft.updated_at = now()
                    await db.flush()
                    return self.job_group_view(group), [self.job_view(job) for job in jobs]
        if expired:
            raise RuntimeError("draft_expired")
        raise RuntimeError("draft_confirmation_failed")

    async def get_job_group(self, group_id: str) -> JobGroupView | None:
        async with self.connections() as db:
            row = await db.get(JobGroupRow, group_id)
            return self.job_group_view(row) if row else None

    async def get_job_group_for_owner(self, group_id: str, owner_id: str) -> JobGroupView | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobGroupRow).where(
                JobGroupRow.id == group_id, JobGroupRow.owner_id == owner_id,
            ))).scalar_one_or_none()
            return self.job_group_view(row) if row else None

    async def get_job(self, job_id: str) -> JobView | None:
        async with self.connections() as db:
            row = await db.get(JobRow, job_id)
            return self.job_view(row) if row else None

    async def get_job_for_owner(self, job_id: str, owner_id: str) -> JobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobRow).where(
                JobRow.id == job_id, JobRow.owner_id == owner_id,
            ))).scalar_one_or_none()
            return self.job_view(row) if row else None

    async def list_jobs_for_group(self, group_id: str, owner_id: str | None = None) -> list[JobView]:
        async with self.connections() as db:
            query = select(JobRow).where(JobRow.group_id == group_id)
            if owner_id is not None:
                query = query.where(JobRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(
                JobRow.created_at, JobRow.local_job_id))).scalars().all()
            return [self.job_view(row) for row in rows]

    async def list_jobs_for_thread(self, thread_id: str) -> list[JobView]:
        async with self.connections() as db:
            rows = (await db.execute(select(JobRow).where(JobRow.thread_id == thread_id)
                .order_by(JobRow.created_at, JobRow.local_job_id))).scalars().all()
            return [self.job_view(row) for row in rows]

    async def get_job_request(self, job_id: str) -> JobRequest | None:
        async with self.connections() as db:
            row = await db.get(JobRow, job_id)
            if not row:
                return None
            return JobRequest.model_validate_json(row.request_json)

    async def latest_job_acceptance(self, job_id: str) -> JobAttemptReceipt | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id == job_id,
                JobAttemptRow.accepted_at.is_not(None),
            ).order_by(JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
            return self.job_attempt_view(row) if row else None

    async def claim_available_jobs(self, worker_id: str, node_id: str, lease_seconds: int,
                                   *, limit: int = 1,
                                   adapters: set[str] | None = None) -> list[JobAttemptReceipt]:
        timestamp = now()
        async with self.connections() as db:
            query = (select(JobRow.id)
                .outerjoin(JobLeaseRow, JobLeaseRow.job_id == JobRow.id)
                .outerjoin(ThreadControlRow, ThreadControlRow.thread_id == JobRow.thread_id)
                .where(or_(JobRow.state == JobState.PAUSING.value,
                    and_(JobRow.kind == JobKind.PROVIDER.value,
                        JobRow.state == JobState.WAITING_EXTERNAL.value), and_(
                    or_(ThreadControlRow.thread_id.is_(None),
                        ThreadControlRow.state == ThreadControlState.RUNNING.value),
                    JobRow.state.in_([JobState.CREATED.value, JobState.QUEUED.value,
                        JobState.RUNNING.value, JobState.WAITING_EXTERNAL.value,
                        JobState.CANCELLING.value]))),
                    or_(JobLeaseRow.job_id.is_(None), JobLeaseRow.expires_at < timestamp)))
            if adapters:
                query = query.where(JobRow.adapter.in_(adapters))
            identities = (await db.execute(query.order_by(
                JobRow.created_at, JobRow.id).limit(limit * 3))).scalars().all()
        result = []
        for identity in identities:
            if len(result) >= limit:
                break
            try:
                result.append(await self.claim_job(identity, worker_id, node_id, lease_seconds))
            except RuntimeError as exc:
                if str(exc) not in {"job_already_leased", "job_terminal"}:
                    raise
        return result

    async def finish_pre_accept_attempt(self, job_id: str, worker_id: str, generation: int, *,
                                        retry: bool, error_code: str, error_detail: str) -> JobView:
        view = None
        async with self.job_lock(f"job:{job_id}"):
            async with self.connections.begin() as db:
                job = await db.get(JobRow, job_id, with_for_update=True)
                attempt = (await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id == job_id,
                    JobAttemptRow.generation == generation,
                ).with_for_update())).scalar_one_or_none()
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if (not job or not attempt or not lease or job.generation != generation
                        or lease.generation != generation or lease.worker_id != worker_id
                        or attempt.accepted_at is not None):
                    raise RuntimeError("stale_job_generation")
                attempt.finished_at = now()
                attempt.outcome = "PRE_ACCEPT_RETRY" if retry else JobState.FAILED.value
                attempt.error_code = error_code
                attempt.error_detail = error_detail
                pause = (await db.execute(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id == job.thread_id,
                    PauseParticipantRow.kind == "child_harness",
                    PauseParticipantRow.participant_id == job.id,
                    PauseParticipantRow.state == PauseParticipantState.REQUESTED.value,
                ).limit(1))).scalar_one_or_none()
                job.state = (JobState.PAUSING.value if pause else
                    JobState.QUEUED.value if retry else JobState.RUNNING.value)
                job.updated_at = now()
                await db.delete(lease)
                await db.flush()
                view = self.job_view(job)
        if retry:
            return view
        return await self.transition_job(job_id, JobState.FAILED, expected_generation=generation,
            result={"error_code": error_code, "error_detail": error_detail})

    async def defer_accepted_job(self, job_id: str, worker_id: str, generation: int) -> JobView:
        async with self.job_lock(f"job:{job_id}"):
            async with self.connections.begin() as db:
                job = await db.get(JobRow, job_id, with_for_update=True)
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if (not job or not lease or job.generation != generation
                        or lease.generation != generation or lease.worker_id != worker_id):
                    raise RuntimeError("stale_job_generation")
                pause = (await db.execute(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id == job.thread_id,
                    PauseParticipantRow.kind == "child_harness",
                    PauseParticipantRow.participant_id == job.id,
                    PauseParticipantRow.state == PauseParticipantState.REQUESTED.value,
                ).limit(1))).scalar_one_or_none()
                job.state = (JobState.PAUSING if pause else JobState.WAITING_EXTERNAL).value
                job.updated_at = now()
                await db.delete(lease)
                await db.flush()
                return self.job_view(job)

    async def request_job_cancel(self, job_id: str, owner_id: str) -> JobView:
        async with self.job_lock(f"job:{job_id}"):
            async with self.connections.begin() as db:
                row = (await db.execute(select(JobRow).where(
                    JobRow.id == job_id, JobRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("job_not_found")
                if JobState(row.state) not in TERMINAL_JOB_STATES:
                    row.cancel_requested = True
                    row.state = JobState.CANCELLING.value
                    row.updated_at = now()
                await db.flush()
                return self.job_view(row)

    async def request_job_group_cancel(self, group_id: str, owner_id: str) -> list[JobView]:
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                group = (await db.execute(select(JobGroupRow).where(
                    JobGroupRow.id == group_id, JobGroupRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not group:
                    raise KeyError("job_group_not_found")
                group.suppress_continuation = True
                group.updated_at = now()
                rows = (await db.execute(select(JobRow).where(
                    JobRow.group_id == group_id).order_by(
                    JobRow.created_at, JobRow.local_job_id).with_for_update())).scalars().all()
                timestamp = now()
                for row in rows:
                    if JobState(row.state) not in TERMINAL_JOB_STATES:
                        row.cancel_requested = True
                        row.state = JobState.CANCELLING.value
                        row.updated_at = timestamp
                await db.flush()
                return [self.job_view(row) for row in rows]

    async def claim_job(self, job_id: str, worker_id: str, node_id: str,
                        lease_seconds: int, *, retry_kind: str = "initial") -> JobAttemptReceipt:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                group = await db.get(JobGroupRow, group_id, with_for_update=True)
                job = (await db.execute(select(JobRow).where(
                    JobRow.id == job_id).with_for_update())).scalar_one_or_none()
                if not job:
                    raise KeyError("job_not_found")
                if JobState(job.state) in TERMINAL_JOB_STATES:
                    raise RuntimeError("job_terminal")
                control = await db.get(ThreadControlRow, job.thread_id)
                if (control and control.state != ThreadControlState.RUNNING.value
                        and job.state != JobState.PAUSING.value
                        and not (job.kind == JobKind.PROVIDER.value
                            and job.state == JobState.WAITING_EXTERNAL.value)):
                    raise RuntimeError("thread_paused")
                job.generation += 1
                job.state = JobState.RUNNING.value
                job.updated_at = now()
                if group.state == JobGroupState.COMMITTED.value:
                    group.state = JobGroupState.RUNNING.value
                    group.updated_at = now()
                attempt = JobAttemptRow(
                    id=identifier(), job_id=job.id, generation=job.generation,
                    worker_id=worker_id, node_id=node_id, executor_kind=job.kind,
                    retry_kind=retry_kind, request_digest=job.request_digest,
                    accepted_at=None, acceptance_receipt_json="{}", external_execution_id="",
                    started_at=now(), finished_at=None, outcome="", error_code="", error_detail="",
                )
                db.add(attempt)
                await db.flush()
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if lease and utc_datetime(lease.expires_at) > now() and lease.worker_id != worker_id:
                    raise RuntimeError("job_already_leased")
                if lease:
                    lease.attempt_id = attempt.id
                    lease.worker_id = worker_id
                    lease.generation = job.generation
                    lease.expires_at = now() + timedelta(seconds=lease_seconds)
                    lease.updated_at = now()
                else:
                    lease = JobLeaseRow(
                        job_id=job.id, attempt_id=attempt.id, worker_id=worker_id,
                        generation=job.generation,
                        expires_at=now() + timedelta(seconds=lease_seconds), updated_at=now(),
                    )
                    db.add(lease)
                await db.flush()
                return self.job_attempt_view(attempt)

    async def release_job_lease(self, job_id: str, worker_id: str, generation: int) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(delete(JobLeaseRow).where(
                JobLeaseRow.job_id == job_id, JobLeaseRow.worker_id == worker_id,
                JobLeaseRow.generation == generation,
            ))
            return bool(result.rowcount)

    async def renew_job_lease(self, job_id: str, worker_id: str, generation: int,
                              lease_seconds: int) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(update(JobLeaseRow).where(
                JobLeaseRow.job_id == job_id, JobLeaseRow.worker_id == worker_id,
                JobLeaseRow.generation == generation,
            ).values(expires_at=now()+timedelta(seconds=lease_seconds),updated_at=now()))
            return bool(result.rowcount)

    async def record_job_acceptance(self, job_id: str, worker_id: str, generation: int, *,
                                    external_execution_id: str, receipt: dict) -> JobAttemptReceipt:
        async with self.connections() as lookup:
            snapshot = await lookup.get(JobRow, job_id)
            if not snapshot:
                raise KeyError("job_not_found")
            request = JobRequest.model_validate_json(snapshot.request_json)
            reservations = (await lookup.execute(select(BudgetReservationRow).where(
                BudgetReservationRow.group_id == snapshot.group_id,
                BudgetReservationRow.budget_key.in_(list(request.budget_requirements) or [""]),
            ))).scalars().all()
            by_key = {row.budget_key: row for row in reservations}
            if set(by_key) != set(request.budget_requirements):
                raise RuntimeError("job_budget_reservation_missing")
            account_ids = sorted({row.account_id for row in reservations})
        async with self.job_lock(f"accept:{job_id}"):
            async with self.connections.begin() as db:
                accounts = {identity: await db.get(BudgetAccountRow, identity, with_for_update=True)
                    for identity in account_ids}
                locked_reservations = {row.budget_key: await db.get(
                    BudgetReservationRow, row.id, with_for_update=True)
                    for row in sorted(reservations, key=lambda value: value.id)}
                job = await db.get(JobRow, job_id, with_for_update=True)
                attempt = (await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id == job_id,
                    JobAttemptRow.generation == generation,
                ).with_for_update())).scalar_one_or_none()
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if (not job or not attempt or not lease or lease.worker_id != worker_id
                        or lease.generation != generation or job.generation != generation):
                    raise RuntimeError("stale_job_generation")
                encoded = canonical_json(receipt)
                if attempt.accepted_at:
                    if (attempt.external_execution_id != external_execution_id
                            or attempt.acceptance_receipt_json != encoded):
                        raise RuntimeError("acceptance_receipt_conflict")
                    return self.job_attempt_view(attempt)
                for budget_key, amount in sorted(request.budget_requirements.items()):
                    reservation = locked_reservations[budget_key]
                    account = accounts[reservation.account_id]
                    idempotency_key = f"accept:{job.id}:{job.request_digest}:{budget_key}"
                    ledger = (await db.execute(select(BudgetLedgerRow).where(
                        BudgetLedgerRow.reservation_id == reservation.id,
                        BudgetLedgerRow.idempotency_key == idempotency_key,
                    ))).scalar_one_or_none()
                    if ledger:
                        continue
                    if reservation.consumed + reservation.released + amount > reservation.reserved:
                        raise RuntimeError("job_budget_reservation_exhausted")
                    reservation.consumed += amount
                    reservation.state = ("consumed" if reservation.consumed + reservation.released
                        == reservation.reserved else "partial")
                    reservation.updated_at = now()
                    account.reserved -= amount
                    account.consumed += amount
                    account.updated_at = now()
                    db.add(BudgetLedgerRow(
                        id=identifier(), reservation_id=reservation.id,
                        group_id=reservation.group_id, job_id=job.id,
                        action="consume", amount=amount, idempotency_key=idempotency_key,
                        detail_json=canonical_json({"generation": generation,
                            "request_digest": job.request_digest}), created_at=now(),
                    ))
                attempt.accepted_at = now()
                attempt.external_execution_id = external_execution_id
                attempt.acceptance_receipt_json = encoded
                pause = (await db.execute(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id == job.thread_id,
                    PauseParticipantRow.kind == "child_harness",
                    PauseParticipantRow.participant_id == job.id,
                    PauseParticipantRow.state == PauseParticipantState.REQUESTED.value,
                ).limit(1))).scalar_one_or_none()
                job.state = (JobState.PAUSING if pause else JobState.WAITING_EXTERNAL).value
                job.updated_at = now()
                await db.flush()
                return self.job_attempt_view(attempt)

    async def transition_job(self, job_id: str, state: JobState, *, expected_generation: int,
                             result: dict | None = None) -> JobView:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        encoded = canonical_json(result or {})
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        release_group_budget=False;view=None
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                group = await db.get(JobGroupRow, group_id, with_for_update=True)
                job = await db.get(JobRow, job_id, with_for_update=True)
                if job.generation != expected_generation:
                    raise RuntimeError("stale_job_generation")
                if JobState(job.state) in TERMINAL_JOB_STATES:
                    if job.state == state.value and job.result_digest == digest:
                        view=self.job_view(job);release_group_budget=(group.state in {
                            JobGroupState.TERMINAL.value,JobGroupState.CONTINUED.value})
                    else:raise RuntimeError("job_already_terminal")
                else:
                    job.state = state.value
                    job.result_json = encoded
                    job.result_digest = digest
                    job.updated_at = now()
                    if state in TERMINAL_JOB_STATES:
                        job.terminal_at = now()
                        attempt = (await db.execute(select(JobAttemptRow).where(
                            JobAttemptRow.job_id == job.id,
                            JobAttemptRow.generation == job.generation,
                        ).with_for_update())).scalar_one_or_none()
                        if attempt:
                            attempt.finished_at = now()
                            attempt.outcome = state.value
                        await db.execute(delete(JobLeaseRow).where(JobLeaseRow.job_id == job.id))
                    if group.state == JobGroupState.COMMITTED.value:
                        group.state = JobGroupState.RUNNING.value
                        group.updated_at = now()
                    jobs = (await db.execute(select(JobRow).where(JobRow.group_id == group.id)
                        .order_by(JobRow.local_job_id).with_for_update())).scalars().all()
                    if all(JobState(member.state) in TERMINAL_JOB_STATES for member in jobs):
                        terminal_payload = [{
                            "job_id": member.id,
                            "local_job_id": member.local_job_id,
                            "state": member.state,
                            "result_digest": member.result_digest,
                        } for member in jobs]
                        result_json = canonical_json(terminal_payload)
                        result_digest = hashlib.sha256(result_json.encode()).hexdigest()
                        group.state = JobGroupState.TERMINAL.value
                        group.terminal_at = group.terminal_at or now()
                        group.updated_at = now();release_group_budget=True
                        delivery_rows=(await db.execute(select(JobArtifactRow,JobRow).join(
                            JobRow,JobRow.id==JobArtifactRow.job_id).where(
                            JobRow.group_id==group.id,
                            JobArtifactRow.output_disposition==OutputDisposition.DELIVERY.value,
                        ).order_by(JobRow.local_job_id,JobArtifactRow.artifact_role,
                            JobArtifactRow.position).with_for_update())).all()
                        existing_items=(await db.execute(select(ItemRow).where(
                            ItemRow.thread_id==group.thread_id,
                            ItemRow.type==ItemType.ARTIFACT.value).with_for_update())).scalars().all()
                        visible_ids={json.loads(item.payload_json).get('artifact_id') for item in existing_items}
                        for artifact_row,artifact_job in delivery_rows:
                            if artifact_row.artifact_id in visible_ids:continue
                            sequence=await self.next_item_sequence(db,group.thread_id);timestamp=now()
                            db.add(ItemRow(id=identifier(),thread_id=group.thread_id,
                                turn_id=artifact_job.origin_turn_id,sequence=sequence,
                                type=ItemType.ARTIFACT.value,status='completed',
                                summary=artifact_row.filename,payload_json=json.dumps({
                                    'artifact_id':artifact_row.artifact_id,'mime':artifact_row.mime,
                                    'size':artifact_row.size,'partial':False,'job_id':artifact_job.id,
                                    'artifact_role':artifact_row.artifact_role}),
                                created_at=timestamp,updated_at=timestamp))
                            visible_ids.add(artifact_row.artifact_id)
                        all_cancelled = all(member.state == JobState.CANCELLED.value for member in jobs)
                        outbox = (await db.execute(select(JobOutboxRow).where(
                            JobOutboxRow.group_id == group.id).with_for_update())).scalar_one_or_none()
                        if not outbox:
                            db.add(JobOutboxRow(
                                id=identifier(), group_id=group.id, thread_id=group.thread_id,
                                state="suppressed" if group.suppress_continuation or all_cancelled else "ready",
                                result_digest=result_digest, result_json=result_json,
                                claim_owner="", claim_batch_digest="", claim_expires_at=None,
                                continuation_turn_id="",
                                created_at=now(), updated_at=now(),
                            ))
                    await db.flush()
                    view=self.job_view(job)
        if release_group_budget:
            for reservation in await self.get_budget_reservations_for_group(group_id):
                await self.release_budget(reservation.id,
                    idempotency_key=f'group-terminal:{group_id}:{reservation.budget_key}')
        return view

    async def get_job_outbox(self, group_id: str) -> JobOutboxView | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.group_id == group_id))).scalar_one_or_none()
            return self.job_outbox_view(row) if row else None

    async def list_job_outboxes(self) -> list[JobOutboxView]:
        async with self.connections() as db:
            rows = (await db.execute(select(JobOutboxRow).order_by(
                JobOutboxRow.created_at, JobOutboxRow.id))).scalars().all()
            return [self.job_outbox_view(row) for row in rows]

    async def claim_job_outbox_batch(self, owner: str, *, lease_seconds: int = 60,
                                     max_groups: int = 32, max_jobs: int = 100,
                                     max_bytes: int = 1024 * 1024) -> list[dict]:
        async with self.connections.begin() as db:
            candidates = list((await db.execute(select(JobOutboxRow)
                .outerjoin(ThreadControlRow, ThreadControlRow.thread_id == JobOutboxRow.thread_id)
                .where(or_(ThreadControlRow.thread_id.is_(None),
                    ThreadControlRow.state == ThreadControlState.RUNNING.value),
                or_(JobOutboxRow.state == "ready", and_(
                    JobOutboxRow.state == "claimed", JobOutboxRow.claim_expires_at < now())),
            ).order_by(JobOutboxRow.created_at, JobOutboxRow.id).limit(max_groups * 4)
                .with_for_update(skip_locked=True))).scalars().all())
            if not candidates:
                return []
            thread_id = candidates[0].thread_id
            selected = []
            total_jobs = total_bytes = 0
            frozen_digest = candidates[0].claim_batch_digest
            source = ([row for row in candidates if row.claim_batch_digest == frozen_digest]
                if frozen_digest else candidates)
            for row in source:
                if row.thread_id != thread_id:
                    continue
                result = json.loads(row.result_json)
                encoded_bytes = len(row.result_json.encode())
                if (selected and (len(selected) >= max_groups
                        or total_jobs + len(result) > max_jobs
                        or total_bytes + encoded_bytes > max_bytes)):
                    break
                if len(result) > max_jobs or encoded_bytes > max_bytes:
                    row.state = "suppressed"
                    row.updated_at = now()
                    continue
                selected.append((row, result))
                total_jobs += len(result); total_bytes += encoded_bytes
            batch_digest = (frozen_digest or hashlib.sha256("\n".join(sorted(
                row.id for row, _ in selected)).encode()).hexdigest())
            expires_at = now() + timedelta(seconds=lease_seconds)
            for row, _ in selected:
                row.state = "claimed"; row.claim_owner = owner
                row.claim_batch_digest = batch_digest
                row.claim_expires_at = expires_at; row.updated_at = now()
            return [{"outbox_id": row.id, "group_id": row.group_id,
                "thread_id": row.thread_id, "result_digest": row.result_digest,
                "batch_digest": batch_digest, "results": result} for row, result in selected]

    async def finish_job_outbox_batch(self, outbox_ids: list[str], owner: str, *,
                                      continuation_turn_id: str, delivered: bool,
                                      error: str = "") -> None:
        if not outbox_ids:
            return
        async with self.connections.begin() as db:
            rows = list((await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.id.in_(outbox_ids)).order_by(JobOutboxRow.id)
                .with_for_update())).scalars().all())
            if len(rows) != len(set(outbox_ids)) or any(row.claim_owner != owner for row in rows):
                raise RuntimeError("stale_job_outbox_claim")
            timestamp = now()
            if delivered:
                for row in rows:
                    row.state = "delivered"; row.continuation_turn_id = continuation_turn_id
                    row.claim_owner = ""; row.claim_expires_at = None; row.updated_at = timestamp
                    group = await db.get(JobGroupRow, row.group_id, with_for_update=True)
                    group.state = JobGroupState.CONTINUED.value
                    group.continued_at = timestamp; group.updated_at = timestamp
            else:
                for row in rows:
                    row.state = "ready"; row.claim_owner = ""; row.claim_expires_at = None
                    row.updated_at = timestamp

    async def stop_all_jobs(self, thread_id: str, owner_id: str) -> list[JobView]:
        async with self.job_lock(f"thread-jobs:{thread_id}"):
            async with self.connections.begin() as db:
                thread = (await db.execute(select(ThreadRow).where(
                    ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not thread:
                    raise KeyError("thread_not_found")
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if control and control.state != ThreadControlState.RUNNING.value:
                    control.state = ThreadControlState.RUNNING.value
                    control.detail_json = canonical_json({"hard_cancelled": True,
                        "generation": control.generation})
                    control.completed_at = now()
                    control.updated_at = now()
                groups = list((await db.execute(select(JobGroupRow).where(
                    JobGroupRow.thread_id == thread_id,
                    JobGroupRow.state.in_([JobGroupState.COMMITTED.value,
                        JobGroupState.RUNNING.value, JobGroupState.TERMINAL.value]),
                ).with_for_update())).scalars().all())
                group_ids = [group.id for group in groups]
                rows = list((await db.execute(select(JobRow).where(
                    JobRow.group_id.in_(group_ids or [""])).with_for_update())).scalars().all())
                timestamp = now()
                for group in groups:
                    group.suppress_continuation = True; group.updated_at = timestamp
                for row in rows:
                    if JobState(row.state) not in TERMINAL_JOB_STATES:
                        row.cancel_requested = True; row.state = JobState.CANCELLING.value
                        row.updated_at = timestamp
                await db.execute(update(JobOutboxRow).where(
                    JobOutboxRow.thread_id == thread_id,
                    JobOutboxRow.state.in_(["ready", "claimed"]),
                ).values(state="suppressed", claim_owner="", claim_expires_at=None,
                    updated_at=timestamp))
                await db.flush()
                return [self.job_view(row) for row in rows]

    async def attach_suppressed_job_context(self, turn_id: str) -> int:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.internal:
                return 0
            rows = list((await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.thread_id == turn.thread_id,
                JobOutboxRow.state == "suppressed",
                JobOutboxRow.continuation_turn_id == "",
            ).order_by(JobOutboxRow.created_at, JobOutboxRow.id).limit(32)
                .with_for_update())).scalars().all())
            if not rows:
                return 0
            facts = [{"group_id": row.group_id, "result_digest": row.result_digest,
                "results": json.loads(row.result_json)} for row in rows]
            encoded = canonical_json(facts)
            if len(encoded.encode()) > 1024 * 1024:
                raise RuntimeError("suppressed_job_context_too_large")
            turn.message = ("[suppressed_job_results] Factual results from stopped background groups; "
                "use them only if relevant to the current user request. " + encoded
                + "\n\n[current_user_message]\n" + turn.message)
            timestamp = now()
            for row in rows:
                row.state = "delivered"; row.continuation_turn_id = turn.id; row.updated_at = timestamp
                group = await db.get(JobGroupRow, row.group_id, with_for_update=True)
                group.state = JobGroupState.CONTINUED.value
                group.continued_at = timestamp; group.updated_at = timestamp
            return len(rows)

    @staticmethod
    def job_artifact_view(row: JobArtifactRow) -> JobArtifactOutput:
        return JobArtifactOutput(
            artifact_id=row.artifact_id, artifact_role=row.artifact_role,
            item_key=row.item_key, filename=row.filename, position=row.position,
            output_disposition=OutputDisposition(row.output_disposition), sha256=row.sha256,
            size=row.size, mime=row.mime, object_ref=row.object_ref,
        )

    async def bind_job_artifact(self, value, *, sha256: str, size: int,
                                mime: str, object_ref: str) -> JobArtifactOutput:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == value.job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                await db.get(JobGroupRow, group_id, with_for_update=True)
                job = await db.get(JobRow, value.job_id, with_for_update=True)
                if job.generation != value.generation or JobState(job.state) in TERMINAL_JOB_STATES:
                    raise RuntimeError("stale_job_generation")
                request = JobRequest.model_validate_json(job.request_json)
                contract = next((item for item in request.expected_outputs
                    if item.artifact_role == value.artifact_role), None)
                if not contract:
                    raise RuntimeError("artifact_role_not_granted")
                if value.output_disposition != contract.output_disposition:
                    raise RuntimeError("artifact_disposition_not_granted")
                if size > contract.max_item_bytes:
                    raise ValueError("artifact_item_too_large")
                from .artifact_gateway import MimeRegistry
                if not MimeRegistry.matches(mime, contract.mime_patterns):
                    raise ValueError("artifact_mime_not_allowed")
                existing = (await db.execute(select(JobArtifactRow).where(
                    JobArtifactRow.job_id == job.id,
                    JobArtifactRow.artifact_role == value.artifact_role,
                    JobArtifactRow.item_key == value.item_key,
                ).with_for_update())).scalar_one_or_none()
                if existing:
                    identity = (existing.position, existing.filename, existing.requested_disposition,
                        existing.sha256, existing.size, existing.mime, existing.object_ref)
                    expected = (value.position, value.filename, value.output_disposition.value,
                        sha256, size, mime, object_ref)
                    if identity != expected:
                        raise RuntimeError("artifact_item_key_conflict")
                    return self.job_artifact_view(existing)
                position_conflict = (await db.execute(select(JobArtifactRow.id).where(
                    JobArtifactRow.job_id == job.id,
                    JobArtifactRow.artifact_role == value.artifact_role,
                    JobArtifactRow.position == value.position,
                ))).scalar_one_or_none()
                if position_conflict:
                    raise RuntimeError("artifact_position_conflict")
                artifact_id = identifier()
                timestamp = now()
                db.add(ArtifactRow(
                    id=artifact_id, turn_id=job.origin_turn_id,
                    path=f"jobs/{job.id}/{value.filename}", sha256=sha256, size=size,
                    mime=mime, object_ref=object_ref, created_at=timestamp,
                ))
                row = JobArtifactRow(
                    id=identifier(), job_id=job.id, artifact_id=artifact_id,
                    artifact_role=value.artifact_role, item_key=value.item_key,
                    filename=value.filename, position=value.position,
                    requested_disposition=value.output_disposition.value,
                    output_disposition=OutputDisposition.CANDIDATE.value,
                    sha256=sha256, size=size, mime=mime, object_ref=object_ref,
                    validation_receipts_json="[]", review_receipts_json="[]",
                    state="committed", created_at=timestamp,
                )
                db.add(row)
                await db.flush()
                return self.job_artifact_view(row)

    async def list_job_artifacts(self, job_id: str,
                                 artifact_role: str | None = None) -> list[JobArtifactOutput]:
        async with self.connections() as db:
            query = select(JobArtifactRow).where(JobArtifactRow.job_id == job_id)
            if artifact_role:
                query = query.where(JobArtifactRow.artifact_role == artifact_role)
            rows = (await db.execute(query.order_by(
                JobArtifactRow.artifact_role, JobArtifactRow.position))).scalars().all()
            return [self.job_artifact_view(row) for row in rows]

    async def finalize_job_artifact_collection(self, job_id: str, generation: int,
                                               artifact_role: str, *,
                                               validation_receipts: list[dict] | None = None,
                                               review_receipt: dict | None = None) -> dict:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                await db.get(JobGroupRow, group_id, with_for_update=True)
                job = await db.get(JobRow, job_id, with_for_update=True)
                if job.generation != generation or JobState(job.state) in TERMINAL_JOB_STATES:
                    raise RuntimeError("stale_job_generation")
                request = JobRequest.model_validate_json(job.request_json)
                contract = next((item for item in request.expected_outputs
                    if item.artifact_role == artifact_role), None)
                if not contract:
                    raise RuntimeError("artifact_role_not_granted")
                rows = list((await db.execute(select(JobArtifactRow).where(
                    JobArtifactRow.job_id == job_id,
                    JobArtifactRow.artifact_role == artifact_role,
                ).order_by(JobArtifactRow.position).with_for_update())).scalars().all())
                if not contract.min_items <= len(rows) <= contract.max_items:
                    raise RuntimeError("artifact_collection_cardinality_invalid")
                if [row.position for row in rows] != list(range(len(rows))):
                    raise RuntimeError("artifact_collection_order_invalid")
                if sum(row.size for row in rows) > contract.max_total_bytes:
                    raise ValueError("artifact_collection_too_large")
                receipts = validation_receipts or []
                by_validator = {str(item.get("validator")): item for item in receipts}
                if any(name not in by_validator or by_validator[name].get("status") != "pass"
                       for name in contract.validators):
                    raise RuntimeError("artifact_validation_required")
                digests = [row.sha256 for row in rows]
                if any(item.get("artifact_digests") not in (None, digests) for item in receipts):
                    raise RuntimeError("artifact_validation_digest_mismatch")
                if (contract.requires_review and review_receipt
                        and review_receipt.get("artifact_digests") != digests):
                    raise RuntimeError("artifact_review_digest_mismatch")
                review_passed = (not contract.requires_review or bool(review_receipt)
                    and review_receipt.get("verdict") == "PASS")
                for row in rows:
                    row.validation_receipts_json = canonical_json(receipts)
                    row.review_receipts_json = canonical_json([review_receipt] if review_receipt else [])
                if not review_passed:
                    return {"artifact_role": artifact_role, "state": "awaiting_review",
                        "output_disposition": "candidate", "items": [
                            self.job_artifact_view(row).model_dump(mode="json") for row in rows]}
                for row in rows:
                    row.output_disposition = row.requested_disposition
                    if row.output_disposition in {OutputDisposition.WORKSPACE.value,
                                                  OutputDisposition.DELIVERY.value}:
                        project_path = f"artifacts/jobs/{job.id}/{row.filename}"
                        await self._register_project_file_locked(db, job.thread_id,
                            path=project_path, source_type="artifact", source_turn_id=job.origin_turn_id,
                            sha256=row.sha256, size=row.size, mime=row.mime, object_ref=row.object_ref)
                await db.flush()
                return {"artifact_role": artifact_role, "state": "published",
                    "output_disposition": contract.output_disposition.value,
                    "manifest": {"count": len(rows), "total_bytes": sum(row.size for row in rows),
                        "items": [{"item_key": row.item_key, "position": row.position,
                            "filename": row.filename, "sha256": row.sha256,
                            "size": row.size, "mime": row.mime} for row in rows]},
                    "items": [self.job_artifact_view(row).model_dump(mode="json") for row in rows]}

    @staticmethod
    def thread_view(row: ThreadRow) -> ThreadView:
        return ThreadView.model_validate({key: getattr(row, key) for key in ThreadView.model_fields})

    @staticmethod
    def thread_control_view(row: ThreadControlRow) -> ThreadControlView:
        return ThreadControlView(thread_id=row.thread_id, state=ThreadControlState(row.state),
            generation=row.generation, operation_id=row.operation_id, degraded=row.degraded,
            detail=json.loads(row.detail_json),
            requested_at=utc_datetime(row.requested_at) if row.requested_at else None,
            completed_at=utc_datetime(row.completed_at) if row.completed_at else None,
            updated_at=utc_datetime(row.updated_at))

    @staticmethod
    def pause_participant_view(row: PauseParticipantRow) -> PauseParticipantView:
        return PauseParticipantView(id=row.id, thread_id=row.thread_id,
            generation=row.generation, kind=row.kind, participant_id=row.participant_id,
            state=PauseParticipantState(row.state), checkpoint_ref=row.checkpoint_ref,
            native_thread_id=row.native_thread_id,
            external_execution_id=row.external_execution_id,
            detail=json.loads(row.detail_json), created_at=utc_datetime(row.created_at),
            updated_at=utc_datetime(row.updated_at))

    @staticmethod
    def account_view(row: UserRow) -> AccountView:
        return AccountView.model_validate({key: getattr(row, key) for key in AccountView.model_fields})

    async def create_user(self, username: str, password_hash: str) -> AccountView:
        timestamp = now();identity = identifier()
        row = UserRow(id=identity, owner_id=hashlib.sha256(("account:" + identity).encode()).hexdigest(),
            username=username, password_hash=password_hash, created_at=timestamp, updated_at=timestamp)
        try:
            async with self.connections.begin() as db:
                if (await db.execute(select(UserRow.id).where(UserRow.username == username))).scalar_one_or_none():
                    raise RuntimeError("username_taken")
                db.add(row)
        except IntegrityError as exc:
            raise RuntimeError("username_taken") from exc
        return self.account_view(row)

    async def register_user(self, username: str, password_hash: str, source_owner_id: str = "", *,
                            session_token_hash: str = "",
                            session_expires_at: datetime | None = None) -> AccountView:
        timestamp = now(); identity = identifier()
        row = UserRow(id=identity, owner_id=hashlib.sha256(("account:" + identity).encode()).hexdigest(),
            username=username, password_hash=password_hash, created_at=timestamp, updated_at=timestamp)
        try:
            async with self.connections.begin() as db:
                if (await db.execute(select(UserRow.id).where(UserRow.username == username))).scalar_one_or_none():
                    raise RuntimeError("username_taken")
                if source_owner_id:
                    active = (await db.execute(select(TurnRow.id).join(ThreadRow,
                        ThreadRow.id == TurnRow.thread_id).where(ThreadRow.owner_id == source_owner_id,
                        TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1))).scalar_one_or_none()
                    if active:
                        raise RuntimeError("active_work_prevents_registration")
                    active_job = (await db.execute(select(JobRow.id).where(
                        JobRow.owner_id == source_owner_id,
                        JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                    ).limit(1))).scalar_one_or_none()
                    active_control = (await db.execute(select(SandboxControlRow.thread_id).where(
                        SandboxControlRow.owner_id == source_owner_id).limit(1))).scalar_one_or_none()
                    active_reference = (await db.execute(select(MediaReferenceUploadRow.id).where(
                        MediaReferenceUploadRow.owner_id == source_owner_id,
                        MediaReferenceUploadRow.status == "pending",
                        MediaReferenceUploadRow.expires_at > timestamp).limit(1))).scalar_one_or_none()
                    active_pause = (await db.execute(select(ThreadControlRow.thread_id).where(
                        ThreadControlRow.owner_id == source_owner_id,
                        ThreadControlRow.state.in_([ThreadControlState.PAUSING.value,
                            ThreadControlState.RESUMING.value])).limit(1))).scalar_one_or_none()
                    active_media = (await db.execute(select(MediaJobRow.id).where(
                        MediaJobRow.owner_id == source_owner_id,
                        MediaJobRow.status.not_in(["succeeded", "failed", "cancelled", "unknown"]),
                    ).limit(1))).scalar_one_or_none()
                    active_site = (await db.execute(select(SiteDeployJobRow.id).where(
                        SiteDeployJobRow.owner_id == source_owner_id,
                        SiteDeployJobRow.status.not_in(["succeeded", "failed"]),
                    ).limit(1))).scalar_one_or_none()
                    if any((active_job, active_control, active_reference, active_pause,
                            active_media, active_site)):
                        raise RuntimeError("active_work_prevents_registration")
                db.add(row)
                if session_token_hash:
                    if not session_expires_at:
                        raise ValueError("session_expires_at_required")
                    db.add(AuthSessionRow(token_hash=session_token_hash, user_id=row.id,
                        expires_at=session_expires_at, created_at=timestamp,
                        last_seen_at=timestamp))
                if source_owner_id:
                    for model in OWNER_MIGRATION_ROWS:
                        await db.execute(update(model).where(model.owner_id == source_owner_id)
                            .values(owner_id=row.owner_id))
        except IntegrityError as exc:
            raise RuntimeError("username_taken") from exc
        return self.account_view(row)

    async def get_user_by_username(self, username: str) -> tuple[AccountView, str] | None:
        async with self.connections() as db:
            row = (await db.execute(select(UserRow).where(UserRow.username == username))).scalar_one_or_none()
        return (self.account_view(row), row.password_hash) if row else None

    async def get_user(self, user_id: str) -> AccountView | None:
        async with self.connections() as db:
            row = await db.get(UserRow, user_id)
        return self.account_view(row) if row else None

    async def create_auth_session(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        timestamp = now()
        async with self.connections.begin() as db:
            if not await db.get(UserRow, user_id):
                raise KeyError("user_not_found")
            db.add(AuthSessionRow(token_hash=token_hash, user_id=user_id, expires_at=expires_at,
                created_at=timestamp, last_seen_at=timestamp))

    async def resolve_auth_session(self, token_hash: str) -> AccountView | None:
        timestamp = now()
        async with self.connections.begin() as db:
            row = (await db.execute(select(AuthSessionRow, UserRow).join(UserRow,
                UserRow.id == AuthSessionRow.user_id).where(AuthSessionRow.token_hash == token_hash,
                AuthSessionRow.expires_at > timestamp, UserRow.disabled.is_(False)))).first()
            if not row:
                await db.execute(delete(AuthSessionRow).where(AuthSessionRow.token_hash == token_hash))
                return None
            session, user = row
            if utc_datetime(session.last_seen_at) < timestamp - timedelta(minutes=5):
                session.last_seen_at = timestamp
            return self.account_view(user)

    async def delete_auth_session(self, token_hash: str) -> None:
        async with self.connections.begin() as db:
            await db.execute(delete(AuthSessionRow).where(AuthSessionRow.token_hash == token_hash))

    async def update_username(self, user_id: str, username: str) -> AccountView:
        try:
            async with self.connections.begin() as db:
                if (await db.execute(select(UserRow.id).where(
                        UserRow.username == username, UserRow.id != user_id))).scalar_one_or_none():
                    raise RuntimeError("username_taken")
                row = await db.get(UserRow, user_id, with_for_update=True)
                if not row: raise KeyError("user_not_found")
                row.username = username;row.updated_at = now()
        except IntegrityError as exc:
            raise RuntimeError("username_taken") from exc
        return self.account_view(row)

    async def update_password(self, user_id: str, password_hash: str, *, keep_token_hash: str = "") -> None:
        async with self.connections.begin() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.password_hash = password_hash;row.updated_at = now()
            query = delete(AuthSessionRow).where(AuthSessionRow.user_id == user_id)
            if keep_token_hash:query = query.where(AuthSessionRow.token_hash != keep_token_hash)
            await db.execute(query)

    async def rehash_password(self, user_id: str, password_hash: str) -> None:
        async with self.connections.begin() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.password_hash = password_hash
            row.updated_at = now()

    async def update_account(self, user_id: str, *, username: str | None = None,
                             password_hash: str | None = None, keep_token_hash: str = "") -> AccountView:
        try:
            async with self.connections.begin() as db:
                row = await db.get(UserRow, user_id, with_for_update=True)
                if not row: raise KeyError("user_not_found")
                if username is not None and username != row.username:
                    if (await db.execute(select(UserRow.id).where(
                            UserRow.username == username, UserRow.id != user_id))).scalar_one_or_none():
                        raise RuntimeError("username_taken")
                    row.username = username
                if password_hash is not None:
                    row.password_hash = password_hash
                    query = delete(AuthSessionRow).where(AuthSessionRow.user_id == user_id)
                    if keep_token_hash: query = query.where(AuthSessionRow.token_hash != keep_token_hash)
                    await db.execute(query)
                row.updated_at = now()
        except IntegrityError as exc:
            raise RuntimeError("username_taken") from exc
        return self.account_view(row)

    async def set_user_avatar(self, user_id: str, object_ref: str = "", mime: str = "") -> AccountView:
        async with self.connections.begin() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.avatar_object_ref = object_ref; row.avatar_mime = mime
            row.avatar_version += 1; row.updated_at = now()
        return self.account_view(row)

    async def get_user_avatar(self, user_id: str) -> tuple[str, str] | None:
        async with self.connections() as db:
            row = await db.get(UserRow, user_id)
        if not row or not row.avatar_object_ref or not row.avatar_mime:
            return None
        return row.avatar_object_ref, row.avatar_mime

    async def set_user_disabled(self, user_id: str, disabled: bool) -> AccountView:
        async with self.connections.begin() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.disabled = disabled;row.updated_at = now()
            if disabled:await db.execute(delete(AuthSessionRow).where(AuthSessionRow.user_id == user_id))
        return self.account_view(row)

    async def list_users(self) -> list[AccountView]:
        async with self.connections() as db:
            rows = (await db.execute(select(UserRow).order_by(UserRow.created_at))).scalars().all()
        return [self.account_view(row) for row in rows]

    @staticmethod
    async def next_item_sequence(db: AsyncSession, thread_id: str) -> int:
        sequence = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
            .values(next_item_sequence=ThreadRow.next_item_sequence + 1)
            .returning(ThreadRow.next_item_sequence))).scalar_one_or_none()
        if sequence is None:
            raise KeyError("thread_not_found")
        return sequence

    @staticmethod
    def turn_view(row: TurnRow) -> TurnView:
        return TurnView.model_validate({
            **{key: (row.public_message if key == "message" else row.message
                if key == "runtime_message" else getattr(row, key))
                for key in TurnView.model_fields if key != "attachments"},
            "attachments": json.loads(row.attachments_json),
        })

    @staticmethod
    def input_view(row: TurnInputRow) -> TurnInputView:
        return TurnInputView.model_validate({
            **{key: getattr(row, key) for key in TurnInputView.model_fields if key != "attachments"},
            "attachments": json.loads(row.attachments_json),
        })

    async def create_thread(self, value: ThreadCreate, resolved: ResolvedProfile, owner_id: str) -> ThreadView:
        timestamp = now()
        row = ThreadRow(id=identifier(), owner_id=owner_id, harness=resolved.harness.value, model=resolved.model,
            reasoning_level=resolved.reasoning_level, reasoning_effort=resolved.reasoning_effort,
            title=value.title, created_at=timestamp, updated_at=timestamp)
        async with self.connections.begin() as db:
            db.add(row)
        return self.thread_view(row)

    async def list_threads(self, owner_id: str, *, archived: bool = False) -> list[ThreadView]:
        async with self.connections() as db:
            archive_filter = ThreadRow.archived_at.is_not(None) if archived else ThreadRow.archived_at.is_(None)
            rows = (await db.execute(select(ThreadRow).where(ThreadRow.owner_id == owner_id, archive_filter)
                .order_by(ThreadRow.updated_at.desc()))).scalars().all()
        return [self.thread_view(row) for row in rows]

    async def list_thread_summaries(self, owner_id: str, *, archived: bool = False) -> list[dict]:
        threads = await self.list_threads(owner_id, archived=archived)
        async with self.connections() as db:
            ranked = select(TurnRow.id, TurnRow.thread_id, TurnRow.state, TurnRow.updated_at,
                func.row_number().over(partition_by=TurnRow.thread_id,
                    order_by=(TurnRow.created_at.desc(), TurnRow.id.desc())).label("rank")).join(
                ThreadRow, ThreadRow.id == TurnRow.thread_id).where(ThreadRow.owner_id == owner_id).subquery()
            rows = (await db.execute(select(ranked.c.id, ranked.c.thread_id, ranked.c.state, ranked.c.updated_at)
                .where(ranked.c.rank == 1))).all()
        latest = {row.thread_id: row for row in rows}
        return [{**thread.model_dump(mode="json"),
            "latest_turn_id": latest[thread.id].id if thread.id in latest else "",
            "unread_completion": bool(thread.id in latest and latest[thread.id].state == TurnState.COMPLETED.value
                and latest[thread.id].id != thread.last_read_turn_id),
            "latest_turn_state": latest[thread.id].state if thread.id in latest else "",
            "latest_turn_updated_at": latest[thread.id].updated_at if thread.id in latest else None,
        } for thread in threads]

    async def mark_thread_read(self, thread_id: str, turn_id: str, owner_id: str) -> str:
        async with self.connections.begin() as db:
            thread = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).values(last_read_turn_id=ThreadRow.last_read_turn_id)
                .returning(ThreadRow))).scalar_one_or_none()
            if thread is None:
                raise KeyError("thread_not_found")
            latest = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id)
                .order_by(TurnRow.created_at.desc(), TurnRow.id.desc()).limit(1))).scalar_one_or_none()
            # A stale read may only acknowledge the completion actually displayed.
            # It must not consume a newer completion or pre-read a running Turn.
            if latest and latest.id == turn_id and latest.state == TurnState.COMPLETED.value:
                thread.last_read_turn_id = turn_id
            return thread.last_read_turn_id

    async def get_thread(self, thread_id: str, owner_id: str | None = None) -> ThreadView | None:
        async with self.connections() as db:
            query = select(ThreadRow).where(ThreadRow.id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
        return self.thread_view(row) if row else None

    async def set_thread_archived(self, thread_id: str, owner_id: str, *, archived: bool) -> ThreadView:
        terminal = {state.value for state in {
            TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED,
            TurnState.PAUSED,
        }}
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if archived:
                states = (await db.execute(select(TurnRow.state).where(TurnRow.thread_id == thread_id))).scalars().all()
                if any(state not in terminal for state in states):
                    raise RuntimeError("active_thread_cannot_be_archived")
            timestamp = now()
            thread.archived_at = timestamp if archived else None
            thread.updated_at = timestamp
        return self.thread_view(thread)

    async def delete_thread(self, thread_id: str, owner_id: str) -> list[str]:
        terminal = {state.value for state in {
            TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED,
            TurnState.PAUSED,
        }}
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            turns = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id))).scalars().all()
            if any(turn.state not in terminal for turn in turns):
                raise RuntimeError("active_thread_cannot_be_deleted")
            media_jobs = (await db.execute(select(MediaJobRow).where(
                MediaJobRow.thread_id == thread_id))).scalars().all()
            if any(job.status not in {"succeeded", "failed", "cancelled", "unknown"} for job in media_jobs):
                raise RuntimeError("active_thread_cannot_be_deleted")
            site_jobs = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.thread_id == thread_id))).scalars().all()
            if any(job.status not in {"succeeded", "failed"} for job in site_jobs):
                raise RuntimeError("active_thread_cannot_be_deleted")
            site_projects = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.thread_id == thread_id))).scalars().all()
            if any(project.status == "active" for project in site_projects):
                raise RuntimeError("published_site_prevents_thread_delete")
            turn_ids = [turn.id for turn in turns]
            await db.execute(delete(SandboxAuditRow).where(SandboxAuditRow.thread_id == thread_id))
            await db.execute(delete(SandboxPortGrantRow).where(SandboxPortGrantRow.thread_id == thread_id))
            await db.execute(delete(SandboxControlRow).where(SandboxControlRow.thread_id == thread_id))
            media_job_ids = [job.id for job in media_jobs]
            if media_job_ids:
                await db.execute(delete(MediaOutboxRow).where(MediaOutboxRow.job_id.in_(media_job_ids)))
                await db.execute(delete(MediaJobOutputRow).where(MediaJobOutputRow.job_id.in_(media_job_ids)))
                await db.execute(delete(MediaJobRow).where(MediaJobRow.id.in_(media_job_ids)))
            await db.execute(delete(MediaDraftRow).where(MediaDraftRow.thread_id == thread_id))
            await db.execute(delete(MediaBatchRow).where(MediaBatchRow.thread_id == thread_id))
            await db.execute(delete(MediaReferenceUploadRow).where(MediaReferenceUploadRow.thread_id == thread_id))
            site_project_ids = [project.id for project in site_projects]
            site_version_ids = list((await db.execute(select(SiteVersionRow.id).where(
                SiteVersionRow.project_id.in_(site_project_ids)))).scalars().all()) if site_project_ids else []
            if site_version_ids:
                await db.execute(delete(SiteFileRow).where(SiteFileRow.version_id.in_(site_version_ids)))
                await db.execute(delete(SiteVersionRow).where(SiteVersionRow.id.in_(site_version_ids)))
            site_draft_ids = list((await db.execute(select(SiteDraftRow.id).where(
                SiteDraftRow.thread_id == thread_id))).scalars().all())
            if site_draft_ids:
                await db.execute(delete(SiteDraftFileRow).where(SiteDraftFileRow.draft_id.in_(site_draft_ids)))
            await db.execute(delete(SiteDeployJobRow).where(SiteDeployJobRow.thread_id == thread_id))
            await db.execute(delete(SiteDraftRow).where(SiteDraftRow.thread_id == thread_id))
            await db.execute(delete(SiteProjectRow).where(SiteProjectRow.thread_id == thread_id))
            await db.execute(delete(InteractionRow).where(InteractionRow.thread_id == thread_id))
            await db.execute(delete(PauseParticipantRow).where(
                PauseParticipantRow.thread_id == thread_id))
            await db.execute(delete(ThreadControlRow).where(
                ThreadControlRow.thread_id == thread_id))
            await db.execute(delete(ProjectFileRow).where(ProjectFileRow.thread_id == thread_id))
            await db.execute(delete(ItemRow).where(ItemRow.thread_id == thread_id))
            if turn_ids:
                await db.execute(delete(TurnProjectFileRow).where(TurnProjectFileRow.turn_id.in_(turn_ids)))
                await db.execute(delete(EventRow).where(EventRow.turn_id.in_(turn_ids)))
                await db.execute(delete(TurnInputRow).where(TurnInputRow.turn_id.in_(turn_ids)))
                await db.execute(delete(ArtifactRow).where(ArtifactRow.turn_id.in_(turn_ids)))
                await db.execute(delete(TurnRow).where(TurnRow.id.in_(turn_ids)))
            await db.delete(thread)
        if self.events:
            await self.events.delete(turn_ids)
        return turn_ids

    @staticmethod
    def sandbox_control_view(row: SandboxControlRow) -> dict:
        return {"thread_id": row.thread_id, "sandbox_id": row.sandbox_id, "state": row.state,
            "generation": row.generation, "pty_session_id": row.pty_session_id,
            "expires_at": utc_datetime(row.expires_at), "created_at": row.created_at,
            "updated_at": row.updated_at}

    async def get_sandbox_control(self, thread_id: str, owner_id: str | None = None) -> dict | None:
        async with self.connections() as db:
            query = select(SandboxControlRow).where(SandboxControlRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(SandboxControlRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
        return self.sandbox_control_view(row) if row else None

    async def sandbox_control_nonce(self, thread_id: str, owner_id: str) -> str:
        async with self.connections() as db:
            return (await db.execute(select(SandboxControlRow.capability_nonce).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id))).scalar_one_or_none() or ""

    async def begin_sandbox_control(self, thread_id: str, owner_id: str, sandbox_id: str,
                                    capability_nonce: str, expires_at: datetime) -> dict:
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            active = (await db.execute(select(TurnRow.id).where(TurnRow.thread_id == thread_id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1))).scalar_one_or_none()
            if active:
                raise RuntimeError("active_turn_prevents_sandbox_takeover")
            row = await db.get(SandboxControlRow, thread_id, with_for_update=True)
            timestamp = now()
            if row and row.state == "user_controlled" and utc_datetime(row.expires_at) > timestamp:
                if row.sandbox_id != sandbox_id:
                    raise RuntimeError("sandbox_control_conflict")
                row.expires_at = expires_at; row.updated_at = timestamp
            elif row:
                row.owner_id = owner_id; row.sandbox_id = sandbox_id; row.state = "user_controlled"
                row.generation += 1; row.capability_nonce = capability_nonce; row.pty_session_id = ""
                row.expires_at = expires_at; row.created_at = timestamp; row.updated_at = timestamp
            else:
                row = SandboxControlRow(thread_id=thread_id, owner_id=owner_id,
                    sandbox_id=sandbox_id, state="user_controlled", generation=1,
                    capability_nonce=capability_nonce, expires_at=expires_at,
                    created_at=timestamp, updated_at=timestamp)
                db.add(row)
        return self.sandbox_control_view(row)

    async def touch_sandbox_control(self, thread_id: str, owner_id: str, expires_at: datetime) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id,
                SandboxControlRow.state == "user_controlled").with_for_update())).scalar_one_or_none()
            if not row or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("sandbox_control_not_active")
            row.expires_at = expires_at; row.updated_at = now()
        return self.sandbox_control_view(row)

    async def set_sandbox_terminal(self, thread_id: str, owner_id: str,
                                   sandbox_id: str, pty_session_id: str) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id,
                SandboxControlRow.sandbox_id == sandbox_id,
                SandboxControlRow.state == "user_controlled").with_for_update())).scalar_one_or_none()
            if not row or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("sandbox_control_not_active")
            row.pty_session_id = pty_session_id; row.updated_at = now()
        return self.sandbox_control_view(row)

    async def mark_sandbox_control_releasing(self, thread_id: str, owner_id: str) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise RuntimeError("sandbox_control_not_active")
            row.state = "releasing"; row.updated_at = now()
        return self.sandbox_control_view(row)

    async def finish_sandbox_control(self, thread_id: str, owner_id: str) -> None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if row:
                await db.delete(row)
            await db.execute(update(SandboxPortGrantRow).where(
                SandboxPortGrantRow.thread_id == thread_id).values(revoked=True))

    async def active_sandbox_control_ids(self) -> set[str]:
        async with self.connections() as db:
            rows = (await db.execute(select(SandboxControlRow.sandbox_id).where(
                SandboxControlRow.state.in_(["user_controlled", "releasing"])))).scalars().all()
        return set(rows)

    async def active_job_sandbox_ids(self) -> set[str]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobAttemptRow.acceptance_receipt_json).join(
                JobRow,JobRow.id==JobAttemptRow.job_id).where(
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                JobAttemptRow.accepted_at.is_not(None)))).scalars().all()
        result=set()
        for encoded in rows:
            try:identity=json.loads(encoded).get('sandbox_id','')
            except (TypeError,ValueError):continue
            if isinstance(identity,str) and identity:result.add(identity)
        return result

    async def expired_sandbox_controls(self, limit: int = 20) -> list[dict]:
        async with self.connections() as db:
            rows = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.expires_at <= now()).order_by(SandboxControlRow.expires_at)
                .limit(limit))).scalars().all()
        return [self.sandbox_control_view(row) | {"owner_id": row.owner_id,
            "capability_nonce": row.capability_nonce} for row in rows]

    async def update_thread_checkpoint_idle(self, thread_id: str, owner_id: str,
                                            checkpoint_ref: str) -> None:
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            active = (await db.execute(select(TurnRow.id).where(TurnRow.thread_id == thread_id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1))).scalar_one_or_none()
            if active:
                raise RuntimeError("active_turn_prevents_checkpoint")
            thread.checkpoint_ref = checkpoint_ref; thread.updated_at = now()

    async def add_sandbox_audit(self, thread_id: str, owner_id: str, sandbox_id: str,
                                action: str, detail: dict | None = None) -> None:
        safe_detail = detail or {}
        async with self.connections.begin() as db:
            db.add(SandboxAuditRow(id=identifier(), thread_id=thread_id, owner_id=owner_id,
                sandbox_id=sandbox_id, action=action[:80],
                detail_json=json.dumps(safe_detail, ensure_ascii=True, sort_keys=True), created_at=now()))

    async def list_sandbox_audit(self, thread_id: str, owner_id: str, limit: int = 100) -> list[dict]:
        async with self.connections() as db:
            rows = (await db.execute(select(SandboxAuditRow).where(
                SandboxAuditRow.thread_id == thread_id, SandboxAuditRow.owner_id == owner_id)
                .order_by(SandboxAuditRow.created_at.desc()).limit(min(max(limit, 1), 500)))).scalars().all()
        return [{"id": row.id, "action": row.action, "detail": json.loads(row.detail_json or "{}"),
            "created_at": row.created_at} for row in rows]

    async def create_sandbox_port_grant(self, thread_id: str, owner_id: str,
                                        sandbox_id: str, port: int,
                                        expires_at: datetime) -> dict:
        row = SandboxPortGrantRow(id=identifier(), thread_id=thread_id, owner_id=owner_id,
            sandbox_id=sandbox_id, port=port, expires_at=expires_at,
            revoked=False, created_at=now())
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow.id).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id))).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            db.add(row)
        return {"id": row.id, "thread_id": thread_id, "sandbox_id": sandbox_id,
            "port": port, "expires_at": expires_at}

    async def get_sandbox_port_grant(self, grant_id: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SandboxPortGrantRow).where(
                SandboxPortGrantRow.id == grant_id, SandboxPortGrantRow.owner_id == owner_id,
                SandboxPortGrantRow.revoked == False,
                SandboxPortGrantRow.expires_at > now()))).scalar_one_or_none()
        return ({"id": row.id, "thread_id": row.thread_id, "sandbox_id": row.sandbox_id,
            "port": row.port, "expires_at": row.expires_at} if row else None)

    async def revoke_sandbox_port_grant(self, grant_id: str, owner_id: str) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(update(SandboxPortGrantRow).where(
                SandboxPortGrantRow.id == grant_id, SandboxPortGrantRow.owner_id == owner_id)
                .values(revoked=True))
        return bool(result.rowcount)

    @observed("db.turn.create")
    async def create_turn(self, thread_id: str, value: TurnCreate, resolved: ResolvedProfile, owner_id: str, *,
                          traceparent: str = "", network_access: str = "restricted",
                          internal: bool = False, runtime_image_digest: str = "",
                          runtime_bundle_digest: str = "",
                          runtime_code_bundle_digest: str = "") -> tuple[TurnView, bool, bool]:
        if network_access not in {"restricted", "public"}:
            raise ValueError("Invalid Turn network access")
        async with self.connections.begin() as db:
            thread = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).values(updated_at=ThreadRow.updated_at)
                .returning(ThreadRow))).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if thread.archived_at is not None:
                raise RuntimeError("thread_archived")
            existing = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id, TurnRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                attachments = [item.model_dump(mode="json") for item in value.attachments]
                if (existing.public_message != value.message or json.loads(existing.attachments_json) != attachments
                        or existing.harness != resolved.harness.value or existing.model != resolved.model
                        or existing.reasoning_level != resolved.reasoning_level
                        or existing.delivery_mode != value.delivery_mode
                        or existing.network_access != network_access or existing.internal != internal):
                    raise RuntimeError("idempotency_conflict")
                return self.turn_view(existing), False, thread.active_turn_id == existing.id
            control = await db.get(ThreadControlRow, thread_id)
            if control and control.state != ThreadControlState.RUNNING.value:
                raise RuntimeError("thread_paused")
            runtime_message = value.message
            consume_resume_context = False
            if control:
                pause_detail = json.loads(control.detail_json)
                if pause_detail.get("resume_context_required"):
                    previous = (await db.execute(select(TurnRow).where(
                        TurnRow.thread_id == thread_id, TurnRow.internal == False,
                    ).order_by(TurnRow.created_at.desc(), TurnRow.id.desc()).limit(1)
                        .with_for_update())).scalar_one_or_none()
                    paths = list((await db.execute(select(ProjectFileRow.path).where(
                        ProjectFileRow.thread_id == thread_id,
                        ProjectFileRow.deleted == False).distinct().order_by(
                            ProjectFileRow.path))).scalars().all())
                    runtime_message = ("[pause_resume_context] Resume the interrupted Thread from its "
                        "durable state. Do not repeat completed external side effects. "
                        + canonical_json({"generation": control.generation,
                            "degraded": bool(pause_detail.get("degraded")),
                            "previous_user_objective": previous.public_message if previous else "",
                            "durable_project_files": paths})
                        + "\n\n[current_user_message]\n" + value.message)
                    consume_resume_context = True
            active_running = False
            if thread.active_turn_id:
                active = await db.get(TurnRow, thread.active_turn_id)
                if active and TurnState(active.state) not in TERMINAL:
                    active_running = True
            if not active_running:
                queued = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id,
                    TurnRow.state.not_in([state.value for state in TERMINAL]))
                    .order_by(TurnRow.created_at, TurnRow.id).limit(1))).scalar_one_or_none()
                if queued:
                    thread.active_turn_id = queued.id
                    active_running = True
            if active_running and value.delivery_mode == "start":
                raise RuntimeError("thread_busy")
            if value.delivery_mode == "steer":
                raise RuntimeError("steer_requires_active_turn_endpoint")
            timestamp = now()
            if consume_resume_context:
                control.detail_json = canonical_json({
                    **pause_detail, "resume_context_required": False})
                control.updated_at = timestamp
            row = TurnRow(
                id=identifier(), thread_id=thread_id, idempotency_key=value.idempotency_key,
                message=runtime_message, public_message=value.message,
                attachments_json=json.dumps([a.model_dump(mode="json") for a in value.attachments]),
                harness=resolved.harness.value, model=resolved.model, reasoning_level=resolved.reasoning_level,
                reasoning_effort=resolved.reasoning_effort, delivery_mode=value.delivery_mode,
                network_access=network_access, internal=internal,
                runtime_image_digest=runtime_image_digest,
                runtime_code_bundle_digest=runtime_code_bundle_digest,
                runtime_bundle_digest=runtime_bundle_digest,
                state=TurnState.QUEUED.value, submit_traceparent=traceparent, created_at=timestamp, updated_at=timestamp,
            )
            db.add(row)
            item_sequence = await self.next_item_sequence(db, thread_id)
            db.add(ItemRow(id=identifier(), thread_id=thread_id, turn_id=row.id, sequence=item_sequence,
                type=ItemType.USER_MESSAGE.value, status="completed", summary=value.message,
                payload_json=json.dumps({"attachments":[item.model_dump(mode="json") for item in value.attachments],
                    "internal": internal}),
                created_at=timestamp, updated_at=timestamp))
            if not active_running:
                thread.active_turn_id = row.id
            thread.updated_at = timestamp
        return self.turn_view(row), True, not active_running

    async def claim_next_turn(self, thread_id: str, completed_turn_id: str) -> TurnView | None:
        async with self.connections.begin() as db:
            thread = await db.get(ThreadRow, thread_id, with_for_update=True)
            if not thread or thread.active_turn_id != completed_turn_id:
                return None
            control = await db.get(ThreadControlRow, thread_id)
            if control and control.state != ThreadControlState.RUNNING.value:
                thread.active_turn_id = ""
                thread.updated_at = now()
                return None
            row = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id,
                TurnRow.state == TurnState.QUEUED.value, TurnRow.id != completed_turn_id).order_by(TurnRow.created_at).limit(1))).scalar_one_or_none()
            thread.active_turn_id = row.id if row else ""
            thread.updated_at = now()
        return self.turn_view(row) if row else None

    async def record_turn_trace(self, turn_id: str, traceparent: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            saved = (await db.execute(update(TurnRow).where(TurnRow.id==turn_id,
                TurnRow.lease_owner==owner,TurnRow.recovery_generation==generation,
                TurnRow.lease_expires_at>now()).values(last_traceparent=traceparent).returning(TurnRow.id))).scalar_one_or_none()
            if saved is None: raise RuntimeError("stale_turn_lease")

    async def get_turn(self, turn_id: str) -> TurnView | None:
        async with self.connections() as db:
            row = await db.get(TurnRow, turn_id)
        return self.turn_view(row) if row else None

    async def repair_thread_heads(self) -> None:
        terminal = [state.value for state in TERMINAL]
        async with self.connections() as db:
            candidates = (await db.execute(select(ThreadRow.id, ThreadRow.active_turn_id)
                .outerjoin(TurnRow, TurnRow.id == ThreadRow.active_turn_id)
                .where(or_(TurnRow.id.is_(None), TurnRow.state.in_(terminal)),
                    or_(ThreadRow.active_turn_id != "", ThreadRow.id.in_(select(TurnRow.thread_id)
                        .where(TurnRow.state.not_in(terminal)).correlate(None)))))).all()
        for thread_id, expected_head in candidates:
            async with self.connections.begin() as db:
                locked = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id,
                    ThreadRow.active_turn_id == expected_head).values(updated_at=ThreadRow.updated_at)
                    .returning(ThreadRow.id))).scalar_one_or_none()
                if locked is None:
                    continue
                next_id = (await db.execute(select(TurnRow.id).where(TurnRow.thread_id == thread_id,
                    TurnRow.state.not_in(terminal)).order_by(TurnRow.created_at, TurnRow.id).limit(1))).scalar_one_or_none()
                await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
                    .values(active_turn_id=next_id or ""))

    async def recoverable_turns(self) -> list[TurnView]:
        # Repair the crash window between committing a terminal Turn and advancing
        # its Thread, before selecting work. Queued followers are never claimable.
        await self.repair_thread_heads()
        terminal = [state.value for state in TERMINAL]
        async with self.connections() as db:
            rows = (await db.execute(select(TurnRow).join(ThreadRow,
                ThreadRow.active_turn_id == TurnRow.id).where(TurnRow.state.not_in(terminal),
                or_(TurnRow.lease_expires_at.is_(None), TurnRow.lease_expires_at <= now()))
                .order_by(TurnRow.created_at))).scalars().all()
        return [self.turn_view(row) for row in rows]

    async def nonterminal_turn_ids(self) -> set[str]:
        terminal = [state.value for state in {
            *TERMINAL,
        }]
        async with self.connections() as db:
            rows = (await db.execute(select(TurnRow.id).where(TurnRow.state.not_in(terminal)))).scalars().all()
        return set(rows)

    async def claim_turn(self, turn_id: str, owner: str, lease_seconds: int) -> TurnView | None:
        timestamp = now()
        terminal = [state.value for state in TERMINAL]
        async with self.connections.begin() as db:
            row = (await db.execute(update(TurnRow).where(
                TurnRow.id == turn_id, TurnRow.state.not_in(terminal),
                TurnRow.id.in_(select(ThreadRow.active_turn_id).where(ThreadRow.id == TurnRow.thread_id)),
                or_(TurnRow.lease_owner == owner, TurnRow.lease_owner == "",
                    TurnRow.lease_expires_at.is_(None), TurnRow.lease_expires_at <= timestamp)
            ).values(lease_owner=owner, lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
                recovery_generation=case((TurnRow.lease_owner == owner, TurnRow.recovery_generation),
                    else_=TurnRow.recovery_generation + 1), updated_at=timestamp)
                .returning(TurnRow))).scalar_one_or_none()
        return self.turn_view(row) if row else None

    async def owns_turn_lease(self, turn_id: str, owner: str, generation: int) -> bool:
        async with self.connections() as db:
            return (await db.execute(select(TurnRow.id).where(TurnRow.id == turn_id,
                TurnRow.lease_owner == owner, TurnRow.recovery_generation == generation,
                TurnRow.lease_expires_at > now()))).scalar_one_or_none() is not None

    async def renew_turn_lease(self, turn_id: str, owner: str, generation: int, lease_seconds: int) -> bool:
        timestamp = now()
        async with self.connections.begin() as db:
            result = await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                TurnRow.lease_owner == owner, TurnRow.recovery_generation == generation,
                TurnRow.lease_expires_at > timestamp).values(
                    lease_expires_at=timestamp + timedelta(seconds=lease_seconds), updated_at=timestamp))
            return result.rowcount == 1

    async def release_turn_lease(self, turn_id: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if row and row.lease_owner == owner and row.recovery_generation == generation:
                row.lease_owner = ""
                row.lease_expires_at = None
                row.updated_at = now()

    async def update_runtime_sequence(self, turn_id: str, sequence: int, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                raise RuntimeError("stale_turn_lease")
            row.runtime_sequence = max(row.runtime_sequence, sequence)
            row.updated_at = now()

    @observed("db.execution.start")
    async def start_runtime_execution(self, turn_id: str, execution_id: str, owner: str, generation: int,
                                      *, input_id: str | None = None) -> None:
        updated_item_id = ""
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                raise RuntimeError("stale_turn_lease")
            if input_id and input_id != turn_id:
                pending = await db.get(TurnInputRow, input_id, with_for_update=True)
                if not pending or pending.turn_id != turn_id or pending.status != "pending":
                    raise RuntimeError("input_not_pending")
                pending.status = "delivered"; pending.delivery_mode = "deferred_safe_point"
                pending.updated_at = now()
                items = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.USER_MESSAGE.value))).scalars().all()
                item = next((candidate for candidate in items
                    if json.loads(candidate.payload_json).get("input_id") == input_id), None)
                if item:
                    payload = json.loads(item.payload_json or "{}")
                    payload.update({"input_status":"delivered",
                        "delivery_mode":"deferred_safe_point"})
                    item.payload_json = json.dumps(payload); item.updated_at = now()
                    updated_item_id = item.id
            row.runtime_execution_id = execution_id
            row.runtime_sequence = 0
            row.updated_at = now()
        if updated_item_id:
            await self.append_event(turn_id, EventKind.ITEM_UPDATED,
                {"item_id":updated_item_id, "status":"in_progress", "input_id":input_id,
                    "input_status":"delivered", "delivery_mode":"deferred_safe_point"},
                item_id=updated_item_id)

    async def set_turn_capability(self, turn_id: str, nonce: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                raise RuntimeError("stale_turn_lease")
            row.capability_nonce = nonce
            row.updated_at = now()

    async def list_turns(self, thread_id: str, owner_id: str | None = None) -> list[TurnView]:
        async with self.connections() as db:
            query = select(TurnRow).join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(TurnRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(TurnRow.created_at, TurnRow.id))).scalars().all()
        return [self.turn_view(row) for row in rows]

    async def get_turn_for_owner(self, turn_id: str, owner_id: str) -> TurnView | None:
        async with self.connections() as db:
            row = (await db.execute(select(TurnRow).join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(
                TurnRow.id == turn_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
        return self.turn_view(row) if row else None

    @observed("db.input.create")
    async def create_turn_input(self, turn_id: str, value: TurnInputCreate, owner_id: str) -> tuple[TurnInputView, bool]:
        accepting = {TurnState.QUEUED.value, TurnState.RESTORING.value, TurnState.STARTING.value,
            TurnState.RUNNING.value, TurnState.AWAITING_USER.value}
        async with self.connections.begin() as db:
            candidate = (await db.execute(select(TurnRow.thread_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(
                    TurnRow.id == turn_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
            if not candidate:
                raise KeyError("turn_not_found")
            thread = await db.get(ThreadRow, candidate, with_for_update=True)
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not thread or thread.owner_id != owner_id or not turn or turn.thread_id != thread.id:
                raise KeyError("turn_not_found")
            existing = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                attachments = [item.model_dump(mode="json") for item in value.attachments]
                if existing.message != value.message or json.loads(existing.attachments_json) != attachments:
                    raise RuntimeError("idempotency_conflict")
                return self.input_view(existing), False
            if turn.state not in accepting or turn.cancel_requested:
                raise RuntimeError("turn_not_accepting_input")
            timestamp = now()
            sequence = (await db.execute(select(TurnInputRow.sequence).where(TurnInputRow.turn_id == turn_id)
                .order_by(TurnInputRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            row = TurnInputRow(id=identifier(), turn_id=turn_id, sequence=sequence + 1,
                idempotency_key=value.idempotency_key, message=value.message,
                attachments_json=json.dumps([item.model_dump(mode="json") for item in value.attachments]),
                status="pending", delivery_mode="pending", created_at=timestamp, updated_at=timestamp)
            db.add(row)
            item_sequence = await self.next_item_sequence(db, turn.thread_id)
            db.add(ItemRow(id=identifier(), thread_id=turn.thread_id, turn_id=turn_id, sequence=item_sequence,
                type=ItemType.USER_MESSAGE.value, status="in_progress", summary=value.message,
                payload_json=json.dumps({"input_id":row.id, "input_status":"pending",
                    "delivery_mode":"pending",
                    "attachments":[item.model_dump(mode="json") for item in value.attachments]}),
                created_at=timestamp, updated_at=timestamp))
        return self.input_view(row), True

    async def next_pending_input(self, turn_id: str) -> TurnInputView | None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.status == "pending").order_by(TurnInputRow.sequence).limit(1).with_for_update(skip_locked=True))).scalar_one_or_none()
            if not row:
                return None
            # Selection is not delivery. start_runtime_execution marks the input
            # only when its execution identity is durably assigned.
        return self.input_view(row)

    async def set_input_status(self, input_id: str, status: str, delivery_mode: str | None = None) -> None:
        updated_item_id = ""
        turn_id = ""
        item_status = ""
        async with self.connections.begin() as db:
            row = await db.get(TurnInputRow, input_id, with_for_update=True)
            if row:
                if row.status == "applied" and status != "applied":
                    return
                row.status = status; row.updated_at = now()
                if delivery_mode:
                    row.delivery_mode = delivery_mode
                items = (await db.execute(select(ItemRow).where(
                    ItemRow.turn_id == row.turn_id,
                    ItemRow.type == ItemType.USER_MESSAGE.value,
                ))).scalars().all()
                item = next((candidate for candidate in items
                    if json.loads(candidate.payload_json).get("input_id") == input_id), None)
                if item:
                    payload = json.loads(item.payload_json or "{}")
                    payload.update({"input_status":status, "delivery_mode":row.delivery_mode})
                    next_status = {"applied":"completed", "failed":"failed"}.get(status, "in_progress")
                    item.status = next_status; item.payload_json = json.dumps(payload)
                    item.updated_at = now(); updated_item_id = item.id
                    turn_id = row.turn_id; item_status = next_status
        if updated_item_id:
            await self.append_event(turn_id, EventKind.ITEM_UPDATED,
                {"item_id":updated_item_id, "status":item_status, "input_id":input_id,
                    "input_status":status, "delivery_mode":delivery_mode or ""},
                item_id=updated_item_id)

    async def get_turn_input(self, input_id: str) -> TurnInputView | None:
        async with self.connections() as db:
            row = await db.get(TurnInputRow, input_id)
        return self.input_view(row) if row else None

    async def fail_unapplied_inputs(self, turn_id: str, *, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> None:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if (lease_owner is not None and (turn.lease_owner != lease_owner
                    or turn.recovery_generation != recovery_generation)):
                raise RuntimeError("stale_turn_lease")
            rows = (await db.execute(select(TurnInputRow).where(
                TurnInputRow.turn_id == turn_id,
                TurnInputRow.status.in_(["pending", "delivered", "unknown"]),
            ).with_for_update())).scalars().all()
            if not rows:
                return
            input_ids = {row.id for row in rows}
            timestamp = now()
            for row in rows:
                row.status = "failed"; row.updated_at = timestamp
            items = (await db.execute(select(ItemRow).where(
                ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.USER_MESSAGE.value,
            ))).scalars().all()
            for item in items:
                if json.loads(item.payload_json).get("input_id") in input_ids:
                    item.status = "failed"; item.updated_at = timestamp

    async def apply_delivered_inputs(self, turn_id: str) -> None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.status == "delivered").with_for_update())).scalars().all()
            for row in rows:
                row.status = "applied"; row.updated_at = now()
            input_ids = {row.id for row in rows}
            if input_ids:
                items = (await db.execute(select(ItemRow).where(
                    ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.USER_MESSAGE.value,
                ))).scalars().all()
                for item in items:
                    if json.loads(item.payload_json).get("input_id") in input_ids:
                        item.status = "completed"; item.updated_at = now()

    async def list_turn_inputs(self, turn_id: str) -> list[TurnInputView]:
        async with self.connections() as db:
            rows = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id)
                .order_by(TurnInputRow.sequence))).scalars().all()
        return [self.input_view(row) for row in rows]

    async def _finish_turn_records(self, db, turn_id: str) -> None:
        inputs = (await db.execute(select(TurnInputRow).where(
            TurnInputRow.turn_id == turn_id, TurnInputRow.status.in_(["pending", "delivered", "unknown"])
        ).with_for_update())).scalars().all()
        ids = {value.id for value in inputs}
        for value in inputs:
            value.status = "failed"; value.updated_at = now()
        await db.execute(update(InteractionRow).where(InteractionRow.turn_id == turn_id,
            InteractionRow.status.in_(["pending", "responding"])).values(status="expired", updated_at=now()))
        items = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id))).scalars().all()
        for item in items:
            if item.status == "in_progress" and item.type not in {ItemType.USER_MESSAGE.value, ItemType.APPROVAL.value}:
                item.status = "interrupted"; item.updated_at = now()
            if (json.loads(item.payload_json).get("input_id") in ids
                    or item.type == ItemType.APPROVAL.value and item.status == "in_progress"):
                item.status = "failed"; item.updated_at = now()
        await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == turn_id,
            MediaBatchRow.status == "open").values(status="sealed", sealed_at=now(), updated_at=now()))

    @observed("db.turn.cancel")
    async def request_turn_cancel(self, turn_id: str) -> TurnView:
        terminal = [state.value for state in (TurnState.COMPLETED, TurnState.WAITING_USER,
            TurnState.FAILED, TurnState.CANCELLED, TurnState.PAUSED)]
        async with self.connections.begin() as db:
            # Conditional UPDATE serializes cancellation with completion and lease claims
            # on PostgreSQL and SQLite alike.
            row = (await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                TurnRow.state.not_in(terminal)).values(cancel_requested=True, updated_at=now())
                .returning(TurnRow))).scalar_one_or_none()
            if row is None:
                if await db.get(TurnRow, turn_id) is None:
                    raise KeyError("turn_not_found")
                raise RuntimeError("turn_already_terminal")
            if row.state == TurnState.QUEUED.value and (not row.lease_expires_at
                    or utc_datetime(row.lease_expires_at) <= now()):
                row.state = TurnState.CANCELLED.value
                row.error_code = "cancelled"
                row.error_detail = "Queued Turn was cancelled"
                await self._finish_turn_records(db, turn_id)
        return self.turn_view(row)

    @observed("db.turn.state")
    async def set_turn_state(self, turn_id: str, state: TurnState, *, sandbox_id: str | None = None, error_code: str = "", error_detail: str = "", lease_owner: str | None = None, recovery_generation: int | None = None) -> TurnView:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row:
                raise KeyError("turn_not_found")
            if lease_owner is not None and (
                row.lease_owner != lease_owner or row.recovery_generation != recovery_generation
            ):
                raise RuntimeError("stale_turn_lease")
            if (row.cancel_requested and error_code != "cleanup_failed"
                    and state in {TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED}):
                state, error_code, error_detail = TurnState.CANCELLED, "cancelled", "Turn was cancelled"
            row.state = state.value
            if sandbox_id is not None:
                row.sandbox_id = sandbox_id
            row.error_code = error_code
            row.error_detail = error_detail[:4000]
            if state in TERMINAL:
                row.pending_final_state = ""
                await self._finish_turn_records(db, turn_id)
            row.updated_at = now()
        return self.turn_view(row)

    @observed("db.turn.cleanup")
    async def begin_turn_cleanup(self, turn_id: str, final_state: TurnState, *, error_code: str,
                                 error_detail: str, owner: str, generation: int) -> TurnView:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                raise RuntimeError("stale_turn_lease")
            if row.cancel_requested:
                final_state, error_code, error_detail = TurnState.CANCELLED, "cancelled", "Turn was cancelled"
            row.state = TurnState.CLEANING.value
            row.pending_final_state = final_state.value
            row.error_code = error_code
            row.error_detail = error_detail[:4000]
            row.updated_at = now()
        return self.turn_view(row)

    @observed("db.checkpoint.commit")
    async def update_thread_checkpoint(self, thread_id: str, checkpoint_ref: str, native_thread_id: str,
                                       *, turn_id: str | None = None, lease_owner: str | None = None,
                                       recovery_generation: int | None = None) -> None:
        async with self.connections.begin() as db:
            if lease_owner is not None:
                fence = (await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                    TurnRow.thread_id == thread_id, TurnRow.lease_owner == lease_owner,
                    TurnRow.recovery_generation == recovery_generation, TurnRow.lease_expires_at > now())
                    .values(updated_at=TurnRow.updated_at).returning(TurnRow.id))).scalar_one_or_none()
                if fence is None:
                    raise RuntimeError("stale_turn_lease")
            row = await db.get(ThreadRow, thread_id, with_for_update=True)
            if not row:
                raise KeyError("thread_not_found")
            row.checkpoint_ref = checkpoint_ref
            row.native_thread_id = native_thread_id
            row.updated_at = now()

    async def persist_runtime_event(self, thread_id: str, turn_id: str, value: RuntimeEvent,
                                    execution_id: str, *, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> Event:
        """Commit transcript mutation and replay receipt together; no crash gap."""
        async with self.connections.begin() as db:
            await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
                .values(updated_at=ThreadRow.updated_at))
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.thread_id != thread_id:
                raise KeyError("turn_not_found")
            if lease_owner is not None and (turn.lease_owner != lease_owner or
                    turn.recovery_generation != recovery_generation):
                raise RuntimeError("stale_turn_lease")
            if execution_id and value.runtime_sequence:
                previous = (await db.execute(select(EventRow).where(EventRow.turn_id == turn_id,
                    EventRow.source_execution_id == execution_id,
                    EventRow.source_sequence == value.runtime_sequence))).scalar_one_or_none()
                if previous:
                    return Event(turn_id=turn_id, sequence=previous.sequence, kind=EventKind(previous.kind),
                        item_id=previous.item_id, payload=json.loads(previous.payload_json), created_at=previous.created_at)
            payload = dict(value.payload)
            timestamp = now()
            if value.kind in {EventKind.TOOL_STARTED, EventKind.STEP_STARTED} and not payload.get("model_request_id"):
                await db.execute(update(ItemRow).where(ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                    .values(status="completed", updated_at=timestamp))
            item = None
            if value.kind != EventKind.TOOL_OUTPUT:
                item_type = {EventKind.TEXT_DELTA: ItemType.AGENT_MESSAGE,
                    EventKind.TOOL_STARTED: ItemType.TOOL, EventKind.TOOL_FINISHED: ItemType.TOOL,
                    EventKind.STEP_STARTED: ItemType.PLAN, EventKind.STEP_PROGRESS: ItemType.PLAN,
                    EventKind.STEP_FINISHED: ItemType.PLAN, EventKind.WARNING: ItemType.WARNING,
                    EventKind.ERROR: ItemType.ERROR, EventKind.WAITING_USER: ItemType.APPROVAL,
                    EventKind.ARTIFACT_CREATED: ItemType.ARTIFACT}.get(value.kind, ItemType.REASONING)
                status = "failed" if payload.get("is_error") or value.kind == EventKind.ERROR else (
                    "completed" if value.kind in {EventKind.TOOL_FINISHED, EventKind.STEP_FINISHED, EventKind.WARNING} else "in_progress")
                summary = str(payload.get("name") or payload.get("text") or value.kind.value)
                if value.kind == EventKind.TEXT_DELTA:
                    item = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                        .order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
                    if item:
                        item.summary += str(payload.get("text", "")); item.updated_at = timestamp
                elif value.kind == EventKind.TOOL_FINISHED and payload.get("tool_use_id"):
                    candidates = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.TOOL.value).order_by(ItemRow.sequence.desc()))).scalars().all()
                    item = next((row for row in candidates if
                        json.loads(row.payload_json).get("tool_use_id") == payload["tool_use_id"] and
                        json.loads(row.payload_json).get("_execution_id", execution_id) == execution_id), None)
                    if item:
                        item.status = status
                        if payload.get("name") not in (None, "", "tool"):
                            item.summary = summary
                        item.payload_json = json.dumps({**json.loads(item.payload_json), **payload, "name": item.summary}, ensure_ascii=False)
                        item.updated_at = timestamp
                elif value.kind == EventKind.STEP_FINISHED and payload.get("background_job_id"):
                    candidates = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.PLAN.value).order_by(ItemRow.sequence.desc()))).scalars().all()
                    item = next((row for row in candidates if
                        json.loads(row.payload_json).get("background_job_id") == payload["background_job_id"] and
                        json.loads(row.payload_json).get("_execution_id", execution_id) == execution_id), None)
                    if item:
                        item.status = status
                        item.summary = summary
                        item.payload_json = json.dumps({**json.loads(item.payload_json), **payload}, ensure_ascii=False)
                        item.updated_at = timestamp
                elif value.kind == EventKind.WAITING_USER:
                    native_id = str(payload.get("interaction_id") or "")
                    kind = payload.get("interaction_kind", "approval")
                    if not native_id or kind not in {"approval", "question"}:
                        raise ValueError("Invalid Runtime interaction")
                    interaction = (await db.execute(select(InteractionRow).where(InteractionRow.turn_id == turn_id,
                        InteractionRow.runtime_interaction_id == native_id))).scalar_one_or_none()
                    if interaction is None:
                        interaction = InteractionRow(id=identifier(), thread_id=thread_id, turn_id=turn_id,
                            runtime_execution_id=execution_id, runtime_interaction_id=native_id, kind=kind,
                            status="pending", request_json=json.dumps(payload), response_json="{}", response_key="",
                            created_at=timestamp, updated_at=timestamp)
                        db.add(interaction)
                    elif interaction.runtime_execution_id != execution_id:
                        raise RuntimeError("interaction_execution_mismatch")
                    payload["interaction_id"] = interaction.id
                    summary = str(payload.get("prompt") or "需要用户确认")
                    approvals = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.APPROVAL.value))).scalars().all()
                    item = next((row for row in approvals if json.loads(row.payload_json).get("interaction_id") == interaction.id), None)
                    if interaction.status == "pending":
                        turn.state = TurnState.AWAITING_USER.value
                if item is None:
                    sequence = await self.next_item_sequence(db, thread_id)
                    item = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=sequence,
                        type=item_type.value, status=status, summary=summary,
                        payload_json=json.dumps({**payload, "_execution_id": execution_id}, ensure_ascii=False),
                        created_at=timestamp, updated_at=timestamp)
                    db.add(item)
            if payload.get("input_completed"):
                await db.execute(update(ItemRow).where(ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                    .values(status="interrupted" if payload.get("is_error") else "completed", updated_at=timestamp))
            last = (await db.execute(select(func.max(EventRow.sequence)).where(EventRow.turn_id == turn_id))).scalar() or 0
            event = Event(turn_id=turn_id, sequence=last + 1, kind=value.kind, payload=payload,
                item_id=item.id if item else "", created_at=timestamp)
            db.add(EventRow(turn_id=turn_id, sequence=event.sequence, kind=value.kind.value,
                item_id=event.item_id, payload_json=json.dumps(payload, ensure_ascii=False),
                source_execution_id=execution_id, source_sequence=value.runtime_sequence, created_at=timestamp))
            if value.runtime_sequence:
                turn.runtime_sequence = max(turn.runtime_sequence, value.runtime_sequence)
        if self.events:
            await self.events.publish(event)
        return event

    async def update_model_activity(self, item_id: str, *, phase: str, status: str, payload: dict) -> None:
        async with self.connections.begin() as db:
            row = await db.get(ItemRow, item_id, with_for_update=True)
            if not row or row.status not in {"in_progress", "interrupted"}:
                return
            model = payload["model"]
            row.summary = {"streaming": f"{model} 正在输出", "failed": f"{model} 请求失败",
                           "completed": f"{model} 响应完成"}.get(phase, f"等待 {model} 响应")
            row.payload_json = json.dumps({**json.loads(row.payload_json), **payload, "phase": phase}, ensure_ascii=False)
            row.status = status; row.updated_at = now()
            turn_id = row.turn_id
        await self.append_event(turn_id, EventKind.STEP_PROGRESS if status == "in_progress" else EventKind.STEP_FINISHED,
            {"model_request_id": payload["model_request_id"], "phase": phase}, item_id=item_id)

    async def runtime_event_recorded(self, turn_id: str, execution_id: str, source_sequence: int) -> bool:
        if not execution_id or source_sequence <= 0:
            return False
        async with self.connections() as db:
            row = (await db.execute(select(EventRow.sequence).where(
                EventRow.turn_id == turn_id,
                EventRow.source_execution_id == execution_id,
                EventRow.source_sequence == source_sequence,
            ).limit(1))).scalar_one_or_none()
        return row is not None

    async def append_event(self, turn_id: str, kind: EventKind, payload: dict, *, item_id: str = "",
                           source_execution_id: str = "", source_sequence: int = 0,
                           lease_owner: str | None = None, recovery_generation: int | None = None) -> Event:
        async with self.event_lock(turn_id):
            return await self._append_event_locked(turn_id, kind, payload, item_id=item_id,
                source_execution_id=source_execution_id, source_sequence=source_sequence,
                lease_owner=lease_owner, recovery_generation=recovery_generation)

    async def _append_event_locked(self, turn_id: str, kind: EventKind, payload: dict, *, item_id: str,
                                   source_execution_id: str, source_sequence: int,
                                   lease_owner: str | None, recovery_generation: int | None) -> Event:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if (lease_owner is not None and (turn.lease_owner != lease_owner
                    or turn.recovery_generation != recovery_generation)):
                raise RuntimeError("stale_turn_lease")
            if source_execution_id and source_sequence:
                existing = (await db.execute(select(EventRow).where(
                    EventRow.turn_id == turn_id,
                    EventRow.source_execution_id == source_execution_id,
                    EventRow.source_sequence == source_sequence,
                ).limit(1))).scalar_one_or_none()
                if existing:
                    return Event(turn_id=existing.turn_id, sequence=existing.sequence,
                        kind=EventKind(existing.kind), item_id=existing.item_id,
                        payload=json.loads(existing.payload_json), created_at=existing.created_at)
            last = (await db.execute(select(EventRow.sequence).where(EventRow.turn_id == turn_id).order_by(EventRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            event = Event(turn_id=turn_id, sequence=last + 1, kind=kind, item_id=item_id, payload=payload)
            db.add(EventRow(turn_id=turn_id, sequence=event.sequence, kind=kind.value, item_id=item_id,
                payload_json=json.dumps(payload, ensure_ascii=False), source_execution_id=source_execution_id,
                source_sequence=source_sequence, created_at=event.created_at))
        if self.events:
            await self.events.publish(event)
        return event

    async def events_after(self, turn_id: str, sequence: int, limit: int | None = None) -> list[Event]:
        async with self.connections() as db:
            query = select(EventRow).where(EventRow.turn_id == turn_id,
                EventRow.sequence > sequence).order_by(EventRow.sequence)
            if limit is not None:
                query = query.limit(limit)
            rows = (await db.execute(query)).scalars().all()
        return [Event(turn_id=row.turn_id, sequence=row.sequence, kind=EventKind(row.kind), item_id=row.item_id, payload=json.loads(row.payload_json), created_at=row.created_at) for row in rows]

    async def append_item(self, thread_id: str, turn_id: str, item_type: ItemType, *, status: str,
                          summary: str = "", payload: dict | None = None,
                          lease_owner: str | None = None, recovery_generation: int | None = None) -> Item:
        timestamp = now()
        async with self.connections.begin() as db:
            await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
                .values(updated_at=ThreadRow.updated_at))
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if (lease_owner is not None and (turn.lease_owner != lease_owner
                    or turn.recovery_generation != recovery_generation)):
                raise RuntimeError("stale_turn_lease")
            sequence = await self.next_item_sequence(db, thread_id)
            row = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=sequence,
                type=item_type.value, status=status, summary=summary, payload_json=json.dumps(payload or {}, ensure_ascii=False),
                created_at=timestamp, updated_at=timestamp)
            db.add(row)
        return Item(id=row.id, thread_id=thread_id, turn_id=turn_id, sequence=row.sequence, type=item_type,
            status=status, summary=summary, payload=payload or {}, created_at=timestamp, updated_at=timestamp)

    async def append_agent_delta(self, thread_id: str, turn_id: str, text: str) -> Item:
        async with self.connections.begin() as db:
            await db.get(ThreadRow, thread_id, with_for_update=True)
            row = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                .order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
            if not row:
                sequence = await self.next_item_sequence(db, thread_id)
                row = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=sequence,
                    type=ItemType.AGENT_MESSAGE.value, status="in_progress", summary="", payload_json="{}", created_at=now(), updated_at=now())
                db.add(row)
            row.summary += text
            row.updated_at = now()
        return Item(id=row.id, thread_id=thread_id, turn_id=turn_id, sequence=row.sequence, type=ItemType.AGENT_MESSAGE,
            status=row.status, summary=row.summary, payload=json.loads(row.payload_json), created_at=row.created_at, updated_at=row.updated_at)

    async def complete_agent_item(self, turn_id: str, status: str, *, lease_owner: str | None = None,
                                  recovery_generation: int | None = None) -> None:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if (lease_owner is not None and (turn.lease_owner != lease_owner
                    or turn.recovery_generation != recovery_generation)):
                raise RuntimeError("stale_turn_lease")
            row = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id, ItemRow.type == ItemType.AGENT_MESSAGE.value).order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
            if row:
                row.status = status
                row.updated_at = now()

    async def finish_tool_item(self, turn_id: str, tool_use_id: str, payload: dict) -> Item | None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(ItemRow).where(
                ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.TOOL.value,
            ).order_by(ItemRow.sequence.desc()))).scalars().all()
            row = next((candidate for candidate in rows
                        if json.loads(candidate.payload_json).get("tool_use_id") == tool_use_id), None)
            if not row:
                return None
            row.status = "failed" if payload.get("is_error") else "completed"
            name = str(payload.get("name") or "")
            row.summary = row.summary if name in {"", "tool"} else name
            previous = json.loads(row.payload_json)
            row.payload_json = json.dumps({**previous, **payload, "name": row.summary}, ensure_ascii=False)
            row.updated_at = now()
        return Item(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id, sequence=row.sequence,
            type=ItemType.TOOL, status=row.status, summary=row.summary, payload=json.loads(row.payload_json),
            created_at=row.created_at, updated_at=row.updated_at)

    async def items_after(self, thread_id: str, sequence: int = 0, limit: int = 200, owner_id: str | None = None) -> list[Item]:
        async with self.connections() as db:
            query = select(ItemRow).join(ThreadRow, ThreadRow.id == ItemRow.thread_id).where(
                ItemRow.thread_id == thread_id, ItemRow.sequence > sequence)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(ItemRow.sequence).limit(limit))).scalars().all()
        return [Item(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id, sequence=row.sequence,
            type=ItemType(row.type), status=row.status, summary=row.summary, payload=json.loads(row.payload_json),
            created_at=row.created_at, updated_at=row.updated_at) for row in rows]

    @staticmethod
    async def require_lease(db, turn_id: str, owner: str, generation: int) -> None:
        owned = (await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
            TurnRow.lease_owner == owner, TurnRow.recovery_generation == generation,
            TurnRow.lease_expires_at > now()).values(updated_at=TurnRow.updated_at)
            .returning(TurnRow.id))).scalar_one_or_none()
        if owned is None:
            raise RuntimeError("stale_turn_lease")

    @observed("db.artifact.commit")
    async def add_artifact(self, turn_id: str, *, path: str, sha256: str, size: int, mime: str, object_ref: str,
                           lease_owner: str | None = None, recovery_generation: int | None = None) -> Artifact:
        value = Artifact(id=identifier(), turn_id=turn_id, path=path, sha256=sha256, size=size, mime=mime, object_ref=object_ref, created_at=now())
        async with self.connections.begin() as db:
            if lease_owner is not None:
                await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            db.add(ArtifactRow(**value.model_dump()))
        return value

    @observed("db.artifact.publish")
    async def publish_artifact(self, thread_id: str, turn_id: str, *, path: str, sha256: str,
                               size: int, mime: str, object_ref: str, partial: bool,
                               lease_owner: str, recovery_generation: int) -> tuple[Artifact, ProjectFile, bool]:
        async with self.connections.begin() as db:
            await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.thread_id != thread_id:
                raise KeyError("turn_not_found")
            artifact_row = (await db.execute(select(ArtifactRow).join(TurnRow,
                TurnRow.id==ArtifactRow.turn_id).where(TurnRow.thread_id==thread_id,
                ArtifactRow.path == path,ArtifactRow.sha256 == sha256).order_by(
                ArtifactRow.created_at,ArtifactRow.id).limit(1).with_for_update())).scalar_one_or_none()
            created = artifact_row is None
            if artifact_row is None:
                artifact_row = ArtifactRow(id=identifier(), turn_id=turn_id, path=path,
                    sha256=sha256, size=size, mime=mime, object_ref=object_ref, created_at=now())
                db.add(artifact_row)
            project_row, _ = await self._register_project_file_locked(db, thread_id,
                path="artifacts/" + path, source_type="artifact", source_turn_id=turn_id,
                sha256=sha256, size=size, mime=artifact_row.mime, object_ref=object_ref)
            artifact_items = (await db.execute(select(ItemRow).where(
                ItemRow.thread_id == thread_id, ItemRow.type == ItemType.ARTIFACT.value
            ).with_for_update())).scalars().all()
            item = next((row for row in artifact_items
                if json.loads(row.payload_json).get("artifact_id") == artifact_row.id), None)
            if item is None:
                timestamp = now(); sequence = await self.next_item_sequence(db, thread_id)
                db.add(ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id,
                    sequence=sequence, type=ItemType.ARTIFACT.value, status="completed",
                    summary=path, payload_json=json.dumps({"artifact_id":artifact_row.id,
                        "mime":artifact_row.mime,"size":artifact_row.size,"partial":partial}),
                    created_at=timestamp, updated_at=timestamp))
        artifact = Artifact.model_validate({key:getattr(artifact_row,key) for key in Artifact.model_fields})
        return artifact, self.project_file_view(project_row), created

    async def artifact_by_identity(self, turn_id: str, path: str, sha256: str) -> Artifact | None:
        async with self.connections() as db:
            row = (await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == turn_id, ArtifactRow.path == path, ArtifactRow.sha256 == sha256,
            ).limit(1))).scalar_one_or_none()
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None

    async def artifacts(self, turn_id: str) -> list[Artifact]:
        async with self.connections() as db:
            rows = (await db.execute(select(ArtifactRow).where(ArtifactRow.turn_id == turn_id,
                ArtifactRow.path.not_like("input/%")).order_by(ArtifactRow.created_at))).scalars().all()
        return [Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) for row in rows]

    @staticmethod
    def project_file_view(row: ProjectFileRow) -> ProjectFile:
        return ProjectFile.model_validate({key: getattr(row, key) for key in ProjectFile.model_fields})

    @staticmethod
    def turn_project_file_view(row: TurnProjectFileRow) -> TurnProjectFileSnapshot:
        return TurnProjectFileSnapshot.model_validate({
            key: getattr(row, key) for key in TurnProjectFileSnapshot.model_fields})

    @staticmethod
    async def _register_project_file_locked(db: AsyncSession, thread_id: str, *, path: str,
                                            source_type: str, source_turn_id: str, sha256: str,
                                            size: int, mime: str, object_ref: str) -> tuple[ProjectFileRow, bool]:
        await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id).values(updated_at=ThreadRow.updated_at))
        latest = (await db.execute(select(ProjectFileRow).where(
            ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
        ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
        if latest and latest.sha256 == sha256 and not latest.deleted:
            return latest, False
        row = ProjectFileRow(id=identifier(), thread_id=thread_id, path=path,
            version=(latest.version + 1 if latest else 1), source_type=source_type,
            source_turn_id=source_turn_id, sha256=sha256, size=size, mime=mime,
            object_ref=object_ref, created_at=now())
        db.add(row)
        return row, True

    async def register_project_file(self, thread_id: str, *, path: str, source_type: str,
                                    source_turn_id: str, sha256: str, size: int, mime: str,
                                    object_ref: str, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> tuple[ProjectFile, bool]:
        if not valid_project_file_path(path, source_type):
            raise ValueError("Invalid managed project path")
        async with self.connections.begin() as db:
            if lease_owner is not None:
                await self.require_lease(db, source_turn_id, lease_owner, recovery_generation)
            row, created = await self._register_project_file_locked(db, thread_id, path=path,
                source_type=source_type, source_turn_id=source_turn_id, sha256=sha256,
                size=size, mime=mime, object_ref=object_ref)
        return self.project_file_view(row), created

    async def current_project_files(self, thread_id: str, owner_id: str | None = None) -> list[ProjectFile]:
        async with self.connections() as db:
            query = select(ProjectFileRow).join(ThreadRow, ThreadRow.id == ProjectFileRow.thread_id).where(
                ProjectFileRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(ProjectFileRow.path, ProjectFileRow.version.desc()))).scalars().all()
        current = {}
        for row in rows:
            current.setdefault(row.path, row)
        return [self.project_file_view(current[path]) for path in sorted(current)
            if not current[path].deleted and valid_project_file_path(current[path].path, current[path].source_type)]

    async def freeze_turn_project_files(self, turn_id: str, thread_id: str, *,
                                        lease_owner: str, recovery_generation: int) -> list[TurnProjectFileSnapshot]:
        async with self.connections.begin() as db:
            await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.thread_id != thread_id:
                raise KeyError("turn_not_found")
            existing = (await db.execute(select(TurnProjectFileRow).where(
                TurnProjectFileRow.turn_id == turn_id).order_by(TurnProjectFileRow.path))).scalars().all()
            if turn.project_files_frozen:
                return [self.turn_project_file_view(row) for row in existing]
            rows = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == thread_id).order_by(
                ProjectFileRow.path, ProjectFileRow.version.desc()).with_for_update())).scalars().all()
            current: dict[str, ProjectFileRow] = {}
            for row in rows:
                current.setdefault(row.path, row)
            timestamp = now()
            snapshots = []
            for path in sorted(current):
                source = current[path]
                if source.deleted or not valid_project_file_path(source.path, source.source_type):
                    continue
                snapshot = TurnProjectFileRow(id=identifier(), turn_id=turn_id,
                    project_file_id=source.id, path=source.path, version=source.version,
                    source_type=source.source_type, source_turn_id=source.source_turn_id,
                    sha256=source.sha256, size=source.size, mime=source.mime,
                    object_ref=source.object_ref, created_at=timestamp)
                db.add(snapshot); snapshots.append(snapshot)
                db.add(ArtifactRow(id=identifier(),turn_id=turn_id,path="input/"+source.path,
                    sha256=source.sha256,size=source.size,mime=source.mime,
                    object_ref=source.object_ref,created_at=timestamp))
            turn.project_files_frozen = True
        return [self.turn_project_file_view(row) for row in snapshots]

    async def turn_project_files(self, turn_id: str, owner_id: str | None = None) -> list[TurnProjectFileSnapshot]:
        async with self.connections() as db:
            query = select(TurnProjectFileRow).join(TurnRow,
                TurnRow.id == TurnProjectFileRow.turn_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(TurnProjectFileRow.turn_id == turn_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(TurnProjectFileRow.path))).scalars().all()
        return [self.turn_project_file_view(row) for row in rows]

    async def turn_input_artifacts(self,turn_id:str,owner_id:str|None=None)->list[dict]:
        async with self.connections() as db:
            query=select(TurnProjectFileRow).join(TurnRow,
                TurnRow.id==TurnProjectFileRow.turn_id).join(ThreadRow,
                ThreadRow.id==TurnRow.thread_id).where(TurnProjectFileRow.turn_id==turn_id)
            if owner_id is not None:query=query.where(ThreadRow.owner_id==owner_id)
            snapshots=(await db.execute(query.order_by(TurnProjectFileRow.path))).scalars().all()
            artifacts=(await db.execute(select(ArtifactRow).where(ArtifactRow.turn_id==turn_id,
                ArtifactRow.path.like("input/%")))).scalars().all()
        by_path={row.path.removeprefix("input/"):row for row in artifacts}
        return [{"artifact":Artifact.model_validate({key:getattr(by_path[row.path],key)
                    for key in Artifact.model_fields}),"project_file_id":row.project_file_id,
                "project_path":row.path,"source_type":row.source_type}
            for row in snapshots if row.path in by_path]

    async def project_file_history(self, thread_id: str, path: str, owner_id: str) -> list[ProjectFile]:
        async with self.connections() as db:
            rows = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                    ThreadRow.owner_id == owner_id,
                ).order_by(ProjectFileRow.version.desc()))).scalars().all()
        return [self.project_file_view(row) for row in rows]

    async def restore_project_file_version(self, file_id: str, owner_id: str) -> tuple[ProjectFile, bool]:
        async with self.connections.begin() as db:
            source = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not source:
                raise KeyError("project_file_not_found")
            latest = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id,
                ProjectFileRow.path == source.path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one()
            if not latest.deleted and latest.sha256 == source.sha256:
                return self.project_file_view(latest), False
            row = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=source.path,
                version=latest.version + 1, source_type=source.source_type,
                source_turn_id=source.source_turn_id, sha256=source.sha256, size=source.size,
                mime=source.mime, object_ref=source.object_ref, deleted=False, created_at=now())
            db.add(row)
        return self.project_file_view(row), True

    async def apply_managed_file_versions(self, thread_id: str, owner_id: str,
                                          changes: list[dict]) -> list[ProjectFile]:
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id).with_for_update()
            )).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            latest_by_path = {}
            for change in changes:
                path = change["path"]
                latest = (await db.execute(select(ProjectFileRow).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
                expected = change.get("expected_version")
                if expected is None:
                    if latest and not latest.deleted:
                        raise RuntimeError("managed_project_version_conflict")
                elif not latest or latest.deleted or latest.version != expected:
                    raise RuntimeError("managed_project_version_conflict")
                latest_by_path[path] = latest
            result = []
            for change in changes:
                latest = latest_by_path[change["path"]]
                if change["kind"] == "missing":
                    if latest:
                        row = ProjectFileRow(id=identifier(), thread_id=thread_id,
                            path=latest.path, version=latest.version + 1,
                            source_type=latest.source_type, source_turn_id=latest.source_turn_id,
                            sha256=latest.sha256, size=latest.size, mime=latest.mime,
                            object_ref=latest.object_ref, deleted=True, created_at=now())
                        db.add(row); result.append(row)
                    continue
                row, _ = await self._register_project_file_locked(db, thread_id,
                    path=change["path"], source_type=change["source_type"], source_turn_id="",
                    sha256=change["sha256"], size=change["size"], mime=change["mime"],
                    object_ref=change["object_ref"])
                result.append(row)
        return [self.project_file_view(row) for row in result]

    async def rename_project_file(self, file_id: str, owner_id: str, new_path: str) -> ProjectFile:
        async with self.connections.begin() as db:
            source = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not source or source.deleted:
                raise KeyError("project_file_not_found")
            latest_source = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id, ProjectFileRow.path == source.path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one()
            if latest_source.id != source.id:
                raise RuntimeError("project_file_version_stale")
            target = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id, ProjectFileRow.path == new_path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
            if target and not target.deleted:
                raise RuntimeError("project_file_path_exists")
            timestamp = now()
            tombstone = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=source.path,
                version=source.version + 1, source_type=source.source_type, source_turn_id=source.source_turn_id,
                sha256=source.sha256, size=source.size, mime=source.mime, object_ref=source.object_ref,
                deleted=True, created_at=timestamp)
            row = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=new_path,
                version=(target.version + 1 if target else 1), source_type=source.source_type,
                source_turn_id=source.source_turn_id, sha256=source.sha256, size=source.size,
                mime=source.mime, object_ref=source.object_ref, deleted=False, created_at=timestamp)
            db.add_all([tombstone, row])
        return self.project_file_view(row)

    async def delete_project_file(self, file_id: str, owner_id: str) -> ProjectFile:
        async with self.connections.begin() as db:
            source = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not source or source.deleted:
                raise KeyError("project_file_not_found")
            latest = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id, ProjectFileRow.path == source.path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one()
            if latest.id != source.id:
                raise RuntimeError("project_file_version_stale")
            row = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=source.path,
                version=source.version + 1, source_type=source.source_type,
                source_turn_id=source.source_turn_id, sha256=source.sha256, size=source.size,
                mime=source.mime, object_ref=source.object_ref, deleted=True, created_at=now())
            db.add(row)
        return self.project_file_view(row)

    async def get_project_file_for_owner(self, file_id: str, owner_id: str) -> ProjectFile | None:
        async with self.connections() as db:
            row = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ))).scalar_one_or_none()
        return self.project_file_view(row) if row else None

    async def resolve_project_file_reference(self, thread_id: str, owner_id: str, *,
                                             file_id: str = "", path: str = "") -> ProjectFile | None:
        async with self.connections() as db:
            if file_id:
                row = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                    ThreadRow.id == ProjectFileRow.thread_id).where(
                        ProjectFileRow.id == file_id, ProjectFileRow.thread_id == thread_id,
                        ThreadRow.owner_id == owner_id,
                    ))).scalar_one_or_none()
                if not row:
                    return None
                latest = (await db.execute(select(ProjectFileRow).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == row.path,
                ).order_by(ProjectFileRow.version.desc()).limit(1))).scalar_one()
                if latest.id != row.id or latest.deleted:
                    return None
            else:
                row = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                    ThreadRow.id == ProjectFileRow.thread_id).where(
                        ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                        ThreadRow.owner_id == owner_id,
                    ).order_by(ProjectFileRow.version.desc()).limit(1))).scalar_one_or_none()
                if not row or row.deleted:
                    return None
        return self.project_file_view(row)

    @staticmethod
    def interaction_view(row: InteractionRow) -> Interaction:
        return Interaction.model_validate({
            "id": row.id, "thread_id": row.thread_id, "turn_id": row.turn_id,
            "runtime_execution_id": row.runtime_execution_id,
            "runtime_interaction_id": row.runtime_interaction_id, "kind": row.kind,
            "status": row.status, "request": json.loads(row.request_json),
            "response": json.loads(row.response_json), "response_key": row.response_key,
            "created_at": row.created_at, "updated_at": row.updated_at,
        })

    async def upsert_interaction(self, thread_id: str, turn_id: str, execution_id: str,
                                 runtime_interaction_id: str, kind: str, request: dict) -> tuple[Interaction, bool]:
        timestamp = now()
        async with self.connections.begin() as db:
            row = (await db.execute(select(InteractionRow).where(
                InteractionRow.turn_id == turn_id,
                InteractionRow.runtime_interaction_id == runtime_interaction_id,
            ).with_for_update())).scalar_one_or_none()
            if row:
                return self.interaction_view(row), False
            row = InteractionRow(id=identifier(), thread_id=thread_id, turn_id=turn_id,
                runtime_execution_id=execution_id, runtime_interaction_id=runtime_interaction_id,
                kind=kind, status="pending", request_json=json.dumps(request, ensure_ascii=False),
                response_json="{}", response_key="", created_at=timestamp, updated_at=timestamp)
            db.add(row)
        return self.interaction_view(row), True

    async def list_interactions(self, thread_id: str, owner_id: str) -> list[Interaction]:
        async with self.connections() as db:
            rows = (await db.execute(select(InteractionRow).join(ThreadRow,
                ThreadRow.id == InteractionRow.thread_id).where(
                    InteractionRow.thread_id == thread_id, ThreadRow.owner_id == owner_id,
                ).order_by(InteractionRow.created_at))).scalars().all()
        return [self.interaction_view(row) for row in rows]

    @observed("db.interaction.claim")
    async def claim_interaction_response(self, interaction_id: str, owner_id: str,
                                         response_key: str, response: dict, *, claim_owner: str = "",
                                         lease_seconds: int = 30) -> tuple[Interaction, bool]:
        expired = False
        timestamp = now()
        async with self.connections.begin() as db:
            row = (await db.execute(select(InteractionRow).join(ThreadRow,
                ThreadRow.id == InteractionRow.thread_id).where(
                    InteractionRow.id == interaction_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("interaction_not_found")
            stored_response = json.loads(row.response_json)
            if row.response_key == response_key and stored_response and stored_response != response:
                raise RuntimeError("idempotency_conflict")
            if row.status == "responded" and stored_response == response:
                return self.interaction_view(row), False
            if row.status not in {"pending", "responding"}:
                raise RuntimeError("interaction_not_pending")
            turn = await db.get(TurnRow, row.turn_id, with_for_update=True)
            if (not turn or turn.runtime_execution_id != row.runtime_execution_id
                    or TurnState(turn.state) not in {TurnState.RUNNING, TurnState.AWAITING_USER}):
                row.status = "expired"; row.updated_at = timestamp
                expired = True
            else:
                if row.status == "responding":
                    if row.response_expires_at and utc_datetime(row.response_expires_at) > timestamp:
                        raise RuntimeError("interaction_response_in_progress")
                    # A lost receipt must not change the user's committed answer.
                    # A new browser key is allowed, a different answer is not.
                    if stored_response != response:
                        raise RuntimeError("interaction_response_conflict")
                row.status = "responding"; row.response_key = response_key
                row.response_json = json.dumps(response, ensure_ascii=False)
                row.response_owner = claim_owner
                row.response_expires_at = timestamp + timedelta(seconds=lease_seconds)
                row.updated_at = timestamp
        if expired:
            raise RuntimeError("interaction_expired")
        return self.interaction_view(row), True

    @observed("db.interaction.finish")
    async def finish_interaction_response(self, interaction_id: str, *, delivered: bool,
                                          claim_owner: str = "") -> Interaction:
        async with self.connections.begin() as db:
            row = await db.get(InteractionRow, interaction_id, with_for_update=True)
            if not row:
                raise KeyError("interaction_not_found")
            if row.status == "responding":
                if claim_owner and row.response_owner != claim_owner:
                    raise RuntimeError("stale_interaction_response_claim")
                row.status = "responded" if delivered else "pending"
                if not delivered:
                    row.response_key = ""; row.response_json = "{}"
                row.response_owner = ""; row.response_expires_at = None
                row.updated_at = now()
        return self.interaction_view(row)

    async def resume_interaction_turn(self, turn_id: str, execution_id: str) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                TurnRow.runtime_execution_id == execution_id,
                TurnRow.state == TurnState.AWAITING_USER.value
            ).values(state=TurnState.RUNNING.value, updated_at=now()))
            return result.rowcount == 1

    async def finish_interaction_item(self, turn_id: str, interaction_id: str, response: dict) -> None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(ItemRow).where(
                ItemRow.turn_id == turn_id, ItemRow.type == ItemType.APPROVAL.value,
            ).order_by(ItemRow.sequence.desc()))).scalars().all()
            row = next((item for item in rows
                if json.loads(item.payload_json).get("interaction_id") == interaction_id), None)
            if row:
                payload = json.loads(row.payload_json); payload["response"] = response
                row.payload_json = json.dumps(payload, ensure_ascii=False)
                row.status = "completed"; row.updated_at = now()

    async def get_artifact(self, artifact_id: str) -> Artifact | None:
        async with self.connections() as db:
            row = await db.get(ArtifactRow, artifact_id)
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None

    async def get_artifact_for_owner(self, artifact_id: str, owner_id: str) -> Artifact | None:
        async with self.connections() as db:
            row = (await db.execute(select(ArtifactRow).join(TurnRow, TurnRow.id == ArtifactRow.turn_id)
                .join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(
                    ArtifactRow.id == artifact_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None

    async def validate_job_input_artifacts(self, owner_id: str, thread_id: str,
                                           jobs: tuple[JobRequest, ...]) -> None:
        identities={item.artifact_id for job in jobs for item in job.input_artifacts}
        if not identities:return
        async with self.connections() as db:
            rows=(await db.execute(select(ArtifactRow.id).join(TurnRow,
                TurnRow.id==ArtifactRow.turn_id).join(ThreadRow,
                ThreadRow.id==TurnRow.thread_id).where(ArtifactRow.id.in_(identities),
                TurnRow.thread_id==thread_id,ThreadRow.owner_id==owner_id))).scalars().all()
        if set(rows)!=identities:raise RuntimeError('job_input_artifact_outside_scope')

    async def record_upload(self, owner_id: str, attachment) -> None:
        async with self.connections.begin() as db:
            values={"owner_id":owner_id,"object_ref":attachment.object_ref,
                "sha256":attachment.sha256,"size":attachment.size,"name":attachment.name,
                "mime":attachment.mime,"created_at":now()}
            dialect=db.bind.dialect.name
            if dialect=="postgresql":
                statement=postgresql_insert(UploadRow).values(**values).on_conflict_do_nothing(
                    index_elements=[UploadRow.owner_id,UploadRow.object_ref])
            elif dialect=="sqlite":
                statement=sqlite_insert(UploadRow).values(**values).on_conflict_do_nothing(
                    index_elements=[UploadRow.owner_id,UploadRow.object_ref])
            else:
                raise RuntimeError("upload_idempotency_requires_supported_database")
            await db.execute(statement)

    async def attachments_owned(self, owner_id: str, attachments: list) -> bool:
        if not attachments:
            return True
        async with self.connections() as db:
            for attachment in attachments:
                row = await db.get(UploadRow, (owner_id, attachment.object_ref))
                if not row or row.sha256 != attachment.sha256 or row.size != attachment.size:
                    return False
        return True

    @staticmethod
    def _media_reference_upload_view(row: MediaReferenceUploadRow) -> MediaReferenceUploadView:
        status = "expired" if row.status == "pending" and utc_datetime(row.expires_at) <= now() else row.status
        return MediaReferenceUploadView(id=row.id, thread_id=row.thread_id, name=row.name,
            kind=row.kind, mime=row.mime, size=row.size, sha256=row.sha256,
            object_key=row.object_key, public_url=row.public_url, status=status,
            project_file_id=row.project_file_id or "", expires_at=utc_datetime(row.expires_at))

    async def create_media_reference_upload(self, owner_id: str, thread_id: str, value, *,
                                            object_key: str, public_url: str,
                                            expires_at: datetime) -> MediaReferenceUploadView:
        async with self.connections.begin() as db:
            thread = await db.get(ThreadRow, thread_id)
            if not thread or thread.owner_id != owner_id:
                raise KeyError("thread_not_found")
            row = MediaReferenceUploadRow(id=identifier(), owner_id=owner_id, thread_id=thread_id,
                name=value.name, kind=value.kind, mime=value.mime, size=value.size,
                sha256=value.sha256, object_key=object_key, public_url=public_url,
                status="pending", expires_at=expires_at, created_at=now(), updated_at=now())
            db.add(row)
        return self._media_reference_upload_view(row)

    async def get_media_reference_upload(self, upload_id: str, owner_id: str) -> MediaReferenceUploadView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.id == upload_id,
                MediaReferenceUploadRow.owner_id == owner_id))).scalar_one_or_none()
        return self._media_reference_upload_view(row) if row else None

    async def finish_media_reference_upload(self, upload_id: str, owner_id: str,
                                            project_file_id: str) -> MediaReferenceUploadView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.id == upload_id,
                MediaReferenceUploadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_reference_upload_not_found")
            if row.status == "completed":
                return self._media_reference_upload_view(row)
            if row.status != "pending" or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("media_reference_upload_expired")
            row.status = "completed"; row.project_file_id = project_file_id; row.updated_at = now()
        return self._media_reference_upload_view(row)

    @staticmethod
    def _media_reference_asset(row: MediaReferenceAssetRow) -> dict:
        return {"sha256": row.sha256, "kind": row.kind, "mime": row.mime, "size": row.size,
            "object_ref": row.object_ref, "object_key": row.object_key, "public_url": row.public_url,
            "boss_status": row.boss_status, "ark_asset_id": row.ark_asset_id,
            "ark_status": row.ark_status, "ark_error": row.ark_error,
            "lease_owner": row.lease_owner, "lease_expires_at": row.lease_expires_at}

    async def ensure_media_reference_asset(self, reference: dict, *, object_key: str,
                                           public_url: str, boss_status: str = "pending") -> dict:
        async with self.connections.begin() as db:
            row = await db.get(MediaReferenceAssetRow, reference["sha256"], with_for_update=True)
            if row:
                expected = (reference["kind"], reference["mime"], reference["size"],
                    reference["object_ref"], object_key, public_url)
                actual = (row.kind, row.mime, row.size, row.object_ref, row.object_key, row.public_url)
                if actual != expected:
                    raise RuntimeError("media_reference_asset_identity_conflict")
            else:
                row = MediaReferenceAssetRow(sha256=reference["sha256"], kind=reference["kind"],
                    mime=reference["mime"], size=reference["size"], object_ref=reference["object_ref"],
                    object_key=object_key, public_url=public_url, boss_status=boss_status,
                    ark_status="pending", created_at=now(), updated_at=now())
                db.add(row)
        return self._media_reference_asset(row)

    async def get_media_reference_asset(self, digest: str) -> dict | None:
        async with self.connections() as db:
            row = await db.get(MediaReferenceAssetRow, digest)
        return self._media_reference_asset(row) if row else None

    async def claim_media_reference_asset(self, digest: str, owner: str,
                                          lease_seconds: int = 60) -> tuple[dict, bool]:
        async with self.connections.begin() as db:
            row = await db.get(MediaReferenceAssetRow, digest, with_for_update=True)
            if not row:
                raise KeyError("media_reference_asset_not_found")
            timestamp = now()
            if (row.lease_owner and row.lease_owner != owner and row.lease_expires_at
                    and utc_datetime(row.lease_expires_at) > timestamp):
                return self._media_reference_asset(row), False
            row.lease_owner = owner; row.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
            row.updated_at = timestamp
        return self._media_reference_asset(row), True

    async def update_media_reference_asset(self, digest: str, owner: str, *,
                                           boss_status: str | None = None,
                                           ark_asset_id: str | None = None,
                                           ark_status: str | None = None,
                                           ark_error: str | None = None,
                                           release: bool = False) -> dict:
        async with self.connections.begin() as db:
            row = await db.get(MediaReferenceAssetRow, digest, with_for_update=True)
            if not row or row.lease_owner != owner:
                raise RuntimeError("media_reference_asset_lease_lost")
            if boss_status is not None: row.boss_status = boss_status
            if ark_asset_id is not None: row.ark_asset_id = ark_asset_id
            if ark_status is not None: row.ark_status = ark_status
            if ark_error is not None: row.ark_error = ark_error[:200]
            row.updated_at = now()
            if release:
                row.lease_owner = ""; row.lease_expires_at = None
            else:
                row.lease_expires_at = now() + timedelta(seconds=60)
        return self._media_reference_asset(row)

    @staticmethod
    def _media_voice_clone(row: MediaVoiceCloneRow) -> dict:
        return {"id": row.id, "owner_id": row.owner_id, "sha256": row.sha256,
            "mime": row.mime, "size": row.size, "object_ref": row.object_ref,
            "voice_id": row.voice_id, "provider_file_id": row.provider_file_id,
            "revision": row.revision, "status": row.status, "error_code": row.error_code,
            "lease_owner": row.lease_owner, "lease_expires_at": row.lease_expires_at,
            "last_used_at": utc_datetime(row.last_used_at) if row.last_used_at else None}

    async def ensure_media_voice_clone(self, owner_id: str, reference: dict) -> dict:
        try:
            async with self.connections.begin() as db:
                row = (await db.execute(select(MediaVoiceCloneRow).where(
                    MediaVoiceCloneRow.owner_id == owner_id,
                    MediaVoiceCloneRow.sha256 == reference["sha256"]).with_for_update())).scalar_one_or_none()
                if row:
                    expected = (reference["mime"], reference["size"], reference["object_ref"])
                    if (row.mime, row.size, row.object_ref) != expected:
                        raise RuntimeError("media_voice_clone_identity_conflict")
                else:
                    identity = hashlib.sha256((owner_id + ":" + reference["sha256"] + ":1").encode()).hexdigest()
                    row = MediaVoiceCloneRow(id=identifier(), owner_id=owner_id,
                        sha256=reference["sha256"], mime=reference["mime"], size=reference["size"],
                        object_ref=reference["object_ref"], voice_id="updream_" + identity[:32],
                        revision=1, status="pending", created_at=now(), updated_at=now())
                    db.add(row)
        except IntegrityError:
            row = None
        if row is None:
            async with self.connections() as db:
                row = (await db.execute(select(MediaVoiceCloneRow).where(
                    MediaVoiceCloneRow.owner_id == owner_id,
                    MediaVoiceCloneRow.sha256 == reference["sha256"]))).scalar_one_or_none()
            if not row:
                raise RuntimeError("media_voice_clone_create_conflict")
            expected = (reference["mime"], reference["size"], reference["object_ref"])
            if (row.mime, row.size, row.object_ref) != expected:
                raise RuntimeError("media_voice_clone_identity_conflict")
        return self._media_voice_clone(row)

    async def get_media_voice_clone(self, owner_id: str, digest: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest))).scalar_one_or_none()
        return self._media_voice_clone(row) if row else None

    async def claim_media_voice_clone(self, owner_id: str, digest: str, lease_owner: str,
                                      lease_seconds: int = 120) -> tuple[dict, bool]:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_voice_clone_not_found")
            timestamp = now()
            if (row.lease_owner and row.lease_owner != lease_owner and row.lease_expires_at
                    and utc_datetime(row.lease_expires_at) > timestamp):
                return self._media_voice_clone(row), False
            row.lease_owner = lease_owner
            row.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
            row.updated_at = timestamp
        return self._media_voice_clone(row), True

    async def update_media_voice_clone(self, owner_id: str, digest: str, lease_owner: str, *,
                                       status: str | None = None,
                                       provider_file_id: str | None = None,
                                       error_code: str | None = None,
                                       release: bool = False) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest).with_for_update())).scalar_one_or_none()
            if not row or row.lease_owner != lease_owner:
                raise RuntimeError("media_voice_clone_lease_lost")
            if status is not None: row.status = status
            if provider_file_id is not None: row.provider_file_id = provider_file_id
            if error_code is not None: row.error_code = error_code[:100]
            row.updated_at = now()
            if release:
                row.lease_owner = ""; row.lease_expires_at = None
            else:
                row.lease_expires_at = now() + timedelta(seconds=120)
        return self._media_voice_clone(row)

    async def touch_media_voice_clone(self, owner_id: str, digest: str, voice_id: str) -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id, MediaVoiceCloneRow.sha256 == digest,
                MediaVoiceCloneRow.voice_id == voice_id).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.status = "active"; row.error_code = ""; row.last_used_at = now(); row.updated_at = now()
            return True

    async def rotate_media_voice_clone(self, owner_id: str, digest: str,
                                       lease_owner: str) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest).with_for_update())).scalar_one_or_none()
            if not row or row.lease_owner != lease_owner:
                raise RuntimeError("media_voice_clone_lease_lost")
            row.revision += 1
            identity = hashlib.sha256((owner_id + ":" + digest + ":" + str(row.revision)).encode()).hexdigest()
            row.voice_id = "updream_" + identity[:32]
            row.provider_file_id = ""; row.status = "pending"; row.error_code = ""
            row.last_used_at = None; row.updated_at = now()
            row.lease_expires_at = now() + timedelta(seconds=120)
        return self._media_voice_clone(row)

    async def invalidate_media_voice_clone(self, owner_id: str, digest: str, voice_id: str) -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id, MediaVoiceCloneRow.sha256 == digest,
                MediaVoiceCloneRow.voice_id == voice_id).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.status = "pending"; row.provider_file_id = ""; row.error_code = "voice_not_available"
            row.last_used_at = None; row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
            return True

    @staticmethod
    def _media_draft_view(row: MediaDraftRow) -> MediaDraftView:
        status = "expired" if row.status == "awaiting_confirmation" and utc_datetime(row.expires_at) <= now() else row.status
        return MediaDraftView(id=row.id, thread_id=row.thread_id, kind=row.kind, model=row.model,
            generation_mode=row.generation_mode or {"image":"text_to_image","video":"text_to_video",
                "audio":"text_to_audio", "model3d":"text_to_model"}[row.kind],
            parameters=json.loads(row.parameters_json or "{}"), references=json.loads(row.references_json or "[]"), status=status,
            expires_at=utc_datetime(row.expires_at), job_id=row.job_id or "", output_role=row.output_role)

    @staticmethod
    def _media_job_view(row: MediaJobRow, artifact_ids: list[str] | None = None,
                        outputs: list[MediaJobOutputView] | None = None) -> MediaJobView:
        reason = "" if row.cancellation_mode == "supported" and row.execution_mode == "async" else "provider_not_cancelable"
        try:
            credits = float(row.credits_consumed) if row.credits_consumed else None
        except ValueError:
            credits = None
        return MediaJobView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id or "", kind=row.kind,
            model=row.model, generation_mode=row.generation_mode or "", references=json.loads(row.references_json or "[]"),
            provider=row.provider, adapter=row.adapter, execution_mode=row.execution_mode,
            cancellation_mode=row.cancellation_mode, status=row.status, phase=row.phase or "",
            progress=row.progress, error_code=row.error_code or "", error_detail=row.error_detail or "",
            error_guidance=media_error_guidance(row.error_code) if row.error_code else "",
            cancelable=row.execution_mode == "async" and row.cancellation_mode == "supported",
            cancel_reason=reason, artifact_ids=artifact_ids or [], outputs=outputs or [],
            credits_consumed=credits, output_role=row.output_role,
            created_at=row.created_at, finished_at=row.finished_at)

    @staticmethod
    def _media_project_path(job_id: str, mime: str) -> str:
        extension = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp",
            "video/mp4": "mp4", "video/webm": "webm", "audio/mpeg": "mp3",
            "audio/wav": "wav", "audio/flac": "flac", "audio/mp4": "m4a",
            "audio/webm": "webm", "model/gltf-binary": "glb"}.get(mime, "bin")
        return f"artifacts/media/{job_id}.{extension}"

    @staticmethod
    async def _media_job_outputs(db: AsyncSession, job_id: str) -> list[MediaJobOutputView]:
        rows = (await db.execute(select(MediaJobOutputRow).where(
            MediaJobOutputRow.job_id == job_id,
        ).order_by(MediaJobOutputRow.created_at, MediaJobOutputRow.id))).scalars().all()
        return [MediaJobOutputView(id=row.id, role=row.role, filename=row.filename, mime=row.mime,
            size=row.size, artifact_id=row.artifact_id, project_file_id=row.project_file_id) for row in rows]

    @staticmethod
    async def _media_artifact_ids(db: AsyncSession, row: MediaJobRow) -> list[str]:
        output_ids = list((await db.execute(select(MediaJobOutputRow.artifact_id).where(
            MediaJobOutputRow.job_id == row.id, MediaJobOutputRow.artifact_id != "",
        ).order_by(MediaJobOutputRow.created_at))).scalars().all())
        if output_ids:
            return output_ids
        if not row.turn_id:
            return []
        return list((await db.execute(select(ArtifactRow.id).where(
            ArtifactRow.turn_id == row.turn_id,
            ArtifactRow.path == f"artifacts/{row.id}",
        ).order_by(ArtifactRow.created_at))).scalars().all())

    async def create_media_draft(self, owner_id: str, thread_id: str, value, *, expires_at: datetime,
                                 provider_spec, reference_snapshots: list[dict] | None = None) -> tuple[MediaDraftView, bool]:
        reference_snapshots = reference_snapshots or []
        async with self.connections.begin() as db:
            turn = None
            if value.turn_id:
                turn = await db.get(TurnRow, value.turn_id, with_for_update=True)
                thread = await db.get(ThreadRow, thread_id)
                if not turn or not thread or turn.thread_id != thread_id or thread.owner_id != owner_id:
                    raise KeyError("media_turn_not_found")
            existing = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.thread_id == thread_id, MediaDraftRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                if (existing.owner_id != owner_id or existing.kind != value.kind or existing.model != value.model
                        or json.loads(existing.parameters_json or "{}") != value.parameters
                        or (existing.generation_mode or "") != (value.generation_mode or "")
                        or json.loads(existing.references_json or "[]") != reference_snapshots
                        or existing.output_role != value.output_role):
                    raise RuntimeError("media_draft_idempotency_conflict")
                return self._media_draft_view(existing), False
            if turn and TurnState(turn.state) in TERMINAL:
                raise RuntimeError("media_turn_terminal")
            if turn:
                batch = await db.get(MediaBatchRow, turn.id, with_for_update=True)
                if batch and batch.status != "open":
                    raise RuntimeError("media_batch_sealed")
                if not batch:
                    db.add(MediaBatchRow(turn_id=turn.id, thread_id=thread_id, owner_id=owner_id,
                        status="open", created_at=now(), updated_at=now()))
            row = MediaDraftRow(id=identifier(), owner_id=owner_id, thread_id=thread_id, turn_id=value.turn_id,
                kind=value.kind, model=value.model, generation_mode=value.generation_mode or "",
                parameters_json=json.dumps(value.parameters, ensure_ascii=False),
                references_json=json.dumps(reference_snapshots, ensure_ascii=False),
                idempotency_key=value.idempotency_key, status="awaiting_confirmation", expires_at=expires_at,
                output_role=value.output_role, created_at=now(), updated_at=now())
            db.add(row)
        return self._media_draft_view(row), True

    async def seal_media_batch(self, turn_id: str, thread_id: str, owner_id: str) -> dict:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            thread = await db.get(ThreadRow, thread_id)
            if not turn or not thread or turn.thread_id != thread_id or thread.owner_id != owner_id:
                raise KeyError("media_turn_not_found")
            batch = await db.get(MediaBatchRow, turn_id, with_for_update=True)
            if not batch:
                raise RuntimeError("media_batch_empty")
            count = (await db.execute(select(func.count()).select_from(MediaDraftRow).where(
                MediaDraftRow.turn_id == turn_id,
            ))).scalar_one()
            if not count:
                raise RuntimeError("media_batch_empty")
            if batch.status == "open":
                batch.status = "sealed"; batch.sealed_at = now(); batch.updated_at = now()
            elif batch.status not in {"sealed", "delivering", "continued"}:
                raise RuntimeError("media_batch_not_sealable")
            return {"turn_id": turn_id, "status": batch.status, "draft_count": count}

    async def get_media_draft(self, draft_id: str, owner_id: str) -> MediaDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return self._media_draft_view(row) if row else None

    async def get_media_draft_snapshot(self, draft_id: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id))).scalar_one_or_none()
        if not row:
            return None
        return {"kind": row.kind, "model": row.model,
            "generation_mode": row.generation_mode or {"image":"text_to_image","video":"text_to_video",
                "audio":"text_to_audio", "model3d":"text_to_model"}[row.kind],
            "parameters": json.loads(row.parameters_json or "{}"),
            "references": json.loads(row.references_json or "[]")}

    async def list_media_drafts(self, thread_id: str, owner_id: str) -> list[MediaDraftView]:
        async with self.connections() as db:
            rows = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.thread_id == thread_id, MediaDraftRow.owner_id == owner_id,
            ).order_by(MediaDraftRow.created_at.desc()))).scalars().all()
        return [self._media_draft_view(row) for row in rows]

    async def owner_id_for_thread(self, thread_id: str) -> str:
        async with self.connections() as db:
            return (await db.execute(select(ThreadRow.owner_id).where(ThreadRow.id == thread_id))).scalar_one_or_none() or ""

    async def update_media_draft(self, draft_id: str, owner_id: str, *, model: str | None,
                                 generation_mode: str | None, parameters: dict | None,
                                 reference_snapshots: list[dict] | None) -> MediaDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_draft_not_found")
            if row.status != "awaiting_confirmation" or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("media_draft_not_editable")
            if model is not None: row.model = model
            if generation_mode is not None: row.generation_mode = generation_mode
            if parameters is not None: row.parameters_json = json.dumps(parameters, ensure_ascii=False)
            if reference_snapshots is not None:
                row.references_json = json.dumps(reference_snapshots, ensure_ascii=False)
            row.updated_at = now()
        return self._media_draft_view(row)

    async def cancel_media_draft(self, draft_id: str, owner_id: str) -> MediaDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_draft_not_found")
            if row.status == "confirmed":
                raise RuntimeError("media_draft_already_confirmed")
            if row.status == "awaiting_confirmation":
                row.status = "cancelled"; row.updated_at = now()
                if row.turn_id:
                    await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == row.turn_id,
                        MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=now()))
        return self._media_draft_view(row)

    async def confirm_media_draft(self, draft_id: str, owner_id: str, *, provider_spec) -> MediaJobView:
        async with self.connections.begin() as db:
            draft = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not draft: raise KeyError("media_draft_not_found")
            if draft.job_id:
                job = await db.get(MediaJobRow, draft.job_id)
                if job: return self._media_job_view(job)
            if draft.status != "awaiting_confirmation" or utc_datetime(draft.expires_at) <= now():
                raise RuntimeError("media_draft_not_confirmable")
            job = MediaJobRow(id=identifier(), owner_id=owner_id, thread_id=draft.thread_id, turn_id=draft.turn_id, draft_id=draft.id,
                kind=draft.kind, model=draft.model, generation_mode=draft.generation_mode,
                provider=provider_spec.provider, adapter=provider_spec.adapter,
                execution_mode=provider_spec.execution, cancellation_mode=provider_spec.cancellation,
                status="created", phase="prepared", parameters_json=draft.parameters_json,
                references_json=draft.references_json,
                idempotency_key=draft.idempotency_key, output_role=draft.output_role,
                created_at=now(), updated_at=now())
            db.add(job); draft.status = "confirmed"; draft.job_id = job.id; draft.updated_at = now()
            if draft.turn_id:
                await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == draft.turn_id,
                    MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=now()))
        return self._media_job_view(job)

    async def get_media_job(self, job_id: str, owner_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id))).scalar_one_or_none()
            artifact_ids = await self._media_artifact_ids(db, row) if row else []
            outputs = await self._media_job_outputs(db, row.id) if row else []
        return self._media_job_view(row, artifact_ids, outputs) if row else None

    async def get_media_job_for_thread(self, job_id: str, thread_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id))).scalar_one_or_none()
            artifact_ids = await self._media_artifact_ids(db, row) if row else []
            outputs = await self._media_job_outputs(db, row.id) if row else []
        return self._media_job_view(row, artifact_ids, outputs) if row else None

    async def media_job_output_for_thread(self, job_id: str, thread_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id))).scalar_one_or_none()
            if not row or row.status != "succeeded" or not row.output_object_ref:
                return None
            return {"object_ref": row.output_object_ref, "mime": row.output_mime,
                "size": row.output_size, "filename": self._media_project_path(row.id, row.output_mime).rsplit("/", 1)[-1]}

    async def promote_media_job_for_thread(self, job_id: str, thread_id: str, role: str) -> dict:
        if role not in {"workspace", "delivery"}:
            raise ValueError("media_output_role_invalid")
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_job_not_found")
            if row.status != "succeeded" or not row.output_object_ref:
                raise RuntimeError("media_job_output_unavailable")
            outputs = (await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id == row.id).order_by(MediaJobOutputRow.created_at))).scalars().all()
            if outputs:
                primary_path = ""
                for output in outputs:
                    path = f"artifacts/3d/{row.id}/{output.filename}"
                    value, _ = await self._register_project_file_locked(db, row.thread_id, path=path,
                        source_type="artifact", source_turn_id=row.turn_id, sha256=output.sha256,
                        size=output.size, mime=output.mime, object_ref=output.object_ref)
                    output.project_file_id = value.id
                    if output.role == "primary":
                        primary_path = value.path
                if not primary_path:
                    raise RuntimeError("media_job_output_invalid")
            else:
                digest = row.output_object_ref.removeprefix("sha256:")
                if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                    raise RuntimeError("media_job_output_invalid")
                path = self._media_project_path(row.id, row.output_mime)
                value, _ = await self._register_project_file_locked(db, row.thread_id, path=path,
                    source_type="artifact", source_turn_id=row.turn_id, sha256=digest,
                    size=row.output_size, mime=row.output_mime or "application/octet-stream",
                    object_ref=row.output_object_ref)
                primary_path = value.path
            row.output_role = role; row.updated_at = now()
            return {"job_id": row.id, "output_role": role, "project_path": primary_path}

    async def list_media_jobs(self, thread_id: str, owner_id: str) -> list[MediaJobView]:
        async with self.connections() as db:
            rows = (await db.execute(select(MediaJobRow).where(MediaJobRow.thread_id == thread_id,
                MediaJobRow.owner_id == owner_id).order_by(MediaJobRow.created_at.desc()))).scalars().all()
            paths = {f"artifacts/{row.id}": row.id for row in rows if row.turn_id}
            artifact_ids: dict[str, list[str]] = {row.id: [] for row in rows}
            if paths:
                artifacts = (await db.execute(select(ArtifactRow.id, ArtifactRow.path).where(
                    ArtifactRow.path.in_(paths),
                ).order_by(ArtifactRow.created_at))).all()
                for artifact_id, path in artifacts:
                    artifact_ids[paths[path]].append(artifact_id)
            output_rows = (await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id.in_([row.id for row in rows]),
            ).order_by(MediaJobOutputRow.created_at, MediaJobOutputRow.id))).scalars().all() if rows else []
            outputs: dict[str, list[MediaJobOutputView]] = {row.id: [] for row in rows}
            for output in output_rows:
                outputs[output.job_id].append(MediaJobOutputView(id=output.id, role=output.role,
                    filename=output.filename, mime=output.mime, size=output.size,
                    artifact_id=output.artifact_id, project_file_id=output.project_file_id))
                if output.artifact_id:
                    artifact_ids[output.job_id].append(output.artifact_id)
        return [self._media_job_view(row, artifact_ids[row.id], outputs[row.id]) for row in rows]

    async def media_continuation_group(self, turn_id: str) -> tuple[bool, list[dict]]:
        terminal = {"succeeded", "failed", "cancelled", "unknown"}
        async with self.connections() as db:
            origin_state = (await db.execute(select(TurnRow.state).where(
                TurnRow.id == turn_id,
            ))).scalar_one_or_none()
            batch_status = (await db.execute(select(MediaBatchRow.status).where(
                MediaBatchRow.turn_id == turn_id,
            ))).scalar_one_or_none()
            drafts = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.turn_id == turn_id,
            ).order_by(MediaDraftRow.created_at, MediaDraftRow.id))).scalars().all()
            rows = (await db.execute(select(MediaJobRow).where(
                MediaJobRow.turn_id == turn_id,
            ).order_by(MediaJobRow.created_at, MediaJobRow.id))).scalars().all()
            job_ids = [row.id for row in rows]
            primary_paths = {}
            if job_ids:
                values = (await db.execute(select(MediaJobOutputRow.job_id, ProjectFileRow.path)
                    .join(ProjectFileRow, ProjectFileRow.id == MediaJobOutputRow.project_file_id)
                    .where(MediaJobOutputRow.job_id.in_(job_ids),
                        MediaJobOutputRow.role == "primary"))).all()
                primary_paths = dict(values)
        origin_finished = origin_state in {state.value for state in TERMINAL}
        jobs = {row.draft_id: row for row in rows}
        states = [("expired" if draft.status == "awaiting_confirmation"
            and utc_datetime(draft.expires_at) <= now() else draft.status) for draft in drafts]
        complete = all(state in {"cancelled", "expired"} or state == "confirmed"
            and draft.id in jobs and jobs[draft.id].status in terminal
            for draft, state in zip(drafts, states))
        ready = (origin_finished and batch_status in {"sealed", "delivering"}
            and bool(drafts) and complete)
        group = []
        for draft, draft_state in zip(drafts, states):
            row = jobs.get(draft.id)
            group.append({
                "id": row.id if row else draft.id, "job_id": row.id if row else "",
                "draft_id": draft.id, "thread_id": draft.thread_id, "turn_id": draft.turn_id,
                "kind": draft.kind, "model": draft.model,
                "status": row.status if row else draft_state,
                "output_object_ref": row.output_object_ref if row else "",
                "output_mime": row.output_mime if row else "",
                "output_size": row.output_size if row else 0,
                "error_code": row.error_code if row else "",
                "error_detail": row.error_detail if row else "",
                "error_guidance": media_error_guidance(row.error_code) if row and row.error_code else "",
                "output_role": draft.output_role,
                "project_path": (primary_paths.get(row.id) or self._media_project_path(row.id, row.output_mime))
                    if row and row.output_object_ref and row.output_role in {"workspace", "delivery"} else "",
            })
        return ready, group

    async def claim_media_batches(self, owner: str, limit: int = 20) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(MediaBatchRow).where(
                MediaBatchRow.status.in_(["sealed", "delivering"]),
                or_(MediaBatchRow.lease_expires_at.is_(None), MediaBatchRow.lease_expires_at < now()),
            ).order_by(MediaBatchRow.created_at).limit(limit).with_for_update())).scalars().all()
            result = []
            for row in rows:
                row.status = "delivering"; row.lease_owner = owner
                row.lease_expires_at = now() + timedelta(seconds=60)
                row.attempt_count += 1; row.updated_at = now()
                result.append({"turn_id": row.turn_id, "thread_id": row.thread_id,
                    "owner_id": row.owner_id})
            return result

    async def finish_media_batch(self, turn_id: str, owner: str, *, delivered: bool, error: str = "") -> None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaBatchRow).where(MediaBatchRow.turn_id == turn_id,
                MediaBatchRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row:
                return
            row.status = "continued" if delivered else "sealed"
            row.continued_at = now() if delivered else None
            row.last_error = error[:2000]; row.lease_owner = ""
            row.lease_expires_at = None if delivered else now() + timedelta(seconds=15)
            row.updated_at = now()
            if delivered:
                job_ids = select(MediaJobRow.id).where(MediaJobRow.turn_id == turn_id)
                await db.execute(update(MediaOutboxRow).where(MediaOutboxRow.job_id.in_(job_ids)).values(
                    status="delivered", lease_owner="", lease_expires_at=None, updated_at=now()))

    async def request_media_cancel(self, job_id: str, owner_id: str) -> MediaJobView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_job_not_found")
            if not (row.execution_mode == "async" and row.cancellation_mode == "supported"):
                raise RuntimeError("media_cancel_not_supported")
            if row.status in {"succeeded", "failed", "cancelled"}:
                return self._media_job_view(row)
            row.status = "cancel_requested"; row.updated_at = now()
        return self._media_job_view(row)

    async def request_media_cancel_for_thread(self, job_id: str, thread_id: str) -> MediaJobView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_job_not_found")
            if not (row.execution_mode == "async" and row.cancellation_mode == "supported"):
                raise RuntimeError("media_cancel_not_supported")
            if row.status in {"succeeded", "failed", "cancelled"}:
                return self._media_job_view(row)
            row.status = "cancel_requested"; row.updated_at = now()
        return self._media_job_view(row)

    async def request_media_refresh(self, job_id: str, owner_id: str) -> MediaJobView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_job_not_found")
            recoverable_audio = (row.adapter == "minimax_t2a" and bool(row.provider_job_id)
                and (row.status == "unknown" or (row.status == "failed"
                    and row.error_detail == "audio_timeout")))
            if recoverable_audio:
                row.status = "running"; row.phase = "provider_polling"; row.progress = None
                row.error_code = ""; row.error_detail = ""; row.finished_at = None
                row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
            elif row.status in {"created", "submitted", "running", "cancel_requested"}:
                if not row.lease_owner or not row.lease_expires_at or utc_datetime(row.lease_expires_at) <= now():
                    row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
            return self._media_job_view(row)

    async def claim_media_jobs(self, owner: str, limit: int = 4, *, adapter: str | None = None,
                               exclude_adapter: str | None = None,
                               exclude_adapters: set[str] | None = None,
                               has_provider_job: bool | None = None) -> list[dict]:
        async with self.connections.begin() as db:
            query = select(MediaJobRow).where(
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
                or_(MediaJobRow.lease_expires_at.is_(None), MediaJobRow.lease_expires_at < now())
            )
            if adapter is not None:
                query = query.where(MediaJobRow.adapter == adapter)
            if exclude_adapter is not None:
                query = query.where(MediaJobRow.adapter != exclude_adapter)
            if exclude_adapters:
                query = query.where(MediaJobRow.adapter.not_in(exclude_adapters))
            if has_provider_job is True:
                query = query.where(MediaJobRow.provider_job_id != "")
            elif has_provider_job is False:
                query = query.where(MediaJobRow.provider_job_id == "")
            rows = (await db.execute(query.order_by(MediaJobRow.created_at.asc()).limit(limit)
                .with_for_update())).scalars().all()
            expiry = now() + timedelta(seconds=60)
            result = []
            for row in rows:
                row.lease_owner = owner; row.lease_expires_at = expiry; row.updated_at = now()
                result.append({"id": row.id, "kind": row.kind, "model": row.model, "provider": row.provider,
                    "adapter": row.adapter, "status": row.status, "phase": row.phase,
                    "generation_mode": row.generation_mode or "", "parameters": json.loads(row.parameters_json or "{}"),
                    "references": json.loads(row.references_json or "[]"), "provider_job_id": row.provider_job_id,
                    "thread_id": row.thread_id, "owner_id": row.owner_id, "turn_id": row.turn_id,
                    "output_role":row.output_role, "created_at":utc_datetime(row.created_at),
                    "provider_started_at":(utc_datetime(row.provider_started_at)
                        if row.provider_started_at else None)})
        return result

    async def count_media_provider_jobs(self, adapter: str) -> int:
        async with self.connections() as db:
            return int((await db.execute(select(func.count()).select_from(MediaJobRow).where(
                MediaJobRow.adapter == adapter, MediaJobRow.provider_job_id != "",
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
            ))).scalar_one())

    async def release_media_job_lease(self, job_id: str, owner: str, *, phase: str,
                                      error_code: str = "", error_detail: str = "",
                                      retry_after_seconds: int = 0) -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.status = "running"; row.phase = phase; row.error_code = error_code
            row.error_detail = error_detail[:2000]; row.lease_owner = ""
            row.lease_expires_at = (now() + timedelta(seconds=retry_after_seconds)
                if retry_after_seconds > 0 else None); row.updated_at = now()
            return True

    async def resolve_tripo_input_job(self, job_id: str, input_job_id: str) -> str:
        async with self.connections() as db:
            current = await db.get(MediaJobRow, job_id)
            source = await db.get(MediaJobRow, input_job_id)
            if (not current or not source or source.owner_id != current.owner_id
                    or source.thread_id != current.thread_id or source.adapter != "tripo_v3"
                    or source.kind != "model3d" or source.status != "succeeded"
                    or not source.provider_job_id):
                raise ValueError("tripo_input_job_unavailable")
            return source.provider_job_id

    async def release_media_job_leases(self, owner: str) -> int:
        async with self.connections.begin() as db:
            result = await db.execute(update(MediaJobRow).where(
                MediaJobRow.lease_owner == owner,
                MediaJobRow.status.not_in(["succeeded", "failed", "cancelled", "unknown"]),
            ).values(lease_owner="", lease_expires_at=None, updated_at=now()))
            return result.rowcount or 0

    @staticmethod
    def _site_draft_view(row: SiteDraftRow) -> SiteDraftView:
        status = ("expired" if row.status == "awaiting_confirmation"
            and utc_datetime(row.expires_at) <= now() else row.status)
        return SiteDraftView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id,
            project_id=row.project_id, name=row.name, slug=row.slug, status=status,
            file_count=row.file_count, total_size=row.total_size,
            bundle_sha256=row.bundle_sha256, expires_at=utc_datetime(row.expires_at),
            job_id=row.job_id or "")

    @staticmethod
    def _site_job_view(row: SiteDeployJobRow, public_url: str = "") -> SiteDeployJobView:
        return SiteDeployJobView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id,
            project_id=row.project_id, draft_id=row.draft_id, name=row.name, slug=row.slug,
            status=row.status, phase=row.phase or "", progress=row.progress,
            error_code=row.error_code or "", public_url=public_url,
            created_at=utc_datetime(row.created_at),
            finished_at=utc_datetime(row.finished_at) if row.finished_at else None)

    async def create_site_draft(self, owner_id: str, thread_id: str, turn_id: str, *,
                                name: str, slug: str, idempotency_key: str,
                                archive_object_ref: str, bundle_sha256: str,
                                archive_size: int, file_count: int, total_size: int,
                                expires_at: datetime, public_base_path: str,
                                files: list[dict] | None = None) -> tuple[SiteDraftView, bool]:
        files = files or []
        if (len(files) != file_count or sum(value.get("size", -1) for value in files) != total_size
                or len({value.get("path") for value in files}) != file_count
                or not any(value.get("path") == "index.html" for value in files)):
            raise ValueError("site_draft_files_invalid")
        timestamp = now(); draft_id = identifier()
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if turn_id:
                turn = await db.get(TurnRow, turn_id)
                if not turn or turn.thread_id != thread_id:
                    raise KeyError("turn_not_found")
            existing = (await db.execute(select(SiteDraftRow).where(
                SiteDraftRow.thread_id == thread_id,
                SiteDraftRow.idempotency_key == idempotency_key).with_for_update())).scalar_one_or_none()
            if existing:
                if (existing.bundle_sha256 != bundle_sha256 or existing.name != name
                        or existing.archive_size != archive_size):
                    raise RuntimeError("site_draft_idempotency_conflict")
                return self._site_draft_view(existing), False
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.thread_id == thread_id, SiteProjectRow.owner_id == owner_id,
                SiteProjectRow.slug == slug).with_for_update())).scalar_one_or_none()
            if project is None:
                project_id = identifier(); candidate = slug
                collision = (await db.execute(select(SiteProjectRow.id).where(
                    SiteProjectRow.slug == candidate))).scalar_one_or_none()
                if collision:
                    candidate = f"{slug[:41]}-{project_id[:6]}"
                project = SiteProjectRow(id=project_id, owner_id=owner_id, thread_id=thread_id,
                    name=name, slug=candidate, status="draft", current_version_id="",
                    public_url=public_base_path.rstrip("/") + "/" + candidate + "/",
                    created_at=timestamp, updated_at=timestamp)
                db.add(project)
            elif project.status == "unpublished":
                project.name = name; project.updated_at = timestamp
            row = SiteDraftRow(id=draft_id, owner_id=owner_id, thread_id=thread_id,
                turn_id=turn_id, project_id=project.id, name=name, slug=project.slug,
                archive_object_ref=archive_object_ref, bundle_sha256=bundle_sha256,
                archive_size=archive_size, file_count=file_count, total_size=total_size,
                idempotency_key=idempotency_key, status="awaiting_confirmation",
                expires_at=expires_at, job_id="", created_at=timestamp, updated_at=timestamp)
            db.add(row)
            for value in files:
                db.add(SiteDraftFileRow(id=identifier(), draft_id=row.id, path=value["path"],
                    sha256=value["sha256"], size=value["size"], mime=value["mime"],
                    object_ref=value["object_ref"], created_at=timestamp))
        return self._site_draft_view(row), True

    async def get_site_draft(self, draft_id: str, owner_id: str) -> SiteDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return self._site_draft_view(row) if row else None

    async def site_draft_by_bundle(self, thread_id: str, bundle_sha256: str) -> SiteDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftRow).where(
                SiteDraftRow.thread_id == thread_id,
                SiteDraftRow.bundle_sha256 == bundle_sha256,
            ).order_by(SiteDraftRow.created_at).limit(1))).scalar_one_or_none()
        return self._site_draft_view(row) if row else None

    async def artifact_site_file(self, artifact_id: str, path: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            root = (await db.execute(select(ArtifactRow).join(TurnRow,
                TurnRow.id == ArtifactRow.turn_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(ArtifactRow.id == artifact_id,
                    ThreadRow.owner_id == owner_id))).scalar_one_or_none()
            if not root or PurePosixPath(root.path).name.lower() != "index.html":
                return None
            prefix = PurePosixPath(root.path).parent
            target = (prefix / path).as_posix() if prefix.as_posix() != "." else path
            row = (await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == root.turn_id, ArtifactRow.path == target))).scalar_one_or_none()
        return ({"object_ref":row.object_ref, "size":row.size, "sha256":row.sha256,
            "mime":row.mime} if row else None)

    async def get_site_draft_archive(self, draft_id: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return ({"object_ref":row.archive_object_ref, "size":row.archive_size,
            "sha256":row.bundle_sha256} if row else None)

    async def get_site_draft_file(self, draft_id: str, path: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDraftFileRow).join(SiteDraftRow,
                SiteDraftRow.id == SiteDraftFileRow.draft_id).where(
                    SiteDraftFileRow.draft_id == draft_id, SiteDraftFileRow.path == path,
                    SiteDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return ({"object_ref":row.object_ref, "size":row.size, "sha256":row.sha256,
            "mime":row.mime} if row else None)

    async def list_site_drafts(self, thread_id: str, owner_id: str) -> list[SiteDraftView]:
        async with self.connections() as db:
            rows = (await db.execute(select(SiteDraftRow).where(
                SiteDraftRow.thread_id == thread_id, SiteDraftRow.owner_id == owner_id,
            ).order_by(SiteDraftRow.created_at.desc()))).scalars().all()
        return [self._site_draft_view(row) for row in rows]

    async def cancel_site_draft(self, draft_id: str, owner_id: str) -> SiteDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("site_draft_not_found")
            if row.status == "confirmed":
                raise RuntimeError("site_draft_already_confirmed")
            if row.status == "awaiting_confirmation":
                row.status = "cancelled"; row.updated_at = now()
        return self._site_draft_view(row)

    async def confirm_site_draft(self, draft_id: str, owner_id: str) -> SiteDeployJobView:
        timestamp = now()
        async with self.connections.begin() as db:
            draft = (await db.execute(select(SiteDraftRow).where(SiteDraftRow.id == draft_id,
                SiteDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not draft:
                raise KeyError("site_draft_not_found")
            project = await db.get(SiteProjectRow, draft.project_id)
            if draft.job_id:
                job = await db.get(SiteDeployJobRow, draft.job_id)
                if job:
                    return self._site_job_view(job, project.public_url if project else "")
            if draft.status != "awaiting_confirmation" or utc_datetime(draft.expires_at) <= timestamp:
                raise RuntimeError("site_draft_not_confirmable")
            job = SiteDeployJobRow(id=identifier(), owner_id=owner_id,
                thread_id=draft.thread_id, turn_id=draft.turn_id, project_id=draft.project_id,
                draft_id=draft.id, name=draft.name, slug=draft.slug, status="created",
                phase="queued", progress=0, created_at=timestamp, updated_at=timestamp)
            db.add(job); draft.status = "confirmed"; draft.job_id = job.id; draft.updated_at = timestamp
        return self._site_job_view(job, project.public_url if project else "")

    async def claim_site_jobs(self, owner: str, limit: int = 2) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.status.in_(["created", "running"]),
                or_(SiteDeployJobRow.lease_expires_at.is_(None),
                    SiteDeployJobRow.lease_expires_at < now()),
            ).order_by(SiteDeployJobRow.created_at).limit(limit).with_for_update())).scalars().all()
            result = []
            for row in rows:
                draft = await db.get(SiteDraftRow, row.draft_id)
                if not draft:
                    row.status = "failed"; row.phase = "terminal"
                    row.error_code = "site_draft_missing"; row.finished_at = now(); row.updated_at = now()
                    continue
                row.status = "running"; row.phase = "validating"; row.progress = 5
                row.lease_owner = owner; row.lease_expires_at = now() + timedelta(seconds=60)
                row.updated_at = now()
                result.append({"id":row.id, "project_id":row.project_id,
                    "draft_id":row.draft_id, "thread_id":row.thread_id, "owner_id":row.owner_id,
                    "archive_object_ref":draft.archive_object_ref,
                    "archive_size":draft.archive_size, "bundle_sha256":draft.bundle_sha256,
                    "file_count":draft.file_count, "total_size":draft.total_size,
                    "files":[{"path":value.path, "sha256":value.sha256, "size":value.size,
                        "mime":value.mime, "object_ref":value.object_ref} for value in
                        (await db.execute(select(SiteDraftFileRow).where(
                            SiteDraftFileRow.draft_id == draft.id))).scalars().all()]})
            return result

    async def update_site_job(self, job_id: str, owner: str, *, phase: str,
                              progress: int, error_code: str = "", error_detail: str = "") -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id, SiteDeployJobRow.lease_owner == owner,
            ).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.phase = phase; row.progress = progress; row.error_code = error_code
            row.error_detail = error_detail[:1000]; row.updated_at = now()
            row.lease_expires_at = now() + timedelta(seconds=60)
            return True

    async def fail_site_job(self, job_id: str, owner: str, *, error_code: str,
                            error_detail: str = "") -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id, SiteDeployJobRow.lease_owner == owner,
            ).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.status = "failed"; row.phase = "terminal"; row.progress = None
            row.error_code = error_code; row.error_detail = error_detail[:1000]
            row.finished_at = now(); row.updated_at = row.finished_at
            row.lease_owner = ""; row.lease_expires_at = None
            return True

    async def complete_site_job(self, job_id: str, owner: str, *, bundle_sha256: str,
                                files: list[dict]) -> bool:
        if not files or not any(value["path"] == "index.html" for value in files):
            raise ValueError("site_files_invalid")
        timestamp = now()
        async with self.connections.begin() as db:
            job = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id, SiteDeployJobRow.lease_owner == owner,
            ).with_for_update())).scalar_one_or_none()
            if not job:
                return False
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id == job.project_id).with_for_update())).scalar_one_or_none()
            draft = await db.get(SiteDraftRow, job.draft_id)
            if not project or not draft or draft.bundle_sha256 != bundle_sha256:
                raise ValueError("site_job_contract_mismatch")
            existing = (await db.execute(select(SiteVersionRow).where(
                SiteVersionRow.project_id == project.id,
                SiteVersionRow.bundle_sha256 == bundle_sha256).with_for_update())).scalar_one_or_none()
            if existing is None:
                latest = (await db.execute(select(func.max(SiteVersionRow.version)).where(
                    SiteVersionRow.project_id == project.id))).scalar_one() or 0
                existing = SiteVersionRow(id=identifier(), project_id=project.id, job_id=job.id,
                    version=latest + 1, bundle_sha256=bundle_sha256, file_count=len(files),
                    total_size=sum(value["size"] for value in files), created_at=timestamp)
                db.add(existing)
                for value in files:
                    db.add(SiteFileRow(id=identifier(), version_id=existing.id, path=value["path"],
                        sha256=value["sha256"], size=value["size"], mime=value["mime"],
                        object_ref=value["object_ref"], created_at=timestamp))
            project.current_version_id = existing.id; project.status = "active"
            project.name = job.name; project.updated_at = timestamp
            job.status = "succeeded"; job.phase = "activated"; job.progress = 100
            job.error_code = ""; job.error_detail = ""; job.finished_at = timestamp
            job.updated_at = timestamp; job.lease_owner = ""; job.lease_expires_at = None
            draft.status = "deployed"; draft.updated_at = timestamp
            return True

    async def release_site_job_leases(self, owner: str) -> int:
        async with self.connections.begin() as db:
            result = await db.execute(update(SiteDeployJobRow).where(
                SiteDeployJobRow.lease_owner == owner,
                SiteDeployJobRow.status == "running",
            ).values(lease_owner="", lease_expires_at=None, updated_at=now()))
            return result.rowcount or 0

    async def get_site_job(self, job_id: str, owner_id: str) -> SiteDeployJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id,
                SiteDeployJobRow.owner_id == owner_id))).scalar_one_or_none()
            project = await db.get(SiteProjectRow, row.project_id) if row else None
        return self._site_job_view(row, project.public_url if project else "") if row else None

    async def get_site_job_for_thread(self, job_id: str, thread_id: str) -> SiteDeployJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.id == job_id,
                SiteDeployJobRow.thread_id == thread_id))).scalar_one_or_none()
            project = await db.get(SiteProjectRow, row.project_id) if row else None
        return self._site_job_view(row, project.public_url if project else "") if row else None

    async def list_site_jobs(self, thread_id: str, owner_id: str) -> list[SiteDeployJobView]:
        async with self.connections() as db:
            rows = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.thread_id == thread_id,
                SiteDeployJobRow.owner_id == owner_id,
            ).order_by(SiteDeployJobRow.created_at.desc()))).scalars().all()
            projects = {value.id:value for value in (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id.in_([row.project_id for row in rows])))).scalars().all()} if rows else {}
        return [self._site_job_view(row, projects[row.project_id].public_url) for row in rows]

    async def list_site_projects(self, thread_id: str, owner_id: str) -> list[SiteProjectView]:
        async with self.connections() as db:
            projects = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.thread_id == thread_id, SiteProjectRow.owner_id == owner_id,
            ).order_by(SiteProjectRow.updated_at.desc()))).scalars().all()
            versions = (await db.execute(select(SiteVersionRow).where(
                SiteVersionRow.project_id.in_([row.id for row in projects]),
            ).order_by(SiteVersionRow.version.desc()))).scalars().all() if projects else []
        grouped: dict[str, list[SiteVersionView]] = defaultdict(list)
        current = {row.id:row.current_version_id for row in projects}
        for row in versions:
            grouped[row.project_id].append(SiteVersionView(id=row.id, version=row.version,
                bundle_sha256=row.bundle_sha256, file_count=row.file_count,
                total_size=row.total_size, created_at=utc_datetime(row.created_at),
                active=current[row.project_id] == row.id))
        return [SiteProjectView(id=row.id, thread_id=row.thread_id, name=row.name,
            slug=row.slug, status=row.status, public_url=row.public_url,
            current_version_id=row.current_version_id, versions=grouped[row.id],
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at))
            for row in projects]

    async def activate_site_version(self, project_id: str, version_id: str,
                                    owner_id: str) -> SiteProjectView:
        async with self.connections.begin() as db:
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id == project_id, SiteProjectRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            version = await db.get(SiteVersionRow, version_id)
            if not project or not version or version.project_id != project.id:
                raise KeyError("site_version_not_found")
            project.current_version_id = version.id; project.status = "active"; project.updated_at = now()
            row = project
        projects = await self.list_site_projects(row.thread_id, owner_id)
        return next(value for value in projects if value.id == row.id)

    async def unpublish_site(self, project_id: str, owner_id: str) -> SiteProjectView:
        async with self.connections.begin() as db:
            project = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.id == project_id, SiteProjectRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            if not project:
                raise KeyError("site_project_not_found")
            project.status = "unpublished"; project.updated_at = now(); thread_id = project.thread_id
        projects = await self.list_site_projects(thread_id, owner_id)
        return next(value for value in projects if value.id == project_id)

    async def public_site_file(self, slug: str, path: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SiteFileRow, SiteProjectRow).join(SiteProjectRow,
                SiteProjectRow.current_version_id == SiteFileRow.version_id).where(
                    SiteProjectRow.slug == slug, SiteProjectRow.status == "active",
                    SiteFileRow.path == path))).first()
        if not row:
            return None
        file, project = row
        return {"project_id":project.id, "version_id":project.current_version_id,
            "path":file.path, "sha256":file.sha256, "size":file.size,
            "mime":file.mime, "object_ref":file.object_ref}

    async def update_media_job(self, job_id: str, owner: str, *, status: str, phase: str = "",
                               progress: int | None = None, provider_job_id: str = "",
                               output_object_ref: str = "", output_mime: str = "", output_size: int = 0,
                               error_code: str = "", error_detail: str = "") -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row: return False
            row.status = status; row.phase = phase or row.phase; row.progress = progress
            if provider_job_id: row.provider_job_id = provider_job_id
            if provider_job_id and row.provider_started_at is None: row.provider_started_at = now()
            if output_object_ref: row.output_object_ref = output_object_ref; row.output_mime = output_mime; row.output_size = output_size
            row.error_code = error_code; row.error_detail = error_detail; row.updated_at = now()
            if status in {"succeeded", "failed", "cancelled", "unknown"}:
                if row.finished_at is None: row.finished_at = row.updated_at
                row.lease_owner = ""; row.lease_expires_at = None
                if row.turn_id:
                    await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == row.turn_id,
                        MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=now()))
                if status == "succeeded" and output_object_ref and row.turn_id:
                    digest = output_object_ref.removeprefix("sha256:")
                    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                        raise ValueError("Invalid media output object reference")
                    path = f"artifacts/{row.id}"
                    existing_artifact = (await db.execute(select(ArtifactRow.id).where(
                        ArtifactRow.turn_id == row.turn_id, ArtifactRow.path == path,
                    ).limit(1))).scalar_one_or_none()
                    if not existing_artifact:
                        db.add(ArtifactRow(id=identifier(), turn_id=row.turn_id, path=path,
                            sha256=digest, size=output_size, mime=output_mime or "application/octet-stream",
                            object_ref=output_object_ref, created_at=now()))
                    if row.output_role in {"workspace", "delivery"}:
                        project_path = self._media_project_path(row.id, output_mime)
                        await self._register_project_file_locked(db, row.thread_id, path=project_path,
                            source_type="artifact", source_turn_id=row.turn_id, sha256=digest,
                            size=output_size, mime=output_mime or "application/octet-stream",
                            object_ref=output_object_ref)
                existing = (await db.execute(select(MediaOutboxRow).where(
                    MediaOutboxRow.job_id == row.id, MediaOutboxRow.event_type == "media.job.terminal"))).scalar_one_or_none()
                if not existing:
                    db.add(MediaOutboxRow(id=identifier(), job_id=row.id, event_type="media.job.terminal",
                        status="pending", created_at=now(), updated_at=now()))
            else:
                row.lease_expires_at = now() + timedelta(seconds=60)
            return True

    async def complete_media_job(self, job_id: str, owner: str, *, provider_job_id: str,
                                 outputs: list[dict], credits_consumed: float | None = None) -> bool:
        if not outputs or sum(output.get("role") == "primary" for output in outputs) != 1:
            raise ValueError("media_outputs_require_one_primary")
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            existing = (await db.execute(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id == row.id))).scalars().all()
            if existing:
                raise RuntimeError("media_outputs_already_committed")
            timestamp = now()
            primary = next(output for output in outputs if output["role"] == "primary")
            artifact_ids: list[str] = []
            seen_roles: set[str] = set()
            seen_names: set[str] = set()
            for output in outputs:
                role = str(output.get("role") or "")
                filename = PurePosixPath(str(output.get("filename") or "")).name
                object_ref = str(output.get("object_ref") or "")
                digest = object_ref.removeprefix("sha256:")
                mime = str(output.get("mime") or "application/octet-stream")
                size = int(output.get("size") or 0)
                if (role not in {"primary", "preview", "auxiliary"} or role in seen_roles
                        or not filename or filename in seen_names or size <= 0 or len(digest) != 64
                        or any(character not in "0123456789abcdef" for character in digest)):
                    raise ValueError("invalid_media_output")
                seen_roles.add(role); seen_names.add(filename)
                artifact_id = identifier() if row.turn_id else ""
                artifact_path = f"artifacts/{row.id}/{filename}"
                if row.turn_id:
                    db.add(ArtifactRow(id=artifact_id, turn_id=row.turn_id, path=artifact_path,
                        sha256=digest, size=size, mime=mime, object_ref=object_ref, created_at=timestamp))
                    artifact_ids.append(artifact_id)
                project_file_id = ""
                if row.output_role in {"workspace", "delivery"}:
                    project, _ = await self._register_project_file_locked(db, row.thread_id,
                        path=f"artifacts/3d/{row.id}/{filename}", source_type="artifact",
                        source_turn_id=row.turn_id, sha256=digest, size=size, mime=mime,
                        object_ref=object_ref)
                    project_file_id = project.id
                if row.turn_id:
                    sequence = await self.next_item_sequence(db, row.thread_id)
                    db.add(ItemRow(id=identifier(), thread_id=row.thread_id, turn_id=row.turn_id,
                        sequence=sequence, type=ItemType.ARTIFACT.value, status="completed",
                        summary=filename, payload_json=json.dumps({"artifact_id": artifact_id,
                            "mime": mime, "size": size}, ensure_ascii=False),
                        created_at=timestamp, updated_at=timestamp))
                db.add(MediaJobOutputRow(id=identifier(), job_id=row.id, role=role,
                    filename=filename, object_ref=object_ref, sha256=digest, size=size, mime=mime,
                    artifact_id=artifact_id, project_file_id=project_file_id, created_at=timestamp))
            row.provider_job_id = provider_job_id or row.provider_job_id
            row.output_object_ref = primary["object_ref"]
            row.output_mime = primary["mime"]
            row.output_size = int(primary["size"])
            row.credits_consumed = "" if credits_consumed is None else str(credits_consumed)
            row.status = "succeeded"; row.phase = "committed"; row.progress = 100
            row.error_code = ""; row.error_detail = ""; row.updated_at = timestamp
            row.finished_at = row.finished_at or timestamp
            row.lease_owner = ""; row.lease_expires_at = None
            if row.turn_id:
                await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == row.turn_id,
                    MediaBatchRow.status == "sealed").values(lease_expires_at=None, updated_at=timestamp))
            outbox = (await db.execute(select(MediaOutboxRow).where(MediaOutboxRow.job_id == row.id,
                MediaOutboxRow.event_type == "media.job.terminal"))).scalar_one_or_none()
            if not outbox:
                db.add(MediaOutboxRow(id=identifier(), job_id=row.id,
                    event_type="media.job.terminal", status="pending",
                    created_at=timestamp, updated_at=timestamp))
            return True

    async def claim_media_outbox(self, owner: str, limit: int = 20) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(MediaOutboxRow, MediaJobRow).join(MediaJobRow, MediaJobRow.id == MediaOutboxRow.job_id).where(
                MediaOutboxRow.status.in_(["pending", "delivering"]),
                or_(MediaOutboxRow.lease_expires_at.is_(None), MediaOutboxRow.lease_expires_at < now())
            ).order_by(MediaOutboxRow.created_at.asc()).limit(limit).with_for_update())).all()
            result = []
            for outbox, job in rows:
                outbox.status = "delivering"; outbox.lease_owner = owner; outbox.lease_expires_at = now() + timedelta(seconds=60)
                outbox.attempt_count += 1; outbox.updated_at = now()
                result.append({"outbox_id": outbox.id, "job_id": job.id, "thread_id": job.thread_id,
                    "owner_id": job.owner_id, "turn_id": job.turn_id, "kind": job.kind, "model": job.model, "status": job.status,
                    "output_object_ref": job.output_object_ref, "output_mime": job.output_mime, "output_size": job.output_size,
                    "error_code": job.error_code, "error_detail": job.error_detail})
        return result

    async def finish_media_outbox(self, outbox_id: str, owner: str, *, delivered: bool, error: str = "") -> None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaOutboxRow).where(MediaOutboxRow.id == outbox_id,
                MediaOutboxRow.lease_owner == owner).with_for_update())).scalar_one_or_none()
            if not row: return
            row.status = "delivered" if delivered else "pending"; row.last_error = error[:2000]
            row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
