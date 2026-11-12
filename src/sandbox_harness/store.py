from __future__ import annotations

import json
import asyncio
import hashlib
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath

from sqlalchemy import case, or_, Boolean, DateTime, Integer, String, Text, UniqueConstraint, delete, func, inspect, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .observability import observed
from .domain import AccountView, RuntimeEvent, Artifact, Event, EventKind, Harness, Interaction, Item, ItemType, ProjectFile, ThreadCreate, ThreadView, TurnCreate, TurnInputCreate, TurnInputView, TurnState, TurnView, TERMINAL, identifier, now, MediaDraftView, MediaJobView, MediaJobOutputView, MediaReferenceUploadView
from .catalog import ResolvedProfile
from .events import RedisEventBus


class Base(DeclarativeBase):
    pass


def valid_project_file_path(path: str, source_type: str) -> bool:
    pure = PurePosixPath(path)
    expected_root = "assets" if source_type == "upload" else "artifacts" if source_type == "artifact" else ""
    return bool(expected_root and not pure.is_absolute() and len(pure.parts) >= 2
        and pure.parts[0] == expected_root and not any(part in {"", ".", ".."} for part in pure.parts))


def utc_datetime(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


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


class TurnRow(Base):
    __tablename__ = "turns"
    __table_args__ = (UniqueConstraint("thread_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    message: Mapped[str] = mapped_column(Text)
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
    capability_nonce: Mapped[str] = mapped_column(String(64), default="", server_default="")
    submit_traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    last_traceparent: Mapped[str] = mapped_column(String(128), default="", server_default="")
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    pending_final_state: Mapped[str] = mapped_column(String(24), default="", server_default="")
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


class Store:
    def __init__(self, database_url: str, redis_url: str = "", redis_prefix: str = "sandbox-harness:"):
        self.engine = create_async_engine(database_url)
        self.connections = async_sessionmaker(self.engine, expire_on_commit=False)
        self.events = RedisEventBus(redis_url, redis_prefix) if redis_url else None
        self.event_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.event_lock_users: dict[str, int] = defaultdict(int)
        self.event_lock_guard = asyncio.Lock()

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

    async def initialize(self) -> None:
        if self.events:
            await self.events.initialize()
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("threads")})
            if "owner_id" not in columns:
                await connection.execute(text("ALTER TABLE threads ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT 'legacy-unowned'"))
            if "last_read_turn_id" not in columns:
                await connection.execute(text("ALTER TABLE threads ADD COLUMN last_read_turn_id VARCHAR(32) NOT NULL DEFAULT ''"))
            if "next_item_sequence" not in columns:
                await connection.execute(text("ALTER TABLE threads ADD COLUMN next_item_sequence INTEGER NOT NULL DEFAULT 0"))
                await connection.execute(text("UPDATE threads SET next_item_sequence = COALESCE((SELECT MAX(items.sequence) FROM items WHERE items.thread_id = threads.id), 0)"))
            if "archived_at" not in columns:
                definition = "TIMESTAMP WITH TIME ZONE NULL" if connection.dialect.name == "postgresql" else "DATETIME NULL"
                await connection.execute(text(f"ALTER TABLE threads ADD COLUMN archived_at {definition}"))
            await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_threads_owner_id ON threads (owner_id)"))
            input_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("turn_inputs")})
            if "delivery_mode" not in input_columns:
                await connection.execute(text("ALTER TABLE turn_inputs ADD COLUMN delivery_mode VARCHAR(30) NOT NULL DEFAULT 'pending'"))
            inspected_turn_columns = await connection.run_sync(lambda value: inspect(value).get_columns("turns"))
            turn_columns = {column["name"] for column in inspected_turn_columns}
            additions = {
                "network_access": "VARCHAR(20) NOT NULL DEFAULT 'restricted'",
                "internal": "BOOLEAN NOT NULL DEFAULT FALSE",
                "lease_owner": "VARCHAR(64) NOT NULL DEFAULT ''",
                "lease_expires_at": "TIMESTAMP WITH TIME ZONE NULL",
                "recovery_generation": "INTEGER NOT NULL DEFAULT 0",
                "runtime_sequence": "INTEGER NOT NULL DEFAULT 0",
                "runtime_execution_id": "VARCHAR(160) NOT NULL DEFAULT ''",
                "capability_nonce": "VARCHAR(64) NOT NULL DEFAULT ''",
                "pending_final_state": "VARCHAR(24) NOT NULL DEFAULT ''",
                "cancel_requested": "BOOLEAN NOT NULL DEFAULT FALSE",
                "submit_traceparent": "VARCHAR(128) NOT NULL DEFAULT ''",
                "last_traceparent": "VARCHAR(128) NOT NULL DEFAULT ''",
            }
            for name, definition in additions.items():
                if name not in turn_columns:
                    await connection.execute(text(f"ALTER TABLE turns ADD COLUMN {name} {definition}"))
            lease_column = next((column for column in inspected_turn_columns
                if column["name"] == "lease_expires_at"), None)
            if (connection.dialect.name == "postgresql" and lease_column is not None
                    and not getattr(lease_column["type"], "timezone", False)):
                await connection.execute(text(
                    "ALTER TABLE turns ALTER COLUMN lease_expires_at TYPE TIMESTAMP WITH TIME ZONE "
                    "USING lease_expires_at AT TIME ZONE current_setting('TIMEZONE')"
                ))
            event_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("events")})
            if "source_execution_id" not in event_columns:
                await connection.execute(text("ALTER TABLE events ADD COLUMN source_execution_id VARCHAR(160) NOT NULL DEFAULT ''"))
            if "source_sequence" not in event_columns:
                await connection.execute(text("ALTER TABLE events ADD COLUMN source_sequence INTEGER NOT NULL DEFAULT 0"))
            project_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("project_files")})
            if "deleted" not in project_columns:
                await connection.execute(text("ALTER TABLE project_files ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE"))
            interaction_columns = await connection.run_sync(
                lambda value: {column["name"] for column in inspect(value).get_columns("interactions")})
            if "response_owner" not in interaction_columns:
                await connection.execute(text(
                    "ALTER TABLE interactions ADD COLUMN response_owner VARCHAR(64) NOT NULL DEFAULT ''"))
            if "response_expires_at" not in interaction_columns:
                await connection.execute(text(
                    "ALTER TABLE interactions ADD COLUMN response_expires_at TIMESTAMP WITH TIME ZONE NULL"))
            for table in ("media_drafts", "media_jobs"):
                media_columns = await connection.run_sync(
                    lambda value, table=table: {column["name"] for column in inspect(value).get_columns(table)})
                if "output_role" not in media_columns:
                    await connection.execute(text(
                        f"ALTER TABLE {table} ADD COLUMN output_role VARCHAR(20) NOT NULL DEFAULT 'candidate'"))
                if "generation_mode" not in media_columns:
                    await connection.execute(text(
                        f"ALTER TABLE {table} ADD COLUMN generation_mode VARCHAR(40) NOT NULL DEFAULT ''"))
                if "references_json" not in media_columns:
                    await connection.execute(text(
                        f"ALTER TABLE {table} ADD COLUMN references_json TEXT NOT NULL DEFAULT '[]'"))
                if table == "media_jobs" and "finished_at" not in media_columns:
                    definition = "TIMESTAMP WITH TIME ZONE NULL" if connection.dialect.name == "postgresql" else "DATETIME NULL"
                    await connection.execute(text(f"ALTER TABLE media_jobs ADD COLUMN finished_at {definition}"))
                    await connection.execute(text(
                        "UPDATE media_jobs SET finished_at = updated_at "
                        "WHERE status IN ('succeeded', 'failed', 'cancelled', 'unknown')"))
                if table == "media_jobs" and "credits_consumed" not in media_columns:
                    await connection.execute(text(
                        "ALTER TABLE media_jobs ADD COLUMN credits_consumed VARCHAR(40) NOT NULL DEFAULT ''"))
            user_columns = await connection.run_sync(
                lambda value: {column["name"] for column in inspect(value).get_columns("users")})
            user_additions = {
                "avatar_object_ref": "TEXT NOT NULL DEFAULT ''",
                "avatar_mime": "VARCHAR(100) NOT NULL DEFAULT ''",
                "avatar_version": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, definition in user_additions.items():
                if name not in user_columns:
                    await connection.execute(text(f"ALTER TABLE users ADD COLUMN {name} {definition}"))

    async def close(self) -> None:
        if self.events:
            await self.events.close()
        await self.engine.dispose()

    @staticmethod
    def thread_view(row: ThreadRow) -> ThreadView:
        return ThreadView.model_validate({key: getattr(row, key) for key in ThreadView.model_fields})

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

    async def register_user(self, username: str, password_hash: str, source_owner_id: str = "") -> AccountView:
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
                db.add(row)
                if source_owner_id:
                    await db.execute(update(ThreadRow).where(ThreadRow.owner_id == source_owner_id)
                        .values(owner_id=row.owner_id))
                    await db.execute(update(UploadRow).where(UploadRow.owner_id == source_owner_id)
                        .values(owner_id=row.owner_id))
                    await db.execute(update(MediaDraftRow).where(MediaDraftRow.owner_id == source_owner_id)
                        .values(owner_id=row.owner_id))
                    await db.execute(update(MediaBatchRow).where(MediaBatchRow.owner_id == source_owner_id)
                        .values(owner_id=row.owner_id))
                    await db.execute(update(MediaJobRow).where(MediaJobRow.owner_id == source_owner_id)
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
            **{key: getattr(row, key) for key in TurnView.model_fields if key != "attachments"},
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
            await db.execute(delete(InteractionRow).where(InteractionRow.thread_id == thread_id))
            await db.execute(delete(ProjectFileRow).where(ProjectFileRow.thread_id == thread_id))
            await db.execute(delete(ItemRow).where(ItemRow.thread_id == thread_id))
            if turn_ids:
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
                          internal: bool = False) -> tuple[TurnView, bool, bool]:
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
                if (existing.message != value.message or json.loads(existing.attachments_json) != attachments
                        or existing.harness != resolved.harness.value or existing.model != resolved.model
                        or existing.reasoning_level != resolved.reasoning_level
                        or existing.delivery_mode != value.delivery_mode
                        or existing.network_access != network_access or existing.internal != internal):
                    raise RuntimeError("idempotency_conflict")
                return self.turn_view(existing), False, thread.active_turn_id == existing.id
            active_running = False
            if thread.active_turn_id:
                active = await db.get(TurnRow, thread.active_turn_id)
                if active and TurnState(active.state) not in {TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED}:
                    active_running = True
            if not active_running:
                queued = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id,
                    TurnRow.state.not_in(["COMPLETED", "WAITING_USER", "FAILED", "CANCELLED"]))
                    .order_by(TurnRow.created_at, TurnRow.id).limit(1))).scalar_one_or_none()
                if queued:
                    thread.active_turn_id = queued.id
                    active_running = True
            if active_running and value.delivery_mode == "start":
                raise RuntimeError("thread_busy")
            if value.delivery_mode == "steer":
                raise RuntimeError("steer_requires_active_turn_endpoint")
            timestamp = now()
            row = TurnRow(
                id=identifier(), thread_id=thread_id, idempotency_key=value.idempotency_key,
                message=value.message, attachments_json=json.dumps([a.model_dump(mode="json") for a in value.attachments]),
                harness=resolved.harness.value, model=resolved.model, reasoning_level=resolved.reasoning_level,
                reasoning_effort=resolved.reasoning_effort, delivery_mode=value.delivery_mode,
                network_access=network_access, internal=internal,
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
        terminal = [state.value for state in (TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED)]
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
        terminal = [state.value for state in (TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED)]
        async with self.connections() as db:
            rows = (await db.execute(select(TurnRow).join(ThreadRow,
                ThreadRow.active_turn_id == TurnRow.id).where(TurnRow.state.not_in(terminal),
                or_(TurnRow.lease_expires_at.is_(None), TurnRow.lease_expires_at <= now()))
                .order_by(TurnRow.created_at))).scalars().all()
        return [self.turn_view(row) for row in rows]

    async def nonterminal_turn_ids(self) -> set[str]:
        terminal = [state.value for state in {
            TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED,
        }]
        async with self.connections() as db:
            rows = (await db.execute(select(TurnRow.id).where(TurnRow.state.not_in(terminal)))).scalars().all()
        return set(rows)

    async def claim_turn(self, turn_id: str, owner: str, lease_seconds: int) -> TurnView | None:
        timestamp = now()
        terminal = [state.value for state in (TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED)]
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
            row.runtime_execution_id = execution_id
            row.runtime_sequence = 0
            row.updated_at = now()

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
                payload_json=json.dumps({"input_id": row.id, "attachments": [item.model_dump(mode="json") for item in value.attachments]}),
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
                    next_status = {"applied":"completed", "failed":"failed"}.get(status, "in_progress")
                    if item.status != next_status:
                        item.status = next_status
                        item.updated_at = now()
                        updated_item_id = item.id
                        turn_id = row.turn_id
                        item_status = next_status
        if updated_item_id:
            await self.append_event(turn_id, EventKind.ITEM_UPDATED,
                {"item_id": updated_item_id, "status": item_status, "input_id": input_id},
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
            TurnState.FAILED, TurnState.CANCELLED)]
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
            if state in {TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED}:
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

    async def artifact_by_identity(self, turn_id: str, path: str, sha256: str) -> Artifact | None:
        async with self.connections() as db:
            row = (await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == turn_id, ArtifactRow.path == path, ArtifactRow.sha256 == sha256,
            ).limit(1))).scalar_one_or_none()
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None

    async def artifacts(self, turn_id: str) -> list[Artifact]:
        async with self.connections() as db:
            rows = (await db.execute(select(ArtifactRow).where(ArtifactRow.turn_id == turn_id).order_by(ArtifactRow.created_at))).scalars().all()
        return [Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) for row in rows]

    @staticmethod
    def project_file_view(row: ProjectFileRow) -> ProjectFile:
        return ProjectFile.model_validate({key: getattr(row, key) for key in ProjectFile.model_fields})

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

    async def project_file_history(self, thread_id: str, path: str, owner_id: str) -> list[ProjectFile]:
        async with self.connections() as db:
            rows = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                    ThreadRow.owner_id == owner_id,
                ).order_by(ProjectFileRow.version.desc()))).scalars().all()
        return [self.project_file_view(row) for row in rows]

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

    async def record_upload(self, owner_id: str, attachment) -> None:
        async with self.connections.begin() as db:
            existing = await db.get(UploadRow, (owner_id, attachment.object_ref))
            if not existing:
                db.add(UploadRow(owner_id=owner_id, object_ref=attachment.object_ref, sha256=attachment.sha256,
                    size=attachment.size, name=attachment.name, mime=attachment.mime, created_at=now()))

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
            progress=row.progress, cancelable=row.execution_mode == "async" and row.cancellation_mode == "supported",
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

    async def claim_media_jobs(self, owner: str, limit: int = 4, *, adapter: str | None = None,
                               exclude_adapter: str | None = None) -> list[dict]:
        async with self.connections.begin() as db:
            query = select(MediaJobRow).where(
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
                or_(MediaJobRow.lease_expires_at.is_(None), MediaJobRow.lease_expires_at < now())
            )
            if adapter is not None:
                query = query.where(MediaJobRow.adapter == adapter)
            if exclude_adapter is not None:
                query = query.where(MediaJobRow.adapter != exclude_adapter)
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
                    "output_role": row.output_role})
        return result

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
