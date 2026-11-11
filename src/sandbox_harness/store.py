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
from .domain import AccountView, RuntimeEvent, Artifact, Event, EventKind, Harness, Interaction, Item, ItemType, ProjectFile, ThreadCreate, ThreadView, TurnCreate, TurnInputCreate, TurnInputView, TurnState, TurnView, identifier, now, MediaDraftView, MediaJobView
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
    parameters_json: Mapped[str] = mapped_column(Text, default="{}")
    idempotency_key: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    job_id: Mapped[str] = mapped_column(String(32), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
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
    provider: Mapped[str] = mapped_column(String(80))
    adapter: Mapped[str] = mapped_column(String(120))
    execution_mode: Mapped[str] = mapped_column(String(16))
    cancellation_mode: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(32), index=True)
    phase: Mapped[str] = mapped_column(String(32), default="", server_default="")
    progress: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parameters_json: Mapped[str] = mapped_column(Text, default="{}")
    provider_job_id: Mapped[str] = mapped_column(String(200), default="", server_default="")
    idempotency_key: Mapped[str] = mapped_column(String(160))
    error_code: Mapped[str] = mapped_column(String(100), default="", server_default="")
    error_detail: Mapped[str] = mapped_column(Text, default="", server_default="")
    output_object_ref: Mapped[str] = mapped_column(Text, default="", server_default="")
    output_mime: Mapped[str] = mapped_column(String(200), default="", server_default="")
    output_size: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    lease_owner: Mapped[str] = mapped_column(String(64), default="", server_default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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


class UserRow(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(Text)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
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
            turn_ids = [turn.id for turn in turns]
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

    @observed("db.turn.create")
    async def create_turn(self, thread_id: str, value: TurnCreate, resolved: ResolvedProfile, owner_id: str, *,
                          traceparent: str = "", network_access: str = "restricted") -> tuple[TurnView, bool, bool]:
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
                        or existing.network_access != network_access):
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
                network_access=network_access,
                state=TurnState.QUEUED.value, submit_traceparent=traceparent, created_at=timestamp, updated_at=timestamp,
            )
            db.add(row)
            item_sequence = await self.next_item_sequence(db, thread_id)
            db.add(ItemRow(id=identifier(), thread_id=thread_id, turn_id=row.id, sequence=item_sequence,
                type=ItemType.USER_MESSAGE.value, status="completed", summary=value.message,
                payload_json=json.dumps({"attachments":[item.model_dump(mode="json") for item in value.attachments]}),
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

    async def register_project_file(self, thread_id: str, *, path: str, source_type: str,
                                    source_turn_id: str, sha256: str, size: int, mime: str,
                                    object_ref: str, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> tuple[ProjectFile, bool]:
        if not valid_project_file_path(path, source_type):
            raise ValueError("Invalid managed project path")
        async with self.connections.begin() as db:
            await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id).values(updated_at=ThreadRow.updated_at))
            if lease_owner is not None:
                await self.require_lease(db, source_turn_id, lease_owner, recovery_generation)
            latest = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
            if latest and latest.sha256 == sha256 and not latest.deleted:
                return self.project_file_view(latest), False
            row = ProjectFileRow(id=identifier(), thread_id=thread_id, path=path,
                version=(latest.version + 1 if latest else 1), source_type=source_type,
                source_turn_id=source_turn_id, sha256=sha256, size=size, mime=mime,
                object_ref=object_ref, created_at=now())
            db.add(row)
        return self.project_file_view(row), True

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
    def _media_draft_view(row: MediaDraftRow) -> MediaDraftView:
        return MediaDraftView(id=row.id, thread_id=row.thread_id, kind=row.kind, model=row.model,
            parameters=json.loads(row.parameters_json or "{}"), status=row.status,
            expires_at=utc_datetime(row.expires_at), job_id=row.job_id or "")

    @staticmethod
    def _media_job_view(row: MediaJobRow, artifact_ids: list[str] | None = None) -> MediaJobView:
        reason = "" if row.cancellation_mode == "supported" and row.execution_mode == "async" else "provider_not_cancelable"
        return MediaJobView(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id or "", kind=row.kind,
            model=row.model, provider=row.provider, adapter=row.adapter, execution_mode=row.execution_mode,
            cancellation_mode=row.cancellation_mode, status=row.status, phase=row.phase or "",
            progress=row.progress, cancelable=row.execution_mode == "async" and row.cancellation_mode == "supported",
            cancel_reason=reason, artifact_ids=artifact_ids or [])

    @staticmethod
    async def _media_artifact_ids(db: AsyncSession, row: MediaJobRow) -> list[str]:
        if not row.turn_id:
            return []
        return list((await db.execute(select(ArtifactRow.id).where(
            ArtifactRow.turn_id == row.turn_id,
            ArtifactRow.path == f"artifacts/{row.id}",
        ).order_by(ArtifactRow.created_at))).scalars().all())

    async def create_media_draft(self, owner_id: str, thread_id: str, value, *, expires_at: datetime, provider_spec) -> tuple[MediaDraftView, bool]:
        async with self.connections.begin() as db:
            if value.turn_id:
                owned_thread = (await db.execute(select(TurnRow.thread_id).join(ThreadRow,
                    ThreadRow.id == TurnRow.thread_id).where(
                        TurnRow.id == value.turn_id, ThreadRow.owner_id == owner_id,
                ))).scalar_one_or_none()
                if owned_thread != thread_id:
                    raise KeyError("media_turn_not_found")
            existing = (await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.thread_id == thread_id, MediaDraftRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                if existing.owner_id != owner_id or existing.kind != value.kind or existing.model != value.model:
                    raise RuntimeError("media_draft_idempotency_conflict")
                return self._media_draft_view(existing), False
            row = MediaDraftRow(id=identifier(), owner_id=owner_id, thread_id=thread_id, turn_id=value.turn_id,
                kind=value.kind, model=value.model, parameters_json=json.dumps(value.parameters, ensure_ascii=False),
                idempotency_key=value.idempotency_key, status="awaiting_confirmation", expires_at=expires_at,
                created_at=now(), updated_at=now())
            db.add(row)
        return self._media_draft_view(row), True

    async def get_media_draft(self, draft_id: str, owner_id: str) -> MediaDraftView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id))).scalar_one_or_none()
        return self._media_draft_view(row) if row else None

    async def update_media_draft(self, draft_id: str, owner_id: str, *, model: str | None, parameters: dict | None) -> MediaDraftView:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaDraftRow).where(MediaDraftRow.id == draft_id,
                MediaDraftRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row: raise KeyError("media_draft_not_found")
            if row.status != "awaiting_confirmation" or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("media_draft_not_editable")
            if model is not None: row.model = model
            if parameters is not None: row.parameters_json = json.dumps(parameters, ensure_ascii=False)
            row.updated_at = now()
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
                kind=draft.kind, model=draft.model, provider=provider_spec.provider, adapter=provider_spec.adapter,
                execution_mode=provider_spec.execution, cancellation_mode=provider_spec.cancellation,
                status="created", phase="prepared", parameters_json=draft.parameters_json,
                idempotency_key=draft.idempotency_key, created_at=now(), updated_at=now())
            db.add(job); draft.status = "confirmed"; draft.job_id = job.id; draft.updated_at = now()
        return self._media_job_view(job)

    async def get_media_job(self, job_id: str, owner_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.owner_id == owner_id))).scalar_one_or_none()
            artifact_ids = await self._media_artifact_ids(db, row) if row else []
        return self._media_job_view(row, artifact_ids) if row else None

    async def get_media_job_for_thread(self, job_id: str, thread_id: str) -> MediaJobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaJobRow).where(MediaJobRow.id == job_id,
                MediaJobRow.thread_id == thread_id))).scalar_one_or_none()
            artifact_ids = await self._media_artifact_ids(db, row) if row else []
        return self._media_job_view(row, artifact_ids) if row else None

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
        return [self._media_job_view(row, artifact_ids[row.id]) for row in rows]

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

    async def claim_media_jobs(self, owner: str, limit: int = 4) -> list[dict]:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(MediaJobRow).where(
                MediaJobRow.status.in_(["created", "submitted", "running", "cancel_requested"]),
                or_(MediaJobRow.lease_expires_at.is_(None), MediaJobRow.lease_expires_at < now())
            ).order_by(MediaJobRow.created_at.asc()).limit(limit).with_for_update())).scalars().all()
            expiry = now() + timedelta(seconds=60)
            result = []
            for row in rows:
                row.lease_owner = owner; row.lease_expires_at = expiry; row.updated_at = now()
                result.append({"id": row.id, "kind": row.kind, "model": row.model, "provider": row.provider,
                    "adapter": row.adapter, "status": row.status, "phase": row.phase,
                    "parameters": json.loads(row.parameters_json or "{}"), "provider_job_id": row.provider_job_id})
        return result

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
                row.lease_owner = ""; row.lease_expires_at = None
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
                existing = (await db.execute(select(MediaOutboxRow).where(
                    MediaOutboxRow.job_id == row.id, MediaOutboxRow.event_type == "media.job.terminal"))).scalar_one_or_none()
                if not existing:
                    db.add(MediaOutboxRow(id=identifier(), job_id=row.id, event_type="media.job.terminal",
                        status="pending", created_at=now(), updated_at=now()))
            else:
                row.lease_expires_at = now() + timedelta(seconds=60)
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
