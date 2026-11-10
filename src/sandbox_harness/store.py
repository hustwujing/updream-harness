from __future__ import annotations

import json
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath

from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint, delete, func, inspect, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .domain import Artifact, Event, EventKind, Harness, Interaction, Item, ItemType, ProjectFile, ThreadCreate, ThreadView, TurnCreate, TurnInputCreate, TurnInputView, TurnState, TurnView, identifier, now
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
    next_item_sequence: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
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
            if "next_item_sequence" not in columns:
                await connection.execute(text("ALTER TABLE threads ADD COLUMN next_item_sequence INTEGER NOT NULL DEFAULT 0"))
                await connection.execute(text("UPDATE threads SET next_item_sequence = COALESCE((SELECT MAX(items.sequence) FROM items WHERE items.thread_id = threads.id), 0)"))
            await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_threads_owner_id ON threads (owner_id)"))
            input_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("turn_inputs")})
            if "delivery_mode" not in input_columns:
                await connection.execute(text("ALTER TABLE turn_inputs ADD COLUMN delivery_mode VARCHAR(30) NOT NULL DEFAULT 'pending'"))
            inspected_turn_columns = await connection.run_sync(lambda value: inspect(value).get_columns("turns"))
            turn_columns = {column["name"] for column in inspected_turn_columns}
            additions = {
                "lease_owner": "VARCHAR(64) NOT NULL DEFAULT ''",
                "lease_expires_at": "TIMESTAMP WITH TIME ZONE NULL",
                "recovery_generation": "INTEGER NOT NULL DEFAULT 0",
                "runtime_sequence": "INTEGER NOT NULL DEFAULT 0",
                "runtime_execution_id": "VARCHAR(160) NOT NULL DEFAULT ''",
                "capability_nonce": "VARCHAR(64) NOT NULL DEFAULT ''",
                "pending_final_state": "VARCHAR(24) NOT NULL DEFAULT ''",
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

    async def list_threads(self, owner_id: str) -> list[ThreadView]:
        async with self.connections() as db:
            rows = (await db.execute(select(ThreadRow).where(ThreadRow.owner_id == owner_id).order_by(ThreadRow.updated_at.desc()))).scalars().all()
        return [self.thread_view(row) for row in rows]

    async def list_thread_summaries(self, owner_id: str) -> list[dict]:
        threads = await self.list_threads(owner_id)
        async with self.connections() as db:
            ranked = select(TurnRow.thread_id, TurnRow.state, TurnRow.updated_at,
                func.row_number().over(partition_by=TurnRow.thread_id,
                    order_by=TurnRow.created_at.desc()).label("rank")).join(
                ThreadRow, ThreadRow.id == TurnRow.thread_id).where(ThreadRow.owner_id == owner_id).subquery()
            rows = (await db.execute(select(ranked.c.thread_id, ranked.c.state, ranked.c.updated_at)
                .where(ranked.c.rank == 1))).all()
        latest = {row.thread_id: row for row in rows}
        return [{**thread.model_dump(mode="json"),
            "latest_turn_state": latest[thread.id].state if thread.id in latest else "",
            "latest_turn_updated_at": latest[thread.id].updated_at if thread.id in latest else None,
        } for thread in threads]

    async def get_thread(self, thread_id: str, owner_id: str | None = None) -> ThreadView | None:
        async with self.connections() as db:
            query = select(ThreadRow).where(ThreadRow.id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
        return self.thread_view(row) if row else None

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

    async def create_turn(self, thread_id: str, value: TurnCreate, resolved: ResolvedProfile, owner_id: str) -> tuple[TurnView, bool, bool]:
        async with self.connections.begin() as db:
            thread = await db.get(ThreadRow, thread_id, with_for_update=True)
            if not thread or thread.owner_id != owner_id:
                raise KeyError("thread_not_found")
            existing = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id, TurnRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                attachments = [item.model_dump(mode="json") for item in value.attachments]
                if (existing.message != value.message or json.loads(existing.attachments_json) != attachments
                        or existing.harness != resolved.harness.value or existing.model != resolved.model
                        or existing.reasoning_level != resolved.reasoning_level
                        or existing.delivery_mode != value.delivery_mode):
                    raise RuntimeError("idempotency_conflict")
                return self.turn_view(existing), False, thread.active_turn_id == existing.id
            active_running = False
            if thread.active_turn_id:
                active = await db.get(TurnRow, thread.active_turn_id)
                if active and TurnState(active.state) not in {TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED}:
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
                state=TurnState.QUEUED.value, created_at=timestamp, updated_at=timestamp,
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

    async def get_turn(self, turn_id: str) -> TurnView | None:
        async with self.connections() as db:
            row = await db.get(TurnRow, turn_id)
        return self.turn_view(row) if row else None

    async def recoverable_turns(self) -> list[TurnView]:
        terminal = [state.value for state in {
            TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED,
        }]
        timestamp = now()
        async with self.connections() as db:
            rows = (await db.execute(select(TurnRow).where(
                TurnRow.state.not_in(terminal),
                (TurnRow.lease_expires_at.is_(None)) | (TurnRow.lease_expires_at <= timestamp),
            ).order_by(TurnRow.created_at))).scalars().all()
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
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or TurnState(row.state) in {
                TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED,
            }:
                return None
            if (row.lease_owner and row.lease_owner != owner and row.lease_expires_at
                    and utc_datetime(row.lease_expires_at) > timestamp):
                return None
            if row.lease_owner != owner:
                row.recovery_generation += 1
            row.lease_owner = owner
            row.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
            row.updated_at = timestamp
        return self.turn_view(row)

    async def renew_turn_lease(self, turn_id: str, owner: str, generation: int, lease_seconds: int) -> bool:
        timestamp = now()
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                return False
            row.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
            row.updated_at = timestamp
        return True

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

    async def start_runtime_execution(self, turn_id: str, execution_id: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                raise RuntimeError("stale_turn_lease")
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
            rows = (await db.execute(query.order_by(TurnRow.created_at))).scalars().all()
        return [self.turn_view(row) for row in rows]

    async def get_turn_for_owner(self, turn_id: str, owner_id: str) -> TurnView | None:
        async with self.connections() as db:
            row = (await db.execute(select(TurnRow).join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(
                TurnRow.id == turn_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
        return self.turn_view(row) if row else None

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
            if turn.state not in accepting:
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
            row.status = "delivered"; row.delivery_mode = "deferred_safe_point"; row.updated_at = now()
        return self.input_view(row)

    async def set_input_status(self, input_id: str, status: str, delivery_mode: str | None = None) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnInputRow, input_id, with_for_update=True)
            if row:
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
                    item.status = {"applied":"completed", "failed":"failed"}.get(status, "in_progress")
                    item.updated_at = now()

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
                TurnInputRow.status.in_(["pending", "delivered"]),
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

    async def set_turn_state(self, turn_id: str, state: TurnState, *, sandbox_id: str | None = None, error_code: str = "", error_detail: str = "", lease_owner: str | None = None, recovery_generation: int | None = None) -> TurnView:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row:
                raise KeyError("turn_not_found")
            if lease_owner is not None and (
                row.lease_owner != lease_owner or row.recovery_generation != recovery_generation
            ):
                raise RuntimeError("stale_turn_lease")
            row.state = state.value
            if sandbox_id is not None:
                row.sandbox_id = sandbox_id
            row.error_code = error_code
            row.error_detail = error_detail[:4000]
            if state in {TurnState.WAITING_USER, TurnState.COMPLETED, TurnState.FAILED, TurnState.CANCELLED}:
                row.pending_final_state = ""
            row.updated_at = now()
        return self.turn_view(row)

    async def begin_turn_cleanup(self, turn_id: str, final_state: TurnState, *, error_code: str,
                                 error_detail: str, owner: str, generation: int) -> TurnView:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row or row.lease_owner != owner or row.recovery_generation != generation:
                raise RuntimeError("stale_turn_lease")
            row.state = TurnState.CLEANING.value
            row.pending_final_state = final_state.value
            row.error_code = error_code
            row.error_detail = error_detail[:4000]
            row.updated_at = now()
        return self.turn_view(row)

    async def update_thread_checkpoint(self, thread_id: str, checkpoint_ref: str, native_thread_id: str) -> None:
        async with self.connections.begin() as db:
            row = await db.get(ThreadRow, thread_id, with_for_update=True)
            if not row:
                raise KeyError("thread_not_found")
            row.checkpoint_ref = checkpoint_ref
            row.native_thread_id = native_thread_id
            row.updated_at = now()

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

    async def add_artifact(self, turn_id: str, *, path: str, sha256: str, size: int, mime: str, object_ref: str) -> Artifact:
        value = Artifact(id=identifier(), turn_id=turn_id, path=path, sha256=sha256, size=size, mime=mime, object_ref=object_ref, created_at=now())
        async with self.connections.begin() as db:
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
                                    object_ref: str) -> tuple[ProjectFile, bool]:
        if not valid_project_file_path(path, source_type):
            raise ValueError("Invalid managed project path")
        async with self.connections.begin() as db:
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
            if row.status == "responded" and row.response_key == response_key:
                return self.interaction_view(row), False
            if row.status == "responding":
                lease_active = bool(row.response_expires_at
                    and utc_datetime(row.response_expires_at) > timestamp)
                if lease_active:
                    raise RuntimeError("interaction_response_in_progress")
                row.response_key = response_key
                row.response_json = json.dumps(response, ensure_ascii=False)
                row.response_owner = claim_owner
                row.response_expires_at = timestamp + timedelta(seconds=lease_seconds)
                row.updated_at = timestamp
                return self.interaction_view(row), True
            if row.status != "pending":
                raise RuntimeError("interaction_not_pending")
            turn = await db.get(TurnRow, row.turn_id, with_for_update=True)
            if (not turn or turn.runtime_execution_id != row.runtime_execution_id
                    or TurnState(turn.state) not in {TurnState.RUNNING, TurnState.AWAITING_USER}):
                row.status = "expired"; row.updated_at = timestamp
                expired = True
            else:
                row.status = "responding"; row.response_key = response_key
                row.response_json = json.dumps(response, ensure_ascii=False)
                row.response_owner = claim_owner
                row.response_expires_at = timestamp + timedelta(seconds=lease_seconds)
                row.updated_at = timestamp
        if expired:
            raise RuntimeError("interaction_expired")
        return self.interaction_view(row), True

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
