from __future__ import annotations

import json
from datetime import datetime, timedelta

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, inspect, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .domain import Artifact, Event, EventKind, Harness, Item, ItemType, ThreadCreate, ThreadView, TurnCreate, TurnInputCreate, TurnInputView, TurnState, TurnView, identifier, now
from .catalog import ResolvedProfile
from .events import RedisEventBus


class Base(DeclarativeBase):
    pass


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


class Store:
    def __init__(self, database_url: str, redis_url: str = "", redis_prefix: str = "sandbox-harness:"):
        self.engine = create_async_engine(database_url)
        self.connections = async_sessionmaker(self.engine, expire_on_commit=False)
        self.events = RedisEventBus(redis_url, redis_prefix) if redis_url else None

    async def initialize(self) -> None:
        if self.events:
            await self.events.initialize()
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("threads")})
            if "owner_id" not in columns:
                await connection.execute(text("ALTER TABLE threads ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT 'legacy-unowned'"))
            await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_threads_owner_id ON threads (owner_id)"))
            input_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("turn_inputs")})
            if "delivery_mode" not in input_columns:
                await connection.execute(text("ALTER TABLE turn_inputs ADD COLUMN delivery_mode VARCHAR(30) NOT NULL DEFAULT 'pending'"))
            turn_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("turns")})
            additions = {
                "lease_owner": "VARCHAR(64) NOT NULL DEFAULT ''",
                "lease_expires_at": "TIMESTAMP NULL",
                "recovery_generation": "INTEGER NOT NULL DEFAULT 0",
                "runtime_sequence": "INTEGER NOT NULL DEFAULT 0",
                "runtime_execution_id": "VARCHAR(160) NOT NULL DEFAULT ''",
                "capability_nonce": "VARCHAR(64) NOT NULL DEFAULT ''",
                "pending_final_state": "VARCHAR(24) NOT NULL DEFAULT ''",
            }
            for name, definition in additions.items():
                if name not in turn_columns:
                    await connection.execute(text(f"ALTER TABLE turns ADD COLUMN {name} {definition}"))
            event_columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("events")})
            if "source_execution_id" not in event_columns:
                await connection.execute(text("ALTER TABLE events ADD COLUMN source_execution_id VARCHAR(160) NOT NULL DEFAULT ''"))
            if "source_sequence" not in event_columns:
                await connection.execute(text("ALTER TABLE events ADD COLUMN source_sequence INTEGER NOT NULL DEFAULT 0"))

    async def close(self) -> None:
        if self.events:
            await self.events.close()
        await self.engine.dispose()

    @staticmethod
    def thread_view(row: ThreadRow) -> ThreadView:
        return ThreadView.model_validate({key: getattr(row, key) for key in ThreadView.model_fields})

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

    async def get_thread(self, thread_id: str, owner_id: str | None = None) -> ThreadView | None:
        async with self.connections() as db:
            query = select(ThreadRow).where(ThreadRow.id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
        return self.thread_view(row) if row else None

    async def create_turn(self, thread_id: str, value: TurnCreate, resolved: ResolvedProfile, owner_id: str) -> tuple[TurnView, bool, bool]:
        async with self.connections.begin() as db:
            thread = await db.get(ThreadRow, thread_id, with_for_update=True)
            if not thread or thread.owner_id != owner_id:
                raise KeyError("thread_not_found")
            existing = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id, TurnRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
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
            last_item = (await db.execute(select(ItemRow.sequence).where(ItemRow.thread_id == thread_id).order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            db.add(ItemRow(id=identifier(), thread_id=thread_id, turn_id=row.id, sequence=last_item + 1,
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
            if row.lease_owner and row.lease_owner != owner and row.lease_expires_at and row.lease_expires_at > timestamp:
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
        accepting = {TurnState.QUEUED.value, TurnState.RESTORING.value, TurnState.STARTING.value, TurnState.RUNNING.value}
        async with self.connections.begin() as db:
            turn = (await db.execute(select(TurnRow).join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(
                TurnRow.id == turn_id, ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not turn:
                raise KeyError("turn_not_found")
            existing = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
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
            item_sequence = (await db.execute(select(ItemRow.sequence).where(ItemRow.thread_id == turn.thread_id)
                .order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            db.add(ItemRow(id=identifier(), thread_id=turn.thread_id, turn_id=turn_id, sequence=item_sequence + 1,
                type=ItemType.USER_MESSAGE.value, status="completed", summary=value.message,
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

    async def apply_delivered_inputs(self, turn_id: str) -> None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.status == "delivered").with_for_update())).scalars().all()
            for row in rows:
                row.status = "applied"; row.updated_at = now()

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
                           source_execution_id: str = "", source_sequence: int = 0) -> Event:
        async with self.connections.begin() as db:
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

    async def events_after(self, turn_id: str, sequence: int) -> list[Event]:
        async with self.connections() as db:
            rows = (await db.execute(select(EventRow).where(EventRow.turn_id == turn_id, EventRow.sequence > sequence).order_by(EventRow.sequence))).scalars().all()
        return [Event(turn_id=row.turn_id, sequence=row.sequence, kind=EventKind(row.kind), item_id=row.item_id, payload=json.loads(row.payload_json), created_at=row.created_at) for row in rows]

    async def append_item(self, thread_id: str, turn_id: str, item_type: ItemType, *, status: str, summary: str = "", payload: dict | None = None) -> Item:
        timestamp = now()
        async with self.connections.begin() as db:
            last = (await db.execute(select(ItemRow.sequence).where(ItemRow.thread_id == thread_id).order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            row = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=last + 1,
                type=item_type.value, status=status, summary=summary, payload_json=json.dumps(payload or {}, ensure_ascii=False),
                created_at=timestamp, updated_at=timestamp)
            db.add(row)
        return Item(id=row.id, thread_id=thread_id, turn_id=turn_id, sequence=row.sequence, type=item_type,
            status=status, summary=summary, payload=payload or {}, created_at=timestamp, updated_at=timestamp)

    async def append_agent_delta(self, thread_id: str, turn_id: str, text: str) -> Item:
        async with self.connections.begin() as db:
            row = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                .order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
            if not row:
                last = (await db.execute(select(ItemRow.sequence).where(ItemRow.thread_id == thread_id).order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
                row = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=last + 1,
                    type=ItemType.AGENT_MESSAGE.value, status="in_progress", summary="", payload_json="{}", created_at=now(), updated_at=now())
                db.add(row)
            row.summary += text
            row.updated_at = now()
        return Item(id=row.id, thread_id=thread_id, turn_id=turn_id, sequence=row.sequence, type=ItemType.AGENT_MESSAGE,
            status=row.status, summary=row.summary, payload=json.loads(row.payload_json), created_at=row.created_at, updated_at=row.updated_at)

    async def complete_agent_item(self, turn_id: str, status: str) -> None:
        async with self.connections.begin() as db:
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
