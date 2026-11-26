"""Fresh domain schemas and single-authority transactions.

There is no routing Session: a session can touch only one domain. Connections
used by schema administration are deliberately separate from business sessions.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, event, inspect, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.schema import Table

from .models import Base, SCHEMA_INITIALIZATION_LOCK_ID, canonical_digest
from .account_models import AccountBase
from .ownership_models import OwnershipBase
from .media_models import MediaAuthorityBase


DOMAIN_TABLES = {
    "accounts": frozenset({"users", "auth_sessions", "registration_intents", "account_registrations"}),
    "core": frozenset({
        "artifacts", "authorization_snapshots", "budget_accounts", "budget_ledger",
        "budget_reservations", "capability_ledger", "child_job_continuations", "events",
        "interactions", "items", "job_activities", "job_artifacts", "job_attempts",
        "job_cleanups", "job_display_activities", "job_group_drafts", "job_groups",
        "job_leases", "job_outbox", "jobs", "model_token_reservations", "nodes",
        "pause_participants", "project_files", "resource_reservations", "review_gates",
        "platform_leaders",
        "sandbox_allocations", "sandbox_audit_events", "sandbox_controls", "sandbox_port_grants",
        "thread_controls", "threads", "turn_inputs", "turn_order", "turn_project_files",
        "turns", "uploads",
        "usage_ledger",
        "sandbox_ownership",
        "remote_obligations",
    }),
    "media": frozenset({
        "media_batches", "media_drafts", "media_job_executions", "media_job_outbox",
        "media_job_outputs", "media_jobs", "media_reference_assets", "media_reference_uploads",
        "media_voice_clones",
        "media_source_permits",
    }),
    "sites": frozenset({
        "site_projects", "site_drafts", "site_draft_files", "site_deploy_jobs",
        "site_versions", "site_files",
    }),
}


class ProtocolBase(DeclarativeBase):
    pass


class DomainIdentity(ProtocolBase):
    __tablename__ = "domain_identity"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    domain: Mapped[str] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(Integer)
    schema_digest: Mapped[str] = mapped_column(String(64))


class DomainStream(ProtocolBase):
    __tablename__ = "domain_streams"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, default=0)


class DomainOutbox(ProtocolBase):
    __tablename__ = "domain_outbox"
    __table_args__ = (UniqueConstraint("stream_id", "sequence"),)
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    target: Mapped[str] = mapped_column(String(32))
    stream_id: Mapped[str] = mapped_column(String(64), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    envelope_json: Mapped[str] = mapped_column(Text)
    digest: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(24), index=True, default="pending")
    receipt_json: Mapped[str] = mapped_column(Text, default="")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str] = mapped_column(String(80), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DomainInbox(ProtocolBase):
    __tablename__ = "domain_inbox"
    __table_args__ = (UniqueConstraint("stream_id", "sequence"),)
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    stream_id: Mapped[str] = mapped_column(String(64), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    digest: Mapped[str] = mapped_column(String(64))
    receipt_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DomainScope(ProtocolBase):
    __tablename__ = "domain_scopes"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32))
    generation: Mapped[int] = mapped_column(Integer, default=0)
    state: Mapped[str] = mapped_column(String(24), default="open")
    frozen_by: Mapped[str] = mapped_column(String(64), default="")


class DomainBarrier(ProtocolBase):
    __tablename__ = "domain_barriers"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    proposal_json: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(24))
    error_code: Mapped[str] = mapped_column(String(80), default="")


class DomainLifecycle(ProtocolBase):
    __tablename__ = "domain_lifecycles"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    proposal_json: Mapped[str] = mapped_column(Text)
    participants_json: Mapped[str] = mapped_column(Text)
    phase: Mapped[str] = mapped_column(String(24), index=True)
    error_code: Mapped[str] = mapped_column(String(80), default="")
    requester: Mapped[str] = mapped_column(String(32), default="")


PROTOCOL_TABLES = frozenset(ProtocolBase.metadata.tables)


class OwnedSession(Session):
    """Reject cross-authority ORM and textual SQL before it reaches the driver."""


@event.listens_for(OwnedSession, "do_orm_execute")
def _check_statement(state):
    owned = state.session.info["owned_tables"]
    for node in visitors.iterate(state.statement):
        if isinstance(node, TextClause):
            raise ValueError("database_raw_sql_forbidden")
        if isinstance(node, Table) and node.name not in owned:
            raise ValueError("database_table_not_owned")


@event.listens_for(OwnedSession, "before_flush")
def _check_models(session, flush_context, instances):
    owned = session.info["owned_tables"]
    for model in session.new | session.dirty | session.deleted:
        if inspect(type(model)).local_table.name not in owned:
            raise ValueError("database_table_not_owned")


def _type_shape(value, dialect):
    result = {"type": value._type_affinity.__name__}
    for name in ("length", "precision", "scale", "timezone", "collation"):
        if name == "timezone" and dialect == "sqlite":
            continue
        if getattr(value, name, None) is not None:
            result[name] = getattr(value, name)
    return result


def _table_shape(table, dialect):
    return {
        "columns": {
            c.name: (_type_shape(c.type, dialect), bool(c.nullable), bool(c.primary_key))
            for c in table.columns
        },
        "indexes": sorted((i.name, tuple(c.name for c in i.columns), bool(i.unique)) for i in table.indexes),
        "unique": sorted(tuple(c.name for c in constraint.columns)
                         for constraint in table.constraints if isinstance(constraint, UniqueConstraint)),
    }


class DomainDatabase:
    schema_version = 1

    def __init__(self, domain: str, url: str):
        if domain not in DOMAIN_TABLES:
            raise ValueError("database_domain_invalid")
        self.domain, self.url = domain, url
        self.owned_tables = DOMAIN_TABLES[domain] | PROTOCOL_TABLES
        self.tables = {name: table for metadata in (Base.metadata, AccountBase.metadata,
                       OwnershipBase.metadata, MediaAuthorityBase.metadata)
                       for name, table in metadata.tables.items() if name in DOMAIN_TABLES[domain]}
        self.tables.update(ProtocolBase.metadata.tables)
        self.engine = create_async_engine(url)
        self.shape = {name: _table_shape(table, self.engine.dialect.name) for name, table in self.tables.items()}
        self.schema_digest = canonical_digest(self.shape)
        self.connections = async_sessionmaker(self.engine, expire_on_commit=False,
            sync_session_class=OwnedSession,
            info={"owned_tables": self.owned_tables, "domain": domain})

    def _check_shape(self, connection):
        inspector = inspect(connection)
        if set(inspector.get_table_names()) != self.owned_tables:
            raise RuntimeError("database_schema_mismatch")
        for name, expected in self.shape.items():
            primary = set(inspector.get_pk_constraint(name)["constrained_columns"])
            columns = {c["name"]: (_type_shape(c["type"], connection.dialect.name), bool(c["nullable"]),
                                   c["name"] in primary) for c in inspector.get_columns(name)}
            indexes = sorted((i["name"], tuple(i["column_names"]), bool(i["unique"]))
                for i in inspector.get_indexes(name) if not i.get("duplicates_constraint"))
            unique = sorted(tuple(c["column_names"]) for c in inspector.get_unique_constraints(name))
            if {"columns": columns, "indexes": indexes, "unique": unique} != expected:
                raise RuntimeError("database_schema_mismatch")

    async def initialize(self):
        async with self.engine.begin() as connection:
            if connection.dialect.name == "postgresql":
                await connection.execute(text("SELECT pg_advisory_xact_lock(:id)"),
                                         {"id": SCHEMA_INITIALIZATION_LOCK_ID})
            elif connection.dialect.name == "sqlite":
                await connection.exec_driver_sql("BEGIN IMMEDIATE")
            existing = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
            if existing:
                if DomainIdentity.__tablename__ not in existing:
                    raise RuntimeError("database_unversioned")
                identity = (await connection.execute(select(DomainIdentity.__table__))).mappings().all()
                if len(identity) != 1 or identity[0]["domain"] != self.domain:
                    raise RuntimeError("database_domain_mismatch")
                if (identity[0]["version"] != self.schema_version
                        or identity[0]["schema_digest"] != self.schema_digest):
                    raise RuntimeError("database_schema_mismatch")
                await connection.run_sync(self._check_shape)
                return
            for metadata in (Base.metadata, AccountBase.metadata, OwnershipBase.metadata,
                             MediaAuthorityBase.metadata):
                await connection.run_sync(lambda c, metadata=metadata: metadata.create_all(c,
                    tables=[table for name, table in metadata.tables.items() if name in DOMAIN_TABLES[self.domain]]))
            await connection.run_sync(ProtocolBase.metadata.create_all)
            await connection.execute(DomainIdentity.__table__.insert().values(
                id=1, domain=self.domain, version=self.schema_version, schema_digest=self.schema_digest))
            await connection.run_sync(self._check_shape)

    @asynccontextmanager
    async def transaction(self):
        async with self.connections() as session:
            async with session.begin():
                if self.engine.dialect.name == "sqlite":
                    connection = await session.connection()
                    await connection.exec_driver_sql("BEGIN IMMEDIATE")
                yield session

    async def close(self):
        await self.engine.dispose()

    async def healthcheck(self):
        async with self.connections() as session:
            await session.execute(select(1))
        return True
