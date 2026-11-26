"""Core-owned Thread sandbox cleanup obligations, independent of provider metadata."""
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class OwnershipBase(DeclarativeBase):
    pass


class SandboxOwnershipRow(OwnershipBase):
    __tablename__ = "sandbox_ownership"
    __table_args__ = (UniqueConstraint("namespace", "provider", "allocation_id"),)
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(64), index=True)
    provider: Mapped[str] = mapped_column(String(64))
    allocation_id: Mapped[str] = mapped_column(String(160))
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    profile_key: Mapped[str] = mapped_column(String(160))
    request_json: Mapped[str] = mapped_column(Text)
    request_digest: Mapped[str] = mapped_column(String(64))
    handle_json: Mapped[str] = mapped_column(Text, default="")
    owner_generation: Mapped[int] = mapped_column(Integer)
    thread_generation: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(24), index=True)
    operation_id: Mapped[str] = mapped_column(String(64), index=True, default="")
    claim_owner: Mapped[str] = mapped_column(String(64), default="")
    claim_generation: Mapped[int] = mapped_column(Integer, default=0)
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    retained_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str] = mapped_column(String(80), default="")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    absent_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    capacity_released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class RemoteObligationRow(OwnershipBase):
    __tablename__ = "remote_obligations"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    target: Mapped[str] = mapped_column(String(32))
    kind: Mapped[str] = mapped_column(String(80))
    state: Mapped[str] = mapped_column(String(24), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
