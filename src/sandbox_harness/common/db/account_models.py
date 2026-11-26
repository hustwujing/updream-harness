"""Account authority records for durable registration orchestration."""
from datetime import datetime

from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class AccountBase(DeclarativeBase):
    pass


class AccountRegistration(AccountBase):
    __tablename__ = "account_registrations"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    username: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(32))
    password_hash: Mapped[str] = mapped_column(Text)
    session_token_hash: Mapped[str] = mapped_column(String(64))
    session_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    request_digest: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(24), index=True)
    error_code: Mapped[str] = mapped_column(String(80), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
