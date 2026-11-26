"""Media authority records for source Job authorization."""
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class MediaAuthorityBase(DeclarativeBase):
    pass


class MediaSourcePermitRow(MediaAuthorityBase):
    __tablename__ = "media_source_permits"
    dispatch_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    media_job_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    source_job_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    source_generation: Mapped[int] = mapped_column(Integer)
    request_digest: Mapped[str] = mapped_column(String(64))
    command_digest: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(24), index=True)
    output_contracts_json: Mapped[str] = mapped_column(Text, default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
