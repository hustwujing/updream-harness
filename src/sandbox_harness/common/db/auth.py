"""Auth persistence; transaction boundaries are preserved."""
from __future__ import annotations
import hashlib
from datetime import (datetime, timedelta)
from sqlalchemy import (delete, select, update)
from sqlalchemy.exc import (IntegrityError)
from sandbox_harness.common.contracts.domain import (AccountView, ThreadControlState, TERMINAL, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (TERMINAL_JOB_STATES)
from .models import (AuthSessionRow, JobRow, MediaJobRow, MediaReferenceUploadRow, OWNER_MIGRATION_ROWS, RegistrationIntentRow, SandboxControlRow, SiteDeployJobRow, ThreadControlRow, ThreadRow, TurnRow, UserRow, utc_datetime)


from .accounts import AccountMethods


class AuthTransactions(AccountMethods):
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
                if source_owner_id:
                    db.add(RegistrationIntentRow(source_owner_id=source_owner_id,
                        username=username, user_id=row.id,
                        expires_at=timestamp + timedelta(hours=24), created_at=timestamp))
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

