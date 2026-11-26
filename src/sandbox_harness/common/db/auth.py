"""Auth persistence; transaction boundaries are preserved."""
from __future__ import annotations
import hashlib
from datetime import (datetime, timedelta)
from sqlalchemy import (delete, select, update)
from sqlalchemy.exc import (IntegrityError)
from sandbox_harness.common.contracts.domain import (AccountView, ThreadControlState, TERMINAL, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (TERMINAL_JOB_STATES)
from .models import (AuthSessionRow, JobRow, MediaJobRow, MediaReferenceUploadRow, OWNER_MIGRATION_ROWS, RegistrationIntentRow, SandboxControlRow, SiteDeployJobRow, ThreadControlRow, ThreadRow, TurnRow, UserRow, utc_datetime)


class AuthTransactions:
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


    async def registration_replay_user(self, source_owner_id: str,
                                       username: str) -> AccountView | None:
        if not source_owner_id:
            return None
        async with self.connections.begin() as db:
            await db.execute(delete(RegistrationIntentRow).where(
                RegistrationIntentRow.expires_at <= now()))
            row = (await db.execute(select(RegistrationIntentRow, UserRow).join(UserRow,
                UserRow.id == RegistrationIntentRow.user_id).where(
                RegistrationIntentRow.source_owner_id == source_owner_id,
                RegistrationIntentRow.username == username,
                RegistrationIntentRow.expires_at > now(),
            ))).first()
            return self.account_view(row[1]) if row else None


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

