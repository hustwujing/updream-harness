"""Account-only authentication transactions and independent repository."""
from __future__ import annotations
import hashlib
from dataclasses import asdict
from datetime import (datetime, timedelta)
from sqlalchemy import delete, select
from sqlalchemy.exc import (IntegrityError)
from sandbox_harness.common.contracts.domain import AccountView, identifier, now
from .models import AuthSessionRow, RegistrationIntentRow, UserRow, utc_datetime, canonical_digest
from .account_models import AccountRegistration
from .lifecycle import LifecycleParticipant, LifecycleProposal


from .domain_database import DomainDatabase


class AccountMethods:
    def account_transaction(self):
        return self.connections.begin()

    async def _username_reservation(self, session, username, *, operation_id=""):
        """Single-authority stores override this with their durable reservation."""

    @staticmethod
    def account_view(row: UserRow) -> AccountView:
        return AccountView.model_validate({key: getattr(row, key) for key in AccountView.model_fields})


    async def create_user(self, username: str, password_hash: str) -> AccountView:
        timestamp = now();identity = identifier()
        row = UserRow(id=identity, owner_id=hashlib.sha256(("account:" + identity).encode()).hexdigest(),
            username=username, password_hash=password_hash, created_at=timestamp, updated_at=timestamp)
        try:
            async with self.account_transaction() as db:
                await self._username_reservation(db, username)
                if (await db.execute(select(UserRow.id).where(UserRow.username == username))).scalar_one_or_none():
                    raise RuntimeError("username_taken")
                db.add(row)
        except IntegrityError as exc:
            raise RuntimeError("username_taken") from exc
        return self.account_view(row)


    async def registration_replay_user(self, source_owner_id: str,
                                       username: str) -> AccountView | None:
        if not source_owner_id:
            return None
        async with self.account_transaction() as db:
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
        async with self.account_transaction() as db:
            if not await db.get(UserRow, user_id):
                raise KeyError("user_not_found")
            db.add(AuthSessionRow(token_hash=token_hash, user_id=user_id, expires_at=expires_at,
                created_at=timestamp, last_seen_at=timestamp))


    async def resolve_auth_session(self, token_hash: str) -> AccountView | None:
        timestamp = now()
        async with self.account_transaction() as db:
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
        async with self.account_transaction() as db:
            await db.execute(delete(AuthSessionRow).where(AuthSessionRow.token_hash == token_hash))


    async def update_username(self, user_id: str, username: str) -> AccountView:
        try:
            async with self.account_transaction() as db:
                await self._username_reservation(db, username)
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
        async with self.account_transaction() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.password_hash = password_hash;row.updated_at = now()
            query = delete(AuthSessionRow).where(AuthSessionRow.user_id == user_id)
            if keep_token_hash:query = query.where(AuthSessionRow.token_hash != keep_token_hash)
            await db.execute(query)


    async def rehash_password(self, user_id: str, password_hash: str) -> None:
        async with self.account_transaction() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.password_hash = password_hash
            row.updated_at = now()


    async def update_account(self, user_id: str, *, username: str | None = None,
                             password_hash: str | None = None, keep_token_hash: str = "") -> AccountView:
        try:
            async with self.account_transaction() as db:
                if username is not None:
                    await self._username_reservation(db, username)
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
        async with self.account_transaction() as db:
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
        async with self.account_transaction() as db:
            row = await db.get(UserRow, user_id, with_for_update=True)
            if not row: raise KeyError("user_not_found")
            row.disabled = disabled;row.updated_at = now()
            if disabled:await db.execute(delete(AuthSessionRow).where(AuthSessionRow.user_id == user_id))
        return self.account_view(row)


    async def list_users(self) -> list[AccountView]:
        async with self.connections() as db:
            rows = (await db.execute(select(UserRow).order_by(UserRow.created_at))).scalars().all()
        return [self.account_view(row) for row in rows]



class AccountsRepository(DomainDatabase, AccountMethods):
    def __init__(self, database_url: str, *, handoff_max_attempts: int = 8,
                 handoff_max_age_seconds: int = 900):
        super().__init__("accounts", database_url)
        self.lifecycle = LifecycleParticipant(self, self._registration_check, self._registration_commit,
            self._registration_abort, handoff_max_attempts=handoff_max_attempts,
            handoff_max_age_seconds=handoff_max_age_seconds)
        self.handoff = self.lifecycle.handoff
        self.handoff.handlers["lifecycle.result"] = self._registration_finished

    def account_transaction(self):
        return self.transaction()

    async def _username_reservation(self, session, username, *, operation_id=""):
        await self.handoff._identity_lock(session, "account-username", username)
        pending = await session.scalar(select(AccountRegistration.id).where(
            AccountRegistration.username == username,
            AccountRegistration.state.in_(["waiting", "committed"]),
            AccountRegistration.id != operation_id).limit(1))
        if pending:
            raise RuntimeError("username_taken")

    async def guest_owner_available(self, owner_id: str) -> bool:
        # Even a disabled account retains the principal claim. An invalid login
        # must never fall back to the old signed anonymous credential.
        async with self.connections() as session:
            return await session.scalar(select(UserRow.id).where(UserRow.owner_id == owner_id)) is None

    async def begin_registration(self, operation_id, owner_id, username, password_hash, *,
                                 session_token_hash, session_expires_at, owner_generation):
        proposal = LifecycleProposal(operation_id, owner_id, "", owner_generation, "register", "open",
                                     owner_generation=owner_generation)
        proposal.validate()
        if not password_hash or not session_token_hash:
            raise ValueError("registration_credentials_invalid")
        digest = canonical_digest({"proposal": asdict(proposal), "username": username,
            "password_hash": password_hash, "session_token_hash": session_token_hash,
            "expires_at": utc_datetime(session_expires_at).isoformat()})
        async with self.transaction() as session:
            await self.handoff._identity_lock(session, "account-registration", operation_id)
            existing = await session.get(AccountRegistration, operation_id)
            if existing:
                if existing.request_digest != digest:
                    raise ValueError("registration_identity_conflict")
                return existing.id
            if utc_datetime(session_expires_at) <= now():
                raise ValueError("registration_credentials_invalid")
            await self._username_reservation(session, username, operation_id=operation_id)
            await self.handoff._identity_lock(session, "account-owner", owner_id)
            if await session.scalar(select(UserRow.id).where(UserRow.username == username)):
                raise RuntimeError("username_taken")
            if await session.scalar(select(UserRow.id).where(UserRow.owner_id == owner_id)):
                raise RuntimeError("principal_already_claimed")
            if await session.scalar(select(AccountRegistration.id).where(
                    AccountRegistration.owner_id == owner_id,
                    AccountRegistration.state.in_(["waiting", "committed"])).limit(1)):
                raise RuntimeError("registration_in_progress")
            session.add(AccountRegistration(id=operation_id, owner_id=owner_id, username=username,
                user_id=identifier(), password_hash=password_hash, session_token_hash=session_token_hash,
                session_expires_at=session_expires_at, request_digest=digest, state="waiting",
                error_code="", created_at=now()))
            await self.handoff.enqueue(session, command_id=canonical_digest(["registration", operation_id]),
                target="core", kind="lifecycle.request", aggregate_id=proposal.scope_id,
                owner_id=owner_id, generation=owner_generation, payload=asdict(proposal))
        return operation_id

    async def _registration_check(self, session, proposal):
        if proposal.action != "register":
            return "lifecycle_action_unsupported"
        row = await session.get(AccountRegistration, proposal.operation_id)
        if row is None or row.owner_id != proposal.owner_id or row.state != "waiting":
            return "registration_request_missing"
        await self._username_reservation(session, row.username, operation_id=row.id)
        if await session.scalar(select(UserRow.id).where(UserRow.username == row.username)):
            return "username_taken"
        if await session.scalar(select(UserRow.id).where(UserRow.owner_id == row.owner_id)):
            return "principal_already_claimed"
        return ""

    async def _registration_commit(self, session, proposal):
        row = await session.get(AccountRegistration, proposal.operation_id, with_for_update=True)
        if row is None or row.owner_id != proposal.owner_id or row.state != "waiting":
            raise ValueError("registration_state_conflict")
        timestamp = now()
        session.add(UserRow(id=row.user_id, owner_id=row.owner_id, username=row.username,
            password_hash=row.password_hash, created_at=timestamp, updated_at=timestamp))
        session.add(AuthSessionRow(token_hash=row.session_token_hash, user_id=row.user_id,
            expires_at=row.session_expires_at, created_at=timestamp, last_seen_at=timestamp))
        session.add(RegistrationIntentRow(source_owner_id=row.owner_id, username=row.username,
            user_id=row.user_id, expires_at=timestamp + timedelta(hours=24), created_at=timestamp))
        row.state = "committed"

    async def _registration_abort(self, session, proposal):
        row = await session.get(AccountRegistration, proposal.operation_id, with_for_update=True)
        if row is not None and row.owner_id == proposal.owner_id:
            if row.state != "waiting":
                raise ValueError("registration_state_conflict")
            row.state = "aborted"
            row.password_hash, row.session_token_hash = "", ""

    async def _registration_finished(self, session, envelope):
        result = envelope.payload
        row = await session.get(AccountRegistration, result.get("operation_id"), with_for_update=True)
        if (envelope.source != "core" or row is None or row.owner_id != envelope.owner_id
                or envelope.aggregate_id != canonical_digest([row.owner_id, ""])):
            raise ValueError("registration_result_scope_invalid")
        state = result.get("state")
        if state == "completed" and row.state == "committed":
            row.state = "completed"
        elif state == "refused" and row.state in {"waiting", "aborted"}:
            row.state = "refused"
            row.error_code = result.get("error_code", "")
        else:
            raise ValueError("registration_result_conflict")
        row.password_hash, row.session_token_hash = "", ""
        return {"recorded": True}

    async def registration_result(self, operation_id, owner_id):
        async with self.connections() as session:
            row = await session.get(AccountRegistration, operation_id)
            if row is None or row.owner_id != owner_id:
                raise KeyError("registration_not_found")
            return {"operation_id": row.id, "state": row.state if row.state in {"completed", "refused"}
                    else "pending", "error_code": row.error_code,
                    "user_id": row.user_id if row.state == "completed" else ""}
