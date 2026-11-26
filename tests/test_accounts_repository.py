from datetime import timedelta

import pytest
from sqlalchemy import inspect

from sandbox_harness.common.contracts.domain import now


async def test_accounts_authentication_uses_only_account_database(tmp_path):
    from sandbox_harness.common.db.accounts import AccountsRepository
    accounts = AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    try:
        await accounts.initialize()
        account = await accounts.create_user("independent.user", "test-password-hash")
        await accounts.create_auth_session("test-session-hash", account.id, now() + timedelta(hours=1))
        assert (await accounts.resolve_auth_session("test-session-hash")).id == account.id
        await accounts.set_user_disabled(account.id, True)
        assert await accounts.resolve_auth_session("test-session-hash") is None
        async with accounts.engine.connect() as connection:
            tables = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
        assert "users" in tables and not {"threads", "jobs", "media_jobs", "site_projects"}.intersection(tables)
        assert not hasattr(accounts, "register_user")
    finally:
        await accounts.close()


async def test_registration_keeps_owner_and_revokes_old_guest_after_durable_commit(tmp_path):
    from sandbox_harness.common.db.accounts import AccountsRepository
    from sandbox_harness.common.db.domain_database import DomainDatabase
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant
    accounts = AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    others = {name: DomainDatabase(name, "sqlite+aiosqlite:///" + str(tmp_path / (name + ".db")))
              for name in ("core", "media", "sites")}
    try:
        for database in [accounts, *others.values()]:
            await database.initialize()
        participants = {name: LifecycleParticipant(database) for name, database in others.items()}
        coordinator = LifecycleCoordinator(participants["core"])
        assert await accounts.guest_owner_available("original-owner")
        await accounts.begin_registration("registration", "original-owner", "registered.user", "test-password-hash",
            session_token_hash="session-hash", session_expires_at=now() + timedelta(hours=1), owner_generation=0)
        # Username reservation also applies to administrative account creation.
        with pytest.raises(RuntimeError, match="username_taken"):
            await accounts.create_user("registered.user", "other-password-hash")
        await accounts.handoff.relay({"core": coordinator.handoff})
        assert (await coordinator.status("registration")).state == "pending"
        destinations = {name: p.handoff for name, p in participants.items() if name != "core"}
        destinations["accounts"] = accounts.handoff
        await coordinator.handoff.relay(destinations)
        await coordinator.advance("registration")
        await coordinator.handoff.relay(destinations)
        await coordinator.advance("registration")
        await coordinator.handoff.relay(destinations)
        result = await accounts.registration_result("registration", "original-owner")
        assert result["state"] == "completed"
        account = await accounts.get_user(result["user_id"])
        assert account.owner_id == "original-owner"
        assert not await accounts.guest_owner_available("original-owner")
        assert (await accounts.resolve_auth_session("session-hash")).id == account.id
        await accounts.delete_auth_session("session-hash")
        assert not await accounts.guest_owner_available("original-owner")
        assert (await accounts.registration_replay_user("original-owner", "registered.user")).id == account.id
        assert await accounts.registration_replay_user("different-owner", "registered.user") is None
    finally:
        for database in [accounts, *others.values()]:
            await database.close()


async def test_registration_refusal_releases_staged_account_and_username(tmp_path):
    from sandbox_harness.common.db.accounts import AccountsRepository
    from sandbox_harness.common.db.domain_database import DomainDatabase
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant
    accounts = AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    core = DomainDatabase("core", "sqlite+aiosqlite:///" + str(tmp_path / "core.db"))

    async def refuse(session, proposal):
        return "active_work_prevents_registration"

    try:
        await accounts.initialize()
        await core.initialize()
        coordinator = LifecycleCoordinator(LifecycleParticipant(core, refuse))
        await accounts.begin_registration("refused", "guest", "refused.user", "test-password-hash",
            session_token_hash="session-hash", session_expires_at=now() + timedelta(hours=1), owner_generation=0)
        await accounts.handoff.relay({"core": coordinator.handoff})
        await coordinator.handoff.relay({"accounts": accounts.handoff})
        result = await accounts.registration_result("refused", "guest")
        assert result["state"] == "refused"
        assert result["error_code"] == "active_work_prevents_registration"
        assert await accounts.guest_owner_available("guest")
        assert await accounts.get_user_by_username("refused.user") is None
        assert await accounts.create_user("refused.user", "other-password-hash")
    finally:
        await accounts.close()
        await core.close()


async def test_persisted_registration_replays_after_frozen_session_expiry(tmp_path, monkeypatch):
    from sandbox_harness.common.db import accounts as module
    accounts = module.AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    current = now()
    arguments = dict(session_token_hash="session-hash", session_expires_at=current + timedelta(hours=1),
                     owner_generation=0)
    try:
        await accounts.initialize()
        await accounts.begin_registration("persisted", "owner", "pending.user", "test-password-hash", **arguments)
        monkeypatch.setattr(module, "now", lambda: current + timedelta(hours=2))
        assert await accounts.begin_registration("persisted", "owner", "pending.user", "test-password-hash",
                                                 **arguments) == "persisted"
    finally:
        await accounts.close()


async def test_refused_operation_replays_while_new_registration_reserves_username(tmp_path):
    from sandbox_harness.common.db.accounts import AccountsRepository
    from sandbox_harness.common.db.domain_database import DomainDatabase
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant
    accounts = AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    core = DomainDatabase("core", "sqlite+aiosqlite:///" + str(tmp_path / "core.db"))

    async def refuse(session, proposal):
        return "active_work_prevents_registration"

    arguments = dict(session_token_hash="session-hash", session_expires_at=now() + timedelta(hours=1),
                     owner_generation=0)
    try:
        await accounts.initialize()
        await core.initialize()
        coordinator = LifecycleCoordinator(LifecycleParticipant(core, refuse))
        await accounts.begin_registration("first", "owner", "same.user", "test-password-hash", **arguments)
        await accounts.handoff.relay({"core": coordinator.handoff})
        await coordinator.handoff.relay({"accounts": accounts.handoff})
        await accounts.begin_registration("second", "another-owner", "same.user", "test-password-hash", **arguments)
        assert await accounts.begin_registration("first", "owner", "same.user", "test-password-hash",
                                                 **arguments) == "first"
    finally:
        await accounts.close()
        await core.close()


async def test_prepared_account_aborts_after_remote_refusal_and_lost_receipts(tmp_path):
    from sandbox_harness.common.db.accounts import AccountsRepository
    from sandbox_harness.common.db.domain_database import DomainDatabase
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant
    accounts = AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    domains = {name: DomainDatabase(name, "sqlite+aiosqlite:///" + str(tmp_path / (name + ".db")))
               for name in ("core", "media", "sites")}

    async def refuse(session, proposal):
        return "active_work_prevents_registration"

    try:
        for database in [accounts, *domains.values()]:
            await database.initialize()
        parts = {name: LifecycleParticipant(database, refuse if name == "media" else None)
                 for name, database in domains.items()}
        coordinator = LifecycleCoordinator(parts["core"])
        targets = {name: part.handoff for name, part in parts.items() if name != "core"}
        targets["accounts"] = accounts.handoff
        await accounts.begin_registration("abort", "owner", "aborted.user", "test-password-hash",
            session_token_hash="session-hash", session_expires_at=now() + timedelta(hours=1), owner_generation=0)
        await accounts.handoff.relay({"core": coordinator.handoff})
        await coordinator.handoff.relay(targets)
        assert (await coordinator.advance("abort")).state == "pending"
        for command in await coordinator.pending_commands("abort"):
            await targets[command.target].receive(command)  # lose every abort reply
        assert (await accounts.registration_result("abort", "owner"))["state"] == "pending"
        await coordinator.handoff.relay(targets)
        assert (await coordinator.advance("abort")).state == "refused"
        await coordinator.handoff.relay(targets)
        assert (await accounts.registration_result("abort", "owner"))["state"] == "refused"
        assert await accounts.guest_owner_available("owner")
        assert await accounts.resolve_auth_session("session-hash") is None
        assert await accounts.create_user("aborted.user", "other-password-hash")
    finally:
        for database in [accounts, *domains.values()]:
            await database.close()
