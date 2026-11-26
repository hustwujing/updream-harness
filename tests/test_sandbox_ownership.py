from datetime import timedelta

import pytest
from sqlalchemy import update

from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, now
from sandbox_harness.common.contracts.sandbox_contracts import SandboxCreateRequest
from sandbox_harness.common.sandbox.sandbox import FakeSandboxBackend


async def setup_owned(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    from sandbox_harness.common.db.sandbox_ownership import SandboxOwnership
    core = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="ownership-test")
    await core.initialize()
    profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
    thread = await core.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model, title="ownership"),
                                      profile, "owner")
    backend = FakeSandboxBackend(namespace="ownership-test")
    request = SandboxCreateRequest(harness=Harness.CODEX, profile=backend.legacy_profile(Harness.CODEX, "image"),
        environment={}, ttl_seconds=300, allocation_id="allocation")
    ledger = SandboxOwnership(core, backend.namespace)
    return core, ledger, backend, thread, request


async def queue_cleanup(tmp_path, core, ledger, thread):
    from sandbox_harness.common.db.domain_database import DomainDatabase
    from sandbox_harness.common.db.lifecycle import LifecycleParticipant, LifecycleProposal
    remotes = {name: DomainDatabase(name, "sqlite+aiosqlite:///" + str(tmp_path / (name + ".db")))
               for name in ("media", "sites")}
    core.lifecycle.prepare_commit = ledger.request_cleanup
    core.lifecycle.commit_ready = ledger.cleanup_ready
    proposal = LifecycleProposal("cleanup", "owner", thread.id, 0, "delete", "deleted")
    try:
        for database in remotes.values():
            await database.initialize()
        targets = {name: LifecycleParticipant(database).handoff for name, database in remotes.items()}
        await core.coordinator.start(proposal, set(targets))
        await core.handoff.relay(targets)
        assert (await core.coordinator.advance("cleanup")).state == "pending"
        return proposal
    finally:
        for database in remotes.values():
            await database.close()


async def test_creation_intent_survives_restart_without_releasing_unknown_capacity(tmp_path):
    from sandbox_harness.common.db.sandbox_ownership import SandboxOwnership
    core, ledger, _, thread, request = await setup_owned(tmp_path)
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        ledger = SandboxOwnership(core, "ownership-test")
        assert (await ledger.get(identity))["state"] == "CREATING"
        with pytest.raises(RuntimeError, match="sandbox_creation_requires_reconciliation"):
            await ledger.begin_create("owner", thread.id, request)
        capacity = await ledger.capacity_obligations()
        assert len(capacity) == 1 and capacity[0]["allocation_id"] == request.allocation_id
        assert (await ledger.get(identity))["handle"] is None
    finally:
        await core.close()


async def test_late_create_receipt_remains_owned_after_lifecycle_freezes_thread(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    core, ledger, backend, thread, request = await setup_owned(tmp_path)
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        proposal = await queue_cleanup(tmp_path, core, ledger, thread)
        sandbox = await backend.create(request)
        await ledger.record_handle(identity, sandbox.handle)
        with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
            await ledger.bind(identity, "owner", thread.id, owner_generation=0, thread_generation=0)
        claim = (await ledger.claim_cleanup("cleaner"))[0]
        assert claim["handle"]["sandbox_id"] == sandbox.id
        assert claim["operation_id"] == "cleanup"
        assert len(await ledger.capacity_obligations()) == 1
    finally:
        await core.close()


async def test_cleanup_claim_fences_absence_and_separate_capacity_release(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
    core, ledger, backend, thread, request = await setup_owned(tmp_path)
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        sandbox = await backend.create(request)
        await ledger.record_handle(identity, sandbox.handle)
        await ledger.bind(identity, "owner", thread.id, owner_generation=0, thread_generation=0)
        proposal = await queue_cleanup(tmp_path, core, ledger, thread)
        first = (await ledger.claim_cleanup("old"))[0]
        async with core.transaction() as session:
            await session.execute(update(SandboxOwnershipRow).where(SandboxOwnershipRow.id == identity)
                .values(claim_expires_at=now() - timedelta(seconds=1)))
        second = (await ledger.claim_cleanup("replacement"))[0]
        with pytest.raises(RuntimeError, match="stale_sandbox_cleanup_claim"):
            await ledger.confirm_absent(identity, "old", first["claim_generation"])
        await backend.delete(sandbox)
        await ledger.confirm_absent(identity, "replacement", second["claim_generation"])
        assert (await ledger.get(identity))["state"] == "ABSENT"
        assert len(await ledger.capacity_obligations()) == 1
        await ledger.confirm_capacity_released(identity, "replacement", second["claim_generation"])
        assert (await ledger.get(identity))["state"] == "RELEASED"
        assert await ledger.capacity_obligations() == []
        async with core.transaction() as session:
            assert await ledger.cleanup_ready(session, proposal)
    finally:
        await core.close()


async def test_unknown_creation_cannot_be_marked_absent_without_a_known_handle(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    core, ledger, _, thread, request = await setup_owned(tmp_path)
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        proposal = await queue_cleanup(tmp_path, core, ledger, thread)
        claim = (await ledger.claim_cleanup("cleaner"))[0]
        with pytest.raises(RuntimeError, match="sandbox_absence_not_proven"):
            await ledger.confirm_absent(identity, "cleaner", claim["claim_generation"])
        assert len(await ledger.capacity_obligations()) == 1
    finally:
        await core.close()


async def test_cleanup_worker_keeps_unknown_creation_pending_without_creating_again(tmp_path):
    from sandbox_harness.common.sandbox.ownership_cleanup import OwnershipCleanup
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, request = await setup_owned(tmp_path)
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness}, max_sandboxes=1)
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        await queue_cleanup(tmp_path, core, ledger, thread)
        await pool.restore_allocation_capacity(await ledger.capacity_obligations())
        result = await OwnershipCleanup(ledger, backend, pool).run_once()
        assert result == {"released": 0, "pending": 1}
        assert not backend.items and pool._user_slots._value == 0
        assert (await ledger.get(identity))["state"] == "DELETE_PENDING"
    finally:
        await pool.close()
        await core.close()


@pytest.mark.parametrize("failure_boundary", ["absence_receipt", "capacity_release"])
async def test_cleanup_worker_recovers_after_delete_or_absence_commit(tmp_path, monkeypatch, failure_boundary):
    from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
    from sandbox_harness.common.sandbox.ownership_cleanup import OwnershipCleanup
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, request = await setup_owned(tmp_path)
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness}, max_sandboxes=1)
    replacement = None
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        sandbox = await backend.create(request)
        await ledger.record_handle(identity, sandbox.handle)
        await queue_cleanup(tmp_path, core, ledger, thread)
        await pool.restore_allocation_capacity(await ledger.capacity_obligations())
        original = ledger.confirm_absent

        async def fail(*args, **kwargs):
            raise ConnectionError("injected")

        if failure_boundary == "absence_receipt":
            monkeypatch.setattr(ledger, "confirm_absent", fail)
        else:
            monkeypatch.setattr(pool, "forget_absent_allocation", fail)
        assert await OwnershipCleanup(ledger, backend, pool).run_once() == {"released": 0, "pending": 1}
        assert sandbox.id not in backend.items
        assert len(await ledger.capacity_obligations()) == 1
        if failure_boundary == "capacity_release":
            assert (await ledger.get(identity))["state"] == "ABSENT"

            async def no_more_provider_calls(*args):
                raise AssertionError("confirmed absence must skip provider requests")

            monkeypatch.setattr(backend, "get", no_more_provider_calls)
            monkeypatch.setattr(backend, "delete", no_more_provider_calls)
        monkeypatch.setattr(ledger, "confirm_absent", original)
        async with core.transaction() as session:
            await session.execute(update(SandboxOwnershipRow).where(SandboxOwnershipRow.id == identity)
                                  .values(next_attempt_at=None))
        replacement = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness}, max_sandboxes=1)
        await replacement.restore_allocation_capacity(await ledger.capacity_obligations())
        assert replacement._user_slots._value == 0
        assert await OwnershipCleanup(ledger, backend, replacement).run_once() == {"released": 1, "pending": 0}
        assert replacement._user_slots._value == 1 and await ledger.capacity_obligations() == []
        assert await OwnershipCleanup(ledger, backend, replacement).run_once() == {"released": 0, "pending": 0}
    finally:
        await pool.close()
        if replacement:
            await replacement.close()
        await core.close()


async def test_cleanup_of_known_handle_does_not_require_runtime_health(tmp_path, monkeypatch):
    from sandbox_harness.common.contracts.sandbox_contracts import SandboxEndpointUnavailable
    from sandbox_harness.common.sandbox.ownership_cleanup import OwnershipCleanup
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, request = await setup_owned(tmp_path)
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness}, max_sandboxes=1)
    try:
        identity = await ledger.begin_create("owner", thread.id, request)
        sandbox = await backend.create(request)
        await ledger.record_handle(identity, sandbox.handle)
        await queue_cleanup(tmp_path, core, ledger, thread)
        await pool.restore_allocation_capacity(await ledger.capacity_obligations())

        async def unavailable(*args):
            raise SandboxEndpointUnavailable("injected Runtime unavailable", provider=backend.provider)

        monkeypatch.setattr(backend, "get", unavailable)
        assert await OwnershipCleanup(ledger, backend, pool).run_once() == {"released": 1, "pending": 0}
        assert sandbox.id not in backend.items and await ledger.capacity_obligations() == []
    finally:
        await pool.close()
        await core.close()


async def test_registration_carries_retained_ownership_forward_but_rejects_old_binding_epoch(tmp_path):
    from sandbox_harness.common.db.accounts import AccountsRepository
    from sandbox_harness.common.db.domain_database import DomainDatabase
    from sandbox_harness.common.db.lifecycle import LifecycleParticipant
    core, ledger, backend, thread, request = await setup_owned(tmp_path)
    accounts = AccountsRepository("sqlite+aiosqlite:///" + str(tmp_path / "accounts.db"))
    remotes = {name: DomainDatabase(name, "sqlite+aiosqlite:///" + str(tmp_path / (name + ".db")))
               for name in ("media", "sites")}
    try:
        for database in [accounts, *remotes.values()]:
            await database.initialize()
        identity = await ledger.begin_create("owner", thread.id, request)
        sandbox = await backend.create(request)
        await ledger.record_handle(identity, sandbox.handle)
        await ledger.bind(identity, "owner", thread.id, owner_generation=0, thread_generation=0)
        await accounts.begin_registration("register", "owner", "existing.sandbox", "test-password-hash",
            session_token_hash="session-hash", session_expires_at=now() + timedelta(hours=1), owner_generation=0)
        await accounts.handoff.relay({"core": core.handoff})
        targets = {name: LifecycleParticipant(database).handoff for name, database in remotes.items()}
        targets["accounts"] = accounts.handoff
        await core.handoff.relay(targets)
        await core.coordinator.advance("register")
        await core.handoff.relay(targets)
        assert (await core.coordinator.advance("register")).state == "completed"
        # A newly authorized bind uses the new owner epoch. Old callers must not
        # infer permission from the retained allocation having the same owner ID.
        with pytest.raises(RuntimeError, match="lifecycle_owner_generation_stale"):
            await ledger.bind(identity, "owner", thread.id, owner_generation=0, thread_generation=0)
        assert await ledger.bind(identity, "owner", thread.id, owner_generation=1, thread_generation=0) == sandbox.handle
    finally:
        for database in [core, accounts, *remotes.values()]:
            await database.close()
