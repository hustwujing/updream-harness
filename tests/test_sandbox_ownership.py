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


async def test_pool_persists_create_handle_and_binding_before_return(tmp_path):
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, _ = await setup_owned(tmp_path)
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                       max_sandboxes=1, ownership=ledger)
    try:
        sandbox = await pool.acquire(thread_id=thread.id, turn_id="turn", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        row = await ledger.get(ledger.identity(sandbox.handle.provider, sandbox.handle.allocation_id))
        assert row["state"] == "LIVE" and row["handle"]["sandbox_id"] == sandbox.id
        assert row["owner_id"] == "owner" and row["thread_id"] == thread.id
        await pool.release(sandbox, keep_attached=False)
        assert (await ledger.get(row["id"]))["state"] == "RELEASED"
        assert pool._user_slots._value == 1
    finally:
        await pool.close()
        await core.close()


async def test_pool_persists_retained_deadline_and_rebind_cancels_expiry(tmp_path):
    from sandbox_harness.common.db.models import utc_datetime
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, _ = await setup_owned(tmp_path)
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                       max_sandboxes=1, ownership=ledger)
    try:
        sandbox = await pool.acquire(thread_id=thread.id, turn_id="first", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        identity = ledger.identity(sandbox.handle.provider, sandbox.handle.allocation_id)
        before = now()
        await pool.release(sandbox, keep_attached=True, keepalive_seconds=60)
        retained = await ledger.get(identity)
        assert retained["state"] == "RETAINED"
        assert utc_datetime(retained["retained_until"]) >= before + timedelta(seconds=59)
        assert thread.id not in pool._expiry
        assert (sandbox.handle.provider,sandbox.handle.allocation_id) in await core.active_sandbox_allocations()
        assert (sandbox.handle.provider,sandbox.id) in await core.active_sandbox_keys()

        reused = await pool.acquire(thread_id=thread.id, turn_id="second", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        assert reused is sandbox
        rebound = await ledger.get(identity)
        assert rebound["state"] == "LIVE" and rebound["retained_until"] is None
        await pool.release(reused, keep_attached=False)
    finally:
        await pool.close()
        await core.close()


async def test_startup_orphan_cleanup_preserves_durable_retained_ownership(tmp_path):
    from sandbox_harness.common.execution.turn_broker import Broker
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core,ledger,backend,thread,_=await setup_owned(tmp_path)
    old=SandboxPool(backend,{h:'image' for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1,ownership=ledger);replacement=None
    try:
        sandbox=await old.acquire(thread_id=thread.id,turn_id='first',harness=Harness.CODEX,
            environment={},ownership_owner_id='owner')
        await old.release(sandbox,keep_attached=True,keepalive_seconds=60)
        await old.close(preserve_sandbox_keys={old.sandbox_key(sandbox)})

        replacement=SandboxPool(backend,{h:'image' for h in Harness},{h:0 for h in Harness},
            max_sandboxes=1,ownership=ledger)
        await replacement.restore_allocation_capacity(await ledger.capacity_obligations())
        broker=Broker.__new__(Broker);broker.pool=replacement;broker.store=core
        broker.active_sandboxes={}
        await broker.cleanup_startup_orphans()

        assert sandbox.id in backend.items
        assert (await ledger.get(ledger.identity(
            sandbox.handle.provider,sandbox.handle.allocation_id)))['state']=='RETAINED'
    finally:
        if not old._closed:await old.close()
        if replacement:await replacement.close(preserve_sandbox_keys={
            replacement.sandbox_key(sandbox)} if 'sandbox' in locals() else set())
        await core.close()


async def test_missing_retained_provider_sandbox_converges_to_fresh_turn_allocation(tmp_path):
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core,ledger,backend,thread,_=await setup_owned(tmp_path)
    old=SandboxPool(backend,{h:'image' for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1,ownership=ledger);replacement=None
    try:
        sandbox=await old.acquire(thread_id=thread.id,turn_id='first',harness=Harness.CODEX,
            environment={},ownership_owner_id='owner')
        old_identity=ledger.identity(sandbox.handle.provider,sandbox.handle.allocation_id)
        await old.release(sandbox,keep_attached=True,keepalive_seconds=60)
        await old.close(preserve_sandbox_keys={old.sandbox_key(sandbox)})
        await backend.delete(sandbox)

        replacement=SandboxPool(backend,{h:'image' for h in Harness},{h:0 for h in Harness},
            max_sandboxes=1,ownership=ledger)
        await replacement.restore_allocation_capacity(await ledger.capacity_obligations())
        fresh=await replacement.acquire(thread_id=thread.id,turn_id='second',
            harness=Harness.CODEX,environment={},ownership_owner_id='owner')

        assert fresh.id!=sandbox.id and fresh.handle.allocation_id=='second'
        assert (await ledger.get(old_identity))['state']=='RELEASED'
        fresh_identity=ledger.identity(fresh.handle.provider,fresh.handle.allocation_id)
        assert (await ledger.get(fresh_identity))['state']=='LIVE'
        await replacement.release(fresh,keep_attached=False)
    finally:
        if not old._closed:await old.close()
        if replacement:await replacement.close()
        await core.close()


@pytest.mark.parametrize("failure_boundary", ["none", "capacity_release"])
async def test_replacement_worker_reaps_expired_retained_sandbox(
        tmp_path, monkeypatch, failure_boundary):
    from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
    from sandbox_harness.common.sandbox.ownership_cleanup import OwnershipCleanup
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, _ = await setup_owned(tmp_path)
    old = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                      max_sandboxes=1, ownership=ledger)
    replacement = None
    try:
        sandbox = await old.acquire(thread_id=thread.id, turn_id="first", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        identity = ledger.identity(sandbox.handle.provider, sandbox.handle.allocation_id)
        await old.release(sandbox, keep_attached=True, keepalive_seconds=60)
        await old.close(preserve_sandbox_keys={old.sandbox_key(sandbox)})
        async with core.transaction() as session:
            await session.execute(update(SandboxOwnershipRow).where(SandboxOwnershipRow.id == identity)
                .values(retained_until=now() - timedelta(seconds=1)))

        replacement = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                                  max_sandboxes=1, ownership=ledger)
        await replacement.restore_allocation_capacity(await ledger.capacity_obligations())
        cleanup = OwnershipCleanup(ledger, backend, replacement)
        if failure_boundary == "capacity_release":
            original = replacement.forget_absent_allocation

            async def fail_once(*args):
                monkeypatch.setattr(replacement, "forget_absent_allocation", original)
                raise ConnectionError("injected")

            monkeypatch.setattr(replacement, "forget_absent_allocation", fail_once)
            assert await cleanup.run_retained_once() == {"released": 0, "pending": 1}
            assert sandbox.id not in backend.items
            assert (await ledger.get(identity))["state"] == "ABSENT"
            async with core.transaction() as session:
                await session.execute(update(SandboxOwnershipRow).where(SandboxOwnershipRow.id == identity)
                    .values(next_attempt_at=None, claim_expires_at=now() - timedelta(seconds=1)))

        assert await cleanup.run_retained_once() == {"released": 1, "pending": 0}
        assert sandbox.id not in backend.items
        assert (await ledger.get(identity))["state"] == "RELEASED"
        assert await ledger.capacity_obligations() == []
        assert replacement._user_slots._value == 1
    finally:
        if not old._closed:
            await old.close(preserve_sandbox_keys={old.sandbox_key(sandbox)} if 'sandbox' in locals() else set())
        if replacement:
            await replacement.close()
        await core.close()


async def test_expired_retention_claim_fences_same_thread_rebind(tmp_path):
    from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, _ = await setup_owned(tmp_path)
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                       max_sandboxes=1, ownership=ledger)
    try:
        sandbox = await pool.acquire(thread_id=thread.id, turn_id="first", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        identity = ledger.identity(sandbox.handle.provider, sandbox.handle.allocation_id)
        await pool.release(sandbox, keep_attached=True, keepalive_seconds=60)
        async with core.transaction() as session:
            await session.execute(update(SandboxOwnershipRow).where(SandboxOwnershipRow.id == identity)
                .values(retained_until=now() - timedelta(seconds=1)))
        claim = (await ledger.claim_retained_cleanup("cleaner"))[0]
        with pytest.raises(RuntimeError, match="sandbox_retained_cleanup_claimed"):
            await pool.acquire(thread_id=thread.id, turn_id="second", harness=Harness.CODEX,
                environment={}, ownership_owner_id="owner")
        await backend.delete(sandbox)
        await ledger.confirm_absent(identity, "cleaner", claim["claim_generation"])
        await pool.forget_absent_allocation(sandbox.handle.provider, sandbox.handle.allocation_id)
        await ledger.confirm_capacity_released(identity, "cleaner", claim["claim_generation"])
    finally:
        await pool.close(preserve_sandbox_keys={pool.sandbox_key(sandbox)} if 'sandbox' in locals() else set())
        await core.close()


async def test_pool_warm_adoption_becomes_owned_before_thread_binding(tmp_path):
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, thread, _ = await setup_owned(tmp_path)
    targets = {h: (1 if h == Harness.CODEX else 0) for h in Harness}
    pool = SandboxPool(backend, {h: "image" for h in Harness}, targets,
                       max_sandboxes=2, ownership=ledger)
    try:
        await pool.fill()
        sandbox = await pool.acquire(thread_id=thread.id, turn_id="turn", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        row = await ledger.get(ledger.identity(sandbox.handle.provider, sandbox.handle.allocation_id))
        assert row["state"] == "LIVE" and sandbox.thread_id == thread.id
        await pool.release(sandbox, keep_attached=False)
    finally:
        await pool.close()
        await core.close()


async def test_pool_releases_rejected_create_but_retains_unknown_outcome(tmp_path):
    from sandbox_harness.common.contracts.sandbox_contracts import (
        SandboxCapacityUnavailable, SandboxCreateOutcomeUnknown)
    from sandbox_harness.common.sandbox.pool import SandboxPool

    class Rejecting(FakeSandboxBackend):
        failure = SandboxCapacityUnavailable("full", provider="fake")

        async def create(self, request):
            if self.failure:
                raise self.failure
            return await super().create(request)

    core, ledger, _, thread, _ = await setup_owned(tmp_path)
    backend = Rejecting(namespace="ownership-test")
    pool = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                       max_sandboxes=1, ownership=ledger)
    try:
        with pytest.raises(SandboxCapacityUnavailable):
            await pool.acquire(thread_id=thread.id, turn_id="rejected", harness=Harness.CODEX,
                environment={}, ownership_owner_id="owner")
        assert await ledger.capacity_obligations() == [] and pool._user_slots._value == 1
        backend.failure = None
        recovered = await pool.acquire(thread_id=thread.id, turn_id="rejected", harness=Harness.CODEX,
            environment={}, ownership_owner_id="owner")
        assert recovered.handle.allocation_id == "rejected"
        await pool.release(recovered, keep_attached=False)
        backend.failure = SandboxCreateOutcomeUnknown("lost", provider="fake")
        with pytest.raises(SandboxCreateOutcomeUnknown):
            await pool.acquire(thread_id=thread.id, turn_id="unknown", harness=Harness.CODEX,
                environment={}, ownership_owner_id="owner")
        obligations = await ledger.capacity_obligations()
        assert len(obligations) == 1 and obligations[0]["allocation_id"] == "unknown"
        assert pool._user_slots._value == 0
    finally:
        await pool.close()
        await core.close()


async def test_replacement_pool_reconciles_lost_create_reply_without_resubmit(tmp_path):
    from sandbox_harness.common.contracts.sandbox_contracts import SandboxCreateOutcomeUnknown
    from sandbox_harness.common.sandbox.pool import SandboxPool

    class LostReply(FakeSandboxBackend):
        creates = 0

        async def create(self, request):
            self.creates += 1
            sandbox = await super().create(request)
            if self.creates == 1:
                raise SandboxCreateOutcomeUnknown("lost", provider=self.provider)
            return sandbox

    core, ledger, _, thread, _ = await setup_owned(tmp_path)
    backend = LostReply(namespace="ownership-test")
    old = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                      max_sandboxes=1, ownership=ledger)
    replacement = None
    try:
        with pytest.raises(SandboxCreateOutcomeUnknown):
            await old.acquire(thread_id=thread.id, turn_id="lost-reply", harness=Harness.CODEX,
                environment={}, ownership_owner_id="owner")
        obligations = await ledger.capacity_obligations()
        assert backend.creates == 1 and len(obligations) == 1
        replacement = SandboxPool(backend, {h: "image" for h in Harness}, {h: 0 for h in Harness},
                                  max_sandboxes=1, ownership=ledger)
        await replacement.restore_allocation_capacity(obligations)
        sandbox = await replacement.acquire(thread_id=thread.id, turn_id="lost-reply",
            harness=Harness.CODEX, environment={}, ownership_owner_id="owner")
        assert backend.creates == 1 and sandbox.handle.allocation_id == "lost-reply"
        record = await ledger.get(ledger.identity(backend.provider, "lost-reply"))
        assert record["state"] == "LIVE" and record["handle"]["sandbox_id"] == sandbox.id
        await replacement.release(sandbox, keep_attached=False)
    finally:
        await old.close(preserve_sandbox_keys=set(await old.managed_sandbox_keys()))
        if replacement:
            await replacement.close()
        await core.close()


async def test_database_capacity_release_wakes_another_process_pool(tmp_path):
    import asyncio
    from sandbox_harness.common.catalog.catalog import HarnessCatalog
    from sandbox_harness.common.contracts.domain import ThreadCreate
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core, ledger, backend, first_thread, _ = await setup_owned(tmp_path)
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    second_thread=await core.create_thread(ThreadCreate(
        harness=Harness.CODEX,model=profile.model,title="second"),profile,"owner")
    first=SandboxPool(backend,{h:"image" for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1,ownership=ledger)
    second=SandboxPool(backend,{h:"image" for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1,ownership=ledger)
    try:
        occupied=await first.acquire(thread_id=first_thread.id,turn_id="first",harness=Harness.CODEX,
            environment={},ownership_owner_id="owner")
        waiting=asyncio.create_task(second.acquire(thread_id=second_thread.id,turn_id="second",
            harness=Harness.CODEX,environment={},ownership_owner_id="owner"))
        await asyncio.sleep(.1)
        assert not waiting.done(),"a second process exceeded database-authoritative capacity"
        await first.release(occupied,keep_attached=False)
        admitted=await asyncio.wait_for(waiting,3)
        assert admitted.thread_id==second_thread.id
        assert len(await ledger.capacity_obligations())==1
        await second.release(admitted,keep_attached=False)
    finally:
        await first.close();await second.close();await core.close()


async def test_replacement_pool_resumes_owned_sandbox_without_local_capacity_snapshot(tmp_path):
    from sandbox_harness.common.sandbox.pool import SandboxPool
    core,ledger,backend,thread,_=await setup_owned(tmp_path)
    first=SandboxPool(backend,{h:"image" for h in Harness},{h:0 for h in Harness},
        max_sandboxes=1,ownership=ledger)
    replacement=None
    try:
        sandbox=await first.acquire(thread_id=thread.id,turn_id="first",harness=Harness.CODEX,
            environment={},ownership_owner_id="owner")
        await first.release(sandbox,keep_attached=True,keepalive_seconds=60)
        await first.close(preserve_sandbox_keys={first.sandbox_key(sandbox)})
        replacement=SandboxPool(backend,{h:"image" for h in Harness},{h:0 for h in Harness},
            max_sandboxes=1,ownership=ledger)
        resumed=await replacement.acquire(thread_id=thread.id,turn_id="second",harness=Harness.CODEX,
            environment={},ownership_owner_id="owner")
        assert resumed.handle.allocation_id==sandbox.handle.allocation_id
        await replacement.release(resumed,keep_attached=False)
    finally:
        if not first._closed:await first.close()
        if replacement:await replacement.close()
        await core.close()
