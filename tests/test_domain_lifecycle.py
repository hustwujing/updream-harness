from __future__ import annotations

import pytest

from sandbox_harness.common.db.domain_database import DomainDatabase


async def lifecycle(tmp_path, *, refuse=False):
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant
    databases = {name: DomainDatabase(name, "sqlite+aiosqlite:///" + str(tmp_path / (name + ".db")))
                 for name in ("core", "media", "sites")}
    for database in databases.values():
        await database.initialize()
    applied = []

    async def check(session, proposal):
        return "active_work" if refuse else ""

    async def effect(session, proposal):
        # Test observation only; real handlers change only rows in this session.
        applied.append((session.info["domain"], proposal.action))

    participants = {name: LifecycleParticipant(database, check if name == "media" else None, effect)
                    for name, database in databases.items()}
    coordinator = LifecycleCoordinator(participants["core"])
    return databases, participants, coordinator, applied


async def cleanup(databases):
    for database in databases.values():
        await database.close()


async def test_commit_recovery_after_target_effect_and_lost_ack(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleProposal
    databases, participants, coordinator, applied = await lifecycle(tmp_path)
    proposal = LifecycleProposal("operation", "owner", "thread", 0, "archive", "archived")
    try:
        result = await coordinator.start(proposal, {"media", "sites"})
        assert result.state == "pending"
        with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
            async with databases["core"].transaction() as session:
                await participants["core"].admit(session, "owner", "thread", generation=0)
        destinations = {name: part.handoff for name, part in participants.items() if name != "core"}
        await coordinator.handoff.relay(destinations)
        assert (await coordinator.advance("operation")).state == "pending"
        assert applied == [("core", "archive")]
        # Simulate a committed participant receipt whose reply never reached core.
        pending = await coordinator.pending_commands("operation")
        for command in pending:
            await destinations[command.target].receive(command)
        coordinator = LifecycleCoordinator(participants["core"])
        await coordinator.handoff.relay(destinations)
        assert (await coordinator.advance("operation")).state == "completed"
        assert sorted(applied) == [("core", "archive"), ("media", "archive"), ("sites", "archive")]
        for name, part in participants.items():
            async with databases[name].transaction() as session:
                with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
                    await part.admit(session, "owner", "thread", generation=0)
        assert (await coordinator.start(proposal, {"media", "sites"})).state == "completed"
    finally:
        await cleanup(databases)


async def test_known_refusal_aborts_all_barriers_before_final_refusal(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    databases, participants, coordinator, applied = await lifecycle(tmp_path, refuse=True)
    try:
        proposal = LifecycleProposal("operation", "owner", "thread", 0, "delete", "deleted")
        await coordinator.start(proposal, {"media", "sites"})
        destinations = {name: part.handoff for name, part in participants.items() if name != "core"}
        await coordinator.handoff.relay(destinations)
        assert (await coordinator.advance("operation")).state == "pending"
        # It cannot return a final 409 while a remote barrier still needs abort.
        assert (await coordinator.status("operation")).state == "pending"
        await coordinator.handoff.relay(destinations)
        result = await coordinator.advance("operation")
        assert result.state == "refused" and result.error_code == "active_work"
        assert applied == []
        for name, part in participants.items():
            async with databases[name].transaction() as session:
                await part.admit(session, "owner", "thread", generation=0)
    finally:
        await cleanup(databases)


async def test_unknown_delivery_stays_pending_and_conflicting_identity_is_rejected(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    databases, participants, coordinator, applied = await lifecycle(tmp_path)
    try:
        proposal = LifecycleProposal("operation", "owner", "thread", 0, "archive", "archived")
        await coordinator.start(proposal, {"media", "sites"})
        await coordinator.handoff.relay({"sites": participants["sites"].handoff})
        assert (await coordinator.advance("operation")).state == "pending"
        assert applied == []
        with pytest.raises(ValueError, match="lifecycle_identity_conflict"):
            await coordinator.start(LifecycleProposal("operation", "owner", "thread", 0, "delete", "deleted"),
                                    {"media", "sites"})
    finally:
        await cleanup(databases)


async def test_owner_barrier_blocks_new_threads_and_epoch_fences_reopen(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    databases, participants, coordinator, _ = await lifecycle(tmp_path)
    try:
        proposal = LifecycleProposal("owner-operation", "owner", "", 0, "register", "open")
        await coordinator.start(proposal, {"media", "sites"})
        async with databases["core"].transaction() as session:
            with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
                await participants["core"].admit(session, "owner", "brand-new-thread", generation=0)
        destinations = {name: part.handoff for name, part in participants.items() if name != "core"}
        await coordinator.handoff.relay(destinations)
        await coordinator.advance("owner-operation")
        async with databases["core"].transaction() as session:
            with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
                await participants["core"].admit(session, "owner", "another-thread", generation=0)
        await coordinator.handoff.relay(destinations)
        assert (await coordinator.advance("owner-operation")).state == "completed"
        async with databases["core"].transaction() as session:
            with pytest.raises(RuntimeError, match="lifecycle_generation_stale"):
                await participants["core"].admit(session, "owner", "", generation=0)
            await participants["core"].admit(session, "owner", "", generation=1)
            with pytest.raises(RuntimeError, match="lifecycle_owner_generation_stale"):
                await participants["core"].admit(session, "owner", "old-thread",
                                                 generation=0, owner_generation=0)
            await participants["core"].admit(session, "owner", "old-thread",
                                             generation=0, owner_generation=1)
    finally:
        await cleanup(databases)


@pytest.mark.parametrize("action,target", [("unknown", None), ([], "open"), ("register", 1)])
async def test_invalid_proposal_cannot_create_a_barrier(tmp_path, action, target):
    from sqlalchemy import func, select
    from sandbox_harness.common.db.domain_database import DomainBarrier
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    databases, _, coordinator, _ = await lifecycle(tmp_path)
    try:
        with pytest.raises(ValueError, match="lifecycle_proposal_invalid"):
            await coordinator.start(LifecycleProposal("operation", "owner", "thread", 0, action, target),
                                    {"media", "sites"})
        async with databases["core"].transaction() as session:
            assert await session.scalar(select(func.count()).select_from(DomainBarrier)) == 0
    finally:
        await cleanup(databases)


async def test_committed_cleanup_decision_stays_pending_across_restart_until_confirmed(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleProposal
    databases, participants, coordinator, applied = await lifecycle(tmp_path)
    ready = False
    requested = []

    async def queue_cleanup(session, proposal):
        requested.append(proposal.operation_id)

    async def cleanup_confirmed(session, proposal):
        return ready

    participants["core"].prepare_commit = queue_cleanup
    participants["core"].commit_ready = cleanup_confirmed
    proposal = LifecycleProposal("cleanup", "owner", "thread", 0, "delete", "deleted")
    targets = {name: part.handoff for name, part in participants.items() if name != "core"}
    try:
        await coordinator.start(proposal, set(targets))
        await coordinator.handoff.relay(targets)
        assert (await coordinator.advance("cleanup")).state == "pending"
        assert requested == ["cleanup"] and applied == []
        await coordinator.handoff.relay(targets)
        coordinator = LifecycleCoordinator(participants["core"])
        assert (await coordinator.advance("cleanup")).state == "pending"
        assert ("core", "delete") not in applied
        async with databases["core"].transaction() as session:
            with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
                await participants["core"].admit(session, "owner", "thread", generation=0)
        ready = True
        assert (await coordinator.advance("cleanup")).state == "completed"
        assert (await coordinator.advance("cleanup")).state == "completed"
        assert requested == ["cleanup"]
        assert applied.count(("core", "delete")) == 1
    finally:
        await cleanup(databases)
