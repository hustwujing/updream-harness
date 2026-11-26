from sqlalchemy import inspect, select
import pytest

from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, TurnCreate, TurnState
from sandbox_harness.common.db.domain_database import DomainOutbox


async def create_thread(core):
    profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
    thread = await core.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model, title="core"),
                                      profile, "owner")
    return profile, thread


async def test_core_turn_terminalization_persists_media_notification_atomically(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    core = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="ownership-test")
    try:
        await core.initialize()
        profile, thread = await create_thread(core)
        turn, _, _ = await core.create_turn(thread.id, TurnCreate(message="task", idempotency_key="core-turn"),
                                           profile, "owner")
        await core.set_turn_state(turn.id, TurnState.COMPLETED)
        await core.set_turn_state(turn.id, TurnState.COMPLETED)
        async with core.connections() as session:
            notifications = list((await session.scalars(select(DomainOutbox))).all())
            assert len(notifications) == 1 and notifications[0].target == "media"
        assert (await core.get_turn(turn.id)).state == TurnState.COMPLETED
        async with core.engine.connect() as connection:
            tables = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
        assert not {"users", "media_jobs", "site_projects"}.intersection(tables)
    finally:
        await core.close()


async def test_core_registration_refuses_active_turn_and_fences_new_admission(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    core = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="ownership-test")
    try:
        await core.initialize()
        profile, thread = await create_thread(core)
        turn, _, _ = await core.create_turn(thread.id, TurnCreate(message="task", idempotency_key="core-turn"),
                                           profile, "owner")
        result = await core.coordinator.start(LifecycleProposal("busy", "owner", "", 0, "register", "open"),
                                               {"accounts", "media", "sites"})
        assert result.state == "refused" and result.error_code == "active_work_prevents_registration"
        await core.set_turn_state(turn.id, TurnState.COMPLETED)
        result = await core.coordinator.start(LifecycleProposal("idle", "owner", "", 0, "register", "open"),
                                               {"accounts", "media", "sites"})
        assert result.state == "pending"
        with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
            await core.create_turn(thread.id, TurnCreate(message="new", idempotency_key="new-core-turn"), profile, "owner")
        with pytest.raises(RuntimeError, match="lifecycle_admission_blocked"):
            await create_thread(core)
    finally:
        await core.close()


async def test_empty_thread_archive_unarchive_and_delete_preserve_domain_fences(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    from sandbox_harness.common.db.domain_database import DomainDatabase, DomainScope
    from sandbox_harness.common.db.lifecycle import LifecycleParticipant, LifecycleProposal
    core = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="ownership-test")
    remote = {name: DomainDatabase(name, "sqlite+aiosqlite:///" + str(tmp_path / (name + ".db")))
              for name in ("media", "sites")}
    try:
        for database in [core, *remote.values()]:
            await database.initialize()
        _, thread = await create_thread(core)
        targets = {name: LifecycleParticipant(database).handoff for name, database in remote.items()}
        for generation, (action, target_state) in enumerate(
                [("archive", "archived"), ("unarchive", "open"), ("delete", "deleted")]):
            proposal = LifecycleProposal(action, "owner", thread.id, generation, action, target_state)
            await core.coordinator.start(proposal, set(targets))
            await core.handoff.relay(targets)
            await core.coordinator.advance(action)
            await core.handoff.relay(targets)
            assert (await core.coordinator.advance(action)).state == "completed"
            current = await core.get_thread(thread.id, "owner")
            if action == "delete":
                assert current is None
            else:
                assert bool(current.archived_at) == (action == "archive")
        async with core.connections() as session:
            scope = await session.get(DomainScope, proposal.scope_id)
            assert scope.state == "deleted" and scope.generation == 3
    finally:
        for database in [core, *remote.values()]:
            await database.close()


async def test_core_terminal_state_rolls_back_when_notification_cannot_commit(tmp_path, monkeypatch):
    from sandbox_harness.common.db.core import CoreRepository
    core = CoreRepository("sqlite+aiosqlite:///" + str(tmp_path / "core.db"), namespace="ownership-test")
    try:
        await core.initialize()
        profile, thread = await create_thread(core)
        turn, _, _ = await core.create_turn(thread.id, TurnCreate(message="task", idempotency_key="core-turn"),
                                           profile, "owner")
        original = core.handoff.enqueue

        async def fail(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("injected")

        monkeypatch.setattr(core.handoff, "enqueue", fail)
        with pytest.raises(RuntimeError, match="injected"):
            await core.set_turn_state(turn.id, TurnState.COMPLETED)
        assert (await core.get_turn(turn.id)).state == TurnState.QUEUED
        async with core.connections() as session:
            assert list((await session.scalars(select(DomainOutbox))).all()) == []
    finally:
        await core.close()
