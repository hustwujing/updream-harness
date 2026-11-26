"""Run only against explicitly provisioned disposable domain databases."""
import asyncio
import json
import os
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from sandbox_harness.common.contracts.domain import now
from sandbox_harness.common.db.domain_database import DomainDatabase, DomainInbox
from sandbox_harness.common.db.handoff import DomainHandoff
from sandbox_harness.common.db.models import SiteProjectRow


URLS = json.loads(os.environ.get("PARALLEL_TEST_DOMAIN_URLS", "{}"))
pytestmark = pytest.mark.skipif(not URLS, reason="disposable domain PostgreSQL databases not configured")


async def test_domain_schemas_and_duplicate_delivery_have_one_effect():
    databases = [DomainDatabase(domain, url) for domain, url in URLS.items()]
    try:
        await asyncio.gather(*(database.initialize() for database in databases))
        await asyncio.gather(*(database.initialize() for database in databases))
    finally:
        await asyncio.gather(*(database.close() for database in databases))

    sources = [DomainDatabase("core", URLS["core"]) for _ in range(8)]
    targets = [DomainDatabase("sites", URLS["sites"]) for _ in range(8)]
    identity = uuid4().hex

    async def create_site(session, envelope):
        session.add(SiteProjectRow(id=envelope.aggregate_id, owner_id=envelope.owner_id,
            thread_id="thread", slug=envelope.aggregate_id, name="site", created_at=now(), updated_at=now()))
        await session.flush()
        return {"id": envelope.aggregate_id}

    async def enqueue(database):
        sender = DomainHandoff(database, {})
        async with database.transaction() as session:
            return await sender.enqueue(session, command_id=identity, target="sites", kind="create",
                aggregate_id=identity, owner_id="owner", generation=1, payload={})

    try:
        envelopes = await asyncio.gather(*(enqueue(database) for database in sources))
        assert all(envelope == envelopes[0] for envelope in envelopes)
        receivers = [DomainHandoff(database, {"create": create_site}) for database in targets]
        receipts = await asyncio.gather(*(receiver.receive(envelopes[0]) for receiver in receivers))
        assert all(receipt == receipts[0] for receipt in receipts)
        # A replacement process retrieves the committed target receipt; no second effect.
        assert await receivers[0].receive(envelopes[0]) == receipts[0]
        await asyncio.gather(*(DomainHandoff(database, {}).acknowledge(identity, receipts[0])
                              for database in sources))
        async with targets[0].transaction() as session:
            assert await session.scalar(select(func.count()).select_from(SiteProjectRow)
                .where(SiteProjectRow.id == identity)) == 1
            assert await session.scalar(select(func.count()).select_from(DomainInbox)
                .where(DomainInbox.stream_id == envelopes[0].stream_id)) == 1
    finally:
        await asyncio.gather(*(database.close() for database in sources + targets))


async def test_concurrent_commands_share_monotonic_stream():
    stores = [DomainDatabase("core", URLS["core"]) for _ in range(12)]
    aggregate = uuid4().hex

    async def enqueue(index, database):
        async with database.transaction() as session:
            return await DomainHandoff(database, {}).enqueue(session, command_id=aggregate + "-" + str(index),
                target="media", kind="update", aggregate_id=aggregate, owner_id="owner", generation=1,
                payload={"index": index})

    try:
        envelopes = await asyncio.gather(*(enqueue(index, database) for index, database in enumerate(stores)))
        assert sorted(envelope.sequence for envelope in envelopes) == list(range(1, 13))
        assert len({envelope.stream_id for envelope in envelopes}) == 1
    finally:
        await asyncio.gather(*(database.close() for database in stores))


async def test_postgres_schema_identity_detects_changed_varchar_length():
    database = DomainDatabase("accounts", URLS["accounts"])
    try:
        async with database.engine.begin() as connection:
            await connection.execute(text("ALTER TABLE domain_outbox ALTER COLUMN id TYPE VARCHAR(1)"))
        with pytest.raises(RuntimeError, match="database_schema_mismatch"):
            await database.initialize()
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(text("ALTER TABLE domain_outbox ALTER COLUMN id TYPE VARCHAR(64)"))
        await database.close()


async def test_lifecycle_decision_recovery_is_idempotent_across_connections():
    from sandbox_harness.common.db.domain_database import DomainScope
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant, LifecycleProposal
    databases = [DomainDatabase("core", URLS["core"]) for _ in range(6)]
    targets = {name: DomainDatabase(name, URLS[name]) for name in ("media", "sites")}
    coordinators = [LifecycleCoordinator(LifecycleParticipant(database)) for database in databases]
    participants = {name: LifecycleParticipant(database) for name, database in targets.items()}
    identity = uuid4().hex
    proposal = LifecycleProposal(identity, identity, identity, 0, "archive", "archived")
    try:
        results = await asyncio.gather(*(c.start(proposal, set(targets)) for c in coordinators))
        assert all(result.state == "pending" for result in results)
        destinations = {name: p.handoff for name, p in participants.items()}
        await asyncio.gather(*(c.handoff.relay(destinations) for c in coordinators))
        await asyncio.gather(*(c.advance(identity) for c in coordinators))
        # Lose replies after participant commits, then recover from another connection.
        for command in await coordinators[0].pending_commands(identity):
            await destinations[command.target].receive(command)
        await asyncio.gather(*(c.handoff.relay(destinations) for c in coordinators))
        results = await asyncio.gather(*(c.advance(identity) for c in coordinators))
        assert all(result.state == "completed" for result in results)
        for database in [databases[0], *targets.values()]:
            async with database.transaction() as session:
                scope = await session.get(DomainScope, proposal.scope_id)
                assert (scope.generation, scope.state, scope.frozen_by) == (1, "archived", "")
    finally:
        await asyncio.gather(*(database.close() for database in databases + list(targets.values())))


async def test_registration_uses_four_databases_and_retains_principal_on_restart():
    from datetime import timedelta
    from sandbox_harness.common.db.accounts import AccountsRepository
    from sandbox_harness.common.db.lifecycle import LifecycleCoordinator, LifecycleParticipant
    accounts = AccountsRepository(URLS["accounts"])
    domains = {name: DomainDatabase(name, URLS[name]) for name in ("core", "media", "sites")}
    participants = {name: LifecycleParticipant(database) for name, database in domains.items()}
    coordinator = LifecycleCoordinator(participants["core"])
    identity = uuid4().hex
    try:
        await accounts.begin_registration(identity, identity, "reg." + identity, "test-password-hash",
            session_token_hash=identity, session_expires_at=now() + timedelta(hours=1), owner_generation=0)
        await accounts.handoff.relay({"core": coordinator.handoff})
        destinations = {name: p.handoff for name, p in participants.items() if name != "core"}
        destinations["accounts"] = accounts.handoff
        await coordinator.handoff.relay(destinations)
        await coordinator.advance(identity)
        await coordinator.handoff.relay(destinations)
        await coordinator.advance(identity)
        await coordinator.handoff.relay(destinations)
        await accounts.close()
        accounts = AccountsRepository(URLS["accounts"])
        result = await accounts.registration_result(identity, identity)
        assert result["state"] == "completed"
        assert (await accounts.get_user(result["user_id"])).owner_id == identity
        assert not await accounts.guest_owner_available(identity)
        assert (await accounts.resolve_auth_session(identity)).owner_id == identity
    finally:
        await accounts.close()
        await asyncio.gather(*(database.close() for database in domains.values()))
