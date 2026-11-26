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
