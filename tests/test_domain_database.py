from __future__ import annotations

import pytest
from sqlalchemy import inspect, select, text


def database(tmp_path, domain):
    from sandbox_harness.common.db.domain_database import DomainDatabase
    return DomainDatabase(domain, "sqlite+aiosqlite:///" + str(tmp_path / (domain + ".db")))


async def test_fresh_domains_contain_only_owned_tables(tmp_path):
    from sandbox_harness.common.db.domain_database import DOMAIN_TABLES, PROTOCOL_TABLES
    observed = set()
    for domain, owned in DOMAIN_TABLES.items():
        db = database(tmp_path, domain)
        try:
            await db.initialize()
            await db.initialize()
            async with db.engine.connect() as connection:
                actual = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
            assert actual == set(owned) | PROTOCOL_TABLES
            assert not observed.intersection(owned)
            observed.update(owned)
        finally:
            await db.close()
    from sandbox_harness.common.db.models import Base
    from sandbox_harness.common.db.account_models import AccountBase
    from sandbox_harness.common.db.ownership_models import OwnershipBase
    from sandbox_harness.common.db.media_models import MediaAuthorityBase
    assert observed == (set(Base.metadata.tables) | set(AccountBase.metadata.tables)
                        | set(OwnershipBase.metadata.tables) | set(MediaAuthorityBase.metadata.tables))


async def test_wrong_domain_and_unversioned_database_fail_without_mutation(tmp_path):
    from sandbox_harness.common.db.domain_database import DomainDatabase
    core = database(tmp_path, "core")
    await core.initialize()
    other = DomainDatabase("media", core.url)
    try:
        with pytest.raises(RuntimeError, match="database_domain_mismatch"):
            await other.initialize()
        async with core.engine.connect() as connection:
            names = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
        assert "media_jobs" not in names
    finally:
        await core.close()
        await other.close()
    unversioned = database(tmp_path, "accounts")
    try:
        async with unversioned.engine.begin() as connection:
            await connection.execute(text("CREATE TABLE unknown_data (id INTEGER)"))
        with pytest.raises(RuntimeError, match="database_unversioned"):
            await unversioned.initialize()
        async with unversioned.engine.connect() as connection:
            names = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
        assert names == {"unknown_data"}
    finally:
        await unversioned.close()


async def test_domain_session_rejects_foreign_tables_and_raw_sql(tmp_path):
    from sandbox_harness.common.db.models import MediaJobRow, ThreadRow
    core = database(tmp_path, "core")
    try:
        await core.initialize()
        async with core.transaction() as session:
            assert list((await session.scalars(select(ThreadRow))).all()) == []
            with pytest.raises(ValueError, match="database_table_not_owned"):
                await session.scalars(select(MediaJobRow))
            with pytest.raises(ValueError, match="database_raw_sql_forbidden"):
                await session.execute(text("SELECT 1"))
        async with core.transaction() as session:
            session.add(MediaJobRow(id="forbidden"))
            with pytest.raises(ValueError, match="database_table_not_owned"):
                await session.flush()
            session.expunge_all()
    finally:
        await core.close()


async def test_schema_drift_is_rejected(tmp_path):
    core = database(tmp_path, "core")
    try:
        await core.initialize()
        async with core.engine.begin() as connection:
            await connection.execute(text("ALTER TABLE threads ADD COLUMN unexpected TEXT"))
        with pytest.raises(RuntimeError, match="database_schema_mismatch"):
            await core.initialize()
    finally:
        await core.close()
