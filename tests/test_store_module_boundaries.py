from __future__ import annotations

import importlib


def test_store_domains_keep_one_orm_identity():
    facade = importlib.import_module("test_support.legacy_store")
    models = importlib.import_module("sandbox_harness.common.db.models")
    assert facade.Base is models.Base
    for name in ("ThreadRow", "TurnRow", "JobRow", "JobAttemptRow", "SandboxAllocationRow",
                 "ResourceReservationRow", "MediaJobRow", "ArtifactRow", "SiteDeployJobRow"):
        assert getattr(facade, name) is getattr(models, name)
        assert getattr(facade, name).metadata is models.Base.metadata


def test_domain_transactions_compose_the_original_store_surface():
    from test_support.legacy_store import Store
    expected = {
        "create_turn": "turns",
        "transition_job": "job_terminal",
        "record_job_acceptance": "job_attempts",
        "enqueue_job_media_execution": "media_jobs",
        "publish_artifact": "files",
        "complete_site_job": "sites",
        "initialize": "test_support.upgrades",
    }
    for method, owner in expected.items():
        expected_module=(owner if owner.startswith("test_support.")
                         else "sandbox_harness.common.db."+owner)
        assert getattr(Store, method).__module__ == expected_module


async def test_domain_facade_uses_one_connection_registry(tmp_path):
    from test_support.legacy_store import Store
    from sandbox_harness.common.db.models import Base
    from sqlalchemy import inspect
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "domains.db"))
    try:
        await store.initialize()
        await store.initialize()
        async with store.engine.connect() as connection:
            actual = await connection.run_sync(lambda value: set(inspect(value).get_table_names()))
        assert actual == set(Base.metadata.tables)
        assert len(actual) == 57
        assert store.connections.kw["bind"] is store.engine
    finally:
        await store.close()
