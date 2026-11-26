from sqlalchemy import inspect


async def test_site_worker_repository_has_only_site_authority(tmp_path):
    from sandbox_harness.common.db.site_repository import SiteRepository
    repository = SiteRepository("sqlite+aiosqlite:///" + str(tmp_path / "sites.db"))
    try:
        await repository.initialize()
        async with repository.engine.connect() as connection:
            tables = await connection.run_sync(lambda value: set(inspect(value).get_table_names()))
        assert {"site_projects", "site_drafts", "site_deploy_jobs", "site_versions", "site_files"} <= tables
        assert not {"threads", "jobs", "media_jobs", "users"}.intersection(tables)
        for method in ("claim_site_jobs", "renew_site_job_lease", "update_site_job",
                       "fail_site_job", "defer_site_job", "complete_site_job",
                       "release_site_job_leases"):
            assert callable(getattr(repository, method))
    finally:
        await repository.close()


def test_site_service_uses_explicit_sites_database():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    source = (root / "src/sandbox_harness/services/site_worker/site_service.py").read_text()
    assert 'for_service("site-worker")["sites"]' in source
    assert "settings.database_url" not in source
    assert "common.db.store" not in source
