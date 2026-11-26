import pytest


async def repositories(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    from sandbox_harness.common.db.gateway_repositories import GatewayRepositories
    from sandbox_harness.common.db.media_repository import MediaRepository
    from sandbox_harness.common.db.site_repository import SiteRepository
    core=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"core.db"),namespace="gateway")
    media=MediaRepository("sqlite+aiosqlite:///"+str(tmp_path/"media.db"))
    sites=SiteRepository("sqlite+aiosqlite:///"+str(tmp_path/"sites.db"))
    value=GatewayRepositories(core,media,sites);await value.initialize();return value


async def test_gateway_repository_surface_routes_only_declared_authorities(tmp_path):
    value=await repositories(tmp_path)
    try:
        assert value.owner_id_for_thread.__self__ is value.core
        assert value.get_media_job_for_thread.__self__ is value.media
        assert value.get_site_job_for_thread.__self__ is value.sites
        for forbidden in ("create_user","claim_media_jobs","claim_site_jobs","claim_available_jobs"):
            with pytest.raises(AttributeError):getattr(value,forbidden)
        assert await value.healthcheck()
    finally:await value.close()
