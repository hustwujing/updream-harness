"""Shared construction for API request access and PlatformWorker execution."""
from __future__ import annotations

from dataclasses import dataclass

from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.catalog.media_catalog import MediaCatalog
from sandbox_harness.common.catalog.model_discovery import ModelDiscovery
from sandbox_harness.common.configuration.config import Settings
from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.contracts.domain import Harness
from sandbox_harness.common.db.accounts import AccountsRepository
from sandbox_harness.common.db.api_repositories import ApiRepositories
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.media_repository import MediaRepository
from sandbox_harness.common.db.site_repository import SiteRepository
from sandbox_harness.common.sandbox.pool import SandboxPool
from sandbox_harness.common.sandbox.runtime import RuntimeClient
from sandbox_harness.common.storage.objects import object_store
import sandbox_harness.common.sandbox.sandbox_factory as sandbox_factory

from .turn_broker import Broker


@dataclass(frozen=True)
class PlatformComponents:
    store: ApiRepositories
    objects: object
    pool: SandboxPool
    catalog: HarnessCatalog
    discovery: ModelDiscovery
    media_catalog: MediaCatalog
    broker: Broker


def build_platform_components(settings: Settings, *, execution_enabled: bool,
                              object_store_factory=object_store) -> PlatformComponents:
    try:
        databases=DatabaseSettings().for_service(
            "platform-worker" if execution_enabled else "api")
    except ValueError:
        if settings.backend!="fake":raise
        databases={
            "accounts":"sqlite+aiosqlite:///"+str(settings.data_root/"api-accounts.db"),
            "core":settings.database_url,
            "media":"sqlite+aiosqlite:///"+str(settings.data_root/"api-media.db"),
            "sites":"sqlite+aiosqlite:///"+str(settings.data_root/"api-sites.db"),
        }
    core=CoreRepository(databases["core"],settings.redis_url,settings.redis_prefix,
        namespace=settings.namespace,**settings.handoff_options)
    store=ApiRepositories(AccountsRepository(databases["accounts"],**settings.handoff_options),core,
        MediaRepository(databases["media"],**settings.handoff_options),
        SiteRepository(databases["sites"],**settings.handoff_options))
    objects=object_store_factory(settings)
    backend=sandbox_factory.build_sandbox_registry(settings)
    releases=sandbox_factory.primary_provider_releases(settings,backend.provider)
    profile_resolver=sandbox_factory.build_profile_resolver(settings,backend,releases)
    images={harness:releases[harness].ref for harness in Harness}
    targets={Harness.CODEX:settings.warm_pool_codex,Harness.CLAUDE:settings.warm_pool_claude,
        Harness.DEEPSEEK:settings.warm_pool_deepseek}
    if sum(targets.values())>=settings.max_sandboxes:
        raise ValueError("max_sandboxes must exceed the total warm pool target")
    pool=SandboxPool(backend,images,targets,max_sandboxes=settings.max_sandboxes,
        profile_resolver=profile_resolver,ttl_seconds=settings.sandbox_timeout_seconds,
        ownership=core.ownership)
    runtime=RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds,
        reattach_attempts=settings.runtime_stream_reattach_attempts,
        reattach_max_delay_seconds=settings.runtime_stream_reattach_max_delay_seconds,
        request_retry_attempts=settings.runtime_request_retry_attempts,
        request_retry_initial_delay_seconds=settings.runtime_request_retry_initial_delay_seconds,
        request_retry_backoff_multiplier=settings.runtime_request_retry_backoff_multiplier,
        request_retry_max_delay_seconds=settings.runtime_request_retry_max_delay_seconds,
        bundle_dir=settings.runtime_bundle_dir,default_bundle_digest=settings.runtime_bundle_digest,
        code_bundle_dir=settings.runtime_code_bundle_dir,
        default_code_bundle_digest=settings.runtime_code_bundle_digest,
        staging_root=settings.data_root/"tmp"/"runtime-downloads",backend=backend)
    catalog=HarnessCatalog(settings.harness_catalog_json or None)
    media_catalog=MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key))
    discovery=ModelDiscovery(settings,catalog)
    if settings.model_catalog_discovery:discovery.load_snapshot()
    broker=Broker(settings,store,objects,pool,runtime,catalog,
        execution_enabled=execution_enabled)
    return PlatformComponents(store,objects,pool,catalog,discovery,media_catalog,broker)
