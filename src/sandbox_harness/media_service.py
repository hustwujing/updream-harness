from __future__ import annotations

import asyncio
import signal

from .config import load_settings, validate_service_settings
from .media_catalog import MediaCatalog
from .media_worker import MediaWorker
from .objects import object_store
from .observability import configure, event
from .store import Store


async def serve() -> None:
    settings = load_settings()
    validate_service_settings(settings)
    store = Store(settings.database_url, settings.redis_url, settings.redis_prefix)
    worker = MediaWorker(settings, store, object_store(settings), MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key)))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop_event.set)
    await store.initialize()
    event("media.worker.started", worker_id=worker.owner)
    try:
        await worker.run()
    finally:
        event("media.worker.stopped", worker_id=worker.owner)
        await store.release_media_job_leases(worker.owner)
        await store.close()


def run() -> None:
    settings = load_settings()
    configure("media-worker", settings.data_root / "logs")
    asyncio.run(serve())


if __name__ == "__main__":
    run()
