from __future__ import annotations

import asyncio
import signal

from .config import load_settings, validate_service_settings
from .objects import object_store
from .observability import configure, event
from .site_worker import SiteWorker
from .store import Store


async def serve() -> None:
    settings = load_settings(); validate_service_settings(settings)
    store = Store(settings.database_url, settings.redis_url, settings.redis_prefix)
    worker = SiteWorker(settings, store, object_store(settings))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop_event.set)
    await store.initialize()
    event("site.worker.started", worker_id=worker.owner)
    try:
        await worker.run()
    finally:
        event("site.worker.stopped", worker_id=worker.owner)
        await store.release_site_job_leases(worker.owner)
        await store.close()


def run() -> None:
    settings = load_settings()
    configure("site-worker", settings.data_root / "logs")
    asyncio.run(serve())


if __name__ == "__main__":
    run()
