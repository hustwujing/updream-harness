from __future__ import annotations

import asyncio
import signal

from sandbox_harness.common.configuration.config import (load_settings, validate_service_settings)
from sandbox_harness.common.configuration.databases import (DatabaseSettings)
from sandbox_harness.common.storage.objects import (object_store)
from sandbox_harness.common.observability.observability import (configure, event)
from sandbox_harness.services.site_worker.site_worker import (SiteWorker)
from sandbox_harness.common.db.site_repository import (SiteRepository)


async def serve() -> None:
    settings = load_settings(); validate_service_settings(settings)
    database_url = DatabaseSettings().for_service("site-worker")["sites"]
    store = SiteRepository(database_url)
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
    configure("site-worker", settings.data_root / "logs", settings)
    asyncio.run(serve())


if __name__ == "__main__":
    run()
