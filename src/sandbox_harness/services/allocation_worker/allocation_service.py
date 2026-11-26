"""Independent owner of Job sandbox create RPCs and their durable responses."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import signal

from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationService)
from sandbox_harness.common.configuration.config import (Settings, load_settings)
from sandbox_harness.common.configuration.databases import (DatabaseSettings)
from sandbox_harness.common.observability.observability import (configure)
from sandbox_harness.common.sandbox.sandbox_factory import (build_sandbox_backend)
from sandbox_harness.common.db.core import (CoreRepository)


async def serve(settings):
    database_url=DatabaseSettings().for_service("allocation-worker")["core"]
    store=CoreRepository(database_url,namespace=settings.namespace,
        **settings.handoff_options);await store.initialize()
    backend=build_sandbox_backend(settings)
    service=AllocationService(store,backend,lease_seconds=settings.allocation_lease_seconds,
        circuit_failure_threshold=settings.provider_circuit_failure_threshold,
        circuit_open_seconds=settings.provider_circuit_open_seconds,
        circuit_probe_seconds=settings.provider_circuit_probe_seconds)
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for name in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(name,stop.set)
    try:
        await service.run(stop,concurrency=settings.job_worker_concurrency)
    finally:await backend.aclose();await store.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--settings-file',type=Path)
    args=parser.parse_args()
    settings=(Settings.model_validate(json.loads(args.settings_file.read_text())['settings'])
        if args.settings_file else load_settings())
    configure('sandbox-allocation',settings.data_root/'logs',settings)
    asyncio.run(serve(settings))


if __name__=='__main__':main()
