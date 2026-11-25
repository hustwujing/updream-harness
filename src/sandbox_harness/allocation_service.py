"""Independent owner of Job sandbox create RPCs and their durable responses."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import signal

from .allocation_dispatch import AllocationService
from .config import Settings,load_settings
from .observability import configure,event
from .sandbox_factory import build_sandbox_backend
from .store import Store


async def serve(settings):
    store=Store(settings.database_url);await store.initialize()
    backend=build_sandbox_backend(settings);service=AllocationService(store,backend)
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for name in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(name,stop.set)
    try:
        while not stop.is_set():
            try:await service.run_once(limit=max(1,settings.job_worker_concurrency))
            except Exception as exc:
                event('sandbox.allocation.dispatch_failed',level=40,exception_type=type(exc).__name__)
            try:await asyncio.wait_for(stop.wait(),timeout=.2)
            except asyncio.TimeoutError:pass
    finally:await backend.aclose();await store.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--settings-file',type=Path)
    args=parser.parse_args()
    settings=(Settings.model_validate(json.loads(args.settings_file.read_text())['settings'])
        if args.settings_file else load_settings())
    configure('sandbox-allocation',settings.data_root/'logs')
    asyncio.run(serve(settings))


if __name__=='__main__':main()
