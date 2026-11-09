from __future__ import annotations

import asyncio
from collections import defaultdict
import logging

from .domain import Harness, identifier
from .sandbox import Sandbox, SandboxBackend


logger = logging.getLogger(__name__)


class SandboxPool:
    def __init__(self, backend: SandboxBackend, images: dict[Harness, str], targets: dict[Harness, int], *, max_sandboxes: int | None = None):
        self.backend = backend
        self.images = images
        self.targets = targets
        self.max_sandboxes = max_sandboxes or sum(targets.values()) + 32
        user_capacity = self.max_sandboxes - sum(targets.values())
        if user_capacity < 1:
            raise ValueError("Sandbox maximum must leave at least one user slot")
        self._user_slots = asyncio.Semaphore(user_capacity)
        self._warm: dict[Harness, list[Sandbox]] = defaultdict(list)
        self._attached: dict[str, Sandbox] = {}
        self._expiry: dict[str, asyncio.Task] = {}
        self._creating: dict[Harness, int] = defaultdict(int)
        self._refill: set[asyncio.Task] = set()
        self._closed = False
        self._lock = asyncio.Lock()

    async def fill(self) -> None:
        await asyncio.gather(*(self._fill_harness(harness) for harness in self.targets))

    async def _fill_harness(self, harness: Harness) -> None:
        while True:
            async with self._lock:
                if self._closed or len(self._warm[harness]) + self._creating[harness] >= self.targets[harness]:
                    return
                self._creating[harness] += 1
            try:
                sandbox = await self.backend.create(
                    harness,
                    self.images[harness],
                    {"sandbox-harness.io/pool": "warm", "sandbox-harness.io/turn": "warm-" + identifier(), "sandbox-harness.io/harness": harness.value},
                    {},
                )
                sandbox.warm = True
                async with self._lock:
                    self._creating[harness] -= 1
                    if self._closed:
                        delete = True
                    else:
                        self._warm[harness].append(sandbox)
                        delete = False
                if delete:
                    await self.backend.delete(sandbox)
                    return
            except Exception:
                async with self._lock:
                    self._creating[harness] -= 1
                raise

    def _schedule_refill(self, harness: Harness) -> None:
        task = asyncio.create_task(self._fill_harness(harness), name="sandbox-refill-" + harness.value)
        self._refill.add(task)
        task.add_done_callback(self._refill.discard)
        task.add_done_callback(lambda value: logger.exception("Warm sandbox refill failed", exc_info=value.exception())
            if not value.cancelled() and value.exception() else None)

    async def acquire(self, *, thread_id: str, turn_id: str, harness: Harness, environment: dict[str, str]) -> Sandbox:
        async with self._lock:
            expiry = self._expiry.pop(thread_id, None)
            if expiry:
                expiry.cancel()
            attached = self._attached.get(thread_id)
            if attached and attached.harness == harness:
                await self.backend.bind(attached, thread_id, turn_id)
                attached.turn_id = turn_id
                return attached
            if attached:
                await self.backend.delete(attached)
                self._attached.pop(thread_id, None)
                self._user_slots.release()
        await self._user_slots.acquire()
        try:
            async with self._lock:
                attached = self._attached.get(thread_id)
                if attached and attached.harness == harness:
                    self._user_slots.release()
                    await self.backend.bind(attached, thread_id, turn_id)
                    attached.turn_id = turn_id
                    return attached
                sandbox = self._warm[harness].pop() if self._warm[harness] else None
                if sandbox:
                    # Pristine warm instances do not contain a user token. Runtime injection
                    # happens through its control endpoint after ownership is frozen.
                    await self.backend.bind(sandbox, thread_id, turn_id)
                    sandbox.warm = False
                else:
                    sandbox = await self.backend.create(
                        harness,
                        self.images[harness],
                        {"sandbox-harness.io/thread": thread_id, "sandbox-harness.io/turn": turn_id, "sandbox-harness.io/harness": harness.value},
                        environment,
                    )
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                self._attached[thread_id] = sandbox
        except BaseException:
            self._user_slots.release()
            raise
        self._schedule_refill(harness)
        return sandbox

    async def release(self, sandbox: Sandbox, *, keep_attached: bool, keepalive_seconds: int = 0) -> None:
        async with self._lock:
            if keep_attached:
                self._attached[sandbox.thread_id] = sandbox
                previous = self._expiry.pop(sandbox.thread_id, None)
                if previous:
                    previous.cancel()
                self._expiry[sandbox.thread_id] = asyncio.create_task(
                    self._expire(sandbox.thread_id, sandbox, keepalive_seconds),
                    name="sandbox-expire-" + sandbox.thread_id,
                )
                return
            self._attached.pop(sandbox.thread_id, None)
            await self.backend.delete(sandbox)
            self._user_slots.release()

    async def _expire(self, thread_id: str, sandbox: Sandbox, seconds: int) -> None:
        try:
            await asyncio.sleep(max(0, seconds))
            async with self._lock:
                if self._attached.get(thread_id) is not sandbox:
                    return
                await self.backend.delete(sandbox)
                self._attached.pop(thread_id, None)
                self._expiry.pop(thread_id, None)
                self._user_slots.release()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to delete expired sandbox %s", sandbox.id)

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            values = [*self._attached.values(), *(item for rows in self._warm.values() for item in rows)]
            expiry = list(self._expiry.values())
            refill = list(self._refill)
            self._refill.clear()
            self._expiry.clear()
            self._attached.clear()
            self._warm.clear()
        for task in expiry:
            task.cancel()
        for task in refill:
            task.cancel()
        if expiry:
            await asyncio.gather(*expiry, return_exceptions=True)
        if refill:
            await asyncio.gather(*refill, return_exceptions=True)
        for sandbox in values:
            await self.backend.delete(sandbox)
