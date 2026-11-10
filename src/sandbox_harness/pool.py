from __future__ import annotations

import asyncio
from collections import defaultdict
import logging
import time

from .domain import Harness, identifier
from .sandbox import Sandbox, SandboxBackend, SandboxNotFound


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
        self._idle_since: dict[str, float] = {}
        self._creating: dict[Harness, int] = defaultdict(int)
        self._refill: set[asyncio.Task] = set()
        self._closed = False
        self._lock = asyncio.Lock()

    async def fill(self) -> None:
        await asyncio.gather(*(self._fill_harness(harness) for harness in self.targets))

    async def managed_sandbox_ids(self) -> set[str]:
        async with self._lock:
            return {sandbox.id for sandbox in self._attached.values()} | {
                sandbox.id for rows in self._warm.values() for sandbox in rows
            }

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
        slot_reserved = False
        async with self._lock:
            expiry = self._expiry.pop(thread_id, None)
            if expiry:
                expiry.cancel()
            attached = self._attached.get(thread_id)
            if attached and attached.harness == harness:
                self._idle_since.pop(thread_id, None)
                try:
                    await self.backend.bind(attached, thread_id, turn_id)
                    attached.turn_id = turn_id
                    attached.reused = True
                    return attached
                except SandboxNotFound:
                    self._attached.pop(thread_id, None)
                    self._user_slots.release()
                    attached = None
            if attached:
                await self.backend.delete(attached)
                self._attached.pop(thread_id, None)
                self._idle_since.pop(thread_id, None)
                self._user_slots.release()
            if self._user_slots.locked() and self._idle_since:
                evict_thread = min(self._idle_since, key=self._idle_since.get)
                evict = self._attached.pop(evict_thread, None)
                self._idle_since.pop(evict_thread, None)
                evict_expiry = self._expiry.pop(evict_thread, None)
                if evict_expiry:
                    evict_expiry.cancel()
                if evict:
                    await self.backend.delete(evict)
                    self._user_slots.release()
            if not self._user_slots.locked():
                await self._user_slots.acquire()
                slot_reserved = True
        if not slot_reserved:
            await self._user_slots.acquire()
        try:
            async with self._lock:
                attached = self._attached.get(thread_id)
                if attached and attached.harness == harness:
                    self._user_slots.release()
                    self._idle_since.pop(thread_id, None)
                    await self.backend.bind(attached, thread_id, turn_id)
                    attached.turn_id = turn_id
                    attached.reused = True
                    return attached
                sandbox = self._warm[harness].pop() if self._warm[harness] else None
                if sandbox:
                    # Pristine warm instances do not contain a user token. Runtime injection
                    # happens through its control endpoint after ownership is frozen.
                    try:
                        await self.backend.bind(sandbox, thread_id, turn_id)
                        sandbox.warm = False
                    except SandboxNotFound:
                        sandbox = await self.backend.create(
                            harness,
                            self.images[harness],
                            {"sandbox-harness.io/thread": thread_id, "sandbox-harness.io/turn": turn_id,
                             "sandbox-harness.io/harness": harness.value},
                            environment,
                        )
                else:
                    sandbox = await self.backend.create(
                        harness,
                        self.images[harness],
                        {"sandbox-harness.io/thread": thread_id, "sandbox-harness.io/turn": turn_id, "sandbox-harness.io/harness": harness.value},
                        environment,
                    )
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                sandbox.reused = False
                self._attached[thread_id] = sandbox
        except BaseException:
            self._user_slots.release()
            raise
        self._schedule_refill(harness)
        return sandbox

    async def adopt(self, sandbox: Sandbox, *, thread_id: str, turn_id: str) -> Sandbox:
        await self._user_slots.acquire()
        try:
            async with self._lock:
                existing = self._attached.get(thread_id)
                if existing and existing.id != sandbox.id:
                    raise RuntimeError("Thread already owns another sandbox")
                await self.backend.bind(sandbox, thread_id, turn_id)
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                sandbox.warm = False
                self._attached[thread_id] = sandbox
            return sandbox
        except BaseException:
            self._user_slots.release()
            raise

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
                self._idle_since[sandbox.thread_id] = time.monotonic()
                return
            self._attached.pop(sandbox.thread_id, None)
            self._idle_since.pop(sandbox.thread_id, None)
            await self.backend.delete(sandbox)
            self._user_slots.release()

    async def discard_thread(self, thread_id: str) -> None:
        async with self._lock:
            expiry = self._expiry.pop(thread_id, None)
            sandbox = self._attached.pop(thread_id, None)
            self._idle_since.pop(thread_id, None)
        if expiry:
            expiry.cancel()
            await asyncio.gather(expiry, return_exceptions=True)
        if sandbox:
            await self.backend.delete(sandbox)
            self._user_slots.release()
            return
        for candidate in await self.backend.list_namespace():
            if candidate.metadata.get("sandbox-harness.io/thread") == thread_id:
                await self.backend.delete(candidate)

    async def _expire(self, thread_id: str, sandbox: Sandbox, seconds: int) -> None:
        try:
            await asyncio.sleep(max(0, seconds))
            async with self._lock:
                if self._attached.get(thread_id) is not sandbox:
                    return
                await self.backend.delete(sandbox)
                self._attached.pop(thread_id, None)
                self._idle_since.pop(thread_id, None)
                self._expiry.pop(thread_id, None)
                self._user_slots.release()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to delete expired sandbox %s", sandbox.id)

    async def close(self, *, preserve_sandbox_ids: set[str] | None = None) -> None:
        preserve_sandbox_ids = preserve_sandbox_ids or set()
        async with self._lock:
            self._closed = True
            values = [*self._attached.values(), *(item for rows in self._warm.values() for item in rows)]
            expiry = list(self._expiry.values())
            refill = list(self._refill)
            self._refill.clear()
            self._expiry.clear()
            self._attached.clear()
            self._idle_since.clear()
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
            if sandbox.id not in preserve_sandbox_ids:
                await self.backend.delete(sandbox)
