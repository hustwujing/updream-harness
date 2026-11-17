from __future__ import annotations

from .observability import observed, event, enrich, carrier, add_link, telemetry

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
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
        self._retiring: dict[str, tuple[Sandbox, bool]] = {}
        self._deleting: set[str] = set()
        self._lock = asyncio.Lock()
        self._thread_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._thread_lock_users: dict[str, int] = defaultdict(int)
        self._thread_lock_guard = asyncio.Lock()

    @asynccontextmanager
    async def _thread_lock(self, thread_id: str):
        async with self._thread_lock_guard:
            lock = self._thread_locks[thread_id]
            self._thread_lock_users[thread_id] += 1
        try:
            async with lock:
                yield
        finally:
            async with self._thread_lock_guard:
                self._thread_lock_users[thread_id] -= 1
                if self._thread_lock_users[thread_id] == 0:
                    self._thread_lock_users.pop(thread_id, None)
                    self._thread_locks.pop(thread_id, None)

    async def fill(self) -> None:
        await asyncio.gather(*(self._fill_harness(harness) for harness in self.targets))

    async def managed_sandbox_ids(self) -> set[str]:
        async with self._lock:
            return set(self._retiring) | {sandbox.id for sandbox in self._attached.values()} | {
            sandbox.id for rows in self._warm.values() for sandbox in rows
            }

    async def execution_available(self) -> bool:
        async with self._lock:
            values=list(self._attached.values())+[sandbox for rows in self._warm.values()
                for sandbox in rows]
        available=False
        for sandbox in values:
            if sandbox.health_state != "ready":
                continue
            try:
                await asyncio.wait_for(self.backend.get(sandbox.id),timeout=5)
                available=True
            except SandboxNotFound:
                try:await self.retire(sandbox)
                except Exception:pass
            except (asyncio.TimeoutError,Exception):
                continue
        return available

    async def retire(self, sandbox: Sandbox) -> None:
        expiry = None
        user_slot = False
        delete_owner = False
        async with self._lock:
            attached = next((thread_id for thread_id,value in self._attached.items()
                if value.id == sandbox.id), None)
            if attached is not None:
                self._attached.pop(attached, None)
                self._idle_since.pop(attached, None)
                expiry = self._expiry.pop(attached, None)
                user_slot = True
            for harness,rows in self._warm.items():
                self._warm[harness]=[value for value in rows if value.id!=sandbox.id]
            existing = self._retiring.get(sandbox.id)
            if existing:
                user_slot = user_slot or existing[1]
                sandbox = existing[0]
            self._retiring[sandbox.id]=(sandbox,user_slot)
            if sandbox.id not in self._deleting:
                self._deleting.add(sandbox.id);delete_owner=True
        if expiry:
            expiry.cancel()
        if delete_owner:
            await self._perform_tracked_delete(sandbox)

    async def get_attached(self, thread_id: str) -> Sandbox | None:
        async with self._lock:
            return self._attached.get(thread_id)

    @observed("sandbox.delete_tracked")
    async def _delete_tracked(self, sandbox: Sandbox, *, user_slot: bool = True) -> None:
        delete_owner=False
        async with self._lock:
            existing=self._retiring.get(sandbox.id)
            self._retiring[sandbox.id]=(existing[0] if existing else sandbox,
                user_slot or bool(existing and existing[1]))
            if sandbox.id not in self._deleting:
                self._deleting.add(sandbox.id);delete_owner=True
        if delete_owner:
            await self._perform_tracked_delete(sandbox)

    async def _perform_tracked_delete(self,sandbox:Sandbox)->None:
        try:
            await self.backend.delete(sandbox)
        except BaseException:
            # Keep both the object and its capacity reservation for retry.
            raise
        else:
            async with self._lock:
                retired = self._retiring.pop(sandbox.id, None)
            if retired and retired[1]:
                self._user_slots.release()
        finally:
            async with self._lock:
                self._deleting.discard(sandbox.id)

    async def retry_deletions(self) -> None:
        async with self._lock:
            pending = list(self._retiring.values())
        for sandbox, user_slot in pending:
            try:
                await self._delete_tracked(sandbox, user_slot=user_slot)
            except Exception:
                event("sandbox.deletion.pending",level=logging.ERROR,sandbox_id=sandbox.id)

    async def _fill_harness(self, harness: Harness) -> None:
        while True:
            async with self._lock:
                if self._closed or len(self._warm[harness]) + self._creating[harness] >= self.targets[harness]:
                    return
                self._creating[harness] += 1
            sandbox = None
            try:
                sandbox = await self.backend.create(harness, self.images[harness],
                    {"sandbox-harness.io/pool": "warm", "sandbox-harness.io/turn": "warm-" + identifier(),
                     "sandbox-harness.io/harness": harness.value}, {})
                sandbox.warm = True
                async with self._lock:
                    if not self._closed:
                        self._warm[harness].append(sandbox)
                        sandbox = None
                if sandbox:
                    await self._delete_tracked(sandbox, user_slot=False)
                    return
            finally:
                async with self._lock:
                    self._creating[harness] -= 1

    def _schedule_refill(self, harness: Harness) -> None:
        task = asyncio.create_task(self._fill_harness(harness), name="sandbox-refill-" + harness.value)
        self._refill.add(task)
        task.add_done_callback(self._refill.discard)
        task.add_done_callback(lambda value: logger.exception("Warm sandbox refill failed", exc_info=value.exception())
            if not value.cancelled() and value.exception() else None)

    @observed("sandbox.acquire")
    async def acquire(self, *, thread_id: str, turn_id: str, harness: Harness,
                      environment: dict[str, str], image: str | None = None) -> Sandbox:
        async with self._thread_lock(thread_id):
            desired_image = image or self.images[harness]
            image_frozen = image is not None
            slot_reserved = False
            async with self._lock:
                if self._closed:
                    raise RuntimeError("sandbox_pool_closed")
                attached = self._attached.get(thread_id)
                expiry = None
                incompatible = bool(attached and (attached.harness != harness
                    or image_frozen and attached.image != desired_image))
                if incompatible:
                    self._attached.pop(thread_id, None)
                    self._idle_since.pop(thread_id, None)
                    expiry = self._expiry.pop(thread_id, None)
            if incompatible:
                if expiry:
                    expiry.cancel()
                await self._delete_tracked(attached)
                attached = None
            if attached:
                try:
                    await self.backend.bind(attached, thread_id, turn_id)
                    async with self._lock:
                        expiry = self._expiry.pop(thread_id, None)
                        self._idle_since.pop(thread_id, None)
                    if expiry:
                        expiry.cancel()
                    attached.turn_id = turn_id
                    attached.reused = True
                    attached.health_state = "ready"
                    attached.health_error = ""
                    return attached
                except SandboxNotFound:
                    async with self._lock:
                        if self._attached.get(thread_id) is attached:
                            self._attached.pop(thread_id, None)
                        self._idle_since.pop(thread_id, None)
                        expiry = self._expiry.pop(thread_id, None)
                    if expiry:
                        expiry.cancel()
                    self._user_slots.release()
                    attached = None
                except BaseException as exc:
                    attached.health_state="unhealthy"
                    attached.health_error=type(exc).__name__
                    raise

            while not slot_reserved:
                evict = None
                evict_expiry = None
                async with self._lock:
                    if self._user_slots.locked() and self._idle_since:
                        evict_thread = min(self._idle_since, key=self._idle_since.get)
                        evict = self._attached.pop(evict_thread, None)
                        self._idle_since.pop(evict_thread, None)
                        evict_expiry = self._expiry.pop(evict_thread, None)
                    elif not self._user_slots.locked():
                        await self._user_slots.acquire()
                        slot_reserved = True
                if evict_expiry:
                    evict_expiry.cancel()
                    await asyncio.gather(evict_expiry, return_exceptions=True)
                if evict:
                    await self._delete_tracked(evict)
                    continue
                if not slot_reserved:
                    await self._user_slots.acquire()
                    slot_reserved = True

            try:
                async with self._lock:
                    if self._closed:
                        raise RuntimeError("sandbox_pool_closed")
                    matching = next((index for index in range(len(self._warm[harness]) - 1, -1, -1)
                        if not image_frozen or self._warm[harness][index].image == desired_image), None)
                    sandbox = self._warm[harness].pop(matching) if matching is not None else None
                if sandbox:
                    # Pristine warm instances do not contain a user token. Runtime injection
                    # happens through its control endpoint after ownership is frozen.
                    try:
                        await self.backend.bind(sandbox, thread_id, turn_id)
                        sandbox.warm = False
                    except SandboxNotFound:
                        sandbox = await self.backend.create(
                            harness,
                            desired_image,
                            {"sandbox-harness.io/thread": thread_id, "sandbox-harness.io/turn": turn_id,
                             "sandbox-harness.io/harness": harness.value},
                            environment,
                        )
                else:
                    sandbox = await self.backend.create(
                        harness,
                        desired_image,
                        {"sandbox-harness.io/thread": thread_id, "sandbox-harness.io/turn": turn_id, "sandbox-harness.io/harness": harness.value},
                        environment,
                    )
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                sandbox.reused = False
                async with self._lock:
                    closed = self._closed
                    if not closed:
                        self._attached[thread_id] = sandbox
                if closed:
                    # The local exception path releases this reservation.
                    await self._delete_tracked(sandbox, user_slot=False)
                    raise RuntimeError("sandbox_pool_closed")
            except BaseException:
                self._user_slots.release()
                raise
            self._schedule_refill(harness)
            return sandbox

    @observed("sandbox.adopt")
    async def adopt(self, sandbox: Sandbox, *, thread_id: str, turn_id: str) -> Sandbox:
        async with self._thread_lock(thread_id):
            async with self._lock:
                if self._closed:
                    raise RuntimeError("sandbox_pool_closed")
                existing = self._attached.get(thread_id)
                if existing:
                    if existing.id != sandbox.id:
                        raise RuntimeError("Thread already owns another sandbox")
                    return existing
            await self._user_slots.acquire()
            try:
                await self.backend.bind(sandbox, thread_id, turn_id)
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                sandbox.warm = False
                async with self._lock:
                    if self._closed:
                        raise RuntimeError("sandbox_pool_closed")
                    self._attached[thread_id] = sandbox
                return sandbox
            except BaseException:
                self._user_slots.release()
                raise

    @observed("sandbox.release")
    async def release(self, sandbox: Sandbox, *, keep_attached: bool, keepalive_seconds: int = 0) -> None:
        async with self._thread_lock(sandbox.thread_id):
            async with self._lock:
                if self._attached.get(sandbox.thread_id) is not sandbox:
                    return
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
            await self._delete_tracked(sandbox)

    async def detach(self, sandbox: Sandbox) -> None:
        """Forget a sandbox after this process loses ownership without deleting it."""
        async with self._thread_lock(sandbox.thread_id):
            async with self._lock:
                if self._attached.get(sandbox.thread_id) is not sandbox:
                    return
                self._attached.pop(sandbox.thread_id, None)
                self._idle_since.pop(sandbox.thread_id, None)
                expiry = self._expiry.pop(sandbox.thread_id, None)
            if expiry:
                expiry.cancel()
            self._user_slots.release()

    async def discard_thread(self, thread_id: str) -> None:
        async with self._thread_lock(thread_id):
            async with self._lock:
                expiry = self._expiry.pop(thread_id, None)
                sandbox = self._attached.pop(thread_id, None)
                self._idle_since.pop(thread_id, None)
            if expiry:
                expiry.cancel()
            if sandbox:
                await self._delete_tracked(sandbox)
                return
        for candidate in await self.backend.list_namespace():
            if candidate.metadata.get("sandbox-harness.io/thread") == thread_id:
                await self.backend.delete(candidate)

    async def _expire(self, thread_id: str, sandbox: Sandbox, seconds: int) -> None:
        try:
            await asyncio.sleep(max(0, seconds))
            async with self._thread_lock(thread_id):
                async with self._lock:
                    if self._attached.get(thread_id) is not sandbox:
                        return
                    self._attached.pop(thread_id, None)
                    self._idle_since.pop(thread_id, None)
                    self._expiry.pop(thread_id, None)
                await self._delete_tracked(sandbox)
        except asyncio.CancelledError:
            raise
        except Exception:
            event("sandbox.expiry.delete_failed",level=logging.ERROR,sandbox_id=sandbox.id)

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
                try:
                    await self._delete_tracked(sandbox, user_slot=not sandbox.warm)
                except Exception:
                    event("sandbox.shutdown.delete_pending",level=logging.WARNING,sandbox_id=sandbox.id)
        await self.retry_deletions()
