from __future__ import annotations

from sandbox_harness.common.observability.observability import (observed, event, enrich, carrier, add_link, telemetry)

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
import logging
import time
from typing import Awaitable,Callable

from sandbox_harness.common.contracts.domain import (Harness, identifier)
from sandbox_harness.common.sandbox.sandbox import (Sandbox, SandboxBackend, SandboxNotFound, SandboxProfileUnavailable)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxCreateOutcomeUnknown, SandboxCreateRequest, SandboxCapacityUnavailable, SandboxProfile, SandboxScope, SandboxState)
from sandbox_harness.common.sandbox.sandbox_factory import (SandboxProfileResolver)


logger = logging.getLogger(__name__)


class SandboxPool:
    def __init__(self, backend: SandboxBackend, images: dict[Harness, str], targets: dict[Harness, int], *, max_sandboxes: int | None = None,profile_resolver:SandboxProfileResolver|None=None,ttl_seconds:int=3600):
        self.backend = backend
        self.images = images
        self.targets = targets
        self.max_sandboxes = max_sandboxes or sum(targets.values()) + 32
        self.profile_resolver=profile_resolver or SandboxProfileResolver(provider=backend.provider,
            runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="legacy",
            runtime_code_bundle_digest="legacy")
        self.ttl_seconds=ttl_seconds
        # Only callers with a durable, lease-fenced cleanup owner opt in.
        self._pending_allocations:dict[tuple[str,str],tuple[str,SandboxProfile]]={}
        self._retained_allocations:dict[tuple[str,str],str]={}
        self._capacity_debt=0
        user_capacity = self.max_sandboxes - sum(targets.values())
        if user_capacity < 1:
            raise ValueError("Sandbox maximum must leave at least one user slot")
        self._user_slots = asyncio.Semaphore(user_capacity)
        self._capacity_changed = asyncio.Event()
        self._warm: dict[Harness, list[Sandbox]] = defaultdict(list)
        self._attached: dict[str, Sandbox] = {}
        self._expiry: dict[str, asyncio.Task] = {}
        self._idle_since: dict[str, float] = {}
        self._creating: dict[Harness, int] = defaultdict(int)
        self._refill: set[asyncio.Task] = set()
        self._closed = False
        self._retiring: dict[tuple[str,str], tuple[Sandbox, bool]] = {}
        self._deleting: set[tuple[str,str]] = set()
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
            return {sandbox.id for sandbox,_ in self._retiring.values()} | {
            sandbox.id for sandbox in self._attached.values()} | {
            sandbox.id for rows in self._warm.values() for sandbox in rows
            }

    @staticmethod
    def sandbox_key(sandbox:Sandbox)->tuple[str,str]:
        return sandbox.handle.provider,sandbox.handle.sandbox_id

    async def managed_sandbox_keys(self)->set[tuple[str,str]]:
        async with self._lock:
            return set(self._retiring) | {self.sandbox_key(value) for value in self._attached.values()} | {
                self.sandbox_key(value) for rows in self._warm.values() for value in rows}

    async def list_owned(self)->list[Sandbox]:
        return await self.backend.list_owned(SandboxScope(self.backend.namespace))

    async def reconcile_create(self,allocation_id:str)->Sandbox|None:
        return await self.backend.reconcile_create(
            SandboxScope(self.backend.namespace),allocation_id)

    async def owns_allocation(self,provider:str,allocation_id:str)->bool:
        async with self._lock:
            return ((provider,allocation_id) in self._pending_allocations
                or (provider,allocation_id) in self._retained_allocations or any(
                value.handle.provider==provider and value.handle.allocation_id==allocation_id
                for value in self._attached.values()))

    async def restore_allocation_capacity(self,allocations)->None:
        """Restore durable Job ownership before admitting work after process loss."""
        async with self._lock:
            if self._closed:raise RuntimeError('sandbox_pool_closed')
            for value in allocations:
                key=(value['provider'],value['allocation_id']);profile_key=value['profile_key']
                existing=self._retained_allocations.get(key)
                if existing:
                    if existing!=profile_key:raise RuntimeError('sandbox_allocation_profile_conflict')
                    continue
                if key in self._pending_allocations or any(
                        (sandbox.handle.provider,sandbox.handle.allocation_id)==key
                        for sandbox in [*self._attached.values(),
                            *(sandbox for sandbox,_ in self._retiring.values())]):continue
                self._retained_allocations[key]=profile_key
                if self._user_slots.locked():self._capacity_debt+=1
                else:await self._user_slots.acquire()

    def _release_user_slot(self):
        # A reduced configured limit cannot discard existing durable ownership.
        if self._capacity_debt:self._capacity_debt-=1
        else:self._user_slots.release()
        self._capacity_changed.set()

    async def retirement_pending(self,sandbox:Sandbox)->bool:
        async with self._lock:return self.sandbox_key(sandbox) in self._retiring

    async def _create_for_owner(self,request:SandboxCreateRequest,thread_id:str,
                                retain_unresolved:bool,before_create=None,create=None)->Sandbox:
        key=(request.profile.provider,request.allocation_id)
        if before_create:await before_create(request)
        if retain_unresolved:
            async with self._lock:
                self._pending_allocations[key]=(thread_id,request.profile)
        try:return await (create(request) if create else self.backend.create(request))
        except (SandboxCreateOutcomeUnknown,asyncio.CancelledError):raise
        except BaseException as exc:
            if retain_unresolved and not getattr(exc,'retain_allocation',False):
                async with self._lock:self._pending_allocations.pop(key,None)
            raise

    async def release_absent_allocation(self,provider:str,allocation_id:str)->None:
        """Release a pending slot only after its durable owner proves absence."""
        async with self._lock:
            key=(provider,allocation_id)
            if any(value.handle.provider==provider and value.handle.allocation_id==allocation_id
                    for value in self._attached.values()):
                raise RuntimeError('sandbox_allocation_is_attached')
            pending=self._pending_allocations.pop(key,None)
            retained=self._retained_allocations.pop(key,None)
            if pending is not None or retained is not None:self._release_user_slot()

    async def forget_absent_allocation(self, provider: str, allocation_id: str) -> None:
        """Evict local ownership only after the durable authority confirms absence.

        Used by lifecycle cleanup after its ABSENT commit. It performs no RPC;
        repeating it after reply/process loss does not release another slot.
        """
        expiries = []
        async with self._lock:
            key = (provider, allocation_id)
            charged = self._pending_allocations.pop(key, None) is not None
            charged = (self._retained_allocations.pop(key, None) is not None) or charged
            for thread_id, sandbox in list(self._attached.items()):
                if (sandbox.handle.provider, sandbox.handle.allocation_id) == key:
                    self._attached.pop(thread_id)
                    self._idle_since.pop(thread_id, None)
                    expiry = self._expiry.pop(thread_id, None)
                    if expiry:
                        expiries.append(expiry)
                    charged = True
            for retiring_key, (sandbox, user_slot) in list(self._retiring.items()):
                if (sandbox.handle.provider, sandbox.handle.allocation_id) == key:
                    self._retiring.pop(retiring_key)
                    charged = charged or user_slot
            if charged:
                self._release_user_slot()
            self._capacity_changed.set()
        tasks = [task for task in expiries if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def execution_available(self) -> bool:
        async with self._lock:
            values=list(self._attached.values())+[sandbox for rows in self._warm.values()
                for sandbox in rows]
        if not values and sum(self.targets.values())==0:return True
        available=False
        for sandbox in values:
            if sandbox.health_state != "ready":
                continue
            try:
                await asyncio.wait_for(self.backend.get(sandbox.handle),timeout=5)
                available=True
            except SandboxNotFound:
                try:await self.retire(sandbox)
                except Exception:pass
            except (asyncio.TimeoutError,Exception):
                continue
        return available

    async def retire(self, sandbox: Sandbox) -> None:
        key=self.sandbox_key(sandbox)
        expiry = None
        user_slot = False
        delete_owner = False
        async with self._lock:
            attached = next((thread_id for thread_id,value in self._attached.items()
                if self.sandbox_key(value)==key), None)
            if attached is not None:
                self._attached.pop(attached, None)
                self._idle_since.pop(attached, None)
                expiry = self._expiry.pop(attached, None)
                user_slot = True
            pending_key=(sandbox.handle.provider,sandbox.handle.allocation_id)
            pending=self._pending_allocations.get(pending_key)
            retained=self._retained_allocations.get(pending_key)
            if retained:
                if retained!=sandbox.compatibility_key:
                    raise SandboxProfileUnavailable('Retained allocation profile mismatch',provider=sandbox.handle.provider)
                self._retained_allocations.pop(pending_key);user_slot=True
            if pending:
                if pending[1].compatibility_key!=sandbox.compatibility_key:
                    raise SandboxProfileUnavailable('Pending allocation profile mismatch',
                        provider=sandbox.handle.provider)
                self._pending_allocations.pop(pending_key)
                user_slot=True
            for harness,rows in self._warm.items():
                self._warm[harness]=[value for value in rows if self.sandbox_key(value)!=key]
            existing = self._retiring.get(key)
            if existing:
                user_slot = user_slot or existing[1]
                sandbox = existing[0]
            self._retiring[key]=(sandbox,user_slot)
            if key not in self._deleting:
                self._deleting.add(key);delete_owner=True
        if expiry:
            expiry.cancel()
        if delete_owner:
            await self._perform_tracked_delete(sandbox)

    async def get_attached(self, thread_id: str) -> Sandbox | None:
        async with self._lock:
            return self._attached.get(thread_id)

    @observed("sandbox.delete_tracked")
    async def _delete_tracked(self, sandbox: Sandbox, *, user_slot: bool = True) -> None:
        key=self.sandbox_key(sandbox)
        delete_owner=False
        async with self._lock:
            existing=self._retiring.get(key)
            self._retiring[key]=(existing[0] if existing else sandbox,
                user_slot or bool(existing and existing[1]))
            if key not in self._deleting:
                self._deleting.add(key);delete_owner=True
        if delete_owner:
            await self._perform_tracked_delete(sandbox)

    async def _perform_tracked_delete(self,sandbox:Sandbox)->None:
        key=self.sandbox_key(sandbox)
        try:
            try:
                await self.backend.delete(sandbox)
            except SandboxNotFound:
                pass
            async with self._lock:
                retired = self._retiring.pop(key, None)
            if retired and retired[1]:
                self._release_user_slot()
                self._capacity_changed.set()
        finally:
            async with self._lock:
                self._deleting.discard(key)

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
                identity="warm-"+identifier();profile=self.profile_resolver.resolve(
                    harness,self.images[harness])
                sandbox=await self.backend.create(SandboxCreateRequest(harness=harness,profile=profile,
                    environment={},allocation_id=identity,ttl_seconds=self.ttl_seconds))
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

    async def _bind(self,sandbox:Sandbox,*,thread_id:str,turn_id:str)->Sandbox:
        sandbox=await self.backend.renew(sandbox,force=True)
        sandbox.warm=False;sandbox.thread_id=thread_id;sandbox.turn_id=turn_id
        return sandbox

    async def _reserve_user_slot(self,*,wait_for_capacity:bool=True) -> None:
        while True:
            evict=None;evict_expiry=None;waiter=None
            async with self._lock:
                if self._closed:
                    raise RuntimeError("sandbox_pool_closed")
                if self._user_slots.locked() and self._idle_since:
                    evict_thread=min(self._idle_since,key=self._idle_since.get)
                    evict=self._attached.pop(evict_thread,None)
                    self._idle_since.pop(evict_thread,None)
                    evict_expiry=self._expiry.pop(evict_thread,None)
                elif not self._user_slots.locked():
                    await self._user_slots.acquire()
                    return
                else:
                    if not wait_for_capacity:
                        raise SandboxCapacityUnavailable('No local sandbox capacity',provider=self.backend.provider)
                    self._capacity_changed.clear()
                    waiter=self._capacity_changed.wait()
            if evict_expiry:
                evict_expiry.cancel()
                await asyncio.gather(evict_expiry,return_exceptions=True)
            if evict:
                await self._delete_tracked(evict)
            elif waiter:
                await waiter

    @observed("sandbox.acquire")
    async def acquire(self, *, thread_id: str, turn_id: str, harness: Harness,
                      environment: dict[str, str], image: str | None = None,
                      profile_id: str | None = None, revision: str | None = None,
                      retain_unresolved_allocation:bool=False,
                      before_create:Callable[[SandboxCreateRequest],Awaitable[None]]|None=None,
                      create:Callable[[SandboxCreateRequest],Awaitable[Sandbox]]|None=None,
                      allocation_id:str|None=None,wait_for_capacity:bool=True) -> Sandbox:
        async with self._thread_lock(thread_id):
            desired_image = image or self.images[harness]
            desired_profile=self.profile_resolver.resolve(harness,desired_image,
                profile_id=profile_id,revision=revision)
            explicit_allocation=bool(allocation_id)
            allocation_id=allocation_id or turn_id
            pending_key=(desired_profile.provider,allocation_id)
            slot_reserved = False
            async with self._lock:
                if self._closed:
                    raise RuntimeError("sandbox_pool_closed")
                attached = self._attached.get(thread_id)
                expiry = None
                incompatible=bool(attached and (attached.harness!=harness
                    or attached.compatibility_key!=desired_profile.compatibility_key
                    or (explicit_allocation and attached.handle.allocation_id!=allocation_id)))
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
                    await self._bind(attached,thread_id=thread_id,turn_id=turn_id)
                    async with self._lock:
                        expiry = self._expiry.pop(thread_id, None)
                        self._idle_since.pop(thread_id, None)
                    if expiry:
                        expiry.cancel()
                    attached.turn_id = turn_id
                    attached.reused = True
                    attached.health_state = "ready"
                    attached.state=SandboxState.READY
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
                    self._release_user_slot()
                    self._capacity_changed.set()
                    attached = None
                except BaseException as exc:
                    attached.health_state="unhealthy"
                    attached.health_error=type(exc).__name__
                    raise

            async with self._lock:
                if pending_key in self._pending_allocations or pending_key in self._retained_allocations:
                    raise SandboxCreateOutcomeUnknown('Allocation requires reconciliation',
                        provider=desired_profile.provider)
            await self._reserve_user_slot(wait_for_capacity=wait_for_capacity)
            slot_reserved=True

            try:
                async with self._lock:
                    if self._closed:
                        raise RuntimeError("sandbox_pool_closed")
                    incompatible_warm=[value for value in self._warm[harness]
                        if value.compatibility_key!=desired_profile.compatibility_key]
                    self._warm[harness]=[value for value in self._warm[harness]
                        if value.compatibility_key==desired_profile.compatibility_key]
                    sandbox = (self._warm[harness].pop() if self._warm[harness]
                        and not retain_unresolved_allocation else None)
                for stale in incompatible_warm:
                    await self._delete_tracked(stale,user_slot=False)
                if sandbox:
                    # Pristine warm instances do not contain a user token. Runtime injection
                    # happens through its control endpoint after ownership is frozen.
                    try:
                        await self._bind(sandbox,thread_id=thread_id,turn_id=turn_id)
                        sandbox.warm = False
                    except SandboxNotFound:
                        sandbox=await self._create_for_owner(SandboxCreateRequest(harness=harness,
                            profile=desired_profile,environment=environment,
                            ttl_seconds=self.ttl_seconds,allocation_id=allocation_id),
                            thread_id,retain_unresolved_allocation,before_create,create)
                    except asyncio.CancelledError:
                        cleanup=asyncio.create_task(self._delete_tracked(sandbox,user_slot=False))
                        try:await asyncio.shield(cleanup)
                        except asyncio.CancelledError:await cleanup
                        raise
                    except Exception:
                        await self._delete_tracked(sandbox,user_slot=False)
                        raise
                else:
                    sandbox=await self._create_for_owner(SandboxCreateRequest(harness=harness,
                        profile=desired_profile,environment=environment,
                        ttl_seconds=self.ttl_seconds,allocation_id=allocation_id),
                        thread_id,retain_unresolved_allocation,before_create,create)
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                sandbox.reused = False
                async with self._lock:
                    closed = self._closed
                    if not closed:
                        self._attached[thread_id] = sandbox
                        self._pending_allocations.pop(pending_key,None)
                if closed:
                    # The local exception path releases this reservation.
                    await self._delete_tracked(sandbox, user_slot=False)
                    async with self._lock:self._pending_allocations.pop(pending_key,None)
                    raise RuntimeError("sandbox_pool_closed")
            except BaseException:
                async with self._lock:pending=pending_key in self._pending_allocations
                if not pending:
                    self._release_user_slot()
                    self._capacity_changed.set()
                raise
            self._schedule_refill(harness)
            return sandbox

    @observed("sandbox.adopt")
    async def adopt(self, sandbox: Sandbox, *, thread_id: str, turn_id: str,
                    expected_profile:SandboxProfile|None=None) -> Sandbox:
        async with self._thread_lock(thread_id):
            expected_profile=expected_profile or self.profile_resolver.resolve(
                sandbox.harness,self.images[sandbox.harness])
            if sandbox.compatibility_key!=expected_profile.compatibility_key:
                raise SandboxProfileUnavailable("Sandbox profile is incompatible with this release",
                    provider=self.backend.provider)
            async with self._lock:
                if self._closed:
                    raise RuntimeError("sandbox_pool_closed")
                existing = self._attached.get(thread_id)
                if existing:
                    if self.sandbox_key(existing)!=self.sandbox_key(sandbox):
                        raise RuntimeError("Thread already owns another sandbox")
                    return existing
                pending_key=(sandbox.handle.provider,sandbox.handle.allocation_id)
                pending=self._pending_allocations.get(pending_key)
                retained=self._retained_allocations.get(pending_key)
                if retained and retained!=expected_profile.compatibility_key:
                    raise SandboxProfileUnavailable('Retained allocation profile mismatch',provider=sandbox.handle.provider)
                if pending and (pending[0]!=thread_id or
                        pending[1].compatibility_key!=expected_profile.compatibility_key):
                    raise SandboxProfileUnavailable('Pending allocation owner/profile mismatch',
                        provider=sandbox.handle.provider)
            if not pending and not retained:await self._reserve_user_slot()
            try:
                await self._bind(sandbox,thread_id=thread_id,turn_id=turn_id)
                sandbox.thread_id = thread_id
                sandbox.turn_id = turn_id
                sandbox.warm = False
                async with self._lock:
                    if self._closed:
                        raise RuntimeError("sandbox_pool_closed")
                    self._attached[thread_id] = sandbox
                    self._pending_allocations.pop(pending_key,None)
                    self._retained_allocations.pop(pending_key,None)
                return sandbox
            except BaseException:
                if not pending and not retained:
                    self._release_user_slot()
                    self._capacity_changed.set()
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
                    self._capacity_changed.set()
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
            self._release_user_slot()
            self._capacity_changed.set()

    async def discard_thread(self, thread_id: str) -> None:
        async with self._thread_lock(thread_id):
            async with self._lock:
                expiry = self._expiry.pop(thread_id, None)
                sandbox = self._attached.pop(thread_id, None)
                self._idle_since.pop(thread_id, None)
                retiring = {self.sandbox_key(value): (value, user_slot)
                    for value, user_slot in self._retiring.values() if value.thread_id == thread_id}
                if sandbox:
                    retiring[self.sandbox_key(sandbox)] = (sandbox, True)
            if expiry:
                expiry.cancel()
            for value, user_slot in retiring.values():
                await self._delete_tracked(value, user_slot=user_slot)
            async with self._lock:
                if (any(value.thread_id == thread_id for value, _ in self._retiring.values())
                        or any(owner == thread_id for owner, _ in self._pending_allocations.values())):
                    raise RuntimeError("sandbox_cleanup_pending")

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

    async def close(self, *, preserve_sandbox_ids: set[str] | None = None,
                    preserve_sandbox_keys:set[tuple[str,str]]|None=None) -> None:
        preserve_sandbox_ids = preserve_sandbox_ids or set()
        preserve_sandbox_keys=preserve_sandbox_keys or set()
        async with self._lock:
            self._closed = True
            self._capacity_changed.set()
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
            if (sandbox.id not in preserve_sandbox_ids
                    and self.sandbox_key(sandbox) not in preserve_sandbox_keys):
                try:
                    await self._delete_tracked(sandbox, user_slot=not sandbox.warm)
                except Exception:
                    event("sandbox.shutdown.delete_pending",level=logging.WARNING,sandbox_id=sandbox.id)
        await self.retry_deletions()
