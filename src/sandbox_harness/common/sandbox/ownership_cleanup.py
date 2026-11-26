"""Retryable cleanup for committed Thread lifecycle operations."""
from __future__ import annotations

import asyncio
from uuid import uuid4

from sandbox_harness.common.contracts.sandbox_contracts import Sandbox, SandboxHandle, SandboxNotFound, SandboxScope
from .allocation_identity import decode_request


class CleanupPending(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class OwnershipCleanup:
    def __init__(self, ledger, backend, pool, *, owner=None, lease_seconds=30, concurrency=4):
        self.ledger, self.backend, self.pool = ledger, backend, pool
        self.owner = owner or "thread-cleanup-" + uuid4().hex
        if not 3 <= lease_seconds <= 300 or not 1 <= concurrency <= 16:
            raise ValueError("sandbox_cleanup_worker_limits_invalid")
        self.lease_seconds, self.concurrency = lease_seconds, concurrency

    async def _renew(self, claim):
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            await self.ledger.renew_cleanup(claim["id"], self.owner, claim["claim_generation"], self.lease_seconds)

    async def _delete_and_release(self, claim):
        identity, generation = claim["id"], claim["claim_generation"]
        if claim["state"] != "ABSENT":
            if (self.backend.namespace != claim["namespace"] or self.backend.provider != claim["provider"]
                    or not self.backend.capabilities.delete_convergence):
                raise CleanupPending("sandbox_backend_unavailable")
            handle = SandboxHandle.from_mapping(claim["handle"]) if claim["handle"] else None
            if handle is None:
                sandbox = await self.backend.reconcile_create(SandboxScope(claim["namespace"]), claim["allocation_id"])
                if sandbox is None:
                    raise CleanupPending("sandbox_creation_outcome_unknown")
                handle = sandbox.handle
                await self.ledger.record_handle(identity, handle)
            request = decode_request(claim["request_json"])
            sandbox = Sandbox(id=handle.sandbox_id, provider=handle.provider, harness=request.harness,
                              profile=request.profile, handle=handle, metadata={})
            try:
                # delete_convergence is a required backend contract: successful
                # return confirms infrastructure absence without Runtime health.
                await self.backend.delete(sandbox)
            except SandboxNotFound:
                pass
            await self.ledger.confirm_absent(identity, self.owner, generation)
        await self.pool.forget_absent_allocation(claim["provider"], claim["allocation_id"])
        await self.ledger.confirm_capacity_released(identity, self.owner, generation)

    async def _one(self, claim):
        heartbeat = asyncio.create_task(self._renew(claim))
        work = asyncio.create_task(self._delete_and_release(claim))
        try:
            done, _ = await asyncio.wait({heartbeat, work}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                await work
                return True
            await heartbeat
            raise RuntimeError("sandbox_cleanup_heartbeat_ended")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, CleanupPending) else "sandbox_cleanup_failed"
            try:
                await self.ledger.defer_cleanup(claim["id"], self.owner, claim["claim_generation"], code)
            except RuntimeError:
                # A replacement generation owns any later authoritative writes.
                pass
            return False
        finally:
            heartbeat.cancel()
            work.cancel()
            await asyncio.gather(heartbeat, work, return_exceptions=True)

    async def run_once(self):
        claims = await self.ledger.claim_cleanup(self.owner, limit=self.concurrency,
                                                lease_seconds=self.lease_seconds)
        results = await asyncio.gather(*(self._one(claim) for claim in claims))
        return {"released": sum(results), "pending": len(results) - sum(results)}

    async def run_retained_once(self):
        claims = await self.ledger.claim_retained_cleanup(self.owner, limit=self.concurrency,
                                                         lease_seconds=self.lease_seconds)
        results = await asyncio.gather(*(self._one(claim) for claim in claims))
        return {"released": sum(results), "pending": len(results) - sum(results)}
