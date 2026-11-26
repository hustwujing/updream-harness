"""Durable Thread sandbox intent, binding and fenced cleanup accounting.

This repository does no provider I/O. A cleanup worker may confirm absence only
after deleting or observing authoritative absence of the frozen known handle.
Unknown creation without a handle remains owned until reconciliation finds it.
"""
from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import or_, select

from sandbox_harness.common.contracts.domain import now
from sandbox_harness.common.contracts.sandbox_contracts import SandboxHandle
from sandbox_harness.common.sandbox.allocation_identity import encode_request
from .domain_database import DomainLifecycle, DomainScope
from .models import ThreadRow, canonical_digest, canonical_json, utc_datetime
from .ownership_models import SandboxOwnershipRow


class SandboxOwnership:
    def __init__(self, core, namespace):
        if core.domain != "core":
            raise ValueError("sandbox_ownership_authority_invalid")
        self.core, self.namespace = core, namespace

    def identity(self, provider, allocation_id):
        return canonical_digest([self.namespace, provider, allocation_id])

    @staticmethod
    def _view(row):
        if row is None:
            return None
        result = {name: getattr(row, name) for name in ("id", "namespace", "provider", "allocation_id",
            "owner_id", "thread_id", "profile_key", "state", "operation_id", "claim_owner", "claim_generation",
            "claim_expires_at", "error_code", "request_json")}
        result["handle"] = json.loads(row.handle_json) if row.handle_json else None
        return result

    async def get(self, identity):
        async with self.core.connections() as db:
            row = await db.get(SandboxOwnershipRow, identity)
            return self._view(row) if row and row.namespace == self.namespace else None

    async def resume_create(self, owner_id, thread_id, request):
        """Return the immutable existing intent; never authorizes a second create."""
        identity = self.identity(request.profile.provider, request.allocation_id)
        digest = canonical_digest(json.loads(encode_request(request)))
        async with self.core.connections() as db:
            row = await db.get(SandboxOwnershipRow, identity)
            if row is None:
                return None
            if (row.namespace != self.namespace or row.owner_id != owner_id
                    or row.thread_id != thread_id or row.request_digest != digest):
                raise ValueError("sandbox_ownership_identity_conflict")
            return self._view(row)

    async def begin_create(self, owner_id, thread_id, request):
        identity = self.identity(request.profile.provider, request.allocation_id)
        encoded = encode_request(request)
        digest = canonical_digest(json.loads(encoded))
        async with self.core.transaction() as db:
            await self.core.handoff._identity_lock(db, "sandbox-ownership", identity)
            row = await db.get(SandboxOwnershipRow, identity)
            if row:
                if row.owner_id != owner_id or row.thread_id != thread_id or row.request_digest != digest:
                    raise ValueError("sandbox_ownership_identity_conflict")
                if row.state == "RELEASED" and not row.handle_json:
                    version = await self.core.lifecycle.admit(db, owner_id, thread_id)
                    row.state = "CREATING"
                    row.owner_generation = version.owner_generation
                    row.thread_generation = version.thread_generation
                    row.error_code = ""
                    row.absent_confirmed_at = None
                    row.capacity_released_at = None
                    row.updated_at = now()
                    return identity
                raise RuntimeError("sandbox_creation_requires_reconciliation")
            version = await self.core.lifecycle.admit(db, owner_id, thread_id)
            thread = await db.scalar(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())
            if thread is None:
                raise KeyError("thread_not_found")
            if await db.scalar(select(SandboxOwnershipRow.id).where(
                    SandboxOwnershipRow.namespace == self.namespace,
                    SandboxOwnershipRow.thread_id == thread_id,
                    SandboxOwnershipRow.state.in_(["CREATING", "ALLOCATED"])).limit(1)):
                raise RuntimeError("sandbox_creation_requires_reconciliation")
            db.add(SandboxOwnershipRow(id=identity, namespace=self.namespace, provider=request.profile.provider,
                allocation_id=request.allocation_id, owner_id=owner_id, thread_id=thread_id,
                profile_key=request.profile.compatibility_key, request_json=encoded, request_digest=digest,
                owner_generation=version.owner_generation, thread_generation=version.thread_generation,
                state="CREATING", created_at=now(), updated_at=now()))
        return identity

    async def record_handle(self, identity, handle: SandboxHandle):
        async with self.core.transaction() as db:
            row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
            if (row is None or row.namespace != self.namespace or row.provider != handle.provider
                    or row.allocation_id != handle.allocation_id or row.profile_key != handle.profile_key
                    or json.loads(row.request_json)["profile"]["revision"] != handle.revision):
                raise ValueError("sandbox_ownership_handle_conflict")
            encoded = canonical_json(handle.to_mapping())
            if row.handle_json and row.handle_json != encoded:
                raise ValueError("sandbox_ownership_handle_conflict")
            row.handle_json = encoded
            if row.state == "CREATING":
                row.state = "ALLOCATED"
            row.updated_at = now()

    async def record_existing(self, owner_id, thread_id, request, handle: SandboxHandle):
        """Take durable ownership of a provider object before binding it."""
        identity = self.identity(handle.provider, handle.allocation_id)
        encoded = encode_request(request)
        digest = canonical_digest(json.loads(encoded))
        async with self.core.transaction() as db:
            await self.core.handoff._identity_lock(db, "sandbox-ownership", identity)
            version = await self.core.lifecycle.admit(db, owner_id, thread_id)
            row = await db.get(SandboxOwnershipRow, identity)
            if row:
                if (row.owner_id != owner_id or row.thread_id != thread_id
                        or row.request_digest != digest or row.handle_json != canonical_json(handle.to_mapping())):
                    raise ValueError("sandbox_ownership_identity_conflict")
                return identity
            thread = await db.scalar(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())
            if thread is None:
                raise KeyError("thread_not_found")
            db.add(SandboxOwnershipRow(id=identity, namespace=self.namespace, provider=handle.provider,
                allocation_id=handle.allocation_id, owner_id=owner_id, thread_id=thread_id,
                profile_key=handle.profile_key, request_json=encoded, request_digest=digest,
                handle_json=canonical_json(handle.to_mapping()), owner_generation=version.owner_generation,
                thread_generation=version.thread_generation, state="ALLOCATED",
                created_at=now(), updated_at=now()))
        return identity

    async def authorization(self, identity, owner_id, thread_id):
        async with self.core.transaction() as db:
            version = await self.core.lifecycle.admit(db, owner_id, thread_id)
            row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
            if (row is None or row.namespace != self.namespace or row.owner_id != owner_id
                    or row.thread_id != thread_id or not row.handle_json
                    or row.state not in {"ALLOCATED", "LIVE", "RETAINED"}):
                raise ValueError("sandbox_ownership_binding_conflict")
            if (row.owner_generation != version.owner_generation
                    or row.thread_generation != version.thread_generation):
                raise RuntimeError("sandbox_ownership_generation_stale")
            return version

    async def bind(self, identity, owner_id, thread_id, *, owner_generation, thread_generation):
        async with self.core.transaction() as db:
            version = await self.core.lifecycle.admit(db, owner_id, thread_id,
                owner_generation=owner_generation, generation=thread_generation)
            row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
            if (row is None or row.namespace != self.namespace or row.owner_id != owner_id
                    or row.thread_id != thread_id or not row.handle_json
                    or row.state not in {"ALLOCATED", "LIVE", "RETAINED"}):
                raise ValueError("sandbox_ownership_binding_conflict")
            if (row.owner_generation != version.owner_generation
                    or row.thread_generation != version.thread_generation):
                raise RuntimeError("sandbox_ownership_generation_stale")
            row.state = "LIVE"
            row.updated_at = now()
            return SandboxHandle.from_mapping(json.loads(row.handle_json))

    async def mark_delete_pending(self, handle: SandboxHandle):
        identity = self.identity(handle.provider, handle.allocation_id)
        async with self.core.transaction() as db:
            row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
            if row is None:
                return False
            if row.handle_json != canonical_json(handle.to_mapping()):
                raise ValueError("sandbox_ownership_handle_conflict")
            if row.state != "RELEASED":
                row.state = "DELETE_PENDING"
                row.updated_at = now()
            return True

    async def confirm_direct_release(self, handle: SandboxHandle):
        identity = self.identity(handle.provider, handle.allocation_id)
        async with self.core.transaction() as db:
            row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
            if row is None:
                return False
            if row.handle_json != canonical_json(handle.to_mapping()):
                raise ValueError("sandbox_ownership_handle_conflict")
            if row.state != "RELEASED":
                timestamp = now()
                row.state = "RELEASED"
                row.absent_confirmed_at = row.absent_confirmed_at or timestamp
                row.capacity_released_at = timestamp
                row.claim_owner, row.claim_expires_at = "", None
                row.updated_at = timestamp
                await self.core._record_usage_locked(db,idempotency_key="sandbox:"+row.id,
                    owner_id=row.owner_id,thread_id=row.thread_id,turn_id="",job_id="",
                    kind="sandbox_seconds",amount=max(0,int((timestamp-
                        utc_datetime(row.created_at)).total_seconds())),unit="second",
                    source="sandbox")
            return True

    async def reject_unstarted_create(self, identity):
        """Release only a provider request proven rejected before acceptance."""
        async with self.core.transaction() as db:
            row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
            if row is None:
                raise ValueError("sandbox_ownership_missing")
            if row.state == "RELEASED":
                return
            if row.state != "CREATING" or row.handle_json:
                raise RuntimeError("sandbox_create_rejection_not_safe")
            timestamp = now()
            row.state = "RELEASED"
            row.error_code = "sandbox_create_rejected"
            row.absent_confirmed_at = timestamp
            row.capacity_released_at = timestamp
            row.updated_at = timestamp

    async def request_cleanup(self, db, proposal):
        if proposal.action not in {"archive", "delete"}:
            return
        scope = await db.get(DomainScope, proposal.scope_id)
        if scope is None or scope.frozen_by != proposal.operation_id or scope.generation != proposal.generation:
            raise RuntimeError("stale_sandbox_cleanup_operation")
        rows = (await db.scalars(select(SandboxOwnershipRow).where(
            SandboxOwnershipRow.namespace == self.namespace, SandboxOwnershipRow.owner_id == proposal.owner_id,
            SandboxOwnershipRow.thread_id == proposal.thread_id,
            SandboxOwnershipRow.state != "RELEASED").with_for_update())).all()
        for row in rows:
            if row.operation_id and row.operation_id != proposal.operation_id:
                raise RuntimeError("sandbox_cleanup_already_owned")
            row.operation_id = proposal.operation_id
            if row.state != "ABSENT":
                row.state = "DELETE_PENDING"
            row.updated_at = now()

    async def cleanup_ready(self, db, proposal):
        if proposal.action not in {"archive", "delete"}:
            return True
        return await db.scalar(select(SandboxOwnershipRow.id).where(
            SandboxOwnershipRow.namespace == self.namespace, SandboxOwnershipRow.thread_id == proposal.thread_id,
            SandboxOwnershipRow.state != "RELEASED").limit(1)) is None

    async def claim_cleanup(self, owner, *, limit=16, lease_seconds=30):
        if not owner or not 1 <= limit <= 100 or not 1 <= lease_seconds <= 300:
            raise ValueError("sandbox_cleanup_claim_invalid")
        async with self.core.transaction() as db:
            rows = (await db.scalars(select(SandboxOwnershipRow).join(DomainLifecycle,
                DomainLifecycle.id == SandboxOwnershipRow.operation_id).where(
                SandboxOwnershipRow.namespace == self.namespace,
                SandboxOwnershipRow.state.in_(["DELETE_PENDING", "ABSENT"]),
                DomainLifecycle.phase == "committing",
                or_(SandboxOwnershipRow.claim_expires_at.is_(None), SandboxOwnershipRow.claim_expires_at <= now()),
                or_(SandboxOwnershipRow.next_attempt_at.is_(None), SandboxOwnershipRow.next_attempt_at <= now()))
                .order_by(SandboxOwnershipRow.created_at).limit(limit).with_for_update(
                    of=SandboxOwnershipRow, skip_locked=True))).all()
            for row in rows:
                row.claim_owner, row.claim_generation = owner, row.claim_generation + 1
                row.claim_expires_at = now() + timedelta(seconds=lease_seconds)
                row.updated_at = now()
            return [self._view(row) for row in rows]

    async def _claimed(self, db, identity, owner, generation):
        row = await db.get(SandboxOwnershipRow, identity, with_for_update=True)
        if (row is None or row.namespace != self.namespace or row.claim_owner != owner
                or row.claim_generation != generation or row.claim_expires_at is None
                or utc_datetime(row.claim_expires_at) <= now()):
            raise RuntimeError("stale_sandbox_cleanup_claim")
        return row

    async def confirm_absent(self, identity, owner, generation):
        async with self.core.transaction() as db:
            row = await self._claimed(db, identity, owner, generation)
            if not row.handle_json:
                raise RuntimeError("sandbox_absence_not_proven")
            if row.state not in {"DELETE_PENDING", "ABSENT"}:
                raise RuntimeError("sandbox_cleanup_state_conflict")
            row.state, row.absent_confirmed_at, row.updated_at = "ABSENT", row.absent_confirmed_at or now(), now()

    async def confirm_capacity_released(self, identity, owner, generation):
        async with self.core.transaction() as db:
            row = await self._claimed(db, identity, owner, generation)
            if row.state != "ABSENT" or row.absent_confirmed_at is None:
                raise RuntimeError("sandbox_absence_not_proven")
            timestamp=now()
            row.state, row.capacity_released_at, row.updated_at = "RELEASED", timestamp, timestamp
            row.claim_owner, row.claim_expires_at = "", None
            await self.core._record_usage_locked(db,idempotency_key="sandbox:"+row.id,
                owner_id=row.owner_id,thread_id=row.thread_id,turn_id="",job_id="",
                kind="sandbox_seconds",amount=max(0,int((timestamp-
                    utc_datetime(row.created_at)).total_seconds())),unit="second",
                source="sandbox")

    async def renew_cleanup(self, identity, owner, generation, lease_seconds=30):
        async with self.core.transaction() as db:
            row = await self._claimed(db, identity, owner, generation)
            row.claim_expires_at = now() + timedelta(seconds=lease_seconds)

    async def defer_cleanup(self, identity, owner, generation, error_code):
        allowed = {"sandbox_backend_unavailable", "sandbox_creation_outcome_unknown", "sandbox_still_present",
                   "sandbox_cleanup_failed", "sandbox_cleanup_interrupted"}
        if error_code not in allowed:
            raise ValueError("sandbox_cleanup_error_invalid")
        async with self.core.transaction() as db:
            row = await self._claimed(db, identity, owner, generation)
            row.attempts += 1
            row.error_code = error_code
            row.next_attempt_at = now() + timedelta(seconds=min(60, 2 ** min(row.attempts, 6)))
            row.claim_owner, row.claim_expires_at = "", None
            row.updated_at = now()

    async def capacity_obligations(self):
        async with self.core.connections() as db:
            rows = (await db.scalars(select(SandboxOwnershipRow).where(
                SandboxOwnershipRow.namespace == self.namespace, SandboxOwnershipRow.state != "RELEASED"))).all()
            return [{"provider": row.provider, "allocation_id": row.allocation_id, "profile_key": row.profile_key}
                    for row in rows]
