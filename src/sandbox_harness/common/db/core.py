"""Core execution authority with a single independently owned database."""
from __future__ import annotations
import json
from pathlib import PurePosixPath
from sqlalchemy import select, update
from sandbox_harness.common.events.events import RedisEventBus
from sandbox_harness.common.contracts.domain import TERMINAL, ThreadControlState, now
from sandbox_harness.common.contracts.job_contracts import TERMINAL_JOB_STATES
from .domain_database import DomainDatabase, DomainLifecycle, DomainOutbox, DomainScope
from .lifecycle import LifecycleCoordinator, LifecycleParticipant, ScopeVersion
from .core_lifecycle import CoreThreadLifecycle
from .sandbox_ownership import SandboxOwnership
from .ownership_models import RemoteObligationRow, SandboxOwnershipRow
from .models import (ArtifactRow, JobGroupDraftRow, JobRow, SandboxControlRow,
                     ThreadControlRow, ThreadRow, TurnRow, canonical_digest)
from .base import BaseConnections
from .capabilities import CapabilitiesTransactions
from .views import ViewsTransactions
from .pause import PauseTransactions
from .budgets import BudgetsTransactions
from .job_activity import JobActivityTransactions
from .job_groups import JobGroupsTransactions
from .job_attempts import JobAttemptsTransactions
from .allocations import AllocationsTransactions
from .cleanup import CleanupTransactions
from .job_terminal import JobTerminalTransactions
from .job_artifacts import JobArtifactsTransactions
from .events import EventsTransactions
from .threads import ThreadsTransactions
from .sandbox import SandboxTransactions
from .turns import TurnsTransactions
from .files import FilesTransactions
from .interactions import InteractionsTransactions
from .leaders import PlatformLeaderTransactions
from .usage import UsageTransactions
from .provider_circuits import ProviderCircuitTransactions


class CoreRepository(CoreThreadLifecycle, DomainDatabase, BaseConnections, CapabilitiesTransactions, ViewsTransactions, PauseTransactions, BudgetsTransactions, JobActivityTransactions, JobGroupsTransactions, JobAttemptsTransactions, AllocationsTransactions, CleanupTransactions, JobTerminalTransactions, JobArtifactsTransactions, EventsTransactions, ThreadsTransactions, SandboxTransactions, TurnsTransactions, FilesTransactions, InteractionsTransactions, PlatformLeaderTransactions, UsageTransactions, ProviderCircuitTransactions):
    def __init__(self, database_url: str, redis_url: str = "", redis_prefix: str = "sandbox-harness:", *,
                 namespace: str, handoff_max_attempts: int = 8,
                 handoff_max_age_seconds: int = 900):
        DomainDatabase.__init__(self, "core", database_url)
        self.events = RedisEventBus(redis_url, redis_prefix) if redis_url else None
        self._initialize_locks()
        self.sandbox_namespace = namespace
        self.ownership = SandboxOwnership(self, namespace)
        self.lifecycle = LifecycleParticipant(self, self._check_lifecycle, self._apply_lifecycle,
            prepare_commit=self.ownership.request_cleanup, commit_ready=self.ownership.cleanup_ready,
            handoff_max_attempts=handoff_max_attempts,
            handoff_max_age_seconds=handoff_max_age_seconds)
        self.coordinator = LifecycleCoordinator(self.lifecycle)
        self.handoff = self.lifecycle.handoff
        self.handoff.handlers["usage.record"] = self._receive_usage

    async def _receive_usage(self,db,envelope):
        value=envelope.payload
        if (envelope.source!="media" or envelope.owner_id!=value["owner_id"]
                or envelope.aggregate_id!=value["job_id"]):
            raise ValueError("usage_envelope_invalid")
        await self._record_usage_locked(db,**value)
        return {"recorded":True}

    async def initialize(self):
        await DomainDatabase.initialize(self)
        if self.events:
            await self.events.initialize()

    async def close(self):
        if self.events:
            await self.events.close()
        await DomainDatabase.close(self)

    async def healthcheck(self):
        async with self.connections() as session:
            await session.execute(select(1))
        return True

    async def _admit_new_work(self, db, owner_id, thread_id):
        return await self.lifecycle.admit(db, owner_id, thread_id)

    async def _seal_media_batch_on_turn_finish(self, db, turn_id):
        identity = canonical_digest(["turn.finished", turn_id])
        if await db.get(DomainOutbox, identity):
            return
        turn = await db.get(TurnRow, turn_id)
        thread = await db.get(ThreadRow, turn.thread_id)
        scope_id = canonical_digest([thread.owner_id, thread.id])
        scope = await db.get(DomainScope, scope_id)
        owner = await db.get(DomainScope, canonical_digest([thread.owner_id, ""]))
        generation=scope.generation if scope else 0
        await self.enqueue_remote(db, command_id=identity, target="media", kind="media.turn.finished",
            aggregate_id=scope_id, owner_id=thread.owner_id, generation=scope.generation if scope else 0,
            thread_id=thread.id, payload={"owner_id":thread.owner_id,"thread_id": thread.id,
                "turn_id": turn_id, "origin_finished": True,
                "owner_generation": owner.generation if owner else 0,
                "thread_generation":generation})

    async def _wake_media_batches_on_resume(self,db,thread,control,resume_turn_id):
        identity=canonical_digest(["media.thread.resumed",thread.id,str(control.generation),resume_turn_id])
        if await db.get(DomainOutbox,identity):return
        scope_id=canonical_digest([thread.owner_id,thread.id])
        scope=await db.get(DomainScope,scope_id)
        owner=await db.get(DomainScope,canonical_digest([thread.owner_id,""]))
        generation=scope.generation if scope else 0
        await self.enqueue_remote(db,command_id=identity,target="media",kind="media.thread.resumed",
            aggregate_id=scope_id,owner_id=thread.owner_id,generation=generation,thread_id=thread.id,
            payload={"owner_id":thread.owner_id,"thread_id":thread.id,"turn_id":"",
                "owner_generation":owner.generation if owner else 0,
                "thread_generation":generation,"resume_turn_id":resume_turn_id,
                "pause_generation":control.generation})

    async def _check_lifecycle(self, db, proposal):
        if proposal.action == "register":
            active_turn = await db.scalar(select(TurnRow.id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(ThreadRow.owner_id == proposal.owner_id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1))
            active_job = await db.scalar(select(JobRow.id).where(JobRow.owner_id == proposal.owner_id,
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES])).limit(1))
            control = await db.scalar(select(SandboxControlRow.thread_id).where(
                SandboxControlRow.owner_id == proposal.owner_id).limit(1))
            pause = await db.scalar(select(ThreadControlRow.thread_id).where(
                ThreadControlRow.owner_id == proposal.owner_id,
                ThreadControlRow.state.in_([ThreadControlState.PAUSING.value, ThreadControlState.RESUMING.value]))
                .limit(1))
            creating = await db.scalar(select(SandboxOwnershipRow.id).where(
                SandboxOwnershipRow.owner_id == proposal.owner_id,
                SandboxOwnershipRow.state.in_(["CREATING", "ALLOCATED"])).limit(1))
            remote = await db.scalar(select(RemoteObligationRow.id).where(
                RemoteObligationRow.owner_id == proposal.owner_id,
                RemoteObligationRow.state == "pending").limit(1))
            return "active_work_prevents_registration" if any(
                (active_turn, active_job, control, pause, creating, remote)) else ""
        return await self._check_thread_lifecycle(db, proposal)

    async def _apply_lifecycle(self, db, proposal):
        if proposal.action == "register":
            await db.execute(update(SandboxOwnershipRow).where(
                SandboxOwnershipRow.owner_id == proposal.owner_id,
                SandboxOwnershipRow.owner_generation == proposal.generation,
                SandboxOwnershipRow.handle_json != "",
                SandboxOwnershipRow.state.in_(["LIVE", "RETAINED"])).values(
                    owner_generation=proposal.generation + 1))
        else:
            await self._apply_thread_lifecycle(db, proposal)

    async def _admit_job_draft(self, db, owner_id, draft_id):
        thread_id = await db.scalar(select(JobGroupDraftRow.thread_id).where(
            JobGroupDraftRow.id == draft_id, JobGroupDraftRow.owner_id == owner_id))
        if thread_id:
            await self._admit_new_work(db, owner_id, thread_id)

    async def owner_id_for_thread(self, thread_id):
        async with self.connections() as db:
            return await db.scalar(select(ThreadRow.owner_id).where(ThreadRow.id == thread_id)) or ""

    async def artifact_site_file(self, artifact_id, path, owner_id):
        async with self.connections() as db:
            root = await db.scalar(select(ArtifactRow).join(TurnRow,
                TurnRow.id == ArtifactRow.turn_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(
                    ArtifactRow.id == artifact_id, ThreadRow.owner_id == owner_id))
            if not root or PurePosixPath(root.path).name.lower() != "index.html":
                return None
            prefix = PurePosixPath(root.path).parent
            target = (prefix / path).as_posix() if prefix.as_posix() != "." else path
            row = await db.scalar(select(ArtifactRow).where(
                ArtifactRow.turn_id == root.turn_id, ArtifactRow.path == target))
        return ({"object_ref":row.object_ref,"size":row.size,"sha256":row.sha256,
                 "mime":row.mime} if row else None)

    async def lifecycle_version(self, owner_id, thread_id):
        async with self.transaction() as db:
            owner = await self.lifecycle._scope(db, owner_id, "")
            scope = await self.lifecycle._scope(db, owner_id, thread_id) if thread_id else owner
            if owner.frozen_by or scope.frozen_by or scope.state == "deleted":
                raise RuntimeError("lifecycle_admission_blocked")
            if thread_id:
                thread = await db.get(ThreadRow, thread_id)
                if thread is None or thread.owner_id != owner_id:
                    raise KeyError("thread_not_found")
            return ScopeVersion(owner.generation, scope.generation)

    async def enqueue_remote(self, db, *, command_id, target, kind, aggregate_id,
                             owner_id, thread_id, generation, payload):
        await self.handoff.enqueue(db, command_id=command_id, target=target, kind=kind,
            aggregate_id=aggregate_id, owner_id=owner_id, generation=generation, payload=payload)
        row = await db.get(RemoteObligationRow, command_id)
        if row:
            if (row.owner_id != owner_id or row.thread_id != thread_id
                    or row.target != target or row.kind != kind):
                raise ValueError("remote_obligation_identity_conflict")
        else:
            db.add(RemoteObligationRow(id=command_id, owner_id=owner_id, thread_id=thread_id,
                target=target, kind=kind, state="pending", created_at=now(), updated_at=now()))

    async def relay_remote(self, destinations, limit=100):
        result = await self.handoff.relay(destinations, limit=limit)
        async with self.transaction() as db:
            rows = list((await db.scalars(select(RemoteObligationRow).join(DomainOutbox,
                DomainOutbox.id == RemoteObligationRow.id).where(
                    RemoteObligationRow.state == "pending",
                    DomainOutbox.state == "acknowledged").with_for_update())).all())
            for row in rows:
                row.state = "acknowledged"; row.updated_at = now()
        return result

    async def pending_lifecycle_operations(self, limit=100):
        async with self.connections() as db:
            return list((await db.scalars(select(DomainLifecycle.id).where(
                DomainLifecycle.phase.not_in(["completed", "refused"])).order_by(
                    DomainLifecycle.id).limit(limit))).all())
