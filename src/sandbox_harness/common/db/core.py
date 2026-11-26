"""Core execution authority with a single independently owned database."""
from __future__ import annotations
import json
from sqlalchemy import select, update
from sandbox_harness.common.events.events import RedisEventBus
from sandbox_harness.common.contracts.domain import TERMINAL, ThreadControlState
from sandbox_harness.common.contracts.job_contracts import TERMINAL_JOB_STATES
from .domain_database import DomainDatabase, DomainOutbox, DomainScope
from .lifecycle import LifecycleCoordinator, LifecycleParticipant
from .core_lifecycle import CoreThreadLifecycle
from .sandbox_ownership import SandboxOwnership
from .ownership_models import SandboxOwnershipRow
from .models import (JobGroupDraftRow, JobRow, SandboxControlRow, ThreadControlRow, ThreadRow, TurnRow, canonical_digest)
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


class CoreRepository(CoreThreadLifecycle, DomainDatabase, BaseConnections, CapabilitiesTransactions, ViewsTransactions, PauseTransactions, BudgetsTransactions, JobActivityTransactions, JobGroupsTransactions, JobAttemptsTransactions, AllocationsTransactions, CleanupTransactions, JobTerminalTransactions, JobArtifactsTransactions, EventsTransactions, ThreadsTransactions, SandboxTransactions, TurnsTransactions, FilesTransactions, InteractionsTransactions):
    def __init__(self, database_url: str, redis_url: str = "", redis_prefix: str = "sandbox-harness:", *, namespace: str):
        DomainDatabase.__init__(self, "core", database_url)
        self.events = RedisEventBus(redis_url, redis_prefix) if redis_url else None
        self._initialize_locks()
        self.ownership = SandboxOwnership(self, namespace)
        self.lifecycle = LifecycleParticipant(self, self._check_lifecycle, self._apply_lifecycle,
            prepare_commit=self.ownership.request_cleanup, commit_ready=self.ownership.cleanup_ready)
        self.coordinator = LifecycleCoordinator(self.lifecycle)
        self.handoff = self.lifecycle.handoff

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
        await self.handoff.enqueue(db, command_id=identity, target="media", kind="media.turn_finished",
            aggregate_id=scope_id, owner_id=thread.owner_id, generation=scope.generation if scope else 0,
            payload={"thread_id": thread.id, "turn_id": turn_id, "origin_finished": True,
                     "owner_generation": owner.generation if owner else 0})

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
            return "active_work_prevents_registration" if any((active_turn, active_job, control, pause, creating)) else ""
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
