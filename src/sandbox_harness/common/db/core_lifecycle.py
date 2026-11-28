"""Core-only lifecycle checks and local deletion effects."""
from sqlalchemy import delete, or_, select, update
from sandbox_harness.common.contracts.domain import TERMINAL, now
from sandbox_harness.common.contracts.job_contracts import JobState, JobGroupState, TERMINAL_JOB_STATES
from .models import (ArtifactRow, AuthorizationSnapshotRow, BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, CheckpointVersionRow, EventRow, InteractionRow, ItemRow, JobActivityRow, JobArtifactRow, JobAttemptRow, JobCleanupRow, JobDisplayActivityRow, JobGroupDraftRow, JobGroupRow, JobLeaseRow, JobOutboxRow, JobRow, PauseParticipantRow, ProjectFileRow, ResourceReservationRow, ReviewGateRow, SandboxAllocationRow, SandboxAuditRow, SandboxControlRow, SandboxPortGrantRow, ThreadControlRow, ThreadRow, TurnInputRow, TurnOrderRow, TurnProjectFileRow, TurnRow)
from .domain_database import DomainOutbox
from .ownership_models import RemoteObligationRow


class CoreThreadLifecycle:
    async def _thread_jobs_quiescent(self, db, thread_id):
        groups=list((await db.execute(select(JobGroupRow).where(
            JobGroupRow.thread_id==thread_id).with_for_update())).scalars().all())
        group_ids=[group.id for group in groups]
        jobs=list((await db.execute(select(JobRow).where(
            JobRow.group_id.in_(group_ids or [""])).with_for_update())).scalars().all())
        job_ids=[job.id for job in jobs]
        cleanups=list((await db.execute(select(JobCleanupRow).where(
            JobCleanupRow.job_id.in_(job_ids or [""])).with_for_update())).scalars().all())
        resource_rows=list((await db.execute(select(ResourceReservationRow).where(
            ResourceReservationRow.job_id.in_(job_ids or [""])).with_for_update())).scalars().all())
        allocations=list((await db.scalars(select(SandboxAllocationRow).where(
            SandboxAllocationRow.job_id.in_(job_ids or [''])).with_for_update())).all())
        reservations=list((await db.execute(select(BudgetReservationRow).where(
            BudgetReservationRow.group_id.in_(group_ids or [""])).with_for_update())).scalars().all())
        outboxes=list((await db.execute(select(JobOutboxRow).where(
            JobOutboxRow.group_id.in_(group_ids or [""])).with_for_update())).scalars().all())
        if (any(JobState(job.state) not in TERMINAL_JOB_STATES for job in jobs)
                or any(row.state!="completed" for row in cleanups)
                or any(row.state!="released" for row in resource_rows)
                or any(row.state not in {'REJECTED','CANCELLED','RELEASED'} for row in allocations)
                or any(row.consumed+row.released<row.reserved for row in reservations)
                or any(row.state in {"ready","claimed","blocked"} for row in outboxes)):
            return None
        return {"groups":groups,"group_ids":group_ids,"jobs":jobs,"job_ids":job_ids,"reservations":reservations}

    async def _check_thread_lifecycle(self, db, proposal):
        thread = await db.scalar(select(ThreadRow).where(ThreadRow.id == proposal.thread_id,
            ThreadRow.owner_id == proposal.owner_id).with_for_update())
        if not thread:
            return "thread_not_found"
        if proposal.action == "unarchive":
            return ""
        if await db.scalar(select(RemoteObligationRow.id).where(
                RemoteObligationRow.owner_id == proposal.owner_id,
                RemoteObligationRow.thread_id == proposal.thread_id,
                RemoteObligationRow.state == "pending").limit(1)):
            return "remote_obligations_pending"
        if await db.get(SandboxControlRow, thread.id):
            return "sandbox_takeover_active"
        if await db.scalar(select(TurnRow.id).where(TurnRow.thread_id == thread.id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1)):
            return "active_thread_cannot_be_archived" if proposal.action == "archive" else "active_thread_cannot_be_deleted"
        if proposal.action == "archive":
            active_job = await db.scalar(select(JobRow.id).where(JobRow.thread_id == thread.id,
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES])).limit(1))
            active_group = await db.scalar(select(JobGroupRow.id).where(JobGroupRow.thread_id == thread.id,
                JobGroupRow.state.in_([JobGroupState.COMMITTED.value, JobGroupState.RUNNING.value])).limit(1))
            outbox = await db.scalar(select(JobOutboxRow.id).where(JobOutboxRow.thread_id == thread.id,
                JobOutboxRow.state.in_(["ready", "claimed"])).limit(1))
            return "active_jobs_prevent_archival" if any((active_job, active_group, outbox)) else ""
        return "" if await self._thread_jobs_quiescent(db, thread.id) is not None else "active_jobs_prevent_thread_delete"

    async def _apply_thread_lifecycle(self, db, proposal):
        thread = await db.get(ThreadRow, proposal.thread_id, with_for_update=True)
        if thread is None or thread.owner_id != proposal.owner_id:
            raise ValueError("lifecycle_thread_scope_invalid")
        if proposal.action in {"archive", "unarchive"}:
            thread.archived_at = now() if proposal.action == "archive" else None
            thread.updated_at = now()
            if proposal.action == "unarchive":
                await db.execute(update(JobOutboxRow).where(JobOutboxRow.thread_id == thread.id,
                    JobOutboxRow.state == "blocked").values(state="ready", updated_at=now()))
            return
        thread_id = thread.id
        state = await self._thread_jobs_quiescent(db, thread_id)
        if state is None:
            raise ValueError("lifecycle_obligations_changed")
        group_ids, job_ids, reservations = state["group_ids"], state["job_ids"], state["reservations"]
        turns = list((await db.scalars(select(TurnRow).where(TurnRow.thread_id == thread_id))).all())
        turn_ids = [turn.id for turn in turns]
        await db.execute(delete(SandboxAuditRow).where(SandboxAuditRow.thread_id == thread_id))
        await db.execute(delete(SandboxPortGrantRow).where(SandboxPortGrantRow.thread_id == thread_id))
        await db.execute(delete(SandboxControlRow).where(SandboxControlRow.thread_id == thread_id))
        if job_ids:
            await db.execute(delete(ReviewGateRow).where(or_(
                ReviewGateRow.reviewer_job_id.in_(job_ids),
                ReviewGateRow.source_job_id.in_(job_ids))))
            await db.execute(delete(JobActivityRow).where(JobActivityRow.job_id.in_(job_ids)))
            await db.execute(delete(JobDisplayActivityRow).where(
                JobDisplayActivityRow.job_id.in_(job_ids)))
            await db.execute(delete(JobArtifactRow).where(JobArtifactRow.job_id.in_(job_ids)))
            await db.execute(delete(JobCleanupRow).where(JobCleanupRow.job_id.in_(job_ids)))
            await db.execute(delete(SandboxAllocationRow).where(SandboxAllocationRow.job_id.in_(job_ids)))
            await db.execute(delete(JobLeaseRow).where(JobLeaseRow.job_id.in_(job_ids)))
            await db.execute(delete(JobAttemptRow).where(JobAttemptRow.job_id.in_(job_ids)))
            await db.execute(delete(ResourceReservationRow).where(
                ResourceReservationRow.job_id.in_(job_ids)))
            await db.execute(delete(JobRow).where(JobRow.id.in_(job_ids)))
        if group_ids:
            reservation_ids=[row.id for row in reservations]
            if reservation_ids:
                await db.execute(delete(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id.in_(reservation_ids)))
                await db.execute(delete(BudgetReservationRow).where(
                    BudgetReservationRow.id.in_(reservation_ids)))
            await db.execute(delete(JobOutboxRow).where(JobOutboxRow.group_id.in_(group_ids)))
            await db.execute(delete(JobGroupRow).where(JobGroupRow.id.in_(group_ids)))
        await db.execute(delete(BudgetAccountRow).where(BudgetAccountRow.thread_id==thread_id))
        await db.execute(delete(AuthorizationSnapshotRow).where(
            AuthorizationSnapshotRow.thread_id==thread_id))
        await db.execute(delete(JobGroupDraftRow).where(JobGroupDraftRow.thread_id==thread_id))
        await db.execute(delete(InteractionRow).where(InteractionRow.thread_id == thread_id))
        await db.execute(delete(PauseParticipantRow).where(
            PauseParticipantRow.thread_id == thread_id))
        await db.execute(delete(ThreadControlRow).where(
            ThreadControlRow.thread_id == thread_id))
        await db.execute(delete(ProjectFileRow).where(ProjectFileRow.thread_id == thread_id))
        await db.execute(delete(CheckpointVersionRow).where(
            CheckpointVersionRow.thread_id==thread_id))
        await db.execute(delete(ItemRow).where(ItemRow.thread_id == thread_id))
        remote_ids = select(RemoteObligationRow.id).where(
            RemoteObligationRow.thread_id == thread_id)
        await db.execute(delete(DomainOutbox).where(DomainOutbox.id.in_(remote_ids)))
        await db.execute(delete(RemoteObligationRow).where(
            RemoteObligationRow.thread_id == thread_id))
        if turn_ids:
            await db.execute(delete(TurnOrderRow).where(TurnOrderRow.turn_id.in_(turn_ids)))
            await db.execute(delete(TurnProjectFileRow).where(TurnProjectFileRow.turn_id.in_(turn_ids)))
            await db.execute(delete(EventRow).where(EventRow.turn_id.in_(turn_ids)))
            await db.execute(delete(TurnInputRow).where(TurnInputRow.turn_id.in_(turn_ids)))
            await db.execute(delete(ArtifactRow).where(ArtifactRow.turn_id.in_(turn_ids)))
            await db.execute(delete(TurnRow).where(TurnRow.id.in_(turn_ids)))
        await db.delete(thread)
