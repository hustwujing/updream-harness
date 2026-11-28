"""Threads persistence; transaction boundaries are preserved."""
from __future__ import annotations
from sqlalchemy import (or_, delete, func, select, update)
from sandbox_harness.common.contracts.domain import (ThreadCreate, ThreadView, TurnState, identifier, now)
from sandbox_harness.common.catalog.catalog import (ResolvedProfile)
from sandbox_harness.common.contracts.job_contracts import (JobGroupState, JobState, TERMINAL_JOB_STATES)
from .models import (ArtifactRow, AuthorizationSnapshotRow, BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, CheckpointVersionRow, EventRow, InteractionRow, ItemRow, JobActivityRow, JobArtifactRow, JobAttemptRow, JobCleanupRow, JobDisplayActivityRow, JobGroupDraftRow, JobGroupRow, JobLeaseRow, JobOutboxRow, JobRow, MediaBatchRow, MediaDraftRow, MediaExecutionRow, MediaJobOutputRow, MediaJobRow, MediaOutboxRow, MediaReferenceUploadRow, PauseParticipantRow, ProjectFileRow, ResourceReservationRow, ReviewGateRow, SandboxAllocationRow, SandboxAuditRow, SandboxControlRow, SandboxPortGrantRow, SiteDeployJobRow, SiteDraftFileRow, SiteDraftRow, SiteFileRow, SiteProjectRow, SiteVersionRow, ThreadControlRow, ThreadRow, TurnInputRow, TurnOrderRow, TurnProjectFileRow, TurnRow, utc_datetime)


class ThreadsTransactions:
    async def create_thread(self, value: ThreadCreate, resolved: ResolvedProfile, owner_id: str) -> ThreadView:
        timestamp = now()
        row = ThreadRow(id=identifier(), owner_id=owner_id, harness=resolved.harness.value, model=resolved.model,
            reasoning_level=resolved.reasoning_level, reasoning_effort=resolved.reasoning_effort,
            title=value.title, created_at=timestamp, updated_at=timestamp)
        async with self.connections.begin() as db:
            await self._admit_new_work(db, owner_id, row.id)
            db.add(row)
        return self.thread_view(row)


    async def list_threads(self, owner_id: str, *, archived: bool = False) -> list[ThreadView]:
        async with self.connections() as db:
            archive_filter = ThreadRow.archived_at.is_not(None) if archived else ThreadRow.archived_at.is_(None)
            rows = (await db.execute(select(ThreadRow).where(ThreadRow.owner_id == owner_id, archive_filter)
                .order_by(ThreadRow.updated_at.desc()))).scalars().all()
        return [self.thread_view(row) for row in rows]


    async def list_thread_summaries(self, owner_id: str, *, archived: bool = False) -> list[dict]:
        threads = await self.list_threads(owner_id, archived=archived)
        async with self.connections() as db:
            ranked = select(TurnRow.id, TurnRow.thread_id, TurnRow.state, TurnRow.updated_at,
                func.row_number().over(partition_by=TurnRow.thread_id,
                    order_by=TurnOrderRow.sequence.desc()).label("rank")).join(
                ThreadRow, ThreadRow.id == TurnRow.thread_id).join(TurnOrderRow,
                    TurnOrderRow.turn_id == TurnRow.id).where(
                        ThreadRow.owner_id == owner_id).subquery()
            rows = (await db.execute(select(ranked.c.id, ranked.c.thread_id, ranked.c.state, ranked.c.updated_at)
                .where(ranked.c.rank == 1))).all()
        latest = {row.thread_id: row for row in rows}
        return [{**thread.model_dump(mode="json"),
            "latest_turn_id": latest[thread.id].id if thread.id in latest else "",
            "unread_completion": bool(thread.id in latest and latest[thread.id].state == TurnState.COMPLETED.value
                and latest[thread.id].id != thread.last_read_turn_id),
            "latest_turn_state": latest[thread.id].state if thread.id in latest else "",
            "latest_turn_updated_at": latest[thread.id].updated_at if thread.id in latest else None,
        } for thread in threads]


    async def mark_thread_read(self, thread_id: str, turn_id: str, owner_id: str) -> str:
        async with self.connections.begin() as db:
            thread = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).values(last_read_turn_id=ThreadRow.last_read_turn_id)
                .returning(ThreadRow))).scalar_one_or_none()
            if thread is None:
                raise KeyError("thread_not_found")
            latest = (await db.execute(select(TurnRow).join(TurnOrderRow,
                TurnOrderRow.turn_id == TurnRow.id).where(TurnRow.thread_id == thread_id)
                .order_by(TurnOrderRow.sequence.desc()).limit(1))).scalar_one_or_none()
            # A stale read may only acknowledge the completion actually displayed.
            # It must not consume a newer completion or pre-read a running Turn.
            if latest and latest.id == turn_id and latest.state == TurnState.COMPLETED.value:
                thread.last_read_turn_id = turn_id
            return thread.last_read_turn_id


    async def get_thread(self, thread_id: str, owner_id: str | None = None) -> ThreadView | None:
        async with self.connections() as db:
            query = select(ThreadRow).where(ThreadRow.id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
        return self.thread_view(row) if row else None


    async def set_thread_archived(self, thread_id: str, owner_id: str, *, archived: bool) -> ThreadView:
        terminal = {state.value for state in {
            TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED,
            TurnState.PAUSED,
        }}
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if archived:
                states = (await db.execute(select(TurnRow.state).where(TurnRow.thread_id == thread_id))).scalars().all()
                if any(state not in terminal for state in states):
                    raise RuntimeError("active_thread_cannot_be_archived")
                active_job=(await db.execute(select(JobRow.id).where(JobRow.thread_id==thread_id,
                    JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES])).limit(1))).scalar_one_or_none()
                active_group=(await db.execute(select(JobGroupRow.id).where(
                    JobGroupRow.thread_id==thread_id,JobGroupRow.state.in_([
                        JobGroupState.COMMITTED.value,JobGroupState.RUNNING.value])).limit(1))).scalar_one_or_none()
                pending_outbox=(await db.execute(select(JobOutboxRow.id).where(
                    JobOutboxRow.thread_id==thread_id,JobOutboxRow.state.in_(["ready","claimed"])
                    ).limit(1))).scalar_one_or_none()
                if any((active_job,active_group,pending_outbox)):
                    raise RuntimeError("active_jobs_prevent_archival")
                active_media=(await db.execute(select(MediaJobRow.id).where(
                    MediaJobRow.thread_id==thread_id,MediaJobRow.status.not_in(
                        ["succeeded","failed","cancelled","unknown"])).limit(1))).scalar_one_or_none()
                active_batch=(await db.execute(select(MediaBatchRow.turn_id).where(
                    MediaBatchRow.thread_id==thread_id,MediaBatchRow.status.in_([
                        "sealed","delivering"])).limit(1))).scalar_one_or_none()
                pending_reference=(await db.execute(select(MediaReferenceUploadRow.id).where(
                    MediaReferenceUploadRow.thread_id==thread_id,
                    MediaReferenceUploadRow.status=="pending",
                    MediaReferenceUploadRow.expires_at>now()).limit(1)
                    .with_for_update())).scalar_one_or_none()
                if any((active_media,active_batch,pending_reference)):
                    raise RuntimeError("active_media_prevents_archival")
            timestamp = now()
            thread.archived_at = timestamp if archived else None
            thread.updated_at = timestamp
            if not archived:
                await db.execute(update(JobOutboxRow).where(JobOutboxRow.thread_id==thread_id,
                    JobOutboxRow.state=="blocked").values(state="ready",updated_at=timestamp))
                await db.execute(update(MediaBatchRow).where(MediaBatchRow.thread_id==thread_id,
                    MediaBatchRow.status=="blocked").values(status="sealed",updated_at=timestamp,
                        lease_owner="",lease_expires_at=None))
        return self.thread_view(thread)


    async def delete_thread(self, thread_id: str, owner_id: str) -> list[str]:
        terminal = {state.value for state in {
            TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED, TurnState.CANCELLED,
            TurnState.PAUSED,
        }}
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id,
            ).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            turns = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id))).scalars().all()
            if any(turn.state not in terminal for turn in turns):
                raise RuntimeError("active_thread_cannot_be_deleted")
            media_jobs = (await db.execute(select(MediaJobRow).where(
                MediaJobRow.thread_id == thread_id))).scalars().all()
            if any(job.status not in {"succeeded", "failed", "cancelled", "unknown"} for job in media_jobs):
                raise RuntimeError("active_thread_cannot_be_deleted")
            batches=list((await db.execute(select(MediaBatchRow).where(
                MediaBatchRow.thread_id==thread_id).with_for_update())).scalars().all())
            references=list((await db.execute(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.thread_id==thread_id).with_for_update())).scalars().all())
            if (any(row.status in {"sealed","delivering","blocked"} for row in batches)
                    or any(row.status=="pending" and utc_datetime(row.expires_at)>now()
                        for row in references)):
                raise RuntimeError("active_media_prevents_thread_delete")
            site_jobs = (await db.execute(select(SiteDeployJobRow).where(
                SiteDeployJobRow.thread_id == thread_id))).scalars().all()
            if any(job.status not in {"succeeded", "failed"} for job in site_jobs):
                raise RuntimeError("active_thread_cannot_be_deleted")
            site_projects = (await db.execute(select(SiteProjectRow).where(
                SiteProjectRow.thread_id == thread_id))).scalars().all()
            if any(project.status == "active" for project in site_projects):
                raise RuntimeError("published_site_prevents_thread_delete")
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
                raise RuntimeError("active_jobs_prevent_thread_delete")
            turn_ids = [turn.id for turn in turns]
            await db.execute(delete(SandboxAuditRow).where(SandboxAuditRow.thread_id == thread_id))
            await db.execute(delete(SandboxPortGrantRow).where(SandboxPortGrantRow.thread_id == thread_id))
            await db.execute(delete(SandboxControlRow).where(SandboxControlRow.thread_id == thread_id))
            media_job_ids = [job.id for job in media_jobs]
            if media_job_ids:
                await db.execute(delete(MediaOutboxRow).where(MediaOutboxRow.job_id.in_(media_job_ids)))
                await db.execute(delete(MediaJobOutputRow).where(MediaJobOutputRow.job_id.in_(media_job_ids)))
                await db.execute(delete(MediaExecutionRow).where(MediaExecutionRow.job_id.in_(media_job_ids)))
                await db.execute(delete(MediaJobRow).where(MediaJobRow.id.in_(media_job_ids)))
            await db.execute(delete(MediaDraftRow).where(MediaDraftRow.thread_id == thread_id))
            await db.execute(delete(MediaBatchRow).where(MediaBatchRow.thread_id == thread_id))
            await db.execute(delete(MediaReferenceUploadRow).where(MediaReferenceUploadRow.thread_id == thread_id))
            site_project_ids = [project.id for project in site_projects]
            site_version_ids = list((await db.execute(select(SiteVersionRow.id).where(
                SiteVersionRow.project_id.in_(site_project_ids)))).scalars().all()) if site_project_ids else []
            if site_version_ids:
                await db.execute(delete(SiteFileRow).where(SiteFileRow.version_id.in_(site_version_ids)))
                await db.execute(delete(SiteVersionRow).where(SiteVersionRow.id.in_(site_version_ids)))
            site_draft_ids = list((await db.execute(select(SiteDraftRow.id).where(
                SiteDraftRow.thread_id == thread_id))).scalars().all())
            if site_draft_ids:
                await db.execute(delete(SiteDraftFileRow).where(SiteDraftFileRow.draft_id.in_(site_draft_ids)))
            await db.execute(delete(SiteDeployJobRow).where(SiteDeployJobRow.thread_id == thread_id))
            await db.execute(delete(SiteDraftRow).where(SiteDraftRow.thread_id == thread_id))
            await db.execute(delete(SiteProjectRow).where(SiteProjectRow.thread_id == thread_id))
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
            if turn_ids:
                await db.execute(delete(TurnOrderRow).where(TurnOrderRow.turn_id.in_(turn_ids)))
                await db.execute(delete(TurnProjectFileRow).where(TurnProjectFileRow.turn_id.in_(turn_ids)))
                await db.execute(delete(EventRow).where(EventRow.turn_id.in_(turn_ids)))
                await db.execute(delete(TurnInputRow).where(TurnInputRow.turn_id.in_(turn_ids)))
                await db.execute(delete(ArtifactRow).where(ArtifactRow.turn_id.in_(turn_ids)))
                await db.execute(delete(TurnRow).where(TurnRow.id.in_(turn_ids)))
            await db.delete(thread)
        if self.events:
            await self.events.delete(turn_ids)
        return turn_ids
