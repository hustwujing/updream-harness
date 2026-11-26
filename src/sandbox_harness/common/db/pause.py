"""Pause persistence; transaction boundaries are preserved."""
from __future__ import annotations
from sqlalchemy import (and_, select)
from sandbox_harness.common.contracts.domain import (ThreadControlState, ThreadControlView, PauseParticipantState, PauseParticipantView, TurnState, TERMINAL, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (JobKind, JobState, TERMINAL_JOB_STATES)
from .models import (JobAttemptRow, JobLeaseRow, JobRow, PauseParticipantRow, ThreadControlRow, ThreadRow, TurnRow, canonical_json, utc_datetime)


class PauseTransactions:
    async def get_thread_control(self, thread_id: str, owner_id: str | None = None) -> ThreadControlView:
        await self.reconcile_terminal_pause_participants(thread_id)
        async with self.connections() as db:
            query = select(ThreadControlRow).where(ThreadControlRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadControlRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
            if row:
                return self.thread_control_view(row)
            thread = await db.get(ThreadRow, thread_id)
            if not thread or owner_id is not None and thread.owner_id != owner_id:
                raise KeyError("thread_not_found")
            timestamp = utc_datetime(thread.updated_at)
            return ThreadControlView(thread_id=thread_id, state=ThreadControlState.RUNNING,
                generation=0, updated_at=timestamp)


    async def reconcile_terminal_pause_participants(self,thread_id:str)->int:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                control=await db.get(ThreadControlRow,thread_id,with_for_update=True)
                if not control or control.state!=ThreadControlState.PAUSING.value:return 0
                rows=list((await db.execute(select(PauseParticipantRow,TurnRow).join(
                    TurnRow,and_(PauseParticipantRow.kind=='primary_harness',
                        PauseParticipantRow.participant_id==TurnRow.id)).where(
                    PauseParticipantRow.thread_id==thread_id,
                    PauseParticipantRow.generation==control.generation,
                    PauseParticipantRow.state==PauseParticipantState.REQUESTED.value,
                    TurnRow.state.in_([state.value for state in TERMINAL])).with_for_update()
                )).all())
                timestamp=now()
                for participant,turn in rows:
                    failed=TurnState(turn.state) in {TurnState.FAILED,TurnState.CANCELLED}
                    participant.state=(PauseParticipantState.FAILED if failed
                        else PauseParticipantState.PAUSED).value
                    participant.detail_json=canonical_json({'terminal_state':turn.state})
                    participant.updated_at=timestamp
                participants=await self._pause_rows_locked(db,thread_id,control.generation)
                if participants and all(value.state in {PauseParticipantState.PAUSED.value,
                        PauseParticipantState.FAILED.value} for value in participants):
                    control.state=ThreadControlState.PAUSED.value
                    control.degraded=any(value.state==PauseParticipantState.FAILED.value
                        for value in participants)
                    control.detail_json=canonical_json({'participants':len(participants),
                        'failed':sum(value.state==PauseParticipantState.FAILED.value
                            for value in participants)})
                    control.completed_at=timestamp;control.updated_at=timestamp
                return len(rows)


    async def list_pause_participants(self, thread_id: str, generation: int | None = None
                                      ) -> list[PauseParticipantView]:
        async with self.connections() as db:
            query = select(PauseParticipantRow).where(PauseParticipantRow.thread_id == thread_id)
            if generation is not None:
                query = query.where(PauseParticipantRow.generation == generation)
            rows = (await db.execute(query.order_by(PauseParticipantRow.created_at,
                PauseParticipantRow.id))).scalars().all()
            return [self.pause_participant_view(row) for row in rows]


    async def get_pause_participant(self, thread_id: str, kind: str, participant_id: str,
                                    generation: int | None = None) -> PauseParticipantView | None:
        async with self.connections() as db:
            query = select(PauseParticipantRow).where(PauseParticipantRow.thread_id == thread_id,
                PauseParticipantRow.kind == kind,
                PauseParticipantRow.participant_id == participant_id)
            if generation is not None:
                query = query.where(PauseParticipantRow.generation == generation)
            row = (await db.execute(query.order_by(
                PauseParticipantRow.generation.desc()).limit(1))).scalar_one_or_none()
            return self.pause_participant_view(row) if row else None


    async def request_thread_pause(self, thread_id: str, owner_id: str) -> ThreadControlView:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                    ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
                if not thread:
                    raise KeyError("thread_not_found")
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if control and control.state in {ThreadControlState.PAUSING.value,
                        ThreadControlState.PAUSED.value}:
                    return self.thread_control_view(control)
                if control and control.state == ThreadControlState.RESUMING.value:
                    raise RuntimeError("thread_resume_in_progress")
                timestamp = now()
                generation = (control.generation if control else 0) + 1
                if not control:
                    control = ThreadControlRow(thread_id=thread_id, owner_id=owner_id,
                        state=ThreadControlState.PAUSING.value, generation=generation,
                        operation_id=identifier(), degraded=False, detail_json="{}",
                        requested_at=timestamp, completed_at=None,
                        created_at=timestamp, updated_at=timestamp)
                    db.add(control)
                else:
                    control.state = ThreadControlState.PAUSING.value
                    control.generation = generation
                    control.operation_id = identifier()
                    control.degraded = False
                    control.detail_json = "{}"
                    control.requested_at = timestamp
                    control.completed_at = None
                    control.updated_at = timestamp
                participants = 0
                active = await db.get(TurnRow, thread.active_turn_id) if thread.active_turn_id else None
                if active and TurnState(active.state) not in TERMINAL:
                    state = (PauseParticipantState.PAUSED if active.state == TurnState.QUEUED.value
                        else PauseParticipantState.REQUESTED)
                    db.add(PauseParticipantRow(id=identifier(), owner_id=owner_id,
                        thread_id=thread_id, generation=generation, kind="primary_harness",
                        participant_id=active.id, state=state.value, checkpoint_ref="",
                        native_thread_id=thread.native_thread_id,
                        external_execution_id=active.runtime_execution_id, detail_json="{}",
                        created_at=timestamp, updated_at=timestamp))
                    participants += 1
                    if state == PauseParticipantState.PAUSED:
                        active.state = TurnState.PAUSED.value
                        active.error_code = "paused"
                        active.error_detail = "Turn paused before execution"
                        await self._finish_turn_records(db, active.id)
                jobs = list((await db.execute(select(JobRow).where(
                    JobRow.thread_id == thread_id, JobRow.kind == JobKind.HARNESS.value,
                    JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                ).with_for_update())).scalars().all())
                for job in jobs:
                    queued = JobState(job.state) in {JobState.CREATED, JobState.QUEUED}
                    state = PauseParticipantState.PAUSED if queued else PauseParticipantState.REQUESTED
                    acceptance = (await db.execute(select(JobAttemptRow).where(
                        JobAttemptRow.job_id == job.id,
                        JobAttemptRow.accepted_at.is_not(None),
                    ).order_by(JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
                    db.add(PauseParticipantRow(id=identifier(), owner_id=owner_id,
                        thread_id=thread_id, generation=generation, kind="child_harness",
                        participant_id=job.id, state=state.value, checkpoint_ref="",
                        native_thread_id="", external_execution_id=(
                            acceptance.external_execution_id if acceptance else ""),
                        detail_json="{}", created_at=timestamp, updated_at=timestamp))
                    job.state = (JobState.PAUSED if queued else JobState.PAUSING).value
                    job.updated_at = timestamp
                    participants += 1
                if not participants or all(row.state == PauseParticipantState.PAUSED.value
                        for row in await self._pause_rows_locked(db, thread_id, generation)):
                    control.state = ThreadControlState.PAUSED.value
                    control.completed_at = timestamp
                await db.flush()
                return self.thread_control_view(control)


    @staticmethod
    async def _pause_rows_locked(db, thread_id: str, generation: int) -> list[PauseParticipantRow]:
        return list((await db.execute(select(PauseParticipantRow).where(
            PauseParticipantRow.thread_id == thread_id,
            PauseParticipantRow.generation == generation).with_for_update())).scalars().all())


    async def complete_pause_participant(self, thread_id: str, generation: int, kind: str,
                                         participant_id: str, *, checkpoint_ref: str = "",
                                         native_thread_id: str = "",
                                         external_execution_id: str = "",
                                         detail: dict | None = None,
                                         failed: bool = False) -> PauseParticipantView:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if not control or control.generation != generation or control.state not in {
                        ThreadControlState.PAUSING.value, ThreadControlState.PAUSED.value}:
                    raise RuntimeError("stale_pause_generation")
                row = (await db.execute(select(PauseParticipantRow).where(
                    PauseParticipantRow.thread_id == thread_id,
                    PauseParticipantRow.generation == generation,
                    PauseParticipantRow.kind == kind,
                    PauseParticipantRow.participant_id == participant_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("pause_participant_not_found")
                timestamp = now()
                row.state = (PauseParticipantState.FAILED if failed
                    else PauseParticipantState.PAUSED).value
                row.checkpoint_ref = checkpoint_ref or row.checkpoint_ref
                row.native_thread_id = native_thread_id or row.native_thread_id
                row.external_execution_id = external_execution_id or row.external_execution_id
                row.detail_json = canonical_json(detail or {})
                row.updated_at = timestamp
                if kind == "child_harness":
                    job = await db.get(JobRow, participant_id, with_for_update=True)
                    if job and JobState(job.state) not in TERMINAL_JOB_STATES:
                        job.state = JobState.PAUSED.value
                        job.updated_at = timestamp
                        lease = await db.get(JobLeaseRow, participant_id, with_for_update=True)
                        if lease:
                            attempt = await db.get(JobAttemptRow, lease.attempt_id, with_for_update=True)
                            if attempt and not attempt.finished_at:
                                attempt.finished_at = timestamp
                                attempt.outcome = JobState.PAUSED.value
                            await db.delete(lease)
                rows = await self._pause_rows_locked(db, thread_id, generation)
                if all(value.state in {PauseParticipantState.PAUSED.value,
                        PauseParticipantState.FAILED.value} for value in rows):
                    control.state = ThreadControlState.PAUSED.value
                    control.degraded = any(value.state == PauseParticipantState.FAILED.value for value in rows)
                    control.detail_json = canonical_json({"participants": len(rows),
                        "failed": sum(value.state == PauseParticipantState.FAILED.value for value in rows)})
                    control.completed_at = timestamp
                    control.updated_at = timestamp
                await db.flush()
                return self.pause_participant_view(row)


    async def resume_thread(self, thread_id: str, owner_id: str) -> ThreadControlView:
        async with self.job_lock(f"thread-pause:{thread_id}"):
            async with self.connections.begin() as db:
                thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                    ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
                if not thread:
                    raise KeyError("thread_not_found")
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if not control:
                    timestamp = now()
                    control = ThreadControlRow(thread_id=thread_id, owner_id=owner_id,
                        state=ThreadControlState.RUNNING.value, generation=0,
                        operation_id="", degraded=False, detail_json="{}",
                        requested_at=None, completed_at=timestamp,
                        created_at=timestamp, updated_at=timestamp)
                    db.add(control);await db.flush();return self.thread_control_view(control)
                if control.state == ThreadControlState.RUNNING.value:
                    return self.thread_control_view(control)
                if control.state == ThreadControlState.PAUSING.value:
                    raise RuntimeError("thread_pause_in_progress")
                control.state = ThreadControlState.RESUMING.value
                control.updated_at = now()
                rows = await self._pause_rows_locked(db, thread_id, control.generation)
                for row in rows:
                    if row.kind == "child_harness":
                        job = await db.get(JobRow, row.participant_id, with_for_update=True)
                        if job and job.state == JobState.PAUSED.value:
                            job.state = JobState.QUEUED.value
                            job.updated_at = now()
                    row.state = PauseParticipantState.RESUMED.value
                    row.updated_at = now()
                control.state = ThreadControlState.RUNNING.value
                control.detail_json = canonical_json({
                    "resume_context_required": True,
                    "degraded": control.degraded,
                    "generation": control.generation,
                })
                control.completed_at = now()
                control.updated_at = now()
                await db.flush()
                return self.thread_control_view(control)

