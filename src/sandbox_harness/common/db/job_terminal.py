"""Job Terminal persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import hashlib
from datetime import (timedelta)
from sqlalchemy import (and_, or_, delete, select, update)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sandbox_harness.common.contracts.domain import (ItemType, ThreadControlState, TurnState, TERMINAL, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (JobGroupState, JobOutboxView, JobState, JobView, TERMINAL_JOB_STATES, OutputDisposition)
from sandbox_harness.common.observability.observability import business_metric, canonical_traceparent
from .models import (BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, ItemRow, JobArtifactRow, JobAttemptRow, JobCleanupRow, JobGroupRow, JobLeaseRow, JobOutboxRow, JobRow, ResourceReservationRow, ThreadControlRow, ThreadRow, TurnRow, canonical_json, utc_datetime)


class JobTerminalTransactions:
    async def _release_group_budgets_locked(self, db: AsyncSession, group_id: str) -> None:
        reservations=list((await db.execute(select(BudgetReservationRow).where(
            BudgetReservationRow.group_id==group_id).order_by(
            BudgetReservationRow.account_id,BudgetReservationRow.budget_key)
            .with_for_update())).scalars().all())
        accounts={identity:await db.get(BudgetAccountRow,identity,with_for_update=True)
            for identity in sorted({row.account_id for row in reservations})}
        for row in reservations:
            key=f"group-terminal:{group_id}:{row.budget_key}"
            existing=(await db.execute(select(BudgetLedgerRow).where(
                BudgetLedgerRow.reservation_id==row.id,
                BudgetLedgerRow.idempotency_key==key))).scalar_one_or_none()
            if existing:continue
            amount=row.reserved-row.consumed-row.released;timestamp=now()
            if amount:
                row.released+=amount;accounts[row.account_id].reserved-=amount
                accounts[row.account_id].updated_at=timestamp
                db.add(BudgetLedgerRow(id=identifier(),reservation_id=row.id,
                    group_id=group_id,job_id="",action="release",amount=amount,
                    idempotency_key=key,detail_json="{}",created_at=timestamp))
            row.state="released" if row.consumed==0 else "closed";row.updated_at=timestamp


    async def reconcile_terminal_group_budgets(self) -> int:
        async with self.connections() as db:
            identities=list((await db.execute(select(JobGroupRow.id).join(BudgetReservationRow,
                BudgetReservationRow.group_id==JobGroupRow.id).where(
                JobGroupRow.state.in_([JobGroupState.TERMINAL.value,
                    JobGroupState.CONTINUED.value]),
                BudgetReservationRow.consumed+BudgetReservationRow.released <
                    BudgetReservationRow.reserved).distinct())).scalars().all())
        for group_id in identities:
            async with self.job_lock(f"group:{group_id}"):
                async with self.connections.begin() as db:
                    await self._release_group_budgets_locked(db,group_id)
        return len(identities)


    async def transition_job(self, job_id: str, state: JobState, *, expected_generation: int,
                             result: dict | None = None) -> JobView:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        encoded = canonical_json(result or {})
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        view=None;transition_metric=None;published_artifacts=0
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                group = await db.get(JobGroupRow, group_id, with_for_update=True)
                job = await db.get(JobRow, job_id, with_for_update=True)
                if job.generation != expected_generation:
                    raise RuntimeError("stale_job_generation")
                if (job.adapter=="reviewer-harness" and state in TERMINAL_JOB_STATES
                        and state!=JobState.SUCCEEDED):
                    gate=await self.apply_review_gate(job_id,
                        execution_error="reviewer_"+state.value.lower(),_db=db)
                    effective={**(result or {}),"review_gate":gate}
                    encoded=canonical_json(effective);digest=hashlib.sha256(encoded.encode()).hexdigest()
                if JobState(job.state) in TERMINAL_JOB_STATES:
                    if job.state == state.value and job.result_digest == digest:
                        view=self.job_view(job)
                        if group.state in {JobGroupState.TERMINAL.value,JobGroupState.CONTINUED.value}:
                            await self._release_group_budgets_locked(db,group_id)
                    else:raise RuntimeError("job_already_terminal")
                else:
                    transition_metric=(job.kind,max(0,(now()-utc_datetime(job.updated_at)).total_seconds()*1000))
                    job.state = state.value
                    job.result_json = encoded
                    job.result_digest = digest
                    job.updated_at = now()
                    if state in TERMINAL_JOB_STATES:
                        job.terminal_at = now()
                        attempt = (await db.execute(select(JobAttemptRow).where(
                            JobAttemptRow.job_id == job.id,
                            JobAttemptRow.generation == job.generation,
                        ).with_for_update())).scalar_one_or_none()
                        if attempt:
                            attempt.finished_at = now()
                            accepted_attempt=(attempt if attempt.accepted_at and
                                attempt.external_execution_id else (await db.execute(
                                select(JobAttemptRow).where(JobAttemptRow.job_id==job.id,
                                    JobAttemptRow.accepted_at.is_not(None),
                                    JobAttemptRow.external_execution_id!="").order_by(
                                    JobAttemptRow.generation.desc()).limit(1).with_for_update()
                                )).scalar_one_or_none())
                            if not accepted_attempt:
                                provisional_attempts=list((await db.execute(select(JobAttemptRow).where(
                                    JobAttemptRow.job_id==job.id,JobAttemptRow.external_execution_id!='',
                                    or_(JobAttemptRow.generation==job.generation,
                                        JobAttemptRow.outcome=='SUBMITTING'))
                                    .order_by(JobAttemptRow.generation.desc()).with_for_update()
                                    )).scalars())
                                for candidate in provisional_attempts:
                                    try:provisional=json.loads(candidate.acceptance_receipt_json)
                                    except (TypeError,ValueError):continue
                                    if (provisional.get('sandbox_id') or
                                            provisional.get('allocation_phase')=='allocating'):
                                        accepted_attempt=candidate;break
                            previous_outcome=attempt.outcome
                            attempt.outcome = state.value
                            if accepted_attempt:
                                cleanup=(await db.execute(select(JobCleanupRow).where(
                                    JobCleanupRow.job_id==job.id,
                                    JobCleanupRow.generation==accepted_attempt.generation)
                                    .with_for_update())).scalar_one_or_none()
                                if not cleanup:
                                    db.add(JobCleanupRow(id=identifier(),job_id=job.id,adapter=job.adapter,
                                        generation=accepted_attempt.generation,
                                        external_execution_id=accepted_attempt.external_execution_id,
                                        acceptance_receipt_json=accepted_attempt.acceptance_receipt_json,
                                        state="pending",phase="external_quiescence",attempt_count=0,last_error="",claim_owner="",
                                        claim_expires_at=None,created_at=now(),updated_at=now()))
                            else:
                                reservations=(await db.scalars(select(ResourceReservationRow).where(
                                    ResourceReservationRow.job_id==job.id)
                                    .with_for_update())).all()
                                for reservation in reservations:
                                    original=(await db.scalars(select(JobAttemptRow).where(
                                        JobAttemptRow.job_id==job.id,
                                        JobAttemptRow.generation==reservation.job_generation)
                                        .with_for_update())).one_or_none()
                                    cleanup=(await db.scalars(select(JobCleanupRow).where(
                                        JobCleanupRow.job_id==job.id,
                                        JobCleanupRow.generation==reservation.job_generation)
                                        .with_for_update())).one_or_none()
                                    if cleanup:continue
                                    value=json.loads(original.acceptance_receipt_json or '{}') if original else {}
                                    safe=value.get('allocation_phase') in {
                                        'reserving_resources','awaiting_capacity','absent'}
                                    original_outcome=(previous_outcome if original is attempt else
                                        original.outcome if original else '')
                                    uncertain=bool(original and (original_outcome=='SUBMITTING'
                                        or original.external_execution_id)) and not safe
                                    if uncertain:value={**value,'allocation_phase':'legacy_unknown'}
                                    receipt=canonical_json({**value,'reservation_id':reservation.id,
                                        'node_id':reservation.node_id,
                                        'node_generation':reservation.node_generation})
                                    db.add(JobCleanupRow(id=identifier(),job_id=job.id,
                                        adapter=job.adapter,generation=reservation.job_generation,
                                        external_execution_id="",acceptance_receipt_json=receipt,
                                        state="pending",phase=('external_quiescence' if uncertain
                                            else 'capacity_release'),attempt_count=0,last_error="",claim_owner="",
                                        claim_expires_at=None,created_at=now(),updated_at=now()))
                        await db.execute(delete(JobLeaseRow).where(JobLeaseRow.job_id == job.id))
                    if group.state == JobGroupState.COMMITTED.value:
                        group.state = JobGroupState.RUNNING.value
                        group.updated_at = now()
                    jobs = (await db.execute(select(JobRow).where(JobRow.group_id == group.id)
                        .order_by(JobRow.local_job_id).with_for_update())).scalars().all()
                    if all(JobState(member.state) in TERMINAL_JOB_STATES for member in jobs):
                        terminal_payload = []
                        for member in jobs:
                            member_result=json.loads(member.result_json or "{}")
                            terminal_payload.append({"job_id":member.id,
                                "local_job_id":member.local_job_id,"state":member.state,
                                "result_digest":member.result_digest,
                                "result_available":bool(member_result),
                                **({"error_code":str(member_result.get("error_code"))[:160]}
                                    if member_result.get("error_code") else {}),
                                **({"review_gate":member_result["review_gate"]}
                                    if isinstance(member_result.get("review_gate"),dict) else {})})
                        result_json = canonical_json(terminal_payload)
                        result_digest = hashlib.sha256(result_json.encode()).hexdigest()
                        group.state = JobGroupState.TERMINAL.value
                        group.terminal_at = group.terminal_at or now()
                        group.updated_at = now()
                        await self._release_group_budgets_locked(db,group.id)
                        delivery_rows=(await db.execute(select(JobArtifactRow,JobRow).join(
                            JobRow,JobRow.id==JobArtifactRow.job_id).where(
                            JobRow.group_id==group.id,
                            JobArtifactRow.output_disposition==OutputDisposition.DELIVERY.value,
                        ).order_by(JobRow.local_job_id,JobArtifactRow.artifact_role,
                            JobArtifactRow.position).with_for_update())).all()
                        existing_items=(await db.execute(select(ItemRow).where(
                            ItemRow.thread_id==group.thread_id,
                            ItemRow.type==ItemType.ARTIFACT.value).with_for_update())).scalars().all()
                        visible_ids={json.loads(item.payload_json).get('artifact_id') for item in existing_items}
                        for artifact_row,artifact_job in delivery_rows:
                            if artifact_row.artifact_id in visible_ids:continue
                            sequence=await self.next_item_sequence(db,group.thread_id);timestamp=now()
                            db.add(ItemRow(id=identifier(),thread_id=group.thread_id,
                                turn_id=artifact_job.origin_turn_id,sequence=sequence,
                                type=ItemType.ARTIFACT.value,status='completed',
                                summary=artifact_row.filename,payload_json=json.dumps({
                                    'artifact_id':artifact_row.artifact_id,'mime':artifact_row.mime,
                                    'size':artifact_row.size,'partial':False,'job_id':artifact_job.id,
                                    'artifact_role':artifact_row.artifact_role,
                                    'local_job_id':artifact_job.local_job_id}),
                                created_at=timestamp,updated_at=timestamp))
                            visible_ids.add(artifact_row.artifact_id)
                            published_artifacts+=1
                        all_cancelled = all(member.state == JobState.CANCELLED.value for member in jobs)
                        thread=await db.get(ThreadRow,group.thread_id,with_for_update=True)
                        outbox = (await db.execute(select(JobOutboxRow).where(
                            JobOutboxRow.group_id == group.id).with_for_update())).scalar_one_or_none()
                        if not outbox:
                            db.add(JobOutboxRow(
                                id=identifier(), group_id=group.id, thread_id=group.thread_id,
                                state=("suppressed" if group.suppress_continuation or all_cancelled
                                    else "blocked" if thread and thread.archived_at is not None else "ready"),
                                result_digest=result_digest, result_json=result_json,
                                claim_owner="", claim_batch_digest="", claim_expires_at=None,
                                continuation_turn_id="",
                                created_at=now(), updated_at=now(),
                            ))
                    await db.flush()
                    view=self.job_view(job)
        if transition_metric:
            kind,duration=transition_metric;stage=state.value.lower()
            business_metric('updream.job.stage.count',1,stage=stage,
                outcome=stage,kind=kind)
            business_metric('updream.job.stage.duration',duration,stage=stage,
                outcome=stage,kind=kind)
        if published_artifacts:
            business_metric('updream.artifact.delivery.count',published_artifacts,
                phase='final_delivery',outcome='success')
        return view


    async def get_job_outbox(self, group_id: str) -> JobOutboxView | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.group_id == group_id))).scalar_one_or_none()
            return self.job_outbox_view(row) if row else None


    async def list_job_outboxes(self) -> list[JobOutboxView]:
        async with self.connections() as db:
            rows = (await db.execute(select(JobOutboxRow).order_by(
                JobOutboxRow.created_at, JobOutboxRow.id))).scalars().all()
            return [self.job_outbox_view(row) for row in rows]


    async def claim_job_outbox_batch(self, owner: str, *, lease_seconds: int = 60,
                                     max_groups: int = 32, max_jobs: int = 100,
                                     max_bytes: int = 1024 * 1024) -> list[dict]:
        async with self.connections.begin() as db:
            candidates = list((await db.execute(select(JobOutboxRow,
                TurnRow.last_traceparent,TurnRow.submit_traceparent)
                .join(ThreadRow,ThreadRow.id==JobOutboxRow.thread_id)
                .join(JobGroupRow,JobGroupRow.id==JobOutboxRow.group_id)
                .join(TurnRow,TurnRow.id==JobGroupRow.origin_turn_id)
                .outerjoin(ThreadControlRow, ThreadControlRow.thread_id == JobOutboxRow.thread_id)
                .where(ThreadRow.archived_at.is_(None),or_(ThreadControlRow.thread_id.is_(None),
                    ThreadControlRow.state == ThreadControlState.RUNNING.value),
                TurnRow.state.in_([state.value for state in TERMINAL]),
                or_(ThreadRow.active_turn_id != JobGroupRow.origin_turn_id,
                    TurnRow.state == TurnState.PAUSED.value),
                or_(JobOutboxRow.state == "ready", and_(
                    JobOutboxRow.state == "claimed", JobOutboxRow.claim_expires_at < now())),
            ).order_by(JobOutboxRow.created_at, JobOutboxRow.id).limit(max_groups * 4)
                .with_for_update(skip_locked=True,of=JobOutboxRow))).all())
            if not candidates:
                return []
            thread_id = candidates[0][0].thread_id
            selected = []
            total_jobs = total_bytes = 0
            frozen_digest = candidates[0][0].claim_batch_digest
            source = ([value for value in candidates if value[0].claim_batch_digest == frozen_digest]
                if frozen_digest else candidates)
            for row,last_traceparent,submit_traceparent in source:
                if row.thread_id != thread_id:
                    continue
                result = json.loads(row.result_json)
                encoded_bytes = len(row.result_json.encode())
                if (selected and (len(selected) >= max_groups
                        or total_jobs + len(result) > max_jobs
                        or total_bytes + encoded_bytes > max_bytes)):
                    break
                if len(result) > max_jobs or encoded_bytes > max_bytes:
                    row.state = "suppressed"
                    row.updated_at = now()
                    continue
                selected.append((row,result,canonical_traceparent(last_traceparent)
                    or canonical_traceparent(submit_traceparent)))
                total_jobs += len(result); total_bytes += encoded_bytes
            batch_digest = (frozen_digest or hashlib.sha256("\n".join(sorted(
                row.id for row,_,_ in selected)).encode()).hexdigest())
            expires_at = now() + timedelta(seconds=lease_seconds)
            for row,_,_ in selected:
                row.state = "claimed"; row.claim_owner = owner
                row.claim_batch_digest = batch_digest
                row.claim_expires_at = expires_at; row.updated_at = now()
            return [{"outbox_id": row.id, "group_id": row.group_id,
                "thread_id": row.thread_id, "result_digest": row.result_digest,
                "batch_digest": batch_digest, "results": result,
                "traceparent":traceparent} for row,result,traceparent in selected]


    async def finish_job_outbox_batch(self, outbox_ids: list[str], owner: str, *,
                                      continuation_turn_id: str, delivered: bool,
                                      error: str = "", permanent: bool = False) -> None:
        if not outbox_ids:
            return
        async with self.connections.begin() as db:
            rows = list((await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.id.in_(outbox_ids)).order_by(JobOutboxRow.id)
                .with_for_update())).scalars().all())
            if len(rows) != len(set(outbox_ids)) or any(row.claim_owner != owner for row in rows):
                raise RuntimeError("stale_job_outbox_claim")
            timestamp = now()
            if delivered:
                for row in rows:
                    row.state = "delivered"; row.continuation_turn_id = continuation_turn_id
                    row.claim_owner = ""; row.claim_expires_at = None; row.updated_at = timestamp
                    group = await db.get(JobGroupRow, row.group_id, with_for_update=True)
                    group.state = JobGroupState.CONTINUED.value
                    group.continued_at = timestamp; group.updated_at = timestamp
            else:
                for row in rows:
                    row.state = "blocked" if permanent else "ready"
                    row.claim_owner = ""; row.claim_expires_at = None
                    row.updated_at = timestamp


    async def stop_all_jobs(self, thread_id: str, owner_id: str) -> list[JobView]:
        async with self.job_lock(f"thread-jobs:{thread_id}"):
            async with self.connections.begin() as db:
                thread = (await db.execute(select(ThreadRow).where(
                    ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not thread:
                    raise KeyError("thread_not_found")
                control = await db.get(ThreadControlRow, thread_id, with_for_update=True)
                if control and control.state != ThreadControlState.RUNNING.value:
                    control.state = ThreadControlState.RUNNING.value
                    control.detail_json = canonical_json({"hard_cancelled": True,
                        "generation": control.generation})
                    control.completed_at = now()
                    control.updated_at = now()
                groups = list((await db.execute(select(JobGroupRow).where(
                    JobGroupRow.thread_id == thread_id,
                    JobGroupRow.state.in_([JobGroupState.COMMITTED.value,
                        JobGroupState.RUNNING.value, JobGroupState.TERMINAL.value]),
                ).with_for_update())).scalars().all())
                group_ids = [group.id for group in groups]
                rows = list((await db.execute(select(JobRow).where(
                    JobRow.group_id.in_(group_ids or [""])).with_for_update())).scalars().all())
                timestamp = now()
                for group in groups:
                    group.suppress_continuation = True; group.updated_at = timestamp
                for row in rows:
                    if JobState(row.state) not in TERMINAL_JOB_STATES:
                        row.cancel_requested = True; row.state = JobState.CANCELLING.value
                        row.updated_at = timestamp
                await db.execute(update(JobOutboxRow).where(
                    JobOutboxRow.thread_id == thread_id,
                    JobOutboxRow.state.in_(["ready", "claimed"]),
                ).values(state="suppressed", claim_owner="", claim_expires_at=None,
                    updated_at=timestamp))
                await db.flush()
                return [self.job_view(row) for row in rows]


    async def attach_suppressed_job_context(self, turn_id: str) -> int:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.internal:
                return 0
            rows = list((await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.thread_id == turn.thread_id,
                JobOutboxRow.state == "suppressed",
                JobOutboxRow.continuation_turn_id == "",
            ).order_by(JobOutboxRow.created_at, JobOutboxRow.id).limit(32)
                .with_for_update())).scalars().all())
            if not rows:
                return 0
            facts = [{"group_id": row.group_id, "result_digest": row.result_digest,
                "results": json.loads(row.result_json)} for row in rows]
            encoded = canonical_json(facts)
            if len(encoded.encode()) > 1024 * 1024:
                raise RuntimeError("suppressed_job_context_too_large")
            turn.message = ("[suppressed_job_results] Factual results from stopped background groups; "
                "use them only if relevant to the current user request. " + encoded
                + "\n\n[current_user_message]\n" + turn.message)
            timestamp = now()
            for row in rows:
                row.state = "delivered"; row.continuation_turn_id = turn.id; row.updated_at = timestamp
                group = await db.get(JobGroupRow, row.group_id, with_for_update=True)
                group.state = JobGroupState.CONTINUED.value
                group.continued_at = timestamp; group.updated_at = timestamp
            return len(rows)
