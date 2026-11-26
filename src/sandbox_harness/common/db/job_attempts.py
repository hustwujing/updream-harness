"""Job Attempts persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import hashlib
from datetime import (datetime, timedelta)
from sqlalchemy import (and_, or_, delete, func, select, update)
from sandbox_harness.common.contracts.domain import (ThreadControlState, PauseParticipantState, identifier, now)
from sandbox_harness.common.contracts.job_errors import (normalize_job_error)
from sandbox_harness.common.observability.observability import canonical_traceparent
from sandbox_harness.common.contracts.job_contracts import (JobAttemptReceipt, JobGroupState, JobKind, JobState, JobView, JobRequest, TERMINAL_JOB_STATES)
from .models import (BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, JobAttemptRow, JobGroupRow, JobLeaseRow, JobRow, OwnerAdmissionRow, PauseParticipantRow, ThreadControlRow, TurnRow, canonical_json, utc_datetime)


class JobAttemptsTransactions:
    @staticmethod
    def _fair_job_identities(rows):
        queues={}
        for identity,owner_id,_ in rows:queues.setdefault(owner_id,[]).append(identity)
        result=[]
        while queues:
            for owner_id in list(queues):
                result.append(queues[owner_id].pop(0))
                if not queues[owner_id]:queues.pop(owner_id)
        return result

    async def claim_available_jobs(self, worker_id: str, node_id: str, lease_seconds: int,
                                   *, limit: int = 1,
                                   adapters: set[str] | None = None,
                                   accepted_only: bool = False,
                                   owner_limit: int = 0) -> list[JobAttemptReceipt]:
        if adapters is not None and not adapters:return []
        timestamp = now()
        async with self.connections() as db:
            query = (select(JobRow.id)
                .outerjoin(JobLeaseRow, JobLeaseRow.job_id == JobRow.id)
                .outerjoin(ThreadControlRow, ThreadControlRow.thread_id == JobRow.thread_id)
                .where(or_(JobRow.state == JobState.PAUSING.value,
                    JobRow.state == JobState.CANCELLING.value,
                    and_(JobRow.kind == JobKind.PROVIDER.value,
                        JobRow.state == JobState.WAITING_EXTERNAL.value), and_(
                    or_(ThreadControlRow.thread_id.is_(None),
                        ThreadControlRow.state == ThreadControlState.RUNNING.value),
                    JobRow.state.in_([JobState.CREATED.value, JobState.QUEUED.value,
                        JobState.RUNNING.value, JobState.WAITING_EXTERNAL.value]))),
                    or_(JobLeaseRow.job_id.is_(None), JobLeaseRow.expires_at < timestamp)))
            if adapters is not None:
                query = query.where(JobRow.adapter.in_(adapters))
            if accepted_only:
                superseded=select(PauseParticipantRow.id).where(
                    PauseParticipantRow.participant_id==JobAttemptRow.job_id,
                    PauseParticipantRow.kind=='child_harness',
                    PauseParticipantRow.state==PauseParticipantState.RESUMED.value,
                    PauseParticipantRow.updated_at>=JobAttemptRow.accepted_at).correlate(JobAttemptRow).exists()
                accepted=select(JobAttemptRow.id).where(JobAttemptRow.job_id==JobRow.id,
                    JobAttemptRow.accepted_at.is_not(None),JobAttemptRow.external_execution_id!='',
                    ~superseded).correlate(JobRow).exists()
                query=query.where(accepted,JobRow.state.in_([JobState.RUNNING.value,
                    JobState.WAITING_EXTERNAL.value,JobState.PAUSING.value,JobState.CANCELLING.value]))
            rows = (await db.execute(query.with_only_columns(
                JobRow.id,JobRow.owner_id,JobRow.created_at).order_by(
                JobRow.updated_at if accepted_only else JobRow.created_at,
                JobRow.id).limit(limit*max(10,owner_limit*4 or 10)))).all()
            identities=self._fair_job_identities(rows)
        result = []
        for identity in identities:
            if len(result) >= limit:
                break
            try:
                result.append(await self.claim_job(identity,worker_id,node_id,lease_seconds,
                    owner_limit=0 if accepted_only else owner_limit))
            except RuntimeError as exc:
                if str(exc) not in {"job_already_leased", "job_terminal","owner_concurrency_wait"}:
                    raise
        return result


    async def finish_pre_accept_attempt(self, job_id: str, worker_id: str, generation: int, *,
                                        retry: bool, error_code: str, error_detail: str,
                                        error_metadata:dict|None=None) -> JobView:
        view = None
        async with self.job_lock(f"job:{job_id}"):
            async with self.connections.begin() as db:
                job = await db.get(JobRow, job_id, with_for_update=True)
                attempt = (await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id == job_id,
                    JobAttemptRow.generation == generation,
                ).with_for_update())).scalar_one_or_none()
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if (not job or not attempt or not lease or job.generation != generation
                        or lease.generation != generation or lease.worker_id != worker_id
                        or attempt.accepted_at is not None):
                    raise RuntimeError("stale_job_generation")
                attempt.finished_at = now()
                attempt.outcome = "PRE_ACCEPT_RETRY" if retry else JobState.FAILED.value
                attempt.error_code = error_code
                attempt.error_detail = error_detail
                pause = (await db.execute(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id == job.thread_id,
                    PauseParticipantRow.kind == "child_harness",
                    PauseParticipantRow.participant_id == job.id,
                    PauseParticipantRow.state == PauseParticipantState.REQUESTED.value,
                ).limit(1))).scalar_one_or_none()
                job.state = (JobState.PAUSING.value if pause else
                    JobState.QUEUED.value if retry else JobState.RUNNING.value)
                job.updated_at = now()
                await db.delete(lease)
                await db.flush()
                view = self.job_view(job)
        if retry:
            return view
        metadata={key:value for key,value in (error_metadata or {}).items()
            if key in {'failed_phase','retry_class','retryable','node_health_generation'}}
        return await self.transition_job(job_id, JobState.FAILED, expected_generation=generation,
            result=normalize_job_error({"error_code":error_code,"error_detail":error_detail,
                **metadata},side_effect_state='none'))


    async def mark_infrastructure_wait(self,job_id:str,worker_id:str,generation:int,*,
                                       error_detail:str)->None:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id,with_for_update=True)
            attempt=(await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job_id,JobAttemptRow.generation==generation)
                .with_for_update())).scalar_one_or_none()
            lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
            if (not job or not attempt or not lease or job.generation!=generation
                    or lease.generation!=generation or lease.worker_id!=worker_id
                    or attempt.accepted_at is not None):
                raise RuntimeError('stale_job_generation')
            attempt.outcome='INFRASTRUCTURE_WAIT'
            attempt.error_code='infrastructure_capacity_wait'
            attempt.error_detail=error_detail[:2000]
            job.updated_at=now()


    async def defer_accepted_job(self, job_id: str, worker_id: str, generation: int) -> JobView:
        async with self.job_lock(f"job:{job_id}"):
            async with self.connections.begin() as db:
                job = await db.get(JobRow, job_id, with_for_update=True)
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if (not job or not lease or job.generation != generation
                        or lease.generation != generation or lease.worker_id != worker_id):
                    raise RuntimeError("stale_job_generation")
                pause = (await db.execute(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id == job.thread_id,
                    PauseParticipantRow.kind == "child_harness",
                    PauseParticipantRow.participant_id == job.id,
                    PauseParticipantRow.state == PauseParticipantState.REQUESTED.value,
                ).limit(1))).scalar_one_or_none()
                job.state = (JobState.PAUSING if pause else JobState.WAITING_EXTERNAL).value
                job.updated_at = now()
                await db.delete(lease)
                await db.flush()
                return self.job_view(job)


    async def defer_collection_retry(self,job_id:str,worker_id:str,generation:int,*,
                                      maximum_attempts:int,error_code:str,
                                      error_detail:str)->JobView|None:
        """Persist a collection-only retry without reopening external submission."""
        async with self.connections() as lookup:
            group_id=(await lookup.execute(select(JobRow.group_id).where(
                JobRow.id==job_id))).scalar_one_or_none()
        if not group_id:raise KeyError('job_not_found')
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                job=await db.get(JobRow,job_id,with_for_update=True)
                attempt=(await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id==job_id,JobAttemptRow.generation==generation)
                    .with_for_update())).scalar_one_or_none()
                lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
                if (not job or not attempt or not lease or job.generation!=generation
                        or lease.generation!=generation or lease.worker_id!=worker_id):
                    raise RuntimeError('stale_job_generation')
                previous=int((await db.execute(select(func.count()).select_from(
                    JobAttemptRow).where(JobAttemptRow.job_id==job_id,
                        JobAttemptRow.outcome=='COLLECTION_RETRY'))).scalar_one())
                if previous+1>=max(1,maximum_attempts):return None
                attempt.finished_at=now();attempt.outcome='COLLECTION_RETRY'
                attempt.error_code=error_code;attempt.error_detail=error_detail[:2000]
                job.state=JobState.WAITING_EXTERNAL.value
                job.result_json=canonical_json(normalize_job_error({
                    'error_code':error_code,'error_detail':'成果收集将使用原执行句柄重试',
                    'failed_phase':'collection','retryable':True,
                    'retry_class':'collect_same_execution','side_effect_state':'completed'}))
                job.result_digest=hashlib.sha256(job.result_json.encode()).hexdigest()
                job.updated_at=now();await db.delete(lease);await db.flush()
                return self.job_view(job)


    async def request_job_cancel(self, job_id: str, owner_id: str) -> JobView:
        async with self.job_lock(f"job:{job_id}"):
            async with self.connections.begin() as db:
                row = (await db.execute(select(JobRow).where(
                    JobRow.id == job_id, JobRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("job_not_found")
                if JobState(row.state) not in TERMINAL_JOB_STATES:
                    request=JobRequest.model_validate_json(row.request_json)
                    accepted=(await db.execute(select(JobAttemptRow.id).where(
                        JobAttemptRow.job_id==row.id,JobAttemptRow.accepted_at.is_not(None))
                        .limit(1))).scalar_one_or_none()
                    row.cancel_requested = True
                    if accepted and request.cancellation_policy.get("supported") is False:
                        row.state=JobState.WAITING_EXTERNAL.value
                        row.result_json=canonical_json({"cancel_mode":"soft",
                            "detail":"Provider does not support cancellation; monitoring continues."})
                    else:row.state = JobState.CANCELLING.value
                    row.updated_at = now()
                await db.flush()
                return self.job_view(row)


    async def request_job_group_cancel(self, group_id: str, owner_id: str) -> list[JobView]:
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                group = (await db.execute(select(JobGroupRow).where(
                    JobGroupRow.id == group_id, JobGroupRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not group:
                    raise KeyError("job_group_not_found")
                group.suppress_continuation = True
                group.updated_at = now()
                rows = (await db.execute(select(JobRow).where(
                    JobRow.group_id == group_id).order_by(
                    JobRow.created_at, JobRow.local_job_id).with_for_update())).scalars().all()
                timestamp = now()
                for row in rows:
                    if JobState(row.state) not in TERMINAL_JOB_STATES:
                        request=JobRequest.model_validate_json(row.request_json)
                        accepted=(await db.execute(select(JobAttemptRow.id).where(
                            JobAttemptRow.job_id==row.id,JobAttemptRow.accepted_at.is_not(None))
                            .limit(1))).scalar_one_or_none()
                        row.cancel_requested = True
                        row.state=(JobState.WAITING_EXTERNAL.value if accepted and
                            request.cancellation_policy.get("supported") is False
                            else JobState.CANCELLING.value)
                        row.updated_at = timestamp
                await db.flush()
                return [self.job_view(row) for row in rows]


    async def claim_job(self, job_id: str, worker_id: str, node_id: str,
                        lease_seconds: int, *, retry_kind: str = "initial",
                        owner_limit: int = 0) -> JobAttemptReceipt:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                group = await db.get(JobGroupRow, group_id, with_for_update=True)
                job = (await db.execute(select(JobRow).where(
                    JobRow.id == job_id).with_for_update())).scalar_one_or_none()
                if not job:
                    raise KeyError("job_not_found")
                if owner_limit:
                    if db.bind.dialect.name=="postgresql":
                        from sqlalchemy.dialects.postgresql import insert
                    else:
                        from sqlalchemy.dialects.sqlite import insert
                    await db.execute(insert(OwnerAdmissionRow).values(id=job.owner_id,
                        updated_at=now()).on_conflict_do_nothing())
                    admission=await db.get(OwnerAdmissionRow,job.owner_id,with_for_update=True)
                    active=await db.scalar(select(func.count()).select_from(JobLeaseRow).join(
                        JobRow,JobRow.id==JobLeaseRow.job_id).where(
                            JobRow.owner_id==job.owner_id,JobLeaseRow.job_id!=job.id,
                            JobLeaseRow.expires_at>now()))
                    if active>=owner_limit:raise RuntimeError("owner_concurrency_wait")
                    admission.updated_at=now()
                if JobState(job.state) in TERMINAL_JOB_STATES:
                    raise RuntimeError("job_terminal")
                control = await db.get(ThreadControlRow, job.thread_id)
                if (control and control.state != ThreadControlState.RUNNING.value
                        and job.state not in {JobState.PAUSING.value,JobState.CANCELLING.value}
                        and not (job.kind == JobKind.PROVIDER.value
                            and job.state == JobState.WAITING_EXTERNAL.value)):
                    raise RuntimeError("thread_paused")
                previous_outcome=(await db.execute(select(JobAttemptRow.outcome).where(
                    JobAttemptRow.job_id==job.id).order_by(
                    JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
                if retry_kind=='initial' and previous_outcome=='COLLECTION_RETRY':
                    retry_kind='collection'
                if retry_kind=='initial' and job.allocation_replacements:
                    retry_kind='allocation_replacement'
                    exhausted=(await db.scalars(select(JobAttemptRow).where(
                        JobAttemptRow.job_id==job.id,or_(JobAttemptRow.outcome=='ALLOCATION_EXPIRED',
                            JobAttemptRow.error_code=='allocation_replacement_capacity_unavailable'))
                        .order_by(JobAttemptRow.generation.desc()).limit(1))).first()
                    if exhausted:
                        retry_kind=('allocation_capacity_exhausted' if exhausted.error_code==
                            'allocation_replacement_capacity_unavailable' else 'allocation_recovery_exhausted')
                job.generation += 1
                job.state = JobState.RUNNING.value
                job.updated_at = now()
                if group.state == JobGroupState.COMMITTED.value:
                    group.state = JobGroupState.RUNNING.value
                    group.updated_at = now()
                attempt = JobAttemptRow(
                    id=identifier(), job_id=job.id, generation=job.generation,
                    worker_id=worker_id, node_id=node_id, executor_kind=job.kind,
                    retry_kind=retry_kind, request_digest=job.request_digest,
                    accepted_at=None, acceptance_receipt_json="{}", external_execution_id="",
                    started_at=now(), finished_at=None, outcome="", error_code="", error_detail="",
                )
                db.add(attempt)
                await db.flush()
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if lease and utc_datetime(lease.expires_at) > now() and lease.worker_id != worker_id:
                    raise RuntimeError("job_already_leased")
                if lease:
                    lease.attempt_id = attempt.id
                    lease.worker_id = worker_id
                    lease.generation = job.generation
                    lease.expires_at = now() + timedelta(seconds=lease_seconds)
                    lease.updated_at = now()
                else:
                    lease = JobLeaseRow(
                        job_id=job.id, attempt_id=attempt.id, worker_id=worker_id,
                        generation=job.generation,
                        expires_at=now() + timedelta(seconds=lease_seconds), updated_at=now(),
                    )
                    db.add(lease)
                await db.flush()
                origin=await db.get(TurnRow,job.origin_turn_id)
                traceparent=(canonical_traceparent(origin.last_traceparent)
                    or canonical_traceparent(origin.submit_traceparent)) if origin else ""
                return self.job_attempt_view(attempt).model_copy(update={"traceparent":traceparent})


    async def release_job_lease(self, job_id: str, worker_id: str, generation: int) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(delete(JobLeaseRow).where(
                JobLeaseRow.job_id == job_id, JobLeaseRow.worker_id == worker_id,
                JobLeaseRow.generation == generation,
            ))
            return bool(result.rowcount)


    async def renew_job_lease(self, job_id: str, worker_id: str, generation: int,
                              lease_seconds: int) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(update(JobLeaseRow).where(
                JobLeaseRow.job_id == job_id, JobLeaseRow.worker_id == worker_id,
                JobLeaseRow.generation == generation,
            ).values(expires_at=now()+timedelta(seconds=lease_seconds),updated_at=now()))
            return bool(result.rowcount)


    async def mark_job_submission_started(self, job_id: str, worker_id: str,
                                          generation: int) -> None:
        async with self.connections.begin() as db:
            lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
            attempt = (await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id == job_id, JobAttemptRow.generation == generation,
            ).with_for_update())).scalar_one_or_none()
            if (not lease or not attempt or lease.worker_id != worker_id
                    or lease.generation != generation or attempt.accepted_at is not None):
                raise RuntimeError("stale_job_generation")
            if attempt.outcome not in {"", "SUBMITTING", "INFRASTRUCTURE_WAIT"}:
                raise RuntimeError("job_submission_intent_conflict")
            attempt.outcome = "SUBMITTING"


    async def unresolved_job_submission(self, job_id: str, generation: int,*,
                                       include_current:bool=False) -> dict | None:
        async with self.connections() as db:
            row=(await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id == job_id,
                (JobAttemptRow.generation <= generation if include_current
                    else JobAttemptRow.generation < generation),
                JobAttemptRow.outcome == "SUBMITTING", JobAttemptRow.accepted_at.is_(None),
            ).order_by(JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
        if not row:return None
        return {"generation":row.generation,"external_execution_id":row.external_execution_id,
            "acceptance_receipt":json.loads(row.acceptance_receipt_json)}


    async def resolve_job_submission_absent(self,job_id:str,worker_id:str,
                                            generation:int,previous_generation:int)->None:
        async with self.connections.begin() as db:
            lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
            previous=(await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job_id,JobAttemptRow.generation==previous_generation)
                .with_for_update())).scalar_one_or_none()
            if (not lease or lease.worker_id!=worker_id or lease.generation!=generation
                    or not previous or previous.accepted_at is not None
                    or previous.outcome!="SUBMITTING"):
                raise RuntimeError("stale_job_generation")
            previous.outcome="PRE_ACCEPT_RETRY";previous.finished_at=now()


    async def job_deadline(self, job_id: str, timeout_seconds: int) -> datetime | None:
        async with self.connections() as db:
            started = (await db.execute(select(func.min(JobAttemptRow.started_at)).where(
                JobAttemptRow.job_id == job_id))).scalar_one_or_none()
        return utc_datetime(started)+timedelta(seconds=timeout_seconds) if started else None


    async def record_job_acceptance(self, job_id: str, worker_id: str, generation: int, *,
                                    external_execution_id: str, receipt: dict) -> JobAttemptReceipt:
        async with self.connections() as lookup:
            snapshot = await lookup.get(JobRow, job_id)
            if not snapshot:
                raise KeyError("job_not_found")
            request = JobRequest.model_validate_json(snapshot.request_json)
            reservations = (await lookup.execute(select(BudgetReservationRow).where(
                BudgetReservationRow.group_id == snapshot.group_id,
                BudgetReservationRow.budget_key.in_(list(request.budget_requirements) or [""]),
            ))).scalars().all()
            by_key = {row.budget_key: row for row in reservations}
            if set(by_key) != set(request.budget_requirements):
                raise RuntimeError("job_budget_reservation_missing")
            account_ids = sorted({row.account_id for row in reservations})
        async with self.job_lock(f"accept:{job_id}"):
            async with self.connections.begin() as db:
                accounts = {identity: await db.get(BudgetAccountRow, identity, with_for_update=True)
                    for identity in account_ids}
                locked_reservations = {row.budget_key: await db.get(
                    BudgetReservationRow, row.id, with_for_update=True)
                    for row in sorted(reservations, key=lambda value: value.id)}
                job = await db.get(JobRow, job_id, with_for_update=True)
                attempt = (await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id == job_id,
                    JobAttemptRow.generation == generation,
                ).with_for_update())).scalar_one_or_none()
                lease = await db.get(JobLeaseRow, job_id, with_for_update=True)
                if (not job or not attempt or not lease or lease.worker_id != worker_id
                        or lease.generation != generation or job.generation != generation):
                    raise RuntimeError("stale_job_generation")
                encoded = canonical_json(receipt)
                if attempt.accepted_at:
                    if (attempt.external_execution_id != external_execution_id
                            or attempt.acceptance_receipt_json != encoded):
                        raise RuntimeError("acceptance_receipt_conflict")
                    return self.job_attempt_view(attempt)
                for budget_key, amount in sorted(request.budget_requirements.items()):
                    reservation = locked_reservations[budget_key]
                    account = accounts[reservation.account_id]
                    idempotency_key = f"accept:{job.id}:{job.request_digest}:{budget_key}"
                    ledger = (await db.execute(select(BudgetLedgerRow).where(
                        BudgetLedgerRow.reservation_id == reservation.id,
                        BudgetLedgerRow.idempotency_key == idempotency_key,
                    ))).scalar_one_or_none()
                    if ledger:
                        continue
                    if reservation.consumed + reservation.released + amount > reservation.reserved:
                        raise RuntimeError("job_budget_reservation_exhausted")
                    reservation.consumed += amount
                    reservation.state = ("consumed" if reservation.consumed + reservation.released
                        == reservation.reserved else "partial")
                    reservation.updated_at = now()
                    account.reserved -= amount
                    account.consumed += amount
                    account.updated_at = now()
                    db.add(BudgetLedgerRow(
                        id=identifier(), reservation_id=reservation.id,
                        group_id=reservation.group_id, job_id=job.id,
                        action="consume", amount=amount, idempotency_key=idempotency_key,
                        detail_json=canonical_json({"generation": generation,
                            "request_digest": job.request_digest}), created_at=now(),
                    ))
                attempt.accepted_at = now()
                attempt.external_execution_id = external_execution_id
                attempt.acceptance_receipt_json = encoded
                pause = (await db.execute(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id == job.thread_id,
                    PauseParticipantRow.kind == "child_harness",
                    PauseParticipantRow.participant_id == job.id,
                    PauseParticipantRow.state == PauseParticipantState.REQUESTED.value,
                ).limit(1))).scalar_one_or_none()
                job.state = (JobState.PAUSING if pause else JobState.WAITING_EXTERNAL).value
                job.updated_at = now()
                await db.flush()
                return self.job_attempt_view(attempt)
