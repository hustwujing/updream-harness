"""Job Groups persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from sqlalchemy import (select)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sandbox_harness.common.contracts.domain import (ItemType, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (JobAttemptReceipt, JobGroupCreate, JobGroupDraftCreate, JobGroupDraftState, JobGroupDraftView, JobGroupState, JobGroupView, JobState, JobView, JobRequest, TERMINAL_JOB_STATES)
from .models import (AuthorizationSnapshotRow, BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, ChildContinuationRow, ItemRow, JobArtifactRow, JobAttemptRow, JobGroupDraftRow, JobGroupRow, JobRow, ReviewGateRow, ThreadRow, canonical_digest, canonical_json, utc_datetime)


class JobGroupsTransactions:
    async def create_job_group_draft(self, owner_id: str,
                                     value: JobGroupDraftCreate) -> tuple[JobGroupDraftView, bool]:
        value.validate_member_limit()
        lock_identity = f"draft-create:{value.thread_id}:{value.idempotency_key}"
        async with self.job_lock(lock_identity):
            async with self.connections.begin() as db:
                existing = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.thread_id == value.thread_id,
                    JobGroupDraftRow.idempotency_key == value.idempotency_key,
                ).with_for_update())).scalar_one_or_none()
                if existing:
                    if existing.owner_id != owner_id or existing.request_digest != value.request_digest:
                        raise RuntimeError("idempotency_conflict")
                    return self.job_group_draft_view(existing), False
                timestamp = now()
                row = JobGroupDraftRow(
                    id=identifier(), owner_id=owner_id, thread_id=value.thread_id,
                    origin_turn_id=value.origin_turn_id, idempotency_key=value.idempotency_key,
                    request_digest=value.request_digest, request_json=value.model_dump_json(),
                    state=(JobGroupDraftState.AWAITING_CONFIRMATION.value
                        if value.requires_confirmation else JobGroupDraftState.DRAFT.value),
                    requires_confirmation=value.requires_confirmation,
                    authorization_gaps_json=canonical_json(value.authorization_gaps),
                    expires_at=value.expires_at, confirmed_group_id="",
                    created_at=timestamp, updated_at=timestamp,
                )
                db.add(row)
                await db.flush()
                return self.job_group_draft_view(row), True


    async def get_job_group_draft(self, draft_id: str,
                                  owner_id: str | None = None) -> JobGroupDraftView | None:
        async with self.connections() as db:
            query = select(JobGroupDraftRow).where(JobGroupDraftRow.id == draft_id)
            if owner_id is not None:
                query = query.where(JobGroupDraftRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
            return self.job_group_draft_view(row) if row else None


    async def get_job_group_draft_request(self, draft_id: str,
                                          owner_id: str) -> JobGroupDraftCreate | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.id == draft_id,
                JobGroupDraftRow.owner_id == owner_id,
            ))).scalar_one_or_none()
            return JobGroupDraftCreate.model_validate_json(row.request_json) if row else None


    async def list_job_group_drafts(self, thread_id: str, owner_id: str) -> list[JobGroupDraftView]:
        async with self.connections() as db:
            rows = (await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.thread_id == thread_id,
                JobGroupDraftRow.owner_id == owner_id,
            ).order_by(JobGroupDraftRow.created_at.desc()))).scalars().all()
            return [self.job_group_draft_view(row) for row in rows]


    async def update_job_group_draft(self, draft_id: str, owner_id: str, *,
                                     expected_digest: str,
                                     value: JobGroupDraftCreate) -> JobGroupDraftView:
        value.validate_member_limit()
        async with self.job_lock(f"draft:{draft_id}"):
            async with self.connections.begin() as db:
                row = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.id == draft_id,
                    JobGroupDraftRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("job_group_draft_not_found")
                if row.state not in {JobGroupDraftState.DRAFT.value,
                                     JobGroupDraftState.AWAITING_CONFIRMATION.value}:
                    raise RuntimeError("draft_not_editable")
                if row.request_digest != expected_digest:
                    raise RuntimeError("draft_digest_conflict")
                if (row.thread_id != value.thread_id or row.origin_turn_id != value.origin_turn_id
                        or row.idempotency_key != value.idempotency_key):
                    raise RuntimeError("draft_identity_immutable")
                row.request_digest = value.request_digest
                row.request_json = value.model_dump_json()
                row.requires_confirmation = value.requires_confirmation
                row.authorization_gaps_json = canonical_json(value.authorization_gaps)
                row.expires_at = value.expires_at
                row.updated_at = now()
                await db.flush()
                return self.job_group_draft_view(row)


    async def reject_job_group_draft(self, draft_id: str, owner_id: str) -> JobGroupDraftView:
        async with self.job_lock(f"draft:{draft_id}"):
            async with self.connections.begin() as db:
                row = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.id == draft_id, JobGroupDraftRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not row:
                    raise KeyError("job_group_draft_not_found")
                if row.state not in {JobGroupDraftState.DRAFT.value,
                                     JobGroupDraftState.AWAITING_CONFIRMATION.value}:
                    raise RuntimeError("draft_not_editable")
                row.state = JobGroupDraftState.REJECTED.value
                row.updated_at = now()
                await db.flush()
                return self.job_group_draft_view(row)


    async def expire_job_group_drafts(self) -> int:
        async with self.connections.begin() as db:
            rows=list((await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.state.in_([
                    JobGroupDraftState.DRAFT.value,
                    JobGroupDraftState.AWAITING_CONFIRMATION.value,
                ]),
                JobGroupDraftRow.expires_at <= now()).with_for_update())).scalars().all())
            for row in rows:
                row.state=JobGroupDraftState.EXPIRED.value;row.updated_at=now()
                existing=(await db.execute(select(ItemRow.id).where(ItemRow.thread_id==row.thread_id,
                    ItemRow.type==ItemType.WARNING.value,
                    ItemRow.payload_json.like(f'%"job_group_draft_id": "{row.id}"%'))
                    .limit(1))).scalar_one_or_none()
                thread=await db.get(ThreadRow,row.thread_id)
                if not existing and thread:
                    sequence=await self.next_item_sequence(db,row.thread_id);timestamp=now()
                    db.add(ItemRow(id=identifier(),thread_id=row.thread_id,
                        turn_id=row.origin_turn_id,sequence=sequence,type=ItemType.WARNING.value,
                        status="completed",summary="并行任务确认已过期，请根据当前需求重新规划。",
                        payload_json=json.dumps({"job_group_draft_id":row.id,
                            "code":"job_group_draft_expired"}),created_at=timestamp,updated_at=timestamp))
            return len(rows)


    async def _create_job_group_locked(self, db: AsyncSession, owner_id: str,
                                       value: JobGroupCreate, *, draft_id: str | None = None
                                       ) -> tuple[JobGroupRow, list[JobRow]]:
        value.validate_member_limit()
        existing = (await db.execute(select(JobGroupRow).where(
            JobGroupRow.thread_id == value.thread_id,
            JobGroupRow.idempotency_key == value.idempotency_key,
        ).with_for_update())).scalar_one_or_none()
        if existing:
            if existing.owner_id != owner_id or existing.request_digest != value.request_digest:
                raise RuntimeError("idempotency_conflict")
            jobs = (await db.execute(select(JobRow).where(
                JobRow.group_id == existing.id).order_by(JobRow.created_at, JobRow.local_job_id))).scalars().all()
            for job in jobs:
                request=JobRequest.model_validate_json(job.request_json)
                await self._ensure_review_gate_locked(db,job,request)
            return existing, list(jobs)
        timestamp = now()
        for request in value.jobs:
            if request.reconciles_job_id:
                source=await db.get(JobRow,request.reconciles_job_id,with_for_update=True)
                if (not source or source.owner_id!=owner_id or source.thread_id!=value.thread_id
                        or JobState(source.state) not in TERMINAL_JOB_STATES):
                    raise RuntimeError("reconciliation_lineage_invalid")
                source_request=JobRequest.model_validate_json(source.request_json)
                if source_request.reconciliation_policy.get("supported") is not True:
                    raise RuntimeError("reconciliation_not_supported")
        snapshot_json = canonical_json(value.authorization_snapshot)
        snapshot = AuthorizationSnapshotRow(
            id=identifier(), owner_id=owner_id, thread_id=value.thread_id,
            origin_turn_id=value.origin_turn_id, scope="job_group",
            version=int(value.authorization_snapshot.get("version", 0)),
            snapshot_digest=canonical_digest(value.authorization_snapshot),
            snapshot_json=snapshot_json, created_at=timestamp,
        )
        group_id = identifier()
        reservations = value.authorization_snapshot.get("budget_reservations", [])
        if not isinstance(reservations, list):
            raise ValueError("budget_reservations must be a list")
        normalized_reservations = []
        for reservation in reservations:
            if (not isinstance(reservation, dict)
                    or set(reservation) != {"account_id", "amount"}
                    or not isinstance(reservation["account_id"], str)
                    or not isinstance(reservation["amount"], int)
                    or reservation["amount"] < 1):
                raise ValueError("invalid budget_reservation request")
            normalized_reservations.append(reservation)
        if len({row["account_id"] for row in normalized_reservations}) != len(normalized_reservations):
            raise ValueError("duplicate budget account reservation")
        for reservation in sorted(normalized_reservations, key=lambda row: row["account_id"]):
            account = await db.get(BudgetAccountRow, reservation["account_id"], with_for_update=True)
            if not account or account.owner_id != owner_id or account.thread_id != value.thread_id:
                raise RuntimeError("budget_account_outside_authorization_scope")
            amount = reservation["amount"]
            if account.limit - account.consumed - account.reserved < amount:
                raise RuntimeError("budget_exhausted")
            budget_reservation = BudgetReservationRow(
                id=identifier(), account_id=account.id, group_id=group_id,
                authorization_snapshot_id=snapshot.id, budget_key=account.budget_key,
                reserved=amount, consumed=0, released=0, state="reserved",
                created_at=timestamp, updated_at=timestamp,
            )
            account.reserved += amount
            account.updated_at = timestamp
            db.add_all([budget_reservation, BudgetLedgerRow(
                id=identifier(), reservation_id=budget_reservation.id, group_id=group_id,
                job_id="", action="reserve", amount=amount,
                idempotency_key=f"group-reserve:{group_id}:{account.budget_key}",
                detail_json="{}", created_at=timestamp,
            )])
        group = JobGroupRow(
            id=group_id, owner_id=owner_id, thread_id=value.thread_id,
            origin_turn_id=value.origin_turn_id, draft_id=draft_id,
            idempotency_key=value.idempotency_key, request_digest=value.request_digest,
            authorization_snapshot_id=snapshot.id, state=JobGroupState.COMMITTED.value,
            member_count=len(value.jobs), suppress_continuation=value.suppress_continuation,
            terminal_at=None, continued_at=None, created_at=timestamp, updated_at=timestamp,
        )
        db.add_all([snapshot, group])
        rows: list[JobRow] = []
        for request in value.jobs:
            row = JobRow(
                id=identifier(), group_id=group.id, local_job_id=request.local_job_id,
                owner_id=owner_id, thread_id=value.thread_id, origin_turn_id=value.origin_turn_id,
                kind=request.kind.value, adapter=request.adapter,
                descriptor_id=request.descriptor_id, descriptor_version=request.descriptor_version,
                descriptor_digest=request.descriptor_digest, idempotency_key=request.idempotency_key,
                request_digest=request.request_digest, request_json=request.model_dump_json(),
                state=JobState.CREATED.value, generation=0, cancel_requested=False,
                replaces_job_id=request.replaces_job_id, reconciles_job_id=request.reconciles_job_id,
                result_json="{}", result_digest="", terminal_at=None,
                created_at=timestamp, updated_at=timestamp,
            )
            db.add(row)
            await self._ensure_review_gate_locked(db,row,request)
            rows.append(row)
        await db.flush()
        return group, rows


    async def _ensure_review_gate_locked(self,db:AsyncSession,job:JobRow,request:JobRequest)->None:
        if request.adapter!="reviewer-harness":return
        context=request.parameters.get("review_context",{});target=context.get("target")
        if not isinstance(target,dict):return
        existing=await db.get(ReviewGateRow,job.id,with_for_update=True)
        encoded_ids=canonical_json(target.get("artifact_ids",[]))
        encoded_digests=canonical_json(target.get("artifact_digests",[]))
        required=bool(context.get("required"))
        if existing:
            identity=(existing.source_job_id,existing.source_generation,existing.artifact_role,
                existing.artifact_ids_json,existing.artifact_digests_json,existing.required)
            expected=(target.get("source_job_id"),target.get("source_generation"),
                target.get("artifact_role"),encoded_ids,encoded_digests,required)
            if identity!=expected:raise RuntimeError("review_gate_conflict")
            return
        db.add(ReviewGateRow(reviewer_job_id=job.id,source_job_id=target["source_job_id"],
            source_generation=int(target["source_generation"]),artifact_role=target["artifact_role"],
            artifact_ids_json=encoded_ids,artifact_digests_json=encoded_digests,required=required,
            state="pending",receipt_json="{}",warning="",created_at=now(),updated_at=now()))


    async def create_job_group(self, owner_id: str,
                               value: JobGroupCreate) -> tuple[JobGroupView, list[JobView]]:
        lock_identity = f"group-create:{value.thread_id}:{value.idempotency_key}"
        async with self.job_lock(lock_identity):
            async with self.connections.begin() as db:
                group, jobs = await self._create_job_group_locked(db, owner_id, value)
                return self.job_group_view(group), [self.job_view(job) for job in jobs]


    async def confirm_job_group_draft(self, draft_id: str,
                                      owner_id: str) -> tuple[JobGroupView, list[JobView]]:
        expired = False
        async with self.job_lock(f"draft:{draft_id}"):
            async with self.connections.begin() as db:
                draft = (await db.execute(select(JobGroupDraftRow).where(
                    JobGroupDraftRow.id == draft_id,
                    JobGroupDraftRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
                if not draft:
                    raise KeyError("job_group_draft_not_found")
                if draft.confirmed_group_id:
                    group = await db.get(JobGroupRow, draft.confirmed_group_id)
                    jobs = (await db.execute(select(JobRow).where(JobRow.group_id == group.id)
                        .order_by(JobRow.created_at, JobRow.local_job_id))).scalars().all()
                    return self.job_group_view(group), [self.job_view(job) for job in jobs]
                if draft.state not in {JobGroupDraftState.DRAFT.value,
                                       JobGroupDraftState.AWAITING_CONFIRMATION.value}:
                    raise RuntimeError("draft_not_editable")
                if utc_datetime(draft.expires_at) <= now():
                    draft.state = JobGroupDraftState.EXPIRED.value
                    draft.updated_at = now()
                    expired = True
                else:
                    draft_value = JobGroupDraftCreate.model_validate_json(draft.request_json)
                    group_value = JobGroupCreate.model_validate(draft_value.model_dump(exclude={
                        "expires_at", "requires_confirmation", "authorization_gaps",
                    }))
                    group, jobs = await self._create_job_group_locked(
                        db, owner_id, group_value, draft_id=draft.id)
                    draft.state = JobGroupDraftState.CONFIRMED.value
                    draft.confirmed_group_id = group.id
                    draft.updated_at = now()
                    await db.flush()
                    return self.job_group_view(group), [self.job_view(job) for job in jobs]
        if expired:
            raise RuntimeError("draft_expired")
        raise RuntimeError("draft_confirmation_failed")


    async def get_job_group(self, group_id: str) -> JobGroupView | None:
        async with self.connections() as db:
            row = await db.get(JobGroupRow, group_id)
            return self.job_group_view(row) if row else None


    async def child_parent_for_group(self,group_id:str)->str:
        async with self.connections() as db:
            encoded=(await db.execute(select(AuthorizationSnapshotRow.snapshot_json).join(
                JobGroupRow,JobGroupRow.authorization_snapshot_id==AuthorizationSnapshotRow.id).where(
                JobGroupRow.id==group_id))).scalar_one_or_none()
        if not encoded:return ''
        try:value=json.loads(encoded)
        except (TypeError,ValueError):return ''
        identity=value.get('parent_job_id','')
        return identity if isinstance(identity,str) else ''


    async def ready_child_continuation(self,parent_job_id:str,group_id:str,outbox_id:str,
                                       result:dict)->None:
        async with self.connections.begin() as db:
            existing=(await db.execute(select(ChildContinuationRow).where(
                ChildContinuationRow.group_id==group_id).with_for_update())).scalar_one_or_none()
            if existing:return
            db.add(ChildContinuationRow(id=identifier(),parent_job_id=parent_job_id,
                group_id=group_id,outbox_id=outbox_id,state='ready',
                result_json=canonical_json(result),resume_execution_id='',
                created_at=now(),updated_at=now()))


    async def child_continuation(self,parent_job_id:str)->dict|None:
        async with self.connections() as db:
            row=(await db.execute(select(ChildContinuationRow).where(
                ChildContinuationRow.parent_job_id==parent_job_id).order_by(
                ChildContinuationRow.created_at.desc()).limit(1))).scalar_one_or_none()
        if not row:return None
        return {'id':row.id,'group_id':row.group_id,'outbox_id':row.outbox_id,
            'state':row.state,'result':json.loads(row.result_json),
            'resume_execution_id':row.resume_execution_id}


    async def child_continuation_artifacts(self,group_id:str)->list[dict]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobArtifactRow,JobRow.id).join(
                JobRow,JobRow.id==JobArtifactRow.job_id).where(
                JobRow.group_id==group_id).order_by(JobRow.local_job_id,
                JobArtifactRow.artifact_role,JobArtifactRow.position))).all()
        return [{'job_id':job_id,'artifact_id':row.artifact_id,'filename':row.filename,
            'object_ref':row.object_ref,'size':row.size,'mime':row.mime,
            'artifact_role':row.artifact_role} for row,job_id in rows]


    async def mark_child_continuation_resumed(self,identity:str,execution_id:str)->None:
        async with self.connections.begin() as db:
            row=await db.get(ChildContinuationRow,identity,with_for_update=True)
            if not row:raise KeyError('child_continuation_not_found')
            if row.resume_execution_id and row.resume_execution_id!=execution_id:
                raise RuntimeError('child_continuation_execution_conflict')
            row.resume_execution_id=execution_id;row.state='resumed';row.updated_at=now()


    async def child_has_open_group(self,parent_job_id:str)->bool:
        async with self.connections() as db:
            snapshots=(await db.execute(select(AuthorizationSnapshotRow.snapshot_json,JobGroupRow.state).join(
                JobGroupRow,JobGroupRow.authorization_snapshot_id==AuthorizationSnapshotRow.id).where(
                JobGroupRow.state.in_([JobGroupState.COMMITTED.value,JobGroupState.RUNNING.value,
                    JobGroupState.TERMINAL.value])))).all()
            drafts=(await db.execute(select(JobGroupDraftRow.request_json,JobGroupDraftRow.state).where(
                JobGroupDraftRow.state.in_([JobGroupDraftState.DRAFT.value,
                    JobGroupDraftState.AWAITING_CONFIRMATION.value,
                    JobGroupDraftState.CONFIRMED.value])))).all()
        for encoded,state in snapshots:
            try:match=json.loads(encoded).get('parent_job_id')==parent_job_id
            except (TypeError,ValueError):match=False
            if match and state!=JobGroupState.CONTINUED.value:return True
        for encoded,state in drafts:
            try:match=JobGroupCreate.model_validate_json(encoded).authorization_snapshot.get(
                'parent_job_id')==parent_job_id
            except (TypeError,ValueError):match=False
            if match and state!=JobGroupDraftState.REJECTED.value:return True
        return False


    async def get_job_group_for_owner(self, group_id: str, owner_id: str) -> JobGroupView | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobGroupRow).where(
                JobGroupRow.id == group_id, JobGroupRow.owner_id == owner_id,
            ))).scalar_one_or_none()
            return self.job_group_view(row) if row else None


    async def get_job(self, job_id: str) -> JobView | None:
        async with self.connections() as db:
            row = await db.get(JobRow, job_id)
            return self.job_view(row) if row else None


    async def get_job_for_owner(self, job_id: str, owner_id: str) -> JobView | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobRow).where(
                JobRow.id == job_id, JobRow.owner_id == owner_id,
            ))).scalar_one_or_none()
            return self.job_view(row) if row else None


    async def list_jobs_for_group(self, group_id: str, owner_id: str | None = None) -> list[JobView]:
        async with self.connections() as db:
            query = select(JobRow).where(JobRow.group_id == group_id)
            if owner_id is not None:
                query = query.where(JobRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(
                JobRow.created_at, JobRow.local_job_id))).scalars().all()
            return [self.job_view(row) for row in rows]


    async def list_jobs_for_thread(self, thread_id: str) -> list[JobView]:
        async with self.connections() as db:
            rows = (await db.execute(select(JobRow).where(JobRow.thread_id == thread_id)
                .order_by(JobRow.created_at, JobRow.local_job_id))).scalars().all()
            return [self.job_view(row) for row in rows]


    async def get_job_request(self, job_id: str) -> JobRequest | None:
        async with self.connections() as db:
            row = await db.get(JobRow, job_id)
            if not row:
                return None
            return JobRequest.model_validate_json(row.request_json)


    async def get_job_authorization_context(self, job_id: str, owner_id: str,
                                            thread_id: str) -> dict | None:
        async with self.connections() as db:
            row=(await db.execute(select(JobRow,JobGroupRow,AuthorizationSnapshotRow)
                .join(JobGroupRow,JobGroupRow.id==JobRow.group_id)
                .join(AuthorizationSnapshotRow,
                    AuthorizationSnapshotRow.id==JobGroupRow.authorization_snapshot_id)
                .where(JobRow.id==job_id,JobRow.owner_id==owner_id,
                    JobRow.thread_id==thread_id))).one_or_none()
            if not row:return None
            job,group,snapshot=row
            return {'job':self.job_view(job),'request':JobRequest.model_validate_json(job.request_json),
                'group_id':group.id,'snapshot':json.loads(snapshot.snapshot_json)}


    async def latest_job_acceptance(self, job_id: str) -> JobAttemptReceipt | None:
        async with self.connections() as db:
            row = (await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id == job_id,
                JobAttemptRow.accepted_at.is_not(None),
            ).order_by(JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
            return self.job_attempt_view(row) if row else None

