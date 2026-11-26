"""Allocations persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import hashlib
from sqlalchemy import (select)
from sandbox_harness.common.contracts.domain import (PauseParticipantState, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (JobState, TERMINAL_JOB_STATES)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxHandle)
from .models import (JobAttemptRow, JobCleanupRow, JobLeaseRow, JobRow, PauseParticipantRow, ResourceReservationRow, SandboxAllocationRow, canonical_json, utc_datetime)


class AllocationsTransactions:
    async def replace_unstarted_allocation(self,job_id:str,worker_id:str,generation:int,
                                          allocation_identity:str)->str:
        """Fence unstarted work, retaining its distinct resource/cleanup ownership."""
        async with self.job_lock(f'job:{job_id}'):
            async with self.connections.begin() as db:
                job=await db.get(JobRow,job_id,with_for_update=True)
                lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
                if (not job or not lease or job.generation!=generation or lease.generation!=generation
                        or lease.worker_id!=worker_id or utc_datetime(lease.expires_at)<=now()
                        or JobState(job.state) in TERMINAL_JOB_STATES):
                    raise RuntimeError('stale_job_generation')
                if job.cancel_requested:return 'cancelled'
                row=await db.get(SandboxAllocationRow,allocation_identity,with_for_update=True)
                if not row or row.job_id!=job_id or row.job_generation>generation:
                    raise RuntimeError('sandbox_allocation_owner_mismatch')
                if row.state in {'READY','REJECTED','CANCELLED','RELEASED'}:return 'observe'
                policy=json.loads(row.recovery_policy_json)
                seconds=policy.get('recovery_seconds')
                if (row.state not in {'SENDING','UNKNOWN'} or not seconds or not row.issued_at
                        or (now()-utc_datetime(row.issued_at)).total_seconds()<seconds):
                    raise RuntimeError('allocation_recovery_not_due')
                source=(await db.scalars(select(JobAttemptRow).where(JobAttemptRow.job_id==job_id,
                    JobAttemptRow.generation==row.job_generation).with_for_update())).one_or_none()
                current=await db.get(JobAttemptRow,lease.attempt_id,with_for_update=True)
                receipt=json.loads(source.acceptance_receipt_json) if source else {}
                if (not source or source.accepted_at or source.outcome!='SUBMITTING'
                        or receipt.get('allocation_phase')!='allocating'
                        or receipt.get('allocation_dispatch_id')!=allocation_identity
                        or not source.external_execution_id or not current or current.accepted_at
                        or current.outcome not in {'','SUBMITTING'}
                        or (current.id!=source.id and current.acceptance_receipt_json!='{}')):
                    raise RuntimeError('allocation_not_replaceable')
                cleanup=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==job_id,
                    JobCleanupRow.generation==source.generation).with_for_update())).one_or_none()
                timestamp=now()
                if not cleanup:
                    db.add(JobCleanupRow(id=identifier(),job_id=job_id,adapter=job.adapter,
                        generation=source.generation,external_execution_id=source.external_execution_id,
                        acceptance_receipt_json=source.acceptance_receipt_json,state='pending',
                        phase='external_quiescence',attempt_count=0,last_error='',claim_owner='',
                        claim_expires_at=None,created_at=timestamp,updated_at=timestamp))
                retry=job.allocation_replacements<min(1,int(policy.get('max_replacements',0)))
                for attempt in {source.id:source,current.id:current}.values():
                    attempt.outcome='ALLOCATION_REPLACED' if retry else 'ALLOCATION_EXPIRED'
                    attempt.finished_at=timestamp;attempt.error_code='allocation_recovery_required'
                job.updated_at=timestamp
                if not retry:return 'exhausted'
                job.allocation_replacements+=1
                pause=(await db.scalars(select(PauseParticipantRow.id).where(
                    PauseParticipantRow.thread_id==job.thread_id,PauseParticipantRow.kind=='child_harness',
                    PauseParticipantRow.participant_id==job.id,
                    PauseParticipantRow.state==PauseParticipantState.REQUESTED.value).limit(1))).first()
                job.state=JobState.PAUSING.value if pause else JobState.QUEUED.value
                await db.delete(lease)
                return 'queued'


    async def record_job_submission_allocation(self,job_id:str,generation:int,*,
                                               external_execution_id:str,
                                               receipt:dict)->None:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id,with_for_update=True)
            lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
            attempt=(await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job_id,JobAttemptRow.generation==generation)
                .with_for_update())).scalar_one_or_none()
            if (not job or not lease or not attempt or job.generation!=generation
                    or lease.generation!=generation or attempt.accepted_at is not None
                    or attempt.outcome!="SUBMITTING"):
                raise RuntimeError("stale_job_generation")
            if receipt.get('allocation_phase')=='runtime_submission' and job.cancel_requested:
                raise RuntimeError('job_cancelled_before_acceptance')
            attempt.external_execution_id=external_execution_id
            attempt.acceptance_receipt_json=canonical_json(receipt)


    async def release_unstarted_job_reservation(self,job_id:str,generation:int,
                                                original_generation:int|None,*,reservation_id:str='')->None:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id,with_for_update=True)
            lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
            if not job or not lease or job.generation!=generation or lease.generation!=generation:
                raise RuntimeError('stale_job_generation')
            if original_generation is None:
                reserved=await db.get(ResourceReservationRow,reservation_id,with_for_update=True)
                if not reserved or reserved.job_id!=job_id:raise RuntimeError('stale_resource_reservation')
                original_generation=reserved.job_generation
            original=(await db.scalars(select(JobAttemptRow).where(JobAttemptRow.job_id==job_id,
                JobAttemptRow.generation==original_generation).with_for_update())).one_or_none()
            if (original_generation>=generation or not original or original.accepted_at):
                raise RuntimeError('stale_job_generation')
            receipt=json.loads(original.acceptance_receipt_json or '{}')
            if receipt.get('allocation_phase') not in {'reserving_resources','awaiting_capacity','absent'}:
                raise RuntimeError('job_allocation_phase_conflict')
            reservation=(await db.scalars(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id==job_id,
                ResourceReservationRow.job_generation==original_generation).with_for_update())).one_or_none()
            if reservation_id and (not reservation or reservation.id!=reservation_id):
                raise RuntimeError('stale_resource_reservation')
            if reservation and reservation.state!='released':
                reservation.state='released';reservation.released_at=now();reservation.expires_at=now()


    @staticmethod
    def _resolved_allocation_receipt(previous:dict,handle:SandboxHandle)->dict:
        profile=previous.get('allocation_profile') or {}
        if (previous.get('allocation_id')!=handle.allocation_id
                or previous.get('allocation_provider')!=handle.provider
                or profile.get('compatibility_key')!=handle.profile_key
                or profile.get('revision')!=handle.revision
                or previous.get('sandbox_id',handle.sandbox_id)!=handle.sandbox_id
                or previous.get('sandbox_handle',handle.to_mapping())!=handle.to_mapping()):
            raise RuntimeError('job_allocation_identity_mismatch')
        return {**previous,'sandbox_id':handle.sandbox_id,'sandbox_handle':handle.to_mapping()}


    async def record_recovered_job_allocation(self,job_id:str,generation:int,
                                               original_generation:int,handle:SandboxHandle)->None:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id,with_for_update=True)
            lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
            previous=(await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job_id,JobAttemptRow.generation==original_generation)
                .with_for_update())).scalar_one_or_none()
            if (not job or not lease or job.generation!=generation or lease.generation!=generation
                    or original_generation>=generation or not previous
                    or previous.outcome!='SUBMITTING' or previous.accepted_at is not None):
                raise RuntimeError('stale_job_generation')
            receipt=json.loads(previous.acceptance_receipt_json)
            if receipt.get('allocation_phase')!='allocating':
                raise RuntimeError('job_allocation_phase_conflict')
            previous.acceptance_receipt_json=canonical_json(
                self._resolved_allocation_receipt(receipt,handle))


    async def mark_recovered_allocation_absent(self,job_id:str,generation:int,
                                               original_generation:int)->None:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id,with_for_update=True)
            lease=await db.get(JobLeaseRow,job_id,with_for_update=True)
            previous=(await db.execute(select(JobAttemptRow).where(JobAttemptRow.job_id==job_id,
                JobAttemptRow.generation==original_generation).with_for_update())).scalar_one_or_none()
            if (not job or not lease or job.generation!=generation or lease.generation!=generation
                    or original_generation>=generation or not previous
                    or previous.outcome!='SUBMITTING' or previous.accepted_at is not None):
                raise RuntimeError('stale_job_generation')
            receipt=json.loads(previous.acceptance_receipt_json)
            if (receipt.get('allocation_phase') not in {'allocating','preparing'}
                    or (not receipt.get('sandbox_handle') and not await self._dispatched_allocation_absent(
                        db,job_id,receipt))):
                raise RuntimeError('job_allocation_phase_conflict')
            previous.acceptance_receipt_json=canonical_json({**receipt,'allocation_phase':'absent'})


    async def _dispatched_allocation_absent(self,db,job_id,receipt):
        identity=receipt.get('allocation_dispatch_id')
        namespace=receipt.get('allocation_namespace')
        provider=receipt.get('allocation_provider');allocation=receipt.get('allocation_id')
        if not all(isinstance(value,str) and value for value in [identity,namespace,provider,allocation]):
            return False
        expected=hashlib.sha256(json.dumps([namespace,provider,allocation],separators=(',',':')).encode()).hexdigest()
        if identity!=expected:raise RuntimeError('sandbox_allocation_identity_mismatch')
        row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
        if row and row.job_id!=job_id:raise RuntimeError('sandbox_allocation_owner_mismatch')
        return not row or row.state in {'REJECTED','CANCELLED','RELEASED'}

