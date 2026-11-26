"""Cleanup persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from datetime import (timedelta)
from sqlalchemy import (and_, or_, select, update)
from sandbox_harness.common.contracts.domain import (identifier, now)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxHandle)
from .models import (JobAttemptRow, JobCleanupRow, JobRow, canonical_json, utc_datetime)


class CleanupTransactions:
    async def enqueue_job_cleanup(self, job_id: str, generation: int) -> bool:
        async with self.connections.begin() as db:
            job = await db.get(JobRow, job_id, with_for_update=True)
            attempt = (await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id == job_id, JobAttemptRow.generation == generation,
                JobAttemptRow.accepted_at.is_not(None)).with_for_update())).scalar_one_or_none()
            if not attempt:
                attempt=(await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id==job_id,JobAttemptRow.accepted_at.is_not(None))
                    .order_by(JobAttemptRow.generation.desc()).limit(1).with_for_update()
                    )).scalar_one_or_none()
            if not attempt:
                attempt=(await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id==job_id,JobAttemptRow.outcome=="SUBMITTING",
                    JobAttemptRow.external_execution_id!="").order_by(
                    JobAttemptRow.generation.desc()).limit(1).with_for_update()
                    )).scalar_one_or_none()
            if not job or not attempt or not attempt.external_execution_id:
                return False
            existing = (await db.execute(select(JobCleanupRow).where(
                JobCleanupRow.job_id==job_id,JobCleanupRow.generation==attempt.generation)
                .with_for_update())).scalar_one_or_none()
            if existing:
                return True
            timestamp=now();db.add(JobCleanupRow(id=identifier(),job_id=job.id,adapter=job.adapter,
                generation=attempt.generation,external_execution_id=attempt.external_execution_id,
                acceptance_receipt_json=attempt.acceptance_receipt_json,state="pending",phase="external_quiescence",
                attempt_count=0,last_error="",claim_owner="",claim_expires_at=None,
                created_at=timestamp,updated_at=timestamp))
            return True


    async def claim_job_cleanups(self, owner: str, lease_seconds: int,
                                 *, limit: int = 10, adapters: set[str] | None = None) -> list[dict]:
        if adapters is not None and not adapters:return []
        async with self.connections.begin() as db:
            query=select(JobCleanupRow).where(or_(
                JobCleanupRow.state=="pending",and_(JobCleanupRow.state=="running",
                    JobCleanupRow.claim_expires_at < now())))
            if adapters is not None:query=query.where(JobCleanupRow.adapter.in_(adapters))
            rows=list((await db.execute(query.order_by(
                JobCleanupRow.updated_at,JobCleanupRow.job_id).limit(limit)
                .with_for_update(skip_locked=True))).scalars().all())
            expires=now()+timedelta(seconds=lease_seconds)
            result=[]
            for row in rows:
                row.state="running";row.claim_owner=owner;row.claim_expires_at=expires
                row.attempt_count+=1;row.updated_at=now()
                result.append({"cleanup_id":row.id,"job_id":row.job_id,"adapter":row.adapter,
                    "generation":row.generation,"external_execution_id":row.external_execution_id,
                    "acceptance_receipt":json.loads(row.acceptance_receipt_json),
                    "attempt_count":row.attempt_count,"phase":row.phase})
            return result


    async def record_job_cleanup_receipt(self,cleanup_id:str,owner:str,receipt:dict)->None:
        """Fence the discovered handle before an unstarted allocation is deleted."""
        async with self.connections() as db:
            job_id=(await db.scalars(select(JobCleanupRow.job_id).where(
                JobCleanupRow.id==cleanup_id))).one_or_none()
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id,with_for_update=True) if job_id else None
            row=await db.get(JobCleanupRow,cleanup_id,with_for_update=True)
            if not row or row.claim_owner!=owner or row.state!='running':
                raise RuntimeError('job_cleanup_claim_lost')
            previous=json.loads(row.acceptance_receipt_json)
            if receipt=={**previous,'allocation_phase':'absent'}:
                if (not job or job.generation<row.generation
                        or not await self._dispatched_allocation_absent(db,job.id,previous)):
                    raise RuntimeError('job_cleanup_receipt_mismatch')
                row.acceptance_receipt_json=canonical_json(receipt);row.updated_at=now();return
            if (any(receipt.get(key)!=value for key,value in previous.items())
                    or set(receipt)-set(previous)-{'sandbox_id','sandbox_handle'}):
                raise RuntimeError('job_cleanup_receipt_mismatch')
            handle=SandboxHandle.from_mapping(receipt.get('sandbox_handle') or {})
            if receipt!=self._resolved_allocation_receipt(previous,handle):
                raise RuntimeError('job_cleanup_receipt_mismatch')
            row.acceptance_receipt_json=canonical_json(receipt);row.updated_at=now()


    async def advance_job_cleanup(self, cleanup_id: str, owner: str, *,
                                  expected_phase: str, next_phase: str) -> None:
        async with self.connections.begin() as db:
            row=await db.get(JobCleanupRow,cleanup_id,with_for_update=True)
            if (not row or row.claim_owner!=owner or row.state!="running"
                    or row.phase!=expected_phase):
                raise RuntimeError("stale_job_cleanup_claim")
            row.phase=next_phase;row.updated_at=now()


    async def finish_job_cleanup(self, cleanup_id: str, owner: str, *,
                                 succeeded: bool, error: str = "") -> None:
        async with self.connections.begin() as db:
            row=await db.get(JobCleanupRow,cleanup_id,with_for_update=True)
            if not row or row.claim_owner!=owner or row.state!="running":
                raise RuntimeError("stale_job_cleanup_claim")
            row.state="completed" if succeeded else "pending"
            if succeeded:row.phase="completed"
            row.last_error="" if succeeded else error[:2000]
            row.claim_owner="";row.claim_expires_at=None;row.updated_at=now()


    async def renew_job_cleanup(self, cleanup_id: str, owner: str,
                                lease_seconds: int) -> bool:
        async with self.connections.begin() as db:
            result=await db.execute(update(JobCleanupRow).where(
                JobCleanupRow.id==cleanup_id,JobCleanupRow.claim_owner==owner,
                JobCleanupRow.state=="running").values(
                    claim_expires_at=now()+timedelta(seconds=lease_seconds),updated_at=now()))
            return bool(result.rowcount)


    async def get_job_cleanup(self, job_id: str) -> dict | None:
        async with self.connections() as db:
            rows=list((await db.execute(select(JobCleanupRow).where(
                JobCleanupRow.job_id==job_id).order_by(JobCleanupRow.generation))).scalars().all())
        row=next((value for value in reversed(rows) if value.state!="completed"),rows[-1] if rows else None)
        return ({"state":row.state,"phase":row.phase,"attempt_count":row.attempt_count,
            "last_error":row.last_error,"generation":row.generation,
            "completed_count":sum(value.state=="completed" for value in rows),
            "updated_at":utc_datetime(row.updated_at)} if row else None)

