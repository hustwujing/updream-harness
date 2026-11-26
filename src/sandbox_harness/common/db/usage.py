"""Idempotent append-only usage ledger for analysis and quota inputs."""
from __future__ import annotations

import re

from sqlalchemy import func, select

from sandbox_harness.common.contracts.domain import now
from .models import UsageLedgerRow, canonical_digest


class UsageTransactions:
    async def _record_usage_locked(self,db,*,idempotency_key,owner_id,thread_id,
                                   turn_id,job_id,kind,amount,unit,source)->bool:
        values=(owner_id,thread_id,turn_id,job_id,kind,amount,unit,source)
        if db.bind.dialect.name=="postgresql":
            key=int(canonical_digest(["usage",idempotency_key])[:16],16)
            if key>=2**63:key-=2**64
            await db.execute(select(func.pg_advisory_xact_lock(key)))
        row=await db.get(UsageLedgerRow,idempotency_key,with_for_update=True)
        if row:
            current=(row.owner_id,row.thread_id,row.turn_id,row.job_id,row.kind,
                row.amount,row.unit,row.source)
            if current!=values:raise ValueError("usage_identity_conflict")
            return False
        db.add(UsageLedgerRow(id=idempotency_key,owner_id=owner_id,thread_id=thread_id,
            turn_id=turn_id,job_id=job_id,kind=kind,amount=amount,unit=unit,
            source=source,created_at=now()))
        return True

    async def record_usage(self,*,idempotency_key:str,owner_id:str,thread_id:str,
                           turn_id:str,job_id:str,kind:str,amount:int,unit:str,
                           source:str)->dict:
        if (not re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}",idempotency_key)
                or not owner_id or len(owner_id)>64
                or not re.fullmatch(r"[a-z][a-z0-9_]{0,39}",kind)
                or not re.fullmatch(r"[a-z][a-z0-9_]{0,23}",unit)
                or not re.fullmatch(r"[a-z][a-z0-9_-]{0,39}",source)
                or isinstance(amount,bool) or not isinstance(amount,int) or amount<0):
            raise ValueError("usage_record_invalid")
        async with self.transaction() as db:
            created=await self._record_usage_locked(db,idempotency_key=idempotency_key,
                owner_id=owner_id,thread_id=thread_id,turn_id=turn_id,job_id=job_id,
                kind=kind,amount=amount,unit=unit,source=source)
            await db.flush()
            return {"id":idempotency_key,"created":created,"amount":amount}

    async def usage_totals(self,owner_id:str)->list[dict]:
        async with self.connections() as db:
            rows=(await db.execute(select(UsageLedgerRow.kind,UsageLedgerRow.unit,
                func.sum(UsageLedgerRow.amount)).where(UsageLedgerRow.owner_id==owner_id)
                .group_by(UsageLedgerRow.kind,UsageLedgerRow.unit)
                .order_by(UsageLedgerRow.kind,UsageLedgerRow.unit))).all()
        return [{"kind":kind,"unit":unit,"amount":int(amount)} for kind,unit,amount in rows]
