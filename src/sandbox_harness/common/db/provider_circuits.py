"""Persisted Provider/Profile circuit state for new create admission only."""
from __future__ import annotations

import re
from datetime import timedelta

from sqlalchemy import func, select

from sandbox_harness.common.contracts.domain import now
from .models import ProviderCircuitRow, canonical_digest, utc_datetime


class ProviderCircuitTransactions:
    @staticmethod
    def _circuit_key(value):
        if not isinstance(value,str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}",value):
            raise ValueError("provider_circuit_key_invalid")

    @staticmethod
    async def _circuit_lock(db,key):
        if db.bind.dialect.name=="postgresql":
            value=int(canonical_digest(["provider-circuit",key])[:16],16)
            if value>=2**63:value-=2**64
            await db.execute(select(func.pg_advisory_xact_lock(value)))

    async def allow_provider_create(self,key:str,owner:str,*,probe_seconds:int)->bool:
        self._circuit_key(key)
        if not owner or not 1<=probe_seconds<=300:raise ValueError("provider_circuit_probe_invalid")
        timestamp=now()
        async with self.transaction() as db:
            await self._circuit_lock(db,key)
            row=await db.get(ProviderCircuitRow,key,with_for_update=True)
            if row is None or row.state=="closed":return True
            if row.state=="open" and row.opened_until and utc_datetime(row.opened_until)>timestamp:
                return False
            if (row.state=="half_open" and row.probe_owner!=owner
                    and row.probe_expires_at and utc_datetime(row.probe_expires_at)>timestamp):
                return False
            row.state="half_open";row.probe_owner=owner
            row.probe_expires_at=timestamp+timedelta(seconds=probe_seconds)
            row.generation+=1;row.updated_at=timestamp
            return True

    async def record_provider_failure(self,key:str,*,threshold:int,open_seconds:int,
                                      owner:str="")->bool:
        self._circuit_key(key)
        if not 1<=threshold<=100 or not 1<=open_seconds<=3600:
            raise ValueError("provider_circuit_limits_invalid")
        timestamp=now()
        async with self.transaction() as db:
            await self._circuit_lock(db,key)
            row=await db.get(ProviderCircuitRow,key,with_for_update=True)
            if row is None:
                row=ProviderCircuitRow(id=key,state="closed",failure_count=0,
                    generation=0,updated_at=timestamp);db.add(row)
            if row.state=="half_open" and owner and row.probe_owner!=owner:
                return row.state=="open"
            row.failure_count+=1
            opened=row.state=="half_open" or row.failure_count>=threshold
            if opened:
                row.state="open";row.opened_until=timestamp+timedelta(seconds=open_seconds)
                row.probe_owner="";row.probe_expires_at=None
            row.updated_at=timestamp
            return opened

    async def record_provider_success(self,key:str,owner:str="")->bool:
        self._circuit_key(key);timestamp=now()
        async with self.transaction() as db:
            await self._circuit_lock(db,key)
            row=await db.get(ProviderCircuitRow,key,with_for_update=True)
            if row is None:return True
            if row.state=="half_open" and row.probe_owner and row.probe_owner!=owner:return False
            row.state="closed";row.failure_count=0;row.opened_until=None
            row.probe_owner="";row.probe_expires_at=None;row.updated_at=timestamp
            return True

    async def provider_circuit_status(self,key:str)->dict:
        self._circuit_key(key)
        async with self.connections() as db:row=await db.get(ProviderCircuitRow,key)
        return ({"state":row.state,"failure_count":row.failure_count,
            "generation":row.generation,"opened_until":utc_datetime(row.opened_until)
                if row.opened_until else None,"probe_owner":row.probe_owner}
            if row else {"state":"closed","failure_count":0,"generation":0,
                "opened_until":None,"probe_owner":""})
