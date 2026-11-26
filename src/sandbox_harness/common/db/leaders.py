"""Database-fenced leases for PlatformWorker singleton coordination loops."""
from __future__ import annotations

from datetime import timedelta

from sqlalchemy import select, update

from sandbox_harness.common.contracts.domain import now
from .models import PlatformLeaderRow, utc_datetime


class PlatformLeaderTransactions:
    @staticmethod
    def _valid_leader(identity: str, owner: str, lease_seconds: int) -> None:
        if (not identity or len(identity)>64 or not owner or len(owner)>160
                or not 3<=lease_seconds<=300):
            raise ValueError("platform_leader_lease_invalid")

    async def claim_platform_leader(self, identity: str, owner: str,
                                    lease_seconds: int) -> int | None:
        self._valid_leader(identity,owner,lease_seconds)
        timestamp=now()
        async with self.transaction() as db:
            row=await db.scalar(select(PlatformLeaderRow).where(
                PlatformLeaderRow.id==identity).with_for_update())
            if row is None:
                row=PlatformLeaderRow(id=identity,owner=owner,generation=1,
                    expires_at=timestamp+timedelta(seconds=lease_seconds),updated_at=timestamp)
                db.add(row);await db.flush();return row.generation
            if (row.owner!=owner and row.expires_at is not None
                    and utc_datetime(row.expires_at)>timestamp):
                return None
            if row.owner!=owner:
                row.generation+=1
            row.owner=owner
            row.expires_at=timestamp+timedelta(seconds=lease_seconds)
            row.updated_at=timestamp
            return row.generation

    async def renew_platform_leader(self, identity: str, owner: str, generation: int,
                                    lease_seconds: int) -> bool:
        self._valid_leader(identity,owner,lease_seconds)
        if generation<1:raise ValueError("platform_leader_generation_invalid")
        timestamp=now()
        async with self.connections.begin() as db:
            result=await db.execute(update(PlatformLeaderRow).where(
                PlatformLeaderRow.id==identity,PlatformLeaderRow.owner==owner,
                PlatformLeaderRow.generation==generation,
                PlatformLeaderRow.expires_at>timestamp).values(
                    expires_at=timestamp+timedelta(seconds=lease_seconds),updated_at=timestamp))
            return result.rowcount==1

    async def release_platform_leader(self, identity: str, owner: str, generation: int) -> bool:
        if not identity or not owner or generation<1:
            raise ValueError("platform_leader_lease_invalid")
        timestamp=now()
        async with self.connections.begin() as db:
            result=await db.execute(update(PlatformLeaderRow).where(
                PlatformLeaderRow.id==identity,PlatformLeaderRow.owner==owner,
                PlatformLeaderRow.generation==generation).values(
                    owner="",expires_at=timestamp,updated_at=timestamp))
            return result.rowcount==1

    async def platform_leader_status(self, identity: str) -> dict:
        async with self.connections() as db:
            row=await db.get(PlatformLeaderRow,identity)
        return ({"id":row.id,"owner":row.owner,"generation":row.generation,
            "expires_at":utc_datetime(row.expires_at) if row.expires_at else None,
            "active":bool(row.owner and row.expires_at and utc_datetime(row.expires_at)>now())}
            if row else {"id":identity,"owner":"","generation":0,"expires_at":None,"active":False})
