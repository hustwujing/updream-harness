"""Capabilities persistence; transaction boundaries are preserved."""
from __future__ import annotations
from datetime import (datetime, timezone)
from sqlalchemy import (select)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sqlalchemy.dialects.postgresql import (insert as postgresql_insert)
from sqlalchemy.dialects.sqlite import (insert as sqlite_insert)
from sandbox_harness.common.contracts.domain import (CapabilityClaims, now)
from .models import (CapabilityLedgerRow, ModelTokenReservationRow, canonical_digest)


class CapabilitiesTransactions:
    async def _ensure_capability_locked(self, db: AsyncSession, claims: CapabilityClaims,
                                        *, parent: bool = False) -> CapabilityLedgerRow:
        timestamp = now(); digest = "" if parent else canonical_digest(claims.model_dump(mode="json"))
        nonce = claims.parent_nonce if parent else claims.nonce
        token_budget = claims.parent_token_budget if parent else claims.token_budget
        values = dict(nonce=nonce, claims_digest=digest,
            parent_nonce="" if parent else claims.parent_nonce,
            expires_at=claims.expires_at, revoked=False, max_requests=0 if parent else claims.max_requests,
            max_public_requests=0 if parent else claims.max_public_requests,
            request_count=0, public_request_count=0, token_budget=token_budget,
            token_reserved=0, token_consumed=0, created_at=timestamp, updated_at=timestamp)
        insert_statement = (postgresql_insert(CapabilityLedgerRow) if self.engine.dialect.name == "postgresql"
            else sqlite_insert(CapabilityLedgerRow)).values(**values).on_conflict_do_nothing(
                index_elements=[CapabilityLedgerRow.nonce])
        await db.execute(insert_statement)
        row = (await db.execute(select(CapabilityLedgerRow).where(
            CapabilityLedgerRow.nonce == nonce).with_for_update())).scalar_one()
        if row.revoked:
            raise ValueError("capability_revoked")
        if not parent:
            if row.claims_digest and row.claims_digest != digest:
                raise ValueError("capability_identity_conflict")
            if not row.claims_digest:
                row.claims_digest = digest; row.max_requests = claims.max_requests
                row.max_public_requests = claims.max_public_requests
                row.token_budget = claims.token_budget; row.expires_at = claims.expires_at
                row.parent_nonce = claims.parent_nonce
            elif (row.max_requests != claims.max_requests
                    or row.max_public_requests != claims.max_public_requests
                    or row.token_budget != claims.token_budget
                    or row.parent_nonce != claims.parent_nonce):
                raise ValueError("capability_identity_conflict")
        if int(datetime.now(timezone.utc).timestamp()) >= row.expires_at:
            raise ValueError("capability_expired")
        row.updated_at = timestamp
        return row


    async def authorize_capability(self, claims: CapabilityClaims, usage: str = "request") -> dict:
        if usage not in {"request", "public", "check"}:
            raise ValueError("capability_usage_invalid")
        async with self.connections.begin() as db:
            row = await self._ensure_capability_locked(db, claims)
            if claims.parent_nonce:
                await self._ensure_capability_locked(db, claims, parent=True)
            if usage == "request":
                if row.request_count >= row.max_requests:
                    raise ValueError("capability_request_limit")
                row.request_count += 1
            elif usage == "public":
                if row.public_request_count >= row.max_public_requests:
                    raise ValueError("capability_public_request_limit")
                row.public_request_count += 1
            return {"request_count":row.request_count,
                "public_request_count":row.public_request_count,
                "token_remaining":max(0,row.token_budget-row.token_reserved-row.token_consumed)}


    async def revoke_capability(self, nonce: str, expires_at: int) -> None:
        if not nonce or len(nonce) > 64:
            raise ValueError("capability_nonce_invalid")
        timestamp = now()
        values = dict(nonce=nonce, claims_digest="", parent_nonce="", expires_at=expires_at,
            revoked=True, max_requests=0, max_public_requests=0, request_count=0,
            public_request_count=0, token_budget=0, token_reserved=0, token_consumed=0,
            created_at=timestamp, updated_at=timestamp)
        async with self.connections.begin() as db:
            statement = (postgresql_insert(CapabilityLedgerRow) if self.engine.dialect.name == "postgresql"
                else sqlite_insert(CapabilityLedgerRow)).values(**values).on_conflict_do_nothing(
                index_elements=[CapabilityLedgerRow.nonce])
            await db.execute(statement)
            row = (await db.execute(select(CapabilityLedgerRow).where(
                CapabilityLedgerRow.nonce == nonce).with_for_update())).scalar_one()
            reservations = list((await db.scalars(select(ModelTokenReservationRow).where(
                ModelTokenReservationRow.nonce == nonce,
                ModelTokenReservationRow.state == "reserved").with_for_update())).all())
            released = sum(value.reserved for value in reservations)
            row.token_reserved = max(0, row.token_reserved - released)
            parent_releases: dict[str, int] = {}
            for reservation in reservations:
                if reservation.parent_nonce:
                    parent_releases[reservation.parent_nonce] = (
                        parent_releases.get(reservation.parent_nonce, 0) + reservation.reserved)
            for parent_nonce, amount in parent_releases.items():
                parent = await db.get(CapabilityLedgerRow, parent_nonce, with_for_update=True)
                if parent:
                    parent.token_reserved = max(0, parent.token_reserved - amount)
                    parent.updated_at = timestamp
            for reservation in reservations:
                reservation.state = "released"
                reservation.charged = 0
                reservation.updated_at = timestamp
            row.revoked = True; row.expires_at = max(row.expires_at, expires_at); row.updated_at = timestamp


    async def reserve_model_tokens(self, claims: CapabilityClaims, request_id: str,
                                   requested_output: int) -> dict:
        if not request_id or requested_output < 1:
            raise ValueError("model_token_reservation_invalid")
        async with self.connections.begin() as db:
            existing = await db.get(ModelTokenReservationRow, request_id, with_for_update=True)
            if existing:
                if existing.nonce != claims.nonce or existing.requested_output != requested_output:
                    raise ValueError("model_token_reservation_conflict")
                return {"reserved":existing.reserved,"remaining":0,"reused":True}
            row = await self._ensure_capability_locked(db, claims)
            parent = await self._ensure_capability_locked(db, claims, parent=True) if claims.parent_nonce else None
            available = row.token_budget - row.token_reserved - row.token_consumed
            if parent:
                available = min(available,
                    parent.token_budget - parent.token_reserved - parent.token_consumed)
            if requested_output > available or available < 1:
                raise ValueError("model_token_budget_exhausted")
            row.token_reserved += requested_output
            if parent: parent.token_reserved += requested_output
            timestamp = now()
            db.add(ModelTokenReservationRow(id=request_id, nonce=claims.nonce,
                parent_nonce=claims.parent_nonce, requested_output=requested_output,
                reserved=requested_output, charged=0, state="reserved",
                created_at=timestamp, updated_at=timestamp))
            return {"reserved":requested_output,"remaining":available-requested_output,"reused":False}


    async def finalize_model_tokens(self, request_id: str, usage_tokens: int | None) -> dict:
        async with self.connections.begin() as db:
            reservation = await db.get(ModelTokenReservationRow, request_id, with_for_update=True)
            if not reservation:
                raise KeyError("model_token_reservation_not_found")
            if reservation.state == "finalized":
                return {"charged":reservation.charged,"reused":True}
            row = await db.get(CapabilityLedgerRow, reservation.nonce, with_for_update=True)
            parent = (await db.get(CapabilityLedgerRow, reservation.parent_nonce,
                with_for_update=True)) if reservation.parent_nonce else None
            charge = (usage_tokens if isinstance(usage_tokens,int) and not isinstance(usage_tokens,bool)
                and 0 <= usage_tokens <= reservation.reserved else reservation.reserved)
            row.token_reserved -= reservation.reserved; row.token_consumed += charge; row.updated_at = now()
            if parent:
                parent.token_reserved -= reservation.reserved
                parent.token_consumed += charge; parent.updated_at = now()
            reservation.charged = charge; reservation.state = "finalized"; reservation.updated_at = now()
            return {"charged":charge,"reused":False}
