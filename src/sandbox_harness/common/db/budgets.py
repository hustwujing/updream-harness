"""Budgets persistence; transaction boundaries are preserved."""
from __future__ import annotations
from sqlalchemy import (select)
from sandbox_harness.common.contracts.domain import (identifier, now)
from sandbox_harness.common.contracts.job_contracts import (BudgetAccountView, BudgetLedgerView, BudgetReservationView)
from .models import (BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, canonical_json)


class BudgetsTransactions:
    async def configure_budget(self, owner_id: str, thread_id: str, budget_key: str,
                               *, limit: int) -> BudgetAccountView:
        if limit < 0:
            raise ValueError("budget limit cannot be negative")
        identity = f"budget:{owner_id}:{thread_id}:{budget_key}"
        async with self.job_lock(identity):
            async with self.connections.begin() as db:
                row = (await db.execute(select(BudgetAccountRow).where(
                    BudgetAccountRow.owner_id == owner_id,
                    BudgetAccountRow.thread_id == thread_id,
                    BudgetAccountRow.budget_key == budget_key,
                ).with_for_update())).scalar_one_or_none()
                timestamp = now()
                if row:
                    if limit < row.consumed + row.reserved:
                        raise RuntimeError("budget_limit_below_committed_usage")
                    row.limit = limit
                    row.updated_at = timestamp
                else:
                    row = BudgetAccountRow(
                        id=identifier(), owner_id=owner_id, thread_id=thread_id,
                        budget_key=budget_key, limit=limit, reserved=0, consumed=0,
                        created_at=timestamp, updated_at=timestamp,
                    )
                    db.add(row)
                await db.flush()
                return self.budget_account_view(row)


    async def get_budget_account(self, account_id: str) -> BudgetAccountView | None:
        async with self.connections() as db:
            row = await db.get(BudgetAccountRow, account_id)
            return self.budget_account_view(row) if row else None


    async def reserve_budget(self, account_id: str, *, group_id: str,
                             authorization_snapshot_id: str, amount: int,
                             idempotency_key: str) -> BudgetReservationView:
        if amount < 1:
            raise ValueError("budget reservation amount must be positive")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                if not account:
                    raise KeyError("budget_account_not_found")
                existing = (await db.execute(select(BudgetReservationRow).where(
                    BudgetReservationRow.group_id == group_id,
                    BudgetReservationRow.budget_key == account.budget_key,
                ).with_for_update())).scalar_one_or_none()
                if existing:
                    ledger = (await db.execute(select(BudgetLedgerRow).where(
                        BudgetLedgerRow.reservation_id == existing.id,
                        BudgetLedgerRow.idempotency_key == idempotency_key,
                    ))).scalar_one_or_none()
                    if not ledger or existing.account_id != account_id or existing.reserved != amount:
                        raise RuntimeError("budget_reservation_idempotency_conflict")
                    return self.budget_reservation_view(existing)
                if account.limit - account.consumed - account.reserved < amount:
                    raise RuntimeError("budget_exhausted")
                timestamp = now()
                row = BudgetReservationRow(
                    id=identifier(), account_id=account.id, group_id=group_id,
                    authorization_snapshot_id=authorization_snapshot_id,
                    budget_key=account.budget_key, reserved=amount, consumed=0, released=0,
                    state="reserved", created_at=timestamp, updated_at=timestamp,
                )
                account.reserved += amount
                account.updated_at = timestamp
                db.add_all([row, BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=group_id, job_id="",
                    action="reserve", amount=amount, idempotency_key=idempotency_key,
                    detail_json="{}", created_at=timestamp,
                )])
                await db.flush()
                return self.budget_reservation_view(row)


    async def consume_budget(self, reservation_id: str, *, job_id: str, amount: int,
                             idempotency_key: str) -> BudgetReservationView:
        if amount < 1:
            raise ValueError("budget consumption amount must be positive")
        async with self.connections() as lookup:
            account_id = (await lookup.execute(select(BudgetReservationRow.account_id).where(
                BudgetReservationRow.id == reservation_id))).scalar_one_or_none()
        if not account_id:
            raise KeyError("budget_reservation_not_found")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                row = await db.get(BudgetReservationRow, reservation_id, with_for_update=True)
                existing = (await db.execute(select(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id == reservation_id,
                    BudgetLedgerRow.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if existing:
                    if existing.action != "consume" or existing.amount != amount or existing.job_id != job_id:
                        raise RuntimeError("budget_ledger_idempotency_conflict")
                    return self.budget_reservation_view(row)
                if row.consumed + row.released + amount > row.reserved:
                    raise RuntimeError("budget_reservation_exhausted")
                timestamp = now()
                row.consumed += amount
                row.state = "consumed" if row.consumed + row.released == row.reserved else "partial"
                row.updated_at = timestamp
                account.reserved -= amount
                account.consumed += amount
                account.updated_at = timestamp
                db.add(BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=row.group_id, job_id=job_id,
                    action="consume", amount=amount, idempotency_key=idempotency_key,
                    detail_json="{}", created_at=timestamp,
                ))
                await db.flush()
                return self.budget_reservation_view(row)


    async def release_budget(self, reservation_id: str, *,
                             idempotency_key: str) -> BudgetReservationView:
        async with self.connections() as lookup:
            account_id = (await lookup.execute(select(BudgetReservationRow.account_id).where(
                BudgetReservationRow.id == reservation_id))).scalar_one_or_none()
        if not account_id:
            raise KeyError("budget_reservation_not_found")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                row = await db.get(BudgetReservationRow, reservation_id, with_for_update=True)
                existing = (await db.execute(select(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id == reservation_id,
                    BudgetLedgerRow.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if existing:
                    if existing.action != "release":
                        raise RuntimeError("budget_ledger_idempotency_conflict")
                    return self.budget_reservation_view(row)
                amount = row.reserved - row.consumed - row.released
                timestamp = now()
                if amount==0:
                    row.state = "released" if row.consumed == 0 else "closed"
                    row.updated_at = timestamp
                    await db.flush()
                    return self.budget_reservation_view(row)
                row.released += amount
                row.state = "released" if row.consumed == 0 else "closed"
                row.updated_at = timestamp
                account.reserved -= amount
                account.updated_at = timestamp
                db.add(BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=row.group_id, job_id="",
                    action="release", amount=amount, idempotency_key=idempotency_key,
                    detail_json="{}", created_at=timestamp,
                ))
                await db.flush()
                return self.budget_reservation_view(row)


    async def adjust_budget(self, reservation_id: str, *, amount: int,
                            idempotency_key: str, detail: dict | None = None) -> BudgetReservationView:
        if amount == 0:
            raise ValueError("budget adjustment cannot be zero")
        async with self.connections() as lookup:
            account_id = (await lookup.execute(select(BudgetReservationRow.account_id).where(
                BudgetReservationRow.id == reservation_id))).scalar_one_or_none()
        if not account_id:
            raise KeyError("budget_reservation_not_found")
        async with self.job_lock(f"budget-account:{account_id}"):
            async with self.connections.begin() as db:
                account = await db.get(BudgetAccountRow, account_id, with_for_update=True)
                row = await db.get(BudgetReservationRow, reservation_id, with_for_update=True)
                existing = (await db.execute(select(BudgetLedgerRow).where(
                    BudgetLedgerRow.reservation_id == reservation_id,
                    BudgetLedgerRow.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if existing:
                    if existing.action != "adjust" or existing.amount != amount:
                        raise RuntimeError("budget_ledger_idempotency_conflict")
                    return self.budget_reservation_view(row)
                if amount > 0 and account.consumed + account.reserved + amount > account.limit:
                    raise RuntimeError("budget_exhausted")
                if amount < 0 and row.consumed < -amount:
                    raise RuntimeError("budget_adjustment_exceeds_consumption")
                row.consumed += amount
                account.consumed += amount
                row.updated_at = account.updated_at = now()
                db.add(BudgetLedgerRow(
                    id=identifier(), reservation_id=row.id, group_id=row.group_id, job_id="",
                    action="adjust", amount=amount, idempotency_key=idempotency_key,
                    detail_json=canonical_json(detail or {}), created_at=now(),
                ))
                await db.flush()
                return self.budget_reservation_view(row)


    async def list_budget_ledger(self, reservation_id: str) -> list[BudgetLedgerView]:
        async with self.connections() as db:
            rows = (await db.execute(select(BudgetLedgerRow).where(
                BudgetLedgerRow.reservation_id == reservation_id).order_by(
                BudgetLedgerRow.created_at, BudgetLedgerRow.id))).scalars().all()
            return [self.budget_ledger_view(row) for row in rows]


    async def get_budget_reservations_for_group(self, group_id: str) -> list[BudgetReservationView]:
        async with self.connections() as db:
            rows = (await db.execute(select(BudgetReservationRow).where(
                BudgetReservationRow.group_id == group_id).order_by(
                BudgetReservationRow.budget_key))).scalars().all()
            return [self.budget_reservation_view(row) for row in rows]

