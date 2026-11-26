"""Interactions persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from datetime import (timedelta)
from sqlalchemy import (or_, select, update)
from sandbox_harness.common.observability.observability import (observed)
from sandbox_harness.common.contracts.domain import (Interaction, ItemType, TurnState, identifier, now)
from .models import (InteractionRow, ItemRow, ThreadRow, TurnRow, utc_datetime)


class InteractionsTransactions:
    @staticmethod
    def interaction_view(row: InteractionRow) -> Interaction:
        return Interaction.model_validate({
            "id": row.id, "thread_id": row.thread_id, "turn_id": row.turn_id,
            "runtime_execution_id": row.runtime_execution_id,
            "runtime_interaction_id": row.runtime_interaction_id, "kind": row.kind,
            "status": row.status, "request": json.loads(row.request_json),
            "response": json.loads(row.response_json), "response_key": row.response_key,
            "created_at": row.created_at, "updated_at": row.updated_at,
        })


    async def upsert_interaction(self, thread_id: str, turn_id: str, execution_id: str,
                                 runtime_interaction_id: str, kind: str, request: dict) -> tuple[Interaction, bool]:
        timestamp = now()
        async with self.connections.begin() as db:
            row = (await db.execute(select(InteractionRow).where(
                InteractionRow.turn_id == turn_id,
                InteractionRow.runtime_interaction_id == runtime_interaction_id,
            ).with_for_update())).scalar_one_or_none()
            if row:
                return self.interaction_view(row), False
            row = InteractionRow(id=identifier(), thread_id=thread_id, turn_id=turn_id,
                runtime_execution_id=execution_id, runtime_interaction_id=runtime_interaction_id,
                kind=kind, status="pending", request_json=json.dumps(request, ensure_ascii=False),
                response_json="{}", response_key="", created_at=timestamp, updated_at=timestamp)
            db.add(row)
        return self.interaction_view(row), True


    async def list_interactions(self, thread_id: str, owner_id: str) -> list[Interaction]:
        async with self.connections() as db:
            rows = (await db.execute(select(InteractionRow).join(ThreadRow,
                ThreadRow.id == InteractionRow.thread_id).where(
                    InteractionRow.thread_id == thread_id, ThreadRow.owner_id == owner_id,
                ).order_by(InteractionRow.created_at))).scalars().all()
        return [self.interaction_view(row) for row in rows]


    @observed("db.interaction.claim")
    async def claim_interaction_response(self, interaction_id: str, owner_id: str,
                                         response_key: str, response: dict, *, claim_owner: str = "",
                                         lease_seconds: int = 30) -> tuple[Interaction, bool]:
        expired = False
        timestamp = now()
        async with self.connections.begin() as db:
            row = (await db.execute(select(InteractionRow).join(ThreadRow,
                ThreadRow.id == InteractionRow.thread_id).where(
                    InteractionRow.id == interaction_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("interaction_not_found")
            stored_response = json.loads(row.response_json)
            if row.response_key == response_key and stored_response and stored_response != response:
                raise RuntimeError("idempotency_conflict")
            if row.status == "responded" and stored_response == response:
                return self.interaction_view(row), False
            if row.status not in {"pending", "responding"}:
                raise RuntimeError("interaction_not_pending")
            turn = await db.get(TurnRow, row.turn_id, with_for_update=True)
            if (not turn or turn.runtime_execution_id != row.runtime_execution_id
                    or TurnState(turn.state) not in {TurnState.RUNNING, TurnState.AWAITING_USER}):
                row.status = "expired"; row.updated_at = timestamp
                expired = True
            else:
                if row.status == "responding":
                    if row.response_expires_at and utc_datetime(row.response_expires_at) > timestamp:
                        raise RuntimeError("interaction_response_in_progress")
                    # A lost receipt must not change the user's committed answer.
                    # A new browser key is allowed, a different answer is not.
                    if stored_response != response:
                        raise RuntimeError("interaction_response_conflict")
                row.status = "responding"; row.response_key = response_key
                row.response_json = json.dumps(response, ensure_ascii=False)
                row.response_owner = claim_owner
                row.response_expires_at = timestamp + timedelta(seconds=lease_seconds)
                row.updated_at = timestamp
        if expired:
            raise RuntimeError("interaction_expired")
        return self.interaction_view(row), True


    @observed("db.interaction.finish")
    async def finish_interaction_response(self, interaction_id: str, *, delivered: bool | None,
                                          claim_owner: str = "") -> Interaction:
        async with self.connections.begin() as db:
            row = await db.get(InteractionRow, interaction_id, with_for_update=True)
            if not row:
                raise KeyError("interaction_not_found")
            if row.status == "responding":
                if claim_owner and row.response_owner != claim_owner:
                    raise RuntimeError("stale_interaction_response_claim")
                row.status = "responded" if delivered else "responding" if delivered is None else "pending"
                if delivered is False:
                    row.response_key = ""; row.response_json = "{}"
                row.response_owner = ""; row.response_expires_at = None
                if delivered is None:
                    row.response_expires_at = now() + timedelta(seconds=5)
                row.updated_at = now()
        return self.interaction_view(row)


    async def claim_pending_interaction_responses(self, owner: str, *, limit: int = 20,
                                                   lease_seconds: int = 30) -> list[Interaction]:
        if not owner or not 1 <= limit <= 100 or not 1 <= lease_seconds <= 300:
            raise ValueError("interaction_claim_invalid")
        timestamp=now()
        async with self.connections.begin() as db:
            rows=list((await db.scalars(select(InteractionRow).join(TurnRow,
                TurnRow.id==InteractionRow.turn_id).where(
                    InteractionRow.status=="responding",
                    or_(InteractionRow.response_expires_at.is_(None),
                        InteractionRow.response_expires_at<=timestamp),
                    TurnRow.runtime_execution_id==InteractionRow.runtime_execution_id,
                    TurnRow.state.in_([TurnState.RUNNING.value,TurnState.AWAITING_USER.value]))
                .order_by(InteractionRow.updated_at,InteractionRow.id).limit(limit)
                .with_for_update(skip_locked=True))).all())
            for row in rows:
                row.response_owner=owner
                row.response_expires_at=timestamp+timedelta(seconds=lease_seconds)
                row.updated_at=timestamp
            return [self.interaction_view(row) for row in rows]


    async def resume_interaction_turn(self, turn_id: str, execution_id: str) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                TurnRow.runtime_execution_id == execution_id,
                TurnRow.state == TurnState.AWAITING_USER.value
            ).values(state=TurnState.RUNNING.value, updated_at=now()))
            return result.rowcount == 1


    async def finish_interaction_item(self, turn_id: str, interaction_id: str, response: dict) -> None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(ItemRow).where(
                ItemRow.turn_id == turn_id, ItemRow.type == ItemType.APPROVAL.value,
            ).order_by(ItemRow.sequence.desc()))).scalars().all()
            row = next((item for item in rows
                if json.loads(item.payload_json).get("interaction_id") == interaction_id), None)
            if row:
                payload = json.loads(row.payload_json); payload["response"] = response
                row.payload_json = json.dumps(payload, ensure_ascii=False)
                row.status = "completed"; row.updated_at = now()
