"""Events persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import re
from sqlalchemy import (func, select, text, update)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sandbox_harness.common.contracts.domain import (RuntimeEvent, Event, EventKind, Item, ItemType, TurnState, identifier, now)
from .models import (EventRow, InteractionRow, ItemRow, ThreadRow, TurnRow, utc_datetime)


class EventsTransactions:
    @staticmethod
    async def next_item_sequence(db: AsyncSession, thread_id: str) -> int:
        sequence = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
            .values(next_item_sequence=ThreadRow.next_item_sequence + 1)
            .returning(ThreadRow.next_item_sequence))).scalar_one_or_none()
        if sequence is None:
            raise KeyError("thread_not_found")
        return sequence


    async def persist_runtime_event(self, thread_id: str, turn_id: str, value: RuntimeEvent,
                                    execution_id: str, *, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> Event:
        """Commit transcript mutation and replay receipt together; no crash gap."""
        async with self.connections.begin() as db:
            await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
                .values(updated_at=ThreadRow.updated_at))
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.thread_id != thread_id:
                raise KeyError("turn_not_found")
            if lease_owner is not None:
                self.require_lease_row(turn, lease_owner, recovery_generation)
            if execution_id and value.runtime_sequence:
                previous = (await db.execute(select(EventRow).where(EventRow.turn_id == turn_id,
                    EventRow.source_execution_id == execution_id,
                    EventRow.source_sequence == value.runtime_sequence))).scalar_one_or_none()
                if previous:
                    return Event(turn_id=turn_id, sequence=previous.sequence, kind=EventKind(previous.kind),
                        item_id=previous.item_id, payload=json.loads(previous.payload_json), created_at=previous.created_at)
            payload = dict(value.payload)
            timestamp = now()
            if value.kind in {EventKind.TOOL_STARTED, EventKind.STEP_STARTED} and not payload.get("model_request_id"):
                await db.execute(update(ItemRow).where(ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                    .values(status="completed", updated_at=timestamp))
            item = None
            if value.kind != EventKind.TOOL_OUTPUT:
                item_type = {EventKind.TEXT_DELTA: ItemType.AGENT_MESSAGE,
                    EventKind.TOOL_STARTED: ItemType.TOOL, EventKind.TOOL_FINISHED: ItemType.TOOL,
                    EventKind.STEP_STARTED: ItemType.PLAN, EventKind.STEP_PROGRESS: ItemType.PLAN,
                    EventKind.STEP_FINISHED: ItemType.PLAN, EventKind.WARNING: ItemType.WARNING,
                    EventKind.ERROR: ItemType.ERROR, EventKind.WAITING_USER: ItemType.APPROVAL,
                    EventKind.ARTIFACT_CREATED: ItemType.ARTIFACT}.get(value.kind, ItemType.REASONING)
                status = "failed" if payload.get("is_error") or value.kind == EventKind.ERROR else (
                    "completed" if value.kind in {EventKind.TOOL_FINISHED, EventKind.STEP_FINISHED, EventKind.WARNING} else "in_progress")
                summary = str(payload.get("name") or payload.get("text") or value.kind.value)
                if value.kind == EventKind.TEXT_DELTA:
                    item = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                        .order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
                    if item:
                        item.summary += str(payload.get("text", "")); item.updated_at = timestamp
                elif value.kind == EventKind.TOOL_FINISHED and payload.get("tool_use_id"):
                    candidates = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.TOOL.value).order_by(ItemRow.sequence.desc()))).scalars().all()
                    item = next((row for row in candidates if
                        json.loads(row.payload_json).get("tool_use_id") == payload["tool_use_id"] and
                        json.loads(row.payload_json).get("_execution_id", execution_id) == execution_id), None)
                    if item:
                        item.status = status
                        if payload.get("name") not in (None, "", "tool"):
                            item.summary = summary
                        item.payload_json = json.dumps({**json.loads(item.payload_json), **payload, "name": item.summary}, ensure_ascii=False)
                        item.updated_at = timestamp
                elif value.kind == EventKind.STEP_FINISHED and payload.get("background_job_id"):
                    candidates = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.PLAN.value).order_by(ItemRow.sequence.desc()))).scalars().all()
                    item = next((row for row in candidates if
                        json.loads(row.payload_json).get("background_job_id") == payload["background_job_id"] and
                        json.loads(row.payload_json).get("_execution_id", execution_id) == execution_id), None)
                    if item:
                        item.status = status
                        item.summary = summary
                        item.payload_json = json.dumps({**json.loads(item.payload_json), **payload}, ensure_ascii=False)
                        item.updated_at = timestamp
                elif value.kind == EventKind.STEP_FINISHED and payload.get("interaction_applied"):
                    native_id=str(payload.get("interaction_id") or "")
                    interaction=(await db.execute(select(InteractionRow).where(
                        InteractionRow.turn_id==turn_id,
                        InteractionRow.runtime_execution_id==execution_id,
                        InteractionRow.runtime_interaction_id==native_id,
                    ).with_for_update())).scalar_one_or_none()
                    if interaction and interaction.status=="responding":
                        interaction.status="responded"
                        interaction.response_owner=""
                        interaction.response_expires_at=None
                        interaction.updated_at=timestamp
                        payload["interaction_id"]=interaction.id
                        approvals=(await db.execute(select(ItemRow).where(
                            ItemRow.turn_id==turn_id,
                            ItemRow.type==ItemType.APPROVAL.value))).scalars().all()
                        item=next((row for row in approvals if
                            json.loads(row.payload_json).get("interaction_id")==interaction.id),None)
                        if item:
                            item_payload=json.loads(item.payload_json)
                            item_payload["response"]=json.loads(interaction.response_json)
                            item.payload_json=json.dumps(item_payload,ensure_ascii=False)
                            item.status="completed";item.updated_at=timestamp
                        if turn.state==TurnState.AWAITING_USER.value:
                            turn.state=TurnState.RUNNING.value
                elif value.kind == EventKind.WAITING_USER:
                    native_id = str(payload.get("interaction_id") or "")
                    kind = payload.get("interaction_kind", "approval")
                    if not native_id or kind not in {"approval", "question"}:
                        raise ValueError("Invalid Runtime interaction")
                    interaction = (await db.execute(select(InteractionRow).where(InteractionRow.turn_id == turn_id,
                        InteractionRow.runtime_interaction_id == native_id))).scalar_one_or_none()
                    if interaction is None:
                        interaction = InteractionRow(id=identifier(), thread_id=thread_id, turn_id=turn_id,
                            runtime_execution_id=execution_id, runtime_interaction_id=native_id, kind=kind,
                            status="pending", request_json=json.dumps(payload), response_json="{}", response_key="",
                            created_at=timestamp, updated_at=timestamp)
                        db.add(interaction)
                    elif interaction.runtime_execution_id != execution_id:
                        raise RuntimeError("interaction_execution_mismatch")
                    payload["interaction_id"] = interaction.id
                    summary = str(payload.get("prompt") or "需要用户确认")
                    approvals = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.APPROVAL.value))).scalars().all()
                    item = next((row for row in approvals if json.loads(row.payload_json).get("interaction_id") == interaction.id), None)
                    if interaction.status == "pending":
                        turn.state = TurnState.AWAITING_USER.value
                if item is None:
                    sequence = await self.next_item_sequence(db, thread_id)
                    item = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=sequence,
                        type=item_type.value, status=status, summary=summary,
                        payload_json=json.dumps({**payload, "_execution_id": execution_id}, ensure_ascii=False),
                        created_at=timestamp, updated_at=timestamp)
                    db.add(item)
            if payload.get("input_completed"):
                await db.execute(update(ItemRow).where(ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                    .values(status="interrupted" if payload.get("is_error") else "completed", updated_at=timestamp))
            last = (await db.execute(select(func.max(EventRow.sequence)).where(EventRow.turn_id == turn_id))).scalar() or 0
            event = Event(turn_id=turn_id, sequence=last + 1, kind=value.kind, payload=payload,
                item_id=item.id if item else "", created_at=timestamp)
            db.add(EventRow(turn_id=turn_id, sequence=event.sequence, kind=value.kind.value,
                item_id=event.item_id, payload_json=json.dumps(payload, ensure_ascii=False),
                source_execution_id=execution_id, source_sequence=value.runtime_sequence, created_at=timestamp))
            if value.runtime_sequence:
                turn.runtime_sequence = max(turn.runtime_sequence, value.runtime_sequence)
        if self.events:
            await self.events.publish(event)
        return event


    async def update_model_activity(self, item_id: str, *, phase: str, status: str, payload: dict) -> None:
        async with self.connections.begin() as db:
            row = await db.get(ItemRow, item_id, with_for_update=True)
            if not row or row.status not in {"in_progress", "interrupted"}:
                return
            model = payload["model"]
            row.summary = {"streaming": f"{model} 正在输出", "failed": f"{model} 请求失败",
                           "completed": f"{model} 响应完成"}.get(phase, f"等待 {model} 响应")
            row.payload_json = json.dumps({**json.loads(row.payload_json), **payload, "phase": phase}, ensure_ascii=False)
            row.status = status; row.updated_at = now()
            turn_id = row.turn_id
        await self.append_event(turn_id, EventKind.STEP_PROGRESS if status == "in_progress" else EventKind.STEP_FINISHED,
            {"model_request_id": payload["model_request_id"], "phase": phase}, item_id=item_id)


    async def latest_model_activity_error(self,turn_id:str)->str:
        async with self.connections() as db:
            row=(await db.execute(select(ItemRow).join(EventRow,
                (EventRow.item_id==ItemRow.id)&(EventRow.turn_id==ItemRow.turn_id)).where(
                ItemRow.turn_id==turn_id,ItemRow.type==ItemType.PLAN.value,
                EventRow.source_execution_id.startswith('gateway:')
                ).order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
        if row is None or row.status!='failed':return ''
        try:value=json.loads(row.payload_json or '{}')
        except (TypeError,ValueError):return ''
        code=value.get('error_code') if isinstance(value,dict) and value.get('model_request_id') else ''
        if isinstance(code,str) and re.fullmatch(r'[a-z][a-z0-9_.-]{0,159}',code):return code
        return ''


    async def runtime_event_recorded(self, turn_id: str, execution_id: str, source_sequence: int) -> bool:
        if not execution_id or source_sequence <= 0:
            return False
        async with self.connections() as db:
            row = (await db.execute(select(EventRow.sequence).where(
                EventRow.turn_id == turn_id,
                EventRow.source_execution_id == execution_id,
                EventRow.source_sequence == source_sequence,
            ).limit(1))).scalar_one_or_none()
        return row is not None


    async def append_event(self, turn_id: str, kind: EventKind, payload: dict, *, item_id: str = "",
                           source_execution_id: str = "", source_sequence: int = 0,
                           lease_owner: str | None = None, recovery_generation: int | None = None) -> Event:
        async with self.event_lock(turn_id):
            return await self._append_event_locked(turn_id, kind, payload, item_id=item_id,
                source_execution_id=source_execution_id, source_sequence=source_sequence,
                lease_owner=lease_owner, recovery_generation=recovery_generation)


    async def _append_event_locked(self, turn_id: str, kind: EventKind, payload: dict, *, item_id: str,
                                   source_execution_id: str, source_sequence: int,
                                   lease_owner: str | None, recovery_generation: int | None) -> Event:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if lease_owner is not None:
                self.require_lease_row(turn, lease_owner, recovery_generation)
            if source_execution_id and source_sequence:
                existing = (await db.execute(select(EventRow).where(
                    EventRow.turn_id == turn_id,
                    EventRow.source_execution_id == source_execution_id,
                    EventRow.source_sequence == source_sequence,
                ).limit(1))).scalar_one_or_none()
                if existing:
                    return Event(turn_id=existing.turn_id, sequence=existing.sequence,
                        kind=EventKind(existing.kind), item_id=existing.item_id,
                        payload=json.loads(existing.payload_json), created_at=existing.created_at)
            last = (await db.execute(select(EventRow.sequence).where(EventRow.turn_id == turn_id).order_by(EventRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            event = Event(turn_id=turn_id, sequence=last + 1, kind=kind, item_id=item_id, payload=payload)
            db.add(EventRow(turn_id=turn_id, sequence=event.sequence, kind=kind.value, item_id=item_id,
                payload_json=json.dumps(payload, ensure_ascii=False), source_execution_id=source_execution_id,
                source_sequence=source_sequence, created_at=event.created_at))
        if self.events:
            await self.events.publish(event)
        return event


    async def events_after(self, turn_id: str, sequence: int, limit: int | None = None) -> list[Event]:
        async with self.connections() as db:
            query = select(EventRow).where(EventRow.turn_id == turn_id,
                EventRow.sequence > sequence).order_by(EventRow.sequence)
            if limit is not None:
                query = query.limit(limit)
            rows = (await db.execute(query)).scalars().all()
        return [Event(turn_id=row.turn_id, sequence=row.sequence, kind=EventKind(row.kind), item_id=row.item_id, payload=json.loads(row.payload_json), created_at=row.created_at) for row in rows]


    async def append_item(self, thread_id: str, turn_id: str, item_type: ItemType, *, status: str,
                          summary: str = "", payload: dict | None = None,
                          lease_owner: str | None = None, recovery_generation: int | None = None) -> Item:
        timestamp = now()
        async with self.connections.begin() as db:
            await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
                .values(updated_at=ThreadRow.updated_at))
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if lease_owner is not None:
                self.require_lease_row(turn, lease_owner, recovery_generation)
            sequence = await self.next_item_sequence(db, thread_id)
            row = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=sequence,
                type=item_type.value, status=status, summary=summary, payload_json=json.dumps(payload or {}, ensure_ascii=False),
                created_at=timestamp, updated_at=timestamp)
            db.add(row)
        return Item(id=row.id, thread_id=thread_id, turn_id=turn_id, sequence=row.sequence, type=item_type,
            status=status, summary=summary, payload=payload or {}, created_at=timestamp, updated_at=timestamp)


    async def append_agent_delta(self, thread_id: str, turn_id: str, text: str) -> Item:
        async with self.connections.begin() as db:
            await db.get(ThreadRow, thread_id, with_for_update=True)
            row = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.AGENT_MESSAGE.value, ItemRow.status == "in_progress")
                .order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
            if not row:
                sequence = await self.next_item_sequence(db, thread_id)
                row = ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id, sequence=sequence,
                    type=ItemType.AGENT_MESSAGE.value, status="in_progress", summary="", payload_json="{}", created_at=now(), updated_at=now())
                db.add(row)
            row.summary += text
            row.updated_at = now()
        return Item(id=row.id, thread_id=thread_id, turn_id=turn_id, sequence=row.sequence, type=ItemType.AGENT_MESSAGE,
            status=row.status, summary=row.summary, payload=json.loads(row.payload_json), created_at=row.created_at, updated_at=row.updated_at)


    async def complete_agent_item(self, turn_id: str, status: str, *, lease_owner: str | None = None,
                                  recovery_generation: int | None = None) -> None:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if lease_owner is not None:
                self.require_lease_row(turn, lease_owner, recovery_generation)
            row = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id, ItemRow.type == ItemType.AGENT_MESSAGE.value).order_by(ItemRow.sequence.desc()).limit(1))).scalar_one_or_none()
            if row:
                row.status = status
                row.updated_at = now()


    async def finish_tool_item(self, turn_id: str, tool_use_id: str, payload: dict) -> Item | None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(ItemRow).where(
                ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.TOOL.value,
            ).order_by(ItemRow.sequence.desc()))).scalars().all()
            row = next((candidate for candidate in rows
                        if json.loads(candidate.payload_json).get("tool_use_id") == tool_use_id), None)
            if not row:
                return None
            row.status = "failed" if payload.get("is_error") else "completed"
            name = str(payload.get("name") or "")
            row.summary = row.summary if name in {"", "tool"} else name
            previous = json.loads(row.payload_json)
            row.payload_json = json.dumps({**previous, **payload, "name": row.summary}, ensure_ascii=False)
            row.updated_at = now()
        return Item(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id, sequence=row.sequence,
            type=ItemType.TOOL, status=row.status, summary=row.summary, payload=json.loads(row.payload_json),
            created_at=row.created_at, updated_at=row.updated_at)


    async def items_after(self, thread_id: str, sequence: int = 0, limit: int = 200, owner_id: str | None = None) -> list[Item]:
        async with self.connections() as db:
            query = select(ItemRow).join(ThreadRow, ThreadRow.id == ItemRow.thread_id).where(
                ItemRow.thread_id == thread_id, ItemRow.sequence > sequence)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(ItemRow.sequence).limit(limit))).scalars().all()
        return [Item(id=row.id, thread_id=row.thread_id, turn_id=row.turn_id, sequence=row.sequence,
            type=ItemType(row.type), status=row.status, summary=row.summary, payload=json.loads(row.payload_json),
            created_at=row.created_at, updated_at=row.updated_at) for row in rows]


    @staticmethod
    def require_lease_row(row: TurnRow | None, owner: str,
                          generation: int | None) -> None:
        if (not row or row.lease_owner != owner or row.recovery_generation != generation
                or not row.lease_expires_at or utc_datetime(row.lease_expires_at) <= now()):
            raise RuntimeError("stale_turn_lease")


    @staticmethod
    async def require_lease(db, turn_id: str, owner: str, generation: int) -> None:
        owned = (await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
            TurnRow.lease_owner == owner, TurnRow.recovery_generation == generation,
            TurnRow.lease_expires_at > now()).values(updated_at=TurnRow.updated_at)
            .returning(TurnRow.id))).scalar_one_or_none()
        if owned is None:
            raise RuntimeError("stale_turn_lease")

