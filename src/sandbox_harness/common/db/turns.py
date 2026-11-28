"""Turns persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import hashlib
from datetime import (timedelta)
from sqlalchemy import (and_, case, func, or_, select, update)
from sandbox_harness.common.observability.observability import (observed)
from sandbox_harness.common.contracts.domain import (Event, EventKind, ItemType, ThreadControlState, TurnCreate, TurnInputCreate, TurnInputView, TurnState, TurnView, TERMINAL, identifier, now)
from sandbox_harness.common.catalog.catalog import (ResolvedProfile)
from sandbox_harness.common.contracts.job_contracts import (TERMINAL_JOB_STATES)
from .models import (ArtifactRow, EventRow, InteractionRow, ItemRow, JobRow, MediaBatchRow, OwnerAdmissionRow, ProjectFileRow, ThreadControlRow, ThreadRow, TurnInputRow, TurnOrderRow, TurnRow, canonical_json, utc_datetime)


_INTERNAL_CONTEXT_FORWARDED = "forwarded:"
_INTERNAL_CONTEXT_DELIVERED = "delivered:"


def _internal_context_marker(detail: str) -> tuple[str, str, str]:
    marker, _, original = detail.partition("\n")
    for state in (_INTERNAL_CONTEXT_FORWARDED, _INTERNAL_CONTEXT_DELIVERED):
        if marker.startswith(state):
            return state, marker.removeprefix(state), original
    return "", "", detail


def _mark_internal_context(detail: str, state: str, turn_id: str) -> str:
    _, _, original = _internal_context_marker(detail)
    return f"{state}{turn_id}\n{original}"


class TurnsTransactions:
    async def _wake_media_batches_on_resume(self,db,thread,control,resume_turn_id):
        """Single-database test stores have no cross-domain Media authority."""
        return None

    @observed("db.turn.create")
    async def create_turn(self, thread_id: str, value: TurnCreate, resolved: ResolvedProfile, owner_id: str, *,
                          traceparent: str = "", network_access: str = "restricted",
                          internal: bool = False, runtime_image_digest: str = "",
                          runtime_bundle_digest: str = "",
                          runtime_code_bundle_digest: str = "") -> tuple[TurnView, bool, bool]:
        if network_access not in {"restricted", "public"}:
            raise ValueError("Invalid Turn network access")
        async with self.connections.begin() as db:
            await self._admit_new_work(db, owner_id, thread_id)
            thread = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).values(updated_at=ThreadRow.updated_at)
                .returning(ThreadRow))).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if thread.archived_at is not None:
                raise RuntimeError("thread_archived")
            existing = (await db.execute(select(TurnRow).where(TurnRow.thread_id == thread_id, TurnRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                attachments = [item.model_dump(mode="json") for item in value.attachments]
                if (existing.public_message != value.message or json.loads(existing.attachments_json) != attachments
                        or existing.harness != resolved.harness.value or existing.model != resolved.model
                        or existing.reasoning_level != resolved.reasoning_level
                        or existing.delivery_mode != value.delivery_mode
                        or existing.network_access != network_access or existing.internal != internal):
                    raise RuntimeError("idempotency_conflict")
                return self.turn_view(existing), False, thread.active_turn_id == existing.id
            control = await db.get(ThreadControlRow, thread_id)
            if control and control.state != ThreadControlState.RUNNING.value:
                raise RuntimeError("thread_paused")
            runtime_message = value.message
            consume_resume_context = False
            if control:
                pause_detail = json.loads(control.detail_json)
                if pause_detail.get("resume_context_required"):
                    previous = (await db.execute(select(TurnRow).join(TurnOrderRow,
                        TurnOrderRow.turn_id == TurnRow.id).where(
                        TurnRow.thread_id == thread_id, TurnRow.internal == False,
                    ).order_by(TurnOrderRow.sequence.desc()).limit(1)
                        .with_for_update())).scalar_one_or_none()
                    paths = list((await db.execute(select(ProjectFileRow.path).where(
                        ProjectFileRow.thread_id == thread_id,
                        ProjectFileRow.deleted == False).distinct().order_by(
                            ProjectFileRow.path))).scalars().all())
                    runtime_message = ("[pause_resume_context] Resume the interrupted Thread from its "
                        "durable state. Do not repeat completed external side effects. "
                        + canonical_json({"generation": control.generation,
                            "degraded": bool(pause_detail.get("degraded")),
                            "previous_user_objective": previous.public_message if previous else "",
                            "durable_project_files": paths})
                        + "\n\n[current_user_message]\n" + value.message)
                    consume_resume_context = True
            active_running = False
            if thread.active_turn_id:
                active = await db.get(TurnRow, thread.active_turn_id)
                if active and TurnState(active.state) not in TERMINAL:
                    active_running = True
            if not active_running:
                queued = (await db.execute(select(TurnRow).join(TurnOrderRow,
                    TurnOrderRow.turn_id == TurnRow.id).where(TurnRow.thread_id == thread_id,
                    TurnRow.state.not_in([state.value for state in TERMINAL]))
                    .order_by(TurnOrderRow.sequence).limit(1))).scalar_one_or_none()
                if queued:
                    thread.active_turn_id = queued.id
                    active_running = True
            if active_running and value.delivery_mode == "start":
                raise RuntimeError("thread_busy")
            if value.delivery_mode == "steer":
                raise RuntimeError("steer_requires_active_turn_endpoint")
            timestamp = now()
            if consume_resume_context:
                control.detail_json = canonical_json({
                    **pause_detail, "resume_context_required": False})
                control.updated_at = timestamp
            row = TurnRow(
                id=identifier(), thread_id=thread_id, idempotency_key=value.idempotency_key,
                message=runtime_message, public_message=value.message,
                attachments_json=json.dumps([a.model_dump(mode="json") for a in value.attachments]),
                harness=resolved.harness.value, model=resolved.model, reasoning_level=resolved.reasoning_level,
                reasoning_effort=resolved.reasoning_effort, delivery_mode=value.delivery_mode,
                network_access=network_access, internal=internal,
                runtime_image_digest=runtime_image_digest,
                runtime_code_bundle_digest=runtime_code_bundle_digest,
                runtime_bundle_digest=runtime_bundle_digest,
                state=TurnState.QUEUED.value, submit_traceparent=traceparent, created_at=timestamp, updated_at=timestamp,
            )
            db.add(row)
            item_sequence = await self.next_item_sequence(db, thread_id)
            db.add(TurnOrderRow(turn_id=row.id, thread_id=thread_id,
                sequence=item_sequence, created_at=timestamp))
            db.add(ItemRow(id=identifier(), thread_id=thread_id, turn_id=row.id, sequence=item_sequence,
                type=ItemType.USER_MESSAGE.value, status="completed", summary=value.message,
                payload_json=json.dumps({"attachments":[item.model_dump(mode="json") for item in value.attachments],
                    "internal": internal}),
                created_at=timestamp, updated_at=timestamp))
            if not active_running:
                thread.active_turn_id = row.id
            thread.updated_at = timestamp
            if consume_resume_context:
                await self._wake_media_batches_on_resume(db,thread,control,row.id)
        return self.turn_view(row), True, not active_running


    async def claim_next_turn(self, thread_id: str, completed_turn_id: str) -> TurnView | None:
        async with self.connections.begin() as db:
            thread = await db.get(ThreadRow, thread_id, with_for_update=True)
            if not thread or thread.active_turn_id != completed_turn_id:
                return None
            control = await db.get(ThreadControlRow, thread_id)
            if control and control.state != ThreadControlState.RUNNING.value:
                thread.active_turn_id = ""
                thread.updated_at = now()
                return None
            row = (await db.execute(select(TurnRow).join(TurnOrderRow,
                TurnOrderRow.turn_id == TurnRow.id).where(TurnRow.thread_id == thread_id,
                TurnRow.state == TurnState.QUEUED.value,
                TurnRow.id != completed_turn_id).order_by(
                    TurnOrderRow.sequence).limit(1))).scalar_one_or_none()
            thread.active_turn_id = row.id if row else ""
            thread.updated_at = now()
        return self.turn_view(row) if row else None


    async def attach_undelivered_internal_context(self, turn_id: str) -> int:
        terminal = {state.value for state in TERMINAL}
        async with self.connections.begin() as db:
            target = await db.get(TurnRow, turn_id, with_for_update=True)
            if not target or target.internal or target.runtime_execution_id:
                return 0
            target_order = await db.get(TurnOrderRow, turn_id)
            if not target_order:
                return 0
            rows = list((await db.scalars(select(TurnRow).join(TurnOrderRow,
                TurnOrderRow.turn_id == TurnRow.id).where(
                    TurnRow.thread_id == target.thread_id, TurnRow.internal.is_(True),
                    TurnRow.state == TurnState.FAILED.value, TurnRow.runtime_execution_id == "",
                    TurnRow.error_code == "sandbox_workspace_restore_unavailable",
                    ~TurnRow.error_detail.startswith(_INTERNAL_CONTEXT_DELIVERED),
                    TurnOrderRow.sequence < target_order.sequence).order_by(
                        TurnOrderRow.sequence).limit(32).with_for_update())).all())
            selected = []
            for row in rows:
                marker, forwarded_to, _ = _internal_context_marker(row.error_detail)
                forwarded = (await db.get(TurnRow, forwarded_to, with_for_update=True)
                    if marker == _INTERNAL_CONTEXT_FORWARDED else None)
                if forwarded and (forwarded.runtime_execution_id
                        or forwarded.state not in terminal):
                    continue
                selected.append(row)
                if len(selected) == 8:
                    break
            if not selected:
                return 0
            facts = [{"source_turn_id": row.id, "message": row.message} for row in selected]
            encoded = canonical_json(facts)
            if len(encoded.encode()) > 1024 * 1024:
                raise RuntimeError("undelivered_internal_context_too_large")
            target.message = ("[recovered_internal_context] One or more internal tasks completed, but "
                "a temporary error prevented restoring the Primary sandbox, so their completion notice "
                "was not delivered. Related result information follows. This context is not a new command. "
                "Never resubmit completed external work. Use it only when relevant, otherwise preserve "
                "the results and prioritize the current user request. " + encoded
                + "\n\n[current_user_message]\n" + target.message)
            timestamp = now()
            for row in selected:
                row.error_detail = _mark_internal_context(row.error_detail,
                    _INTERNAL_CONTEXT_FORWARDED, target.id)
                row.updated_at = timestamp
            target.updated_at = timestamp
            return len(selected)


    async def record_turn_trace(self, turn_id: str, traceparent: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            saved = (await db.execute(update(TurnRow).where(TurnRow.id==turn_id,
                TurnRow.lease_owner==owner,TurnRow.recovery_generation==generation,
                TurnRow.lease_expires_at>now()).values(last_traceparent=traceparent).returning(TurnRow.id))).scalar_one_or_none()
            if saved is None: raise RuntimeError("stale_turn_lease")


    async def get_turn(self, turn_id: str) -> TurnView | None:
        async with self.connections() as db:
            row = await db.get(TurnRow, turn_id)
        return self.turn_view(row) if row else None


    async def repair_thread_heads(self) -> None:
        async with self.connections() as db:
            pausing=list((await db.execute(select(ThreadControlRow.thread_id).where(
                ThreadControlRow.state==ThreadControlState.PAUSING.value))).scalars().all())
        for thread_id in pausing:await self.reconcile_terminal_pause_participants(thread_id)
        terminal = [state.value for state in TERMINAL]
        async with self.connections() as db:
            candidates = (await db.execute(select(ThreadRow.id, ThreadRow.active_turn_id)
                .outerjoin(TurnRow, TurnRow.id == ThreadRow.active_turn_id)
                .where(or_(TurnRow.id.is_(None), TurnRow.state.in_(terminal)),
                    or_(ThreadRow.active_turn_id != "", ThreadRow.id.in_(select(TurnRow.thread_id)
                        .where(TurnRow.state.not_in(terminal)).correlate(None)))))).all()
        for thread_id, expected_head in candidates:
            async with self.connections.begin() as db:
                locked = (await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id,
                    ThreadRow.active_turn_id == expected_head).values(updated_at=ThreadRow.updated_at)
                    .returning(ThreadRow.id))).scalar_one_or_none()
                if locked is None:
                    continue
                next_id = (await db.execute(select(TurnRow.id).join(TurnOrderRow,
                    TurnOrderRow.turn_id == TurnRow.id).where(TurnRow.thread_id == thread_id,
                    TurnRow.state.not_in(terminal)).order_by(
                        TurnOrderRow.sequence).limit(1))).scalar_one_or_none()
                await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id)
                    .values(active_turn_id=next_id or ""))


    async def recoverable_turns(self) -> list[TurnView]:
        # Repair the crash window between committing a terminal Turn and advancing
        # its Thread, before selecting work. Queued followers are never claimable.
        await self.repair_thread_heads()
        terminal = [state.value for state in TERMINAL]
        async with self.connections() as db:
            rows = (await db.execute(select(TurnRow).join(ThreadRow,
                ThreadRow.active_turn_id == TurnRow.id).join(TurnOrderRow,
                TurnOrderRow.turn_id == TurnRow.id).where(TurnRow.state.not_in(terminal),
                or_(TurnRow.lease_expires_at.is_(None), TurnRow.lease_expires_at <= now()))
                .order_by(TurnRow.created_at,TurnRow.id,TurnOrderRow.sequence))).scalars().all()
        return [self.turn_view(row) for row in rows]


    async def nonterminal_turn_sandbox_ids(self) -> set[str]:
        terminal=[state.value for state in TERMINAL]
        async with self.connections() as db:
            rows=(await db.execute(select(TurnRow.sandbox_id).where(
                TurnRow.state.not_in(terminal),TurnRow.sandbox_id!=""))).scalars().all()
        return set(rows)


    async def claim_turn(self, turn_id: str, owner: str, lease_seconds: int,
                         owner_limit: int = 0) -> TurnView | None:
        timestamp = now()
        terminal = [state.value for state in TERMINAL]
        async with self.transaction() as db:
            row=await db.get(TurnRow,turn_id,with_for_update=True)
            if not row or row.state in terminal:return None
            thread=await db.get(ThreadRow,row.thread_id,with_for_update=True)
            if not thread or thread.active_turn_id!=row.id:return None
            if (row.lease_owner not in {"",owner} and row.lease_expires_at
                    and utc_datetime(row.lease_expires_at)>timestamp):return None
            if owner_limit and row.lease_owner!=owner:
                if db.bind.dialect.name=="postgresql":
                    from sqlalchemy.dialects.postgresql import insert
                else:
                    from sqlalchemy.dialects.sqlite import insert
                await db.execute(insert(OwnerAdmissionRow).values(id=thread.owner_id,
                    updated_at=timestamp).on_conflict_do_nothing())
                admission=await db.get(OwnerAdmissionRow,thread.owner_id,with_for_update=True)
                active=await db.scalar(select(func.count()).select_from(TurnRow).join(
                    ThreadRow,ThreadRow.id==TurnRow.thread_id).where(
                        ThreadRow.owner_id==thread.owner_id,TurnRow.id!=row.id,
                        TurnRow.lease_owner!="",TurnRow.lease_expires_at>timestamp))
                if active>=owner_limit:return None
                available=owner_limit-active
                older=await db.scalar(select(func.count()).select_from(TurnRow).join(
                    ThreadRow,ThreadRow.id==TurnRow.thread_id).where(
                        ThreadRow.owner_id==thread.owner_id,
                        ThreadRow.active_turn_id==TurnRow.id,
                        TurnRow.state.not_in(terminal),
                        or_(TurnRow.lease_owner=="",TurnRow.lease_expires_at.is_(None),
                            TurnRow.lease_expires_at<=timestamp),
                        or_(TurnRow.created_at<row.created_at,and_(
                            TurnRow.created_at==row.created_at,TurnRow.id<row.id))))
                if older>=available:return None
                admission.updated_at=timestamp
            if row.lease_owner!=owner:row.recovery_generation+=1
            row.lease_owner=owner
            row.lease_expires_at=timestamp+timedelta(seconds=lease_seconds)
            row.updated_at=timestamp
        return self.turn_view(row) if row else None


    async def owns_turn_lease(self, turn_id: str, owner: str, generation: int) -> bool:
        async with self.connections() as db:
            return (await db.execute(select(TurnRow.id).where(TurnRow.id == turn_id,
                TurnRow.lease_owner == owner, TurnRow.recovery_generation == generation,
                TurnRow.lease_expires_at > now()))).scalar_one_or_none() is not None


    async def renew_turn_lease(self, turn_id: str, owner: str, generation: int, lease_seconds: int) -> bool:
        timestamp = now()
        async with self.connections.begin() as db:
            result = await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                TurnRow.lease_owner == owner, TurnRow.recovery_generation == generation,
                TurnRow.lease_expires_at > timestamp).values(
                    lease_expires_at=timestamp + timedelta(seconds=lease_seconds), updated_at=timestamp))
            return result.rowcount == 1


    async def release_turn_lease(self, turn_id: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if row and row.lease_owner == owner and row.recovery_generation == generation:
                row.lease_owner = ""
                row.lease_expires_at = None
                row.updated_at = now()


    async def update_runtime_sequence(self, turn_id: str, sequence: int, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            self.require_lease_row(row, owner, generation)
            row.runtime_sequence = max(row.runtime_sequence, sequence)
            row.updated_at = now()


    @observed("db.execution.start")
    async def start_runtime_execution(self, turn_id: str, execution_id: str, owner: str, generation: int,
                                      *, input_id: str | None = None) -> None:
        event = None
        async with self.event_lock(turn_id):
            async with self.connections.begin() as db:
                row = await db.get(TurnRow, turn_id, with_for_update=True)
                self.require_lease_row(row, owner, generation)
                if input_id and input_id != turn_id:
                    pending = await db.get(TurnInputRow, input_id, with_for_update=True)
                    if not pending or pending.turn_id != turn_id or pending.status != "pending":
                        raise RuntimeError("input_not_pending")
                    await self._freeze_input_artifacts_locked(db,pending)
                    pending.status = "delivered"
                    pending.delivery_mode = "deferred_safe_point"
                    pending.updated_at = now()
                    items = (await db.execute(select(ItemRow).where(
                        ItemRow.turn_id == turn_id,
                        ItemRow.type == ItemType.USER_MESSAGE.value))).scalars().all()
                    item = next((candidate for candidate in items
                        if json.loads(candidate.payload_json).get("input_id") == input_id), None)
                    if item:
                        payload = json.loads(item.payload_json or "{}")
                        payload.update({"input_status":"delivered",
                            "delivery_mode":"deferred_safe_point"})
                        item.payload_json = json.dumps(payload)
                        item.updated_at = now()
                        last = (await db.execute(select(EventRow.sequence).where(
                            EventRow.turn_id == turn_id).order_by(
                                EventRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
                        event = Event(turn_id=turn_id, sequence=last + 1,
                            kind=EventKind.ITEM_UPDATED, item_id=item.id,
                            payload={"item_id":item.id, "status":"in_progress",
                                "input_id":input_id, "input_status":"delivered",
                                "delivery_mode":"deferred_safe_point"})
                        db.add(EventRow(turn_id=turn_id, sequence=event.sequence,
                            kind=event.kind.value, item_id=item.id,
                            payload_json=json.dumps(event.payload, ensure_ascii=False),
                            source_execution_id=execution_id, source_sequence=0,
                            created_at=event.created_at))
                row.runtime_execution_id = execution_id
                row.runtime_sequence = 0
                row.updated_at = now()
                sources = list((await db.scalars(select(TurnRow).where(
                    TurnRow.thread_id == row.thread_id, TurnRow.internal.is_(True),
                    TurnRow.error_detail.startswith(
                        _INTERNAL_CONTEXT_FORWARDED + turn_id + "\n")).with_for_update())).all())
                for source in sources:
                    source.error_detail = _mark_internal_context(source.error_detail,
                        _INTERNAL_CONTEXT_DELIVERED, turn_id)
                    source.updated_at = row.updated_at
        if event and self.events:
            await self.events.publish(event)


    async def set_turn_capability(self, turn_id: str, nonce: str, owner: str, generation: int) -> None:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            self.require_lease_row(row, owner, generation)
            row.capability_nonce = nonce
            row.updated_at = now()


    async def list_turns(self, thread_id: str, owner_id: str | None = None) -> list[TurnView]:
        async with self.connections() as db:
            query = select(TurnRow).join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(TurnRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.join(TurnOrderRow,
                TurnOrderRow.turn_id == TurnRow.id).order_by(
                    TurnOrderRow.sequence))).scalars().all()
        return [self.turn_view(row) for row in rows]


    async def get_turn_for_owner(self, turn_id: str, owner_id: str) -> TurnView | None:
        async with self.connections() as db:
            row = (await db.execute(select(TurnRow).join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(
                TurnRow.id == turn_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
        return self.turn_view(row) if row else None


    @observed("db.input.create")
    async def create_turn_input(self, turn_id: str, value: TurnInputCreate, owner_id: str) -> tuple[TurnInputView, bool]:
        accepting = {TurnState.QUEUED.value, TurnState.RESTORING.value, TurnState.STARTING.value,
            TurnState.RUNNING.value, TurnState.AWAITING_USER.value}
        async with self.connections.begin() as db:
            candidate = (await db.execute(select(TurnRow.thread_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(
                    TurnRow.id == turn_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
            if not candidate:
                raise KeyError("turn_not_found")
            thread = await db.get(ThreadRow, candidate, with_for_update=True)
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not thread or thread.owner_id != owner_id or not turn or turn.thread_id != thread.id:
                raise KeyError("turn_not_found")
            existing = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.idempotency_key == value.idempotency_key))).scalar_one_or_none()
            if existing:
                attachments = [item.model_dump(mode="json") for item in value.attachments]
                if existing.message != value.message or json.loads(existing.attachments_json) != attachments:
                    raise RuntimeError("idempotency_conflict")
                return self.input_view(existing), False
            if turn.state not in accepting or turn.cancel_requested:
                raise RuntimeError("turn_not_accepting_input")
            timestamp = now()
            sequence = (await db.execute(select(TurnInputRow.sequence).where(TurnInputRow.turn_id == turn_id)
                .order_by(TurnInputRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
            row = TurnInputRow(id=identifier(), turn_id=turn_id, sequence=sequence + 1,
                idempotency_key=value.idempotency_key, message=value.message,
                attachments_json=json.dumps([item.model_dump(mode="json") for item in value.attachments]),
                status="pending", delivery_mode="pending", created_at=timestamp, updated_at=timestamp)
            db.add(row)
            item_sequence = await self.next_item_sequence(db, turn.thread_id)
            db.add(ItemRow(id=identifier(), thread_id=turn.thread_id, turn_id=turn_id, sequence=item_sequence,
                type=ItemType.USER_MESSAGE.value, status="in_progress", summary=value.message,
                payload_json=json.dumps({"input_id":row.id, "input_status":"pending",
                    "delivery_mode":"pending",
                    "attachments":[item.model_dump(mode="json") for item in value.attachments]}),
                created_at=timestamp, updated_at=timestamp))
        return self.input_view(row), True


    async def next_pending_input(self, turn_id: str) -> TurnInputView | None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.status == "pending").order_by(TurnInputRow.sequence).limit(1).with_for_update(skip_locked=True))).scalar_one_or_none()
            if not row:
                return None
            # Selection is not delivery. start_runtime_execution marks the input
            # only when its execution identity is durably assigned.
        return self.input_view(row)


    @staticmethod
    def input_artifact_id(input_id:str,name:str)->str:
        return hashlib.sha256(("turn-input-artifact:"+input_id+":"+name).encode()).hexdigest()[:32]


    async def _freeze_input_artifacts_locked(self,db,row)->None:
        for attachment in json.loads(row.attachments_json):
            identity=self.input_artifact_id(row.id,attachment['name'])
            if await db.get(ArtifactRow,identity) is None:
                db.add(ArtifactRow(id=identity,turn_id=row.turn_id,
                    path='input/inbox/'+attachment['name'],sha256=attachment['sha256'],
                    size=attachment['size'],mime=attachment['mime'],
                    object_ref=attachment['object_ref'],created_at=now()))


    async def prepare_turn_input_artifacts(self,input_id:str,*,turn_id:str,
            execution_id:str,lease_owner:str,recovery_generation:int)->bool:
        """Bind materialized inputs before the Runtime can consume their message."""
        async with self.event_lock(turn_id):
            async with self.connections.begin() as db:
                turn=await db.get(TurnRow,turn_id,with_for_update=True)
                self.require_lease_row(turn,lease_owner,recovery_generation)
                if (turn.state!=TurnState.RUNNING.value or turn.cancel_requested
                        or not execution_id or turn.runtime_execution_id!=execution_id):
                    return False
                row=await db.get(TurnInputRow,input_id,with_for_update=True)
                if not row or row.turn_id!=turn_id or row.status!='pending':return False
                await self._freeze_input_artifacts_locked(db,row)
        return True


    async def set_input_status(self, input_id: str, status: str,
                               delivery_mode: str | None = None, *,
                               execution_id: str = "", lease_owner: str | None = None,
                               recovery_generation: int | None = None) -> bool:
        transitions = {
            "pending": {"delivered", "unknown", "applied", "failed"},
            "delivered": {"applied", "unknown", "failed"},
            "unknown": {"applied", "failed"},
            "applied": set(),
            "failed": set(),
        }
        if status not in transitions:
            raise ValueError("invalid_input_status")
        async with self.connections() as lookup:
            turn_id = (await lookup.execute(select(TurnInputRow.turn_id).where(
                TurnInputRow.id == input_id))).scalar_one_or_none()
        if not turn_id:
            return False
        event = None
        async with self.event_lock(turn_id):
            async with self.connections.begin() as db:
                turn = await db.get(TurnRow, turn_id, with_for_update=True)
                row = await db.get(TurnInputRow, input_id, with_for_update=True)
                if not row or not turn:
                    return False
                if lease_owner is not None:
                    self.require_lease_row(turn, lease_owner, recovery_generation)
                if execution_id and turn.runtime_execution_id != execution_id:
                    return False
                if row.status == status:
                    return True
                if status not in transitions.get(row.status, set()):
                    return False
                if TurnState(turn.state) in TERMINAL and status not in {"applied", "failed"}:
                    return False
                if status == "applied":
                    await self._freeze_input_artifacts_locked(db,row)
                row.status = status
                row.updated_at = now()
                if delivery_mode:
                    row.delivery_mode = delivery_mode
                items = (await db.execute(select(ItemRow).where(
                    ItemRow.turn_id == row.turn_id,
                    ItemRow.type == ItemType.USER_MESSAGE.value,
                ))).scalars().all()
                item = next((candidate for candidate in items
                    if json.loads(candidate.payload_json).get("input_id") == input_id), None)
                if item:
                    payload = json.loads(item.payload_json or "{}")
                    payload.update({"input_status":status, "delivery_mode":row.delivery_mode})
                    item_status = {"applied":"completed", "failed":"failed"}.get(
                        status, "in_progress")
                    item.status = item_status
                    item.payload_json = json.dumps(payload)
                    item.updated_at = now()
                    last = (await db.execute(select(EventRow.sequence).where(
                        EventRow.turn_id == turn_id).order_by(
                            EventRow.sequence.desc()).limit(1))).scalar_one_or_none() or 0
                    event = Event(turn_id=turn_id, sequence=last + 1,
                        kind=EventKind.ITEM_UPDATED, item_id=item.id,
                        payload={"item_id":item.id, "status":item_status,
                            "input_id":input_id, "input_status":status,
                            "delivery_mode":row.delivery_mode})
                    db.add(EventRow(turn_id=turn_id, sequence=event.sequence,
                        kind=event.kind.value, item_id=item.id,
                        payload_json=json.dumps(event.payload, ensure_ascii=False),
                        source_execution_id=execution_id, source_sequence=0,
                        created_at=event.created_at))
        if event and self.events:
            await self.events.publish(event)
        return True


    async def get_turn_input(self, input_id: str) -> TurnInputView | None:
        async with self.connections() as db:
            row = await db.get(TurnInputRow, input_id)
        return self.input_view(row) if row else None


    async def fail_unapplied_inputs(self, turn_id: str, *, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> None:
        async with self.connections.begin() as db:
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn:
                raise KeyError("turn_not_found")
            if lease_owner is not None:
                self.require_lease_row(turn, lease_owner, recovery_generation)
            rows = (await db.execute(select(TurnInputRow).where(
                TurnInputRow.turn_id == turn_id,
                TurnInputRow.status.in_(["pending", "delivered", "unknown"]),
            ).with_for_update())).scalars().all()
            if not rows:
                return
            input_ids = {row.id for row in rows}
            timestamp = now()
            for row in rows:
                row.status = "failed" if row.status == "pending" else "unknown"
                row.updated_at = timestamp
            items = (await db.execute(select(ItemRow).where(
                ItemRow.turn_id == turn_id,
                ItemRow.type == ItemType.USER_MESSAGE.value,
            ))).scalars().all()
            for item in items:
                payload=json.loads(item.payload_json)
                if payload.get("input_id") in input_ids:
                    row=next(value for value in rows if value.id==payload["input_id"])
                    payload["input_status"]=row.status
                    item.payload_json=json.dumps(payload)
                    item.status = "failed" if row.status=="failed" else "in_progress"
                    item.updated_at = timestamp


    async def apply_delivered_inputs(self, turn_id: str) -> None:
        async with self.connections.begin() as db:
            rows = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id,
                TurnInputRow.status == "delivered").with_for_update())).scalars().all()
            for row in rows:
                row.status = "applied"; row.updated_at = now()
            input_ids = {row.id for row in rows}
            if input_ids:
                items = (await db.execute(select(ItemRow).where(
                    ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.USER_MESSAGE.value,
                ))).scalars().all()
                for item in items:
                    if json.loads(item.payload_json).get("input_id") in input_ids:
                        item.status = "completed"; item.updated_at = now()


    async def list_turn_inputs(self, turn_id: str) -> list[TurnInputView]:
        async with self.connections() as db:
            rows = (await db.execute(select(TurnInputRow).where(TurnInputRow.turn_id == turn_id)
                .order_by(TurnInputRow.sequence))).scalars().all()
        return [self.input_view(row) for row in rows]


    async def authoritative_user_context(self, thread_id: str) -> dict:
        async with self.connections() as db:
            turn = (await db.execute(select(TurnRow).join(TurnOrderRow,
                TurnOrderRow.turn_id == TurnRow.id).where(
                TurnRow.thread_id == thread_id, TurnRow.internal == False,
            ).order_by(TurnOrderRow.sequence.desc()).limit(1))).scalar_one_or_none()
            if not turn:return {}
            inputs = list((await db.execute(select(TurnInputRow).where(
                TurnInputRow.turn_id == turn.id, TurnInputRow.status == 'applied',
            ).order_by(TurnInputRow.sequence))).scalars().all())
        return {'source_turn_id':turn.id,'initial_message':turn.public_message,
            'applied_inputs':[{'input_id':row.id,'sequence':row.sequence,'message':row.message,
                'attachments':json.loads(row.attachments_json)} for row in inputs]}


    async def _finish_turn_records(self, db, turn_id: str) -> None:
        live_job_ids=set((await db.execute(select(JobRow.id).where(
            JobRow.origin_turn_id==turn_id,
            JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES])))).scalars().all())
        inputs = (await db.execute(select(TurnInputRow).where(
            TurnInputRow.turn_id == turn_id, TurnInputRow.status.in_(["pending", "delivered", "unknown"])
        ).with_for_update())).scalars().all()
        ids = {value.id for value in inputs}
        for value in inputs:
            value.status = "failed" if value.status == "pending" else "unknown"
            value.updated_at = now()
        await db.execute(update(InteractionRow).where(InteractionRow.turn_id == turn_id,
            InteractionRow.status.in_(["pending", "responding"])).values(status="expired", updated_at=now()))
        items = (await db.execute(select(ItemRow).where(ItemRow.turn_id == turn_id))).scalars().all()
        for item in items:
            payload=json.loads(item.payload_json)
            job_activity=bool(payload.get('model_request_id') and payload.get('job_id') in live_job_ids)
            if (item.status == "in_progress" and not job_activity
                    and item.type not in {ItemType.USER_MESSAGE.value, ItemType.APPROVAL.value}):
                item.status = "interrupted"; item.updated_at = now()
            if payload.get("input_id") in ids:
                value=next(row for row in inputs if row.id==payload["input_id"])
                payload["input_status"]=value.status
                item.payload_json=json.dumps(payload)
                item.status="failed" if value.status=="failed" else "in_progress"
                item.updated_at=now()
            elif item.type == ItemType.APPROVAL.value and item.status == "in_progress":
                item.status = "failed"; item.updated_at = now()
        await self._seal_media_batch_on_turn_finish(db, turn_id)

    async def _seal_media_batch_on_turn_finish(self, db, turn_id):
        await db.execute(update(MediaBatchRow).where(MediaBatchRow.turn_id == turn_id,
            MediaBatchRow.status == "open").values(status="sealed", sealed_at=now(), updated_at=now()))


    @observed("db.turn.cancel")
    async def request_turn_cancel(self, turn_id: str) -> TurnView:
        terminal = [state.value for state in (TurnState.COMPLETED, TurnState.WAITING_USER,
            TurnState.FAILED, TurnState.CANCELLED, TurnState.PAUSED)]
        async with self.connections.begin() as db:
            # Conditional UPDATE serializes cancellation with completion and lease claims
            # on PostgreSQL and SQLite alike.
            row = (await db.execute(update(TurnRow).where(TurnRow.id == turn_id,
                TurnRow.state.not_in(terminal)).values(cancel_requested=True, updated_at=now())
                .returning(TurnRow))).scalar_one_or_none()
            if row is None:
                if await db.get(TurnRow, turn_id) is None:
                    raise KeyError("turn_not_found")
                raise RuntimeError("turn_already_terminal")
            if row.state == TurnState.QUEUED.value and (not row.lease_expires_at
                    or utc_datetime(row.lease_expires_at) <= now()):
                row.state = TurnState.CANCELLED.value
                row.error_code = "cancelled"
                row.error_detail = "Queued Turn was cancelled"
                await self._finish_turn_records(db, turn_id)
        return self.turn_view(row)


    @observed("db.turn.state")
    async def set_turn_state(self, turn_id: str, state: TurnState, *, sandbox_id: str | None = None,
                             sandbox_handle_json: str | None = None, error_code: str = "",
                             error_detail: str = "", lease_owner: str | None = None,
                             recovery_generation: int | None = None) -> TurnView:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            if not row:
                raise KeyError("turn_not_found")
            if lease_owner is not None:
                self.require_lease_row(row, lease_owner, recovery_generation)
            if (row.cancel_requested and error_code != "cleanup_failed"
                    and state in {TurnState.COMPLETED, TurnState.WAITING_USER, TurnState.FAILED}):
                state, error_code, error_detail = TurnState.CANCELLED, "cancelled", "Turn was cancelled"
            row.state = state.value
            if sandbox_id is not None:
                row.sandbox_id = sandbox_id
                if not sandbox_id and sandbox_handle_json is None:row.sandbox_handle_json=""
            if sandbox_handle_json is not None:
                if sandbox_id is not None and sandbox_id:
                    try:handle=json.loads(sandbox_handle_json)
                    except (TypeError,ValueError) as exc:raise ValueError(
                        "sandbox_handle_json_invalid") from exc
                    if handle.get("sandbox_id")!=sandbox_id:
                        raise ValueError("sandbox_handle_id_mismatch")
                row.sandbox_handle_json=sandbox_handle_json
            row.error_code = error_code
            row.error_detail = error_detail[:4000]
            if state in TERMINAL:
                row.pending_final_state = ""
                await self._finish_turn_records(db, turn_id)
            row.updated_at = now()
        return self.turn_view(row)


    @observed("db.turn.cleanup")
    async def begin_turn_cleanup(self, turn_id: str, final_state: TurnState, *, error_code: str,
                                 error_detail: str, owner: str, generation: int) -> TurnView:
        async with self.connections.begin() as db:
            row = await db.get(TurnRow, turn_id, with_for_update=True)
            self.require_lease_row(row, owner, generation)
            if row.cancel_requested:
                final_state, error_code, error_detail = TurnState.CANCELLED, "cancelled", "Turn was cancelled"
            row.state = TurnState.CLEANING.value
            row.pending_final_state = final_state.value
            row.error_code = error_code
            row.error_detail = error_detail[:4000]
            row.updated_at = now()
        return self.turn_view(row)


    @observed("db.checkpoint.commit")
    async def update_thread_checkpoint(self, thread_id: str, checkpoint_ref: str, native_thread_id: str,
                                       *, turn_id: str | None = None, lease_owner: str | None = None,
                                       recovery_generation: int | None = None,
                                       checkpoint_size:int=0,partial:bool=False,
                                       verification:dict|None=None) -> None:
        await self.commit_checkpoint_version(thread_id,checkpoint_ref,native_thread_id,
            size=checkpoint_size,source_turn_id=turn_id or "",partial=partial,
            verification=verification,lease_owner=lease_owner,
            recovery_generation=recovery_generation)
