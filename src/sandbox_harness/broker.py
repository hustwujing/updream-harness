from __future__ import annotations

import asyncio
import logging
import mimetypes
from collections import defaultdict
import httpx

from .capability import CapabilityCodec
from .catalog import HarnessCatalog
from .config import Settings
from .domain import EventKind, Harness, ItemType, RuntimeResult, TurnCreate, TurnState, TurnView
from .objects import ObjectStore
from .pool import SandboxPool
from .runtime import RuntimeClient
from .store import Store


ROUTES = {
    Harness.CODEX: ["/v1/models/codex", "/v1/providers", "/v1/artifacts", "/v1/public-http"],
    Harness.CLAUDE: ["/v1/models/claude", "/v1/providers", "/v1/artifacts", "/v1/public-http"],
    Harness.DEEPSEEK: ["/v1/models/deepseek", "/v1/providers", "/v1/artifacts", "/v1/public-http"],
}

logger = logging.getLogger(__name__)


class CheckpointRestoreError(RuntimeError):
    pass


class Broker:
    def __init__(self, settings: Settings, store: Store, objects: ObjectStore, pool: SandboxPool, runtime: RuntimeClient, catalog: HarnessCatalog):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.pool = pool
        self.runtime = runtime
        self.codec = CapabilityCodec(settings.gateway_signing_key)
        self.catalog = catalog
        self.tasks: dict[str, asyncio.Task] = {}
        self.cancelled: set[str] = set()
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.input_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.active_sandboxes = {}
        self.instance_id = "broker-" + __import__("uuid").uuid4().hex
        self.leases: dict[str, int] = {}
        self.heartbeats: dict[str, asyncio.Task] = {}
        self.recovery_task: asyncio.Task | None = None
        self.reaper_task: asyncio.Task | None = None
        self.closing = False

    def _schedule(self, turn_id: str) -> None:
        task = self.tasks.get(turn_id)
        if task and not task.done():
            return
        self.tasks[turn_id] = asyncio.create_task(self._process(turn_id), name="turn-" + turn_id)

    def status(self) -> dict:
        return {
            "instance_id": self.instance_id,
            "active_turns": len([task for task in self.tasks.values() if not task.done()]),
            "owned_leases": len(self.leases),
            "recovery_scanner": bool(self.recovery_task and not self.recovery_task.done()),
            "orphan_reaper": bool(self.reaper_task and not self.reaper_task.done()),
        }

    async def start(self) -> None:
        await self.reconcile()
        try:
            await self.cleanup_startup_orphans(delete_stale_warm=True)
        except Exception:
            logger.exception("Startup orphan reconciliation failed")
        self.recovery_task = asyncio.create_task(self._recovery_loop(), name="turn-recovery-loop")
        self.reaper_task = asyncio.create_task(self._reaper_loop(), name="sandbox-orphan-reaper")

    async def cleanup_startup_orphans(self, *, delete_stale_warm: bool = False) -> None:
        live_turns = await self.store.nonterminal_turn_ids()
        active_ids = {sandbox.id for sandbox in self.active_sandboxes.values()} | await self.pool.managed_sandbox_ids()
        for sandbox in await self.pool.backend.list_namespace():
            turn_id = sandbox.metadata.get("sandbox-harness.io/turn", "")
            if turn_id.startswith("warm-") and not delete_stale_warm:
                continue
            if sandbox.id not in active_ids and turn_id not in live_turns:
                logger.warning("Deleting orphan sandbox id=%s turn=%s", sandbox.id, turn_id)
                await self.pool.backend.delete(sandbox)

    async def close(self) -> None:
        self.closing = True
        if self.recovery_task:
            self.recovery_task.cancel()
        if self.reaper_task:
            self.reaper_task.cancel()
        for task in list(self.tasks.values()):
            task.cancel()
        values = [task for task in (self.recovery_task, self.reaper_task) if task] + list(self.tasks.values())
        if values:
            await asyncio.gather(*values, return_exceptions=True)
        for turn_id, generation in list(self.leases.items()):
            await self.store.release_turn_lease(turn_id, self.instance_id, generation)

    async def _recovery_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.recovery_scan_seconds)
            try:
                await self.reconcile()
            except Exception:
                logger.exception("Expired Turn reconciliation failed")

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.orphan_reaper_seconds)
            try:
                await self.cleanup_startup_orphans()
            except Exception:
                logger.exception("Periodic orphan reconciliation failed")

    async def reconcile(self) -> None:
        for turn in await self.store.recoverable_turns():
            try:
                await self._reconcile_turn(turn)
            except Exception:
                logger.exception("Turn reconciliation failed turn=%s", turn.id)
                generation = self.leases.pop(turn.id, None)
                if generation is not None:
                    await self.store.release_turn_lease(turn.id, self.instance_id, generation)

    async def _reconcile_turn(self, turn) -> None:
        if self.closing or turn.id in self.tasks:
            return
        claimed = await self.store.claim_turn(turn.id, self.instance_id, self.settings.turn_lease_seconds)
        if not claimed:
            return
        self.leases[turn.id] = claimed.recovery_generation
        if claimed.state == TurnState.QUEUED:
            self._schedule(turn.id); return
        if claimed.state == TurnState.CLEANING and claimed.pending_final_state:
            if claimed.capability_nonce:
                try:
                    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
                        response = await client.post(self.settings.gateway_url.rstrip('/') + '/internal/revoke/' + claimed.capability_nonce,
                            headers={"X-Sandbox-Harness-Control": self.settings.gateway_control_key})
                        response.raise_for_status()
                except Exception as exc:
                    await self.store.append_event(turn.id, EventKind.WARNING, {
                        "code": "recovery_capability_revoke_failed", "detail": type(exc).__name__})
            for sandbox in await self.pool.backend.find_by_turn(turn.id):
                await self.pool.backend.delete(sandbox)
            target = TurnState(claimed.pending_final_state)
            await self.store.append_event(turn.id, EventKind.WARNING, {"code": "turn_cleanup_recovered",
                "generation": claimed.recovery_generation, "final_state": target.value})
            logger.warning("Recovered cleanup turn=%s generation=%s final=%s", turn.id, claimed.recovery_generation, target.value)
            await self._state(turn.id, target, sandbox_id="", error_code=claimed.error_code, error_detail=claimed.error_detail)
            await self.store.release_turn_lease(turn.id, self.instance_id, claimed.recovery_generation)
            self.leases.pop(turn.id, None); return
        if claimed.state in {TurnState.RESTORING, TurnState.STARTING}:
            for sandbox in await self.pool.backend.find_by_turn(turn.id):
                await self.pool.backend.delete(sandbox)
            await self.store.append_event(turn.id, EventKind.WARNING, {
                "code": "turn_recovered_before_harness_start", "generation": claimed.recovery_generation})
            await self._state(turn.id, TurnState.QUEUED, sandbox_id=""); self._schedule(turn.id); return
        if claimed.state in {TurnState.RUNNING, TurnState.AWAITING_USER, TurnState.DRAINING_INPUTS, TurnState.CHECKPOINTING} and claimed.runtime_execution_id:
            matches = await self.pool.backend.find_by_turn(turn.id)
            if len(matches) == 1 and await self.runtime.execution_status(matches[0], claimed.runtime_execution_id) is not None:
                await self.store.append_event(turn.id, EventKind.WARNING, {"code": "turn_runtime_reattached",
                    "generation": claimed.recovery_generation, "runtime_sequence": claimed.runtime_sequence})
                logger.warning("Reattaching runtime turn=%s generation=%s sequence=%s", turn.id, claimed.recovery_generation, claimed.runtime_sequence)
                self._schedule(turn.id); return
            for sandbox in matches:
                await self.pool.backend.delete(sandbox)
        await self.store.append_event(turn.id, EventKind.ERROR, {"code": "turn_interrupted_during_harness_execution",
            "detail": "The previous broker lease expired after Harness execution began; automatic replay was suppressed.",
            "generation": claimed.recovery_generation})
        logger.error("Runtime recovery unavailable turn=%s generation=%s", turn.id, claimed.recovery_generation)
        await self.store.complete_agent_item(turn.id, "interrupted")
        await self._state(turn.id, TurnState.FAILED, sandbox_id="", error_code="turn_interrupted_during_harness_execution",
            error_detail="Harness execution was interrupted and was not replayed because external side effects may already exist")
        await self.store.release_turn_lease(turn.id, self.instance_id, claimed.recovery_generation)
        self.leases.pop(turn.id, None)

    async def try_deliver_input(self, value) -> bool:
        async with self.input_locks[value.turn_id]:
            sandbox = self.active_sandboxes.get(value.turn_id)
            turn = await self.store.get_turn(value.turn_id)
            if not sandbox or not turn or turn.state != TurnState.RUNNING:
                return False
            project_files = await self._register_project_attachments(
                turn.thread_id, turn.id, value.attachments,
            )
            await self._materialize_project_files(sandbox, project_files)
            await self._materialize_attachments(sandbox, value.attachments)
            delivery_mode = await self.runtime.append_input(sandbox, message=value.message,
                attachments=value.attachments, reasoning_effort=turn.reasoning_effort,
                execution_id=turn.runtime_execution_id, input_id=value.id)
            if delivery_mode:
                current = await self.store.get_turn(value.turn_id)
                await self.store.set_input_status(value.id,
                    "delivered" if current and current.state == TurnState.RUNNING else "applied", delivery_mode)
            return bool(delivery_mode)

    async def respond_interaction(self, interaction_id: str, owner_id: str, value) -> object:
        response = {"action": value.action, "answers": value.answers}
        interaction, claimed = await self.store.claim_interaction_response(
            interaction_id, owner_id, value.idempotency_key, response,
        )
        if not claimed:
            return interaction
        turn = await self.store.get_turn(interaction.turn_id)
        sandbox = self.active_sandboxes.get(interaction.turn_id)
        if (not turn or not sandbox or turn.runtime_execution_id != interaction.runtime_execution_id):
            await self.store.finish_interaction_response(interaction_id, delivered=False)
            raise RuntimeError("interaction_runtime_unavailable")
        if interaction.kind == "approval" and value.action not in {"approve", "reject"}:
            await self.store.finish_interaction_response(interaction_id, delivered=False)
            raise ValueError("Approval requires approve or reject")
        if interaction.kind == "question" and (value.action != "answer" or not value.answers):
            await self.store.finish_interaction_response(interaction_id, delivered=False)
            raise ValueError("Question requires at least one answer")
        delivered = await self.runtime.respond_interaction(
            sandbox, interaction.runtime_interaction_id, response,
        )
        current = await self.store.finish_interaction_response(interaction_id, delivered=delivered)
        if not delivered:
            raise RuntimeError("interaction_transport_unsupported")
        await self.store.finish_interaction_item(interaction.turn_id, interaction.id, response)
        await self._state(interaction.turn_id, TurnState.RUNNING, sandbox_id=sandbox.id)
        return current

    async def background_jobs(self, turn_id: str, owner_id: str) -> list[dict]:
        turn = await self.store.get_turn_for_owner(turn_id, owner_id)
        if not turn:
            raise KeyError("turn_not_found")
        sandbox = self.active_sandboxes.get(turn_id)
        if not sandbox:
            return []
        return await self.runtime.background_jobs(sandbox)

    async def cancel_background_job(self, turn_id: str, job_id: str, owner_id: str) -> dict:
        turn = await self.store.get_turn_for_owner(turn_id, owner_id)
        if not turn:
            raise KeyError("turn_not_found")
        sandbox = self.active_sandboxes.get(turn_id)
        if not sandbox:
            raise RuntimeError("background_runtime_unavailable")
        return await self.runtime.cancel_background_job(sandbox, job_id)

    async def submit(self, thread_id: str, value: TurnCreate) -> TurnView:
        if sum(item.size for item in value.attachments) > self.settings.max_attachment_bytes:
            raise ValueError("Attachments exceed the Turn byte limit")
        thread = await self.store.get_thread(thread_id)
        if not thread:
            raise KeyError("thread_not_found")
        if value.harness is not None and value.harness != thread.harness:
            raise ValueError("Harness is fixed for this conversation; create a new conversation to switch Harness")
        resolved = self.catalog.resolve(thread.harness, value.model or thread.model,
            value.reasoning_level or thread.reasoning_level)
        turn, created, start_now = await self.store.create_turn(thread_id, value, resolved, thread.owner_id)
        if created and start_now:
            self._schedule(turn.id)
        return turn

    async def cancel(self, turn_id: str) -> None:
        self.cancelled.add(turn_id)
        turn = await self.store.get_turn(turn_id)
        if not turn:
            raise KeyError("turn_not_found")
        await self.store.append_event(turn_id, EventKind.WARNING, {"code": "cancel_requested"})
        task = self.tasks.get(turn_id)
        if task:
            task.cancel()
        elif turn.state == TurnState.QUEUED:
            await self._state(turn_id, TurnState.CANCELLED, error_code="cancelled", error_detail="Queued Turn was cancelled")

    async def _state(self, turn_id: str, state: TurnState, **kwargs) -> TurnView:
        generation = self.leases.get(turn_id)
        turn = await self.store.set_turn_state(turn_id, state, **kwargs,
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)
        await self.store.append_event(turn_id, EventKind.STATE, {"state": state.value})
        return turn

    async def _heartbeat(self, turn_id: str, generation: int) -> None:
        while True:
            await asyncio.sleep(self.settings.turn_heartbeat_seconds)
            renewed = await self.store.renew_turn_lease(
                turn_id, self.instance_id, generation, self.settings.turn_lease_seconds,
            )
            if not renewed:
                task = self.tasks.get(turn_id)
                if task:
                    task.cancel()
                return

    async def _record_runtime_event(self, thread_id: str, turn_id: str, value: RuntimeEvent,
                                    execution_id: str = "") -> None:
        if await self.store.runtime_event_recorded(turn_id, execution_id, value.runtime_sequence):
            return
        if value.kind == EventKind.TOOL_OUTPUT:
            await self.store.append_event(turn_id, value.kind, value.payload,
                source_execution_id=execution_id, source_sequence=value.runtime_sequence)
            return
        if value.kind == EventKind.WAITING_USER:
            runtime_id = str(value.payload.get("interaction_id") or "")
            if not runtime_id:
                raise ValueError("Runtime interaction has no ID")
            kind = str(value.payload.get("interaction_kind") or "approval")
            if kind not in {"approval", "question"}:
                raise ValueError("Runtime interaction kind is invalid")
            interaction, created = await self.store.upsert_interaction(
                thread_id, turn_id, execution_id, runtime_id, kind, value.payload,
            )
            if created:
                item = await self.store.append_item(thread_id, turn_id, ItemType.APPROVAL,
                    status="in_progress", summary=str(value.payload.get("prompt") or "需要用户确认"),
                    payload={**value.payload, "interaction_id": interaction.id})
            else:
                item = next((candidate for candidate in await self.store.items_after(thread_id)
                    if candidate.turn_id == turn_id and candidate.payload.get("interaction_id") == interaction.id), None)
                if item is None:
                    item = await self.store.append_item(thread_id, turn_id, ItemType.APPROVAL,
                        status="in_progress", summary=str(value.payload.get("prompt") or "需要用户确认"),
                        payload={**value.payload, "interaction_id": interaction.id})
            await self.store.append_event(turn_id, value.kind, value.payload, item_id=item.id,
                source_execution_id=execution_id, source_sequence=value.runtime_sequence)
            await self._state(turn_id, TurnState.AWAITING_USER)
            return
        if value.kind == EventKind.TEXT_DELTA:
            item = await self.store.append_agent_delta(thread_id, turn_id, str(value.payload.get("text", "")))
        elif value.kind == EventKind.TOOL_FINISHED and value.payload.get("tool_use_id"):
            item = await self.store.finish_tool_item(turn_id, str(value.payload["tool_use_id"]), value.payload)
            if item is None:
                item = await self.store.append_item(thread_id, turn_id, ItemType.TOOL,
                    status="failed" if value.payload.get("is_error") else "completed",
                    summary=str(value.payload.get("name") or "tool"), payload=value.payload)
        else:
            item_type = {
                EventKind.TOOL_STARTED: ItemType.TOOL, EventKind.TOOL_OUTPUT: ItemType.TOOL,
                EventKind.TOOL_FINISHED: ItemType.TOOL, EventKind.STEP_STARTED: ItemType.PLAN,
                EventKind.STEP_PROGRESS: ItemType.PLAN, EventKind.STEP_FINISHED: ItemType.PLAN,
                EventKind.ARTIFACT_CREATED: ItemType.ARTIFACT, EventKind.WARNING: ItemType.WARNING,
                EventKind.ERROR: ItemType.ERROR, EventKind.WAITING_USER: ItemType.APPROVAL,
            }.get(value.kind, ItemType.REASONING)
            item = await self.store.append_item(thread_id, turn_id, item_type,
                status="completed" if value.kind in {
                    EventKind.TOOL_FINISHED, EventKind.STEP_FINISHED, EventKind.WARNING, EventKind.ERROR,
                } else "in_progress",
                summary=str(value.payload.get("name") or value.payload.get("text") or value.kind.value), payload=value.payload)
        await self.store.append_event(turn_id, value.kind, value.payload, item_id=item.id,
            source_execution_id=execution_id, source_sequence=value.runtime_sequence)
        if value.payload.get("input_completed"):
            await self.store.complete_agent_item(turn_id, "completed")

    async def _materialize_attachments(self, sandbox, attachments) -> None:
        for attachment in attachments:
            data = self.objects.get(attachment.object_ref, maximum=self.settings.max_attachment_bytes)
            if len(data) != attachment.size:
                raise ValueError("Attachment size does not match its immutable record")
            await self.runtime.put_attachment(sandbox, attachment.name, data, attachment.mime)

    async def _register_project_attachments(self, thread_id: str, turn_id: str, attachments) -> list:
        values = []
        for attachment in attachments:
            value, _ = await self.store.register_project_file(thread_id,
                path="assets/" + attachment.name, source_type="upload", source_turn_id=turn_id,
                sha256=attachment.sha256, size=attachment.size, mime=attachment.mime,
                object_ref=attachment.object_ref)
            values.append(value)
        return values

    async def _materialize_project_files(self, sandbox, files) -> None:
        for value in files:
            data = self.objects.get(value.object_ref, maximum=self.settings.max_artifact_bytes)
            if len(data) != value.size:
                raise ValueError("Project file size does not match its immutable record")
            await self.runtime.put_project_file(sandbox, value.path, data, value.mime)

    async def _process(self, turn_id: str) -> None:
        claimed = await self.store.claim_turn(
            turn_id, self.instance_id, self.settings.turn_lease_seconds,
        )
        if not claimed:
            self.tasks.pop(turn_id, None)
            return
        generation = claimed.recovery_generation
        self.leases[turn_id] = generation
        heartbeat = asyncio.create_task(self._heartbeat(turn_id, generation), name="turn-heartbeat-" + turn_id)
        self.heartbeats[turn_id] = heartbeat
        turn = await self.store.get_turn(turn_id)
        if not turn:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            self.heartbeats.pop(turn_id, None)
            await self.store.release_turn_lease(turn_id, self.instance_id, generation)
            self.leases.pop(turn_id, None)
            self.tasks.pop(turn_id, None)
            return
        thread = await self.store.get_thread(turn.thread_id)
        if not thread:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            self.heartbeats.pop(turn_id, None)
            await self.store.release_turn_lease(turn_id, self.instance_id, generation)
            self.leases.pop(turn_id, None)
            self.tasks.pop(turn_id, None)
            return
        sandbox = None
        final_state = TurnState.FAILED
        error_code = "turn_failed"
        error_detail = "Turn failed before the Harness returned a result"
        abandoned = False
        try:
            async with self.locks[thread.id]:
                recovering = turn.state in {
                    TurnState.RUNNING, TurnState.AWAITING_USER, TurnState.DRAINING_INPUTS, TurnState.CHECKPOINTING,
                } and bool(turn.runtime_execution_id)
                recovered_result = None
                if recovering:
                    matches = await self.pool.backend.find_by_turn(turn.id)
                    if len(matches) != 1:
                        raise RuntimeError("recoverable_sandbox_not_found")
                    sandbox = await self.pool.adopt(matches[0], thread_id=thread.id, turn_id=turn.id)
                    self.active_sandboxes[turn.id] = sandbox
                    status = await self.runtime.execution_status(sandbox, turn.runtime_execution_id)
                    if status is None:
                        raise RuntimeError("recoverable_runtime_execution_not_found")
                    if status.get("state") == "completed" and turn.runtime_sequence >= int(status.get("last_sequence") or 0):
                        recovered_result = RuntimeResult.model_validate({
                            **(status.get("result") or {}),
                            "runtime_sequence": int(status.get("last_sequence") or 0),
                        })
                    else:
                        async for value in self.runtime.resume(
                            sandbox, turn.runtime_execution_id, turn.runtime_sequence,
                        ):
                            if isinstance(value, RuntimeResult):
                                recovered_result = value
                            else:
                                await self._record_runtime_event(thread.id, turn.id, value, turn.runtime_execution_id)
                            if value.runtime_sequence:
                                await self.store.update_runtime_sequence(
                                    turn.id, value.runtime_sequence, self.instance_id, generation,
                                )
                        if recovered_result is None:
                            raise RuntimeError("Runtime recovery stream ended without a result")
                else:
                    await self._state(turn.id, TurnState.RESTORING)
                    sandbox = await self.pool.acquire(
                        thread_id=thread.id,
                        turn_id=turn.id,
                        harness=turn.harness,
                        environment={},
                    )
                    token, claims = self.codec.issue(
                        thread_id=thread.id,
                        turn_id=turn.id,
                        sandbox_id=sandbox.id,
                        harness=turn.harness,
                        model=turn.model,
                        routes=ROUTES[turn.harness],
                        ttl_seconds=self.settings.capability_ttl_seconds,
                        max_requests=self.settings.capability_request_limit,
                        max_public_requests=self.settings.capability_public_request_limit,
                        token_budget=self.settings.model_token_budget,
                    )
                    await self.store.set_turn_capability(
                        turn.id, claims.nonce, self.instance_id, generation,
                    )
                    await self._state(turn.id, TurnState.STARTING, sandbox_id=sandbox.id)
                    await self.runtime.inject(
                        sandbox,
                        capability_token=token,
                        gateway_url=self.settings.gateway_url,
                        proxy_url=self.settings.public_proxy_url,
                        harness=turn.harness.value,
                        model=turn.model,
                        model_start_timeout_seconds=self.settings.runtime_model_start_timeout_seconds,
                        event_idle_timeout_seconds=self.settings.runtime_event_idle_timeout_seconds,
                    )
                    if thread.checkpoint_ref and not sandbox.reused:
                        archive = self.objects.get(thread.checkpoint_ref, maximum=self.settings.max_checkpoint_bytes)
                        try:
                            await self.runtime.restore(sandbox, archive)
                        except httpx.HTTPStatusError as exc:
                            raise CheckpointRestoreError("Runtime rejected the saved workspace checkpoint") from exc
                    await self._register_project_attachments(thread.id, turn.id, turn.attachments)
                    await self.runtime.reset_project_files(sandbox)
                    await self._materialize_project_files(sandbox,
                        await self.store.current_project_files(thread.id))
                    await self._materialize_attachments(sandbox, turn.attachments)
                    await self._state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id)
                self.active_sandboxes[turn.id] = sandbox
                message, attachments, native_thread_id = turn.message, turn.attachments, thread.native_thread_id
                result, current_input_id = None, None
                while True:
                    if recovered_result is not None:
                        result, recovered_result = recovered_result, None
                    else:
                        result = None
                        input_id = current_input_id or turn.id
                        execution_id = turn.id + ":" + input_id
                        await self.store.start_runtime_execution(
                            turn.id, execution_id, self.instance_id, generation,
                        )
                        async for value in self.runtime.run(sandbox, message=message, attachments=attachments,
                            native_thread_id=native_thread_id, reasoning_effort=turn.reasoning_effort,
                            execution_id=execution_id, input_id=input_id, after_sequence=0):
                            if isinstance(value, RuntimeResult):
                                result = value
                            else:
                                await self._record_runtime_event(thread.id, turn.id, value, execution_id)
                            if value.runtime_sequence:
                                await self.store.update_runtime_sequence(
                                    turn.id, value.runtime_sequence, self.instance_id, generation,
                                )
                    if result is None:
                        raise RuntimeError("Runtime stream ended without a result")
                    if result.status in ("completed", "waiting_user"):
                        async with self.input_locks[turn.id]:
                            await self._state(turn.id, TurnState.DRAINING_INPUTS, sandbox_id=sandbox.id)
                            await self.store.apply_delivered_inputs(turn.id)
                    if current_input_id:
                        await self.store.set_input_status(current_input_id,
                            "applied" if result.status in ("completed", "waiting_user") else "failed")
                    for input_id in result.input_ids:
                        if input_id != turn.id:
                            await self.store.set_input_status(input_id,
                                "applied" if result.status in ("completed", "waiting_user") else "failed")
                    if result.status not in ("completed", "waiting_user"):
                        break
                    native_thread_id = result.native_thread_id or native_thread_id
                    pending = await self.store.next_pending_input(turn.id)
                    if not pending:
                        await self.store.apply_delivered_inputs(turn.id)
                        break
                    try:
                        await self.store.complete_agent_item(turn.id, "completed")
                        project_files = await self._register_project_attachments(
                            thread.id, turn.id, pending.attachments,
                        )
                        await self._materialize_project_files(sandbox, project_files)
                        await self._materialize_attachments(sandbox, pending.attachments)
                        message, attachments = pending.message, pending.attachments
                        current_input_id = pending.id
                        await self._state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id)
                    except Exception:
                        await self.store.set_input_status(pending.id, "failed")
                        raise
                if result.status in ("completed", "waiting_user"):
                    await self._state(turn.id, TurnState.CHECKPOINTING, sandbox_id=sandbox.id)
                    archive = await self.runtime.checkpoint(sandbox)
                    if len(archive) > self.settings.max_checkpoint_bytes:
                        raise ValueError("Checkpoint exceeds byte limit")
                    checkpoint_ref, _ = self.objects.put(archive)
                    await self.store.update_thread_checkpoint(thread.id, checkpoint_ref, result.native_thread_id)
                    total = 0
                    for item in await self.runtime.artifacts(sandbox):
                        path = item["path"]
                        data = await self.runtime.artifact(sandbox, path)
                        total += len(data)
                        if total > self.settings.max_artifact_bytes:
                            raise ValueError("Artifacts exceed Turn byte limit")
                        object_ref, digest = self.objects.put(data)
                        artifact = await self.store.artifact_by_identity(turn.id, path, digest)
                        if artifact is None:
                            artifact = await self.store.add_artifact(turn.id, path=path, sha256=digest, size=len(data), mime=item.get("mime") or mimetypes.guess_type(path)[0] or "application/octet-stream", object_ref=object_ref)
                            await self.store.append_item(thread.id, turn.id, ItemType.ARTIFACT, status="completed", summary=path,
                                payload={"artifact_id": artifact.id, "mime": artifact.mime, "size": artifact.size})
                        await self.store.register_project_file(thread.id,
                            path="artifacts/" + path, source_type="artifact", source_turn_id=turn.id,
                            sha256=digest, size=len(data), mime=artifact.mime, object_ref=object_ref)
                    final_state = TurnState.COMPLETED if result.status == "completed" else TurnState.WAITING_USER
                    error_code = ""
                    error_detail = ""
                elif result.status == "cancelled":
                    final_state = TurnState.CANCELLED
                    error_code = "cancelled"
                    error_detail = result.error_detail
                else:
                    final_state = TurnState.FAILED
                    error_code = result.error_code or "harness_failed"
                    error_detail = result.error_detail
        except asyncio.CancelledError:
            if self.closing:
                abandoned = True
                return
            final_state = TurnState.CANCELLED
            error_code = "cancelled"
            error_detail = "Turn was cancelled"
            if sandbox:
                try:
                    await self.runtime.cancel(sandbox)
                except Exception:
                    pass
        except Exception as exc:
            error_code = type(exc).__name__
            error_detail = str(exc)
            await self.store.append_event(turn.id, EventKind.ERROR, {"code": error_code, "detail": error_detail[:1000]})
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            self.heartbeats.pop(turn_id, None)
            if abandoned:
                await self.store.release_turn_lease(turn_id, self.instance_id, generation)
                self.leases.pop(turn_id, None)
                self.tasks.pop(turn.id, None)
                return
            try:
                await self.store.complete_agent_item(turn.id, "completed" if final_state in {TurnState.COMPLETED, TurnState.WAITING_USER} else "interrupted")
                await self.store.begin_turn_cleanup(turn.id, final_state, error_code=error_code,
                    error_detail=error_detail, owner=self.instance_id, generation=generation)
                await self.store.append_event(turn.id, EventKind.STATE, {"state": TurnState.CLEANING.value})
                if sandbox:
                    revoked = False
                    capability_nonce = claims.nonce if 'claims' in locals() else turn.capability_nonce
                    if capability_nonce:
                        for attempt in range(3):
                            try:
                                async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
                                    response = await client.post(
                                        self.settings.gateway_url.rstrip('/') + '/internal/revoke/' + capability_nonce,
                                        headers={"X-Sandbox-Harness-Control": self.settings.gateway_control_key},
                                    )
                                    response.raise_for_status()
                                revoked = True
                                break
                            except Exception as exc:
                                if attempt == 2:
                                    await self.store.append_event(turn.id, EventKind.WARNING, {
                                        "code": "capability_revoke_failed",
                                        "detail": type(exc).__name__,
                                        "action": "delete_sandbox",
                                    })
                                else:
                                    await asyncio.sleep(0.2 * (attempt + 1))
                    keep_attached = (
                        revoked
                        and final_state in {TurnState.COMPLETED, TurnState.WAITING_USER}
                        and self.settings.session_keepalive_seconds > 0
                    )
                    await self.pool.release(sandbox, keep_attached=keep_attached,
                        keepalive_seconds=self.settings.session_keepalive_seconds)
            except Exception as exc:
                final_state = TurnState.FAILED
                error_code = "cleanup_failed"
                error_detail = type(exc).__name__ + ": " + str(exc)
                await self.store.append_event(turn.id, EventKind.ERROR, {
                    "code": error_code,
                    "detail": error_detail[:1000],
                })
            finally:
                if final_state == TurnState.FAILED:
                    public_detail = {
                        "HTTPStatusError": "沙箱运行环境暂时不可用，请重新发送消息。",
                        "CheckpointRestoreError": "会话工作区恢复失败，请重新发送消息。",
                        "ConnectError": "沙箱运行环境暂时无法连接，请重新发送消息。",
                        "harness_exit": "Harness 执行失败，请重试或切换模型。",
                        "model_response_timeout": "模型服务在规定时间内没有返回，请稍后重试或切换模型。",
                        "harness_idle_timeout": "Harness 长时间没有产生新进展，本轮已安全停止。",
                        "cleanup_failed": "运行结果保存或环境清理失败，请重试。",
                    }.get(error_code, "本轮执行失败，请重试；如果持续失败，可以切换模型。")
                    item = await self.store.append_item(thread.id, turn.id, ItemType.ERROR, status="failed",
                        summary=public_detail, payload={"code": error_code, "detail": public_detail})
                    await self.store.append_event(turn.id, EventKind.ERROR,
                        {"code": error_code, "detail": public_detail}, item_id=item.id)
                await self._state(turn.id, final_state, sandbox_id=sandbox.id if sandbox else "", error_code=error_code, error_detail=error_detail)
                self.active_sandboxes.pop(turn.id, None)
                self.input_locks.pop(turn.id, None)
                next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
                if next_turn:
                    self._schedule(next_turn.id)
                await self.store.release_turn_lease(turn.id, self.instance_id, generation)
                self.leases.pop(turn.id, None)
                self.tasks.pop(turn.id, None)
