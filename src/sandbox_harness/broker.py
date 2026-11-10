from __future__ import annotations

import asyncio
import logging
import mimetypes
from collections import defaultdict
from contextlib import asynccontextmanager
import httpx

from .capability import CapabilityCodec
from .catalog import HarnessCatalog
from .config import Settings
from .domain import EventKind, Harness, ItemType, RuntimeResult, TurnCreate, TurnState, TurnView
from .objects import ObjectStore
from .errors import public_failure
from .pool import SandboxPool
from .runtime import RuntimeClient, RuntimeStreamInterrupted
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
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.lock_users: dict[str, int] = defaultdict(int)
        self.lock_guard = asyncio.Lock()
        self.input_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.input_lock_users: dict[str, int] = defaultdict(int)
        self.input_lock_guard = asyncio.Lock()
        self.active_sandboxes = {}
        self.instance_id = "broker-" + __import__("uuid").uuid4().hex
        self.leases: dict[str, int] = {}
        self.heartbeats: dict[str, asyncio.Task] = {}
        self.recovery_task: asyncio.Task | None = None
        self.reaper_task: asyncio.Task | None = None
        self.closing = False

    @asynccontextmanager
    async def thread_lock(self, thread_id: str):
        async with self.lock_guard:
            lock = self.locks[thread_id]
            self.lock_users[thread_id] += 1
        try:
            async with lock:
                yield
        finally:
            async with self.lock_guard:
                self.lock_users[thread_id] -= 1
                if self.lock_users[thread_id] == 0:
                    self.lock_users.pop(thread_id, None)
                    self.locks.pop(thread_id, None)

    @asynccontextmanager
    async def input_lock(self, turn_id: str):
        async with self.input_lock_guard:
            lock = self.input_locks[turn_id]
            self.input_lock_users[turn_id] += 1
        try:
            async with lock:
                yield
        finally:
            async with self.input_lock_guard:
                self.input_lock_users[turn_id] -= 1
                if self.input_lock_users[turn_id] == 0:
                    self.input_lock_users.pop(turn_id, None)
                    self.input_locks.pop(turn_id, None)

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
        await self.pool.retry_deletions()
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
        heartbeat = asyncio.create_task(self._heartbeat(turn.id, claimed.recovery_generation))
        try:
            if claimed.cancel_requested and claimed.state != TurnState.CLEANING:
                self._schedule(turn.id); return
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
                self.leases.pop(turn.id, None)
                next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
                if next_turn:
                    self._schedule(next_turn.id)
                return
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
            next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
            if next_turn:
                self._schedule(next_turn.id)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def try_deliver_input(self, value) -> bool:
        async with self.input_lock(value.turn_id):
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
                # An HTTP acceptance is not a completion receipt. The result's
                # explicit input_ids determine whether this input was applied.
                if current and current.state in {TurnState.RUNNING, TurnState.AWAITING_USER}:
                    await self.store.set_input_status(value.id, "delivered", delivery_mode)
            return bool(delivery_mode)

    async def respond_interaction(self, interaction_id: str, owner_id: str, value) -> object:
        response = {"action": value.action, "answers": value.answers}
        interaction, claimed = await self.store.claim_interaction_response(
            interaction_id, owner_id, value.idempotency_key, response,
            claim_owner=self.instance_id,
        )
        if not claimed:
            return interaction
        turn = await self.store.get_turn(interaction.turn_id)
        sandbox = self.active_sandboxes.get(interaction.turn_id)
        if (not turn or not sandbox or turn.runtime_execution_id != interaction.runtime_execution_id):
            await self.store.finish_interaction_response(
                interaction_id, delivered=False, claim_owner=self.instance_id)
            raise RuntimeError("interaction_runtime_unavailable")
        if interaction.kind == "approval" and value.action not in {"approve", "reject"}:
            await self.store.finish_interaction_response(
                interaction_id, delivered=False, claim_owner=self.instance_id)
            raise ValueError("Approval requires approve or reject")
        if interaction.kind == "question" and (value.action != "answer" or not value.answers):
            await self.store.finish_interaction_response(
                interaction_id, delivered=False, claim_owner=self.instance_id)
            raise ValueError("Question requires at least one answer")
        try:
            delivered = await self.runtime.respond_interaction(
                sandbox, interaction.runtime_interaction_id, response,
            )
        except Exception as exc:
            await self.store.finish_interaction_response(
                interaction_id, delivered=False, claim_owner=self.instance_id)
            raise RuntimeError("interaction_delivery_failed") from exc
        current = await self.store.finish_interaction_response(
            interaction_id, delivered=delivered, claim_owner=self.instance_id)
        if not delivered:
            raise RuntimeError("interaction_transport_unsupported")
        await self.store.finish_interaction_item(interaction.turn_id, interaction.id, response)
        if await self.store.resume_interaction_turn(interaction.turn_id, interaction.runtime_execution_id):
            await self.store.append_event(interaction.turn_id, EventKind.STATE, {"state": TurnState.RUNNING.value})
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

    async def delete_thread(self, thread_id: str, owner_id: str) -> None:
        async with self.thread_lock(thread_id):
            thread = await self.store.get_thread(thread_id, owner_id)
            if not thread:
                raise KeyError("thread_not_found")
            turns = await self.store.list_turns(thread_id, owner_id)
            if any(turn.id in self.tasks or turn.id in self.active_sandboxes for turn in turns):
                raise RuntimeError("active_thread_cannot_be_deleted")
            await self.pool.discard_thread(thread_id)
            await self.store.delete_thread(thread_id, owner_id)

    async def cancel(self, turn_id: str) -> None:
        turn = await self.store.request_turn_cancel(turn_id)
        await self.store.append_event(turn_id, EventKind.WARNING, {"code": "cancel_requested"})
        task = self.tasks.get(turn_id)
        if task and not task.cancelling() and turn.state != TurnState.CLEANING:
            task.cancel()
        if turn.state == TurnState.CANCELLED:
            await self.store.append_event(turn_id, EventKind.STATE, {"state": turn.state.value})
            next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
            if next_turn:
                self._schedule(next_turn.id)

    async def _state(self, turn_id: str, state: TurnState, **kwargs) -> TurnView:
        generation = self.leases.get(turn_id)
        turn = await self.store.set_turn_state(turn_id, state, **kwargs,
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)
        await self.store.append_event(turn_id, EventKind.STATE, {"state": turn.state.value},
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)
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
            turn = await self.store.get_turn(turn_id)
            task = self.tasks.get(turn_id)
            if (turn and turn.cancel_requested and turn.state != TurnState.CLEANING
                    and task and not task.cancelling()):
                task.cancel()

    async def _record_runtime_event(self, thread_id: str, turn_id: str, value: RuntimeEvent,
                                    execution_id: str = "") -> None:
        generation = self.leases.get(turn_id)
        await self.store.persist_runtime_event(thread_id, turn_id, value, execution_id,
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)

    async def _materialize_attachments(self, sandbox, attachments) -> None:
        for attachment in attachments:
            data = await asyncio.to_thread(self.objects.get, attachment.object_ref, maximum=self.settings.max_attachment_bytes)
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
            data = await asyncio.to_thread(self.objects.get, value.object_ref, maximum=self.settings.max_artifact_bytes)
            if len(data) != value.size:
                raise ValueError("Project file size does not match its immutable record")
            await self.runtime.put_project_file(sandbox, value.path, data, value.mime)

    async def _save_workspace(self, sandbox, thread_id: str, turn_id: str,
                              native_thread_id: str, *, partial: bool = False) -> None:
        generation = self.leases.get(turn_id)
        archive = await self.runtime.checkpoint(sandbox)
        if len(archive) > self.settings.max_checkpoint_bytes:
            raise ValueError("Checkpoint exceeds byte limit")
        checkpoint_ref, _ = await asyncio.to_thread(self.objects.put, archive)
        await self.store.update_thread_checkpoint(thread_id, checkpoint_ref, native_thread_id,
            turn_id=turn_id, lease_owner=self.instance_id, recovery_generation=generation)
        total = 0
        for item in await self.runtime.artifacts(sandbox):
            path = item["path"]
            data = await self.runtime.artifact(sandbox, path)
            total += len(data)
            if total > self.settings.max_artifact_bytes:
                raise ValueError("Artifacts exceed Turn byte limit")
            object_ref, digest = await asyncio.to_thread(self.objects.put, data)
            artifact = await self.store.artifact_by_identity(turn_id, path, digest)
            if artifact is None:
                artifact = await self.store.add_artifact(turn_id, path=path, sha256=digest, size=len(data), mime=item.get("mime") or mimetypes.guess_type(path)[0] or "application/octet-stream", object_ref=object_ref,
                    lease_owner=self.instance_id, recovery_generation=generation)
                await self.store.append_item(thread_id, turn_id, ItemType.ARTIFACT, status="completed", summary=path,
                    payload={"artifact_id": artifact.id, "mime": artifact.mime, "size": artifact.size, "partial": partial},
                    lease_owner=self.instance_id, recovery_generation=generation)
            await self.store.register_project_file(thread_id,
                path="artifacts/" + path, source_type="artifact", source_turn_id=turn_id,
                sha256=digest, size=len(data), mime=artifact.mime, object_ref=object_ref,
                lease_owner=self.instance_id, recovery_generation=generation)

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
        extra_sandboxes = []
        final_state = TurnState.FAILED
        error_code = "turn_failed"
        error_detail = "Turn failed before the Harness returned a result"
        abandoned = False
        lost_lease = False
        try:
            async with self.thread_lock(thread.id):
                if turn.cancel_requested:
                    matches = await self.pool.backend.find_by_turn(turn.id)
                    if matches:
                        sandbox = matches[0]
                        # Stop every owned process, then revoke before deleting any match.
                        extra_sandboxes = matches[1:]
                        for extra in extra_sandboxes:
                            await self.runtime.cancel(extra)
                    raise asyncio.CancelledError()
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
                        archive = await asyncio.to_thread(self.objects.get, thread.checkpoint_ref, maximum=self.settings.max_checkpoint_bytes)
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
                            turn.id, execution_id, self.instance_id, generation, input_id=input_id,
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
                        raise RuntimeStreamInterrupted("Runtime stream ended without a result")
                    if result.status in ("completed", "waiting_user"):
                        async with self.input_lock(turn.id):
                            await self._state(turn.id, TurnState.DRAINING_INPUTS, sandbox_id=sandbox.id)
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
                    await self._save_workspace(sandbox, thread.id, turn.id, result.native_thread_id)
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
                    # Preserve inspectable work even if the next model request failed.
                    # A failed Turn remains failed; partial artifacts are labelled as such.
                    try:
                        await self._state(turn.id, TurnState.CHECKPOINTING, sandbox_id=sandbox.id)
                        await self._save_workspace(sandbox, thread.id, turn.id,
                            result.native_thread_id or native_thread_id, partial=True)
                    except Exception:
                        logger.exception("Failed to preserve partial workspace turn=%s", turn.id)
                        await self.store.append_event(turn.id, EventKind.WARNING,
                            {"code": "partial_workspace_save_failed", "detail": "本轮中间文件未能完整保存。"})
        except asyncio.CancelledError:
            if self.closing:
                abandoned = True
                return
            final_state = TurnState.CANCELLED
            error_code = "cancelled"
            error_detail = "Turn was cancelled"
            lost_lease = not await self.store.owns_turn_lease(turn.id, self.instance_id, generation)
            if sandbox and not lost_lease:
                try:
                    await self.runtime.cancel(sandbox)
                except Exception:
                    pass
        except Exception as exc:
            error_code = type(exc).__name__
            error_detail = str(exc)
            logger.exception("Turn execution failed turn=%s execution=%s", turn.id, turn.runtime_execution_id)
            try:
                await self.store.append_event(turn.id, EventKind.ERROR,
                    {"code": error_code, "detail": error_detail[:1000]},
                    lease_owner=self.instance_id, recovery_generation=generation)
            except RuntimeError as fence_error:
                if str(fence_error) != "stale_turn_lease":
                    raise
                lost_lease = True
        finally:
            try:
                if abandoned:
                    await self.store.release_turn_lease(turn_id, self.instance_id, generation)
                    self.leases.pop(turn_id, None)
                    self.tasks.pop(turn.id, None)
                    return
                try:
                    cleaning = await self.store.begin_turn_cleanup(turn.id, final_state, error_code=error_code,
                        error_detail=error_detail, owner=self.instance_id, generation=generation)
                    final_state = TurnState(cleaning.pending_final_state)
                    error_code, error_detail = cleaning.error_code, cleaning.error_detail
                    await self.store.fail_unapplied_inputs(turn.id,
                        lease_owner=self.instance_id, recovery_generation=generation)
                    await self.store.complete_agent_item(turn.id,
                        "completed" if final_state in {TurnState.COMPLETED, TurnState.WAITING_USER} else "interrupted",
                        lease_owner=self.instance_id, recovery_generation=generation)
                    await self.store.append_event(turn.id, EventKind.STATE, {"state": TurnState.CLEANING.value},
                        lease_owner=self.instance_id, recovery_generation=generation)
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
                                        }, lease_owner=self.instance_id, recovery_generation=generation)
                                    else:
                                        await asyncio.sleep(0.2 * (attempt + 1))
                        keep_attached = (
                            revoked
                            and final_state in {TurnState.COMPLETED, TurnState.WAITING_USER}
                            and self.settings.session_keepalive_seconds > 0
                        )
                        for extra in extra_sandboxes:
                            await self.pool.backend.delete(extra)
                        await self.pool.release(sandbox, keep_attached=keep_attached,
                            keepalive_seconds=self.settings.session_keepalive_seconds)
                except Exception as exc:
                    if isinstance(exc, RuntimeError) and str(exc) == "stale_turn_lease":
                        lost_lease = True
                    else:
                        final_state = TurnState.FAILED
                        error_code = "cleanup_failed"
                        error_detail = type(exc).__name__ + ": " + str(exc)
                        try:
                            await self.store.append_event(turn.id, EventKind.ERROR, {
                                "code": error_code,
                                "detail": error_detail[:1000],
                            }, lease_owner=self.instance_id, recovery_generation=generation)
                        except RuntimeError as fence_error:
                            if str(fence_error) != "stale_turn_lease":
                                raise
                            lost_lease = True
                finally:
                    if lost_lease:
                        if sandbox:
                            await self.pool.detach(sandbox)
                        self.active_sandboxes.pop(turn.id, None)
                        self.leases.pop(turn.id, None)
                        self.tasks.pop(turn.id, None)
                        return
                    if final_state == TurnState.FAILED:
                        error_code, public_detail = public_failure(error_code, error_detail)
                        item = await self.store.append_item(thread.id, turn.id, ItemType.ERROR, status="failed",
                            summary=public_detail, payload={"code": error_code, "detail": public_detail},
                            lease_owner=self.instance_id, recovery_generation=generation)
                        await self.store.append_event(turn.id, EventKind.ERROR,
                            {"code": error_code, "detail": public_detail}, item_id=item.id,
                            lease_owner=self.instance_id, recovery_generation=generation)
                    await self._state(turn.id, final_state, sandbox_id=sandbox.id if sandbox else "", error_code=error_code, error_detail=error_detail)
                    self.active_sandboxes.pop(turn.id, None)
                    next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
                    if next_turn:
                        self._schedule(next_turn.id)
                    await self.store.release_turn_lease(turn.id, self.instance_id, generation)
                    self.leases.pop(turn.id, None)
                    self.tasks.pop(turn.id, None)
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                self.heartbeats.pop(turn_id, None)
                self.active_sandboxes.pop(turn_id, None)
                self.leases.pop(turn_id, None)
                self.tasks.pop(turn_id, None)
