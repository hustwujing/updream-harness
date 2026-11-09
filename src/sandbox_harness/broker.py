from __future__ import annotations

import asyncio
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

    async def try_deliver_input(self, value) -> bool:
        async with self.input_locks[value.turn_id]:
            sandbox = self.active_sandboxes.get(value.turn_id)
            turn = await self.store.get_turn(value.turn_id)
            if not sandbox or not turn or turn.state != TurnState.RUNNING:
                return False
            await self._materialize_attachments(sandbox, value.attachments)
            delivery_mode = await self.runtime.append_input(sandbox, message=value.message,
                attachments=value.attachments, reasoning_effort=turn.reasoning_effort)
            if delivery_mode:
                current = await self.store.get_turn(value.turn_id)
                await self.store.set_input_status(value.id,
                    "delivered" if current and current.state == TurnState.RUNNING else "applied", delivery_mode)
            return bool(delivery_mode)

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
            self.tasks[turn.id] = asyncio.create_task(self._process(turn.id), name="turn-" + turn.id)
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
        turn = await self.store.set_turn_state(turn_id, state, **kwargs)
        await self.store.append_event(turn_id, EventKind.STATE, {"state": state.value})
        return turn

    async def _record_runtime_event(self, thread_id: str, turn_id: str, value: RuntimeEvent) -> None:
        if value.kind == EventKind.TOOL_OUTPUT:
            await self.store.append_event(turn_id, value.kind, value.payload)
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
        await self.store.append_event(turn_id, value.kind, value.payload, item_id=item.id)
        if value.payload.get("input_completed"):
            await self.store.complete_agent_item(turn_id, "completed")

    async def _materialize_attachments(self, sandbox, attachments) -> None:
        for attachment in attachments:
            data = self.objects.get(attachment.object_ref, maximum=self.settings.max_attachment_bytes)
            if len(data) != attachment.size:
                raise ValueError("Attachment size does not match its immutable record")
            await self.runtime.put_attachment(sandbox, attachment.name, data, attachment.mime)

    async def _process(self, turn_id: str) -> None:
        turn = await self.store.get_turn(turn_id)
        if not turn:
            return
        thread = await self.store.get_thread(turn.thread_id)
        if not thread:
            return
        sandbox = None
        final_state = TurnState.FAILED
        error_code = "turn_failed"
        error_detail = "Turn failed before the Harness returned a result"
        try:
            async with self.locks[thread.id]:
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
                await self._state(turn.id, TurnState.STARTING, sandbox_id=sandbox.id)
                await self.runtime.inject(
                    sandbox,
                    capability_token=token,
                    gateway_url=self.settings.gateway_url,
                    proxy_url=self.settings.public_proxy_url,
                    harness=turn.harness.value,
                    model=turn.model,
                )
                if thread.checkpoint_ref:
                    archive = self.objects.get(thread.checkpoint_ref, maximum=self.settings.max_checkpoint_bytes)
                    await self.runtime.restore(sandbox, archive)
                await self._materialize_attachments(sandbox, turn.attachments)
                await self._state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id)
                self.active_sandboxes[turn.id] = sandbox
                message, attachments, native_thread_id = turn.message, turn.attachments, thread.native_thread_id
                result, current_input_id = None, None
                while True:
                    result = None
                    async for value in self.runtime.run(sandbox, message=message, attachments=attachments,
                        native_thread_id=native_thread_id, reasoning_effort=turn.reasoning_effort):
                        if isinstance(value, RuntimeResult):
                            result = value
                        else:
                            await self._record_runtime_event(thread.id, turn.id, value)
                    if result is None:
                        raise RuntimeError("Runtime stream ended without a result")
                    if result.status in ("completed", "waiting_user"):
                        await self._state(turn.id, TurnState.DRAINING_INPUTS, sandbox_id=sandbox.id)
                        await self.store.apply_delivered_inputs(turn.id)
                    if current_input_id:
                        await self.store.set_input_status(current_input_id,
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
                        artifact = await self.store.add_artifact(turn.id, path=path, sha256=digest, size=len(data), mime=item.get("mime") or mimetypes.guess_type(path)[0] or "application/octet-stream", object_ref=object_ref)
                        await self.store.append_item(thread.id, turn.id, ItemType.ARTIFACT, status="completed", summary=path,
                            payload={"artifact_id": artifact.id, "mime": artifact.mime, "size": artifact.size})
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
            try:
                await self.store.complete_agent_item(turn.id, "completed" if final_state in {TurnState.COMPLETED, TurnState.WAITING_USER} else "interrupted")
                if sandbox:
                    revoked = False
                    if 'claims' in locals():
                        for attempt in range(3):
                            try:
                                async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
                                    response = await client.post(
                                        self.settings.gateway_url.rstrip('/') + '/internal/revoke/' + claims.nonce,
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
                        "ConnectError": "沙箱运行环境暂时无法连接，请重新发送消息。",
                        "harness_exit": "Harness 执行失败，请重试或切换模型。",
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
                    self.tasks[next_turn.id] = asyncio.create_task(self._process(next_turn.id), name="turn-" + next_turn.id)
                self.tasks.pop(turn.id, None)
