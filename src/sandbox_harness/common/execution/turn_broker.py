from __future__ import annotations

from sandbox_harness.common.observability.observability import (observed, event, enrich, carrier, add_link, business_metric, telemetry, failure_origin)
from sandbox_harness.common.observability.http import (observed_async_client)

import asyncio
import hashlib
import json
import logging
import mimetypes
from pathlib import PurePosixPath
from collections import defaultdict,OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
import httpx

from sandbox_harness.common.security.capability import (CapabilityCodec)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (EventKind, Harness, ItemType, RuntimeResult, TurnCreate, TurnState, TurnView, now)
from sandbox_harness.common.storage.objects import (ObjectStore, object_chunks)
from sandbox_harness.common.contracts.errors import (public_failure)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient, RuntimeStreamInterrupted)
from sandbox_harness.common.sandbox.sandbox import (SandboxNotFound)
from sandbox_harness.common.sandbox.sandbox_factory import (SandboxProfileResolver)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxHandle)
from sandbox_harness.common.sites.site_bundle import (build_site_archive_from_paths, normalize_site_slug)


ROUTES = {
    Harness.CODEX: ["/v1/models/codex", "/v1/providers", "/v1/artifacts", "/v1/media", "/v1/sites", "/v1/job-group-drafts", "/v1/job-groups", "/v1/jobs", "/v1/job-artifacts", "/v1/execution/catalog", "/v1/operations", "/v1/reviews", "/v1/public-http"],
    Harness.CLAUDE: ["/v1/models/claude", "/v1/providers", "/v1/artifacts", "/v1/media", "/v1/sites", "/v1/job-group-drafts", "/v1/job-groups", "/v1/jobs", "/v1/job-artifacts", "/v1/execution/catalog", "/v1/operations", "/v1/reviews", "/v1/public-http"],
    Harness.DEEPSEEK: ["/v1/models/deepseek", "/v1/providers", "/v1/artifacts", "/v1/media", "/v1/sites", "/v1/job-group-drafts", "/v1/job-groups", "/v1/jobs", "/v1/job-artifacts", "/v1/execution/catalog", "/v1/operations", "/v1/reviews", "/v1/public-http"],
}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeDiagnosticState:
    instance_id:str
    cursor:int
    counters:dict[str,int]


async def reconcile_native_model_error(store,turn_id:str,error_code:str,error_detail:str)->tuple[str,str]:
    if error_code not in {'model_rate_limited','harness_failed','other'}:return error_code,error_detail
    observed=await store.latest_model_activity_error(turn_id)
    if observed=='model_token_budget_exhausted' and error_code in {'model_rate_limited','harness_failed'}:
        return observed,observed
    if observed=='model_request_too_large' and error_code in {'harness_failed','other'}:
        return observed,observed
    return error_code,error_detail


class CheckpointRestoreError(RuntimeError):
    pass


class Broker:
    def __init__(self, settings: Settings, store, objects: ObjectStore, pool: SandboxPool,
                 runtime: RuntimeClient, catalog: HarnessCatalog, *, execution_enabled: bool = True):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.pool = pool
        self.runtime = runtime
        self.codec = CapabilityCodec(settings.gateway_signing_key)
        self.catalog = catalog
        self.execution_enabled = execution_enabled
        self.tasks: dict[str, asyncio.Task] = {}
        self.reconciliation_tasks: dict[str, asyncio.Task] = {}
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.lock_users: dict[str, int] = defaultdict(int)
        self.lock_guard = asyncio.Lock()
        self.input_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.input_lock_users: dict[str, int] = defaultdict(int)
        self.input_lock_guard = asyncio.Lock()
        self.active_sandboxes = {}
        self._runtime_diagnostics:OrderedDict[str,RuntimeDiagnosticState]=OrderedDict()
        self.instance_id = "broker-" + __import__("uuid").uuid4().hex
        self.leases: dict[str, int] = {}
        self.heartbeats: dict[str, asyncio.Task] = {}
        self.cancellation_handlers: set[str] = set()
        self.recovery_task: asyncio.Task | None = None
        self.reaper_task: asyncio.Task | None = None
        self.closing = False

    def _sandbox_profile_for_turn(self,turn:TurnView):
        current=self.pool.profile_resolver
        handle=self._turn_handle(turn)
        resolver=SandboxProfileResolver(provider=self.pool.backend.provider,
            runtime_contract=current.runtime_contract,
            runtime_bundle_digest=turn.runtime_bundle_digest or current.runtime_bundle_digest,
            runtime_code_bundle_digest=(turn.runtime_code_bundle_digest
                or current.runtime_code_bundle_digest))
        return resolver.resolve(turn.harness,
            turn.runtime_image_digest or self.settings.image_for(turn.harness.value),
            revision=handle.revision)

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
        if self.closing or not self.execution_enabled:return
        task = self.tasks.get(turn_id)
        if task and not task.done():
            return
        self.tasks[turn_id] = asyncio.create_task(self._process(turn_id), name="turn-" + turn_id)

    def status(self) -> dict:
        return {
            "instance_id": self.instance_id,
            "execution_enabled": self.execution_enabled,
            "active_turns": len([task for task in self.tasks.values() if not task.done()]),
            "active_reconciliations": len([task for task in self.reconciliation_tasks.values()
                if not task.done()]),
            "owned_leases": len(self.leases),
            "recovery_scanner": bool(self.recovery_task and not self.recovery_task.done()),
            "orphan_reaper": bool(self.reaper_task and not self.reaper_task.done()),
        }

    async def start(self, *, start_reaper: bool = True) -> None:
        if not self.execution_enabled:
            raise RuntimeError("request_only_broker_cannot_start")
        validate=getattr(self.pool.backend,"validate_active_providers",None)
        if validate:validate(await self.store.active_sandbox_providers())
        await self.reconcile()
        await self.deliver_pending_interaction_responses()
        try:
            await self.cleanup_startup_orphans()
        except Exception:
            logger.exception("Startup orphan reconciliation failed")
        self.recovery_task = asyncio.create_task(self._recovery_loop(), name="turn-recovery-loop")
        if start_reaper:
            self.reaper_task = asyncio.create_task(self._reaper_loop(), name="sandbox-orphan-reaper")

    async def cleanup_startup_orphans(self) -> None:
        await self.pool.retry_deletions()
        active_keys=({self.pool.sandbox_key(sandbox) for sandbox in self.active_sandboxes.values()}
            | await self.pool.managed_sandbox_keys() | await self.store.active_sandbox_keys())
        allocations=getattr(self.store,'active_sandbox_allocations',None)
        active_allocations=await allocations() if allocations else set()
        for sandbox in await self.pool.list_owned():
            allocation=(sandbox.handle.provider,sandbox.handle.allocation_id)
            if self.pool.sandbox_key(sandbox) not in active_keys and allocation not in active_allocations:
                event("sandbox.orphan.delete",level=logging.WARNING,sandbox_id=sandbox.id)
                await self.pool.backend.delete(sandbox)

    async def _turn_inventory(self,turn)->list:
        if not turn.sandbox_id:return []
        handle=self._turn_handle(turn)
        try:return [await self.pool.backend.get(handle)]
        except SandboxNotFound:return []

    @staticmethod
    def _turn_handle(turn)->SandboxHandle:
        if not turn.sandbox_handle_json:
            raise RuntimeError("nonterminal_turn_sandbox_handle_missing")
        try:return SandboxHandle.from_mapping(json.loads(turn.sandbox_handle_json))
        except (TypeError,ValueError) as exc:
            raise RuntimeError("nonterminal_turn_sandbox_handle_invalid") from exc

    @staticmethod
    def _handle_json(sandbox)->str:
        if sandbox.handle is None:raise RuntimeError("allocated_sandbox_handle_missing")
        return json.dumps(sandbox.handle.to_mapping(),sort_keys=True,separators=(",",":"))

    def _revoke_headers(self, turn, sandbox_id="") -> dict:
        return {**carrier(),"X-Sandbox-Harness-Control":self.settings.gateway_control_key,
            "X-Updream-Turn-ID":turn.id,"X-Updream-Thread-ID":turn.thread_id,
            "X-Updream-Sandbox-ID":sandbox_id or turn.sandbox_id}

    async def close(self) -> set[tuple[str,str]]:
        preserve_sandbox_keys={self.pool.sandbox_key(sandbox)
            for sandbox in self.active_sandboxes.values()}
        self.closing = True
        if self.recovery_task:
            self.recovery_task.cancel()
        if self.reaper_task:
            self.reaper_task.cancel()
        for turn_id,task in list(self.tasks.items()):
            # A pause is already represented by task cancellation. Cancelling it a
            # second time interrupts the checkpoint transaction during shutdown.
            if not task.cancelling() and turn_id not in self.cancellation_handlers:
                task.cancel()
        for task in list(self.reconciliation_tasks.values()):
            if not task.cancelling():task.cancel()
        values = ([task for task in (self.recovery_task, self.reaper_task) if task]
            + list(self.tasks.values()) + list(self.reconciliation_tasks.values()))
        if values:
            await asyncio.gather(*values, return_exceptions=True)
        heartbeats=list(self.heartbeats.values())
        for heartbeat in heartbeats:heartbeat.cancel()
        if heartbeats:await asyncio.gather(*heartbeats,return_exceptions=True)
        self.heartbeats.clear()
        for turn_id, generation in list(self.leases.items()):
            await self._release_turn_lease_safely(turn_id,generation)
        return preserve_sandbox_keys

    async def _recovery_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.recovery_scan_seconds)
            try:
                await self.reconcile()
                await self.deliver_pending_interaction_responses()
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
        pending=[]
        for turn in await self.store.recoverable_turns():
            if turn.id in self.tasks or turn.id in self.reconciliation_tasks:
                continue
            async def run(value=turn):
                try:
                    await self._reconcile_turn(value)
                except asyncio.CancelledError:
                    generation=self.leases.get(value.id)
                    if (generation is not None and await self.store.owns_turn_lease(
                            value.id,self.instance_id,generation)):
                        await asyncio.shield(self.store.release_turn_lease(
                            value.id,self.instance_id,generation))
                    self.leases.pop(value.id,None)
                    event("turn.reconciliation.cancelled",level=logging.WARNING,
                        turn_id=value.id)
                except Exception:
                    logger.exception("Turn reconciliation failed turn=%s", value.id)
                    generation = self.leases.pop(value.id, None)
                    if generation is not None:
                        await self._release_turn_lease_safely(value.id,generation)
                finally:
                    self.reconciliation_tasks.pop(value.id,None)
            task=asyncio.create_task(run(),name="turn-reconcile-"+turn.id)
            self.reconciliation_tasks[turn.id]=task
            pending.append(task)
        if pending:
            await asyncio.gather(*pending,return_exceptions=True)

    @observed("turn.recover", new_trace=True)
    async def _reconcile_turn(self, turn) -> None:
        if self.closing or turn.id in self.tasks:
            return
        claimed = await self._claim_turn_safely(turn.id)
        if not claimed:
            return
        self.leases[turn.id] = claimed.recovery_generation
        heartbeat = asyncio.create_task(self._heartbeat(turn.id, claimed.recovery_generation))
        try:
            if claimed.cancel_requested and claimed.state != TurnState.CLEANING:
                self._schedule(turn.id); return
            control = await self.store.get_thread_control(turn.thread_id)
            participant = (await self.store.get_pause_participant(turn.thread_id,
                "primary_harness", turn.id, control.generation)
                if control.state.value == "PAUSING" else None)
            if participant and participant.state.value == "REQUESTED":
                self._schedule(turn.id); return
            if claimed.state == TurnState.QUEUED:
                self._schedule(turn.id); return
            if claimed.state == TurnState.CLEANING and claimed.pending_final_state:
                if claimed.capability_nonce:
                    try:
                        async with observed_async_client('sandbox-gateway',timeout=5, trust_env=False) as client:
                            response = await client.post(self.settings.gateway_url.rstrip('/') + '/internal/revoke/' + claimed.capability_nonce,
                                headers=self._revoke_headers(claimed))
                            response.raise_for_status()
                    except Exception as exc:
                        await self.store.append_event(turn.id, EventKind.WARNING, {
                            "code": "recovery_capability_revoke_failed", "detail": type(exc).__name__})
                for sandbox in await self._turn_inventory(claimed):
                    await self.pool.retire(sandbox)
                target = TurnState(claimed.pending_final_state)
                await self.store.append_event(turn.id, EventKind.WARNING, {"code": "turn_cleanup_recovered",
                    "generation": claimed.recovery_generation, "final_state": target.value})
                logger.warning("Recovered cleanup turn=%s generation=%s final=%s", turn.id, claimed.recovery_generation, target.value)
                if target == TurnState.FAILED:
                    errors=[item for item in await self.store.items_after(turn.thread_id)
                        if item.turn_id==turn.id and item.type==ItemType.ERROR]
                    if not errors:
                        code,detail=public_failure(claimed.error_code,claimed.error_detail)
                        item=await self.store.append_item(turn.thread_id,turn.id,ItemType.ERROR,
                            status="failed",summary=detail,payload={"code":code,"detail":detail},
                            lease_owner=self.instance_id,
                            recovery_generation=claimed.recovery_generation)
                        await self.store.append_event(turn.id,EventKind.ERROR,
                            {"code":code,"detail":detail},item_id=item.id,
                            lease_owner=self.instance_id,
                            recovery_generation=claimed.recovery_generation)
                await self._state(turn.id, target, sandbox_id="", error_code=claimed.error_code, error_detail=claimed.error_detail)
                await self._release_turn_lease_safely(turn.id,claimed.recovery_generation)
                self.leases.pop(turn.id, None)
                next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
                if next_turn:
                    self._schedule(next_turn.id)
                return
            if claimed.state in {TurnState.RESTORING, TurnState.STARTING}:
                for sandbox in await self._turn_inventory(claimed):
                    await self.pool.retire(sandbox)
                await self.store.append_event(turn.id, EventKind.WARNING, {
                    "code": "turn_recovered_before_harness_start", "generation": claimed.recovery_generation})
                await self._state(turn.id, TurnState.QUEUED, sandbox_id=""); self._schedule(turn.id); return
            if claimed.state in {TurnState.RUNNING, TurnState.AWAITING_USER, TurnState.DRAINING_INPUTS, TurnState.CHECKPOINTING} and claimed.runtime_execution_id:
                matches=[];candidates=[];durable_missing=not bool(claimed.sandbox_id)
                if claimed.sandbox_id:
                    try:durable=await self.pool.backend.get(self._turn_handle(claimed))
                    except SandboxNotFound:durable_missing=True
                    else:
                        matches=[durable]
                        if (durable.health_state=="ready" and await self.runtime.execution_status(
                                durable,claimed.runtime_execution_id) is not None):
                            candidates=[durable]
                if durable_missing:
                    matches=[]
                if candidates:
                    selected=candidates[0]
                    for duplicate in matches:
                        if duplicate.id!=selected.id:
                            await self.pool.retire(duplicate)
                    await self.store.append_event(turn.id, EventKind.WARNING, {"code": "turn_runtime_reattached",
                        "generation": claimed.recovery_generation, "runtime_sequence": claimed.runtime_sequence})
                    event("turn.runtime.reattached",turn_id=turn.id,thread_id=turn.thread_id,generation=claimed.recovery_generation,runtime_sequence=claimed.runtime_sequence)
                    self._schedule(turn.id); return
                transient=[value for value in matches
                    if value.health_state in {"inventory","starting","unhealthy"}]
                if transient:
                    await self.store.append_event(turn.id,EventKind.WARNING,{
                        "code":"turn_runtime_not_ready",
                        "generation":claimed.recovery_generation,
                        "sandboxes":[{"id":value.id,"health":value.health_state,
                            "detail":value.health_error} for value in transient]})
                    await self._release_turn_lease_safely(
                        turn.id,claimed.recovery_generation)
                    self.leases.pop(turn.id,None)
                    return
                for sandbox in matches:
                    await self.pool.retire(sandbox)
            await self.store.append_event(turn.id, EventKind.ERROR, {"code": "turn_interrupted_during_harness_execution",
                "detail": "The previous broker lease expired after Harness execution began; automatic replay was suppressed.",
                "generation": claimed.recovery_generation})
            event("turn.recovery.unavailable",level=logging.ERROR,turn_id=turn.id,generation=claimed.recovery_generation)
            await self.store.complete_agent_item(turn.id, "interrupted")
            await self._state(turn.id, TurnState.FAILED, sandbox_id="", error_code="turn_interrupted_during_harness_execution",
                error_detail="Harness execution was interrupted and was not replayed because external side effects may already exist")
            await self._release_turn_lease_safely(turn.id,claimed.recovery_generation)
            self.leases.pop(turn.id, None)
            next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
            if next_turn:
                self._schedule(next_turn.id)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    @observed("input.deliver")
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
            prepared=await self.store.prepare_turn_input_artifacts(value.id,
                turn_id=turn.id,execution_id=turn.runtime_execution_id,
                lease_owner=self.instance_id,recovery_generation=self.leases.get(turn.id))
            if not prepared:return False
            delivery_mode = await self.runtime.append_input(sandbox, message=value.message,
                attachments=value.attachments, reasoning_effort=turn.reasoning_effort,
                execution_id=turn.runtime_execution_id, input_id=value.id)
            if delivery_mode:
                current = await self.store.get_turn(value.turn_id)
                # An HTTP acceptance is not a completion receipt. The result's
                # explicit input_ids determine whether this input was applied.
                if current and current.state in {TurnState.RUNNING, TurnState.AWAITING_USER}:
                    await self.store.set_input_status(value.id, "delivered", delivery_mode,
                        execution_id=turn.runtime_execution_id,
                        lease_owner=self.instance_id,
                        recovery_generation=self.leases.get(value.turn_id))
            return bool(delivery_mode)

    @observed("interaction.respond")
    async def respond_interaction(self, interaction_id: str, owner_id: str, value) -> object:
        response = {"action": value.action, "answers": value.answers}
        interaction, claimed = await self.store.claim_interaction_response(
            interaction_id, owner_id, value.idempotency_key, response,
            claim_owner=self.instance_id if self.execution_enabled else "",
            lease_seconds=30 if self.execution_enabled else 0,
        )
        if not claimed:
            return interaction
        if interaction.kind == "approval" and value.action not in {"approve", "reject"}:
            await self.store.finish_interaction_response(
                interaction_id, delivered=False,
                claim_owner=self.instance_id if self.execution_enabled else "")
            raise ValueError("Approval requires approve or reject")
        if interaction.kind == "question" and (value.action != "answer" or not value.answers):
            await self.store.finish_interaction_response(
                interaction_id, delivered=False,
                claim_owner=self.instance_id if self.execution_enabled else "")
            raise ValueError("Question requires at least one answer")
        if not self.execution_enabled:
            return interaction
        return await self._deliver_interaction_response(interaction,response,raise_errors=True)

    async def _deliver_interaction_response(self,interaction,response,*,raise_errors:bool):
        turn = await self.store.get_turn(interaction.turn_id)
        sandbox = self.active_sandboxes.get(interaction.turn_id)
        if (not turn or not sandbox or turn.runtime_execution_id != interaction.runtime_execution_id):
            await self.store.finish_interaction_response(
                interaction.id, delivered=None, claim_owner=self.instance_id)
            if raise_errors:raise RuntimeError("interaction_runtime_unavailable")
            return interaction
        try:
            delivered = await self.runtime.respond_interaction(
                sandbox, interaction.runtime_interaction_id, response,
            )
        except Exception as exc:
            await self.store.finish_interaction_response(
                interaction.id, delivered=False, claim_owner=self.instance_id)
            if raise_errors:raise RuntimeError("interaction_delivery_failed") from exc
            return interaction
        current = await self.store.finish_interaction_response(
            interaction.id, delivered=delivered, claim_owner=self.instance_id)
        if delivered is None:
            if raise_errors:raise RuntimeError("interaction_delivery_unknown")
            return current
        if not delivered:
            if raise_errors:raise RuntimeError("interaction_transport_unsupported")
            return current
        await self.store.finish_interaction_item(interaction.turn_id,interaction.id,response)
        if await self.store.resume_interaction_turn(interaction.turn_id, interaction.runtime_execution_id):
            await self.store.append_event(interaction.turn_id, EventKind.STATE, {"state": TurnState.RUNNING.value})
        return current

    async def deliver_pending_interaction_responses(self,limit:int=20)->int:
        if not self.execution_enabled:return 0
        values=await self.store.claim_pending_interaction_responses(
            self.instance_id,limit=limit,lease_seconds=30)
        delivered=0
        for interaction in values:
            current=await self._deliver_interaction_response(
                interaction,interaction.response,raise_errors=False)
            if current.status=="responded":delivered+=1
        return delivered

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

    @observed("turn.submit")
    async def submit(self, thread_id: str, value: TurnCreate, *, network_access: str = "restricted",
                     internal: bool = False) -> TurnView:
        if sum(item.size for item in value.attachments) > self.settings.max_attachment_bytes:
            raise ValueError("Attachments exceed the Turn byte limit")
        thread = await self.store.get_thread(thread_id)
        if not thread:
            raise KeyError("thread_not_found")
        control = await self.store.get_sandbox_control(thread_id, thread.owner_id)
        if control and control["state"] in {"user_controlled", "releasing"} and control["expires_at"] > now():
            raise RuntimeError("sandbox_takeover_active")
        if value.harness is not None and value.harness != thread.harness:
            raise ValueError("Harness is fixed for this conversation; create a new conversation to switch Harness")
        resolved = self.catalog.resolve(thread.harness, value.model or thread.model,
            value.reasoning_level or thread.reasoning_level)
        turn, created, start_now = await self.store.create_turn(thread_id, value, resolved, thread.owner_id,
            traceparent=carrier().get("traceparent", ""), network_access=network_access, internal=internal,
            runtime_image_digest=self.pool.images[resolved.harness],
            runtime_code_bundle_digest=self.settings.runtime_code_bundle_digest,
            runtime_bundle_digest=self.settings.runtime_bundle_digest)
        if created and not internal:
            if await self.store.attach_suppressed_job_context(turn.id):
                turn = await self.store.get_turn(turn.id)
        event("turn.accepted",thread_id=thread_id,turn_id=turn.id,harness=turn.harness,model=turn.model,reasoning_effort=turn.reasoning_effort,reused=not created,state=turn.state)
        if created and start_now:
            self._schedule(turn.id)
        return turn

    async def delete_thread(self, thread_id: str, owner_id: str) -> None:
        async with self.thread_lock(thread_id):
            thread = await self.store.get_thread(thread_id, owner_id)
            if not thread:
                raise KeyError("thread_not_found")
            if await self.store.get_sandbox_control(thread_id, owner_id):
                raise RuntimeError("sandbox_takeover_active")
            turns = await self.store.list_turns(thread_id, owner_id)
            if any(turn.id in self.tasks or turn.id in self.active_sandboxes for turn in turns):
                raise RuntimeError("active_thread_cannot_be_deleted")
            result = await self.store.begin_thread_lifecycle(thread_id, owner_id, "delete")
            if result.state == "refused":
                raise RuntimeError(result.error_code)
            return result

    async def set_thread_archived(self, thread_id: str, owner_id: str, *, archived: bool):
        async with self.thread_lock(thread_id):
            if not await self.store.get_thread(thread_id, owner_id):
                raise KeyError("thread_not_found")
            if archived and await self.store.get_sandbox_control(thread_id, owner_id):
                raise RuntimeError("sandbox_takeover_active")
            result = await self.store.begin_thread_lifecycle(
                thread_id, owner_id, "archive" if archived else "unarchive")
            if result.state == "refused":
                raise RuntimeError(result.error_code)
            if result.state == "pending":
                return result
            return await self.store.get_thread(thread_id, owner_id)

    @observed("turn.cancel")
    async def cancel(self, turn_id: str) -> None:
        turn = await self.store.request_turn_cancel(turn_id)
        await self.store.append_event(turn_id, EventKind.WARNING, {"code": "cancel_requested"})
        task = self.tasks.get(turn_id)
        if (task and not task.cancelling() and turn_id not in self.cancellation_handlers
                and turn.state != TurnState.CLEANING):
            task.cancel()
        if turn.state == TurnState.CANCELLED:
            await self.store.append_event(turn_id, EventKind.STATE, {"state": turn.state.value})
            next_turn = await self.store.claim_next_turn(turn.thread_id, turn.id)
            if next_turn:
                self._schedule(next_turn.id)

    async def pause_thread(self, thread_id: str, owner_id: str):
        control = await self.store.request_thread_pause(thread_id, owner_id)
        participants = await self.store.list_pause_participants(thread_id, control.generation)
        for participant in participants:
            if participant.kind != "primary_harness" or participant.state.value != "REQUESTED":
                continue
            await self.store.append_event(participant.participant_id, EventKind.WARNING,
                {"code": "pause_requested", "generation": control.generation})
            task = self.tasks.get(participant.participant_id)
            if (task and not task.cancelling()
                    and participant.participant_id not in self.cancellation_handlers):
                task.cancel()
        return await self.store.get_thread_control(thread_id, owner_id)

    async def _state(self, turn_id: str, state: TurnState, **kwargs) -> TurnView:
        generation = self.leases.get(turn_id)
        turn = await self.store.set_turn_state(turn_id, state, **kwargs,
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)
        await self.store.append_event(turn_id, EventKind.STATE, {"state": turn.state.value},
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)
        event("turn.state.changed",turn_id=turn_id,thread_id=turn.thread_id,state=turn.state,
            state_after=turn.state,generation=generation or 0,error_code=turn.error_code,
            failure_origin=failure_origin(turn.error_code) if turn.error_code else None)
        return turn

    async def _heartbeat(self, turn_id: str, generation: int) -> None:
        while True:
            await asyncio.sleep(self.settings.turn_heartbeat_seconds)
            renewed = await self._database_call(self.store.renew_turn_lease(
                turn_id, self.instance_id, generation, self.settings.turn_lease_seconds,
            ))
            if not renewed:
                event("turn.lease.lost",level=logging.WARNING,turn_id=turn_id,generation=generation)
                task = self.tasks.get(turn_id)
                task = task or self.reconciliation_tasks.get(turn_id)
                if task and not task.cancelling() and turn_id not in self.cancellation_handlers:
                    task.cancel()
                return
            turn = await self._database_call(self.store.get_turn(turn_id))
            sandbox = self.active_sandboxes.get(turn_id)
            if sandbox and turn and turn.state != TurnState.CLEANING:
                try:
                    await self.pool.backend.renew(sandbox)
                except SandboxNotFound:
                    event("sandbox.renew.not_found",level=logging.ERROR,turn_id=turn_id,sandbox_id=sandbox.id)
                    task = self.tasks.get(turn_id)
                    task = task or self.reconciliation_tasks.get(turn_id)
                    if task and not task.cancelling() and turn_id not in self.cancellation_handlers:
                        task.cancel()
                    return
                except Exception as exc:
                    event("sandbox.renew.failed",level=logging.WARNING,turn_id=turn_id,
                        sandbox_id=sandbox.id,exc=exc)
            task = self.tasks.get(turn_id)
            task = task or self.reconciliation_tasks.get(turn_id)
            if (turn and turn.cancel_requested and turn.state != TurnState.CLEANING
                    and turn_id not in self.cancellation_handlers
                    and task and not task.cancelling()):
                task.cancel()

    @staticmethod
    async def _database_call(operation):
        """Finish a database operation before propagating even repeated cancellation."""
        task=asyncio.create_task(operation)
        cancelled=False
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError:cancelled=True
            except Exception:
                if not cancelled:raise
                break
        if cancelled:
            # Retrieve any exception before preserving the caller's cancellation.
            if not task.cancelled():task.exception()
            raise asyncio.CancelledError()
        return task.result()

    async def _release_turn_lease_safely(self,turn_id:str,generation:int)->None:
        async def release_and_forget():
            await self.store.release_turn_lease(turn_id,self.instance_id,generation)
            if self.leases.get(turn_id)==generation:self.leases.pop(turn_id,None)
        await self._database_call(release_and_forget())

    async def _claim_turn_safely(self,turn_id:str):
        async def claim_and_record():
            claimed=await self.store.claim_turn(
                turn_id,self.instance_id,self.settings.turn_lease_seconds)
            if claimed:self.leases[turn_id]=claimed.recovery_generation
            return claimed
        try:return await self._database_call(claim_and_record())
        except BaseException:
            generation=self.leases.get(turn_id)
            if generation is not None:
                await self._release_turn_lease_safely(turn_id,generation)
                self.leases.pop(turn_id,None)
            raise

    async def _record_runtime_event(self, thread_id: str, turn_id: str, value: RuntimeEvent,
                                    execution_id: str = "") -> None:
        generation = self.leases.get(turn_id)
        if value.kind not in {EventKind.TEXT_DELTA, EventKind.TOOL_OUTPUT}:
            event("runtime.event.received",thread_id=thread_id,turn_id=turn_id,execution_id=execution_id,runtime_sequence=value.runtime_sequence,tool_id=value.payload.get("tool_use_id"),reason=value.kind.value)
        await self.store.persist_runtime_event(thread_id, turn_id, value, execution_id,
            lease_owner=self.instance_id if generation is not None else None,
            recovery_generation=generation)
        if value.kind==EventKind.STEP_FINISHED and value.payload.get("input_completed"):
            for input_id in value.payload.get("input_ids") or []:
                if input_id!=turn_id:
                    await self.store.set_input_status(input_id,"applied",
                        execution_id=execution_id,
                        lease_owner=self.instance_id if generation is not None else None,
                        recovery_generation=generation)

    @observed("input.materialize")
    async def _materialize_attachments(self, sandbox, attachments) -> None:
        for attachment in attachments:
            data = await asyncio.to_thread(self.objects.get, attachment.object_ref, maximum=self.settings.max_attachment_bytes)
            if len(data) != attachment.size:
                raise ValueError("Attachment size does not match its immutable record")
            await self.runtime.put_attachment(sandbox, attachment.name, data, attachment.mime)

    async def _register_project_attachments(self, thread_id: str, turn_id: str, attachments) -> list:
        values = []
        generation = self.leases.get(turn_id)
        for attachment in attachments:
            value, _ = await self.store.register_project_file(thread_id,
                path="assets/" + attachment.name, source_type="upload", source_turn_id=turn_id,
                sha256=attachment.sha256, size=attachment.size, mime=attachment.mime,
                object_ref=attachment.object_ref, lease_owner=self.instance_id,
                recovery_generation=generation)
            values.append(value)
        return values

    @observed("workspace.materialize")
    async def _materialize_project_files(self, sandbox, files) -> None:
        for value in files:
            data = await asyncio.to_thread(self.objects.get, value.object_ref, maximum=self.settings.max_artifact_bytes)
            if len(data) != value.size or hashlib.sha256(data).hexdigest() != value.sha256:
                raise ValueError("Project file does not match its immutable record")
            await self.runtime.put_project_file(sandbox, value.path, data, value.mime)

    @observed("workspace.save")
    async def _save_workspace(self, sandbox, thread_id: str, turn_id: str,
                              native_thread_id: str, *, partial: bool = False) -> str:
        generation = self.leases.get(turn_id)
        downloads = []
        if partial:
            await self.store.mark_turn_artifacts_partial(turn_id, lease_owner=self.instance_id,
                recovery_generation=generation)
        try:
            archive = await self.runtime.checkpoint(sandbox, maximum=self.settings.max_checkpoint_bytes)
            downloads.append(archive)
            checkpoint_ref, _ = await asyncio.to_thread(self.objects.put_file, archive.path,
                digest=archive.sha256, size=archive.size)
            await self.store.update_thread_checkpoint(thread_id, checkpoint_ref, native_thread_id,
                turn_id=turn_id, lease_owner=self.instance_id, recovery_generation=generation)
            event("workspace.checkpoint.persisted",bytes=archive.size,partial=partial)
            total = 0; runtime_artifacts = await self.runtime.artifacts(sandbox, turn_id)
            site_roots = {PurePosixPath(item["path"]).parent.as_posix()
                for item in runtime_artifacts if PurePosixPath(item["path"]).name.lower() == "index.html"}
            site_files: dict[str, dict[str, object]] = {root:{} for root in site_roots}
            for item in runtime_artifacts:
                path = item["path"]
                remaining = self.settings.max_artifact_bytes - total
                value = await self.runtime.artifact(sandbox, path, maximum=remaining)
                downloads.append(value); total += value.size
                if item.get("size") != value.size or item.get("sha256") != value.sha256:
                    raise ValueError("runtime_artifact_identity_mismatch")
                object_ref, digest = await asyncio.to_thread(self.objects.put_file, value.path,
                    digest=value.sha256, size=value.size)
                artifact, _, _ = await self.store.publish_artifact(thread_id, turn_id, path=path,
                    sha256=digest, size=value.size, mime=item.get("mime") or
                    mimetypes.guess_type(path)[0] or "application/octet-stream", object_ref=object_ref,
                    partial=partial, lease_owner=self.instance_id, recovery_generation=generation)
                event("artifact.persisted",artifact_id=artifact.id,bytes=value.size,partial=partial)
                pure = PurePosixPath(path)
                for root in site_roots:
                    root_path = PurePosixPath(root)
                    try: relative = pure.relative_to(root_path).as_posix()
                    except ValueError: continue
                    site_files[root][relative] = value.path
            if not partial:
                owner_id = await self.store.owner_id_for_thread(thread_id)
                for root, files in site_files.items():
                    try:
                        bundle, values = await asyncio.to_thread(build_site_archive_from_paths, files)
                        bundle_ref, digest = await asyncio.to_thread(self.objects.put, bundle)
                        if await self.store.site_draft_by_bundle(thread_id, digest):
                            continue
                        site_rows = []
                        for value in values:
                            object_ref, file_digest = await asyncio.to_thread(self.objects.put, value.data)
                            if file_digest != value.sha256: raise ValueError("site_file_identity_mismatch")
                            site_rows.append({"path":value.path,"sha256":value.sha256,"size":value.size,
                                "mime":value.mime,"object_ref":object_ref})
                        name = PurePosixPath(root).name if root not in {"", "."} else "互动站点"
                        slug = normalize_site_slug(name if name != "互动站点" else "site-" + turn_id[:8])
                        await self.store.create_site_draft(owner_id, thread_id, turn_id, name=name,
                            slug=slug, idempotency_key="artifact-site-" + digest,
                            archive_object_ref=bundle_ref, bundle_sha256=digest, archive_size=len(bundle),
                            file_count=len(values), total_size=sum(value.size for value in values),
                            expires_at=now() + timedelta(seconds=self.settings.site_draft_ttl_seconds),
                            public_base_path=self.settings.site_public_base_path, files=site_rows)
                        event("site.draft.auto_created",turn_id=turn_id,files=len(values),bytes=len(bundle))
                    except ValueError as exc:
                        event("site.draft.auto_skipped",level=logging.WARNING,turn_id=turn_id,
                            error_code=str(exc)[:100])
            return checkpoint_ref
        finally:
            for value in downloads:
                value.remove()

    async def _collect_runtime_logs(self, sandbox) -> None:
        target=telemetry()
        if target is None or not hasattr(self.runtime,"diagnostics"):return
        from sandbox_harness.common.observability.telemetry_ingest import (import_snapshot)
        if not isinstance(self._runtime_diagnostics,OrderedDict):
            self._runtime_diagnostics=OrderedDict(self._runtime_diagnostics)
        previous=self._runtime_diagnostics.pop(sandbox.id,None)
        after=previous.cursor if previous else 0
        previous_instance=previous.instance_id if previous else ''
        baseline=dict(previous.counters) if previous else {}
        count=0;stats={};instance_id=previous_instance
        try:
            async with asyncio.timeout(5):
                for _ in range(20):
                    snapshot=await self.runtime.diagnostics(sandbox,after)
                    current_instance=str(snapshot.get("instance_id") or "unknown")[:160]
                    if previous_instance and current_instance!=previous_instance:
                        after=0;baseline={};previous_instance=''
                        snapshot=await self.runtime.diagnostics(sandbox,after)
                        current_instance=str(snapshot.get("instance_id") or "unknown")[:160]
                    instance_id=current_instance
                    stats=snapshot.get("stats",{})
                    retained_from=max(1,int(snapshot.get("retained_from",1)))
                    if retained_from>after+1:
                        event("runtime.logs.window",retained_from=retained_from,
                            reason="runtime_cursor_rotated")
                        after=retained_from-1
                    rows=snapshot.get("records",[])
                    if not rows:break
                    count+=import_snapshot(target,snapshot,sandbox)
                    cursor=max(int(row.get("sequence",0)) for row in rows)
                    if cursor<=after:break
                    after=cursor
                    if after>=snapshot.get("last_sequence",after):break
            current={key:max(0,int(stats.get(key,0) or 0))
                for key in ("dropped_records","export_failures","disk_errors")}
            delta={key:(value-baseline.get(key,0) if value>=baseline.get(key,0) else value)
                for key,value in current.items()}
            self._runtime_diagnostics[sandbox.id]=RuntimeDiagnosticState(instance_id,after,current)
            while len(self._runtime_diagnostics)>512:self._runtime_diagnostics.popitem(last=False)
            event("runtime.logs.collected",level=logging.WARNING if any(delta.values()) else logging.INFO,
                sandbox_id=sandbox.id,count=count,last_sequence=after,reason="runtime_turn_delta",
                dropped_count=delta["dropped_records"],export_failures=delta["export_failures"],
                disk_errors=delta["disk_errors"],last_export_http_status=stats.get("last_export_http_status"))
        except Exception as exc:
            if previous:self._runtime_diagnostics[sandbox.id]=previous
            event("runtime.logs.collection_failed",level=logging.WARNING,exc=exc,sandbox_id=sandbox.id)

    @observed("turn.process", new_trace=True)
    async def _process(self, turn_id: str) -> None:
        try:
            claimed=await self._claim_turn_safely(turn_id)
            turn=(await self._database_call(self.store.get_turn(turn_id))) if claimed else None
            thread=(await self._database_call(self.store.get_thread(turn.thread_id))) if turn else None
        except BaseException:
            try:
                generation=self.leases.get(turn_id)
                if generation is not None:
                    await self._release_turn_lease_safely(turn_id,generation)
            finally:self.tasks.pop(turn_id,None)
            raise
        if not claimed:
            self.tasks.pop(turn_id, None)
            return
        generation = claimed.recovery_generation
        if not turn or not thread:
            await self._release_turn_lease_safely(turn_id,generation)
            self.leases.pop(turn_id,None)
            self.tasks.pop(turn_id,None)
            return
        enrich(thread_id=claimed.thread_id,turn_id=turn_id,harness=claimed.harness,model=claimed.model,reasoning_effort=claimed.reasoning_effort,generation=generation)
        add_link(claimed.submit_traceparent);add_link(claimed.last_traceparent)
        event("turn.lease.claimed",generation=generation)
        self.leases[turn_id] = generation
        heartbeat = asyncio.create_task(self._heartbeat(turn_id, generation), name="turn-heartbeat-" + turn_id)
        self.heartbeats[turn_id] = heartbeat
        sandbox = None
        extra_sandboxes = []
        final_state = TurnState.FAILED
        error_code = "turn_failed"
        error_detail = "Turn failed before the Harness returned a result"
        abandoned = False
        lost_lease = False
        cleanup_pending = False
        try:
            async with self.thread_lock(thread.id):
                try:
                    await self.store.record_turn_trace(turn_id,carrier().get("traceparent", ""),self.instance_id,generation)
                except Exception as exc:
                    event("turn.trace.persist_failed",level=logging.WARNING,exc=exc)
                control = await self.store.get_thread_control(thread.id)
                participant = (await self.store.get_pause_participant(thread.id,
                    "primary_harness", turn.id, control.generation)
                    if control.state.value == "PAUSING" else None)
                if participant and participant.state.value == "REQUESTED":
                    matches = await self._turn_inventory(turn)
                    ready = [value for value in matches if value.health_state == "ready"]
                    if ready:
                        sandbox = await self.pool.adopt(ready[0], thread_id=thread.id,
                            turn_id=turn.id,expected_profile=self._sandbox_profile_for_turn(turn),
                            ownership_owner_id=thread.owner_id)
                        self.active_sandboxes[turn.id] = sandbox
                        extra_sandboxes = [value for value in matches if value.id != sandbox.id]
                    else:
                        extra_sandboxes = matches
                    raise asyncio.CancelledError()
                if turn.cancel_requested:
                    matches = await self._turn_inventory(turn)
                    ready = [value for value in matches if value.health_state == "ready"]
                    if ready:
                        sandbox = await self.pool.adopt(ready[0], thread_id=thread.id,
                            turn_id=turn.id,expected_profile=self._sandbox_profile_for_turn(turn),
                            ownership_owner_id=thread.owner_id)
                        self.active_sandboxes[turn.id] = sandbox
                        extra_sandboxes = [value for value in matches if value.id != sandbox.id]
                    else:
                        extra_sandboxes = matches
                    raise asyncio.CancelledError()
                recovering = turn.state in {
                    TurnState.RUNNING, TurnState.AWAITING_USER, TurnState.DRAINING_INPUTS, TurnState.CHECKPOINTING,
                } and bool(turn.runtime_execution_id)
                recovered_result = None
                if recovering:
                    matches=[];candidates=[];durable_missing=not bool(turn.sandbox_id)
                    if turn.sandbox_id:
                        try:durable=await self.pool.backend.get(self._turn_handle(turn))
                        except SandboxNotFound:durable_missing=True
                        else:
                            if durable.health_state=="ready":
                                status=await self.runtime.execution_status(
                                    durable,turn.runtime_execution_id)
                                if status is not None:candidates.append((durable,status))
                    if durable_missing:
                        matches=[]
                    if not candidates:
                        raise RuntimeError("recoverable_sandbox_not_found")
                    selected,status = candidates[0]
                    for duplicate in matches:
                        if duplicate.id != selected.id:
                            await self.pool.retire(duplicate)
                    sandbox = await self.pool.adopt(selected,thread_id=thread.id,turn_id=turn.id,
                        expected_profile=self._sandbox_profile_for_turn(turn),
                        ownership_owner_id=thread.owner_id)
                    self.active_sandboxes[turn.id] = sandbox
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
                    if turn.state==TurnState.QUEUED:
                        business_metric('updream.turn.queue.duration',
                            max(0,(now()-turn.created_at).total_seconds()*1000),outcome='started')
                    await self._state(turn.id, TurnState.RESTORING)
                    sandbox = await self.pool.acquire(
                        thread_id=thread.id,
                        turn_id=turn.id,
                        harness=turn.harness,
                        environment={},
                        image=turn.runtime_image_digest or self.settings.image_for(turn.harness.value),
                        ownership_owner_id=thread.owner_id,
                    )
                    token, claims = self.codec.issue(
                        thread_id=thread.id,
                        turn_id=turn.id,
                        sandbox_id=sandbox.id,
                        harness=turn.harness,
                        model=turn.model,
                        routes=[*ROUTES[turn.harness],"/v1/telemetry"],
                        network_access=turn.network_access,
                        ttl_seconds=self.settings.capability_ttl_seconds,
                        max_requests=self.settings.capability_request_limit,
                        max_public_requests=self.settings.capability_public_request_limit,
                        token_budget=self.settings.model_token_budget,
                    )
                    await self.store.set_turn_capability(
                        turn.id, claims.nonce, self.instance_id, generation,
                    )
                    await self._state(turn.id, TurnState.STARTING, sandbox_id=sandbox.id,
                        sandbox_handle_json=self._handle_json(sandbox))
                    route=self.pool.backend.runtime_route(sandbox)
                    primary_skills = sorted(skill.id for skill in self.catalog.profile_resolver.skills.values()
                        if turn.harness.value in skill.supported_harnesses) if self.catalog.profile_resolver else []
                    await self.runtime.inject(
                        sandbox,
                        capability_token=token,
                        gateway_url=route.gateway_url,
                        proxy_url=route.proxy_url,
                        harness=turn.harness.value,
                        model=turn.model,
                        model_start_timeout_seconds=self.settings.runtime_model_start_timeout_seconds,
                        event_idle_timeout_seconds=self.settings.runtime_event_idle_timeout_seconds,
                        defer_skill_install=bool(thread.checkpoint_ref and not sandbox.reused),
                        runtime_bundle_digest=turn.runtime_bundle_digest or self.settings.runtime_bundle_digest,
                        runtime_code_bundle_digest=(turn.runtime_code_bundle_digest
                            or self.settings.runtime_code_bundle_digest),
                        runtime_profile_id="primary-full-cpu", skill_ids=primary_skills,
                        telemetry_metrics_enabled=self.settings.runtime_metrics_enabled,
                        stderr_log_max_bytes=getattr(self.settings,'runtime_stderr_log_max_bytes',1024),
                        skill_digests={identity:self.catalog.profile_resolver.skills[identity].digest
                            for identity in primary_skills} if self.catalog.profile_resolver else {},
                    )
                    if thread.checkpoint_ref and not sandbox.reused:
                        archive_size = await asyncio.to_thread(self.objects.size,
                            thread.checkpoint_ref, maximum=self.settings.max_checkpoint_bytes)
                        try:
                            await self.runtime.restore(sandbox, object_chunks(self.objects,
                                thread.checkpoint_ref, maximum=self.settings.max_checkpoint_bytes),
                                size=archive_size)
                        except httpx.HTTPStatusError as exc:
                            raise CheckpointRestoreError("Runtime rejected the saved workspace checkpoint") from exc
                    await self._register_project_attachments(thread.id, turn.id, turn.attachments)
                    project_files = await self.store.freeze_turn_project_files(turn.id, thread.id,
                        lease_owner=self.instance_id, recovery_generation=generation)
                    await self.runtime.reset_project_files(sandbox)
                    await self._materialize_project_files(sandbox, project_files)
                    await self._materialize_attachments(sandbox, turn.attachments)
                    await self._state(turn.id, TurnState.RUNNING, sandbox_id=sandbox.id,
                        sandbox_handle_json=self._handle_json(sandbox))
                enrich(sandbox_id=sandbox.id,image_digest=sandbox.image.rsplit("@",1)[-1])
                self.active_sandboxes[turn.id] = sandbox
                message = turn.runtime_message or turn.message
                attachments, native_thread_id = turn.attachments, thread.native_thread_id
                execution_id = turn.runtime_execution_id
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
                            "applied" if result.status in ("completed","waiting_user")
                                or current_input_id in result.input_ids else "unknown",
                            execution_id=execution_id, lease_owner=self.instance_id,
                            recovery_generation=generation)
                    for input_id in result.input_ids:
                        if input_id != turn.id:
                            await self.store.set_input_status(input_id,
                                "applied",
                                execution_id=execution_id, lease_owner=self.instance_id,
                                recovery_generation=generation)
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
                        await self.store.set_input_status(pending.id, "failed",
                            lease_owner=self.instance_id, recovery_generation=generation)
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
                    error_code,error_detail=await reconcile_native_model_error(
                        self.store,turn.id,error_code,error_detail)
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
            self.cancellation_handlers.add(turn_id)
            if self.closing:
                enrich(outcome="abandoned")
                event("turn.processing.abandoned",turn_id=turn.id,reason="broker_shutdown")
                abandoned = True
                return
            lost_lease = not await self.store.owns_turn_lease(turn.id, self.instance_id, generation)
            control = await self.store.get_thread_control(thread.id)
            participant = (await self.store.get_pause_participant(thread.id,
                "primary_harness", turn.id, control.generation)
                if control.state.value == "PAUSING" else None)
            if participant and participant.state.value == "REQUESTED":
                checkpoint_ref = ""
                native_thread_id = thread.native_thread_id
                pause_error = ""
                try:
                    if not sandbox or lost_lease:
                        raise RuntimeError("pause_sandbox_unavailable")
                    stopped = await self.runtime.cancel(sandbox)
                    native_thread_id = str(stopped.get("native_thread_id") or native_thread_id)
                    checkpoint_ref = await self._save_workspace(sandbox, thread.id, turn.id,
                        native_thread_id, partial=True)
                except Exception as exc:
                    pause_error = type(exc).__name__ + ": " + str(exc)
                await self.store.complete_pause_participant(thread.id, control.generation,
                    "primary_harness", turn.id, checkpoint_ref=checkpoint_ref,
                    native_thread_id=native_thread_id,
                    external_execution_id=turn.runtime_execution_id,
                    detail={"error": pause_error} if pause_error else {}, failed=bool(pause_error))
                final_state = TurnState.PAUSED
                error_code = "pause_degraded" if pause_error else "paused"
                error_detail = ("Paused at the last durable checkpoint: " + pause_error
                    if pause_error else "Turn paused with a durable checkpoint")
            else:
                final_state = TurnState.CANCELLED
                error_code = "cancelled"
                error_detail = "Turn was cancelled"
                if sandbox and not lost_lease:
                    try:
                        await self.runtime.cancel(sandbox)
                    except Exception:
                        pass
        except Exception as exc:
            error_code = getattr(exc, "error_code", type(exc).__name__)
            error_detail = str(exc)
            event("turn.execution.failed",level=logging.ERROR,exc=exc,turn_id=turn.id,execution_id=turn.runtime_execution_id)
            if sandbox and not lost_lease:
                try:
                    await self._state(turn.id, TurnState.CHECKPOINTING, sandbox_id=sandbox.id)
                    await self._save_workspace(sandbox, thread.id, turn.id,
                        thread.native_thread_id, partial=True)
                except Exception as save_error:
                    event("workspace.partial_save.failed",level=logging.WARNING,exc=save_error,
                        turn_id=turn.id,sandbox_id=sandbox.id)
                    try:
                        await self.store.append_event(turn.id, EventKind.WARNING,
                            {"code": "partial_workspace_save_failed",
                             "detail": "本轮中间文件未能完整保存。"},
                            lease_owner=self.instance_id, recovery_generation=generation)
                    except RuntimeError as fence_error:
                        if str(fence_error) == "stale_turn_lease":
                            lost_lease = True
                        else:
                            raise
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
                    await self._release_turn_lease_safely(turn_id,generation)
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
                        await self._collect_runtime_logs(sandbox)
                        revoked = False
                        capability_nonce = claims.nonce if 'claims' in locals() else turn.capability_nonce
                        if capability_nonce:
                            for attempt in range(3):
                                try:
                                    async with observed_async_client('sandbox-gateway',timeout=5, trust_env=False) as client:
                                        response = await client.post(
                                            self.settings.gateway_url.rstrip('/') + '/internal/revoke/' + capability_nonce,
                                            headers=self._revoke_headers(turn,sandbox.id),
                                        )
                                        response.raise_for_status()
                                    event("capability.revocation.confirmed",turn_id=turn.id,thread_id=turn.thread_id,sandbox_id=sandbox.id)
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
                            await self.pool.retire(extra)
                        await self.pool.release(sandbox, keep_attached=keep_attached,
                            keepalive_seconds=self.settings.session_keepalive_seconds)
                except Exception as exc:
                    if isinstance(exc, RuntimeError) and str(exc) == "stale_turn_lease":
                        lost_lease = True
                    else:
                        cleanup_pending = True
                        event("turn.cleanup.pending",level=logging.WARNING,exc=exc,
                            turn_id=turn.id,final_state=final_state.value)
                        try:
                            await self.store.append_event(turn.id, EventKind.WARNING, {
                                "code": "cleanup_pending",
                                "detail": "执行结果已确定，环境清理仍在后台重试。",
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
                    if cleanup_pending:
                        self.active_sandboxes.pop(turn.id, None)
                        await self._release_turn_lease_safely(turn.id,generation)
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
                    await self._release_turn_lease_safely(turn.id,generation)
                    self.leases.pop(turn.id, None)
                    self.tasks.pop(turn.id, None)
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                self.heartbeats.pop(turn_id, None)
                self.cancellation_handlers.discard(turn_id)
                self.active_sandboxes.pop(turn_id, None)
                self.leases.pop(turn_id, None)
                self.tasks.pop(turn_id, None)
