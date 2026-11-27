"""PlatformWorker process: multi-active durable claims plus fenced singleton loops."""
from __future__ import annotations

import asyncio
import json
import signal
import uuid
from contextlib import suppress
from types import SimpleNamespace

from sandbox_harness.common.configuration.config import Settings, load_settings, validate_service_settings
from sandbox_harness.common.contracts.domain import (ThreadControlState,TurnCreate)
from sandbox_harness.common.execution.platform_components import (
    PlatformComponents, build_platform_components,
)
from sandbox_harness.common.execution.sandbox_inspector import SandboxInspector
from sandbox_harness.common.observability.observability import (
    add_link, canonical_traceparent, configure, event, operation,
)
from sandbox_harness.common.sandbox.ownership_cleanup import OwnershipCleanup
from sandbox_harness.common.storage.object_gc import (
    ObjectGarbageCollector, authoritative_object_references,
)

from .job_continuation import ContinuationDispatcher


def media_group_has_executed_job(items: list[dict]) -> bool:
    return any(item.get("job_id") for item in items)


def media_group_needs_agent_continuation(items: list[dict]) -> bool:
    return media_group_has_executed_job(items) or any(item.get("status")=="expired" for item in items)


class MediaContinuationDeferred(RuntimeError):
    pass


async def supervise_warm_pool(state,pool,*,initial_delay:float=.5,
                              maximum_delay:float=30,probe_interval:float=30)->None:
    delay=initial_delay
    while True:
        state.state.pool_attempts+=1
        fill_error=None
        try:await pool.fill()
        except asyncio.CancelledError:raise
        except Exception as exc:fill_error=exc
        try:available=await pool.execution_available()
        except asyncio.CancelledError:raise
        except Exception as exc:
            state.state.execution_backend_available=False
            state.state.pool_status="unavailable"
            state.state.pool_last_error="probe:"+type(exc).__name__
            await asyncio.sleep(delay);delay=min(delay*2,maximum_delay);continue
        state.state.execution_backend_available=available
        if fill_error:
            state.state.pool_status="degraded" if available else "unavailable"
            state.state.pool_last_error=type(fill_error).__name__
            await asyncio.sleep(delay);delay=min(delay*2,maximum_delay)
        else:
            state.state.pool_status="ready" if available else "unavailable"
            state.state.pool_last_error="" if available else "execution_unavailable"
            delay=initial_delay
            await asyncio.sleep(probe_interval if available else delay)


class PlatformWorker:
    leader_identity="platform-coordinator"

    def __init__(self,settings:Settings,components:PlatformComponents|None=None):
        self.settings=settings
        self.components=components or build_platform_components(settings,execution_enabled=True)
        self.store=self.components.store
        self.pool=self.components.pool
        self.broker=self.components.broker
        self.discovery=self.components.discovery
        self.owner="platform-worker-"+uuid.uuid4().hex
        self.stop_event=asyncio.Event()
        self.leader_generation:int|None=None
        self.media_outbox_status="stopped"
        self.media_outbox_last_error=""
        self.media_outbox_failures=0
        self.state=SimpleNamespace(pool_status="starting",pool_attempts=0,pool_last_error="",
            execution_backend_available=False)
        self.tasks:set[asyncio.Task]=set()
        self.leader_tasks:set[asyncio.Task]=set()
        self.inspector=SandboxInspector(settings,self.store,self.components.objects,self.pool,
            self.broker.runtime,self.broker)

    async def _wait(self,seconds:float)->None:
        try:await asyncio.wait_for(self.stop_event.wait(),timeout=seconds)
        except asyncio.TimeoutError:pass

    async def _domain_protocols(self)->None:
        cleanup=OwnershipCleanup(self.store.core.ownership,self.pool.backend,self.pool)
        delay=self.settings.supervisor_retry_initial_seconds
        while not self.stop_event.is_set():
            try:
                await self.store.relay_pending()
                await self.store.advance_lifecycle_operations()
                await cleanup.run_once()
                await cleanup.run_retained_once()
                delay=self.settings.supervisor_retry_initial_seconds
            except asyncio.CancelledError:raise
            except Exception as exc:
                event("domain.protocol.failed",level=40,exc=exc)
                delay=min(delay*2,self.settings.supervisor_retry_max_seconds)
            await self._wait(max(.1,delay))

    async def _draft_expiry(self)->None:
        while not self.stop_event.is_set():
            try:await self.store.expire_job_group_drafts()
            except asyncio.CancelledError:raise
            except Exception as exc:event("job_group_draft.expiry_failed",level=40,exc=exc)
            await self._wait(5)

    async def _catalog_refresh(self)->None:
        while not self.stop_event.is_set():
            try:await self.discovery.refresh()
            except asyncio.CancelledError:raise
            except Exception as exc:event("model.catalog.refresh_failed",level=30,exc=exc)
            await self._wait(self.settings.model_catalog_refresh_seconds)

    async def _orphan_reaper(self)->None:
        while not self.stop_event.is_set():
            try:await self.broker.cleanup_startup_orphans()
            except asyncio.CancelledError:raise
            except Exception as exc:event("sandbox.orphan.reconcile_failed",level=40,exc=exc)
            await self._wait(self.settings.orphan_reaper_seconds)

    async def _object_gc(self)->None:
        async def audit(value):
            event('object.gc.deleted',object_digest=value['object_ref'].removeprefix('sha256:'),
                bytes=value['size'],reason=value['reason'])
        collector=ObjectGarbageCollector(self.components.objects,
            lambda:authoritative_object_references(self.store),audit=audit)
        while not self.stop_event.is_set():
            try:
                result=await collector.run(grace_seconds=self.settings.object_gc_grace_seconds,
                    dry_run=self.settings.object_gc_dry_run)
                event('object.gc.completed',count=result['deleted_count'],
                    bytes=result['bytes_deleted'],partial=result['dry_run'])
            except asyncio.CancelledError:raise
            except Exception as exc:event('object.gc.failed',level=40,exc=exc)
            await self._wait(self.settings.object_gc_interval_seconds)

    async def _warm_pool(self)->None:
        await supervise_warm_pool(self,self.pool,
            initial_delay=self.settings.supervisor_retry_initial_seconds,
            maximum_delay=self.settings.supervisor_retry_max_seconds)

    async def _leader_renewal(self,generation:int)->None:
        lease=max(6,self.settings.platform_leader_lease_seconds)
        while not self.stop_event.is_set():
            await self._wait(lease/3)
            if self.stop_event.is_set():return
            if not await self.store.core.renew_platform_leader(
                    self.leader_identity,self.owner,generation,lease):
                raise RuntimeError("platform_leader_lost")

    async def _run_as_leader(self,generation:int)->None:
        self.leader_generation=generation
        await self.inspector.start()
        loops=[self._domain_protocols(),self._draft_expiry(),self._orphan_reaper(),
            self._leader_renewal(generation)]
        if self.settings.model_catalog_discovery:loops.append(self._catalog_refresh())
        if self.pool.backend.provider!="fake":loops.append(self._warm_pool())
        if self.settings.object_gc_enabled:loops.append(self._object_gc())
        self.leader_tasks={asyncio.create_task(value) for value in loops}
        event("platform.leader.acquired",worker_id=self.owner,generation=generation)
        try:
            done,_=await asyncio.wait(self.leader_tasks,return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.exception():raise task.exception()
        finally:
            for task in self.leader_tasks:task.cancel()
            await asyncio.gather(*self.leader_tasks,return_exceptions=True)
            self.leader_tasks.clear()
            await self.inspector.close()
            await self.store.core.release_platform_leader(
                self.leader_identity,self.owner,generation)
            self.leader_generation=None
            event("platform.leader.released",worker_id=self.owner,generation=generation)

    async def _leader_election(self)->None:
        lease=max(6,self.settings.platform_leader_lease_seconds)
        while not self.stop_event.is_set():
            generation=await self.store.core.claim_platform_leader(
                self.leader_identity,self.owner,lease)
            if generation is None:
                await self._wait(lease/3);continue
            try:await self._run_as_leader(generation)
            except asyncio.CancelledError:raise
            except Exception as exc:
                event("platform.leader.failed",level=40,worker_id=self.owner,
                    generation=generation,exc=exc)
                await self._wait(self.settings.supervisor_retry_initial_seconds)

    async def _continue_media(self,batch:dict)->None:
        turn_id=batch.get("turn_id") or ""
        if not turn_id:return
        get_control=getattr(self.store,"get_thread_control",None)
        if get_control:
            control=await get_control(batch["thread_id"])
            if control.state!=ThreadControlState.RUNNING:
                raise MediaContinuationDeferred(control.state.value)
        ready,jobs=await self.store.media_continuation_group(turn_id)
        if not ready:raise RuntimeError("media_group_pending")
        promote=[item for item in jobs if item["status"]=="succeeded"
            and item["output_role"] in {"workspace","delivery"}
            and item["job_id"] and not item["project_path"]]
        for item in promote:
            await self.store.promote_media_job_for_thread(item["job_id"],batch["thread_id"],
                item["output_role"])
        if promote:
            ready,jobs=await self.store.media_continuation_group(turn_id)
            if (not ready or any(item["status"]=="succeeded"
                    and item["output_role"] in {"workspace","delivery"}
                    and not item["project_path"] for item in jobs)):
                raise RuntimeError("media_promotion_pending")
        if not media_group_needs_agent_continuation(jobs):return
        results=[]
        for item in jobs:
            candidate=(item["status"]=="succeeded" and item["output_role"]=="candidate"
                and bool(item["job_id"]))
            results.append({"draft_id":item["draft_id"],"job_id":item["job_id"],
                "kind":item["kind"],"model":item["model"],"status":item["status"],
                "output_role":item["output_role"],
                "path":"/workspace/project/"+item["project_path"] if item["project_path"] else "",
                "inspect_command":f"sandbox-media fetch --job-id {item['job_id']} --output /workspace/outputs/{item['job_id']}"
                    if candidate else "","error":item["error_code"] or "",
                "error_reason":item.get("error_reason") or "",
                "error_guidance":item.get("error_guidance") or ""})
        expired_only=bool(results) and all(item["status"]=="expired" for item in results)
        if expired_only:
            message=("[media_group_expired] The media drafts expired before confirmation. "
                "Recreate the same validated drafts and request confirmation. Results: "+
                json.dumps(results,ensure_ascii=False,sort_keys=True))
        else:
            message=("[media_group_completed] Continue the original user task using only successful "
                "results with paths or inspect commands. Results: "+
                json.dumps(results,ensure_ascii=False,sort_keys=True))
        origin=await self.store.get_turn(turn_id)
        with operation("media.continuation.dispatch",new_trace=True,
                thread_id=batch["thread_id"],turn_id=turn_id):
            if origin:add_link(canonical_traceparent(origin.last_traceparent)
                or canonical_traceparent(origin.submit_traceparent))
            try:
                await self.broker.submit(batch["thread_id"],TurnCreate(message=message,
                    idempotency_key=f"media-continuation-{turn_id}",delivery_mode="queue"),
                    network_access=origin.network_access if origin else "restricted",internal=True)
            except RuntimeError as exc:
                if str(exc)=="thread_paused":raise MediaContinuationDeferred("PAUSED") from exc
                raise

    async def _media_outbox(self)->None:
        owner="media-outbox-"+self.owner
        delay=self.settings.supervisor_retry_initial_seconds
        self.media_outbox_status="running"
        while not self.stop_event.is_set():
            try:batches=await self.store.claim_media_batches(owner)
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.media_outbox_status="degraded"
                self.media_outbox_last_error=type(exc).__name__
                self.media_outbox_failures+=1
                event("media.outbox.claim_failed",level=40,exc=exc,
                    failures=self.media_outbox_failures)
                await self._wait(delay)
                delay=min(delay*2,self.settings.supervisor_retry_max_seconds)
                continue
            iteration_error=None
            for batch in batches:
                try:
                    try:await self._continue_media(batch)
                    except asyncio.CancelledError:raise
                    except MediaContinuationDeferred as exc:
                        await self.store.finish_media_batch(batch["turn_id"],owner,
                            delivered=False,error="thread_paused",waiting_thread_resume=True)
                        event("media.continuation.deferred",thread_id=batch["thread_id"],
                            turn_id=batch["turn_id"],state_after="WAITING_THREAD_RESUME",
                            error_code="thread_paused",outcome="deferred")
                    except Exception as exc:
                        permanent=isinstance(exc,RuntimeError) and str(exc)=="thread_archived"
                        await self.store.finish_media_batch(batch["turn_id"],owner,
                            delivered=False,error=type(exc).__name__,permanent=permanent)
                    else:await self.store.finish_media_batch(
                        batch["turn_id"],owner,delivered=True)
                except asyncio.CancelledError:raise
                except Exception as exc:
                    iteration_error=exc;break
            if iteration_error:
                self.media_outbox_status="degraded"
                self.media_outbox_last_error=type(iteration_error).__name__
                self.media_outbox_failures+=1
                event("media.outbox.finish_failed",level=40,exc=iteration_error,
                    failures=self.media_outbox_failures)
                await self._wait(delay)
                delay=min(delay*2,self.settings.supervisor_retry_max_seconds)
                continue
            self.media_outbox_status="running";self.media_outbox_last_error=""
            delay=self.settings.supervisor_retry_initial_seconds
            await self._wait(2)

    async def run(self)->None:
        await self.store.initialize()
        await self.pool.restore_allocation_capacity(
            await self.store.core.ownership.capacity_obligations())
        await self.broker.start(start_reaper=False)
        continuation=ContinuationDispatcher(self.store,self.broker)
        self.tasks={asyncio.create_task(continuation.run(),name="job-continuation"),
            asyncio.create_task(self._media_outbox(),name="media-continuation"),
            asyncio.create_task(self._leader_election(),name="platform-leader")}
        try:
            stop=asyncio.create_task(self.stop_event.wait())
            done,_=await asyncio.wait((*self.tasks,stop),return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop and task.exception():raise task.exception()
        finally:
            self.stop_event.set();continuation.stop_event.set()
            for task in self.tasks:task.cancel()
            await asyncio.gather(*self.tasks,return_exceptions=True)
            preserved=await self.broker.close()
            await self.pool.close(preserve_sandbox_keys=(preserved
                | await self.store.active_sandbox_keys()))
            await self.pool.backend.aclose()
            await self.store.close()


async def serve(settings:Settings,*,stop_event:asyncio.Event|None=None)->None:
    worker=PlatformWorker(settings)
    if stop_event is not None:worker.stop_event=stop_event
    else:
        loop=asyncio.get_running_loop()
        for name in (signal.SIGINT,signal.SIGTERM):
            with suppress(NotImplementedError):loop.add_signal_handler(name,worker.stop_event.set)
    await worker.run()


def run()->None:
    settings=load_settings();validate_service_settings(settings)
    configure("platform-worker",settings.data_root/"logs",settings)
    asyncio.run(serve(settings))


if __name__=="__main__":run()
