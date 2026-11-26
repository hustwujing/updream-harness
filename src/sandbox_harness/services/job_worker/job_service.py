from __future__ import annotations

from sandbox_harness.common.configuration.resources import resource_path

import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal
import time

from sandbox_harness.common.configuration.config import (Settings, load_settings)
from sandbox_harness.common.sandbox.allocation_client import (AllocationClient)
from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.services.job_worker.harness_job import (HarnessJobAdapter, JobGrantFactory, SandboxChildBackend)
from sandbox_harness.services.job_worker.image_decompose_adapter import (ImageDecomposeAdapter)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from sandbox_harness.common.catalog.job_catalog import (JobCatalog)
from sandbox_harness.common.jobs.job_coordinator import (JobCoordinator)
from sandbox_harness.common.scheduling.node_agent import (NodeAgent)
from sandbox_harness.common.catalog.media_catalog import (MediaCatalog)
from sandbox_harness.services.job_worker.media_job_adapter import (CAPABILITY_SCOPES, MediaProviderJobAdapter)
from sandbox_harness.services.job_worker.media_render_adapter import (MediaRenderOperationAdapter)
from sandbox_harness.common.storage.objects import (object_store)
from sandbox_harness.common.observability.observability import (configure, event)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.catalog.profiles import (ProfileResolver)
from sandbox_harness.services.job_worker.reviewer import (ReviewerJobAdapter)
from sandbox_harness.common.jobs.review import (ReviewerCoordinator, ReviewSubmission, ReviewTrigger)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
import sandbox_harness.common.sandbox.sandbox_factory as sandbox_factory
from sandbox_harness.common.scheduling.scheduler import (NodeProfile, Scheduler)
from sandbox_harness.common.db.store import (Store)


def resolved_node_profiles(resolver: ProfileResolver, backend=None) -> tuple[NodeProfile, ...]:
    values=[]
    for profile in sorted(resolver.runtimes.values(),key=lambda row:row.id):
        for harness in profile.supported_harnesses:
            release=profile.release(resolver.provider,harness)
            digest=release.artifact_digest;reference=release.ref
            if backend:backend.validate_release(reference,digest)
            values.append(NodeProfile(runtime_profile_id=profile.id,harness=harness,
                image_digest=digest,image_ref=reference,warm_slots=0))
    if not values:raise ValueError("Runtime Profile image matrix is empty")
    return tuple(values)


def profile_resolver(settings: Settings) -> ProfileResolver:
    raw=settings.profile_catalog_path.read_text()
    manifest=json.loads(resource_path("runtime/skills/manifest.json").read_text())
    return ProfileResolver.from_json(raw,expected_digest=(settings.profile_catalog_digest
        or hashlib.sha256(raw.encode()).hexdigest()),skill_manifest=manifest,
        provider=sandbox_factory.release_provider(settings.backend))


def sandbox_pool(settings: Settings) -> SandboxPool:
    backend=sandbox_factory.build_sandbox_registry(settings)
    releases=sandbox_factory.primary_provider_releases(settings,backend.provider)
    profiles=sandbox_factory.build_profile_resolver(settings,backend,releases)
    return SandboxPool(backend,{harness:releases[harness].ref for harness in Harness},
        {harness:0 for harness in Harness},max_sandboxes=settings.max_sandboxes,
        profile_resolver=profiles,ttl_seconds=settings.sandbox_timeout_seconds)


async def serve(settings: Settings, *, stop_event: asyncio.Event | None = None) -> None:
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    pool=sandbox_pool(settings);objects=object_store(settings)
    runtime=RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds,
        reattach_attempts=settings.runtime_stream_reattach_attempts,
        reattach_max_delay_seconds=settings.runtime_stream_reattach_max_delay_seconds,
        bundle_dir=settings.runtime_bundle_dir,default_bundle_digest=settings.runtime_bundle_digest,
        code_bundle_dir=settings.runtime_code_bundle_dir,
        default_code_bundle_digest=settings.runtime_code_bundle_digest,
        staging_root=settings.data_root / "tmp" / "runtime-downloads",backend=pool.backend)
    scheduler=Scheduler(store)
    resolver=profile_resolver(settings)
    allocation_client=AllocationClient(store,pool.backend,
        recovery_seconds=settings.allocation_recovery_seconds,
        provision_ttl_seconds=settings.allocation_provision_ttl_seconds)
    child_backend=SandboxChildBackend(pool=pool,runtime=runtime,store=store,objects=objects,
        settings=settings,token_factory=JobGrantFactory(settings),profile_resolver=resolver,
        allocation_client=allocation_client)
    harness_adapter=HarnessJobAdapter(scheduler,child_backend)
    media_catalog=MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key))
    media_adapter=MediaProviderJobAdapter(settings,store,objects,media_catalog)
    render_adapter=MediaRenderOperationAdapter(scheduler=scheduler,pool=pool,runtime=runtime,
        store=store,objects=objects,settings=settings,token_factory=JobGrantFactory(settings),
        profile_resolver=resolver,allocation_client=allocation_client)
    decompose_adapter=ImageDecomposeAdapter(settings,store,objects)
    catalog_raw=settings.job_catalog_path.read_text()
    job_catalog=JobCatalog.from_json(catalog_raw,expected_digest=(settings.job_catalog_digest
        or hashlib.sha256(catalog_raw.encode()).hexdigest()))
    reviewer=ReviewerCoordinator(JobCoordinator(store,job_catalog,profile_resolver=resolver,
        draft_ttl_seconds=settings.media_draft_ttl_seconds))
    async def dispatch_policy_review(context,result):
        for contract in context.request.expected_outputs:
            if not contract.requires_review:continue
            artifacts=await store.list_job_artifacts(context.job.id,contract.artifact_role)
            if not artifacts:continue
            trigger=(ReviewTrigger.CAPABILITY_POLICY if context.request.kind.value=="provider"
                else ReviewTrigger.PUBLICATION_POLICY if contract.review_policy in {"optional","required"}
                else ReviewTrigger.PRIMARY)
            await reviewer.prepare_submission(context.job.owner_id,context.job.thread_id,
                context.job.origin_turn_id,ReviewSubmission(
                    artifact_ids=tuple(item.artifact_id for item in artifacts),
                    required=contract.review_policy!="optional",harness=settings.reviewer_harness,
                    model=settings.reviewer_model,rubric=("Review the frozen "+contract.artifact_role+
                        " collection against its declared output contract and deterministic validation evidence."),
                    idempotency_key=f"policy-review-{context.job.id}-{contract.artifact_role}"),trigger=trigger)
    worker=JobWorker(store,{"child-harness":harness_adapter,
        "reviewer-harness":ReviewerJobAdapter(harness_adapter,store),
        **{identity:media_adapter for identity in CAPABILITY_SCOPES},"media.render":render_adapter,
        "audio.process":render_adapter,"media.concat_sequence":render_adapter,
        "image.decompose":decompose_adapter},node_id=settings.job_node_id,
        concurrency=settings.job_worker_concurrency,lease_seconds=settings.job_lease_seconds,
        recovery_concurrency=settings.job_recovery_concurrency,
        review_dispatcher=dispatch_policy_review)
    profiles=resolved_node_profiles(resolver,pool.backend)
    cpu_total=max(1,min(os.cpu_count() or 1,settings.job_node_cpu_limit))
    memory_total=max(128,min(int(os.sysconf("SC_PAGE_SIZE")*os.sysconf("SC_PHYS_PAGES")//(1024*1024)),
        settings.job_node_memory_mb_limit))
    node=NodeAgent(scheduler,node_id=settings.job_node_id,generation=int(time.time()),profiles=profiles,
        data_root=settings.data_root,cpu_total=cpu_total,memory_mb_total=memory_total,
        heartbeat_seconds=settings.job_node_heartbeat_seconds,
        cached_bundle_digests=tuple(value for value in (settings.runtime_bundle_digest,
            settings.runtime_code_bundle_digest) if value))
    await store.initialize()
    validate=getattr(pool.backend,"validate_active_providers",None)
    if validate:validate(await store.active_sandbox_providers())
    await pool.restore_allocation_capacity(await store.job_sandbox_capacity())
    await node.run_once()
    try:
        live_job_ids=await store.pending_job_cleanup_ids()
        released=await scheduler.reap_stale_reservations(
            node.node_id,node.generation,live_job_ids)
        if released:event("scheduler.stale_reservations.released",count=len(released),node_id=node.node_id)
    except Exception as exc:
        event("scheduler.stale_reservations.reconcile_failed",level=40,node_id=node.node_id,
            exception_type=type(exc).__name__)
    stop=stop_event or asyncio.Event();loop=asyncio.get_running_loop()
    if stop_event is None:
        for name in (signal.SIGTERM,signal.SIGINT):
            try:loop.add_signal_handler(name,stop.set)
            except NotImplementedError:pass
    node_task=asyncio.create_task(node.run(),name="job-node-agent")
    worker_task=asyncio.create_task(worker.run(),name="durable-job-worker")
    stop_task=asyncio.create_task(stop.wait(),name="job-service-stop")
    try:
        done,_=await asyncio.wait((node_task,worker_task,stop_task),return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is not stop_task and task.exception():raise task.exception()
    finally:
        node.stop_event.set();worker.stop_event.set();stop.set()
        await asyncio.gather(node_task,worker_task,return_exceptions=True)
        stop_task.cancel();await asyncio.gather(stop_task,return_exceptions=True)
        preserved={pool.sandbox_key(value) for value in child_backend.sessions.values()}
        await pool.close(preserve_sandbox_keys=preserved)
        await pool.backend.aclose()
        await store.close()


def run() -> None:
    settings=load_settings();configure("job-worker",settings.data_root/"logs")
    asyncio.run(serve(settings))
