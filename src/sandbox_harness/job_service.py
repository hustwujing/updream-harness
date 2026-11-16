from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal
import time

from .config import Settings, load_settings
from .domain import Harness
from .harness_job import HarnessJobAdapter, JobGrantFactory, OpenSandboxChildBackend
from .job_worker import JobWorker
from .node_agent import NodeAgent
from .media_catalog import MediaCatalog
from .media_job_adapter import MediaProviderJobAdapter
from .media_render_adapter import MediaRenderOperationAdapter
from .objects import object_store
from .pool import SandboxPool
from .profiles import ProfileResolver
from .reviewer import ReviewerJobAdapter
from .runtime import RuntimeClient
from .sandbox import FakeSandboxBackend, OpenSandboxBackend
from .scheduler import NodeProfile, Scheduler
from .store import Store


def resolved_node_profiles(resolver: ProfileResolver) -> tuple[NodeProfile, ...]:
    values=[]
    for profile in sorted(resolver.runtimes.values(),key=lambda row:row.id):
        if set(profile.image_refs)!=set(profile.supported_harnesses):
            raise ValueError("Runtime Profile image matrix is not complete")
        for harness in profile.supported_harnesses:
            digest=profile.image_digests[harness];reference=profile.image_refs[harness]
            if not reference.endswith("@sha256:"+digest):
                raise ValueError("Runtime Profile image is not digest-pinned")
            values.append(NodeProfile(runtime_profile_id=profile.id,harness=harness,
                image_digest=digest,image_ref=reference,warm_slots=0))
    if not values:raise ValueError("Runtime Profile image matrix is empty")
    return tuple(values)


def profile_resolver(settings: Settings) -> ProfileResolver:
    raw=settings.profile_catalog_path.read_text()
    manifest=json.loads((Path(__file__).resolve().parents[2]/"runtime/skills/manifest.json").read_text())
    return ProfileResolver.from_json(raw,expected_digest=(settings.profile_catalog_digest
        or hashlib.sha256(raw.encode()).hexdigest()),skill_manifest=manifest)


def sandbox_pool(settings: Settings) -> SandboxPool:
    backend=(FakeSandboxBackend(namespace=settings.namespace) if settings.backend=="fake" else
        OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,
            settings.sandbox_timeout_seconds,settings.namespace,settings.gateway_url,
            settings.gateway_proxy_port))
    return SandboxPool(backend,{harness:settings.image_for(harness.value) for harness in Harness},
        {harness:0 for harness in Harness},max_sandboxes=settings.max_sandboxes)


async def serve(settings: Settings, *, stop_event: asyncio.Event | None = None) -> None:
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    pool=sandbox_pool(settings);objects=object_store(settings)
    runtime=RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds,
        reattach_attempts=settings.runtime_stream_reattach_attempts,
        reattach_max_delay_seconds=settings.runtime_stream_reattach_max_delay_seconds,
        bundle_dir=settings.runtime_bundle_dir,default_bundle_digest=settings.runtime_bundle_digest,
        code_bundle_dir=settings.runtime_code_bundle_dir,
        default_code_bundle_digest=settings.runtime_code_bundle_digest)
    scheduler=Scheduler(store)
    child_backend=OpenSandboxChildBackend(pool=pool,runtime=runtime,store=store,objects=objects,
        settings=settings,token_factory=JobGrantFactory(settings))
    harness_adapter=HarnessJobAdapter(scheduler,child_backend)
    media_catalog=MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key))
    media_adapter=MediaProviderJobAdapter(settings,store,objects,media_catalog)
    render_adapter=MediaRenderOperationAdapter(scheduler=scheduler,pool=pool,runtime=runtime,
        store=store,objects=objects,settings=settings,token_factory=JobGrantFactory(settings))
    worker=JobWorker(store,{"child-harness":harness_adapter,
        "reviewer-harness":ReviewerJobAdapter(harness_adapter),
        "image.generate":media_adapter,"media.render":render_adapter,
        "audio.process":render_adapter},node_id=settings.job_node_id,
        concurrency=settings.job_worker_concurrency,lease_seconds=settings.job_lease_seconds)
    profiles=resolved_node_profiles(profile_resolver(settings))
    cpu_total=max(1,min(os.cpu_count() or 1,settings.job_node_cpu_limit))
    memory_total=max(128,min(int(os.sysconf("SC_PAGE_SIZE")*os.sysconf("SC_PHYS_PAGES")//(1024*1024)),
        settings.job_node_memory_mb_limit))
    node=NodeAgent(scheduler,node_id=settings.job_node_id,generation=int(time.time()),profiles=profiles,
        data_root=settings.data_root,cpu_total=cpu_total,memory_mb_total=memory_total,
        heartbeat_seconds=settings.job_node_heartbeat_seconds,
        cached_bundle_digests=tuple(value for value in (settings.runtime_bundle_digest,
            settings.runtime_code_bundle_digest) if value))
    await store.initialize();await node.run_once()
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
        preserved={value.id for value in child_backend.sessions.values()}
        await pool.close(preserve_sandbox_ids=preserved)
        client=getattr(pool.backend,"client",None)
        if client:await client.aclose()
        await store.close()


def run() -> None:
    asyncio.run(serve(load_settings()))
