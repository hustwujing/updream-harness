from __future__ import annotations

import asyncio
import os
from pathlib import Path
import signal
import time
import threading

from sandbox_harness.common.configuration.config import (Settings, load_settings)
from sandbox_harness.services.controlled_worker.controlled_media_adapter import (ControlledMediaOperationAdapter)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from sandbox_harness.common.scheduling.node_agent import (NodeAgent, discover_gpus)
from sandbox_harness.common.storage.objects import (object_store)
from sandbox_harness.common.observability.observability import (configure, event)
from sandbox_harness.common.scheduling.scheduler import (NodeProfile, Scheduler)
from sandbox_harness.common.db.store import (Store)
from sandbox_harness.services.controlled_worker.controlled_media_runner import (probe_controlled_gpu)


REQUIRED_RUNTIME_PATHS=(
    'envs/sam3/bin/python','envs/face-makeup/bin/python','lib-director-vF/src',
    'models/sam3/sam3.pt','models/ZIM/zim-anything-vitl/zim_vit_l_2092',
    'envs/media-foundation/bin/python','models/media-foundation/faster-whisper-small/model.bin',
    'models/sam3/sam3/__init__.py','models/ZIM/source/zim_anything/__init__.py',
    'models/ZIM/source/zim_config/__init__.py',
)


class ControlledGPUProvider:
    def __init__(self,runtime:Path,data_root:Path,*,refresh_seconds:float=300):
        self.runtime=runtime;self.work_root=data_root/'controlled-gpu-probes'
        self.refresh_seconds=refresh_seconds;self.next_refresh=0.0
        self.healthy:set[str]=set();self.generation=0;self.lock=threading.Lock()

    def __call__(self):
        values=discover_gpus();timestamp=time.monotonic()
        with self.lock:
            if timestamp>=self.next_refresh:
                healthy={value.id for value in values if probe_controlled_gpu(
                    value.id.removeprefix('gpu-'),self.runtime,self.work_root)}
                if healthy!=self.healthy:
                    self.healthy=healthy;self.generation+=1
                    event('controlled.gpu_health.changed',generation=self.generation,
                        healthy_count=len(healthy),discovered_count=len(values))
                self.next_refresh=timestamp+self.refresh_seconds
            return tuple(value for value in values if value.id in self.healthy)

    def invalidate(self,gpu_ids)->None:
        with self.lock:
            previous=set(self.healthy);self.healthy.difference_update(gpu_ids)
            if self.healthy!=previous:
                self.generation+=1
                event('controlled.gpu_health.changed',generation=self.generation,
                    healthy_count=len(self.healthy),discovered_count=len(previous))
            self.next_refresh=max(self.next_refresh,time.monotonic()+self.refresh_seconds)


def validate_runtime(settings:Settings)->Path:
    declared=settings.controlled_runtime_root.absolute();data_root=settings.data_root.resolve()
    if not declared.is_relative_to(data_root) or declared==data_root:
        raise ValueError('Controlled Runtime entry must remain below Parallel data root')
    root=declared.resolve()
    if settings.backend!='fake' and not root.is_relative_to(Path('/data2/wujing03')):
        raise ValueError('Controlled Runtime target must remain below /data2/wujing03')
    if not settings.controlled_runtime_digest:raise ValueError('controlled_runtime_digest_required')
    missing=[value for value in REQUIRED_RUNTIME_PATHS if not (root/value).exists()]
    if missing:raise ValueError('Controlled Runtime is incomplete: '+','.join(missing))
    return root


async def serve(settings:Settings,*,stop_event:asyncio.Event|None=None)->None:
    if os.geteuid()!=0:raise RuntimeError('controlled_worker_requires_root')
    runtime=validate_runtime(settings);store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    objects=object_store(settings);scheduler=Scheduler(store)
    gpu_provider=ControlledGPUProvider(runtime,settings.data_root)
    adapter=ControlledMediaOperationAdapter(scheduler=scheduler,store=store,objects=objects,
        settings=settings,gpu_health=gpu_provider)
    worker=JobWorker(store,{'image.segment':adapter,'audio.transcribe':adapter},
        node_id=settings.controlled_node_id,concurrency=settings.controlled_worker_concurrency,
        recovery_concurrency=settings.job_recovery_concurrency,
        lease_seconds=settings.job_lease_seconds)
    cpu_total=max(1,min(os.cpu_count() or 1,settings.controlled_node_cpu_limit))
    memory_total=max(4096,min(int(os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES')//(1024*1024)),
        settings.controlled_node_memory_mb_limit))
    profile=NodeProfile(runtime_profile_id='host-controlled',harness='codex',
        image_digest=settings.controlled_runtime_digest,
        image_ref='controlled-runtime@sha256:'+settings.controlled_runtime_digest)
    node=NodeAgent(scheduler,node_id=settings.controlled_node_id,generation=int(time.time()),
        profiles=(profile,),data_root=settings.data_root,cpu_total=cpu_total,memory_mb_total=memory_total,
        heartbeat_seconds=settings.job_node_heartbeat_seconds,
        cached_bundle_digests=(settings.controlled_runtime_digest,),
        gpu_provider=gpu_provider)
    await store.initialize();await node.run_once()
    try:
        released=await scheduler.reap_stale_reservations(node.node_id,node.generation,set())
        if released:event('controlled.scheduler.stale_reservations.released',count=len(released))
    except Exception as exc:
        event('controlled.scheduler.reconcile_failed',level=40,exception_type=type(exc).__name__)
    stop=stop_event or asyncio.Event();loop=asyncio.get_running_loop()
    if stop_event is None:
        for name in (signal.SIGTERM,signal.SIGINT):
            try:loop.add_signal_handler(name,stop.set)
            except NotImplementedError:pass
    node_task=asyncio.create_task(node.run(),name='controlled-node-agent')
    worker_task=asyncio.create_task(worker.run(),name='controlled-job-worker')
    stop_task=asyncio.create_task(stop.wait(),name='controlled-service-stop')
    try:
        done,_=await asyncio.wait((node_task,worker_task,stop_task),return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is not stop_task and task.exception():raise task.exception()
    finally:
        node.stop_event.set();worker.stop_event.set();stop.set()
        await asyncio.gather(node_task,worker_task,return_exceptions=True)
        stop_task.cancel();await asyncio.gather(stop_task,return_exceptions=True)
        await store.close()


def run()->None:
    settings=load_settings();configure('controlled-worker',settings.data_root/'logs')
    asyncio.run(serve(settings))
