from __future__ import annotations

import asyncio
import shutil
import subprocess
from pathlib import Path
from typing import Callable

from .scheduler import GPUInventory, NodeInventory, NodeProfile


def discover_gpus() -> tuple[GPUInventory, ...]:
    executable=shutil.which('nvidia-smi')
    if not executable:return ()
    try:
        result=subprocess.run([executable,
            '--query-gpu=index,name,memory.total,memory.free,utilization.gpu,compute_mode,mig.mode.current',
            '--format=csv,noheader,nounits'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
            timeout=10,check=False)
    except (OSError,subprocess.SubprocessError):
        return ()
    if result.returncode:return ()
    values={}
    for line in result.stdout.splitlines():
        parts=[item.strip() for item in line.split(',')]
        if len(parts)!=7:continue
        index,name,memory,memory_free,utilization,compute_mode,mig_mode=parts
        try:
            memory_mb=int(memory);memory_free_mb=int(memory_free);utilization_percent=int(utilization)
            mode=('mig' if mig_mode.lower() in {'enabled','enable'} else
                'exclusive' if 'exclusive' in compute_mode.lower() else 'shared')
            value=GPUInventory(id='gpu-'+index,type=name,memory_mb=memory_mb,
                memory_free_mb=memory_free_mb,utilization_percent=utilization_percent,mode=mode)
        except ValueError:continue
        values.setdefault(value.id,value)
    return tuple(values.values())


def disk_capacity_mb(path: Path) -> int:
    return int(shutil.disk_usage(path).free//(1024*1024))


class NodeAgent:
    def __init__(self,scheduler,*,node_id:str,generation:int,profiles:tuple[NodeProfile,...],
                 data_root:Path,cpu_total:int,memory_mb_total:int,heartbeat_seconds:float=10,
                 cached_bundle_digests:tuple[str,...]=(),
                 gpu_provider:Callable[[],tuple[GPUInventory,...]]|None=None):
        self.scheduler=scheduler;self.node_id=node_id;self.generation=generation;self.profiles=profiles
        self.data_root=data_root;self.cpu_total=cpu_total;self.memory_mb_total=memory_mb_total
        self.heartbeat_seconds=heartbeat_seconds;self.cached_bundle_digests=cached_bundle_digests
        self.gpu_provider=gpu_provider or discover_gpus
        self.stop_event=asyncio.Event()

    def inventory(self)->NodeInventory:
        try:load=__import__('os').getloadavg()[0]/max(1,self.cpu_total)
        except OSError:load=0
        try:gpus=self.gpu_provider()
        except (OSError,subprocess.SubprocessError,ValueError):gpus=()
        return NodeInventory(node_id=self.node_id,generation=self.generation,cpu_total=self.cpu_total,
            memory_mb_total=self.memory_mb_total,disk_mb_total=disk_capacity_mb(self.data_root),
            load=max(0,load),profiles=self.profiles,gpus=gpus,
            cached_bundle_digests=self.cached_bundle_digests)

    async def run_once(self)->NodeInventory:
        value=await asyncio.to_thread(self.inventory);await self.scheduler.register_node(value);return value

    async def run(self)->None:
        while not self.stop_event.is_set():
            await self.run_once()
            try:await asyncio.wait_for(self.stop_event.wait(),timeout=self.heartbeat_seconds)
            except asyncio.TimeoutError:pass
