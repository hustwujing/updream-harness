from pathlib import Path

from sandbox_harness.common.scheduling.node_agent import (NodeAgent, discover_gpus)
from sandbox_harness.services.controlled_worker.controlled_service import (ControlledGPUProvider)
from sandbox_harness.common.scheduling.scheduler import (GPUInventory)


def test_gpu_inventory_comes_from_nvidia_smi_identity_not_configured_count(monkeypatch):
    class Result:
        returncode=0;stderr='';stdout=('0, NVIDIA A100-SXM4-40GB, 40960, 32000, 8, Exclusive_Process, Disabled\n'
            '1, NVIDIA A100-SXM4-40GB, 40960, 12000, 72, Default, Enabled\n')
    monkeypatch.setattr('sandbox_harness.common.scheduling.node_agent.shutil.which',lambda name:'/usr/bin/nvidia-smi')
    monkeypatch.setattr('sandbox_harness.common.scheduling.node_agent.subprocess.run',lambda *args,**kwargs:Result())
    values=discover_gpus()
    assert [(value.id,value.type,value.memory_mb,value.memory_free_mb,
        value.utilization_percent,value.mode) for value in values]==[
        ('gpu-0','NVIDIA A100-SXM4-40GB',40960,32000,8,'exclusive'),
        ('gpu-1','NVIDIA A100-SXM4-40GB',40960,12000,72,'mig')]


def test_gpu_inventory_skips_invalid_and_duplicate_samples(monkeypatch):
    class Result:
        returncode=0;stderr='';stdout=(
            '0, NVIDIA A100, 40960, 32000, 8, Default, Disabled\n'
            '0, duplicate sample, 40960, 12000, 72, Default, Disabled\n'
            '1, invalid utilization, 40960, 12000, 101, Default, Disabled\n'
            '2, unavailable sample, [N/A], [N/A], [N/A], Default, Disabled\n')
    monkeypatch.setattr('sandbox_harness.common.scheduling.node_agent.shutil.which',lambda name:'/usr/bin/nvidia-smi')
    monkeypatch.setattr('sandbox_harness.common.scheduling.node_agent.subprocess.run',lambda *args,**kwargs:Result())
    values=discover_gpus()
    assert [(value.id,value.type) for value in values]==[('gpu-0','NVIDIA A100')]


def test_controlled_inventory_advertises_only_exactly_probed_gpus(tmp_path,monkeypatch):
    values=(GPUInventory(id='gpu-0',type='RTX 4090',memory_mb=49140),
        GPUInventory(id='gpu-1',type='RTX 4090',memory_mb=49140))
    monkeypatch.setattr('sandbox_harness.services.controlled_worker.controlled_service.discover_gpus',lambda:values)
    monkeypatch.setattr('sandbox_harness.services.controlled_worker.controlled_service.probe_controlled_gpu',
        lambda index,runtime,root:index=='1')
    provider=ControlledGPUProvider(tmp_path/'runtime',tmp_path,refresh_seconds=300)
    assert [value.id for value in provider()]==['gpu-1']
    assert provider.healthy=={'gpu-1'} and provider.generation==1
    provider.invalidate(('gpu-1',));assert provider()==() and provider.generation==2


async def test_node_agent_registers_dynamic_inventory_under_data2(tmp_path,monkeypatch):
    calls=[]
    class Scheduler:
        async def register_node(self,value):calls.append(value)
    monkeypatch.setattr('sandbox_harness.common.scheduling.node_agent.discover_gpus',lambda:())
    agent=NodeAgent(Scheduler(),node_id='node',generation=1,profiles=({'runtime_profile_id':'general-cpu',
        'harness':'codex','image_digest':'a'*64,'warm_slots':1},),data_root=tmp_path,
        cpu_total=8,memory_mb_total=16000)
    value=await agent.run_once()
    assert value.node_id=='node' and value.disk_mb_total>0 and calls==[value]


async def test_node_agent_keeps_cpu_inventory_when_gpu_probe_is_transiently_invalid(tmp_path):
    calls=[]
    class Scheduler:
        async def register_node(self,value):calls.append(value)
    def failed_probe():raise ValueError('transient invalid GPU telemetry')
    agent=NodeAgent(Scheduler(),node_id='node',generation=1,profiles=({'runtime_profile_id':'general-cpu',
        'harness':'codex','image_digest':'a'*64,'warm_slots':1},),data_root=tmp_path,
        cpu_total=8,memory_mb_total=16000,gpu_provider=failed_probe)
    value=await agent.run_once()
    assert value.gpus==() and calls==[value]
