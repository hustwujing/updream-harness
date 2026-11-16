from pathlib import Path

from sandbox_harness.node_agent import NodeAgent,discover_gpus


def test_gpu_inventory_comes_from_nvidia_smi_identity_not_configured_count(monkeypatch):
    class Result:
        returncode=0;stderr='';stdout='0, NVIDIA A100-SXM4-40GB, 40960, Exclusive_Process, Disabled\n1, NVIDIA A100-SXM4-40GB, 40960, Default, Enabled\n'
    monkeypatch.setattr('sandbox_harness.node_agent.shutil.which',lambda name:'/usr/bin/nvidia-smi')
    monkeypatch.setattr('sandbox_harness.node_agent.subprocess.run',lambda *args,**kwargs:Result())
    values=discover_gpus()
    assert [(value.id,value.type,value.memory_mb,value.mode) for value in values]==[
        ('gpu-0','NVIDIA A100-SXM4-40GB',40960,'exclusive'),
        ('gpu-1','NVIDIA A100-SXM4-40GB',40960,'mig')]


async def test_node_agent_registers_dynamic_inventory_under_data2(tmp_path,monkeypatch):
    calls=[]
    class Scheduler:
        async def register_node(self,value):calls.append(value)
    monkeypatch.setattr('sandbox_harness.node_agent.discover_gpus',lambda:())
    agent=NodeAgent(Scheduler(),node_id='node',generation=1,profiles=({'runtime_profile_id':'general-cpu',
        'harness':'codex','image_digest':'a'*64,'warm_slots':1},),data_root=tmp_path,
        cpu_total=8,memory_mb_total=16000)
    value=await agent.run_once()
    assert value.node_id=='node' and value.disk_mb_total>0 and calls==[value]
