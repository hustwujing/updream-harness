"""Local deployment integration: use the installed dependency, never a GPU."""
import fcntl
import importlib.util
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

import pytest


ROOT=Path(__file__).resolve().parents[1]
SERVICE=ROOT/'.data/services/image-layered'
pytestmark=pytest.mark.skipif(not (SERVICE/'media_core/gpu.py').is_file(),
    reason='requires the locally materialized inference service')


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module);return module


@pytest.mark.parametrize('group',[False,True])
def test_shared_gpu_lock_is_read_only_and_still_exclusive(tmp_path,monkeypatch,group):
    service=tmp_path/'service-root'
    for relative in ['media_core/gpu.py','service/worker.py']:
        target=service/relative;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(SERVICE/relative,target)
    module=load('isolate_image_service',ROOT/'deploy/ali/isolate-image-service.py')
    module.isolate(service);module.isolate(service)
    gpu=load('isolated_gpu_dependency',service/'media_core/gpu.py')
    locks=tmp_path/'locks';locks.mkdir();lock=locks/'gpu-0.lock'
    lock.write_text('existing shared owner receipt\n');before=lock.stat().st_mtime_ns
    monkeypatch.setattr(gpu,'query_gpus',lambda:[SimpleNamespace(index=0,free_mb=100000,utilization=0)])
    lease=gpu.GPULeaseGroup(locks,(0,),1000,1) if group else gpu.GPULease(locks,(0,),1000)
    assert lease.acquire() in (0,(0,))
    with lock.open('r') as competing:
        with pytest.raises(BlockingIOError):fcntl.flock(competing,fcntl.LOCK_EX|fcntl.LOCK_NB)
    lease.release()
    assert lock.read_text()=='existing shared owner receipt\n'
    assert lock.stat().st_mtime_ns==before
    lock.unlink();assert lease.acquire() is None
    assert not lock.exists()
    assert '"--user-directory", str(user_dir)' in (service/'service/worker.py').read_text()
