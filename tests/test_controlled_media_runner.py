from pathlib import Path

import pytest

from sandbox_harness.controlled_media_runner import (
    THREAD_ENVIRONMENT,_base_args,_gpu_uuid,_thread_environment_args,probe_controlled_gpu,
)


def test_controlled_bwrap_exposes_read_only_gpu_system_topology(tmp_path):
    runtime=tmp_path/'platform'/'.data'/'provider-runtime'
    work=tmp_path/'work'
    args=_base_args(work,runtime)

    assert args[args.index('/sys')-1]=='--ro-bind'
    assert args[args.index('/sys')+1]=='/sys'
    assert '--unshare-net' in args
    assert args[args.index(str(runtime))-1]=='--ro-bind'


def test_reserved_gpu_index_resolves_to_host_uuid(monkeypatch):
    monkeypatch.setattr('sandbox_harness.controlled_media_runner.subprocess.check_output',
        lambda *args,**kwargs:'0, GPU-aaaa-bbbb\n7, GPU-cccc-dddd\n')

    assert _gpu_uuid('7')=='GPU-cccc-dddd'
    with pytest.raises(RuntimeError,match='segment_gpu_id_invalid'):_gpu_uuid('../7')
    with pytest.raises(RuntimeError,match='segment_gpu_uuid_missing'):_gpu_uuid('2')


def test_controlled_scientific_runtimes_have_bounded_thread_pools():
    args=_thread_environment_args()
    configured={args[index+1]:args[index+2] for index,value in enumerate(args) if value=='--setenv'}
    assert configured==THREAD_ENVIRONMENT
    assert all(int(value)<=4 for value in configured.values())


def test_controlled_gpu_probe_uses_exact_sam3_python_and_returns_health(tmp_path,monkeypatch):
    runtime=tmp_path/'runtime';(runtime/'envs/sam3/bin').mkdir(parents=True)
    observed=[]
    monkeypatch.setattr('sandbox_harness.controlled_media_runner.pwd.getpwnam',
        lambda name:type('Account',(),{'pw_uid':10001,'pw_gid':10001})())
    monkeypatch.setattr('sandbox_harness.controlled_media_runner.os.chown',lambda *args:None)
    monkeypatch.setattr('sandbox_harness.controlled_media_runner._gpu_isolation_args',
        lambda work,runtime,index:['bwrap',index])
    monkeypatch.setattr('sandbox_harness.controlled_media_runner._run',
        lambda args,work,timeout:observed.append((args,work,timeout)))
    assert probe_controlled_gpu('3',runtime,tmp_path/'probes')
    assert observed[0][0][:2]==['bwrap','3']
    assert str(runtime/'envs/sam3/bin/python') in observed[0][0]
    assert observed[0][2]==90
