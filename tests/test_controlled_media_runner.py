from pathlib import Path

import pytest

from sandbox_harness.controlled_media_runner import (
    THREAD_ENVIRONMENT,_base_args,_gpu_uuid,_thread_environment_args,
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
