from types import SimpleNamespace

import httpx
import pytest

from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.harness_job import OpenSandboxChildBackend
from sandbox_harness.sandbox import Sandbox


def failure(status):
    request=httpx.Request('GET','http://runtime/v1/executions/job:1')
    response=httpx.Response(status,request=request)
    return httpx.HTTPStatusError('runtime unavailable',request=request,response=response)


async def test_child_recovery_retries_transient_runtime_proxy_status(monkeypatch,tmp_path):
    digest='a'*64;job_id='b'*32
    sandbox=Sandbox(id='sandbox',harness=Harness.CLAUDE,
        image='runtime@sha256:'+digest,runtime_url='http://runtime',metadata={},health_state='ready')

    class Inventory:
        async def find_by_turn(self,identity):return [sandbox]

    class Pool:
        backend=Inventory()
        async def adopt(self,value,**kwargs):return value
        async def release(self,*args,**kwargs):raise AssertionError('accepted execution was released')

    class Runtime:
        calls=0
        async def execution_status(self,*args):
            self.calls+=1
            if self.calls<3:raise failure(503)
            return {'result':None}

    async def no_sleep(_):return None
    monkeypatch.setattr('sandbox_harness.harness_job.asyncio.sleep',no_sleep)
    runtime=Runtime();backend=OpenSandboxChildBackend(pool=Pool(),runtime=runtime,
        store=SimpleNamespace(),objects=SimpleNamespace(),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=lambda *args:None)
    backend._ensure_collector=lambda *args:None
    context=SimpleNamespace(job=SimpleNamespace(id=job_id))
    reservation=SimpleNamespace(id='reservation',job_generation=1,harness='claude',
        image_digest=digest,node_id='node',node_generation=1,runtime_profile_id='profile',gpu_ids=())

    recovered=await backend.recover_submission(context,reservation)

    assert runtime.calls==3
    assert recovered.submission.external_execution_id==job_id+':1'


async def test_child_status_does_not_retry_business_http_error(tmp_path):
    backend=OpenSandboxChildBackend(pool=SimpleNamespace(),runtime=SimpleNamespace(),
        store=SimpleNamespace(),objects=SimpleNamespace(),
        settings=Settings(backend='fake',data_root=tmp_path),token_factory=lambda *args:None)

    class Runtime:
        calls=0
        async def execution_status(self,*args):self.calls+=1;raise failure(400)

    backend.runtime=Runtime()
    with pytest.raises(httpx.HTTPStatusError):
        await backend._execution_status(SimpleNamespace(),'execution',retry_seconds=1)
    assert backend.runtime.calls==1
