import io
from types import SimpleNamespace
import zipfile

import httpx
from PIL import Image
import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.image_decompose_adapter import ImageDecomposeAdapter
from sandbox_harness.job_adapters import JobExecutionContext
from sandbox_harness.job_contracts import (JobArtifactInput,JobExpectedOutput,JobGroupCreate,JobKind,
    JobRequest,JobState,OutputDisposition)
from sandbox_harness.objects import ObjectStore
from sandbox_harness.store import Store


def png():
    stream=io.BytesIO();Image.new('RGB',(32,32),'blue').save(stream,format='PNG');return stream.getvalue()


def ora():
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w') as archive:
        archive.writestr('mimetype','image/openraster',compress_type=zipfile.ZIP_STORED)
        archive.writestr('stack.xml','<image w="32" h="32"><stack/></image>')
    return stream.getvalue()


async def setup(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'decompose.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title='decompose'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='decompose',
        idempotency_key='decompose-turn'),profile,'owner')
    objects=ObjectStore(tmp_path/'objects');source=png();reference,digest=objects.put(source)
    artifact=await store.add_artifact(turn.id,path='source.png',sha256=digest,size=len(source),
        mime='image/png',object_ref=reference)
    request=JobRequest(local_job_id='decompose',kind=JobKind.OPERATION,adapter='image.decompose',
        descriptor_id='image.decompose',descriptor_version='1',descriptor_digest=
            'd830b2eaf9fd55ad2a7d0445a04c15689103eaa1ea755c02c35a5918acb51640',
        idempotency_key='decompose-request',parameters={'layer_count':3,'resolution':640,
            'quality':'balanced','reconstruction_guard':True},
        input_artifacts=(JobArtifactInput(artifact_id=artifact.id,artifact_role='source'),),
        expected_outputs=(JobExpectedOutput(artifact_role='layer_package',
            output_disposition=OutputDisposition.DELIVERY,min_items=2,max_items=30),),
        retry_policy={'max_pre_accept_attempts':1},cancellation_policy={'supported':True},
        reconciliation_policy={'supported':True})
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='decompose-group',jobs=(request,)))
    attempt=await store.claim_job(jobs[0].id,'worker','node',60);job=await store.get_job(jobs[0].id)
    settings=SimpleNamespace(data_root=tmp_path/'data',image_decompose_base_url='http://127.0.0.1:8912',
        max_artifact_bytes=128*1024*1024)
    return store,objects,settings,job,attempt,request


async def test_decompose_submit_poll_collect_and_recover(tmp_path):
    layer=png();package=ora()
    def handler(request:httpx.Request):
        if request.method=='POST' and request.url.path=='/api/jobs':
            assert b'source_image' in request.content and b'layer_count' in request.content
            return httpx.Response(202,json={'id':'remote-1'},headers={'set-cookie':'session=one'})
        if request.method=='GET' and request.url.path=='/api/jobs/remote-1':
            return httpx.Response(200,json={'status':'succeeded','artifacts':[
                {'id':'layer','name':'layer.png','media_type':'image/png'},
                {'id':'package','name':'layers.ora','media_type':'application/octet-stream'}]})
        if request.url.path.endswith('/artifacts/layer'):
            return httpx.Response(200,content=layer,headers={'content-type':'image/png'})
        if request.url.path.endswith('/artifacts/package'):
            return httpx.Response(200,content=package,
                headers={'content-type':'application/octet-stream'})
        raise AssertionError(str(request.url))
    store,objects,settings,job,attempt,request=await setup(tmp_path)
    adapter=ImageDecomposeAdapter(settings,store,objects,transport=httpx.MockTransport(handler))
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        submission=await adapter.submit(context)
        assert submission.external_execution_id=='remote-1'
        recovery=await adapter.recover_submission(context)
        assert recovery.submission.external_execution_id=='remote-1'
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_external_execution_id=submission.external_execution_id,
            previous_acceptance_receipt=submission.acceptance_receipt)
        status=await adapter.status(context,submission.external_execution_id)
        assert status.terminal and status.state==JobState.SUCCEEDED
        result=await adapter.collect(context,submission.external_execution_id,status)
        assert {row['filename'] for row in result['artifacts']}=={'layer.png','layers.ora'}
        assert result['collections'][0]['state']=='published'
    finally:
        await adapter.client.aclose();await store.close()


def test_decompose_rejects_non_loopback_service(tmp_path):
    settings=SimpleNamespace(data_root=tmp_path,image_decompose_base_url='http://10.0.0.8:8912',
        max_artifact_bytes=1024)
    with pytest.raises(ValueError,match='loopback'):
        ImageDecomposeAdapter(settings,None,None)
