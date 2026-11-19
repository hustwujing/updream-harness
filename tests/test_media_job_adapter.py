import io
from types import SimpleNamespace

from PIL import Image

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.job_adapters import JobExecutionContext,PreAcceptFailure
from sandbox_harness.job_contracts import (JobArtifactInput,JobExpectedOutput,JobGroupCreate,JobKind,JobRequest,
    JobState,OutputDisposition)
from sandbox_harness.media_job_adapter import MediaProviderJobAdapter
from sandbox_harness.objects import ObjectStore
from sandbox_harness.store import Store


class Catalog:
    spec=SimpleNamespace(provider='test-provider',adapter='test-adapter',execution='async',
        cancellation='supported')
    def resolve(self,kind,model):assert (kind,model)==('image','test-image');return self.spec
    def validate_parameters(self,*args,**kwargs):return None


async def setup(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'media-adapter.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Provider Job'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='generate',
        idempotency_key='provider-job-turn'),profile,'owner')
    output=JobExpectedOutput(artifact_role='generated_images',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=('image/png',))
    request=JobRequest(local_job_id='generate',kind=JobKind.PROVIDER,adapter='image.generate',
        descriptor_id='image.generate',descriptor_version='1',descriptor_digest='c'*64,
        idempotency_key='generic-provider-job',parameters={'kind':'image','model':'test-image',
            'generation_mode':'text_to_image','parameters':{'prompt':'a blue cup'}},
        expected_outputs=(output,))
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='generic-provider-group',jobs=[request]))
    attempt=(await store.claim_available_jobs('generic-worker','node',60,limit=1,
        adapters={'image.generate'}))[0];job=await store.get_job(jobs[0].id)
    return store,thread,job,attempt,request


async def test_media_provider_adapter_reuses_media_job_handle_and_binds_generic_artifact(tmp_path):
    store,thread,job,attempt,request=await setup(tmp_path);objects=ObjectStore(tmp_path/'objects')
    adapter=MediaProviderJobAdapter(Settings(backend='fake',data_root=tmp_path),store,objects,Catalog())
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        accepted=await adapter.submit(context)
        assert accepted.external_execution_id and accepted.acceptance_receipt['media_job_id']==accepted.external_execution_id
        claimed=(await store.claim_media_jobs('media-worker'))[0]
        assert claimed['id']==accepted.external_execution_id
        data=b'\x89PNG\r\n\x1a\nprovider-output';object_ref,_=objects.put(data)
        await store.update_media_job(claimed['id'],'media-worker',claimed['generation'],
            status='succeeded',phase='committed',
            output_object_ref=object_ref,output_mime='image/png',output_size=len(data),progress=100)
        context=context.__class__(job=job,request=request,generation=attempt.generation,
            previous_external_execution_id=accepted.external_execution_id,
            previous_acceptance_receipt=accepted.acceptance_receipt)
        status=await adapter.status(context,accepted.external_execution_id)
        assert status.terminal and status.state==JobState.SUCCEEDED
        result=await adapter.collect(context,accepted.external_execution_id,status)
        assert result['collections'][0]['state']=='published'
        artifacts=await store.list_job_artifacts(job.id)
        assert len(artifacts)==1 and artifacts[0].artifact_role=='generated_images'
        assert artifacts[0].output_disposition==OutputDisposition.DELIVERY
        assert [value.path for value in await store.current_project_files(thread.id,'owner')]==[
            'artifacts/jobs/'+job.id+'/'+artifacts[0].filename]
    finally:await store.close()


async def test_media_provider_adapter_maps_unknown_and_cancellation_without_resubmit(tmp_path):
    store,thread,job,attempt,request=await setup(tmp_path);objects=ObjectStore(tmp_path/'objects')
    adapter=MediaProviderJobAdapter(Settings(backend='fake',data_root=tmp_path),store,objects,Catalog())
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        accepted=await adapter.submit(context);claimed=(await store.claim_media_jobs('media-worker'))[0]
        await store.update_media_job(claimed['id'],'media-worker',claimed['generation'],
            status='unknown',phase='recovery_required',
            error_code='provider_submission_outcome_unknown',error_detail='response lost')
        status=await adapter.reconcile(context,accepted.external_execution_id)
        assert status.terminal and status.state==JobState.UNKNOWN
        cancelled=await adapter.cancel(context,accepted.external_execution_id)
        assert cancelled.state==JobState.UNKNOWN
    finally:await store.close()


async def test_media_provider_adapter_preserves_artifact_reference_lineage(tmp_path):
    store,thread,job,attempt,request=await setup(tmp_path);objects=ObjectStore(tmp_path/'objects')
    adapter=MediaProviderJobAdapter(Settings(backend='fake',data_root=tmp_path),store,objects,Catalog())
    image=io.BytesIO();Image.new('RGB',(64,64),(40,90,180)).save(image,format='PNG')
    data=image.getvalue();object_ref,digest=objects.put(data)
    artifact=await store.add_artifact(job.origin_turn_id,path='input/reference.png',sha256=digest,
        size=len(data),mime='image/png',object_ref=object_ref)
    referenced=request.model_copy(update={
        'parameters':{'kind':'image','model':'test-image','generation_mode':'reference_image',
            'parameters':{'prompt':'preserve the subject'},
            'reference_bindings':{artifact.id:{'role':'reference_image','usage':'subject'}}},
        'input_artifacts':(JobArtifactInput(artifact_id=artifact.id,artifact_role='reference'),),
    })
    context=JobExecutionContext(job=job,request=referenced,generation=attempt.generation)
    try:
        accepted=await adapter.submit(context)
        assert accepted.external_execution_id
        drafts=await store.list_media_drafts(thread.id,'owner')
        assert len(drafts)==1
        assert drafts[0].references[0].artifact_id==artifact.id
        assert drafts[0].references[0].project_file_id==''
        assert drafts[0].references[0].path=='input/reference.png'
    finally:await store.close()


async def test_media_provider_adapter_rejects_invalid_input_before_external_acceptance(tmp_path):
    store,thread,job,attempt,request=await setup(tmp_path);objects=ObjectStore(tmp_path/'objects')
    adapter=MediaProviderJobAdapter(Settings(backend='fake',data_root=tmp_path),store,objects,Catalog())
    invalid=request.model_copy(update={'parameters':{'kind':'','model':'','parameters':[]}})
    context=JobExecutionContext(job=job,request=invalid,generation=attempt.generation)
    try:
        with __import__('pytest').raises(PreAcceptFailure) as raised:await adapter.submit(context)
        assert raised.value.error_code=='provider_input_invalid'
        assert raised.value.failed_phase=='input_validation'
        assert raised.value.retryable is False
    finally:await store.close()


async def test_media_provider_adapter_rejects_kind_or_mode_outside_frozen_capability(tmp_path):
    store,thread,job,attempt,request=await setup(tmp_path);objects=ObjectStore(tmp_path/'objects')
    adapter=MediaProviderJobAdapter(Settings(backend='fake',data_root=tmp_path),store,objects,Catalog())
    cases=(
        request.model_copy(update={'parameters':{**request.parameters,'kind':'video',
            'generation_mode':'text_to_video'}}),
        request.model_copy(update={'adapter':'video.generate','descriptor_id':'video.generate',
            'descriptor_digest':'3'*64,'parameters':{**request.parameters,'kind':'video',
                'generation_mode':'edit_video'}}),
    )
    try:
        for invalid in cases:
            context=JobExecutionContext(job=job,request=invalid,generation=attempt.generation)
            with __import__('pytest').raises(PreAcceptFailure) as raised:await adapter.submit(context)
            assert raised.value.error_code=='provider_input_invalid'
            assert raised.value.failed_phase=='authorization' and raised.value.retryable is False
        assert await store.list_media_drafts(thread.id,'owner')==[]
    finally:await store.close()
