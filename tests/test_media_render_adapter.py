from types import SimpleNamespace

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.job_adapters import AdapterStatus,JobExecutionContext
from sandbox_harness.job_contracts import (JobArtifactInput,JobExpectedOutput,JobGroupCreate,JobKind,
    JobRequest,JobState,OutputDisposition)
from sandbox_harness.media_render_adapter import MediaRenderOperationAdapter
from sandbox_harness.objects import ObjectStore
from sandbox_harness.sandbox import Sandbox
from sandbox_harness.sandbox_contracts import SandboxRuntimeRoute
from sandbox_harness.scheduler import NodeInventory,Scheduler
from sandbox_harness.runtime import StagedRuntimeObject
from sandbox_harness.store import Store


async def test_media_render_adapter_reserves_profile_injects_frozen_input_and_binds_output(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'render-adapter.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Render Operation'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='resize',
        idempotency_key='render-operation-turn'),profile,'owner')
    objects=ObjectStore(tmp_path/'objects');source=b'\x89PNG\r\n\x1a\nsource';object_ref,digest=objects.put(source)
    artifact=await store.add_artifact(turn.id,path='source.png',sha256=digest,size=len(source),
        mime='image/png',object_ref=object_ref)
    output=JobExpectedOutput(artifact_role='rendered_media',output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=('image/png',),
        validators=('image.dimensions_exact','image.png'))
    request=JobRequest(local_job_id='resize',kind=JobKind.OPERATION,adapter='media.render',
        descriptor_id='media.render',descriptor_version='1',descriptor_digest='b'*64,
        idempotency_key='render-operation-job',parameters={'recipe':{'operation':'image.resize',
            'input_role':'source','width':32,'height':24,'output_filename':'resized.png'}},
        input_artifacts=(JobArtifactInput(artifact_id=artifact.id,artifact_role='source'),),
        expected_outputs=(output,),child_profile_id='operation-controlled',runtime_profile_id='media-cpu',
        resource_requirements={'harness':'codex','image_digest':'d'*64,'delegation_depth':1,
            'cpu':1,'memory_mb':512,'disk_mb':1024})
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='render-operation-group',jobs=[request]))
    attempt=(await store.claim_available_jobs('worker','node',60,limit=1,adapters={'media.render'}))[0]
    job=await store.get_job(jobs[0].id);scheduler=Scheduler(store)
    await scheduler.register_node(NodeInventory(node_id='node',generation=1,cpu_total=4,
        memory_mb_total=8192,disk_mb_total=10000,profiles=[{'runtime_profile_id':'media-cpu',
            'harness':'codex','image_digest':'d'*64,
            'image_ref':'registry/media-cpu-codex@sha256:'+'d'*64}]))
    sandbox=Sandbox(id='sandbox-operation',harness=Harness.CODEX,
        image='registry/media-cpu-codex@sha256:'+'d'*64,runtime_url='http://runtime',metadata={})
    class Pool:
        backend=SimpleNamespace(list_namespace=lambda:None,
            runtime_route=lambda sandbox:SandboxRuntimeRoute(
                "http://sandbox-gateway:29880","http://sandbox-proxy:29881"))
        async def acquire(self,**kwargs):return sandbox
        async def get_attached(self,*args):return sandbox
        async def release(self,*args,**kwargs):return None
        async def adopt(self,value,**kwargs):return value
    result_data=b'\x89PNG\r\n\x1a\nresized'
    class Runtime:
        def __init__(self):self.files=[];self.rendered=None
        async def inject(self,*args,**kwargs):self.injected=kwargs
        async def put_project_file(self,sandbox,path,data,mime):self.files.append((path,data,mime))
        async def media_render(self,sandbox,value):self.rendered=value;return {
            'path':'resized.png','size':len(result_data),'mime':'image/png',
            'sha256':__import__('hashlib').sha256(result_data).hexdigest()}
        async def artifact(self,sandbox,path,*,maximum):
            assert path=='resized.png';target=tmp_path/'staged-resized.png';target.write_bytes(result_data)
            return StagedRuntimeObject(target,len(result_data),__import__('hashlib').sha256(result_data).hexdigest())
    runtime=Runtime()
    async def token_factory(*args):return 'token',SimpleNamespace(nonce='nonce')
    adapter=MediaRenderOperationAdapter(scheduler=scheduler,pool=Pool(),runtime=runtime,store=store,
        objects=objects,settings=Settings(backend='fake',data_root=tmp_path),token_factory=token_factory)
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        await store.mark_job_submission_started(job.id,"worker",attempt.generation)
        submission=await adapter.submit(context)
        assert submission.completed and runtime.rendered['input_path'].startswith('assets/inputs/source-')
        assert runtime.files[0][1]==source and runtime.injected['runtime_profile_id']=='media-cpu'
        assert runtime.injected['gateway_url']=='http://sandbox-gateway:29880'
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_external_execution_id=submission.external_execution_id,
            previous_acceptance_receipt=submission.acceptance_receipt)
        result=await adapter.collect(context,submission.external_execution_id,
            AdapterStatus(True,JobState.SUCCEEDED,submission.result))
        assert result['collections'][0]['state']=='published'
        assert (await store.list_job_artifacts(job.id))[0].output_disposition==OutputDisposition.DELIVERY
    finally:await store.close()


async def test_media_render_adapter_collects_role_addressed_operation_outputs(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'multi-output.db'));await store.initialize()
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
            reasoning_level='standard',title='Audio Operation'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='audio',
            idempotency_key='audio-operation-turn'),profile,'owner')
        request=JobRequest(local_job_id='audio',kind=JobKind.OPERATION,adapter='audio.process',
            descriptor_id='audio.process',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='audio-operation-job',parameters={'recipe':{'operation':'audio.process'}},
            expected_outputs=(
                JobExpectedOutput(artifact_role='processed_audio',output_disposition='delivery',min_items=1,
                    validators=('audio',)),
                JobExpectedOutput(artifact_role='measurement_report',output_disposition='workspace',min_items=1)))
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='audio-operation-group',jobs=[request]))
        attempt=(await store.claim_available_jobs('worker','node',60,limit=1,
            adapters={'audio.process'}))[0]
        job=await store.get_job(jobs[0].id);objects=ObjectStore(tmp_path/'objects')
        outputs={'processed.wav':b'RIFF\x04\x00\x00\x00WAVE','audio-process-report.json':b'{"status":"succeeded"}'}
        class Runtime:
            async def artifact(self,sandbox,path,*,maximum):
                data=outputs[path];target=tmp_path/('staged-'+path);target.write_bytes(data)
                return StagedRuntimeObject(target,len(data),__import__('hashlib').sha256(data).hexdigest())
        sandbox=Sandbox(id='sandbox',harness=Harness.CODEX,image='runtime@sha256:'+'d'*64,
            runtime_url='http://runtime',metadata={})
        class Pool:
            async def get_attached(self,*args):return sandbox
        class HoldScheduler:
            @__import__('contextlib').asynccontextmanager
            async def hold(self,*args):yield
        adapter=MediaRenderOperationAdapter(scheduler=HoldScheduler(),pool=Pool(),runtime=Runtime(),store=store,
            objects=objects,settings=Settings(backend='fake',data_root=tmp_path),token_factory=None)
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_acceptance_receipt={'sandbox_id':'sandbox',
                'sandbox_handle':sandbox.handle.to_mapping(),'reservation_id':'reservation',
                'node_id':'node','node_generation':1})
        result=await adapter.collect(context,'operation:'+job.id,AdapterStatus(True,JobState.SUCCEEDED,{
            'outputs':[{'role':'processed_audio','path':'processed.wav','mime':'audio/wav',
                'size':len(outputs['processed.wav']),
                'sha256':__import__('hashlib').sha256(outputs['processed.wav']).hexdigest(),
                'validation_receipts':[{'validator':'audio','status':'pass'}]},
                {'role':'measurement_report','path':'audio-process-report.json','mime':'application/json',
                'size':len(outputs['audio-process-report.json']),
                'sha256':__import__('hashlib').sha256(outputs['audio-process-report.json']).hexdigest()}]}))
        assert {row['artifact_role'] for row in result['artifacts']}=={
            'processed_audio','measurement_report'}
        assert {row['state'] for row in result['collections']}=={'published'}
    finally:await store.close()
