import hashlib
import io
import json
from types import SimpleNamespace

from PIL import Image

from sandbox_harness.controlled_media_adapter import ControlledMediaOperationAdapter
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.job_adapters import AdapterStatus,JobExecutionContext
from sandbox_harness.job_contracts import (JobArtifactInput,JobExpectedOutput,JobGroupCreate,JobKind,
    JobRequest,JobState,OutputDisposition)
from sandbox_harness.objects import ObjectStore
from sandbox_harness.scheduler import GPUInventory,NodeInventory,NodeProfile,Scheduler
from sandbox_harness.store import Store


def png(mode='RGB',color='blue'):
    stream=io.BytesIO();Image.new(mode,(64,64),color).save(stream,format='PNG');return stream.getvalue()


async def setup(tmp_path,identity,parameters,outputs,resources):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/(identity.replace('.','-')+'.db')))
    await store.initialize();profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title=identity),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message=identity,
        idempotency_key=identity+'-turn'),profile,'owner')
    objects=ObjectStore(tmp_path/'objects');source=png();object_ref,digest=objects.put(source)
    artifact=await store.add_artifact(turn.id,path='source.png',sha256=digest,size=len(source),
        mime='image/png',object_ref=object_ref)
    request=JobRequest(local_job_id=identity.replace('.','-'),kind=JobKind.OPERATION,adapter=identity,
        descriptor_id=identity,descriptor_version='1',descriptor_digest=('1' if identity=='image.segment' else '2')*64,
        idempotency_key=identity+'-request',parameters=parameters,
        input_artifacts=(JobArtifactInput(artifact_id=artifact.id,artifact_role='source'),),
        expected_outputs=outputs,resource_requirements=resources)
    _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key=identity+'-group',jobs=[request]))
    attempt=await store.claim_job(jobs[0].id,'worker','controlled-node',60)
    job=await store.get_job(jobs[0].id);scheduler=Scheduler(store)
    runtime_digest='a'*64
    await scheduler.register_node(NodeInventory(node_id='controlled-node',generation=1,cpu_total=16,
        memory_mb_total=65536,disk_mb_total=100000,profiles=(NodeProfile(
            runtime_profile_id='host-controlled',harness='codex',image_digest=runtime_digest),),
        gpus=(GPUInventory(id='gpu-0',type='A100',memory_mb=40000),)))
    settings=SimpleNamespace(data_root=tmp_path/'data',max_artifact_bytes=1024*1024,
        controlled_runtime_root=tmp_path/'runtime',controlled_runtime_digest=runtime_digest)
    settings.controlled_runtime_root.mkdir();return store,objects,scheduler,settings,job,attempt,request


async def test_segment_reserves_gpu_and_publishes_scoped_outputs(tmp_path):
    outputs=(
        JobExpectedOutput(artifact_role='foreground',output_disposition=OutputDisposition.DELIVERY,
            min_items=1,validators=('image.alpha','image.dimensions_exact')),
        JobExpectedOutput(artifact_role='mask',output_disposition=OutputDisposition.WORKSPACE,
            min_items=1,validators=('mask.nontrivial','image.dimensions_exact')),
        JobExpectedOutput(artifact_role='segmentation_report',output_disposition=OutputDisposition.WORKSPACE,
            min_items=1,mime_patterns=('application/json',)))
    store,objects,scheduler,settings,job,attempt,request=await setup(tmp_path,'image.segment',
        {'prompt':'astronaut','confidence':.35,'refine':True},outputs,
        {'cpu':2,'memory_mb':32768,'disk_mb':4096,'gpu':1})
    observed={}
    async def runner(operation,work,source,parameters,gpu_ids,runtime_root):
        observed.update(operation=operation,source=source,parameters=parameters,gpu_ids=gpu_ids,
            runtime_root=runtime_root)
        foreground=png('RGBA',(0,0,255,128));mask=png('L',128)
        (work/'foreground.png').write_bytes(foreground);(work/'mask.png').write_bytes(mask)
        (work/'segmentation.json').write_text(json.dumps({'segmenter':'sam3','matting':'zim'}))
        return {'outputs':[
            {'role':'foreground','path':'foreground.png','mime':'image/png',
                'validation_receipts':[{'validator':'image.alpha','status':'pass'},
                    {'validator':'image.dimensions_exact','status':'pass'}]},
            {'role':'mask','path':'mask.png','mime':'image/png',
                'validation_receipts':[{'validator':'mask.nontrivial','status':'pass'},
                    {'validator':'image.dimensions_exact','status':'pass'}]},
            {'role':'segmentation_report','path':'segmentation.json','mime':'application/json'}]}
    adapter=ControlledMediaOperationAdapter(scheduler=scheduler,store=store,objects=objects,
        settings=settings,runner=runner)
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        submission=await adapter.submit(context)
        assert submission.completed and submission.acceptance_receipt['gpu_ids']==['gpu-0']
        assert observed['operation']=='image.segment' and observed['parameters']['prompt']=='astronaut'
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_external_execution_id=submission.external_execution_id,
            previous_acceptance_receipt=submission.acceptance_receipt)
        result=await adapter.collect(context,submission.external_execution_id,
            AdapterStatus(True,JobState.SUCCEEDED,submission.result))
        assert {row['artifact_role'] for row in result['artifacts']}=={
            'foreground','mask','segmentation_report'}
        assert {row['state'] for row in result['collections']}=={'published'}
    finally:await store.close()


async def test_transcribe_reserves_cpu_without_gpu_and_publishes_timeline(tmp_path):
    outputs=tuple(JobExpectedOutput(artifact_role=role,output_disposition=OutputDisposition.DELIVERY,
        min_items=1) for role in ('transcript_json','transcript_text','transcript_srt'))
    store,objects,scheduler,settings,job,attempt,request=await setup(tmp_path,'audio.transcribe',
        {'language':'zh','translate':False,'beam_size':5},outputs,
        {'cpu':4,'memory_mb':4096,'disk_mb':2048,'gpu':0})
    async def runner(operation,work,source,parameters,gpu_ids,runtime_root):
        assert operation=='audio.transcribe' and not gpu_ids
        files={'transcript.json':b'{"segments":[]}',
            'transcript.txt':'测试\n'.encode(),'transcript.srt':b''}
        for name,data in files.items():(work/name).write_bytes(data)
        return {'outputs':[{'role':role,'path':name,'mime':mime} for role,name,mime in (
            ('transcript_json','transcript.json','application/json'),
            ('transcript_text','transcript.txt','text/plain'),
            ('transcript_srt','transcript.srt','text/plain'))]}
    adapter=ControlledMediaOperationAdapter(scheduler=scheduler,store=store,objects=objects,
        settings=settings,runner=runner)
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        submission=await adapter.submit(context)
        assert submission.acceptance_receipt['gpu_ids']==[]
        context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
            previous_external_execution_id=submission.external_execution_id,
            previous_acceptance_receipt=submission.acceptance_receipt)
        result=await adapter.collect(context,submission.external_execution_id,
            AdapterStatus(True,JobState.SUCCEEDED,submission.result))
        assert len(result['artifacts'])==3
    finally:await store.close()
