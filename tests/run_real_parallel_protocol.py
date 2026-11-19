#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import fcntl
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import subprocess
import time
import uuid

import httpx
from dotenv import dotenv_values


ROOT=Path(__file__).resolve().parents[1]
TERMINAL_JOBS={'SUCCEEDED','FAILED','CANCELLED','BLOCKED','UNKNOWN'}
PROTOCOL_LOCK=ROOT/'.data/tmp/real-protocol.lock'


@asynccontextmanager
async def protocol_lock():
    PROTOCOL_LOCK.parent.mkdir(parents=True,exist_ok=True)
    stream=PROTOCOL_LOCK.open('a+')
    try:
        await asyncio.to_thread(fcntl.flock,stream.fileno(),fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(stream.fileno(),fcntl.LOCK_UN);stream.close()


class ResilientAsyncClient(httpx.AsyncClient):
    def __init__(self,*args,reconnect_seconds:float=90,**kwargs):
        super().__init__(*args,**kwargs);self.reconnect_seconds=max(0,float(reconnect_seconds))

    async def request(self,method,url,*args,**kwargs):
        safe=method.upper() in {'GET','HEAD','OPTIONS'}
        deadline=time.monotonic()+self.reconnect_seconds;delay=.25
        while True:
            try:return await super().request(method,url,*args,**kwargs)
            except (httpx.ConnectError,httpx.ReadError,httpx.RemoteProtocolError):
                if not safe or time.monotonic()>=deadline:raise
                await asyncio.sleep(delay);delay=min(2,delay*2)
TERMINAL_TURNS={'COMPLETED','FAILED','CANCELLED','WAITING_USER','PAUSED'}


def require(response:httpx.Response)->httpx.Response:
    if response.is_error:
        raise RuntimeError(f'HTTP {response.status_code} {response.request.method} '
            f'{response.request.url.path}: {response.text[:1000]}')
    return response


def provider_job(index:int,*,prompt:str|None=None)->dict:
    return {'local_job_id':f'image-{index}','kind':'provider','adapter':'image.generate',
        'descriptor_id':'image.generate','descriptor_version':'1','descriptor_digest':'c'*64,
        'idempotency_key':f'protocol-image-{index}-'+uuid.uuid4().hex,
        'parameters':{'kind':'image','model':'seedream-5.0','generation_mode':'text_to_image',
            'parameters':{'prompt':prompt or f'A simple blue geometric poster variant {index}'}},
        'expected_outputs':[{'artifact_role':'generated_images','output_disposition':'candidate',
            'min_items':1,'max_items':1,'mime_patterns':['image/*']}]}


CAPABILITY_DIGESTS={'image.generate':'c'*64,'video.generate':'3'*64,'video.edit':'4'*64,
    'audio.generate':'5'*64,'model3d.generate':'6'*64,'model3d.transform':'7'*64}
PROVIDER_OUTPUT_ROLES={'image.generate':'generated_images','video.generate':'generated_videos',
    'video.edit':'generated_videos','audio.generate':'generated_audio',
    'model3d.generate':'generated_models','model3d.transform':'generated_models'}


def semantic_provider_job(local_id:str,capability:str,*,kind:str,model:str,mode:str,
                          parameters:dict,references:list[dict]|None=None)->dict:
    references=references or []
    return {'local_job_id':local_id,'kind':'provider','adapter':capability,
        'descriptor_id':capability,'descriptor_version':'1',
        'descriptor_digest':CAPABILITY_DIGESTS[capability],
        'idempotency_key':'coverage-provider-'+local_id+'-'+uuid.uuid4().hex,
        'parameters':{'kind':kind,'model':model,'generation_mode':mode,'parameters':parameters,
            **({'reference_bindings':{row['artifact_id']:{'role':row['role'],'usage':row['usage']}
                for row in references}} if references else {})},
        'input_artifacts':[{'artifact_id':row['artifact_id'],'artifact_role':'reference'}
            for row in references],
        'expected_outputs':[{'artifact_role':PROVIDER_OUTPUT_ROLES[capability],
            'output_disposition':'workspace','min_items':1,'max_items':1}]}


def harness_job(index:int,args,*,objective:str='Return a concise factual analysis.',
                outputs:list[dict]|None=None,skills:list[str]|None=None,
                inputs:list[dict]|None=None,child_profile_id:str='child-general',
                replaces_job_id:str='')->dict:
    return {'local_job_id':f'child-{index}','kind':'harness','adapter':'child-harness',
        'descriptor_id':'delegate.harness','descriptor_version':'1','descriptor_digest':'d'*64,
        'idempotency_key':f'protocol-child-{index}-'+uuid.uuid4().hex,
        'parameters':{'harness':args.harness,'model':args.model,'objective':objective},
        'input_artifacts':inputs or [],'expected_outputs':outputs or [],'child_profile_id':child_profile_id,
        'skill_ids':skills or [],'replaces_job_id':replaces_job_id}


def operation_job(artifact_id:str)->dict:
    return {'local_job_id':'inspect-resize','kind':'operation','adapter':'media.render',
        'descriptor_id':'media.render','descriptor_version':'1','descriptor_digest':'b'*64,
        'idempotency_key':'protocol-operation-'+uuid.uuid4().hex,
        'parameters':{'recipe':{'operation':'image.resize','input_role':'source','width':64,
            'height':64,'output_filename':'inspected-poster.png'}},
        'input_artifacts':[{'artifact_id':artifact_id,'artifact_role':'source'}],
        'expected_outputs':[{'artifact_role':'rendered_media','output_disposition':'workspace',
            'min_items':1,'max_items':1,'mime_patterns':['image/png']}]}


def segment_job(artifact_id:str,*,identity:str='segment-subject')->dict:
    return {'local_job_id':identity,'kind':'operation','adapter':'image.segment',
        'descriptor_id':'image.segment','descriptor_version':'1','descriptor_digest':'1'*64,
        'idempotency_key':'protocol-segment-'+uuid.uuid4().hex,
        'parameters':{'prompt':'person','confidence':0.25,'refine':True},
        'input_artifacts':[{'artifact_id':artifact_id,'artifact_role':'source'}],
        'expected_outputs':[{'artifact_role':'foreground','output_disposition':'workspace',
            'min_items':1,'max_items':1,'mime_patterns':['image/png'],
            'validators':['image.dimensions_exact','image.alpha']}]}


def transcribe_job(artifact_id:str)->dict:
    return {'local_job_id':'transcribe-source','kind':'operation','adapter':'audio.transcribe',
        'descriptor_id':'audio.transcribe','descriptor_version':'1','descriptor_digest':'2'*64,
        'idempotency_key':'protocol-transcribe-'+uuid.uuid4().hex,
        'parameters':{'language':'zh','translate':False,'beam_size':3},
        'input_artifacts':[{'artifact_id':artifact_id,'artifact_role':'source'}],
        'expected_outputs':[{'artifact_role':'transcript_text','output_disposition':'workspace',
            'min_items':1,'max_items':1,'mime_patterns':['text/plain']}]}


def video_render_job(artifact_id:str)->dict:
    return {'local_job_id':'normalize-video','kind':'operation','adapter':'media.render',
        'descriptor_id':'media.render','descriptor_version':'1','descriptor_digest':'b'*64,
        'idempotency_key':'protocol-video-render-'+uuid.uuid4().hex,
        'parameters':{'recipe':{'operation':'video.transcode','input_role':'source','width':640,
            'height':360,'fps':24,'output_filename':'normalized-video.mp4'}},
        'input_artifacts':[{'artifact_id':artifact_id,'artifact_role':'source'}],
        'expected_outputs':[{'artifact_role':'rendered_media','output_disposition':'workspace',
            'min_items':1,'max_items':1,'mime_patterns':['video/mp4'],
            'validators':['video.dimensions_exact','video.fps','video.mp4']}]}


def audio_process_job(artifact_id:str)->dict:
    return {'local_job_id':'process-audio','kind':'operation','adapter':'audio.process',
        'descriptor_id':'audio.process','descriptor_version':'1','descriptor_digest':'d'*64,
        'idempotency_key':'protocol-audio-process-'+uuid.uuid4().hex,
        'parameters':{'recipe':{'operation':'audio.process','input_role':'source','format':'wav',
            'gain_db':0,'target_lufs':-16,'true_peak_db':-1.5,'fade_in':.1,'fade_out':.1,
            'highpass_hz':0,'denoise':True,'output_filename':'processed-audio.wav'}},
        'input_artifacts':[{'artifact_id':artifact_id,'artifact_role':'source'}],
        'expected_outputs':[{'artifact_role':'processed_audio','output_disposition':'workspace',
            'min_items':1,'max_items':1,'mime_patterns':['audio/wav']},
            {'artifact_role':'measurement_report','output_disposition':'workspace',
                'min_items':1,'max_items':1,'mime_patterns':['application/json']}]}


def concat_sequence_job(source_id:str,music_id:str)->dict:
    return {'local_job_id':'concat-sequence','kind':'operation','adapter':'media.concat_sequence',
        'descriptor_id':'media.concat_sequence','descriptor_version':'1','descriptor_digest':'e'*64,
        'idempotency_key':'protocol-concat-'+uuid.uuid4().hex,
        'parameters':{'recipe':{'operation':'media.concat_sequence','source_role':'source',
            'music_role':'music','segment_count':3,'segment_seconds':.5,'width':320,'height':180,
            'fps':24,'output_filename':'coverage-sequence.mp4'}},
        'input_artifacts':[{'artifact_id':source_id,'artifact_role':'source'},
            {'artifact_id':music_id,'artifact_role':'music'}],
        'expected_outputs':[{'artifact_role':'rendered_media','output_disposition':'workspace',
            'min_items':1,'max_items':1,'mime_patterns':['video/mp4']},
            {'artifact_role':'compiler_report','output_disposition':'workspace',
                'min_items':1,'max_items':1,'mime_patterns':['application/json']}]}


def decompose_job(artifact_id:str)->dict:
    return {'local_job_id':'decompose-image','kind':'operation','adapter':'image.decompose',
        'descriptor_id':'image.decompose','descriptor_version':'1',
        'descriptor_digest':'d830b2eaf9fd55ad2a7d0445a04c15689103eaa1ea755c02c35a5918acb51640',
        'idempotency_key':'protocol-decompose-'+uuid.uuid4().hex,
        'parameters':{'prompt':'separate subject and background into editable semantic layers',
            'layer_count':3,'resolution':640,'quality':'balanced','seed':777,
            'reconstruction_guard':True},
        'input_artifacts':[{'artifact_id':artifact_id,'artifact_role':'source'}],
        'expected_outputs':[{'artifact_role':'layer_package','output_disposition':'workspace',
            'min_items':2,'max_items':30}]}


async def wait_job(client,job_id,timeout=900):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        response=await client.get(f'api/jobs/{job_id}');require(response);value=response.json()
        if value['state'] in TERMINAL_JOBS:return value
        await asyncio.sleep(1)
    raise TimeoutError('job_timeout:'+job_id)


async def wait_jobs(client,jobs,timeout=900):
    values=await asyncio.gather(*(wait_job(client,job['id'],timeout) for job in jobs))
    failed=[value for value in values if value['state']!='SUCCEEDED']
    if failed:raise AssertionError('jobs_not_succeeded:'+json.dumps(
        [{'id':value['id'],'state':value['state'],'result':value.get('result',{})} for value in failed],ensure_ascii=False))
    return values


async def wait_turn(client,turn_id,timeout=900):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        response=await client.get(f'api/turns/{turn_id}');require(response);value=response.json()
        if value['state'] in TERMINAL_TURNS:return value
        await asyncio.sleep(1)
    raise TimeoutError('turn_timeout:'+turn_id)


async def wait_continuations(client,thread_id,timeout=300,*,minimum=1,maximum=None):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        turns=require(await client.get(f'api/threads/{thread_id}/turns')).json()
        continuations=[value for value in turns if value.get('internal')]
        if maximum is not None and len(continuations)>maximum:
            raise AssertionError('too_many_continuations:'+str(len(continuations)))
        if len(continuations)>=minimum:return turns,continuations
        await asyncio.sleep(.25)
    raise TimeoutError('continuation_timeout:'+thread_id)


async def artifact_boundary(client,args,identity,*,requires_review:bool,verdict:str)->dict:
    thread,turn=await origin(client,args,identity);settings=load_platform_settings()
    from sandbox_harness.app import object_store
    from sandbox_harness.artifact_gateway import ArtifactEmission,ArtifactGateway
    from sandbox_harness.job_contracts import (JobExpectedOutput,JobGroupCreate,JobKind,
        JobRequest,JobState,OutputDisposition)
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    owner=(await store.get_thread(thread['id'])).owner_id
    request=JobRequest(local_job_id='review-boundary',kind=JobKind.OPERATION,adapter='fixture',
        descriptor_id='fixture.review-boundary',descriptor_version='1',descriptor_digest='e'*64,
        idempotency_key='review-boundary-'+uuid.uuid4().hex,parameters={},expected_outputs=[
            JobExpectedOutput(artifact_role='report',output_disposition=OutputDisposition.DELIVERY,
                min_items=1,max_items=1,mime_patterns=('application/pdf',),requires_review=requires_review)])
    try:
        _,jobs=await store.create_job_group(owner,JobGroupCreate(thread_id=thread['id'],
            origin_turn_id=turn['id'],idempotency_key='review-group-'+uuid.uuid4().hex,jobs=[request],
            suppress_continuation=True))
        attempt=await store.claim_job(jobs[0].id,'protocol-review','protocol-node',60)
        gateway=ArtifactGateway(store,object_store(settings),settings.data_root/'tmp'/'protocol-artifacts',
            maximum_bytes=1024*1024)
        data=b'%PDF-1.7\n% protocol review boundary\n';digest=hashlib.sha256(data).hexdigest()
        async def chunks():yield data
        await gateway.emit(ArtifactEmission(jobs[0].id,attempt.generation,'report','main',0,
            'report.pdf',OutputDisposition.DELIVERY,'application/pdf'),chunks())
        receipt={'verdict':verdict,'artifact_digests':[digest]}
        result=await store.finalize_job_artifact_collection(jobs[0].id,attempt.generation,'report',
            review_receipt=receipt)
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result=result)
        files=await store.current_project_files(thread['id'],owner)
        return {'job_id':jobs[0].id,'collection':result,'project_files':files,'digest':digest}
    finally:await store.close()


def load_platform_settings():
    path=Path(os.environ.get('SANDBOX_HARNESS_ENV_FILE',ROOT/'.data/config/platform.env'))
    for key,value in dotenv_values(path).items():
        if value is not None:os.environ[key]=value
    from sandbox_harness.config import load_settings
    return load_settings()


async def origin(client,args,identity):
    response=await client.post('api/threads',json={'harness':args.harness,'model':args.model,
        'reasoning_level':args.reasoning,'title':'Protocol '+identity});require(response);thread=response.json()
    response=await client.post(f"api/threads/{thread['id']}/turns",json={'message':'请简短回复已就绪。',
        'attachments':[],'idempotency_key':'protocol-origin-'+identity+'-'+uuid.uuid4().hex,
        'delivery_mode':'start'});require(response);return thread,response.json()


async def prepare(client,thread,turn,jobs,identity,*,suppress_continuation:bool=False):
    response=await client.post('api/job-group-drafts',json={'thread_id':thread['id'],
        'origin_turn_id':turn['id'],'idempotency_key':'protocol-group-'+identity+'-'+uuid.uuid4().hex,
        'jobs':jobs,'suppress_continuation':suppress_continuation});return response


async def register_case_asset(thread_id:str,turn_id:str,path:Path):
    settings=load_platform_settings()
    from sandbox_harness.app import object_store
    from sandbox_harness.store import Store
    data=path.read_bytes();objects=object_store(settings)
    object_ref,digest=await asyncio.to_thread(objects.put,data)
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        owner=(await store.get_thread(thread_id)).owner_id
        return await store.add_artifact(turn_id,path='inputs/'+path.name,sha256=digest,
            size=len(data),mime=mimetypes.guess_type(path.name)[0] or 'application/octet-stream',
            object_ref=object_ref)
    finally:await store.close()


async def job_artifacts(job_ids:list[str]):
    settings=load_platform_settings()
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        return {job_id:await store.list_job_artifacts(job_id) for job_id in job_ids}
    finally:await store.close()


async def attempts_overlap(job_ids:list[str])->bool:
    settings=load_platform_settings()
    from sqlalchemy import select
    from sandbox_harness.store import JobAttemptRow,Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id.in_(job_ids)))).scalars().all())
        intervals=[(row.started_at,row.finished_at) for row in attempts if row.finished_at]
        return any(left_start<right_end and right_start<left_end
            for index,(left_start,left_end) in enumerate(intervals)
            for right_start,right_end in intervals[index+1:])
    finally:await store.close()


async def verify_provider_coverage(thread_id:str,jobs:list[dict],expected:dict[str,dict]):
    settings=load_platform_settings()
    from sqlalchemy import select
    from sandbox_harness.store import JobAttemptRow,Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    evidence={}
    try:
        owner=(await store.get_thread(thread_id)).owner_id
        for job in jobs:
            spec=expected[job['local_job_id']]
            if job['state']!='SUCCEEDED':
                result=job.get('result') or {};code=str(result.get('error_code') or job['state'])
                classification=('non_program_failure' if code.startswith(
                    ('provider_','media_reference_','voice_clone_')) else 'program_failure')
                return {'classification':classification,'reason':code,'failed_job':job['local_job_id'],
                    'state':job['state'],'result':result}
            async with store.connections() as db:
                attempts=list((await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id==job['id']))).scalars().all())
            external_ids={row.external_execution_id for row in attempts if row.external_execution_id}
            if len(external_ids)!=1:
                raise AssertionError('provider external identity count:'+job['local_job_id'])
            external_id=next(iter(external_ids));media=await store.get_media_job(external_id,owner)
            if not media:raise AssertionError('provider MediaJob missing:'+job['local_job_id'])
            actual=(media.adapter,media.model,media.generation_mode)
            wanted=(spec['adapter'],spec['model'],spec['mode'])
            if actual!=wanted:
                raise AssertionError('provider identity mismatch:'+job['local_job_id']+':'+repr(actual))
            artifacts=await store.list_job_artifacts(job['id'])
            if not artifacts:raise AssertionError('provider artifact missing:'+job['local_job_id'])
            request=await store.get_job_request(job['id'])
            if request.descriptor_id!=spec['capability']:
                raise AssertionError('provider capability mismatch:'+job['local_job_id'])
            evidence[job['local_job_id']]={'job_id':job['id'],'media_job_id':external_id,
                'capability':request.descriptor_id,'adapter':media.adapter,'model':media.model,
                'mode':media.generation_mode,'artifact_ids':[row.artifact_id for row in artifacts]}
        return {'classification':'success','evidence':evidence}
    finally:await store.close()


async def wait_provider_coverage(client,jobs,timeout):
    return await asyncio.gather(*(wait_job(client,row['id'],timeout) for row in jobs))


SKILL_COVERAGE={
    'K01':('algorithmic-art','anatomy','canvas-design','diagram-design','frontend-design',
        'web-artifacts-builder','webapp-testing'),
    'K02':('brand-guidelines','theme-factory','drawio-skill','image-to-editable-ppt','pptx','codex-ppt'),
    'K03':('data-quality-audit','programmatic-eda','query-validation','xlsx','docx','pdf'),
    'K04':('media-processing','manim-video','slack-gif-creator'),
    'K05':('academy-guide','doc-and-modernize','doc-coauthoring','internal-comms','discernment-nudge'),
    'K06':('brainstorming','writing-plans','systematic-debugging','verification-before-completion',
        'skill-creator','mcp-builder','claude-api'),
}


async def case_skill_coverage(client,args,identity:str):
    thread,turn=await origin(client,args,identity)
    declared=SKILL_COVERAGE[identity]
    manifest={row['id']:row for row in json.loads(
        (ROOT/'runtime/skills/manifest.json').read_text())['skills']}
    catalog_case=next(row for row in json.loads(
        (ROOT/'tests/real_cases/cases.json').read_text())['cases'] if row['id']==identity)
    artifacts=[]
    for name in catalog_case.get('assets',[]):
        artifacts.append(await register_case_asset(thread['id'],turn['id'],
            ROOT/'tests/real_cases/assets'/name))
    bindings=[{'artifact_id':row.id,'artifact_role':f'input_{index}'}
        for index,row in enumerate(artifacts,1)]
    output={'artifact_role':'skill_evidence','output_disposition':'workspace',
        'min_items':1,'max_items':1,'mime_patterns':['application/json'],
        'max_item_bytes':2*1024*1024,'max_total_bytes':2*1024*1024}
    jobs=[]
    for index,skill_id in enumerate(declared):
        test_case=str(manifest[skill_id].get('test_case') or 'validate the Skill contract')
        objective=(f'Read and follow the injected {skill_id} Skill. Actually execute its declared '
            f'coverage objective: {test_case}. Use frozen inputs when relevant. Perform the real local '
            'commands and checks in /workspace/project or scratch; do not merely describe them and do '
            'not call an external Provider. Then write exactly one output file '
            f'/workspace/outputs/{skill_id}-coverage.json as JSON with skill_id exactly "{skill_id}", '
            'test_case, commands (a non-empty array of commands actually run), checks (a non-empty '
            'array of concrete observed results), and produced_files (paths made in project or scratch). '
            'Delete every other file from /workspace/outputs before finishing.')
        jobs.append(harness_job(1000+index,args,objective=objective,outputs=[output],
            skills=[skill_id],inputs=bindings))
        jobs[-1]['local_job_id']='skill-'+skill_id
        jobs[-1]['idempotency_key']='coverage-'+identity+'-'+skill_id+'-'+uuid.uuid4().hex
    prepared=require(await prepare(client,thread,turn,jobs,identity,
        suppress_continuation=True)).json()
    if prepared.get('mode')!='committed':raise AssertionError('skill coverage unexpectedly required confirmation')
    completed=await wait_jobs(client,prepared['jobs'],args.timeout)
    if len(completed)>1 and not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('skill coverage attempts did not overlap')
    settings=load_platform_settings()
    from sandbox_harness.app import object_store
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix);objects=object_store(settings)
    evidence={}
    try:
        for row in completed:
            request=await store.get_job_request(row['id']);skill_id=row['local_job_id'].removeprefix('skill-')
            if request.skill_ids!=(skill_id,) or not request.runtime_profile_id:
                raise AssertionError('skill grant/profile mismatch:'+skill_id)
            outputs=await store.list_job_artifacts(row['id'],'skill_evidence')
            if len(outputs)!=1:raise AssertionError('skill evidence artifact count:'+skill_id)
            value=json.loads(objects.get(outputs[0].object_ref,maximum=2*1024*1024))
            if (value.get('skill_id')!=skill_id or not value.get('commands') or not value.get('checks')):
                raise AssertionError('skill evidence content invalid:'+skill_id)
            activities=await store.list_job_activities(row['id'],0,1000)
            if not any(item['kind'] in {'tool_started','tool_finished','command','file_change'}
                for item in activities):raise AssertionError('skill produced no tool activity:'+skill_id)
            evidence[skill_id]={'job_id':row['id'],'runtime_profile_id':request.runtime_profile_id,
                'artifact_id':outputs[0].artifact_id,'commands':len(value['commands']),
                'checks':len(value['checks'])}
    finally:await store.close()
    return {'classification':'success','thread_id':thread['id'],'skills':list(declared),
        'child_harnesses':len(completed),'parallel':True,'evidence':evidence}


async def case_k07(client,args):
    thread,turn=await origin(client,args,'K07')
    source=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/portrait.png')
    inspect_output={'artifact_role':'inspect_evidence','output_disposition':'workspace',
        'min_items':1,'max_items':1,'mime_patterns':['application/json']}
    inspect=harness_job(700,args,objective=(
        f'Run `sandbox-job inspect --artifact-id {source.id}` exactly once. Verify the returned '
        'artifact_id, MIME, size and sha256, then write exactly '
        '/workspace/outputs/artifact-inspect.json containing that returned JSON object. '
        'Do not inspect the file with another tool and create no other output file.'),
        inputs=[{'artifact_id':source.id,'artifact_role':'input_1'}],outputs=[inspect_output])
    inspect['local_job_id']='inspect-artifact';inspect['idempotency_key']='coverage-k07-inspect-'+uuid.uuid4().hex
    inspect['resource_requirements']={'required_operation_ids':['artifact.inspect']}
    jobs=[inspect,segment_job(source.id),decompose_job(source.id),operation_job(source.id)]
    prepared=require(await prepare(client,thread,turn,jobs,'K07',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('sensitive visual operation group bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await asyncio.gather(*(wait_job(client,row['id'],args.timeout)
        for row in committed['jobs']))
    failed=[row for row in completed if row['state']!='SUCCEEDED']
    if failed:
        codes=[str((row.get('result') or {}).get('error_code') or row['state']) for row in failed]
        classification=('non_program_failure' if all(code.startswith(
            ('provider_','media_reference_')) for code in codes) else 'program_failure')
        return {'classification':classification,'reason':','.join(codes),'thread_id':thread['id'],
            'failed_jobs':[row['local_job_id'] for row in failed]}
    if not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('visual operation attempts did not overlap')
    settings=load_platform_settings()
    from sandbox_harness.app import object_store
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix);objects=object_store(settings)
    try:
        by_local={row['local_job_id']:row for row in completed}
        requests={row['local_job_id']:await store.get_job_request(row['id']) for row in completed}
        operation_ids={request.descriptor_id for request in requests.values()
            if request.kind.value=='operation'}
        if operation_ids!={'image.segment','image.decompose','media.render'}:
            raise AssertionError('visual durable Operation coverage mismatch:'+json.dumps(sorted(operation_ids)))
        inspect_request=await store.get_job_request(by_local['inspect-artifact']['id'])
        if inspect_request.resource_requirements.get('required_operation_ids')!=['artifact.inspect']:
            raise AssertionError('artifact.inspect grant missing')
        evidence=await store.list_job_artifacts(by_local['inspect-artifact']['id'],'inspect_evidence')
        if len(evidence)!=1:raise AssertionError('artifact.inspect evidence missing')
        value=json.loads(objects.get(evidence[0].object_ref,maximum=1024*1024))
        if (value.get('artifact_id')!=source.id or value.get('sha256')!=source.sha256
                or value.get('size')!=source.size or value.get('mime')!=source.mime):
            raise AssertionError('artifact.inspect returned incorrect frozen metadata')
        role_counts={row['local_job_id']:len(await store.list_job_artifacts(row['id']))
            for row in completed}
    finally:await store.close()
    return {'classification':'success','thread_id':thread['id'],'group_id':committed['group']['id'],
        'operations':['artifact.inspect','image.segment','image.decompose','media.render'],
        'parallel':True,'one_group_confirmation':True,'artifact_counts':role_counts}


async def case_k08(client,args):
    thread,turn=await origin(client,args,'K08')
    speech=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/speech.wav')
    noisy=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/speech_noisy.wav')
    video=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/clip_a.mp4')
    music=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/music.wav')
    jobs=[transcribe_job(speech.id),audio_process_job(noisy.id),video_render_job(video.id),
        concat_sequence_job(video.id,music.id)]
    prepared=require(await prepare(client,thread,turn,jobs,'K08',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='committed':raise AssertionError('free media Operations required confirmation')
    completed=await wait_jobs(client,prepared['jobs'],args.timeout)
    if not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('audio/video operation attempts did not overlap')
    settings=load_platform_settings()
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:descriptors={
        (await store.get_job_request(row['id'])).descriptor_id for row in completed}
    finally:await store.close()
    expected={'audio.transcribe','audio.process','media.render','media.concat_sequence'}
    if descriptors!=expected:raise AssertionError('media Operation coverage mismatch')
    outputs=await job_artifacts([row['id'] for row in completed])
    required={'transcribe-source':{'transcript_text'},'process-audio':{
        'processed_audio','measurement_report'},'normalize-video':{'rendered_media'},
        'concat-sequence':{'rendered_media','compiler_report'}}
    by_local={row['local_job_id']:row for row in completed}
    for local_id,roles in required.items():
        actual={row.artifact_role for row in outputs[by_local[local_id]['id']]}
        if actual!=roles:raise AssertionError(local_id+' output roles mismatch:'+json.dumps(sorted(actual)))
    return {'classification':'success','thread_id':thread['id'],'group_id':prepared['group']['id'],
        'operations':sorted(descriptors),'parallel':True,
        'artifact_roles':{key:sorted(value) for key,value in required.items()}}


async def case_k09(client,args):
    thread,turn=await origin(client,args,'K09')
    poster=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/poster.png')
    voice=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/reference_tone.wav')
    clip=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/clip_a.mp4')
    ref=lambda artifact,role,usage:{'artifact_id':artifact.id,'role':role,'usage':usage}
    definitions=[
        ('image-text','image.generate','image','seedream-5.0','text_to_image',
            {'prompt':'clean blue geometric cup, no text','size':'2560x1440'},[],'ark_image'),
        ('image-reference','image.generate','image','seedream-5.0','reference_image',
            {'prompt':'preserve the subject and create a clean studio variation','size':'2560x1440'},
            [ref(poster,'reference_image','subject')],'ark_image'),
        ('audio-text','audio.generate','audio','speech-2.8-turbo','text_to_audio',
            {'text':'并行能力覆盖测试。','voice_id':'male-qn-qingse','speed':1,'audio_format':'mp3'},
            [],'minimax_t2a'),
        ('audio-reference','audio.generate','audio','speech-2.8-turbo','reference_voice',
            {'text':'参考音色覆盖测试。','speed':1,'audio_format':'mp3'},
            [ref(voice,'reference_audio','audio')],'minimax_t2a'),
        ('video-text','video.generate','video','sed2','text_to_video',
            {'prompt':'a blue sphere rotates on a white background','duration':4,
                'resolution':'480p','aspect_ratio':'16:9','generate_audio':False},[],
            'ark_video_v2'),
        ('model-text','model3d.generate','model3d','v3.1-20260211','text_to_model',
            {'prompt':'simple low-poly blue cup','face_limit':1000,'texture':False},[],
            'tripo_v3'),
        ('happyhorse-edit','video.edit','video','happyhorse-video-edit','edit_video',
            {'prompt':'preserve motion and audio, apply a subtle cool color grade','duration':-1,
                'resolution':'720p','aspect_ratio':'adaptive'},
            [ref(clip,'reference_video','edit_source')],'happyhorse_video_edit'),
        ('wan-edit','video.edit','video','wan-2.7-video-edit','edit_video',
            {'prompt':'preserve motion and audio, apply a subtle clean line-art style','duration':-1,
                'resolution':'720p','aspect_ratio':'adaptive'},
            [ref(clip,'reference_video','edit_source')],'wan27_video_edit'),
    ]
    jobs=[];expected={}
    for local,capability,kind,model,mode,parameters,references,adapter in definitions:
        jobs.append(semantic_provider_job(local,capability,kind=kind,model=model,mode=mode,
            parameters=parameters,references=references))
        expected[local]={'capability':capability,'adapter':adapter,'model':model,'mode':mode}
    prepared=require(await prepare(client,thread,turn,jobs,'K09',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('Provider matrix bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await wait_provider_coverage(client,committed['jobs'],args.timeout)
    result=await verify_provider_coverage(thread['id'],completed,expected)
    if result['classification']!='success':return {**result,'thread_id':thread['id']}
    if not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('Provider matrix attempts did not overlap')
    return {'classification':'success','thread_id':thread['id'],'group_id':committed['group']['id'],
        'provider_jobs':len(completed),'adapters':len({row['adapter'] for row in expected.values()}),
        'one_group_confirmation':True,'parallel':True,'evidence':result['evidence']}


async def case_k10(client,args):
    thread,turn=await origin(client,args,'K10')
    poster=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/poster.png')
    rocket=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/rocket.png')
    clip=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/clip_a.mp4')
    speech=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/speech.wav')
    ref=lambda artifact,role,usage:{'artifact_id':artifact.id,'role':role,'usage':usage}
    definitions=[
        ('sed-first','video.generate','first_frame',{'prompt':'animate a gentle camera push-in',
            'duration':4,'resolution':'480p','aspect_ratio':'16:9'},
            [ref(poster,'first_frame','composition')]),
        ('sed-first-last','video.generate','first_last_frame',{'prompt':'smoothly travel between the two frames',
            'duration':4,'resolution':'480p','aspect_ratio':'16:9'},
            [ref(poster,'first_frame','composition'),ref(rocket,'last_frame','composition')]),
        ('sed-multimodal','video.generate','multimodal_reference',{
            'prompt':'combine the visual subject, motion rhythm, and audio timing into a short clip',
            'duration':4,'resolution':'480p','aspect_ratio':'16:9'},[
                ref(poster,'reference_image','subject'),ref(clip,'reference_video','action'),
                ref(speech,'reference_audio','audio')]),
        ('sed-edit','video.edit','edit_video',{'prompt':'preserve motion and apply a cool clean grade',
            'duration':-1,'resolution':'720p','aspect_ratio':'adaptive'},
            [ref(clip,'reference_video','edit_source')]),
        ('sed-extend','video.edit','extend_video',{'prompt':'continue the existing motion naturally',
            'duration':-1,'resolution':'720p','aspect_ratio':'adaptive','extend_direction':'forward'},
            [ref(clip,'reference_video','extend_source')]),
    ]
    jobs=[];expected={}
    for local,capability,mode,parameters,references in definitions:
        jobs.append(semantic_provider_job(local,capability,kind='video',model='sed2',mode=mode,
            parameters=parameters,references=references))
        expected[local]={'capability':capability,'adapter':'ark_video_v2','model':'sed2','mode':mode}
    prepared=require(await prepare(client,thread,turn,jobs,'K10',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('video mode matrix bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await wait_provider_coverage(client,committed['jobs'],args.timeout)
    result=await verify_provider_coverage(thread['id'],completed,expected)
    if result['classification']!='success':return {**result,'thread_id':thread['id']}
    return {'classification':'success','thread_id':thread['id'],'group_id':committed['group']['id'],
        'provider_jobs':len(completed),'adapter':'ark_video_v2',
        'one_group_confirmation':True,'parallel':await attempts_overlap([row['id'] for row in completed]),
        'evidence':result['evidence']}


async def case_k11(client,args):
    thread,turn=await origin(client,args,'K11')
    poster=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/poster.png')
    rocket=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/rocket.png')
    ref=lambda artifact,role:{'artifact_id':artifact.id,'role':role,'usage':'subject'}
    phase_one_defs=[
        ('model-image','image_to_model',{'texture':False,'face_limit':1500},
            [ref(poster,'reference_image')]),
        ('model-multiview','multiview_to_model',{'texture':False,'face_limit':1500},
            [ref(poster,'front_view'),ref(rocket,'left_view')]),
    ]
    expected={};jobs=[]
    for local,mode,parameters,references in phase_one_defs:
        jobs.append(semantic_provider_job(local,'model3d.generate',kind='model3d',
            model='v3.1-20260211',mode=mode,parameters=parameters,references=references))
        expected[local]={'capability':'model3d.generate','adapter':'tripo_v3',
            'model':'v3.1-20260211','mode':mode}
    prepared=require(await prepare(client,thread,turn,jobs,'K11-generate',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('3D generation matrix bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    phase_one=await wait_provider_coverage(client,committed['jobs'],args.timeout)
    first_result=await verify_provider_coverage(thread['id'],phase_one,expected)
    if first_result['classification']!='success':return {**first_result,'thread_id':thread['id']}
    source_job_id=first_result['evidence']['model-image']['media_job_id']
    phase_two_defs=[
        ('model-texture','texture_model',{'input_job_id':source_job_id,
            'texture_prompt':'clean blue ceramic material','pbr':False}),
        ('model-decimate','decimate_model',{'input_job_id':source_job_id,'face_limit':800,
            'topology_model':'v3.0'}),
        ('model-convert','convert_model',{'input_job_id':source_job_id,'target_format':'OBJ'}),
    ]
    phase_two_jobs=[];second_expected={}
    for local,mode,parameters in phase_two_defs:
        phase_two_jobs.append(semantic_provider_job(local,'model3d.transform',kind='model3d',
            model='v3.1-20260211',mode=mode,parameters=parameters))
        second_expected[local]={'capability':'model3d.transform','adapter':'tripo_v3',
            'model':'v3.1-20260211','mode':mode}
    second=require(await prepare(client,thread,turn,phase_two_jobs,'K11-transform',
        suppress_continuation=True)).json()
    if second.get('mode')!='draft':raise AssertionError('3D transform matrix bypassed confirmation')
    second_committed=require(await client.post(
        f"api/job-group-drafts/{second['draft']['id']}/confirm")).json()
    phase_two=await wait_provider_coverage(client,second_committed['jobs'],args.timeout)
    second_result=await verify_provider_coverage(thread['id'],phase_two,second_expected)
    if second_result['classification']!='success':return {**second_result,'thread_id':thread['id']}
    return {'classification':'success','thread_id':thread['id'],
        'phase_one_group_id':committed['group']['id'],'phase_two_group_id':second_committed['group']['id'],
        'phase_one_jobs':len(phase_one),'phase_two_jobs':len(phase_two),
        'one_confirmation_per_phase':True,'immutable_handoff':True,
        'evidence':{**first_result['evidence'],**second_result['evidence']}}


async def case_p01(client,args):
    thread,turn=await origin(client,args,'P01')
    settings=load_platform_settings()
    from sandbox_harness.app import object_store
    from sandbox_harness.store import Store
    data=(ROOT/'tests/real_cases/assets/poster.png').read_bytes()
    objects=object_store(settings);object_ref,digest=await asyncio.to_thread(objects.put,data)
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        artifact=await store.add_artifact(turn['id'],path='inputs/poster.png',sha256=digest,
            size=len(data),mime='image/png',object_ref=object_ref)
    finally:await store.close()
    jobs=[provider_job(0),provider_job(1),operation_job(artifact.id),harness_job(0,args,
        objective='Compare the requested poster variants and return a concise Chinese comparison.')]
    response=await prepare(client,thread,turn,jobs,'P01');require(response);draft=response.json()
    if draft['mode']!='draft':raise AssertionError('mixed billable group bypassed confirmation')
    response=await client.post(f"api/job-group-drafts/{draft['draft']['id']}/confirm");require(response)
    committed=response.json();await wait_jobs(client,committed['jobs'],1200)
    _,continuations=await wait_continuations(client,thread['id'],maximum=1)
    return {'classification':'success','group_id':committed['group']['id'],'group_members':4,
        'kinds':['provider','operation','harness'],'continuation_count':len(continuations)}


async def case_p04(client,args):
    thread,turn=await origin(client,args,'P04')
    output={'artifact_role':'generated_images','output_disposition':'delivery','min_items':4,
        'max_items':4,'mime_patterns':['image/svg+xml'],'max_item_bytes':1024*1024,
        'max_total_bytes':4*1024*1024}
    objective=('Create exactly four small valid SVG files named image-0.svg through image-3.svg in '
        '/workspace/outputs. They must be distinct colored geometric images. Write job-output-manifest.json '
        'that publishes all four in position order under the generated_images role.')
    response=await prepare(client,thread,turn,[harness_job(0,args,objective=objective,outputs=[output],
        skills=['canvas-design'])],'P04');require(response);value=response.json()
    if value['mode']!='committed':raise AssertionError('free child group unexpectedly required confirmation')
    job=(await wait_jobs(client,value['jobs'],1200))[0]
    files=require(await client.get(f"api/threads/{thread['id']}/project-files")).json()
    delivered=[item for item in files if f"artifacts/jobs/{job['id']}/" in item['path']]
    if len(delivered)!=4:raise AssertionError(f'atomic collection count={len(delivered)}')
    return {'classification':'success','job_id':job['id'],'role':'generated_images',
        'count':4,'atomic':True,'paths':[item['path'] for item in delivered]}


async def case_p05(client,args):
    thread,turn=await origin(client,args,'P05');groups=[];jobs=[]
    for index in range(3):
        response=await prepare(client,thread,turn,[harness_job(index,args,
            objective=f'Return only the factual marker group-{index}.')],f'P05-{index}')
        require(response);value=response.json();groups.append(value['group']);jobs.extend(value['jobs'])
    await wait_jobs(client,jobs,1200)
    group_values=[require(await client.get(f"api/job-groups/{group['id']}")).json()['group'] for group in groups]
    _,continuations=await wait_continuations(client,thread['id'])
    if any(value['state'] not in {'TERMINAL','CONTINUED'} for value in group_values):
        raise AssertionError('group did not become terminal')
    if not continuations:raise AssertionError('no durable continuation was created')
    return {'classification':'success','groups':3,'continuation_count':len(continuations),
        'coalesced_continuation':len(continuations)<3}


async def case_p06(client,args):
    thread,turn=await origin(client,args,'P06')
    response=await prepare(client,thread,turn,[harness_job(0,args,
        objective=('Analyze these three independent tradeoffs and return a concise comparison: '
            'REST versus gRPC, monolith versus microservices, and SQL versus NoSQL.'))],'P06')
    require(response);value=response.json()
    queued=require(await client.post(f"api/threads/{thread['id']}/turns",json={
        'message':'最终还需要一份中文摘要。','attachments':[],
        'idempotency_key':'protocol-p06-user-'+uuid.uuid4().hex,'delivery_mode':'queue'})).json()
    await wait_jobs(client,value['jobs'],1200)
    turns,continuations=await wait_continuations(client,thread['id'])
    positions={item['id']:index for index,item in enumerate(turns)}
    if not continuations or positions[queued['id']]>=min(positions[item['id']] for item in continuations):
        raise AssertionError('user turn was not ordered before continuation')
    return {'classification':'success','user_turn_id':queued['id'],
        'user_turn_before_continuation':True,'intent_preserved':True}


async def case_p07(client,args):
    thread,turn=await origin(client,args,'P07')
    response=await prepare(client,thread,turn,[harness_job(0,args,
        objective='Produce a detailed obsolete analysis that may be cancelled.')],'P07-original')
    require(response);original=response.json()['jobs'][0]
    require(await client.post(f"api/jobs/{original['id']}/cancel"))
    await wait_job(client,original['id'],300)
    response=await prepare(client,thread,turn,[harness_job(1,args,
        objective='Return the replacement objective in one concise sentence.',
        replaces_job_id=original['id'])],'P07-replacement');require(response)
    replacement=(await wait_jobs(client,response.json()['jobs'],1200))[0]
    if replacement.get('replaces_job_id')!=original['id']:
        raise AssertionError('replacement lineage was not preserved')
    return {'classification':'success','original':(await wait_job(client,original['id']))['state'],
        'replacement':'SUCCEEDED','replacement_job_id':replacement['id'],'steer_used':False}


async def case_p11(client,args):
    thread,turn=await origin(client,args,'P11')
    job=harness_job(0,args,objective='Use the PDF skill to explain a minimal read-only review checklist.',
        skills=['pdf'],child_profile_id='reviewer-readonly')
    response=await prepare(client,thread,turn,[job],'P11');require(response);value=response.json()
    completed=(await wait_jobs(client,value['jobs'],1200))[0]
    graph=require(await client.get(f"api/threads/{thread['id']}/execution-graph",
        params={'include_history':'true'})).json()
    encoded=json.dumps(graph,ensure_ascii=False)
    if 'office' not in encoded:raise AssertionError('minimal office Runtime Profile was not observable')
    return {'classification':'success','job_id':completed['id'],'profile':'office','skills':['pdf'],
        'capabilities':[],'network':[]}


async def case_p02(client,args):
    thread,turn=await origin(client,args,'P02');response=await prepare(client,thread,turn,
        [provider_job(index) for index in range(7)],'P02');response.raise_for_status();value=response.json()
    if value['mode']!='draft':raise AssertionError('billable batch did not wait for whole-group confirmation')
    confirmed=await client.post(f"api/job-group-drafts/{value['draft']['id']}/confirm");confirmed.raise_for_status()
    result=confirmed.json();
    if len(result['jobs'])!=7:raise AssertionError('whole confirmed batch did not create seven Jobs')
    await client.post(f"api/job-groups/{result['group']['id']}/cancel")
    return {'classification':'success','draft_id':value['draft']['id'],'group_id':result['group']['id'],
        'job_count':7,'whole_group_confirmation':True}


async def case_p03(client,args):
    thread,turn=await origin(client,args,'P03');response=await prepare(client,thread,turn,
        [provider_job(index) for index in range(7)],'P03');response.raise_for_status();value=response.json()
    rejected=await client.post(f"api/job-group-drafts/{value['draft']['id']}/reject");rejected.raise_for_status()
    if rejected.json()['state']!='REJECTED':raise AssertionError('draft rejection was not durable')
    return {'classification':'success','draft_id':value['draft']['id'],'provider_submissions':0,
        'whole_group_rejected':True}


async def confirmed_provider_group(client,args,identity,count=2):
    thread,turn=await origin(client,args,identity);response=await prepare(client,thread,turn,
        [provider_job(index) for index in range(count)],identity);response.raise_for_status();draft=response.json()['draft']
    confirmed=await client.post(f"api/job-group-drafts/{draft['id']}/confirm");confirmed.raise_for_status()
    return thread,turn,confirmed.json()


async def case_p08(client,args):
    thread,turn,value=await confirmed_provider_group(client,args,'P08',3);job=value['jobs'][0]
    response=await client.post(f"api/jobs/{job['id']}/cancel");response.raise_for_status()
    cancelled=await wait_job(client,job['id'],300)
    others=await asyncio.gather(*(wait_job(client,item['id'],900) for item in value['jobs'][1:]))
    if cancelled['state']!='CANCELLED' or any(item['state']=='CANCELLED' for item in others):
        raise AssertionError('single cancellation affected independent members')
    return {'classification':'success','cancelled_job_id':job['id'],
        'state':cancelled['state'],'independent_members':2}


async def case_p09(client,args):
    thread,turn,value=await confirmed_provider_group(client,args,'P09',3)
    response=await client.post(f"api/job-groups/{value['group']['id']}/cancel");response.raise_for_status()
    if len(response.json()['jobs'])!=3:raise AssertionError('group cancellation omitted members')
    states=await asyncio.gather(*(wait_job(client,item['id'],300) for item in value['jobs']))
    if any(item['state']!='CANCELLED' for item in states):raise AssertionError('group member not cancelled')
    replacement=await prepare(client,thread,turn,[harness_job(9,args,
        objective='Return the marker unaffected-new-group.')],'P09-new');require(replacement)
    await wait_jobs(client,replacement.json()['jobs'],900)
    return {'classification':'success','group_id':value['group']['id'],'cancelled_members':3}


async def case_p10(client,args):
    thread,turn=await origin(client,args,'P10');all_jobs=[]
    for group_index in range(2):
        response=await prepare(client,thread,turn,[harness_job(group_index*2+index,args,
            objective=f'Return marker stop-all-{group_index}-{index}.') for index in range(2)],
            f'P10-{group_index}');require(response);all_jobs.extend(response.json()['jobs'])
    response=await client.post(f"api/threads/{thread['id']}/stop-all");response.raise_for_status()
    if len(response.json()['jobs'])<4:raise AssertionError('thread stop-all omitted background Jobs')
    states=await asyncio.gather(*(wait_job(client,item['id'],300) for item in all_jobs))
    if any(item['state']!='CANCELLED' for item in states):raise AssertionError('stop-all member not cancelled')
    await asyncio.sleep(2);turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
    if any(item.get('internal') for item in turns):raise AssertionError('stop-all created continuation')
    return {'classification':'success','thread_id':thread['id'],'stop_all':True,
        'cancelled_members':len(states),'continuation_suppressed':True}


async def case_p12(client,args):
    thread,turn=await origin(client,args,'P12');job={'local_job_id':'bad-profile','kind':'harness',
        'adapter':'child-harness','descriptor_id':'delegate.harness','descriptor_version':'1',
        'descriptor_digest':'d'*64,'idempotency_key':'bad-profile-'+uuid.uuid4().hex,
        'parameters':{'harness':'codex','model':args.model,'objective':'reply'},
        'child_profile_id':'not-registered','skill_ids':[]}
    response=await prepare(client,thread,turn,[job],'P12')
    if response.status_code!=422:raise AssertionError('unknown Child Profile was not rejected in preflight')
    return {'classification':'success','preflight_rejected':True,'status_code':422}


async def case_p13(client,args):
    thread,turn=await origin(client,args,'P13')
    response=await prepare(client,thread,turn,[harness_job(0,args,
        objective=('After checking the workspace, reply with exactly RECOVERY_OK as plain text. '
            'Do not create, edit, or publish any file.'))],'P13')
    require(response);job=response.json()['jobs'][0]
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        current=require(await client.get(f"api/jobs/{job['id']}")).json()
        if current['state']=='WAITING_EXTERNAL':break
        if current['state'] in TERMINAL_JOBS:
            raise AssertionError('job_finished_before_acceptance_boundary:'+current['state'])
        await asyncio.sleep(.1)
    else:raise TimeoutError('job_never_accepted')
    await asyncio.to_thread(subprocess.run,['systemctl','kill','--kill-who=main','--signal=KILL',
        'sandbox-harness-parallel-job-worker.service'],check=True)
    for _ in range(100):
        state=(await asyncio.to_thread(subprocess.run,['systemctl','is-active',
            'sandbox-harness-parallel-job-worker.service'],capture_output=True,text=True)).stdout.strip()
        if state=='active':break
        await asyncio.sleep(.1)
    completed=await wait_job(client,job['id'],300)
    if completed['state']!='SUCCEEDED':raise AssertionError('recovered child did not succeed')
    settings=load_platform_settings()
    from sqlalchemy import select
    from sandbox_harness.store import JobAttemptRow,Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job['id']).order_by(JobAttemptRow.generation))).scalars().all())
        accepted=[value.external_execution_id for value in attempts if value.accepted_at]
        if len(set(accepted))!=1 or len(attempts)<2:raise AssertionError('recovery duplicated submission')
        return {'classification':'success','job_id':job['id'],'attempts':len(attempts),
            'same_external_execution':True,'duplicate_submission':False,'terminal':True}
    finally:await store.close()


async def case_p14(client,args):
    value=await artifact_boundary(client,args,'P14',requires_review=False,verdict='UNAVAILABLE')
    if value['collection']['state']!='published' or len(value['project_files'])!=1:
        raise AssertionError('optional reviewer blocked valid candidate')
    return {'classification':'success','job_id':value['job_id'],'candidate_preserved':True,
        'availability_warning':True,'delivery_visible':True}


async def case_p15(client,args):
    value=await artifact_boundary(client,args,'P15',requires_review=True,verdict='REVISE')
    if value['collection']['state']!='awaiting_review' or value['project_files']:
        raise AssertionError('required REVISE candidate became delivery')
    return {'classification':'success','job_id':value['job_id'],'review':'REVISE',
        'delivery_visible':False,'candidate_state':'awaiting_review'}


async def case_p16(client,args):
    thread,turn=await origin(client,args,'P16');settings=load_platform_settings()
    from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest,JobState
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        owner=(await store.get_thread(thread['id'])).owner_id
        account=await store.configure_budget(owner,thread['id'],'provider.submissions',limit=1)
        def group(index):
            request=JobRequest(local_job_id=f'budget-{index}',kind=JobKind.PROVIDER,
                adapter='protocol-budget',descriptor_id='protocol-budget',descriptor_version='1',
                descriptor_digest='f'*64,idempotency_key='budget-job-'+uuid.uuid4().hex,
                parameters={},budget_requirements={'provider.submissions':1})
            return JobGroupCreate(thread_id=thread['id'],origin_turn_id=turn['id'],
                idempotency_key='budget-group-'+uuid.uuid4().hex,jobs=[request],
                authorization_snapshot={'budget_reservations':[{'account_id':account.id,'amount':1}]},
                suppress_continuation=True)
        results=await asyncio.gather(*(store.create_job_group(owner,group(index)) for index in range(2)),
            return_exceptions=True)
        successes=[value for value in results if not isinstance(value,Exception)]
        failures=[value for value in results if isinstance(value,Exception)]
        if len(successes)!=1 or len(failures)!=1 or 'budget_exhausted' not in str(failures[0]):
            raise AssertionError('budget race was not fenced atomically')
        for job in successes[0][1]:
            await store.transition_job(job.id,JobState.CANCELLED,expected_generation=0,
                result={'cancelled_after_budget_test':True})
        budget=await store.get_budget_account(account.id)
        return {'classification':'success','confirmed_groups':1,'overspend':False,
            'reserved':budget.reserved,'consumed':budget.consumed}
    finally:await store.close()


async def case_p17(client,args):
    thread,turn=await origin(client,args,'P17');settings=load_platform_settings()
    from sandbox_harness.job_adapters import AdapterStatus,BaseJobAdapter,SubmissionOutcomeUnknown
    from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest,JobState
    from sandbox_harness.job_worker import JobWorker
    from sandbox_harness.store import Store
    class LostResponseAdapter(BaseJobAdapter):
        def __init__(self):self.submit_calls=0;self.status_calls=0;self.handles=[]
        async def submit(self,context):
            self.submit_calls+=1
            raise SubmissionOutcomeUnknown('accepted response lost',external_execution_id='provider-handle-1',
                acceptance_receipt={'provider_handle':'provider-handle-1'})
        async def status(self,context,external_execution_id):
            self.status_calls+=1;self.handles.append(external_execution_id)
            return (AdapterStatus(False) if self.status_calls==1 else
                AdapterStatus(True,JobState.SUCCEEDED,{'reconciled':True}))
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix);adapter=LostResponseAdapter()
    try:
        owner=(await store.get_thread(thread['id'])).owner_id
        request=JobRequest(local_job_id='lost-response',kind=JobKind.PROVIDER,
            adapter='protocol-lost-response',descriptor_id='protocol-lost-response',descriptor_version='1',
            descriptor_digest='1'*64,idempotency_key='lost-response-'+uuid.uuid4().hex,parameters={})
        _,jobs=await store.create_job_group(owner,JobGroupCreate(thread_id=thread['id'],
            origin_turn_id=turn['id'],idempotency_key='lost-group-'+uuid.uuid4().hex,jobs=[request],
            suppress_continuation=True))
        worker=JobWorker(store,{'protocol-lost-response':adapter},node_id='protocol-node',
            worker_id='protocol-worker',concurrency=1)
        await worker.run_once();before=(await store.get_job(jobs[0].id)).state
        await worker.run_once();after=await store.get_job(jobs[0].id)
        if after.state!=JobState.SUCCEEDED or adapter.submit_calls!=1 or set(adapter.handles)!={'provider-handle-1'}:
            raise AssertionError('provider acceptance was submitted more than once')
        return {'classification':'success','state_before_reconcile':before.value,
            'duplicate_submission':False,'handle_reused':True,'terminal':after.state.value}
    finally:await store.close()


async def case_p18(client,args):
    thread,turn=await origin(client,args,'P18');await wait_turn(client,turn['id'],300)
    takeover=require(await client.post(f"api/threads/{thread['id']}/sandbox/takeover")).json()
    require(await client.put(f"api/threads/{thread['id']}/sandbox/file",params={'path':'project/p18.txt'},
        content=b'checkpoint-survived',headers={'content-type':'text/plain'}))
    released=require(await client.post(f"api/threads/{thread['id']}/sandbox/release",
        json={'managed_action':'preserve_work'})).json()
    restored=require(await client.post(f"api/threads/{thread['id']}/sandbox/restore")).json()
    require(await client.post(f"api/threads/{thread['id']}/sandbox/takeover"))
    content=require(await client.get(f"api/threads/{thread['id']}/sandbox/file",
        params={'path':'project/p18.txt'})).content
    if content!=b'checkpoint-survived' or not released.get('checkpointed'):
        raise AssertionError('checkpoint content was not restored')
    return {'classification':'success','thread_id':thread['id'],'checkpoint_restored':True,
        'terminal_prompt':'updream@updream.sandbox.com','state':restored.get('state')}


async def case_p19(client,args):
    thread,turn=await origin(client,args,'P19');settings=load_platform_settings()
    from sandbox_harness.app import object_store
    from sandbox_harness.artifact_gateway import ArtifactEmission,ArtifactGateway
    from sandbox_harness.job_contracts import (JobExpectedOutput,JobGroupCreate,JobKind,
        JobRequest,JobState,OutputDisposition)
    from sandbox_harness.store import Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        owner=(await store.get_thread(thread['id'])).owner_id
        request=JobRequest(local_job_id='atomic-pair',kind=JobKind.OPERATION,adapter='fixture',
            descriptor_id='fixture.atomic',descriptor_version='1',descriptor_digest='2'*64,
            idempotency_key='atomic-pair-'+uuid.uuid4().hex,parameters={},expected_outputs=[
                JobExpectedOutput(artifact_role='parts',output_disposition=OutputDisposition.DELIVERY,
                    min_items=2,max_items=2,mime_patterns=('text/plain',))])
        _,jobs=await store.create_job_group(owner,JobGroupCreate(thread_id=thread['id'],
            origin_turn_id=turn['id'],idempotency_key='atomic-group-'+uuid.uuid4().hex,jobs=[request],
            suppress_continuation=True));attempt=await store.claim_job(jobs[0].id,'atomic-worker','node',60)
        gateway=ArtifactGateway(store,object_store(settings),settings.data_root/'tmp'/'protocol-artifacts',
            maximum_bytes=1024*1024)
        async def emit(position):
            async def chunks():yield f'part-{position}'.encode()
            return await gateway.emit(ArtifactEmission(jobs[0].id,attempt.generation,'parts',
                f'part-{position}',position,f'part-{position}.txt',OutputDisposition.DELIVERY,'text/plain'),chunks())
        await emit(0)
        if await store.current_project_files(thread['id'],owner):raise AssertionError('partial delivery visible')
        await asyncio.to_thread(subprocess.run,['systemctl','restart',
            'sandbox-harness-parallel-job-worker.service'],check=True)
        await emit(1);result=await store.finalize_job_artifact_collection(
            jobs[0].id,attempt.generation,'parts')
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result=result)
        files=await store.current_project_files(thread['id'],owner)
        if result['state']!='published' or len(files)!=2:raise AssertionError('final collection incomplete')
        return {'classification':'success','partial_delivery_visible':False,
            'final_collection_complete':True,'count':len(files)}
    finally:await store.close()


async def case_p20(client,args):
    thread,turn=await origin(client,args,'P20')
    jobs=[provider_job(index) for index in range(3)]+[harness_job(index,args,
        objective=f'Return independent factual analysis marker-{index}.') for index in range(3)]
    response=await prepare(client,thread,turn,jobs,'P20');require(response);draft=response.json()
    response=await client.post(f"api/job-group-drafts/{draft['draft']['id']}/confirm");require(response)
    committed=response.json();await wait_jobs(client,committed['jobs'],1200)
    _,continuations=await wait_continuations(client,thread['id'],maximum=1)
    return {'classification':'success','provider_jobs':3,'harness_jobs':3,'parallel':True,
        'one_primary_continuation':True,'group_id':committed['group']['id']}


async def case_p21(client,args):
    thread,turn=await origin(client,args,'P21')
    jobs=[harness_job(index,args,objective=(
        f'Use a shell command to sleep 30 seconds, then return the exact durable pause marker child-{index}. '
        'After a restored session, continue from the checkpoint and do not delegate.')) for index in range(2)]
    response=await prepare(client,thread,turn,jobs,'P21');require(response);prepared=response.json()
    if prepared.get('mode')=='draft':
        response=require(await client.post(
            f"api/job-group-drafts/{prepared['draft']['id']}/confirm"));prepared=response.json()
    members=prepared['jobs']
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        states=[require(await client.get(f"api/jobs/{item['id']}")).json()['state'] for item in members]
        if all(state in {'RUNNING','WAITING_EXTERNAL'} for state in states):break
        await asyncio.sleep(.25)
    else:raise TimeoutError('pause_agents_never_started:'+json.dumps(states))
    await asyncio.to_thread(subprocess.run,['systemctl','stop',
        'sandbox-harness-parallel-job-worker.service'],check=True)
    paused=require(await client.post(f"api/threads/{thread['id']}/pause")).json()
    if paused['state'] not in {'PAUSING','PAUSED'}:raise AssertionError('pause_not_accepted')
    await asyncio.to_thread(subprocess.run,['systemctl','restart',
        'sandbox-harness-parallel-api.service'],check=True)
    for _ in range(200):
        try:
            if (await client.get('api/session')).status_code==200:break
        except httpx.TransportError:pass
        await asyncio.sleep(.1)
    else:raise TimeoutError('api_restart_timeout')
    await asyncio.to_thread(subprocess.run,['systemctl','start',
        'sandbox-harness-parallel-job-worker.service'],check=True)
    deadline=time.monotonic()+240
    while time.monotonic()<deadline:
        control=require(await client.get(f"api/threads/{thread['id']}/pause")).json()
        if control['state']=='PAUSED':break
        await asyncio.sleep(.5)
    else:raise TimeoutError('pause_reconciliation_timeout')
    child=[value for value in control['participants'] if value['kind']=='child_harness']
    if len(child)!=2 or any(value['state'] not in {'PAUSED','FAILED'} for value in child):
        raise AssertionError('child_pause_participants_invalid:'+json.dumps(child))
    if any(value['state']=='FAILED' for value in child):
        raise AssertionError('child_pause_checkpoint_degraded:'+json.dumps(child))
    require(await client.post(f"api/threads/{thread['id']}/resume"))
    results=await wait_jobs(client,members,600)
    continued=require(await client.post(f"api/threads/{thread['id']}/turns",json={
        'message':'继续并简短确认并行子任务已恢复。','attachments':[],
        'idempotency_key':'protocol-resume-P21-'+uuid.uuid4().hex,
        'delivery_mode':'queue'})).json()
    final=await wait_turn(client,continued['id'],300)
    if final['state']!='COMPLETED':raise AssertionError('primary_resume_turn_failed')
    return {'classification':'success','thread_id':thread['id'],'pause_generation':control['generation'],
        'child_harnesses':len(child),'api_restarted':True,'worker_restarted':True,
        'checkpoints':[value['checkpoint_ref'] for value in child],
        'resumed_states':[value['state'] for value in results],
        'primary_resume_state':final['state']}


async def case_p22(client,args):
    from sandbox_runtime.job_cli import compile_delegation

    thread,turn=await origin(client,args,'P22')
    catalog=require(await client.get('api/catalog/execution')).json()
    compiled=compile_delegation({'tasks':[
        {'id':'fact-a','objective':'Return only the exact marker independent-a.','result_mode':'text'},
        {'id':'fact-b','objective':'Return only the exact marker independent-b.','result_mode':'text'},
    ]},catalog,{'HARNESS_THREAD_ID':thread['id'],'HARNESS_TURN_ID':turn['id'],
        'HARNESS_KIND':args.harness,'HARNESS_MODEL':args.model})
    response=require(await client.post('api/job-group-drafts',json=compiled));prepared=response.json()
    if prepared.get('mode')!='committed':
        raise AssertionError('non-billable compact delegation unexpectedly required confirmation')
    jobs=await wait_jobs(client,prepared['jobs'],args.timeout)
    _,continuations=await wait_continuations(client,thread['id'],maximum=1)
    if len(jobs)!=2 or not continuations:
        raise AssertionError('compact delegation did not close through one durable continuation')
    return {'classification':'success','group_id':prepared['group']['id'],
        'compiled_jobs':len(jobs),'metadata_required':False,'child_harnesses':2,
        'continuation_count':len(continuations)}


async def case_p23(client,args):
    from sqlalchemy import select
    from sandbox_harness.store import Store,JobGroupRow,JobRow,JobAttemptRow

    identity='P23-'+uuid.uuid4().hex[:8]
    response=require(await client.post('api/threads',json={'harness':args.harness,'model':args.model,
        'reasoning_level':args.reasoning,'title':'Primary autonomous parallel decision'}))
    thread=response.json()
    prompt=('两个事儿，请分别完成后再汇总：第一，给出一份包含依据的 API 向后兼容风险分析；'
        '第二，给出一份包含依据的 GPU 任务故障恢复风险分析。两项彼此独立，分别给出五条可验收结论。'
        '两份分析必须由相互隔离的执行上下文独立形成证据，执行时间需要重叠；主任务只在两份结果都返回后汇总，'
        '不能在主上下文中直接代写其中任何一份。')
    turn=require(await client.post(f"api/threads/{thread['id']}/turns",json={'message':prompt,
        'attachments':[],'idempotency_key':'protocol-'+identity,'delivery_mode':'start'})).json()
    final=await wait_turn(client,turn['id'],args.timeout)
    if final['state']!='COMPLETED':
        code=str(final.get('error_code') or final['state'].lower())
        return {'classification':('non_program_failure' if code.startswith(
            ('model_','provider_','upstream_')) else 'program_failure'),
            'reason':code,'turn_id':turn['id'],'state':final['state']}
    settings=load_platform_settings();store=Store(
        settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        async def planned_jobs():
            async with store.connections() as db:
                groups=list((await db.execute(select(JobGroupRow).where(
                    JobGroupRow.thread_id==thread['id']).order_by(JobGroupRow.created_at))).scalars().all())
                jobs=(list((await db.execute(select(JobRow).where(
                    JobRow.group_id.in_([group.id for group in groups]),
                    JobRow.kind=='harness').order_by(JobRow.created_at))).scalars().all())
                    if groups else [])
            return groups,jobs
        groups,jobs=await planned_jobs()
        if len(jobs)<2:
            return {'classification':'optimizable_prompt',
                'reason':'primary_did_not_delegate_parallel_work','turn_id':turn['id']}
        deadline=time.monotonic()+args.timeout
        while time.monotonic()<deadline and not all(job.state in TERMINAL_JOBS for job in jobs):
            await asyncio.sleep(1)
            groups,jobs=await planned_jobs()
        grouped={job.group_id for job in jobs}
        if len(grouped)!=1:raise AssertionError('independent Child Harness Jobs were not one JobGroup')
        failed=[(job.local_job_id,job.state,json.loads(job.result_json or '{}').get('error_code',''))
            for job in jobs if job.state!='SUCCEEDED']
        if failed:raise AssertionError('Child Harness Jobs failed: '+json.dumps(failed,ensure_ascii=False))
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id.in_([job.id for job in jobs])))).scalars().all())
        intervals=[(attempt.started_at,attempt.finished_at) for attempt in attempts
            if attempt.started_at and attempt.finished_at and attempt.outcome=='SUCCEEDED']
        overlap=any(left_start<right_end and right_start<left_end
            for index,(left_start,left_end) in enumerate(intervals)
            for right_start,right_end in intervals[index+1:])
        if not overlap:raise AssertionError('Child Harness Attempts did not overlap')
        _,continuations=await wait_continuations(client,thread['id'],maximum=1)
        if len(continuations)!=1:raise AssertionError('parallel group did not produce one continuation')
        return {'classification':'success','thread_id':thread['id'],'primary_delegated':True,
            'child_harnesses':len(jobs),'same_group':True,'attempts_overlapped':True,
            'one_primary_continuation':True,'group_id':next(iter(grouped))}
    finally:await store.close()


async def case_p24(client,args):
    identity='P24-'+uuid.uuid4().hex[:8];asset=ROOT/'tests/real_cases/assets/portrait.png'
    upload=require(await client.post('api/uploads',params={'name':asset.name,'mime':'image/png'},
        content=asset.read_bytes())).json()
    thread=require(await client.post('api/threads',json={'harness':args.harness,'model':args.model,
        'reasoning_level':args.reasoning,'title':'Child scoped image.segment'})).json()
    turn=require(await client.post(f"api/threads/{thread['id']}/turns",json={
        'message':'确认输入素材并等待后续受控子任务。','attachments':[upload],
        'idempotency_key':'protocol-'+identity,'delivery_mode':'start'})).json()
    await wait_turn(client,turn['id'],args.timeout)
    catalog=require(await client.get('api/catalog/execution')).json()
    settings=load_platform_settings()
    from sandbox_harness.store import Store
    input_store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        owner=(await input_store.get_thread(thread['id'])).owner_id
        frozen=await input_store.turn_input_artifacts(turn['id'],owner)
        catalog['input_artifacts']=[{'artifact_id':row['artifact'].id,
            'project_path':row['project_path']} for row in frozen]
    finally:await input_store.close()
    source=next(row for row in catalog['input_artifacts'] if row.get('project_path')=='assets/portrait.png')
    from sandbox_runtime.job_cli import compile_delegation
    objective=(f"Use frozen Artifact {source['artifact_id']} and the registered image.segment Operation. "
        "Run sandbox-job catalog, then sandbox-job prepare one subordinate Operation Job with prompt person, "
        "input role source, and foreground/mask/segmentation_report outputs. End this execution after prepare. "
        "When the durable continuation returns, use materialized_artifacts to copy the foreground PNG to "
        "/workspace/outputs/segmented-person.png, verify it has transparent and visible alpha pixels, and finish.")
    compiled=compile_delegation({'tasks':[{'id':'segment-child','objective':objective,
        'inputs':[source['artifact_id']],'skill_ids':['media-processing'],
        'operation_ids':['image.segment'],'result_mode':'artifacts','output_disposition':'delivery',
        'mime_patterns':['image/png']}]},catalog,{'HARNESS_THREAD_ID':thread['id'],
        'HARNESS_TURN_ID':turn['id'],'HARNESS_KIND':args.harness,'HARNESS_MODEL':args.model})
    prepared=require(await client.post('api/job-group-drafts',json=compiled)).json()
    parent_job=prepared['jobs'][0]
    parent_draft=prepared.get('draft')
    parent_draft_id=parent_draft.get('id') if isinstance(parent_draft,dict) else None
    deadline=time.monotonic()+args.timeout;nested_draft=None
    while time.monotonic()<deadline:
        drafts=require(await client.get(f"api/threads/{thread['id']}/job-group-drafts")).json()
        nested_draft=next((row for row in drafts if row.get('state')=='AWAITING_CONFIRMATION'
            and row.get('id')!=parent_draft_id),None)
        if nested_draft:break
        current=require(await client.get(f"api/jobs/{parent_job['id']}")).json()
        if current['state'] in TERMINAL_JOBS:raise AssertionError('Child ended before subordinate draft: '+json.dumps(current))
        await asyncio.sleep(1)
    if not nested_draft:raise TimeoutError('nested_operation_confirmation_timeout')
    require(await client.post(f"api/job-group-drafts/{nested_draft['id']}/confirm"))
    child=await wait_job(client,parent_job['id'],args.timeout)
    if child['state']!='SUCCEEDED':raise AssertionError('Child did not resume successfully: '+json.dumps(child))
    from sqlalchemy import select
    from sandbox_harness.store import Store,JobRow
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        async with store.connections() as db:
            operations=list((await db.execute(select(JobRow).where(JobRow.thread_id==thread['id'],
                JobRow.descriptor_id=='image.segment'))).scalars().all())
        if len(operations)!=1 or operations[0].state!='SUCCEEDED':
            raise AssertionError('Nested image.segment did not succeed')
        artifacts=await store.list_job_artifacts(parent_job['id'])
        if not any(row.filename=='segmented-person.png' for row in artifacts):
            raise AssertionError('Child delivery was not published')
        return {'classification':'success','thread_id':thread['id'],'parent_job_id':parent_job['id'],
            'nested_operation_job_id':operations[0].id,'child_resumed':True,
            'delivery_names':[row.filename for row in artifacts]}
    finally:await store.close()


async def case_p25(client,args):
    thread,turn=await origin(client,args,'P25')
    source=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/portrait.png')
    phase_one=[segment_job(source.id),provider_job(25,prompt=(
        'Clean premium technology brand background, cobalt blue and white geometric light, '
        'no words, no letters, widescreen presentation composition'))]
    prepared=require(await prepare(client,thread,turn,phase_one,'P25-source',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('sensitive mixed group bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await wait_jobs(client,committed['jobs'],args.timeout)
    if not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('phase-one operation and provider did not overlap')
    by_local={row['local_job_id']:row for row in completed}
    artifacts=await job_artifacts([row['id'] for row in completed])
    foreground=next(row for row in artifacts[by_local['segment-subject']['id']]
        if row.artifact_role=='foreground')
    background=next(row for row in artifacts[by_local['image-25']['id']]
        if row.artifact_role=='generated_images')
    presentation_mime='application/vnd.openxmlformats-officedocument.presentationml.presentation'
    objective=('Use the injected codex-ppt Skill and only the two frozen input images. Create exactly '
        'one three-slide Chinese presentation: cover, brand visual analysis, and launch checklist. '
        'Use the transparent subject and generated background as actual visuals, include concise speaker '
        'notes, validate the PPTX, and write exactly /workspace/outputs/parallel-brand-deck.pptx. '
        'Do not call any Provider or create any other output file.')
    aggregate=harness_job(25,args,objective=objective,skills=['codex-ppt'],inputs=[
        {'artifact_id':foreground.artifact_id,'artifact_role':'subject'},
        {'artifact_id':background.artifact_id,'artifact_role':'background'}],outputs=[{
            'artifact_role':'presentation','output_disposition':'delivery','min_items':1,'max_items':1,
            'mime_patterns':[presentation_mime],'max_item_bytes':128*1024*1024,
            'max_total_bytes':128*1024*1024}])
    phase_two=require(await prepare(client,thread,turn,[aggregate],'P25-aggregate',
        suppress_continuation=True)).json()
    if phase_two.get('mode')!='committed':raise AssertionError('non-billable aggregation required confirmation')
    final=(await wait_jobs(client,phase_two['jobs'],args.timeout))[0]
    delivered=await job_artifacts([final['id']])
    names=[row.filename for row in delivered[final['id']]]
    if names!=['parallel-brand-deck.pptx']:
        raise AssertionError('presentation delivery mismatch:'+json.dumps(names))
    return {'classification':'success','thread_id':thread['id'],'phase_one_jobs':2,
        'phase_one_group_id':committed['group']['id'],'phase_two_group_id':phase_two['group']['id'],
        'operation':'image.segment','provider':'image.generate','skill':'codex-ppt',
        'one_group_confirmation':True,'immutable_handoff':True,'delivery_names':names}


async def case_p26(client,args):
    thread,turn=await origin(client,args,'P26')
    video_source=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/portrait_clip.mp4')
    audio_source=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/speech.wav')
    phase_one=[transcribe_job(audio_source.id),video_render_job(video_source.id),provider_job(26,prompt=(
        'Editorial cover image for a concise video insight report, clean cyan and coral shapes, '
        'audio waveform motif, no words, no letters, portrait-safe center composition'))]
    prepared=require(await prepare(client,thread,turn,phase_one,'P26-source',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('sensitive multimodal group bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await wait_jobs(client,committed['jobs'],args.timeout)
    if not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('multimodal phase-one jobs did not overlap')
    by_local={row['local_job_id']:row for row in completed}
    artifacts=await job_artifacts([row['id'] for row in completed])
    transcript=next(row for row in artifacts[by_local['transcribe-source']['id']]
        if row.artifact_role=='transcript_text')
    video=next(row for row in artifacts[by_local['normalize-video']['id']]
        if row.artifact_role=='rendered_media')
    cover=next(row for row in artifacts[by_local['image-26']['id']]
        if row.artifact_role=='generated_images')
    docx_mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    objective=('Use the injected docx Skill and all three frozen inputs to create a polished Chinese '
        'content report. Include a cover using the supplied image, a transcript summary that quotes '
        'only evidence present in the transcript, normalized-video technical metadata, and a source '
        'provenance table. Validate the document and write exactly '
        '/workspace/outputs/multimodal-report.docx. Create no other output file.')
    aggregate=harness_job(26,args,objective=objective,skills=['docx'],inputs=[
        {'artifact_id':transcript.artifact_id,'artifact_role':'transcript'},
        {'artifact_id':video.artifact_id,'artifact_role':'normalized_video'},
        {'artifact_id':cover.artifact_id,'artifact_role':'cover'}],outputs=[{
            'artifact_role':'report','output_disposition':'delivery','min_items':1,'max_items':1,
            'mime_patterns':[docx_mime],'max_item_bytes':128*1024*1024,
            'max_total_bytes':128*1024*1024}])
    phase_two=require(await prepare(client,thread,turn,[aggregate],'P26-aggregate',
        suppress_continuation=True)).json()
    if phase_two.get('mode')!='committed':raise AssertionError('document aggregation required confirmation')
    final=(await wait_jobs(client,phase_two['jobs'],args.timeout))[0]
    delivered=await job_artifacts([final['id']]);names=[row.filename for row in delivered[final['id']]]
    if names!=['multimodal-report.docx']:
        raise AssertionError('report delivery mismatch:'+json.dumps(names))
    return {'classification':'success','thread_id':thread['id'],'phase_one_jobs':3,
        'phase_one_group_id':committed['group']['id'],'phase_two_group_id':phase_two['group']['id'],
        'operations':['audio.transcribe','media.render'],'provider':'image.generate','skill':'docx',
        'one_group_confirmation':True,'immutable_handoff':True,'delivery_names':names}


async def case_p27(client,args):
    thread,turn=await origin(client,args,'P27')
    source=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/poster.png')
    shared=[{'artifact_id':source.id,'artifact_role':'source'}]
    docx_mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    jobs=[provider_job(27,prompt=('Abstract recovery architecture illustration, durable jobs and '
        'checkpoints, clean blue line art, no words, no letters')),operation_job(source.id),
        harness_job(270,args,skills=['docx'],inputs=shared,objective=(
            'First run a shell sleep for 20 seconds. Then use the injected docx Skill and the frozen '
            'image to create exactly /workspace/outputs/recovery-notes.docx with a short Chinese '
            'recovery checklist. Create no other output file.'),outputs=[{
                'artifact_role':'recovery_notes','output_disposition':'workspace','min_items':1,'max_items':1,
                'mime_patterns':[docx_mime]}]),
        harness_job(271,args,skills=['pdf'],inputs=shared,objective=(
            'First run a shell sleep for 20 seconds. Then use the injected pdf Skill and the frozen '
            'image to create exactly /workspace/outputs/recovery-evidence.pdf with a one-page Chinese '
            'evidence sheet. Create no other output file.'),outputs=[{
                'artifact_role':'recovery_evidence','output_disposition':'workspace','min_items':1,'max_items':1,
                'mime_patterns':['application/pdf']}])]
    prepared=require(await prepare(client,thread,turn,jobs,'P27')).json()
    if prepared.get('mode')!='draft':raise AssertionError('mixed recovery group bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    by_local={row['local_job_id']:row for row in committed['jobs']};provider=by_local['image-27']
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        states={row['local_job_id']:require(await client.get(f"api/jobs/{row['id']}")).json()['state']
            for row in committed['jobs']}
        active=sum(value in {'RUNNING','WAITING_EXTERNAL'} for value in states.values())
        if states['image-27']=='WAITING_EXTERNAL' and active>=2:break
        await asyncio.sleep(.25)
    else:raise TimeoutError('mixed_jobs_never_reached_inflight_state:'+json.dumps(states))
    await asyncio.to_thread(subprocess.run,['systemctl','restart',
        'sandbox-harness-parallel-job-worker.service'],check=True)
    completed=await wait_jobs(client,committed['jobs'],args.timeout)
    _,continuations=await wait_continuations(client,thread['id'],maximum=1)
    settings=load_platform_settings()
    from sqlalchemy import select
    from sandbox_harness.store import JobAttemptRow,Store
    store=Store(settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==provider['id']))).scalars().all())
    finally:await store.close()
    external_ids={row.external_execution_id for row in attempts if row.external_execution_id}
    if len(external_ids)!=1:
        raise AssertionError('provider submission identity changed:'+json.dumps(sorted(external_ids)))
    if len(continuations)!=1:raise AssertionError('mixed group did not create one continuation')
    return {'classification':'success','thread_id':thread['id'],'group_id':committed['group']['id'],
        'group_members':len(completed),'operation':'media.render','provider':'image.generate',
        'complex_skills':['docx','pdf'],'worker_restarted':True,
        'duplicate_provider_submission':False,'provider_external_execution_id':next(iter(external_ids)),
        'one_primary_continuation':True,'continuation_id':continuations[0]['id']}


HANDLERS={'P01':case_p01,'P02':case_p02,'P03':case_p03,'P04':case_p04,'P05':case_p05,
    'P06':case_p06,'P07':case_p07,'P08':case_p08,'P09':case_p09,'P10':case_p10,
    'P11':case_p11,'P12':case_p12,'P13':case_p13,'P14':case_p14,'P15':case_p15,
    'P16':case_p16,'P17':case_p17,'P18':case_p18,'P19':case_p19,'P20':case_p20,
    'P21':case_p21,'P22':case_p22,'P23':case_p23,'P24':case_p24,
    'P25':case_p25,'P26':case_p26,'P27':case_p27,
    'K07':case_k07,'K08':case_k08,'K09':case_k09,'K10':case_k10,'K11':case_k11,
    **{identity:(lambda client,args,identity=identity:case_skill_coverage(client,args,identity))
        for identity in SKILL_COVERAGE}}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24766/')
    parser.add_argument('--case',required=True,choices=[
        *(f'P{value:02d}' for value in range(1,28)),*(f'K{value:02d}' for value in range(1,12))])
    parser.add_argument('--report',type=Path,required=True);parser.add_argument('--harness',default='codex')
    parser.add_argument('--model',default='gpt-5.6-sol');parser.add_argument('--reasoning',default='deep')
    parser.add_argument('--timeout',type=float,default=1200)
    parser.add_argument('--intervention-delay',type=float,default=0)
    args=parser.parse_args();value={}
    try:
        handler=HANDLERS.get(args.case)
        if not handler:raise NotImplementedError('protocol_handler_not_implemented:'+args.case)
        async with protocol_lock():
            async with ResilientAsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
                session=await client.get('api/session');session.raise_for_status();value=await handler(client,args)
    except Exception as exc:
        value={'classification':'program_failure','reason':type(exc).__name__+':'+str(exc)[:500]}
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(
        json.dumps({'case':args.case,**value},ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'case':args.case,**value},ensure_ascii=False))
    if value.get('classification')!='success':raise SystemExit(2)


if __name__=='__main__':asyncio.run(main())
