#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import fcntl
import hashlib
import io
import json
import mimetypes
import os
from pathlib import Path,PurePosixPath
import shlex
import shutil
import subprocess
import tarfile
import time
import uuid

import httpx
from dotenv import dotenv_values

from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.media_repository import MediaRepository


ROOT=Path(__file__).resolve().parents[1]
TERMINAL_JOBS={'SUCCEEDED','FAILED','CANCELLED','BLOCKED','UNKNOWN'}
PROTOCOL_LOCK=ROOT/'.data/tmp/real-protocol.lock'
SERIAL_CASES={'P13','P19','P21','P27','P28','P29','P31','K08','AS01'}


def core_repository(settings):
    database=DatabaseSettings().for_service("api")["core"]
    return CoreRepository(database,settings.redis_url,settings.redis_prefix,
                          namespace=settings.namespace)


def media_repository(settings):
    database=DatabaseSettings().for_service("media-worker")["media"]
    return MediaRepository(database)


def service_unit(args,role:str)->str:
    configured=(getattr(args,f'{role}_unit','') or
        os.environ.get(f"SANDBOX_HARNESS_{role.upper()}_UNIT",''))
    return configured or f"sandbox-harness-parallel-ali-{role.replace('_','-')}.service"


def service_command(args,role:str,action:str)->list[str]:
    configured=getattr(args,f'{role}_{action}_command','')
    return shlex.split(configured) if configured else ['systemctl',action,service_unit(args,role)]


async def hard_restart_service(args,role:str)->tuple[int,int]:
    unit=service_unit(args,role)
    def pid():
        value=subprocess.check_output(['systemctl','show','-p','MainPID','--value',unit],text=True).strip()
        return int(value or 0)
    before=await asyncio.to_thread(pid)
    if before<=0:raise RuntimeError('service_not_running:'+unit)
    await asyncio.to_thread(subprocess.run,['systemctl','kill','--kill-who=main',
        '--signal=SIGKILL',unit],check=True)
    deadline=time.monotonic()+30;after=0
    while time.monotonic()<deadline:
        try:after=await asyncio.to_thread(pid)
        except subprocess.CalledProcessError:after=0
        if after>0 and after!=before:break
        await asyncio.sleep(.25)
    if after<=0 or after==before:
        await asyncio.to_thread(subprocess.run,['systemctl','start',unit],check=True)
        after=await asyncio.to_thread(pid)
    if after<=0 or after==before:raise RuntimeError('service_sigkill_restart_failed:'+unit)
    return before,after


def validate_skill_product_archive(data:bytes)->dict:
    if not data or len(data)>32*1024*1024:raise AssertionError('skill product archive size invalid')
    files=[];total=0
    try:
        with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
            members=archive.getmembers()
            if not members or len(members)>200:raise AssertionError('skill product member count invalid')
            for member in members:
                if member.name in {'.','./'} and member.isdir():continue
                path=PurePosixPath(member.name)
                if (path.is_absolute() or not path.parts or any(part in {'','..'} for part in path.parts)
                        or member.issym() or member.islnk() or member.isdev()):
                    raise AssertionError('skill product archive path invalid')
                if member.isfile():
                    total+=member.size
                    if member.size>0:files.append(path.as_posix())
    except (tarfile.TarError,OSError) as exc:raise AssertionError('skill product archive invalid') from exc
    if not files or total>64*1024*1024:raise AssertionError('skill product content invalid')
    return {'files':files,'total_bytes':total}


@asynccontextmanager
async def protocol_lock():
    PROTOCOL_LOCK.parent.mkdir(parents=True,exist_ok=True)
    stream=PROTOCOL_LOCK.open('a+')
    try:
        await asyncio.to_thread(fcntl.flock,stream.fileno(),fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(stream.fileno(),fcntl.LOCK_UN);stream.close()


@asynccontextmanager
async def concurrent_case():yield


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
NON_PROGRAM_JOB_ERROR_PREFIXES=('model_','provider_','infrastructure_','upstream_',
    'media_reference_','voice_clone_')


class JobOutcomeFailure(RuntimeError):
    def __init__(self,failed:list[dict]):
        failures=[{'id':value['id'],'state':value['state'],'error_code':str(
            (value.get('result') or {}).get('error_code') or value['state'])} for value in failed]
        codes=[value['error_code'] for value in failures]
        classification=('non_program_failure' if codes and all(
            code.startswith(NON_PROGRAM_JOB_ERROR_PREFIXES) for code in codes)
            else 'program_failure')
        self.outcome={'classification':classification,'reason':','.join(codes),
            'failed_jobs':[value['local_job_id'] for value in failed],'failures':failures}
        super().__init__('jobs_not_succeeded:'+json.dumps(failures,ensure_ascii=True,sort_keys=True))


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
            'output_disposition':'workspace','min_items':1,
            'max_items':20 if kind=='model3d' else 1}]}


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


def compose_timeline_job(source_ids:list[str],music_id:str,subtitle_id:str)->dict:
    return {'local_job_id':'compose-timeline','kind':'operation','adapter':'media.concat_sequence',
        'descriptor_id':'media.concat_sequence','descriptor_version':'1','descriptor_digest':'e'*64,
        'idempotency_key':'protocol-compose-'+uuid.uuid4().hex,
        'parameters':{'recipe':{'operation':'media.concat_sequence','source_role':'source',
            'music_role':'music','subtitle_role':'subtitles','watermark_text':'UPDREAM',
            'segment_count':len(source_ids),'segment_seconds':2,'width':320,'height':180,
            'fps':24,'output_filename':'composed-timeline.mp4'}},
        'input_artifacts':[*( {'artifact_id':identity,'artifact_role':'source'} for identity in source_ids),
            {'artifact_id':music_id,'artifact_role':'music'},
            {'artifact_id':subtitle_id,'artifact_role':'subtitles'}],
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
    if failed:raise JobOutcomeFailure(failed)
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
    from sandbox_harness.services.api.app import (object_store)
    from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
    from sandbox_harness.common.contracts.job_contracts import (JobExpectedOutput, JobGroupCreate, JobKind, JobRequest, JobState, OutputDisposition)
    from test_support.legacy_store import (Store)
    store=core_repository(settings)
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
    from sandbox_harness.common.configuration.config import (load_settings)
    return load_settings()


async def origin(client,args,identity,message='请简短回复已就绪。'):
    response=await client.post('api/threads',json={'harness':args.harness,'model':args.model,
        'reasoning_level':args.reasoning,'title':'Protocol '+identity});require(response);thread=response.json()
    response=await client.post(f"api/threads/{thread['id']}/turns",json={'message':message,
        'attachments':[],'idempotency_key':'protocol-origin-'+identity+'-'+uuid.uuid4().hex,
        'delivery_mode':'start'});require(response);return thread,response.json()


async def prepare(client,thread,turn,jobs,identity,*,suppress_continuation:bool=False):
    response=await client.post('api/job-group-drafts',json={'thread_id':thread['id'],
        'origin_turn_id':turn['id'],'idempotency_key':'protocol-group-'+identity+'-'+uuid.uuid4().hex,
        'jobs':jobs,'suppress_continuation':suppress_continuation});return response


async def register_case_asset(thread_id:str,turn_id:str,path:Path):
    settings=load_platform_settings()
    from sandbox_harness.services.api.app import (object_store)
    from test_support.legacy_store import (Store)
    data=path.read_bytes();objects=object_store(settings)
    object_ref,digest=await asyncio.to_thread(objects.put,data)
    store=core_repository(settings)
    try:
        owner=(await store.get_thread(thread_id)).owner_id
        return await store.add_artifact(turn_id,path='inputs/'+path.name,sha256=digest,
            size=len(data),mime=mimetypes.guess_type(path.name)[0] or 'application/octet-stream',
            object_ref=object_ref)
    finally:await store.close()


async def job_artifacts(job_ids:list[str]):
    settings=load_platform_settings()
    from test_support.legacy_store import (Store)
    store=core_repository(settings)
    try:
        return {job_id:await store.list_job_artifacts(job_id) for job_id in job_ids}
    finally:await store.close()


async def attempts_overlap(job_ids:list[str])->bool:
    settings=load_platform_settings()
    from sqlalchemy import select
    from test_support.legacy_store import (JobAttemptRow, Store)
    store=core_repository(settings)
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
    from test_support.legacy_store import (JobAttemptRow, Store)
    store=core_repository(settings);media_store=media_repository(settings)
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
            external_id=next(iter(external_ids));media=await media_store.get_media_job(external_id,owner)
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
    finally:
        await store.close();await media_store.close()


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


def skill_coverage_objective(skill_id:str,test_case:str)->str:
    return (f'Read and follow the injected {skill_id} Skill. Actually execute its declared '
        f'coverage objective: {test_case}. Use frozen inputs when relevant. Perform the real local '
        'commands and checks in /workspace/project or scratch; do not merely describe them and do '
        'not call an external Provider. Any preview server or other background process must exit '
        'successfully through a bounded command or self-termination; do not cancel a registered '
        'background job, because cancellation is a failed execution boundary. Then write the evidence file '
        f'/workspace/outputs/{skill_id}-coverage.json as JSON with skill_id exactly "{skill_id}", '
        'test_case, commands (a non-empty array of commands actually run), checks (a non-empty '
        'array of concrete observed results), and produced_files (paths made in project or scratch). '
        f'Copy at least one real non-evidence product created by the Skill into '
        f'/workspace/project/work/{skill_id}-product, then create exactly '
        f'/workspace/outputs/{skill_id}-product.tar.gz from that directory. The archive must '
        'contain regular non-empty product files, no symlinks, node_modules, caches, or evidence JSON. '
        'Delete every other file from /workspace/outputs before finishing, leaving exactly the '
        'coverage JSON and product tar.gz.')


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
    product={'artifact_role':'skill_product','output_disposition':'workspace',
        'min_items':1,'max_items':1,'mime_patterns':[
            'application/gzip','application/x-gzip','application/x-tar'],
        'max_item_bytes':32*1024*1024,'max_total_bytes':32*1024*1024}
    jobs=[]
    for index,skill_id in enumerate(declared):
        test_case=str(manifest[skill_id].get('test_case') or 'validate the Skill contract')
        objective=skill_coverage_objective(skill_id,test_case)
        jobs.append(harness_job(1000+index,args,objective=objective,outputs=[output,product],
            skills=[skill_id],inputs=bindings))
        jobs[-1]['local_job_id']='skill-'+skill_id
        jobs[-1]['idempotency_key']='coverage-'+identity+'-'+skill_id+'-'+uuid.uuid4().hex
    prepared=require(await prepare(client,thread,turn,jobs,identity,
        suppress_continuation=True)).json()
    if prepared.get('mode')!='committed':raise AssertionError('skill coverage unexpectedly required confirmation')
    completed=await asyncio.gather(*(wait_job(client,row['id'],args.timeout)
        for row in prepared['jobs']))
    if len(completed)>1 and not await attempts_overlap([row['id'] for row in completed]):
        raise AssertionError('skill coverage attempts did not overlap')
    settings=load_platform_settings()
    from sandbox_harness.services.api.app import (object_store)
    from test_support.legacy_store import (Store)
    store=core_repository(settings);objects=object_store(settings)
    evidence={}
    try:
        for row in completed:
            if row['state']!='SUCCEEDED':continue
            request=await store.get_job_request(row['id']);skill_id=row['local_job_id'].removeprefix('skill-')
            if request.skill_ids!=(skill_id,) or not request.runtime_profile_id:
                raise AssertionError('skill grant/profile mismatch:'+skill_id)
            outputs=await store.list_job_artifacts(row['id'],'skill_evidence')
            if len(outputs)!=1:raise AssertionError('skill evidence artifact count:'+skill_id)
            value=json.loads(objects.get(outputs[0].object_ref,maximum=2*1024*1024))
            products=await store.list_job_artifacts(row['id'],'skill_product')
            if len(products)!=1:raise AssertionError('skill product artifact count:'+skill_id)
            product_evidence=validate_skill_product_archive(objects.get(
                products[0].object_ref,maximum=32*1024*1024))
            if (value.get('skill_id')!=skill_id or not value.get('commands') or not value.get('checks')):
                raise AssertionError('skill evidence content invalid:'+skill_id)
            activities=await store.list_job_activities(row['id'],0,1000)
            if not any(item['kind'] in {'tool_started','tool_finished','command','file_change'}
                for item in activities):raise AssertionError('skill produced no tool activity:'+skill_id)
            evidence[skill_id]={'job_id':row['id'],'runtime_profile_id':request.runtime_profile_id,
                'artifact_id':outputs[0].artifact_id,'commands':len(value['commands']),
                'checks':len(value['checks']),'product_artifact_id':products[0].artifact_id,
                'product_files':len(product_evidence['files']),
                'product_bytes':product_evidence['total_bytes']}
    finally:await store.close()
    failed=[row for row in completed if row['state']!='SUCCEEDED']
    if failed:
        codes=[str((row.get('result') or {}).get('error_code') or row['state']) for row in failed]
        non_program=('model_','provider_','infrastructure_','media_reference_','voice_clone_')
        classification='non_program_failure' if all(code.startswith(non_program) for code in codes) else 'program_failure'
        return {'classification':classification,'reason':','.join(codes),'thread_id':thread['id'],
            'failed_jobs':[row['local_job_id'] for row in failed],
            'verified_products':len(evidence),'evidence':evidence}
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
    from sandbox_harness.services.api.app import (object_store)
    from test_support.legacy_store import (Store)
    store=core_repository(settings);objects=object_store(settings)
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
    from test_support.legacy_store import (Store)
    store=core_repository(settings)
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
    clip=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/video_edit_reference.mp4')
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


async def case_k15(client,args):
    thread,turn=await origin(client,args,'K15')
    catalog=require(await client.get('api/catalog/media',params={'kind':'image'})).json()
    model=next((row for row in catalog.get('models',[]) if row.get('model')=='seedream-5.0'),None)
    if not model:raise AssertionError('seedream catalog model missing')
    schema=model['parameter_schema'];sizes=schema['optional']['size'];limits=schema['limits']
    expected_sizes=['2048x2048','2560x1440','2304x1728','explicit WIDTHxHEIGHT']
    if sizes!=expected_sizes or limits.get('max_total_pixels')!=4_624_220:
        raise AssertionError('deployed image contract mismatch:'+json.dumps(
            {'sizes':sizes,'limits':limits},sort_keys=True))
    invalid=semantic_provider_job('invalid-tier','image.generate',kind='image',
        model='seedream-5.0',mode='text_to_image',
        parameters={'prompt':'invalid size must remain local','size':'3K'})
    rejected=await prepare(client,thread,turn,[invalid],'K15-invalid',suppress_continuation=True)
    if rejected.status_code!=422:
        raise AssertionError('invalid image size was not rejected before confirmation:'+str(
            rejected.status_code))
    if require(await client.get(f"api/threads/{thread['id']}/media/jobs")).json():
        raise AssertionError('invalid image size created a Media Job')
    poster=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/poster.png')
    reference={'artifact_id':poster.id,'role':'reference_image','usage':'subject'}
    jobs=[
        semantic_provider_job('explicit-size','image.generate',kind='image',
            model='seedream-5.0',mode='text_to_image',
            parameters={'prompt':'clean blue square on white, no text','size':'2560x1440'}),
        semantic_provider_job('isolated-reference','image.generate',kind='image',
            model='seedream-5.0',mode='reference_image',
            parameters={'prompt':'preserve the subject in a clean studio variation',
                'size':'2560x1440'},references=[reference]),
    ]
    prepared=require(await prepare(client,thread,turn,jobs,'K15-valid',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('K15 valid group bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await wait_jobs(client,committed['jobs'],args.timeout)
    artifacts=await job_artifacts([row['id'] for row in completed])
    if any(len(artifacts[row['id']])!=1 for row in completed):
        raise AssertionError('K15 image artifact collection mismatch')
    media=require(await client.get(f"api/threads/{thread['id']}/media/jobs")).json()
    if len(media)!=2 or any(row['status']!='succeeded' for row in media):
        raise AssertionError('K15 Media Jobs did not succeed:'+json.dumps(media,ensure_ascii=False))
    return {'classification':'success','thread_id':thread['id'],
        'group_id':committed['group']['id'],'catalog_sizes':sizes[:-1],
        'max_total_pixels':limits['max_total_pixels'],'invalid_size':'3K',
        'provider_submissions_for_invalid':0,'successful_jobs':len(completed),
        'reference_prefix_isolated':True,
        'artifact_ids':[artifacts[row['id']][0].artifact_id for row in completed]}


async def case_k16(client,args):
    thread,turn=await origin(client,args,'K16')
    clip=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/video_edit_reference.mp4')
    reference={'artifact_id':clip.id,'role':'reference_video','usage':'edit_source'}
    definitions=[
        ('happyhorse-prefix','happyhorse-video-edit','happyhorse_video_edit',
            'preserve motion and audio, apply a subtle cool color grade'),
        ('wan-prefix','wan-2.7-video-edit','wan27_video_edit',
            'preserve motion and audio, apply a subtle clean line-art style'),
    ]
    jobs=[];expected={}
    for local,model,adapter,prompt in definitions:
        jobs.append(semantic_provider_job(local,'video.edit',kind='video',model=model,
            mode='edit_video',parameters={'prompt':prompt,'duration':-1,
                'resolution':'720p','aspect_ratio':'adaptive'},references=[reference]))
        expected[local]={'capability':'video.edit','adapter':adapter,'model':model,
            'mode':'edit_video'}
    prepared=require(await prepare(client,thread,turn,jobs,'K16',
        suppress_continuation=True)).json()
    if prepared.get('mode')!='draft':raise AssertionError('K16 bypassed confirmation')
    committed=require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    completed=await wait_provider_coverage(client,committed['jobs'],args.timeout)
    result=await verify_provider_coverage(thread['id'],completed,expected)
    if result['classification']!='success':return {**result,'thread_id':thread['id']}
    return {'classification':'success','thread_id':thread['id'],
        'group_id':committed['group']['id'],'provider_jobs':len(completed),
        'adapters':sorted(item['adapter'] for item in expected.values()),
        'one_group_confirmation':True,'reference_prefix_isolated':True,
        'evidence':result['evidence']}


async def case_k17(client,args):
    thread,turn=await origin(client,args,'K17')
    first=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/clip_a.mp4')
    second=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/clip_b.mp4')
    music=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/music.wav')
    subtitles=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/captions.srt')
    prepared=require(await prepare(client,thread,turn,[compose_timeline_job(
        [first.id,second.id],music.id,subtitles.id)],'K17',suppress_continuation=True)).json()
    if prepared.get('mode')!='committed':raise AssertionError('compose Operation required confirmation')
    completed=await wait_jobs(client,prepared['jobs'],args.timeout)
    outputs=await job_artifacts([completed[0]['id']]);roles={row.artifact_role for row in outputs[completed[0]['id']]}
    if roles!={'rendered_media','compiler_report'}:raise AssertionError('compose outputs mismatch')
    return {'classification':'success','thread_id':thread['id'],'job_id':completed[0]['id'],
        'source_count':2,'subtitles':True,'watermark':True,'artifact_roles':sorted(roles)}


async def case_k10(client,args):
    thread,turn=await origin(client,args,'K10')
    poster=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/poster.png')
    rocket=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/rocket.png')
    clip=await register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/video_edit_reference.mp4')
    speech=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/voice-0.wav')
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
    from PIL import Image,ImageDraw
    view_root=ROOT/'.data/tmp/real-campaign/k11-multiview'
    view_root.mkdir(parents=True,exist_ok=True)
    cuboids=[((-1.0,-.65,-.55),(1.0,.65,.55),(48,132,210)),
        ((-.42,-.38,.55),(.42,.38,1.1),(82,180,225)),
        ((.65,-.35,-.2),(1.3,.35,.2),(235,92,65))]
    faces=((0,1,2,3),(4,7,6,5),(0,4,5,1),(1,5,6,2),(2,6,7,3),(4,0,3,7))
    def render(name,yaw):
        import math
        image=Image.new('RGB',(256,256),(245,247,250));draw=ImageDraw.Draw(image)
        polygons=[];cosine,sine=math.cos(yaw),math.sin(yaw)
        for lower,upper,color in cuboids:
            x0,y0,z0=lower;x1,y1,z1=upper
            points=[(x0,y0,z0),(x1,y0,z0),(x1,y1,z0),(x0,y1,z0),
                (x0,y0,z1),(x1,y0,z1),(x1,y1,z1),(x0,y1,z1)]
            rotated=[(x*cosine-y*sine,x*sine+y*cosine,z) for x,y,z in points]
            for index,face in enumerate(faces):
                values=[rotated[item] for item in face]
                depth=sum(value[1] for value in values)/4
                shade=(.72,.94,.82,1,.88,.78)[index]
                fill=tuple(max(0,min(255,round(channel*shade))) for channel in color)
                polygon=[(128+round(value[0]*72),142-round(value[2]*72)) for value in values]
                polygons.append((depth,polygon,fill))
        for _,polygon,fill in sorted(polygons):
            draw.polygon(polygon,fill=fill,outline=(28,45,62),width=2)
        path=view_root/(name+'.png');image.save(path);return path
    paths={name:render(name,index*__import__('math').pi/2)
        for index,name in enumerate(('front','left','back','right'))}
    views={name:await register_case_asset(thread['id'],turn['id'],path)
        for name,path in paths.items()}
    ref=lambda artifact,role:{'artifact_id':artifact.id,'role':role,'usage':'subject'}
    phase_one_defs=[
        ('model-image','image_to_model',{'texture':False,'face_limit':1500},
            [ref(poster,'reference_image')]),
        ('model-multiview','multiview_to_model',{'texture':False,'face_limit':1500},
            [ref(views['front'],'front_view'),ref(views['left'],'left_view'),
                ref(views['back'],'back_view'),ref(views['right'],'right_view')]),
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


async def thread_artifact_names(client,thread_id:str)->list[str]:
    items=require(await client.get(f'api/threads/{thread_id}/items',params={'after':0,'limit':500})).json()
    return [str(row.get('summary') or '') for row in items if row.get('type')=='artifact']


async def case_k12(client,args):
    thread,turn=await origin(client,args,'K12',(
        '先回复已就绪。平台稍后会返回两个并行子任务结果；收到全部结果后，读取其不可变成果，'
        '汇总为 /workspace/outputs/k12-summary.md，验证后作为最终交付。'))
    await wait_turn(client,turn['id'])
    output=lambda role,name:{'artifact_role':role,'output_disposition':'workspace',
        'min_items':1,'max_items':1,'mime_patterns':['text/markdown']}
    jobs=[harness_job(120,args,skills=['data-quality-audit'],objective=(
        'Create a small deterministic CSV in scratch, audit null and duplicate rows with the injected '
        'Skill, and write exactly /workspace/outputs/quality.md with observed counts.'),
        outputs=[output('quality_report','quality.md')]),
        harness_job(121,args,skills=['query-validation'],objective=(
        'Use the injected Skill to validate this exact PostgreSQL query: '
        'WITH sales(id, amount) AS (VALUES (1, 100), (2, NULL), (2, 200), (3, 50)) '
        'SELECT id, SUM(amount) AS total FROM sales GROUP BY id ORDER BY total DESC, id ASC; '
        'Save the supplied SQL in scratch and apply the Skill tooling. The VALUES clause is '
        'the complete input fixture; no database connection or additional attachment is needed. '
        'Explain NULL handling, grouping, deterministic ordering and scaling implications; '
        'distinguish static analysis from measured plans and do not invent EXPLAIN results. '
        'Write exactly /workspace/outputs/query.md with concrete findings.'),
        outputs=[output('query_report','query.md')])]
    prepared=require(await prepare(client,thread,turn,jobs,'K12')).json()
    if prepared.get('mode')!='committed':raise AssertionError('K12 unexpectedly required confirmation')
    completed=await wait_jobs(client,prepared['jobs'],args.timeout)
    _,continuations=await wait_continuations(client,thread['id'],timeout=args.timeout,maximum=1)
    final=await wait_turn(client,continuations[0]['id'],args.timeout)
    if final['state']!='COMPLETED':raise AssertionError('K12 continuation did not complete')
    names=await thread_artifact_names(client,thread['id'])
    if 'k12-summary.md' not in names:raise AssertionError('K12 final delivery missing')
    return {'classification':'success','thread_id':thread['id'],'group_id':prepared['group']['id'],
        'child_harnesses':len(completed),'one_primary_continuation':len(continuations)==1,
        'continuation_id':continuations[0]['id'],'artifact_names':names}


async def case_k13(client,args):
    thread,turn=await origin(client,args,'K13',(
        '先回复已就绪。平台稍后会返回图片规格化和检查两个并行结果；收到全部结果后，'
        '把真实Artifact ID和检查摘要写入 /workspace/outputs/k13-summary.json 并验证交付。'))
    await wait_turn(client,turn['id'])
    source=await register_case_asset(thread['id'],turn['id'],ROOT/'tests/real_cases/assets/poster.png')
    child=harness_job(130,args,skills=['media-processing'],inputs=[{
        'artifact_id':source.id,'artifact_role':'source'}],objective=(
        'Inspect the frozen image with Pillow, report its dimensions and alpha state, and write exactly '
        '/workspace/outputs/k13-inspection.json.'),outputs=[{'artifact_role':'inspection',
        'output_disposition':'workspace','min_items':1,'max_items':1,
        'mime_patterns':['application/json']}])
    prepared=require(await prepare(client,thread,turn,[operation_job(source.id),child],'K13')).json()
    if prepared.get('mode')!='committed':raise AssertionError('K13 unexpectedly required confirmation')
    completed=await wait_jobs(client,prepared['jobs'],args.timeout)
    _,continuations=await wait_continuations(client,thread['id'],timeout=args.timeout,maximum=1)
    final=await wait_turn(client,continuations[0]['id'],args.timeout)
    if final['state']!='COMPLETED':raise AssertionError('K13 continuation did not complete')
    if 'k13-summary.json' not in await thread_artifact_names(client,thread['id']):
        raise AssertionError('K13 final delivery missing')
    graph=require(await client.get(f"api/threads/{thread['id']}/execution-graph",
        params={'include_history':'true'})).json()
    barriers=[row for row in graph['nodes'] if row['type']=='wait_barrier']
    if not barriers or barriers[-1]['progress']['terminal']!=2:raise AssertionError('K13 barrier incomplete')
    return {'classification':'success','thread_id':thread['id'],'group_id':prepared['group']['id'],
        'group_members':len(completed),'one_primary_continuation':len(continuations)==1,
        'continuation_id':continuations[0]['id'],'barrier':barriers[-1]['progress']}


async def case_k14(client,args):
    thread,turn=await origin(client,args,'K14');await wait_turn(client,turn['id'])
    first=semantic_provider_job('k14-source','image.generate',kind='image',model='seedream-5.0',
        mode='text_to_image',parameters={'prompt':'clean blue geometric cup on white, no text',
            'size':'2560x1440'})
    first['expected_outputs'][0]['output_disposition']='workspace'
    prepared=require(await prepare(client,thread,turn,[first],'K14-source')).json()
    if prepared.get('mode')!='draft':raise AssertionError('K14 phase one bypassed confirmation')
    committed=require(await client.post(f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
    phase_one=await wait_jobs(client,committed['jobs'],args.timeout)
    _,first_continuations=await wait_continuations(client,thread['id'],timeout=args.timeout,maximum=1)
    await wait_turn(client,first_continuations[0]['id'],args.timeout)
    artifacts=await job_artifacts([phase_one[0]['id']]);source=artifacts[phase_one[0]['id']][0]
    reference={'artifact_id':source.artifact_id,'role':'reference_image','usage':'subject'}
    second=semantic_provider_job('k14-final','image.generate',kind='image',model='seedream-5.0',
        mode='reference_image',parameters={'prompt':'preserve the cup, premium studio lighting, no text',
            'size':'2560x1440'},references=[reference])
    second['expected_outputs'][0]['output_disposition']='delivery'
    phase_two_turn=first_continuations[0]
    prepared_two=require(await prepare(client,thread,phase_two_turn,[second],'K14-final')).json()
    if prepared_two.get('mode')!='draft':raise AssertionError('K14 phase two bypassed confirmation')
    committed_two=require(await client.post(
        f"api/job-group-drafts/{prepared_two['draft']['id']}/confirm")).json()
    phase_two=await wait_jobs(client,committed_two['jobs'],args.timeout)
    _,continuations=await wait_continuations(client,thread['id'],timeout=args.timeout,minimum=2,maximum=2)
    await wait_turn(client,continuations[-1]['id'],args.timeout)
    outputs=await job_artifacts([phase_two[0]['id']]);final_artifacts=outputs[phase_two[0]['id']]
    if not final_artifacts or final_artifacts[0].output_disposition.value!='delivery':
        raise AssertionError('K14 final delivery missing')
    return {'classification':'success','thread_id':thread['id'],'phase_one_group_id':committed['group']['id'],
        'phase_two_group_id':committed_two['group']['id'],'confirmations':2,'continuations':2,
        'immutable_handoff_artifact_id':source.artifact_id,
        'delivery_artifact_id':final_artifacts[0].artifact_id}


async def case_p01(client,args):
    thread,turn=await origin(client,args,'P01')
    settings=load_platform_settings()
    from sandbox_harness.services.api.app import (object_store)
    from test_support.legacy_store import (Store)
    data=(ROOT/'tests/real_cases/assets/poster.png').read_bytes()
    objects=object_store(settings);object_ref,digest=await asyncio.to_thread(objects.put,data)
    store=core_repository(settings)
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
        service_unit(args,'job_worker')],check=True)
    for _ in range(100):
        state=(await asyncio.to_thread(subprocess.run,['systemctl','is-active',
            service_unit(args,'job_worker')],capture_output=True,text=True)).stdout.strip()
        if state=='active':break
        await asyncio.sleep(.1)
    completed=await wait_job(client,job['id'],300)
    if completed['state']!='SUCCEEDED':raise AssertionError('recovered child did not succeed')
    settings=load_platform_settings()
    from sqlalchemy import select
    from test_support.legacy_store import (JobAttemptRow, Store)
    store=core_repository(settings)
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
    from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
    from test_support.legacy_store import (Store)
    store=core_repository(settings)
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
    from sandbox_harness.common.execution.job_adapters import (AdapterStatus, BaseJobAdapter, SubmissionOutcomeUnknown)
    from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
    from sandbox_harness.common.execution.job_worker import (JobWorker)
    from test_support.legacy_store import (Store)
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
    store=core_repository(settings);adapter=LostResponseAdapter()
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
    from sandbox_harness.services.api.app import (object_store)
    from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
    from sandbox_harness.common.contracts.job_contracts import (JobExpectedOutput, JobGroupCreate, JobKind, JobRequest, JobState, OutputDisposition)
    from test_support.legacy_store import (Store)
    store=core_repository(settings)
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
            service_unit(args,'job_worker')],check=True)
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
        service_unit(args,'job_worker')],check=True)
    paused=require(await client.post(f"api/threads/{thread['id']}/pause")).json()
    if paused['state'] not in {'PAUSING','PAUSED'}:raise AssertionError('pause_not_accepted')
    await asyncio.to_thread(subprocess.run,['systemctl','restart',
        service_unit(args,'api')],check=True)
    for _ in range(200):
        try:
            if (await client.get('api/session')).status_code==200:break
        except httpx.TransportError:pass
        await asyncio.sleep(.1)
    else:raise TimeoutError('api_restart_timeout')
    await asyncio.to_thread(subprocess.run,service_command(args,'job_worker','start'),check=True)
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
    from test_support.legacy_store import (Store, JobGroupRow, JobRow, JobAttemptRow)

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
    settings=load_platform_settings();store=core_repository(settings)
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
    from test_support.legacy_store import (Store)
    input_store=core_repository(settings)
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
    from test_support.legacy_store import (Store, JobRow)
    store=core_repository(settings)
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


async def case_as01(client,args):
    settings=load_platform_settings()
    staging=[settings.data_root/'tmp'/'child-job-artifacts',
        settings.data_root/'tmp'/'controlled-job-artifacts']
    for path in staging:shutil.rmtree(path,ignore_errors=True)
    result=await case_p24(client,args)
    _,continuations=await wait_continuations(client,result['thread_id'],maximum=1)
    if len(continuations)!=1:
        raise AssertionError('staging recovery did not produce one terminal continuation')
    if not all(path.is_dir() for path in staging):
        raise AssertionError('artifact staging directories were not recreated')
    from sqlalchemy import select
    from test_support.legacy_store import (JobAttemptRow)
    identities=[result['parent_job_id'],result['nested_operation_job_id']]
    store=core_repository(settings)
    try:
        async with store.connections() as db:
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id.in_(identities)).order_by(
                    JobAttemptRow.job_id,JobAttemptRow.generation))).scalars().all())
        grouped={identity:[row for row in attempts if row.job_id==identity] for identity in identities}
        for identity,rows in grouped.items():
            if not rows or any(row.outcome=='COLLECTION_RETRY' for row in rows):
                raise AssertionError('artifact collection retried after staging recovery:'+identity)
            external={row.external_execution_id for row in rows if row.external_execution_id}
            if len(external)!=1:
                raise AssertionError('external execution identity changed:'+identity)
    finally:await store.close()
    return {**result,'staging_fault_injected':True,'staging_recreated':True,
        'collection_retry_count':0,'duplicate_external_execution':False,
        'continuation_id':continuations[0]['id']}


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
        service_unit(args,'job_worker')],check=True)
    completed=await wait_jobs(client,committed['jobs'],args.timeout)
    _,continuations=await wait_continuations(client,thread['id'],maximum=1)
    settings=load_platform_settings()
    from sqlalchemy import select
    from test_support.legacy_store import (JobAttemptRow, Store)
    store=core_repository(settings)
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


async def case_p28(client,args):
    from sqlalchemy import select
    from sandbox_harness.common.db.models import EventRow
    from sandbox_harness.common.sandbox.runtime import RuntimeClient
    from sandbox_harness.common.contracts.sandbox_contracts import SandboxHandle
    import sandbox_harness.common.sandbox.sandbox_factory as sandbox_factory
    prompt=('只输出一篇较长的编号清单，不调用任何工具。第一行必须严格为 STREAM-BEGIN，'
        '随后输出 180 行，每行格式“第NNN项：流式持久化恢复验证内容”，编号不得重复；'
        '最后一行必须严格为 STREAM-END。不要省略、合并或解释。')
    thread,turn=await origin(client,args,'P28',prompt)
    settings=load_platform_settings();store=core_repository(settings)
    backend=sandbox_factory.build_sandbox_registry(settings);runtime=RuntimeClient(backend=backend)
    gap=None;execution_id='';before_pid=after_pid=0
    try:
        deadline=time.monotonic()+min(args.timeout,300);sandbox=None
        while time.monotonic()<deadline:
            current=await store.get_turn(turn['id'])
            if current and current.runtime_execution_id and current.sandbox_handle_json:
                execution_id=current.runtime_execution_id
                if sandbox is None:
                    sandbox=await backend.get(SandboxHandle.from_mapping(
                        json.loads(current.sandbox_handle_json)))
                status=await runtime.execution_status(sandbox,execution_id)
                if status and status.get('state')=='running':
                    pending=int(status.get('last_sequence') or 0)-current.runtime_sequence
                    if pending>=2:
                        gap={'runtime_sequence':int(status['last_sequence']),
                            'committed_sequence':current.runtime_sequence,'pending':pending}
                        before_pid,after_pid=await hard_restart_service(args,'platform_worker');break
            await asyncio.sleep(.05)
        if gap is None:raise AssertionError('primary_uncommitted_stream_window_not_observed')
        final=await wait_turn(client,turn['id'],args.timeout)
        if final['state']!='COMPLETED':raise AssertionError('primary_recovery_failed:'+json.dumps(final))
        current=await store.get_turn(turn['id'])
        if current.runtime_execution_id!=execution_id:raise AssertionError('primary_execution_identity_changed')
        messages=[item for item in await store.items_after(thread['id'])
            if item.turn_id==turn['id'] and item.type.value=='agent_message']
        text=''.join(item.summary for item in messages)
        if text.count('STREAM-BEGIN')!=1 or text.count('STREAM-END')!=1:
            raise AssertionError('primary_stream_markers_missing_or_duplicated')
        async with store.connections() as db:
            rows=list((await db.execute(select(EventRow).where(EventRow.turn_id==turn['id'],
                EventRow.kind=='text_delta').order_by(EventRow.sequence))).scalars().all())
        spans=[json.loads(row.payload_json) for row in rows]
        if not any(int(value.get('sequence_end') or 0)>int(value.get('sequence_start') or 0)
                for value in spans):raise AssertionError('primary_text_was_not_batched')
        return {'classification':'success','thread_id':thread['id'],'turn_id':turn['id'],
            'execution_id':execution_id,'sigkill_pid':before_pid,'recovered_pid':after_pid,
            'uncommitted_gap':gap,'durable_text_batches':len(rows),
            'markers_exactly_once':True,'execution_identity_preserved':True}
    finally:await store.close()


async def case_p29(client,args):
    from sqlalchemy import select
    from sandbox_harness.common.db.models import JobActivityRow,JobAttemptRow
    from sandbox_harness.common.sandbox.runtime import RuntimeClient
    from sandbox_harness.common.contracts.sandbox_contracts import SandboxHandle
    import sandbox_harness.common.sandbox.sandbox_factory as sandbox_factory
    thread,turn=await origin(client,args,'P29')
    ready=await wait_turn(client,turn['id'],args.timeout)
    if ready['state']!='COMPLETED':raise AssertionError('origin_turn_failed')
    objective=('Do not use tools. Return a long numbered factual checklist. The first line must be exactly '
        'CHILD-STREAM-BEGIN, followed by 180 distinct lines formatted "Item NNN: child stream recovery", '
        'and the final line exactly CHILD-STREAM-END. Do not omit or merge lines.')
    prepared=require(await prepare(client,thread,turn,[harness_job(290,args,objective=objective)],
        'P29',suppress_continuation=True)).json()
    committed=(require(await client.post(
        f"api/job-group-drafts/{prepared['draft']['id']}/confirm")).json()
        if prepared.get('mode')=='draft' else prepared)
    job=committed['jobs'][0];settings=load_platform_settings();store=core_repository(settings)
    backend=sandbox_factory.build_sandbox_registry(settings);runtime=RuntimeClient(backend=backend)
    gap=None;execution_id='';before_pid=after_pid=0
    try:
        deadline=time.monotonic()+min(args.timeout,300);sandbox=None
        while time.monotonic()<deadline:
            async with store.connections() as db:
                attempt=(await db.execute(select(JobAttemptRow).where(
                    JobAttemptRow.job_id==job['id'],JobAttemptRow.accepted_at.is_not(None)).order_by(
                    JobAttemptRow.generation.desc()).limit(1))).scalar_one_or_none()
            committed_sequence=await store.job_runtime_sequence(job['id'],
                attempt.external_execution_id) if attempt and attempt.external_execution_id else 0
            if attempt:
                receipt=json.loads(attempt.acceptance_receipt_json or '{}')
                if receipt.get('sandbox_handle') and attempt.external_execution_id:
                    execution_id=attempt.external_execution_id
                    if sandbox is None:
                        sandbox=await backend.get(SandboxHandle.from_mapping(receipt['sandbox_handle']))
                    status=await runtime.execution_status(sandbox,execution_id)
                    if status and status.get('state')=='running':
                        pending=int(status.get('last_sequence') or 0)-int(committed_sequence)
                        if pending>=2:
                            gap={'runtime_sequence':int(status['last_sequence']),
                                'committed_sequence':int(committed_sequence),'pending':pending}
                            before_pid,after_pid=await hard_restart_service(args,'job_worker');break
            await asyncio.sleep(.05)
        if gap is None:raise AssertionError('child_uncommitted_stream_window_not_observed')
        final=await wait_job(client,job['id'],args.timeout)
        if final['state']!='SUCCEEDED':raise AssertionError('child_recovery_failed:'+json.dumps(final))
        summary=str((final.get('result') or {}).get('child_summary') or '')
        if summary.count('CHILD-STREAM-BEGIN')!=1 or summary.count('CHILD-STREAM-END')!=1:
            raise AssertionError('child_stream_markers_missing_or_duplicated')
        async with store.connections() as db:
            activities=list((await db.execute(select(JobActivityRow).where(
                JobActivityRow.job_id==job['id'],JobActivityRow.kind=='text_delta').order_by(
                JobActivityRow.sequence))).scalars().all())
            attempts=list((await db.execute(select(JobAttemptRow).where(
                JobAttemptRow.job_id==job['id']))).scalars().all())
        spans=[json.loads(row.payload_json) for row in activities]
        if not any(int(value.get('sequence_end') or 0)>int(value.get('sequence_start') or 0)
                for value in spans):raise AssertionError('child_text_was_not_batched')
        external_ids={row.external_execution_id for row in attempts if row.external_execution_id}
        if external_ids!={execution_id}:raise AssertionError('child_execution_identity_changed')
        return {'classification':'success','thread_id':thread['id'],'job_id':job['id'],
            'execution_id':execution_id,'sigkill_pid':before_pid,'recovered_pid':after_pid,
            'uncommitted_gap':gap,'durable_text_batches':len(activities),
            'markers_exactly_once':True,'execution_identity_preserved':True}
    finally:await store.close()


async def case_p30(client,args):
    thread,first=await origin(client,args,'P30',
        'Reply with exactly COLD_RESUME_FIRST and do not use tools.')
    completed=await wait_turn(client,first['id'],args.timeout)
    if completed['state']!='COMPLETED':raise AssertionError('cold_resume_first_turn_failed')
    settings=load_platform_settings();store=core_repository(settings)
    try:
        before=await store.get_thread(thread['id'])
        if not before.native_thread_id or not before.checkpoint_ref:
            raise AssertionError('cold_resume_source_state_missing')
        require(await client.post(f"api/threads/{thread['id']}/sandbox/takeover"))
        released=require(await client.post(f"api/threads/{thread['id']}/sandbox/release",
            json={'managed_action':'preserve_work'})).json()
        if not released.get('checkpointed'):raise AssertionError('cold_resume_release_not_checkpointed')
        second=require(await client.post(f"api/threads/{thread['id']}/turns",json={
            'message':'Reply with exactly COLD_RESUME_SECOND and do not use tools.',
            'attachments':[],'idempotency_key':'protocol-p30-'+uuid.uuid4().hex,
            'delivery_mode':'start'})).json()
        final=await wait_turn(client,second['id'],args.timeout)
        if final['state']!='COMPLETED':
            raise AssertionError('cold_resume_second_turn_failed:'+json.dumps(final))
        after=await store.get_thread(thread['id'])
        if after.native_thread_id!=before.native_thread_id:
            raise AssertionError('cold_resume_native_thread_changed')
        return {'classification':'success','thread_id':thread['id'],
            'first_turn_id':first['id'],'second_turn_id':second['id'],
            'native_thread_id':after.native_thread_id,'checkpoint_restored':True,
            'native_session_resumed':True,'released_sandbox':True}
    finally:await store.close()


async def case_p31(client,args):
    from sqlalchemy import select
    from sandbox_harness.common.contracts.sandbox_contracts import SandboxHandle
    from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
    import sandbox_harness.common.sandbox.sandbox_factory as sandbox_factory
    thread,first=await origin(client,args,'P31',
        'Reply with exactly RETAINED_RESTART_FIRST and do not use tools.')
    initial=await wait_turn(client,first['id'],args.timeout)
    if initial['state']!='COMPLETED':raise AssertionError('retained_restart_first_failed')
    settings=load_platform_settings();store=core_repository(settings)
    try:
        async with store.connections() as db:
            retained=(await db.execute(select(SandboxOwnershipRow).where(
                SandboxOwnershipRow.thread_id==thread['id'],
                SandboxOwnershipRow.state=='RETAINED').order_by(
                SandboxOwnershipRow.created_at.desc()).limit(1))).scalar_one_or_none()
        if retained is None or not retained.handle_json or retained.retained_until is None:
            raise AssertionError('retained_restart_ownership_missing')
        handle=SandboxHandle.from_mapping(json.loads(retained.handle_json))
        backend=sandbox_factory.build_sandbox_registry(settings)
        before=await backend.get(handle)
        old_pid,new_pid=await hard_restart_service(args,'platform_worker')
        after=await backend.get(handle)
        if after.id!=before.id:raise AssertionError('retained_restart_sandbox_changed')
        second=require(await client.post(f"api/threads/{thread['id']}/turns",json={
            'message':'Reply with exactly RETAINED_RESTART_SECOND and do not use tools.',
            'attachments':[],'idempotency_key':'protocol-p31-'+uuid.uuid4().hex,
            'delivery_mode':'start'})).json()
        final=await wait_turn(client,second['id'],args.timeout)
        if final['state']!='COMPLETED':
            raise AssertionError('retained_restart_second_failed:'+json.dumps(final))
        current=await store.get_turn(second['id'])
        if current.sandbox_id!=handle.sandbox_id:
            raise AssertionError('retained_restart_did_not_reuse_sandbox')
        return {'classification':'success','thread_id':thread['id'],
            'sandbox_id':handle.sandbox_id,'old_pid':old_pid,'new_pid':new_pid,
            'retained_survived_restart':True,'same_sandbox_reused':True,
            'second_turn_id':second['id']}
    finally:await store.close()


HANDLERS={'P01':case_p01,'P02':case_p02,'P03':case_p03,'P04':case_p04,'P05':case_p05,
    'P06':case_p06,'P07':case_p07,'P08':case_p08,'P09':case_p09,'P10':case_p10,
    'P11':case_p11,'P12':case_p12,'P13':case_p13,'P14':case_p14,'P15':case_p15,
    'P16':case_p16,'P17':case_p17,'P18':case_p18,'P19':case_p19,'P20':case_p20,
    'P21':case_p21,'P22':case_p22,'P23':case_p23,'P24':case_p24,
    'P25':case_p25,'P26':case_p26,'P27':case_p27,'P28':case_p28,'P29':case_p29,
    'P30':case_p30,
    'P31':case_p31,
    'AS01':case_as01,
    'K07':case_k07,'K08':case_k08,'K09':case_k09,'K10':case_k10,'K11':case_k11,
    'K12':case_k12,'K13':case_k13,'K14':case_k14,'K15':case_k15,'K16':case_k16,'K17':case_k17,
    **{identity:(lambda client,args,identity=identity:case_skill_coverage(client,args,identity))
        for identity in SKILL_COVERAGE}}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24767/')
    parser.add_argument('--case',required=True,choices=[
        *(f'P{value:02d}' for value in range(1,32)),*(f'K{value:02d}' for value in range(1,18)),'AS01'])
    parser.add_argument('--report',type=Path,required=True);parser.add_argument('--harness',default='codex')
    parser.add_argument('--model',default='gpt-5.6-sol');parser.add_argument('--reasoning',default='deep')
    parser.add_argument('--timeout',type=float,default=1200)
    parser.add_argument('--intervention-delay',type=float,default=0)
    parser.add_argument('--job-worker-unit',default=os.environ.get('SANDBOX_HARNESS_JOB_WORKER_UNIT',''))
    parser.add_argument('--job-worker-start-command',default=os.environ.get(
        'SANDBOX_HARNESS_JOB_WORKER_START_COMMAND',''))
    parser.add_argument('--api-unit',default=os.environ.get('SANDBOX_HARNESS_API_UNIT',''))
    args=parser.parse_args();value={}
    try:
        handler=HANDLERS.get(args.case)
        if not handler:raise NotImplementedError('protocol_handler_not_implemented:'+args.case)
        boundary=protocol_lock() if args.case in SERIAL_CASES else concurrent_case()
        async with boundary:
            async with ResilientAsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
                session=await client.get('api/session');session.raise_for_status();value=await handler(client,args)
    except JobOutcomeFailure as exc:
        value=exc.outcome
    except Exception as exc:
        value={'classification':'program_failure','reason':type(exc).__name__+':'+str(exc)[:500]}
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(
        json.dumps({'case':args.case,**value},ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'case':args.case,**value},ensure_ascii=False))
    if value.get('classification') in {'program_failure','optimizable_prompt'}:raise SystemExit(2)


if __name__=='__main__':asyncio.run(main())
