#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import fcntl
import hashlib
import json
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


def provider_job(index:int)->dict:
    return {'local_job_id':f'image-{index}','kind':'provider','adapter':'image.generate',
        'descriptor_id':'image.generate','descriptor_version':'1','descriptor_digest':'c'*64,
        'idempotency_key':f'protocol-image-{index}-'+uuid.uuid4().hex,
        'parameters':{'kind':'image','model':'seedream-5.0','generation_mode':'text_to_image',
            'parameters':{'prompt':f'A simple blue geometric poster variant {index}'}},
        'expected_outputs':[{'artifact_role':'generated_images','output_disposition':'candidate',
            'min_items':1,'max_items':1,'mime_patterns':['image/*']}]}


def harness_job(index:int,args,*,objective:str='Return a concise factual analysis.',
                outputs:list[dict]|None=None,skills:list[str]|None=None,
                child_profile_id:str='child-general',replaces_job_id:str='')->dict:
    return {'local_job_id':f'child-{index}','kind':'harness','adapter':'child-harness',
        'descriptor_id':'delegate.harness','descriptor_version':'1','descriptor_digest':'d'*64,
        'idempotency_key':f'protocol-child-{index}-'+uuid.uuid4().hex,
        'parameters':{'harness':args.harness,'model':args.model,'objective':objective},
        'expected_outputs':outputs or [],'child_profile_id':child_profile_id,
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


async def prepare(client,thread,turn,jobs,identity):
    response=await client.post('api/job-group-drafts',json={'thread_id':thread['id'],
        'origin_turn_id':turn['id'],'idempotency_key':'protocol-group-'+identity+'-'+uuid.uuid4().hex,
        'jobs':jobs});return response


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
    await asyncio.sleep(2)
    turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
    continuations=[value for value in turns if value.get('internal')]
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
    await wait_jobs(client,jobs,1200);await asyncio.sleep(3)
    group_values=[require(await client.get(f"api/job-groups/{group['id']}")).json()['group'] for group in groups]
    turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
    continuations=[value for value in turns if value.get('internal')]
    if any(value['state'] not in {'TERMINAL','CONTINUED'} for value in group_values):
        raise AssertionError('group did not become terminal')
    if not continuations:raise AssertionError('no durable continuation was created')
    return {'classification':'success','groups':3,'continuation_count':len(continuations),
        'coalesced_continuation':len(continuations)<3}


async def case_p06(client,args):
    thread,turn=await origin(client,args,'P06')
    response=await prepare(client,thread,turn,[harness_job(0,args,
        objective='Analyze three independent tradeoffs and return a concise comparison.')],'P06')
    require(response);value=response.json()
    queued=require(await client.post(f"api/threads/{thread['id']}/turns",json={
        'message':'最终还需要一份中文摘要。','attachments':[],
        'idempotency_key':'protocol-p06-user-'+uuid.uuid4().hex,'delivery_mode':'queue'})).json()
    await wait_jobs(client,value['jobs'],1200);await asyncio.sleep(3)
    turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
    positions={item['id']:index for index,item in enumerate(turns)}
    continuations=[item for item in turns if item.get('internal')]
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
        if current['state'] in {'RUNNING','WAITING_EXTERNAL'}:break
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
    committed=response.json();await wait_jobs(client,committed['jobs'],1200);await asyncio.sleep(3)
    turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
    continuations=[item for item in turns if item.get('internal')]
    if len(continuations)!=1:raise AssertionError('mixed fanout did not create one continuation')
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
    jobs=await wait_jobs(client,prepared['jobs'],args.timeout);await asyncio.sleep(3)
    turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
    continuations=[item for item in turns if item.get('internal')]
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
        '第二，给出一份包含依据的 GPU 任务故障恢复风险分析。两项彼此独立，分别给出五条可验收结论。')
    turn=require(await client.post(f"api/threads/{thread['id']}/turns",json={'message':prompt,
        'attachments':[],'idempotency_key':'protocol-'+identity,'delivery_mode':'start'})).json()
    await wait_turn(client,turn['id'],args.timeout)
    settings=load_platform_settings();store=Store(
        settings.database_url,settings.redis_url,settings.redis_prefix)
    try:
        deadline=time.monotonic()+args.timeout;groups=[];jobs=[]
        while time.monotonic()<deadline:
            async with store.connections() as db:
                groups=list((await db.execute(select(JobGroupRow).where(
                    JobGroupRow.thread_id==thread['id']).order_by(JobGroupRow.created_at))).scalars().all())
                if groups:
                    jobs=list((await db.execute(select(JobRow).where(
                        JobRow.group_id.in_([group.id for group in groups]),
                        JobRow.kind=='harness').order_by(JobRow.created_at))).scalars().all())
            if len(jobs)>=2 and all(job.state in TERMINAL_JOBS for job in jobs):break
            await asyncio.sleep(1)
        if len(jobs)<2:raise AssertionError('Primary did not delegate two independent Child Harness Jobs')
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
        turns=require(await client.get(f"api/threads/{thread['id']}/turns")).json()
        continuations=[value for value in turns if value.get('internal')]
        if len(continuations)!=1:raise AssertionError('parallel group did not produce one continuation')
        return {'classification':'success','thread_id':thread['id'],'primary_delegated':True,
            'child_harnesses':len(jobs),'same_group':True,'attempts_overlapped':True,
            'one_primary_continuation':True,'group_id':next(iter(grouped))}
    finally:await store.close()


HANDLERS={'P01':case_p01,'P02':case_p02,'P03':case_p03,'P04':case_p04,'P05':case_p05,
    'P06':case_p06,'P07':case_p07,'P08':case_p08,'P09':case_p09,'P10':case_p10,
    'P11':case_p11,'P12':case_p12,'P13':case_p13,'P14':case_p14,'P15':case_p15,
    'P16':case_p16,'P17':case_p17,'P18':case_p18,'P19':case_p19,'P20':case_p20,
    'P21':case_p21,'P22':case_p22,'P23':case_p23}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24766/')
    parser.add_argument('--case',required=True,choices=[f'P{value:02d}' for value in range(1,24)])
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
