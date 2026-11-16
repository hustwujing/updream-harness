#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import time
import uuid

import httpx
from dotenv import dotenv_values


ROOT=Path(__file__).resolve().parents[1]
TERMINAL_JOBS={'SUCCEEDED','FAILED','CANCELLED','BLOCKED','UNKNOWN'}


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


def load_platform_settings():
    for key,value in dotenv_values(ROOT/'.data/config/platform.env').items():
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
    await client.post(f"api/job-groups/{value['group']['id']}/cancel")
    return {'classification':'success','cancelled_job_id':job['id'],
        'state':response.json()['state'],'independent_members':2}


async def case_p09(client,args):
    thread,turn,value=await confirmed_provider_group(client,args,'P09',3)
    response=await client.post(f"api/job-groups/{value['group']['id']}/cancel");response.raise_for_status()
    if len(response.json()['jobs'])!=3:raise AssertionError('group cancellation omitted members')
    return {'classification':'success','group_id':value['group']['id'],'cancelled_members':3}


async def case_p10(client,args):
    thread,turn,value=await confirmed_provider_group(client,args,'P10',3)
    response=await client.post(f"api/threads/{thread['id']}/stop-all");response.raise_for_status()
    if len(response.json()['jobs'])<3:raise AssertionError('thread stop-all omitted background Jobs')
    return {'classification':'success','thread_id':thread['id'],'stop_all':True,
        'cancelled_members':len(response.json()['jobs'])}


async def case_p12(client,args):
    thread,turn=await origin(client,args,'P12');job={'local_job_id':'bad-profile','kind':'harness',
        'adapter':'child-harness','descriptor_id':'delegate.harness','descriptor_version':'1',
        'descriptor_digest':'d'*64,'idempotency_key':'bad-profile-'+uuid.uuid4().hex,
        'parameters':{'harness':'codex','model':args.model,'objective':'reply'},
        'child_profile_id':'not-registered','skill_ids':[]}
    response=await prepare(client,thread,turn,[job],'P12')
    if response.status_code!=422:raise AssertionError('unknown Child Profile was not rejected in preflight')
    return {'classification':'success','preflight_rejected':True,'status_code':422}


HANDLERS={'P01':case_p01,'P02':case_p02,'P03':case_p03,'P04':case_p04,'P05':case_p05,
    'P06':case_p06,'P07':case_p07,'P08':case_p08,'P09':case_p09,'P10':case_p10,
    'P11':case_p11,'P12':case_p12}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24766/')
    parser.add_argument('--case',required=True,choices=[f'P{value:02d}' for value in range(1,21)])
    parser.add_argument('--report',type=Path,required=True);parser.add_argument('--harness',default='codex')
    parser.add_argument('--model',default='gpt-5.6-sol');parser.add_argument('--reasoning',default='deep')
    args=parser.parse_args();value={}
    try:
        handler=HANDLERS.get(args.case)
        if not handler:raise NotImplementedError('protocol_handler_not_implemented:'+args.case)
        async with httpx.AsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
            session=await client.get('api/session');session.raise_for_status();value=await handler(client,args)
    except Exception as exc:
        value={'classification':'program_failure','reason':type(exc).__name__+':'+str(exc)[:500]}
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(
        json.dumps({'case':args.case,**value},ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'case':args.case,**value},ensure_ascii=False))
    if value.get('classification')!='success':raise SystemExit(2)


if __name__=='__main__':asyncio.run(main())
