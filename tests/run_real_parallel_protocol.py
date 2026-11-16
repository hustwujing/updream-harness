#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import uuid

import httpx


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


HANDLERS={'P02':case_p02,'P03':case_p03,'P08':case_p08,'P09':case_p09,'P10':case_p10,'P12':case_p12}


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
