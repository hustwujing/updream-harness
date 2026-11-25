#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

import httpx


ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
import run_real_parallel_protocol as parallel
import run_real_campaign as campaign


SOURCE_CASES={value['id']:value for value in json.loads(
    (ROOT/'tests/real_cases/cases.json').read_text())['cases'] if value['id'].startswith('D')}


def decorate(identity:str,boundary:str,value:dict)->dict:
    return {**value,'source_case_id':identity,
        'source_project':'sandbox-harness-platform-plus','parallel_boundary':boundary}


def business_artifact_prompt(case:dict)->str:
    external=(' 本阶段如需外部计费调用，请直接创建结构化 JobGroup/媒体授权草案；测试用户会在'
        '决策卡确认。不要使用普通聊天文本重复询问授权，也不要在授权草案创建前结束 Turn。'
        if case.get('allowed_external_operations') else '')
    contract=json.dumps(case.get('expect',{}),ensure_ascii=False,sort_keys=True,
        separators=(',',':'))
    return (case['prompt']+'\n\n'
        '【当前阶段边界】当前系统使用持续 Primary Harness 和 Durable Job，而不是旧 '
        'Manager/WorkUnit 名称。上文中的人工确认、用户介入、暂停恢复、服务重启、故障注入、'
        '旧组件拆分、依赖关系、delivery 标志、审查策略、输入屏障和协议时序，已在同一用例的独立'
        '协议阶段完成验证；本阶段不要再次触发、等待或模拟这些流程，也不要创建子 Harness、审查'
        '任务或用户确认。只在当前 Primary Harness 中直接完成上文要求的最终业务产物，严格保留'
        '最终文件的格式、尺寸、时长、数量及内容规格；上文与这些控制流程有关的指令在本阶段忽略。'
        +external+' 最终产物必须满足以下机器可读契约：'+contract+'。')


def adapt(identity:str,boundary:str,handler):
    async def run(client,args):
        async with parallel.protocol_lock():protocol=await handler(client,args)
        if protocol.get('classification')!='success':return decorate(identity,boundary,protocol)
        case=SOURCE_CASES[identity]
        if case.get('expect',{}).get('kind')=='protocol':
            return decorate(identity,boundary,protocol)
        business={**case,'dynamic_hook':None,'prompt':business_artifact_prompt(case)}
        running={identity:{'state':'STARTING','expected':case.get('expect',{})}}
        artifact=await campaign.primary_case(client,business,args,running)
        if artifact.get('classification')!='success':
            return decorate(identity,boundary,{**artifact,'protocol_evidence':protocol})
        return decorate(identity,boundary,{**artifact,'protocol_evidence':protocol,
            'business_artifact_verified':True})
    run.__name__='case_'+identity.lower()
    return run


async def usage_allow(client,args):
    thread,turn=await parallel.origin(client,args,'D50')
    response=await parallel.prepare(client,thread,turn,[parallel.provider_job(index) for index in range(8)],'D50')
    parallel.require(response);draft=response.json()
    if draft['mode']!='draft':raise AssertionError('billable batch bypassed confirmation')
    confirmed=parallel.require(await client.post(
        f"api/job-group-drafts/{draft['draft']['id']}/confirm")).json()
    if len(confirmed['jobs'])!=8:raise AssertionError('confirmed batch count is not eight')
    parallel.require(await client.post(f"api/job-groups/{confirmed['group']['id']}/cancel"))
    return {'classification':'success','draft_id':draft['draft']['id'],
        'group_id':confirmed['group']['id'],'authorized_submissions':8,'maximum_calls':8}


async def usage_reject(client,args):
    thread,turn=await parallel.origin(client,args,'D51')
    response=await parallel.prepare(client,thread,turn,[parallel.provider_job(index) for index in range(8)],'D51')
    parallel.require(response);draft=response.json()
    rejected=parallel.require(await client.post(
        f"api/job-group-drafts/{draft['draft']['id']}/reject")).json()
    drafts=parallel.require(await client.get(
        f"api/threads/{thread['id']}/job-group-drafts")).json()
    durable=next((value for value in drafts if value['id']==draft['draft']['id']),None)
    if rejected['state']!='REJECTED' or not durable or durable['state']!='REJECTED':
        raise AssertionError('rejected usage was not durably rejected')
    return {'classification':'success','draft_id':draft['draft']['id'],
        'maximum_calls':8,'provider_submissions':0,'rejected':True}


# Each entry is explicit so a missing inherited risk fails collection. Dedicated implementations
# replace these boundary adapters group by group; the referenced protocol always performs real API,
# database, Worker, Sandbox or Provider work and never returns a synthetic pass.
MAPPINGS={
    'D01':('staged_job_continuation',parallel.case_p05),
    'D02':('parallel_join_continuation',parallel.case_p20),
    'D03':('atomic_artifact_publication',parallel.case_p19),
    'D04':('continuation_idempotency',parallel.case_p05),
    'D05':('postgres_group_race',parallel.case_p16),
    'D06':('child_worker_takeover',parallel.case_p13),
    'D07':('thread_stop',parallel.case_p10),
    'D08':('durable_pause_resume',parallel.case_p21),
    'D09':('whole_group_authorization',parallel.case_p02),
    'D10':('authorization_rejection',parallel.case_p03),
    'D11':('required_review_gate',parallel.case_p15),
    'D12':('owner_scope_preflight',parallel.case_p12),
    'D13':('budget_race',parallel.case_p16),
    'D14':('atomic_artifact_publication',parallel.case_p19),
    'D15':('frozen_profile_preflight',parallel.case_p12),
    'D16':('provider_uncertain_submission',parallel.case_p17),
    'D17':('mixed_group_observability',parallel.case_p01),
    'D18':('checkpoint_restore',parallel.case_p18),
    'D19':('deployment_namespace_isolation',parallel.case_p18),
    'D20':('minimal_skill_profile',parallel.case_p11),
    'D21':('immutable_replacement',parallel.case_p07),
    'D22':('mixed_group_single_identity',parallel.case_p01),
    'D23':('thread_dispatch_fence',parallel.case_p10),
    'D24':('ordered_user_input',parallel.case_p06),
    'D25':('ordered_user_input',parallel.case_p06),
    'D26':('provider_handle_reuse',parallel.case_p17),
    'D27':('provider_unknown_input_fence',parallel.case_p17),
    'D28':('required_review_gate',parallel.case_p15),
    'D29':('required_review_gate',parallel.case_p15),
    'D30':('atomic_artifact_publication',parallel.case_p19),
    'D31':('mixed_parallel_timing',parallel.case_p20),
    'D32':('atomic_artifact_publication',parallel.case_p19),
    'D33':('successor_input_ordering',parallel.case_p06),
    'D34':('provider_handle_reuse',parallel.case_p17),
    'D35':('frozen_provider_route',parallel.case_p17),
    'D36':('immutable_replacement',parallel.case_p07),
    'D37':('immutable_replacement',parallel.case_p07),
    'D38':('provider_handle_reuse',parallel.case_p17),
    'D39':('immutable_replacement',parallel.case_p07),
    'D40':('mixed_operation_lineage',parallel.case_p01),
    'D41':('atomic_artifact_publication',parallel.case_p19),
    'D42':('primary_checkpoint_restore',parallel.case_p18),
    'D43':('reviewer_worker_takeover',parallel.case_p13),
    'D44':('child_worker_takeover',parallel.case_p13),
    'D45':('repeated_child_takeover',parallel.case_p13),
    'D46':('provider_handle_reuse',parallel.case_p17),
    'D47':('multi_leaf_delivery',parallel.case_p19),
    'D48':('attachment_preflight',parallel.case_p12),
    'D49':('successor_input_ordering',parallel.case_p06),
    'D50':('usage_authorization_allow',usage_allow),
    'D51':('usage_authorization_reject',usage_reject),
}
HANDLERS={identity:adapt(identity,boundary,handler)
    for identity,(boundary,handler) in MAPPINGS.items()}


async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--base-url',default='http://127.0.0.1:24766/')
    parser.add_argument('--case',required=True,choices=[f'D{value:02d}' for value in range(1,52)])
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--harness',default='claude');parser.add_argument('--model',default='claude-opus-5-5')
    parser.add_argument('--reasoning',default='deep');parser.add_argument('--timeout',type=int,default=7200)
    parser.add_argument('--intervention-delay',type=int,default=10)
    parser.add_argument('--job-worker-unit',default=(
        os.environ.get('SANDBOX_HARNESS_JOB_WORKER_UNIT') or
        'sandbox-harness-parallel-job-worker.service'))
    parser.add_argument('--job-worker-start-command',default=(
        os.environ.get('SANDBOX_HARNESS_JOB_WORKER_START_COMMAND') or ''))
    parser.add_argument('--api-unit',default=(os.environ.get('SANDBOX_HARNESS_API_UNIT') or
        'sandbox-harness-parallel-api.service'));args=parser.parse_args();value={}
    configured_root=Path(json.loads((ROOT/'tests/real_cases/cases.json').read_text()).get(
        'asset_root') or ROOT/'tests/real_cases/assets')
    args.asset_root=(configured_root if configured_root.is_absolute()
        else (ROOT/'tests/real_cases'/configured_root).resolve())
    try:
        async with parallel.ResilientAsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
            parallel.require(await client.get('api/session'))
            value=await HANDLERS[args.case](client,args)
    except Exception as exc:
        value={'classification':'program_failure','reason':type(exc).__name__+':'+str(exc)[:1000],
            'source_case_id':args.case,'source_project':'sandbox-harness-platform-plus',
            'parallel_boundary':MAPPINGS[args.case][0]}
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps({'case':args.case,**value},ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'case':args.case,**value},ensure_ascii=False))
    if value.get('classification')!='success':raise SystemExit(2)


if __name__=='__main__':asyncio.run(main())
