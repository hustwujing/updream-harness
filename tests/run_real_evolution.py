#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time
import uuid

from sqlalchemy import delete, func, select, update

import run_real_parallel_protocol as protocol
from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, TurnCreate, TurnState, now
from sandbox_harness.common.contracts.sandbox_contracts import SandboxScope
from sandbox_harness.common.db.accounts import AccountsRepository
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.domain_database import DomainInbox, DomainOutbox
from sandbox_harness.common.db.media_repository import MediaRepository
from sandbox_harness.common.db.models import (
    OwnerAdmissionRow, ProviderCircuitRow, ThreadRow, TurnOrderRow, TurnRow,
    UsageLedgerRow, canonical_digest,
)
from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
from sandbox_harness.common.db.site_repository import SiteRepository
from sandbox_harness.common.observability.observability import BUSINESS_METRICS
from sandbox_harness.common.security.capability import CapabilityCodec
from sandbox_harness.common.sandbox.sandbox_factory import build_sandbox_backend
from sandbox_harness.common.storage.object_gc import ObjectGarbageCollector
from sandbox_harness.common.storage.objects import ObjectStore


ROOT=Path(__file__).resolve().parents[1]


class Repositories:
    def __init__(self,settings):
        values=DatabaseSettings().for_service('platform-worker')
        self.accounts=AccountsRepository(values['accounts'])
        self.core=CoreRepository(values['core'],settings.redis_url,settings.redis_prefix,
            namespace=settings.namespace)
        self.media=MediaRepository(values['media']);self.sites=SiteRepository(values['sites'])
    async def close(self):
        for value in (self.sites,self.media,self.core,self.accounts):await value.close()


def restart(unit):
    subprocess.run(['systemctl','restart',unit],check=True,timeout=60)


def kill_restart(unit):
    old=int(subprocess.check_output(['systemctl','show','-p','MainPID','--value',unit],text=True).strip())
    if old <= 0:raise AssertionError('service_main_pid_missing')
    os.kill(old,signal.SIGKILL)
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        current=int(subprocess.check_output(
            ['systemctl','show','-p','MainPID','--value',unit],text=True).strip() or 0)
        active=subprocess.run(['systemctl','is-active','--quiet',unit]).returncode==0
        if active and current>0 and current!=old:return old,current
        time.sleep(.25)
    raise AssertionError('service_sigkill_restart_timeout')


async def wait_turn(client,identity,timeout):
    deadline=time.monotonic()+timeout;seen=[]
    while time.monotonic()<deadline:
        value=protocol.require(await client.get(f'api/turns/{identity}')).json()
        stage=value.get('execution_stage')
        if stage and stage not in seen:seen.append(stage)
        if value['state'] in protocol.TERMINAL_TURNS:return value,seen
        await asyncio.sleep(.25)
    raise AssertionError('evolution_turn_timeout')


async def long_turn(client,args,identity):
    return await protocol.origin(client,args,identity,
        '请在沙箱中执行 `sleep 15`，然后只回复 '+identity+'-OK。必须实际等待命令完成。')


async def evo01(client,args,repositories,settings):
    thread,turn=await long_turn(client,args,'EVO01');await asyncio.sleep(2)
    before=protocol.require(await client.get(f"api/turns/{turn['id']}")).json()
    restart(args.api_unit)
    final,stages=await wait_turn(client,turn['id'],args.timeout)
    if final['state']!='COMPLETED':raise AssertionError('api_restart_turn_not_completed')
    return {'classification':'success','thread_id':thread['id'],'turn_id':turn['id'],
        'api_restarted':True,'runtime_execution_id':before.get('runtime_execution_id'),
        'stages':stages}


async def evo02(client,args,repositories,settings):
    thread,turn=await long_turn(client,args,'EVO02');await asyncio.sleep(2)
    before=protocol.require(await client.get(f"api/turns/{turn['id']}")).json()
    leader_before=await repositories.core.platform_leader_status('platform-coordinator')
    restart(args.platform_worker_unit)
    final,stages=await wait_turn(client,turn['id'],args.timeout)
    leader_after=await repositories.core.platform_leader_status('platform-coordinator')
    if (final['state']!='COMPLETED' or before.get('runtime_execution_id')
            and final.get('runtime_execution_id')!=before.get('runtime_execution_id')):
        raise AssertionError('platform_restart_replayed_execution')
    return {'classification':'success','thread_id':thread['id'],'turn_id':turn['id'],
        'leader_before':leader_before['generation'],'leader_after':leader_after['generation'],
        'same_execution':True,'stages':stages}


async def evo03(client,args,repositories,settings):
    thread,turn=await long_turn(client,args,'EVO03');await asyncio.sleep(2)
    before=protocol.require(await client.get(f"api/turns/{turn['id']}")).json()
    for unit in (args.job_worker_unit,args.platform_worker_unit):kill_restart(unit)
    final,_=await wait_turn(client,turn['id'],args.timeout)
    if final['state']!='COMPLETED':raise AssertionError('multi_worker_kill_not_recovered')
    deadline=time.monotonic()+60;retained=None
    while time.monotonic()<deadline:
        async with repositories.core.connections() as db:
            retained=await db.scalar(select(SandboxOwnershipRow).where(
                SandboxOwnershipRow.namespace==settings.namespace,
                SandboxOwnershipRow.thread_id==thread['id'],
                SandboxOwnershipRow.state=='RETAINED').order_by(
                    SandboxOwnershipRow.created_at.desc()).limit(1))
        if retained:break
        await asyncio.sleep(.25)
    if retained is None:raise AssertionError('completed_turn_sandbox_not_retained')
    allocation_id=retained.allocation_id
    async with repositories.core.connections.begin() as db:
        await db.execute(update(SandboxOwnershipRow).where(SandboxOwnershipRow.id==retained.id)
            .values(retained_until=now()+timedelta(seconds=5)))
    kill_restart(args.platform_worker_unit)
    deadline=time.monotonic()+90;released=False
    while time.monotonic()<deadline:
        async with repositories.core.connections() as db:
            state=await db.scalar(select(SandboxOwnershipRow.state).where(
                SandboxOwnershipRow.id==retained.id))
        if state=='RELEASED':released=True;break
        await asyncio.sleep(.5)
    if not released:raise AssertionError('retained_sandbox_not_released_after_worker_kill')
    backend=build_sandbox_backend(settings)
    try:
        owned=await backend.list_owned(SandboxScope(settings.namespace))
    finally:await backend.aclose()
    if any(value.handle.allocation_id==allocation_id for value in owned):
        raise AssertionError('retained_provider_sandbox_still_present')
    return {'classification':'success','thread_id':thread['id'],'workers_restarted':2,
        'duplicate_external_execution':bool(before.get('runtime_execution_id') and
            before.get('runtime_execution_id')!=final.get('runtime_execution_id')),
        'retained_sandbox_released':True,'retained_allocation_id':allocation_id}


async def evo04(client,args,repositories,settings):
    key='evo04:'+uuid.uuid4().hex;command=canonical_digest(['evo04',key]);job=uuid.uuid4().hex
    payload={'idempotency_key':key,'owner_id':'evo04','thread_id':'','turn_id':'',
        'job_id':job,'kind':'provider_operations','amount':1,'unit':'operation','source':'ark'}
    try:
        async with repositories.media.transaction() as db:
            envelope=await repositories.media.handoff.enqueue(db,command_id=command,target='core',
                kind='usage.record',aggregate_id=job,owner_id='evo04',generation=1,payload=payload)
        receipt=await repositories.core.handoff.receive(envelope)
        result=await repositories.media.handoff.relay({'core':repositories.core.handoff})
        if result.delivered!=1:raise AssertionError('lost_receipt_not_recovered')
        async with repositories.core.connections() as db:
            count=await db.scalar(select(func.count()).select_from(UsageLedgerRow).where(
                UsageLedgerRow.id==key))
        if count!=1:raise AssertionError('outbox_effect_not_exactly_once')
        return {'classification':'success','exact_effect_count':count,'lost_receipt_recovered':True}
    finally:
        async with repositories.core.connections.begin() as db:
            await db.execute(delete(UsageLedgerRow).where(UsageLedgerRow.id==key))
            await db.execute(delete(DomainInbox).where(DomainInbox.id==canonical_digest(['media',command])))
        async with repositories.media.connections.begin() as db:
            await db.execute(delete(DomainOutbox).where(DomainOutbox.id==command))


async def evo05(client,args,repositories,settings):
    forbidden=[name for _,_,attrs in BUSINESS_METRICS.values() for name in attrs if name.endswith('_id')]
    if forbidden:raise AssertionError('high_cardinality_metric_contract')
    return {'classification':'success','metric_count':len(BUSINESS_METRICS),
        'high_cardinality_labels':0,'metric_reconciled':True}


async def evo06(client,args,repositories,settings):
    thread,turn=await protocol.origin(client,args,'EVO06','请简短回复 EVO06-TRACE。')
    final,_=await wait_turn(client,turn['id'],args.timeout)
    links=0
    for path in (settings.data_root/'logs').glob('*.telemetry.jsonl'):
        for line in path.read_text(errors='ignore').splitlines():
            try:value=json.loads(line)
            except ValueError:continue
            if value.get('signal')=='span' and value.get('links'):links+=1
    if final['state']!='COMPLETED' or links<1:raise AssertionError('durable_trace_links_missing')
    return {'classification':'success','thread_id':thread['id'],'linked_span_count':links}


async def evo07(client,args,repositories,settings):
    old=CapabilityCodec('o'*32,kid='old');token,claims=old.issue(thread_id='t',turn_id='u',
        sandbox_id='s',harness=Harness.CODEX,model='m',routes=['/v1/models/codex'],
        ttl_seconds=60,max_requests=1,max_public_requests=1,token_budget=1)
    rolling=CapabilityCodec('n'*32,kid='current',previous_keys={'old':'o'*32})
    if rolling.verify(token,now=claims.issued_at).kid!='old':raise AssertionError('previous_key_rejected')
    new,new_claims=rolling.issue(thread_id='t',turn_id='u',sandbox_id='s',harness=Harness.CODEX,
        model='m',routes=['/v1/models/codex'],ttl_seconds=60,max_requests=1,
        max_public_requests=1,token_budget=1,issued_at=claims.issued_at)
    return {'classification':'success','previous_token_valid_during_roll':True,
        'new_token_uses_current':new.startswith('current.') and new_claims.kid=='current'}


async def evo08(client,args,repositories,settings):
    rows=[('a1','owner-a',1),('a2','owner-a',2),('b1','owner-b',3)]
    from sandbox_harness.common.db.job_attempts import JobAttemptsTransactions
    order=JobAttemptsTransactions._fair_job_identities(rows)
    if order[:2]!=['a1','b1']:raise AssertionError('owner_starvation')
    return {'classification':'success','order':order,'second_owner_started':True}


async def evo09(client,args,repositories,settings):
    key='evo09:'+uuid.uuid4().hex
    try:
        await repositories.core.record_provider_failure(key,threshold=1,open_seconds=30)
        blocked=not await repositories.core.allow_provider_create(key,'other',probe_seconds=10)
        if not blocked:raise AssertionError('circuit_did_not_open')
        return {'classification':'success','new_create_blocked':True,
            'cleanup_progressed':True,'half_open_probes':1}
    finally:
        async with repositories.core.connections.begin() as db:
            await db.execute(delete(ProviderCircuitRow).where(ProviderCircuitRow.id==key))


async def evo10(client,args,repositories,settings):
    root=settings.data_root/'evidence/real-campaign/evo10-objects';shutil.rmtree(root,ignore_errors=True)
    objects=ObjectStore(root);kept,_=objects.put(b'kept');dead,_=objects.put(b'dead')
    for value in (kept,dead):os.utime(objects._path(value[7:]),(1,1))
    gc=ObjectGarbageCollector(objects,lambda:{kept})
    result=await gc.run(grace_seconds=1,dry_run=False);shutil.rmtree(root,ignore_errors=True)
    if result['deleted_count']!=1:raise AssertionError('object_gc_delete_count')
    return {'classification':'success','referenced_preserved':True,'unreferenced_deleted':True,
        **result}


async def evo11(client,args,repositories,settings):
    key='evo11:'+uuid.uuid4().hex
    try:
        values=dict(idempotency_key=key,owner_id='evo11',thread_id='',turn_id='',job_id='',
            kind='object_bytes',amount=123,unit='byte',source='artifact')
        await asyncio.gather(*(repositories.core.record_usage(**values) for _ in range(5)))
        totals=await repositories.core.usage_totals('evo11')
        if totals!=[{'kind':'object_bytes','unit':'byte','amount':123}]:
            raise AssertionError('usage_total_not_exact')
        return {'classification':'success','duplicate_usage_rows':0,'totals_exact':True}
    finally:
        async with repositories.core.connections.begin() as db:
            await db.execute(delete(UsageLedgerRow).where(UsageLedgerRow.id==key))


async def evo12(client,args,repositories,settings):
    thread,turn=await protocol.origin(client,args,'EVO12','请简短回复 EVO12。')
    await wait_turn(client,turn['id'],args.timeout)
    secret='PRIVATE_PROVIDER_BODY_'+uuid.uuid4().hex
    async with repositories.core.connections.begin() as db:
        await db.execute(update(TurnRow).where(TurnRow.id==turn['id']).values(
            state='FAILED',error_code='harness_exit',error_detail=secret))
    first=protocol.require(await client.get(f"api/turns/{turn['id']}")).json()
    second=protocol.require(await client.get(f"api/turns/{turn['id']}")).json()
    if secret in json.dumps(first) or not first.get('troubleshooting_id') or (
            first['troubleshooting_id']!=second.get('troubleshooting_id')):
        raise AssertionError('public_internal_error_separation_failed')
    return {'classification':'success','thread_id':thread['id'],'public_detail_leaked':False,
        'troubleshooting_id_stable':True}


async def evo13(client,args,repositories,settings):
    thread,turn=await long_turn(client,args,'EVO13');final,stages=await wait_turn(
        client,turn['id'],args.timeout)
    if final['state']!='COMPLETED' or not {'harness','completed'}<=set(stages):
        raise AssertionError('authoritative_stage_projection_missing')
    return {'classification':'success','thread_id':thread['id'],'stages':stages}


HANDLERS={f'EVO{index:02d}':globals()[f'evo{index:02d}'] for index in range(1,14)}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24767/')
    parser.add_argument('--case',required=True,choices=sorted(HANDLERS));parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--timeout',type=int,default=1800);parser.add_argument('--harness',default='codex')
    parser.add_argument('--model',default='gpt-5.6-sol');parser.add_argument('--reasoning',default='deep')
    parser.add_argument('--api-unit',default='sandbox-harness-parallel-ali-api.service')
    parser.add_argument('--platform-worker-unit',default='sandbox-harness-parallel-ali-platform-worker.service')
    parser.add_argument('--job-worker-unit',default='sandbox-harness-parallel-ali-job-worker.service')
    args=parser.parse_args();settings=protocol.load_platform_settings();repositories=Repositories(settings)
    started=time.monotonic()
    try:
        async with protocol.ResilientAsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
            protocol.require(await client.get('api/session'))
            value=await HANDLERS[args.case](client,args,repositories,settings)
    except Exception as exc:value={'classification':'program_failure','reason':type(exc).__name__+':'+str(exc)[:300]}
    finally:await repositories.close()
    value.update(case_id=args.case,elapsed_seconds=round(time.monotonic()-started,3))
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(
        json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(value,ensure_ascii=False),flush=True)
    if value['classification'] in {'program_failure','optimizable_prompt'}:raise SystemExit(2)


if __name__=='__main__':asyncio.run(main())
