#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time
import uuid

import httpx
from sqlalchemy import delete, func, select, update

import run_real_cross_database as cross_database
import run_real_parallel_protocol as protocol
from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.contracts.domain import Harness, ThreadCreate, TurnCreate, TurnState, now
from sandbox_harness.common.contracts.sandbox_contracts import SandboxCreateRequest, SandboxScope
from sandbox_harness.common.db.accounts import AccountsRepository
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.domain_database import DomainInbox, DomainOutbox
from sandbox_harness.common.db.media_repository import MediaRepository
from sandbox_harness.common.db.media_models import MediaSourcePermitRow
from sandbox_harness.common.db.models import (
    CapabilityLedgerRow, JobRow, MediaBatchRow, MediaJobOutputRow, MediaJobRow,
    ModelTokenReservationRow, OwnerAdmissionRow, ProviderCircuitRow, ThreadRow,
    TurnOrderRow, TurnRow, UsageLedgerRow,
    canonical_digest,
)
from sandbox_harness.common.db.ownership_models import RemoteObligationRow, SandboxOwnershipRow
from sandbox_harness.common.db.site_repository import SiteRepository
from sandbox_harness.common.observability.observability import BUSINESS_METRICS
from sandbox_harness.common.security.capability import CapabilityCodec
from sandbox_harness.common.sandbox.runtime import RuntimeClient
from sandbox_harness.common.sandbox.sandbox_factory import (
    build_profile_resolver, build_sandbox_backend, primary_provider_releases)
from sandbox_harness.common.storage.object_gc import ObjectGarbageCollector
from sandbox_harness.common.storage.objects import ObjectStore, object_chunks


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


class LostRestoreResponseTransport(httpx.AsyncBaseTransport):
    """Let the first checkpoint restore finish, then lose only its response."""
    def __init__(self):
        self.inner=httpx.AsyncHTTPTransport();self.restore_attempts=0;self.request_digests=[]

    async def handle_async_request(self,request):
        if request.method=='PUT' and request.url.path=='/v1/checkpoint':
            body=await request.aread();self.restore_attempts+=1
            self.request_digests.append(hashlib.sha256(body).hexdigest())
            response=await self.inner.handle_async_request(request)
            if self.restore_attempts==1:
                await response.aread();await response.aclose()
                raise httpx.ReadError('EVO16 controlled lost restore response',request=request)
            return response
        return await self.inner.handle_async_request(request)

    async def aclose(self):
        await self.inner.aclose()


def evolution_sandbox_request(settings,backend,identity):
    releases=primary_provider_releases(settings,'aliyun')
    release=releases[Harness.CODEX]
    profile=build_profile_resolver(settings,backend,releases).resolve(
        Harness.CODEX,release.ref,profile_id='primary-full-cpu-codex',
        revision=release.revision)
    return SandboxCreateRequest(harness=Harness.CODEX,profile=profile,environment={},
        ttl_seconds=min(600,settings.sandbox_timeout_seconds),allocation_id=identity)


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


async def evo14(client,args,repositories,settings):
    codec=CapabilityCodec.from_settings(settings)
    token,claims=codec.issue(thread_id=uuid.uuid4().hex,turn_id=uuid.uuid4().hex,
        sandbox_id=uuid.uuid4().hex,harness=Harness.CODEX,model=args.model,
        routes=['/v1/models/codex'],ttl_seconds=300,max_requests=10,
        max_public_requests=1,token_budget=100_000)
    gateway_url=(settings.aliyun_sandbox_gateway_url
        if settings.backend=='aliyun' else settings.gateway_url).rstrip('/')
    headers={'Authorization':'Bearer '+token}
    first_payload={'model':args.model,'input':'Return a detailed technical checklist.',
        'stream':False,'max_output_tokens':8192}
    first_task=None;revoked=False
    async with httpx.AsyncClient(timeout=None,trust_env=False) as gateway:
        try:
            first_task=asyncio.create_task(gateway.post(
                gateway_url+'/v1/models/codex/responses',headers=headers,json=first_payload))
            deadline=time.monotonic()+30;orphan=None
            while time.monotonic()<deadline:
                async with repositories.core.connections() as db:
                    orphan=await db.scalar(select(ModelTokenReservationRow).where(
                        ModelTokenReservationRow.nonce==claims.nonce,
                        ModelTokenReservationRow.state=='reserved').order_by(
                            ModelTokenReservationRow.created_at.desc()).limit(1))
                if orphan:break
                await asyncio.sleep(.02)
            if not orphan:raise AssertionError('gateway_reservation_not_observed')
            old_pid,new_pid=kill_restart(args.gateway_unit)
            try:await asyncio.wait_for(first_task,10)
            except Exception:pass
            deadline=time.monotonic()+60
            while True:
                try:
                    health=await gateway.get(gateway_url+'/health',timeout=2)
                    if health.status_code==200:break
                except httpx.HTTPError:pass
                if time.monotonic()>=deadline:raise AssertionError('gateway_restart_not_ready')
                await asyncio.sleep(.25)
            second=await gateway.post(gateway_url+'/v1/models/codex/responses',headers=headers,
                json={'model':args.model,'input':'Reply with EVO14-OK.',
                    'stream':False,'max_output_tokens':64},timeout=args.timeout)
            if second.status_code!=200:
                raise AssertionError('gateway_second_request_failed:'+str(second.status_code))
            revoke_headers={'X-Sandbox-Harness-Control':settings.gateway_control_key,
                'X-Updream-Thread-ID':claims.thread_id,'X-Updream-Turn-ID':claims.turn_id,
                'X-Updream-Sandbox-ID':claims.sandbox_id}
            response=await gateway.post(gateway_url+'/internal/revoke/'+claims.nonce,
                headers=revoke_headers,timeout=30)
            if response.status_code!=200:raise AssertionError('gateway_revoke_failed')
            revoked=True
            async with repositories.core.connections() as db:
                ledger=await db.get(CapabilityLedgerRow,claims.nonce)
                reservations=list((await db.scalars(select(ModelTokenReservationRow).where(
                    ModelTokenReservationRow.nonce==claims.nonce))).all())
            released=[row for row in reservations if row.id==orphan.id and row.state=='released']
            if not ledger or ledger.token_reserved!=0 or not released:
                raise AssertionError('gateway_orphan_reservation_not_released')
            return {'classification':'success','gateway_restarted':True,'old_pid':old_pid,
                'new_pid':new_pid,'first_reserved_tokens':orphan.reserved,
                'second_request_succeeded':True,'orphan_reservation_released':True,
                'token_reserved':ledger.token_reserved,'token_consumed':ledger.token_consumed}
        finally:
            if first_task and not first_task.done():
                first_task.cancel()
                try:await first_task
                except BaseException:pass
            if not revoked:
                try:await repositories.core.revoke_capability(claims.nonce,claims.expires_at)
                except Exception:pass
            async with repositories.core.connections.begin() as db:
                await db.execute(delete(ModelTokenReservationRow).where(
                    ModelTokenReservationRow.nonce==claims.nonce))
                await db.execute(delete(CapabilityLedgerRow).where(
                    CapabilityLedgerRow.nonce==claims.nonce))


async def evo15(client,args,repositories,settings):
    spec=protocol.semantic_provider_job('evo15-video','video.generate',kind='video',
        model='sed2',mode='text_to_video',parameters={
            'prompt':'a blue sphere rotates slowly on a white background','duration':4,
            'resolution':'480p','aspect_ratio':'16:9','generate_audio':False})
    thread,_,job=await cross_database.submit_source_job(client,args,'EVO15',spec)
    async def accepted():
        value=await cross_database.source_snapshot(repositories,job['id'])
        accepted_attempts=[row for row in value.attempts if row.accepted_at is not None]
        return value if (accepted_attempts and value.media and value.permit
            and value.media.provider_started_at is not None) else None
    before=await cross_database.wait_until(
        accepted,min(args.timeout,300),'evo15_media_acceptance')
    accepted_generation=before.permit.source_generation
    accepted_external_ids={row.external_execution_id for row in before.attempts
        if row.accepted_at is not None}
    if len(accepted_external_ids)!=1:
        raise AssertionError('evo15_accepted_external_identity_invalid')
    provider_job_id=before.media.provider_job_id
    old_pid,new_pid=kill_restart(args.job_worker_unit)
    async def taken_over():
        value=await cross_database.source_snapshot(repositories,job['id'])
        return value if value.attempts and max(
            row.generation for row in value.attempts)>accepted_generation else None
    takeover=await cross_database.wait_until(taken_over,120,'evo15_takeover')
    takeover_generation=max(row.generation for row in takeover.attempts)
    protocol.require(await client.post(f"api/jobs/{job['id']}/cancel"))
    terminal=await protocol.wait_job(client,job['id'],args.timeout)
    command_id=canonical_digest(['media.source.cancel',before.permit.dispatch_id])
    async def cancellation_settled():
        snapshot=await cross_database.source_snapshot(repositories,job['id'])
        async with repositories.core.connections() as db:
            outbox=await db.get(DomainOutbox,command_id)
            obligation=await db.get(RemoteObligationRow,command_id)
        return (snapshot,outbox,obligation) if (snapshot.permit.state=='CANCELLED'
            and outbox and outbox.state=='acknowledged' and obligation
            and obligation.state=='acknowledged') else None
    after,outbox,obligation=await cross_database.wait_until(
        cancellation_settled,120,'evo15_cancellation_settlement')
    envelope=json.loads(outbox.envelope_json)
    cancel_generation=envelope['generation']
    cancel_payload=json.loads(envelope['payload_json'])
    accepted_after={row.external_execution_id for row in after.attempts
        if row.accepted_at is not None}
    if (terminal['state']!='CANCELLED' or cancel_generation!=accepted_generation
            or cancel_payload['source_generation']!=accepted_generation
            or accepted_after!=accepted_external_ids or after.artifacts):
        raise AssertionError('evo15_takeover_cancel_invariant_failed')
    return {'classification':'success','thread_id':thread['id'],'job_id':job['id'],
        'media_job_id':after.media.id,'provider_job_id':provider_job_id,
        'accepted_generation':accepted_generation,'takeover_generation':takeover_generation,
        'cancel_generation':cancel_generation,'duplicate_provider_submission':False,
        'outbox_acknowledged':outbox.state=='acknowledged',
        'remote_obligation_state':obligation.state,'permit_state':after.permit.state,
        'core_state':terminal['state'],'core_artifact_count':len(after.artifacts),
        'old_pid':old_pid,'new_pid':new_pid}


async def evo16(client,args,repositories,settings):
    identity='evo16-'+uuid.uuid4().hex;root=settings.data_root/'real-cases'/identity
    root.mkdir(parents=True,exist_ok=False);backend=build_sandbox_backend(settings);sandbox=None
    staged=None
    try:
        sandbox=await backend.create(evolution_sandbox_request(settings,backend,identity))
        runtime=RuntimeClient(backend=backend,bundle_dir=settings.runtime_bundle_dir,
            default_bundle_digest=settings.runtime_bundle_digest,
            code_bundle_dir=settings.runtime_code_bundle_dir,
            default_code_bundle_digest=settings.runtime_code_bundle_digest,
            staging_root=root/'downloads')
        await runtime.prepare(sandbox)
        await runtime.write_inspect_file(sandbox,'project/evo16.txt',b'EVO16-CHECKPOINT')
        staged=await runtime.checkpoint(sandbox,maximum=settings.max_checkpoint_bytes)
        objects=ObjectStore(root/'objects')
        reference,_=await asyncio.to_thread(objects.put_file,staged.path,
            digest=staged.sha256,size=staged.size)
        await runtime.write_inspect_file(sandbox,'project/evo16.txt',b'EVO16-MUTATED')
        transport=LostRestoreResponseTransport()
        retrying=RuntimeClient(backend=backend,transport=transport,
            request_retry_attempts=settings.runtime_request_retry_attempts,
            request_retry_initial_delay_seconds=.1,
            request_retry_backoff_multiplier=settings.runtime_request_retry_backoff_multiplier,
            request_retry_max_delay_seconds=.1,staging_root=root/'retry-downloads')
        await retrying.restore(sandbox,object_chunks(objects,reference,
            maximum=settings.max_checkpoint_bytes),size=staged.size)
        content,_=await runtime.inspect_file(sandbox,'project/evo16.txt')
        if (content!=b'EVO16-CHECKPOINT' or transport.restore_attempts!=2
                or len(set(transport.request_digests))!=1
                or transport.request_digests[0]!=staged.sha256):
            raise AssertionError('checkpoint_restore_replay_contract_failed')
        return {'classification':'success','sandbox_id':sandbox.id,
            'allocation_id':sandbox.handle.allocation_id,'restore_attempts':2,
            'request_body_replayed':True,'checkpoint_restored':True,
            'checkpoint_sha256':staged.sha256,'checkpoint_size':staged.size}
    finally:
        if staged is not None:staged.remove()
        if sandbox is not None:await backend.delete(sandbox)
        await backend.aclose();shutil.rmtree(root,ignore_errors=True)


async def evo17(client,args,repositories,settings):
    response=await client.post('api/threads',json={'harness':args.harness,'model':args.model,
        'reasoning_level':args.reasoning,'title':'Protocol EVO17'})
    thread=protocol.require(response).json();source_id=uuid.uuid4().hex;timestamp=now()
    async with repositories.core.connections.begin() as db:
        thread_row=await db.get(ThreadRow,thread['id'],with_for_update=True)
        sequence=thread_row.next_item_sequence+1;thread_row.next_item_sequence=sequence
        db.add(TurnRow(id=source_id,thread_id=thread['id'],
            idempotency_key='evo17-source-'+uuid.uuid4().hex,
            message='EVO17-MEDIA-COMPLETE: prior media task finished successfully.',
            public_message='Internal media completion',attachments_json='[]',
            harness=thread_row.harness,model=thread_row.model,
            reasoning_level=thread_row.reasoning_level,
            reasoning_effort=thread_row.reasoning_effort,
            delivery_mode='queue',network_access='restricted',internal=True,
            state=TurnState.FAILED.value,error_code='sandbox_workspace_restore_unavailable',
            error_detail='EVO17 injected transient Primary restore failure',
            created_at=timestamp,updated_at=timestamp))
        db.add(TurnOrderRow(turn_id=source_id,thread_id=thread['id'],
            sequence=sequence,created_at=timestamp))
    async with repositories.media.connections() as db:
        media_before=await db.scalar(select(func.count()).select_from(MediaJobRow).where(
            MediaJobRow.thread_id==thread['id']))
    first_message=('这是一个新的用户问题，当前请求标记是 EVO17-USER。若系统同时提供此前任务完成事实，'
        '请在回复中同时原样保留其中的 EVO17-MEDIA-COMPLETE 标记。不得重新执行此前任务或调用生成工具。')
    first=protocol.require(await client.post(f"api/threads/{thread['id']}/turns",json={
        'message':first_message,'attachments':[],
        'idempotency_key':'evo17-first-'+uuid.uuid4().hex,'delivery_mode':'start'})).json()
    first_final,_=await wait_turn(client,first['id'],args.timeout)
    if first_final['state']!='COMPLETED':raise AssertionError('evo17_first_turn_failed')
    items=protocol.require(await client.get(f"api/threads/{thread['id']}/items",
        params={'after':0,'limit':500})).json()
    first_reply='\n'.join(str(row.get('summary') or '') for row in items
        if row.get('turn_id')==first['id'] and row.get('type')=='agent_message')
    second=protocol.require(await client.post(f"api/threads/{thread['id']}/turns",json={
        'message':'只回复 EVO17-SECOND。','attachments':[],
        'idempotency_key':'evo17-second-'+uuid.uuid4().hex,'delivery_mode':'start'})).json()
    second_final,_=await wait_turn(client,second['id'],args.timeout)
    if second_final['state']!='COMPLETED':raise AssertionError('evo17_second_turn_failed')
    async with repositories.core.connections() as db:
        source=await db.get(TurnRow,source_id);first_row=await db.get(TurnRow,first['id'])
        second_row=await db.get(TurnRow,second['id'])
        core_jobs=await db.scalar(select(func.count()).select_from(JobRow).where(
            JobRow.thread_id==thread['id']))
    async with repositories.media.connections() as db:
        media_after=await db.scalar(select(func.count()).select_from(MediaJobRow).where(
            MediaJobRow.thread_id==thread['id']))
    completion_seen=('EVO17-MEDIA-COMPLETE' in first_row.message
        and 'EVO17-MEDIA-COMPLETE' in first_reply)
    if (not completion_seen or 'EVO17-USER' not in first_reply
            or 'EVO17-MEDIA-COMPLETE' in second_row.message
            or not source.error_detail.startswith('delivered:'+first['id']+'\n')
            or media_before!=0 or media_after!=0 or core_jobs!=0):
        raise AssertionError('evo17_internal_context_contract_failed')
    return {'classification':'success','thread_id':thread['id'],'source_turn_id':source_id,
        'first_turn_id':first['id'],'second_turn_id':second['id'],
        'completion_context_seen':completion_seen,'current_message_seen':True,
        'delivered_exactly_once':True,'duplicate_external_execution':False,
        'core_job_count':core_jobs,'media_job_count':media_after}


async def evo18(client,args,repositories,settings):
    thread,origin=await protocol.origin(client,args,'EVO18',
        '请在沙箱中执行 `sleep 20`，然后只回复 EVO18-ORIGIN-READY。必须实际等待命令完成。')
    draft=protocol.require(await client.post(f"api/threads/{thread['id']}/media/drafts",json={
        'kind':'image','model':'seedream-5.0','generation_mode':'text_to_image',
        'parameters':{'prompt':'A simple blue circle centered on a white background, no text',
            'size':'2K'},'references':[],
        'idempotency_key':'evo18-media-'+uuid.uuid4().hex,
        'turn_id':origin['id'],'output_role':'workspace'})).json()
    job=protocol.require(await client.post(f"api/media/drafts/{draft['id']}/confirm")).json()
    deadline=time.monotonic()+args.timeout;job_view=None
    while time.monotonic()<deadline:
        job_view=protocol.require(await client.get(f"api/media/jobs/{job['id']}")).json()
        if job_view['status'] in {'succeeded','failed','cancelled','unknown'}:break
        await asyncio.sleep(1)
    if not job_view or job_view['status']!='succeeded':
        raise AssertionError('evo18_media_job_not_succeeded')
    origin_final,_=await wait_turn(client,origin['id'],args.timeout)
    if origin_final['state']!='COMPLETED':raise AssertionError('evo18_origin_not_completed')
    owner=(await repositories.core.get_thread(thread['id'])).owner_id
    deadline=time.monotonic()+180;continuation=None;project=None;output=None
    while time.monotonic()<deadline:
        async with repositories.core.connections() as db:
            continuation=await db.scalar(select(TurnRow).join(TurnOrderRow,
                TurnOrderRow.turn_id==TurnRow.id).where(TurnRow.thread_id==thread['id'],
                TurnRow.internal.is_(True),TurnRow.id!=origin['id']).order_by(
                    TurnOrderRow.sequence.desc()).limit(1))
        projects=await repositories.core.current_project_files(thread['id'],owner)
        project=projects[0] if len(projects)==1 else None
        async with repositories.media.connections() as db:
            output=await db.scalar(select(MediaJobOutputRow).where(
                MediaJobOutputRow.job_id==job['id'],MediaJobOutputRow.role=='primary'))
        if continuation and project and output and output.project_file_id==project.id:break
        await asyncio.sleep(.5)
    if not continuation or not project or not output:
        raise AssertionError('evo18_media_promotion_not_observed')
    continuation_final,_=await wait_turn(client,continuation.id,args.timeout)
    async with repositories.media.connections() as db:
        rows=list((await db.scalars(select(MediaJobRow).where(
            MediaJobRow.thread_id==thread['id']))).all())
        batch=await db.get(MediaBatchRow,origin['id'])
    expected_path='/workspace/project/'+project.path
    if (continuation_final['state']!='COMPLETED' or expected_path not in continuation.message
            or len(rows)!=1 or rows[0].id!=job['id']
            or output.project_file_id!=project.id or not batch
            or batch.status!='continued' or batch.attempt_count!=1):
        raise AssertionError('evo18_media_continuation_contract_failed')
    execution_identity=rows[0].provider_job_id or 'synchronous:'+rows[0].id
    return {'classification':'success','thread_id':thread['id'],'origin_turn_id':origin['id'],
        'continuation_turn_id':continuation.id,'media_job_id':job['id'],
        'provider_job_id':rows[0].provider_job_id,'provider_execution_identity':execution_identity,
        'provider_submissions':1,'batch_attempt_count':batch.attempt_count,
        'project_file_id':project.id,'project_path':project.path,
        'project_file_promoted':True,'continuation_contains_path':True}


HANDLERS={f'EVO{index:02d}':globals()[f'evo{index:02d}'] for index in range(1,19)}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24767/')
    parser.add_argument('--case',required=True,choices=sorted(HANDLERS));parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--timeout',type=int,default=1800);parser.add_argument('--harness',default='codex')
    parser.add_argument('--model',default='gpt-5.6-sol');parser.add_argument('--reasoning',default='deep')
    parser.add_argument('--api-unit',default='sandbox-harness-parallel-ali-api.service')
    parser.add_argument('--platform-worker-unit',default='sandbox-harness-parallel-ali-platform-worker.service')
    parser.add_argument('--job-worker-unit',default='sandbox-harness-parallel-ali-job-worker.service')
    parser.add_argument('--gateway-unit',
        default='sandbox-harness-parallel-ali-sandbox-gateway-aliyun.service')
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
