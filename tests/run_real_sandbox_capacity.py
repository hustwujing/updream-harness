#!/usr/bin/env python3
"""Real sandbox capacity gate, using an explicitly isolated provider fixture.

The fixture supplies Settings, a frozen SandboxProfile and image_digest. Its
provider Team/project must allow exactly one sandbox. The runner creates its own
PostgreSQL database, namespace, data directory and Worker subprocesses. No model
is called; successful Jobs resize a real input through the sandbox Runtime.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime,timezone
import io
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from PIL import Image
from sqlalchemy import select,text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.harness_job import JobGrantFactory
from sandbox_harness.job_contracts import JobArtifactInput,JobExpectedOutput,JobGroupCreate,JobRequest,JobState
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.media_render_adapter import MediaRenderOperationAdapter
from sandbox_harness.objects import ObjectStore
from sandbox_harness.pool import SandboxPool
from sandbox_harness.profiles import ProviderRelease
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox_contracts import (SandboxCreateRequest,SandboxProfile,SandboxScope,
    SandboxCapacityUnavailable,SandboxRateLimited)
from sandbox_harness.sandbox_factory import build_sandbox_backend,build_profile_resolver
from sandbox_harness.scheduler import NodeInventory,ResourceRequest,Scheduler
from sandbox_harness.store import JobAttemptRow,Store

ROOT=Path(__file__).resolve().parents[1]
CASES=('RC01','RC02','RC03','RC04','RC05')


def append_audit(path:Path,value:dict):
    with path.open('a') as f:f.write(json.dumps(value,separators=(',',':'))+'\n')


def fixture(path):
    raw=json.loads(Path(path).read_text())
    return raw,Settings.model_validate(raw['settings']),SandboxProfile(**raw['profile'])


async def worker_main(path):
    raw,settings,profile=fixture(path);store=Store(settings.database_url)
    await store.initialize();backend=build_sandbox_backend(settings)
    audit=settings.data_root/'execution-audit.jsonl';create=backend.create
    async def counted_create(request):
        append_audit(audit,{'event':'create_requested','allocation_id':request.allocation_id})
        try:value=await create(request)
        except (SandboxCapacityUnavailable,SandboxRateLimited) as exc:
            append_audit(audit,{'event':'create_rejected','allocation_id':request.allocation_id,
                'code':exc.code})
            raise
        append_audit(audit,{'event':'created','allocation_id':request.allocation_id,'sandbox_id':value.id})
        return value
    backend.create=counted_create
    pool=SandboxPool(backend,{Harness.CODEX:profile.provider_ref},{Harness.CODEX:0},
        max_sandboxes=2,profile_resolver=build_profile_resolver(settings,backend),ttl_seconds=300)
    runtime=RuntimeClient(timeout_seconds=60,backend=backend,
        bundle_dir=settings.runtime_bundle_dir,default_bundle_digest=settings.runtime_bundle_digest,
        code_bundle_dir=settings.runtime_code_bundle_dir,
        default_code_bundle_digest=settings.runtime_code_bundle_digest,
        staging_root=settings.data_root/'downloads')
    render=runtime.media_render
    async def counted_render(sandbox,recipe):
        append_audit(audit,{'event':'runtime_submitted','allocation_id':sandbox.handle.allocation_id})
        return await render(sandbox,recipe)
    runtime.media_render=counted_render;scheduler=Scheduler(store)
    inventory=NodeInventory(node_id='capacity-node',generation=raw['worker_generation'],
        cpu_total=raw['node_cpu'],memory_mb_total=131072,disk_mb_total=100000,
        profiles=[{'runtime_profile_id':profile.profile_id,'harness':'codex',
            'image_digest':raw['image_digest'],'image_ref':profile.provider_ref}])
    await scheduler.register_node(inventory)
    (settings.data_root/f"worker-ready-{inventory.generation}.json").write_text(json.dumps(
        {'pid':os.getpid(),'generation':inventory.generation}))
    async def heartbeat():
        while True:
            await asyncio.sleep(2);await scheduler.register_node(inventory)
    heartbeat_task=asyncio.create_task(heartbeat())
    class Profiles:
        def provider_release(self,*args,**kwargs):
            return ProviderRelease(ref=profile.provider_ref,revision=profile.revision,
                artifact_digest=raw['image_digest'])
    adapter=MediaRenderOperationAdapter(scheduler=scheduler,pool=pool,runtime=runtime,
        store=store,objects=ObjectStore(settings.data_root/'objects'),settings=settings,
        token_factory=JobGrantFactory(settings),profile_resolver=Profiles())
    worker=JobWorker(store,{'media.render':adapter},node_id='capacity-node',
        worker_id='capacity-'+uuid.uuid4().hex,concurrency=1,lease_seconds=2,poll_seconds=.1)
    try:await worker.run()
    finally:
        heartbeat_task.cancel();await asyncio.gather(heartbeat_task,return_exceptions=True)
        await pool.close();await store.close();await backend.aclose()


async def wait_until(check,timeout=60):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=await check()
        if value:return value
        await asyncio.sleep(.1)
    raise TimeoutError('capacity_case_observation_timeout')


async def create_job(store,objects,profile,digest,case,timeout,adapter='media.render'):
    native=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=native.model,
        reasoning_level='standard',title=case+' resource exhaustion'),native,'capacity-owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='resize capacity fixture',
        idempotency_key='turn-'+uuid.uuid4().hex),native,'capacity-owner')
    data=io.BytesIO();Image.new('RGB',(16,16),(23,85,130)).save(data,format='PNG');content=data.getvalue()
    ref,sha=objects.put(content)
    source=await store.add_artifact(turn.id,path='input.png',sha256=sha,size=len(content),mime='image/png',object_ref=ref)
    request=JobRequest(local_job_id='resize',kind='operation',adapter=adapter,
        descriptor_id='media.render',descriptor_version='1',descriptor_digest='a'*64,
        idempotency_key='job-'+uuid.uuid4().hex,timeout_seconds=timeout,
        parameters={'recipe':{'operation':'image.resize','input_role':'source','width':8,
            'height':8,'output_filename':'result.png'}},
        input_artifacts=(JobArtifactInput(artifact_id=source.id,artifact_role='source'),),
        expected_outputs=(JobExpectedOutput(artifact_role='rendered',min_items=1,max_items=1,
            mime_patterns=('image/png',),output_disposition='delivery'),),
        child_profile_id='capacity-operation',runtime_profile_id=profile.profile_id,
        resource_requirements={'harness':'codex','image_digest':digest,'cpu':8,
            'memory_mb':8192,'disk_mb':1024,'delegation_depth':1},
        retry_policy={'max_pre_accept_attempts':1,'deterministic':True})
    _,jobs=await store.create_job_group('capacity-owner',JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key='group-'+uuid.uuid4().hex,jobs=[request]))
    return jobs[0]


async def run_case(case,raw,path,store,backend,profile):
    settings=Settings.model_validate(raw['settings']);scheduler=Scheduler(store)
    allocation=case.lower()+'-holder-'+uuid.uuid4().hex;held=None;process=None;log=None;reserved=None
    evidence={'case':case,'passed':False,'scope':'real_provider_quota' if case!='RC05' else 'real_scheduler_capacity'}
    def start_worker():
        nonlocal log
        raw['worker_generation']=raw.get('worker_generation',int(time.time()))+1
        path.write_text(json.dumps(raw));path.chmod(0o600)
        log=(settings.data_root/(case+'-worker-'+uuid.uuid4().hex[:8]+'.log')).open('w')
        return subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--fixture',str(path),'--worker'],
            cwd=ROOT,env={**os.environ,'TMPDIR':str(settings.data_root/'tmp'),'PYTHONDONTWRITEBYTECODE':'1'},
            stdout=log,stderr=subprocess.STDOUT)
    async def attempt(job_id):
        async with store.connections() as db:
            return (await db.scalars(select(JobAttemptRow).where(JobAttemptRow.job_id==job_id)
                .order_by(JobAttemptRow.generation.desc()).limit(1))).first()
    async def node_ready():
        from sandbox_harness.store import NodeRow
        ready=settings.data_root/f"worker-ready-{raw['worker_generation']}.json"
        if not ready.exists() or json.loads(ready.read_text()).get('pid')!=process.pid:return False
        async with store.connections() as db:
            row=await db.get(NodeRow,'capacity-node')
            return row and row.generation==raw['worker_generation']
    try:
        held=await backend.create(SandboxCreateRequest(harness=Harness.CODEX,profile=profile,
            environment={},ttl_seconds=300,allocation_id=allocation))
        evidence['holder_sandbox_id']=held.id
        objects=ObjectStore(settings.data_root/'objects')
        process=start_worker()
        await wait_until(node_ready)
        if case=='RC05':
            holder=await create_job(store,objects,profile,raw['image_digest'],case+' holder',120,
                adapter='capacity-fixture-holder')
            reserved=await scheduler.reserve(holder.id,1,ResourceRequest(child_profile_id='capacity-operation',
                runtime_profile_id=profile.profile_id,harness='codex',image_digest=raw['image_digest'],
                cpu=raw['node_cpu'],memory_mb=8192,disk_mb=1024))
        job=await create_job(store,objects,profile,raw['image_digest'],case,5 if case=='RC03' else 120)
        evidence['job_id']=job.id
        async def waiting():
            row=await attempt(job.id)
            return row if row and row.outcome=='INFRASTRUCTURE_WAIT' else None
        first=await wait_until(waiting);evidence['waiting_generation']=first.generation
        assert first.accepted_at is None and await store.latest_job_acceptance(job.id) is None
        evidence['waiting_without_acceptance']=True
        events=[json.loads(line) for line in (settings.data_root/'execution-audit.jsonl').read_text().splitlines()] if (settings.data_root/'execution-audit.jsonl').exists() else []
        own=[r for r in events if r.get('allocation_id')==job.id]
        assert not any(r['event'] in {'created','runtime_submitted'} for r in own)
        if case=='RC05':assert not own
        else:assert any(r['event']=='create_rejected' for r in own)
        await backend.get(held.handle);evidence['holder_healthy_while_waiting']=True
        if case=='RC02':await store.request_job_cancel(job.id,'capacity-owner')
        elif case=='RC04':
            old_pid=process.pid;process.kill();await asyncio.to_thread(process.wait);log.close()
            process=start_worker();evidence['killed_worker_pid']=old_pid
            await wait_until(node_ready)
            async def replaced():
                row=await waiting()
                return row if row and row.generation>first.generation else None
            recovered=await wait_until(replaced);evidence['replacement_generation']=recovered.generation
        if case not in {'RC02','RC03'}:
            await backend.delete(held);held=None
            if reserved:
                await scheduler.release(reserved.id,reserved.node_id,reserved.node_generation);reserved=None
        async def terminal():
            value=await store.get_job(job.id)
            return value if value.state in {JobState.SUCCEEDED,JobState.FAILED,JobState.CANCELLED,JobState.UNKNOWN} else None
        final=await wait_until(terminal,150);evidence['job_state']=final.state.value
        evidence['result_code']=final.result.get('error_code')
        if case=='RC02':assert final.state==JobState.CANCELLED
        elif case=='RC03':
            assert final.state==JobState.FAILED
            assert final.result.get('side_effect_state')=='none'
        else:assert final.state==JobState.SUCCEEDED
        async def cleaned():
            row=await store.get_job_cleanup(job.id)
            return not await scheduler.active_reservation(job.id) and (not row or row['state']=='completed')
        await wait_until(cleaned)
        events=[json.loads(line) for line in (settings.data_root/'execution-audit.jsonl').read_text().splitlines()]
        own=[r for r in events if r.get('allocation_id')==job.id]
        evidence['runtime_submissions']=sum(r['event']=='runtime_submitted' for r in own)
        assert evidence['runtime_submissions']==(0 if case in {'RC02','RC03'} else 1)
        if case in {'RC01','RC05'}:assert (await store.get_job(job.id)).generation==first.generation
        evidence['passed']=True
    except Exception as exc:
        evidence['error_type']=type(exc).__name__
    finally:
        if process and process.poll() is None:process.kill();await asyncio.to_thread(process.wait)
        if log:log.close()
        if held:await backend.delete(held)
        if reserved:await scheduler.release(reserved.id,reserved.node_id,reserved.node_generation)
    return evidence


async def run(args):
    selected=args.cases.split(',')
    if len(set(selected))!=len(selected) or set(selected)-set(CASES):
        raise ValueError('invalid_capacity_cases')
    raw,settings,profile=fixture(args.fixture);run_id=uuid.uuid4().hex[:16]
    data_root=ROOT/'.data/evidence/resource-exhaustion'/run_id;(data_root/'tmp').mkdir(parents=True)
    url=make_url(settings.database_url);assert url.host in {'127.0.0.1','localhost'} and url.port==28433
    name='capacity_'+run_id;admin=create_async_engine(url.set(database='postgres'),isolation_level='AUTOCOMMIT')
    async with admin.connect() as db:await db.execute(text('CREATE DATABASE "'+name+'"'))
    updated=settings.model_copy(update={'database_url':url.set(database=name).render_as_string(hide_password=False),
        'data_root':data_root,'namespace':'capacity-'+run_id,'redis_url':'','redis_prefix':'capacity-'+run_id+':'})
    raw={**raw,'settings':updated.model_dump(mode='json'),'node_cpu':16}
    path=data_root/'worker-fixture.json';fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as f:json.dump(raw,f)
    store=Store(updated.database_url);await store.initialize();backend=build_sandbox_backend(updated)
    report={'revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        'run_id':run_id,'timestamp':datetime.now(timezone.utc).isoformat(),'selected_cases':selected,
        'cases':{},'database_removed':False,'source_state':{
            'dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)),
            'sha256':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in [
                'tests/run_real_sandbox_capacity.py','src/sandbox_harness/job_worker.py',
                'src/sandbox_harness/job_allocation.py','src/sandbox_harness/store.py']}}}
    try:
        for case in selected:
            started=time.monotonic();value=await run_case(case,raw,path,store,backend,profile)
            value['elapsed_seconds']=round(time.monotonic()-started,3);report['cases'][case]=value
            print(json.dumps(value),flush=True)
            Path(args.report).write_text(json.dumps(report,indent=2)+'\n')
            if not value['passed']:break
    except Exception as exc:
        report['run_error']=type(exc).__name__
    finally:
        remaining=await backend.list_owned(SandboxScope(updated.namespace));report['remaining_owned']=[r.id for r in remaining]
        unresolved=await store.active_sandbox_allocations()
        report['unresolved_allocations']=[list(value) for value in sorted(unresolved)]
        await store.close();await backend.aclose()
        if not remaining and not unresolved:
            async with admin.connect() as db:await db.execute(text('DROP DATABASE "'+name+'"'))
            report['database_removed']=True;path.unlink()
        else:report['retained_database']=name
        await admin.dispose()
        report['passed']=(set(report['cases'])==set(selected) and not report.get('run_error')
            and all(r['passed'] for r in report['cases'].values()) and report['database_removed'])
        Path(args.report).write_text(json.dumps(report,indent=2)+'\n')
    return report


def main():
    p=argparse.ArgumentParser();p.add_argument('--fixture',required=True);p.add_argument('--worker',action='store_true')
    p.add_argument('--cases',default=','.join(CASES));p.add_argument('--report',type=Path)
    args=p.parse_args()
    if args.worker:asyncio.run(worker_main(args.fixture));return
    if not args.report:p.error('--report is required')
    args.report.parent.mkdir(parents=True,exist_ok=True)
    result=asyncio.run(run(args));print(json.dumps({'passed':result['passed'],'report':str(args.report)}))
    if not result['passed']:raise SystemExit(1)


if __name__=='__main__':main()
