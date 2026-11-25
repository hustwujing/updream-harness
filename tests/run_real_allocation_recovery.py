#!/usr/bin/env python3
"""Real provider allocation crashes in an exclusively owned two-sandbox fixture.

SIGKILL targets owned subprocesses. Visibility/response barriers are deliberate
fault injection; creation, Runtime commands and deletion use the real backend.
AR01 keeps its unobservable pre-RPC intent charged until fixture-scope retirement.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime,timezone
import hashlib
import io
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

from run_real_sandbox_capacity import ROOT,fixture,append_audit,create_job,worker_main,wait_until
from sandbox_harness.allocation_dispatch import AllocationService
from sandbox_harness.job_contracts import JobState
from sandbox_harness.objects import ObjectStore
from sandbox_harness.sandbox_contracts import SandboxScope
from sandbox_harness.sandbox_factory import build_sandbox_backend
from sandbox_harness.store import Store,SandboxAllocationRow,JobRow,ResourceReservationRow,JobCleanupRow

CASES=('AR02','AR03','AR04','AR01')


def audit_rows(root):
    path=root/'execution-audit.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def allocator_main(path):
    raw,settings,_=fixture(path);store=Store(settings.database_url)
    await store.initialize();backend=build_sandbox_backend(settings)
    create=backend.create;observe=backend.reconcile_create
    audit=settings.data_root/'execution-audit.jsonl'
    async def barrier(point,identity):
        gate=settings.data_root/'allocation-barrier.json'
        if gate.exists():
            value=json.loads(gate.read_text())
            if value=={'point':point,'allocation_id':identity}:
                append_audit(audit,{'event':'allocator_barrier','point':point,'allocation_id':identity,'pid':os.getpid()})
                while gate.exists():await asyncio.sleep(.05)
    async def counted(request):
        await barrier('before_rpc',request.allocation_id)
        append_audit(audit,{'event':'create_requested','allocation_id':request.allocation_id,
            'ttl_seconds':request.ttl_seconds})
        value=await create(request)
        append_audit(audit,{'event':'created','allocation_id':request.allocation_id,'sandbox_id':value.id})
        await barrier('after_rpc',request.allocation_id)
        return value
    async def delayed(scope,identity):
        value=await observe(scope,identity)
        gate=settings.data_root/'hold-allocation-visibility.json'
        if gate.exists() and json.loads(gate.read_text()).get('allocation_id')==identity:
            append_audit(audit,{'event':'visibility_withheld','allocation_id':identity,'found':value is not None})
            return None
        return value
    backend.create=counted;backend.reconcile_create=delayed
    service=AllocationService(store,backend,lease_seconds=settings.allocation_lease_seconds)
    try:await service.run(asyncio.Event(),concurrency=2)
    finally:await backend.aclose();await store.close()


async def run_case(case,base,profile,run_root,progress):
    raw,settings,_=fixture(base)
    data=run_root/case;(data/'tmp').mkdir(parents=True)
    url=make_url(settings.database_url)
    assert url.host in {'127.0.0.1','localhost'} and url.port==28433
    database='allocation_'+case.lower()+'_'+uuid.uuid4().hex[:12]
    admin=create_async_engine(url.set(database='postgres'),isolation_level='AUTOCOMMIT')
    async with admin.connect() as db:await db.execute(text('CREATE DATABASE "'+database+'"'))
    settings=settings.model_copy(update={'database_url':url.set(database=database).render_as_string(hide_password=False),
        'namespace':database.replace('_','-'),'data_root':data,'redis_url':'','object_backend':'local',
        'allocation_lease_seconds':30,'allocation_recovery_seconds':90,'allocation_provision_ttl_seconds':300})
    raw={**raw,'settings':settings.model_dump(mode='json'),'node_cpu':16,
        'worker_generation':int(time.time()),'pool_limit':1 if case=='AR04' else 2}
    path=data/'fixture.json'
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as stream:json.dump(raw,stream)
    store=Store(settings.database_url);await store.initialize()
    backend=build_sandbox_backend(settings);processes=[];logs=[]
    result={'case':case,'passed':False,'status':'running','provider':backend.provider,
        'sandbox_profile':asdict(profile),'artifact_image_digest':raw['image_digest'],
        'runtime_bundle_digest':settings.runtime_bundle_digest,
        'runtime_code_bundle_digest':settings.runtime_code_bundle_digest,
        'scope':'real_provider_and_runtime_with_process_kill_and_visibility_barriers',
        'database':database,'data_root':str(data),'database_removed':False,
        'policy':{'lease_seconds':30,'recovery_seconds':90,'provision_ttl_seconds':300}}
    started=time.monotonic()
    def phase(value):
        result['phase']=value;result['elapsed_seconds']=round(time.monotonic()-started,3)
        progress(dict(result))
    def launch(role):
        log=(data/(role+'-'+uuid.uuid4().hex[:6]+'.log')).open('w');logs.append(log)
        process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--fixture',str(path),'--'+role],
            cwd=ROOT,env={**os.environ,'TMPDIR':str(data/'tmp'),'PYTHONDONTWRITEBYTECODE':'1'},
            stdout=log,stderr=subprocess.STDOUT)
        processes.append(process);return process
    async def first_row():
        async with store.connections() as db:
            return (await db.scalars(select(SandboxAllocationRow).where(
                SandboxAllocationRow.job_id==job.id,SandboxAllocationRow.job_generation==1))).first()
    async def terminal():
        current=await store.get_job(job.id)
        return current if current.state in {JobState.SUCCEEDED,JobState.FAILED,JobState.UNKNOWN,JobState.CANCELLED} else None
    async def old_released():
        row=await first_row()
        return row and row.state=='RELEASED'
    try:
        job=await create_job(store,ObjectStore(data/'objects'),profile,raw['image_digest'],case,360)
        result['job_id']=job.id;original=job.id+'-g1'
        point='before_rpc' if case=='AR01' else 'after_rpc'
        (data/'allocation-barrier.json').write_text(json.dumps({'point':point,'allocation_id':original}))
        if case in {'AR03','AR04'}:
            (data/'hold-allocation-visibility.json').write_text(json.dumps({'allocation_id':original}))
        if case=='AR03':(data/'hold-runtime-request.json').write_text(json.dumps({'job_id':job.id}))
        worker=launch('worker');allocator=launch('allocator');phase('waiting_for_real_create_boundary')
        async def barrier_reached():
            return any(v.get('event')=='allocator_barrier' and v.get('point')==point
                and v.get('allocation_id')==original for v in audit_rows(data))
        await wait_until(barrier_reached,120)
        row=await first_row();assert row.state=='SENDING' and row.handle_json=='{}'
        allocator.kill();await asyncio.to_thread(allocator.wait)
        assert allocator.returncode==-9
        result['killed_allocator_pid']=allocator.pid
        result['provider_response_received_before_kill']=case!='AR01'
        (data/'allocation-barrier.json').unlink()
        if case=='AR04':
            worker.kill();await asyncio.to_thread(worker.wait)
            result['killed_job_worker_pid']=worker.pid
            raw['worker_generation']+=1;path.write_text(json.dumps(raw));path.chmod(0o600)
            worker=launch('worker')
        allocator=launch('allocator');phase('recovering_expired_claim_without_create_replay')
        if case=='AR03':
            async def submitted():return any(v['event']=='runtime_submission_intent' for v in audit_rows(data))
            await wait_until(submitted,150)
            phase('replacement_intent_persisted_retiring_original_before_runtime_call')
            (data/'hold-allocation-visibility.json').unlink()
            await wait_until(old_released,90)
            (data/'hold-runtime-request.json').unlink()
        final=await wait_until(terminal,180)
        result['job_state']=final.state.value;result['error_code']=final.result.get('error_code')
        if case=='AR04':
            assert final.state==JobState.FAILED
            assert final.result.get('error_code')=='allocation_replacement_capacity_unavailable'
            (data/'hold-allocation-visibility.json').unlink()
        else:
            assert final.state==JobState.SUCCEEDED
            artifacts=await store.list_job_artifacts(job.id,'rendered')
            assert len(artifacts)==1
            content=ObjectStore(data/'objects').get(artifacts[0].object_ref,maximum=1024*1024)
            with Image.open(io.BytesIO(content)) as output:
                assert output.size==(8,8)
                result['output_dimensions']=list(output.size)
        phase('confirming_owned_cleanup_and_submission_counts')
        if case!='AR01':await wait_until(old_released,90)
        async def cleaned():
            async with store.connections() as db:
                rows=(await db.scalars(select(ResourceReservationRow).where(ResourceReservationRow.job_id==job.id,
                    ResourceReservationRow.state!='released'))).all()
                expected=1 if case=='AR01' else 0
                return len(rows)==expected
        await wait_until(cleaned,90)
        values=audit_rows(data)
        result['create_rpc_count']=sum(v['event']=='create_requested' for v in values)
        result['provider_created_count']=sum(v['event']=='created' for v in values)
        result['runtime_submissions']=sum(v['event']=='runtime_submitted' for v in values)
        assert result['create_rpc_count']==(2 if case=='AR03' else 1)
        assert result['runtime_submissions']==(0 if case=='AR04' else 1)
        assert all(v['ttl_seconds']==300 for v in values if v['event']=='create_requested')
        async with store.connections() as db:
            source=await db.get(JobRow,job.id)
            result['allocation_replacements']=source.allocation_replacements
            assert source.allocation_replacements==(0 if case=='AR02' else 1)
            rows=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==job.id))).all()
            result['cleanups']=[{'generation':r.generation,'state':r.state,'phase':r.phase} for r in rows]
        if case=='AR01':
            original_row=await first_row();assert original_row.state=='UNKNOWN'
            assert not any(v.get('allocation_id')==original and v['event']=='create_requested' for v in values)
            result['retained_unknown_expected']=original
        result['passed']=True
    except Exception as exc:
        result['error_type']=type(exc).__name__
    finally:
        for process in processes:
            if process.poll() is None:process.kill();await asyncio.to_thread(process.wait)
        for log in logs:log.close()
        remaining=await backend.list_owned(SandboxScope(settings.namespace))
        result['remaining_owned']=[v.id for v in remaining]
        result['unresolved_allocations']=[list(v) for v in sorted(await store.active_sandbox_allocations())]
        await store.close();await backend.aclose()
        if not remaining and not result['unresolved_allocations']:
            async with admin.connect() as db:await db.execute(text('DROP DATABASE "'+database+'"'))
            result['database_removed']=True;path.unlink()
        await admin.dispose()
        result['passed']=result['passed'] and not remaining
        result['status']='passed' if result['passed'] else 'failed'
        result['elapsed_seconds']=round(time.monotonic()-started,3)
        progress(result)
    return result


async def run(args):
    selected=args.cases.split(',')
    if set(selected)-set(CASES) or len(set(selected))!=len(selected):raise ValueError('invalid_cases')
    _,_,profile=fixture(args.fixture)
    run_root=ROOT/'.data/evidence/allocation-recovery'/('run-'+uuid.uuid4().hex[:12]);run_root.mkdir(parents=True)
    files=['src/sandbox_harness/'+name+'.py' for name in ('allocation_dispatch','allocation_service','job_worker',
        'job_allocation','job_adapters','store','pool','job_service','harness_job','media_render_adapter','sandbox_contracts')]
    files+=['tests/run_real_allocation_recovery.py','tests/run_real_sandbox_capacity.py']
    report={'revision':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'base_fixture':str(Path(args.fixture).resolve()),
        'working_tree_dirty':bool(subprocess.check_output(['git','status','--porcelain'],text=True)),
        'source_sha256':{p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in files},
        'timestamp':datetime.now(timezone.utc).isoformat(),'cases':{},'selected_cases':selected,'run_root':str(run_root)}
    def progress(value):
        report['cases'][value['case']]=value
        args.report.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({k:value.get(k) for k in ('case','status','phase','passed','elapsed_seconds','error_type')}),flush=True)
    for case in selected:
        value=await run_case(case,args.fixture,profile,run_root,progress)
        if not value['passed']:break
    report['passed']=set(report['cases'])==set(selected) and all(v['passed'] for v in report['cases'].values())
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    return report


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--fixture',required=True)
    parser.add_argument('--worker',action='store_true');parser.add_argument('--allocator',action='store_true')
    parser.add_argument('--cases',default=','.join(CASES));parser.add_argument('--report',type=Path)
    args=parser.parse_args()
    if args.worker:asyncio.run(worker_main(args.fixture));return
    if args.allocator:asyncio.run(allocator_main(args.fixture));return
    if not args.report:parser.error('--report required')
    args.report.parent.mkdir(parents=True,exist_ok=True)
    result=asyncio.run(run(args))
    if not result['passed']:raise SystemExit(1)


if __name__=='__main__':main()
