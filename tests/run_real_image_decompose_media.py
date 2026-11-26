#!/usr/bin/env python3
"""Real inference + isolated platform DB/Workers; no fake provider responses.

DM01 drops one real artifact response. DM02 SIGKILLs its own MediaWorker after
the provider ID commits. DM03 cancels an accepted real job. DM04 drops a real
create response and verifies UNKNOWN/no replay before owned operator cleanup.
"""
from __future__ import annotations

import argparse
import asyncio
from contextvars import ContextVar
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
import zipfile

import httpx
from PIL import Image
from sqlalchemy import select

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate, TurnState)
from sandbox_harness.services.job_worker.image_decompose_adapter import (ImageDecomposeAdapter)
from sandbox_harness.services.media_worker.image_decompose_provider import (ImageDecomposeProvider)
from sandbox_harness.common.contracts.job_contracts import (JobArtifactInput, JobExpectedOutput, JobGroupCreate, JobRequest)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from sandbox_harness.common.jobs.media_authority import MediaAuthorityClient
from sandbox_harness.services.media_worker.media_worker import (MediaWorker)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.media_repository import MediaRepository
from sandbox_harness.common.db.models import (MediaJobRow, MediaOutboxRow)

ROOT=Path(__file__).resolve().parents[1]
CASES=('DM01','DM02','DM03','DM04')
CURRENT_JOB=ContextVar('media_test_job',default='')


def write_json(path,value):
    temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(value,indent=2)+'\n')
    temporary.replace(path)


def audit(root,event,**fields):
    with (root/'audit.jsonl').open('a') as stream:
        stream.write(json.dumps({'time':time.time(),'event':event,**fields})+'\n');stream.flush()


def events(root):
    path=root/'audit.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


class FaultTransport(httpx.AsyncBaseTransport):
    def __init__(self,root):
        self.root=root;self.inner=httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self,request):
        creating=request.method=='POST' and request.url.path=='/api/jobs'
        if creating:audit(self.root,'create_requested',media_job_id=CURRENT_JOB.get())
        response=await self.inner.handle_async_request(request)
        flags=json.loads((self.root/'faults.json').read_text())
        if creating and response.status_code==202:
            body=await response.aread();identity=json.loads(body)['id']
            audit(self.root,'native_accepted',media_job_id=CURRENT_JOB.get(),native_id=identity)
            if flags.get('drop_create'):
                await response.aclose();audit(self.root,'create_response_dropped',native_id=identity)
                raise httpx.ReadError('injected_real_create_response_loss',request=request)
        if request.method=='GET' and '/artifacts/' in request.url.path:
            identity=request.url.path.rsplit('/',1)[-1]
            audit(self.root,'artifact_requested',artifact_id=identity)
            requests=[row for row in events(self.root) if row['event']=='artifact_requested']
            if (flags.get('drop_download') and len(requests)==2
                    and not any(row['event']=='artifact_response_dropped' for row in events(self.root))):
                await response.aread();await response.aclose()
                audit(self.root,'artifact_response_dropped',artifact_id=identity)
                raise httpx.ReadError('injected_real_artifact_response_loss',request=request)
        return response

    async def aclose(self):await self.inner.aclose()


class AuditedProvider(ImageDecomposeProvider):
    def __init__(self,settings,objects):
        super().__init__(settings,objects);self.test_root=settings.data_root

    def _client(self):
        headers={'Cookie':self.cookie_path.read_text().strip()} if self.cookie_path.is_file() else {}
        return httpx.AsyncClient(base_url=self.base_url,headers=headers,timeout=120,
            trust_env=False,follow_redirects=False,transport=FaultTransport(self.test_root))

    async def submit(self,job):
        token=CURRENT_JOB.set(job['id'])
        try:return await super().submit(job)
        finally:CURRENT_JOB.reset(token)


async def worker_main(fixture):
    value=json.loads(fixture.read_text());settings=Settings.model_validate(value['settings'])
    store=MediaRepository(value['media_database_url']);await store.initialize()
    objects=ObjectStore(settings.data_root/'objects')
    provider=AuditedProvider(settings,objects)
    worker=MediaWorker(settings,store,objects,None,external_providers={'image_layered_http':provider})
    update=store.update_media_job
    async def held_update(*args,**kwargs):
        result=await update(*args,**kwargs)
        if kwargs.get('provider_job_id') and json.loads((settings.data_root/'faults.json').read_text()).get('hold_acceptance'):
            audit(settings.data_root,'provider_id_committed',media_job_id=args[0],
                native_id=kwargs['provider_job_id'],generation=args[2],worker_id=worker.owner)
            while True:await asyncio.sleep(.2)
        return result
    store.update_media_job=held_update
    for sig in (signal.SIGINT,signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig,worker.stop_event.set)
    audit(settings.data_root,'worker_started',pid=os.getpid(),worker_id=worker.owner)
    try:await worker.run()
    finally:await store.release_media_job_leases(worker.owner);await store.close()


async def eventually(predicate,*,timeout=1800):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=await predicate()
        if value:return value
        await asyncio.sleep(.5)
    raise TimeoutError('real_media_case_deadline')


class Campaign:
    def __init__(self,args):
        self.args=args;self.root=args.report.parent;self.running={};self.results={};self.started=0
        self.revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
        paths=[*sorted((ROOT/'src/sandbox_harness').glob('*.py')),Path(__file__)]
        self.source_digest=hashlib.sha256(b''.join(
            str(path.relative_to(ROOT)).encode()+b'\0'+path.read_bytes() for path in paths)).hexdigest()

    def save(self):
        elapsed=[row['elapsed_seconds'] for row in self.results.values()]
        classifications=[row['classification'] for row in self.results.values()]
        write_json(self.args.report,{'revision':self.revision,'source_digest':self.source_digest,
            'verification_scope':'real_inference_isolated_platform_fixture',
            'updated_at':datetime.now(timezone.utc).isoformat(),'total':len(self.args.case),
            'started':self.started,'running':self.running,'results':self.results,
            'summary':{'success':classifications.count('success'),
                'program_failure':classifications.count('program_failure'),
                'non_program_failure':classifications.count('non_program_failure'),
                'unrun':len(self.args.case)-self.started,
                'average_seconds':sum(elapsed)/len(elapsed) if elapsed else None}})

    async def case(self,identity):
        started=time.monotonic();self.started+=1
        self.running[identity]={'stage':'preparing','expected':True};self.save()
        root=self.root/identity;root.mkdir(parents=True,exist_ok=False)
        core_url='sqlite+aiosqlite:///'+str(root/'core.db')
        media_url='sqlite+aiosqlite:///'+str(root/'media.db')
        settings=Settings(backend='fake',data_root=root,database_url=core_url,
            media_job_lease_seconds=10,image_decompose_base_url=self.args.service_url)
        fixture=root/'settings.json';write_json(fixture,{
            'settings':settings.model_dump(mode='json',include={
                'backend','data_root','database_url','media_job_lease_seconds','image_decompose_base_url'}),
            'media_database_url':media_url})
        flags={'drop_download':identity=='DM01','hold_acceptance':identity=='DM02',
            'drop_create':identity=='DM04'};write_json(root/'faults.json',flags)
        store=CoreRepository(core_url,namespace='image-decompose-real')
        media_store=MediaRepository(media_url)
        await store.initialize();await media_store.initialize();objects=ObjectStore(root/'objects')
        child=None;job_task=None;job_worker=None;native_ids=[];cleanup=[]
        async def spawn():
            with (root/'media-worker.log').open('ab') as log:
                return await asyncio.create_subprocess_exec(sys.executable,str(Path(__file__).resolve()),
                    '--worker',str(fixture),cwd=ROOT,env={**os.environ,'TMPDIR':str(ROOT/'.data/tmp'),
                    'PYTHONDONTWRITEBYTECODE':'1','XDG_CACHE_HOME':str(ROOT/'.data/cache')},
                    stdout=log,stderr=asyncio.subprocess.STDOUT)
        async def native_receipt():
            return next((row for row in events(root) if row['event']=='native_accepted'),None)
        try:
            profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
            thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
                title=identity+' real image media'),profile,'real-media-test')
            turn,_,_=await store.create_turn(thread.id,TurnCreate(message='real media protocol fixture',
                idempotency_key='fixture-origin'),profile,'real-media-test')
            await store.set_turn_state(turn.id,TurnState.COMPLETED)
            data=(ROOT/'tests/real_cases/assets/portrait.png').read_bytes();ref,digest=objects.put(data)
            source=await store.add_artifact(turn.id,path='source.png',sha256=digest,size=len(data),
                mime='image/png',object_ref=ref)
            request=JobRequest(local_job_id='decompose',kind='operation',adapter='image.decompose',
                descriptor_id='image.decompose',descriptor_version='1',descriptor_digest='a'*64,
                idempotency_key='real-media-'+uuid.uuid4().hex,timeout_seconds=self.args.timeout,
                parameters={'layer_count':3,'resolution':640,'quality':'balanced','seed':777,
                    'prompt':'Separate the subject and background into editable image layers.'},
                input_artifacts=(JobArtifactInput(artifact_id=source.id,artifact_role='source'),),
                expected_outputs=(JobExpectedOutput(artifact_role='layer_package',
                    output_disposition='delivery',min_items=2,max_items=30,
                    mime_patterns=('image/png','image/openraster')),),
                cancellation_policy={'supported':True},reconciliation_policy={'supported':True})
            group,jobs=await store.create_job_group('real-media-test',JobGroupCreate(thread_id=thread.id,
                origin_turn_id=turn.id,idempotency_key='real-media-group',jobs=(request,)))
            job=jobs[0];self.running[identity].update(job_id=job.id,stage='submitting');self.save()
            media_authority=MediaAuthorityClient(store,media_store)
            job_worker=JobWorker(store,{'image.decompose':ImageDecomposeAdapter(
                settings,store,objects,media_authority)},
                node_id='isolated-media-test',lease_seconds=30,poll_seconds=.2)
            job_task=asyncio.create_task(job_worker.run());child=await spawn()
            receipt=await eventually(native_receipt,timeout=90)
            native_ids=[receipt['native_id']];media_id=receipt['media_job_id']
            self.running[identity].update(native_id=native_ids[0],media_job_id=media_id,stage='native inference')
            self.save();recovery={}
            if identity=='DM02':
                async def committed():
                    return next((row for row in events(root) if row['event']=='provider_id_committed'),None)
                held=await eventually(committed,timeout=30)
                child.kill();await child.wait();flags['hold_acceptance']=False
                write_json(root/'faults.json',flags);child=await spawn()
                recovery={'killed_worker_id':held['worker_id'],'old_generation':held['generation'],
                    'native_id_before':held['native_id']}
                self.running[identity]['stage']='replacement worker waiting for lease then polling same native job'
            elif identity=='DM03':
                await store.request_job_cancel(job.id,'real-media-test')
                self.running[identity]['stage']='confirming native cancellation'
            elif identity=='DM04':
                self.running[identity]['stage']='verifying lost response remains UNKNOWN without replay'
            self.save()
            async def terminal():
                value=await store.get_job(job.id)
                self.running[identity]['job_state']=value.state.value
                media=await media_store.get_media_job(media_id,'real-media-test')
                self.running[identity]['media_phase']=media.phase;self.save()
                return value if value.state.value in {'SUCCEEDED','FAILED','BLOCKED','UNKNOWN','CANCELLED'} else None
            final=await eventually(terminal,timeout=self.args.timeout)
            expected={'DM01':'SUCCEEDED','DM02':'SUCCEEDED','DM03':'CANCELLED','DM04':'UNKNOWN'}[identity]
            assert final.state.value==expected,(final.state.value,final.result.get('error_code'))
            trace=events(root)
            assert sum(row['event']=='create_requested' for row in trace)==1
            assert sum(row['event']=='native_accepted' for row in trace)==1
            if identity=='DM01':assert sum(row['event']=='artifact_response_dropped' for row in trace)==1
            async with media_store.connections() as db:
                media=await db.get(MediaJobRow,media_id)
                if identity=='DM02':
                    assert media.generation>recovery['old_generation']
                    assert media.provider_job_id==recovery['native_id_before']
                    recovery.update(new_generation=media.generation,native_id_after=media.provider_job_id)
                outboxes=list((await db.execute(select(MediaOutboxRow).where(
                    MediaOutboxRow.job_id==media_id))).scalars())
                assert len(outboxes)==1 and media.turn_id==''
            artifacts=await store.list_job_artifacts(job.id)
            if expected=='SUCCEEDED':
                assert len(artifacts)>=2 and [a.position for a in artifacts]==list(range(len(artifacts)))
                for artifact in artifacts:
                    raw=objects.get(artifact.object_ref,maximum=128*1024**2)
                    assert len(raw)==artifact.size and hashlib.sha256(raw).hexdigest()==artifact.sha256
                    path=objects._path(artifact.sha256)
                    if artifact.mime=='image/png':
                        with Image.open(path) as value:value.verify()
                    elif artifact.mime=='image/openraster':
                        with zipfile.ZipFile(path) as value:assert value.testzip() is None
                    else:raise AssertionError('unselected format published')
            else:assert artifacts==[]
            if identity=='DM04':
                await media_store.request_media_cancel(media_id,'real-media-test')
                async def still_unknown():
                    return (await media_store.get_media_job(media_id,'real-media-test')).status=='unknown'
                await eventually(still_unknown,timeout=40)
                assert sum(row['event']=='create_requested' for row in events(root))==1
            outboxes=await store.list_job_outboxes()
            assert len([row for row in outboxes if row.group_id==group.id])==1
            self.results[identity]={'classification':'success','expected_state':expected,
                'actual_state':final.state.value,'job_id':job.id,'media_job_id':media_id,
                'native_ids':native_ids,'create_count':1,'artifact_count':len(artifacts),
                'recovery':recovery,'cleanup':cleanup,'elapsed_seconds':round(time.monotonic()-started,2)}
        except Exception as exc:
            self.results[identity]={'classification':'program_failure','exception_type':type(exc).__name__,
                'reason':str(exc)[:200] if isinstance(exc,AssertionError) else type(exc).__name__,
                'native_ids':native_ids,'cleanup':cleanup,'elapsed_seconds':round(time.monotonic()-started,2)}
        finally:
            if job_worker:job_worker.stop_event.set()
            if job_task:
                job_task.cancel();await asyncio.gather(job_task,return_exceptions=True)
            if child and child.returncode is None:
                child.terminate()
                try:await asyncio.wait_for(child.wait(),10)
                except asyncio.TimeoutError:child.kill();await child.wait()
            # Test-only operator cleanup may use IDs captured by its real transport.
            native_ids=list({row['native_id'] for row in events(root) if row['event']=='native_accepted'})
            provider=ImageDecomposeProvider(settings,objects)
            for native_id in native_ids:
                try:
                    result=await provider.cancel({'provider_job_id':native_id})
                    cleanup.append({'native_id':native_id,'quiescent':result.status=='cancelled'})
                except Exception as exc:cleanup.append({'native_id':native_id,'exception_type':type(exc).__name__})
            await media_store.close();await store.close()
            if any(not row.get('quiescent') for row in cleanup):
                self.results[identity]['classification']='program_failure'
                self.results[identity]['reason']='owned_native_cleanup_unconfirmed'
            self.running.pop(identity,None);self.save()


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--worker',type=Path)
    parser.add_argument('--report',type=Path);parser.add_argument('--case',action='append',choices=CASES)
    parser.add_argument('--service-url',default='http://127.0.0.1:8912')
    parser.add_argument('--timeout',type=int,default=1800);args=parser.parse_args()
    if args.worker:return await worker_main(args.worker)
    if not args.report:parser.error('--report is required')
    args.report=args.report.resolve()
    if not args.report.is_relative_to(Path('/data2/wujing03')):raise ValueError('test_output_root_invalid')
    args.report.parent.mkdir(parents=True,exist_ok=True);args.case=args.case or list(CASES)
    campaign=Campaign(args);campaign.save()
    await asyncio.gather(*(campaign.case(identity) for identity in args.case))
    assert all(row['classification']=='success' for row in campaign.results.values())


if __name__=='__main__':asyncio.run(main())
