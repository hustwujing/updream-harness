#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import shlex
import signal
import subprocess
import time
from types import SimpleNamespace
import uuid

import httpx
from sqlalchemy import func, select

import run_real_parallel_protocol as protocol
from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.contracts.domain import MediaDraftCreate, now
from sandbox_harness.common.db.accounts import AccountsRepository
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.cross_domain import CrossDomainCommands
from sandbox_harness.common.db.media_models import MediaSourcePermitRow
from sandbox_harness.common.db.media_repository import MediaRepository
from sandbox_harness.common.db.models import (
    JobArtifactRow, JobAttemptRow, JobRow, MediaDraftRow, MediaJobOutputRow,
    MediaJobRow, SiteDeployJobRow, SiteDraftFileRow, SiteDraftRow, SiteFileRow,
    SiteProjectRow, SiteVersionRow, ThreadRow, UserRow,
)
from sandbox_harness.common.db.site_repository import SiteRepository
from sandbox_harness.common.sites.site_bundle import build_site_archive_from_files
from sandbox_harness.services.api.app import object_store


ROOT=Path(__file__).resolve().parents[1]
EVIDENCE_ROOT=ROOT/'.data/evidence/real-campaign'


@dataclass
class Repositories:
    accounts: AccountsRepository
    core: CoreRepository
    media: MediaRepository
    sites: SiteRepository

    @classmethod
    def live(cls, settings):
        databases=DatabaseSettings().for_service("api")
        return cls(AccountsRepository(databases["accounts"]),
            CoreRepository(databases["core"],settings.redis_url,settings.redis_prefix,
                namespace=settings.namespace),
            MediaRepository(databases["media"]),SiteRepository(databases["sites"]))

    @property
    def commands(self):
        return CrossDomainCommands(self.core,self.media,self.sites)

    async def close(self):
        for repository in (self.sites,self.media,self.core,self.accounts):
            await repository.close()


async def wait_until(check, timeout:float, label:str, interval:float=.25):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=await check()
        if value:return value
        await asyncio.sleep(interval)
    raise AssertionError(label+'_timeout')


async def create_thread(client,args,identity):
    response=await client.post('api/threads',json={'harness':args.harness,'model':args.model,
        'reasoning_level':args.reasoning,'title':'Cross database '+identity})
    protocol.require(response);return response.json()


async def submit_source_job(client,args,identity,spec):
    thread,turn=await protocol.origin(client,args,identity)
    response=await protocol.prepare(client,thread,turn,[spec],identity,suppress_continuation=True)
    protocol.require(response);value=response.json()
    if value.get('mode')=='draft':
        response=await client.post(f"api/job-group-drafts/{value['draft']['id']}/confirm")
        protocol.require(response);value=response.json()
    jobs=value.get('jobs') or []
    if len(jobs)!=1:raise AssertionError('source_job_count_invalid')
    return thread,turn,jobs[0]


async def source_snapshot(repositories:Repositories,job_id:str):
    async with repositories.core.connections() as db:
        job=await db.get(JobRow,job_id)
        attempts=list((await db.scalars(select(JobAttemptRow).where(
            JobAttemptRow.job_id==job_id).order_by(JobAttemptRow.generation))).all())
        artifacts=list((await db.scalars(select(JobArtifactRow).where(
            JobArtifactRow.job_id==job_id).order_by(JobArtifactRow.position))).all())
    async with repositories.media.connections() as db:
        media=await db.scalar(select(MediaJobRow).where(MediaJobRow.source_job_id==job_id))
        permit=await db.scalar(select(MediaSourcePermitRow).where(
            MediaSourcePermitRow.source_job_id==job_id))
        outputs=[] if not media else list((await db.scalars(select(MediaJobOutputRow).where(
            MediaJobOutputRow.job_id==media.id).order_by(MediaJobOutputRow.created_at,
                MediaJobOutputRow.filename))).all())
    return SimpleNamespace(job=job,attempts=attempts,artifacts=artifacts,media=media,
                           permit=permit,outputs=outputs)


def output_identity(row):
    return (row.filename,row.object_ref,row.sha256,row.size,row.mime)


async def verify_source_outputs(repositories:Repositories,settings,job_id:str,*,minimum:int=1):
    snapshot=await source_snapshot(repositories,job_id)
    if not snapshot.media or not snapshot.permit:raise AssertionError('media_authority_missing')
    if snapshot.permit.media_job_id!=snapshot.media.id:
        raise AssertionError('media_permit_job_identity_mismatch')
    if len(snapshot.outputs)<minimum or len(snapshot.artifacts)!=len(snapshot.outputs):
        raise AssertionError('cross_database_output_count_mismatch')
    media={row.role:output_identity(row) for row in snapshot.outputs}
    core={row.item_key:output_identity(row) for row in snapshot.artifacts}
    if media!=core:raise AssertionError('cross_database_output_identity_mismatch')
    objects=object_store(settings)
    verified=[]
    for row in snapshot.artifacts:
        data=await asyncio.to_thread(objects.get,row.object_ref,maximum=row.size)
        if len(data)!=row.size or hashlib.sha256(data).hexdigest()!=row.sha256:
            raise AssertionError('object_content_identity_mismatch')
        verified.append({'role':row.item_key,'position':row.position,'filename':row.filename,
            'sha256':row.sha256,'size':row.size,'mime':row.mime,'object_ref':row.object_ref})
    return snapshot,verified


async def case_xdb01(client,args,repositories,settings):
    thread,_,job=await submit_source_job(client,args,'XDB01',protocol.provider_job(1,
        prompt='A clean green square centered on a white background'))
    completed=await protocol.wait_job(client,job['id'],args.timeout)
    if completed['state']!='SUCCEEDED':raise AssertionError('source_job_not_succeeded')
    snapshot,outputs=await verify_source_outputs(repositories,settings,job['id'])
    accepted=[row for row in snapshot.attempts if row.accepted_at is not None]
    if len({row.external_execution_id for row in accepted})!=1:
        raise AssertionError('source_external_identity_mismatch')
    return {'classification':'success','thread_id':thread['id'],'job_id':job['id'],
        'media_job_id':snapshot.media.id,'permit_state':snapshot.permit.state,
        'output_count':len(outputs),'outputs':outputs}


async def run_command(command:list[str],label:str):
    result=await asyncio.to_thread(subprocess.run,command,cwd=ROOT,text=True,
        stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    if result.returncode:raise AssertionError(label+'_failed')


async def kill_service_main(unit:str,start_command:str):
    await run_command(['systemctl','kill','--kill-who=main','--signal='+signal.Signals.SIGKILL.name,
        unit],'job_worker_sigkill')
    async def stopped():
        result=await asyncio.to_thread(subprocess.run,['systemctl','is-active','--quiet',unit])
        return result.returncode!=0
    await wait_until(stopped,30,'job_worker_stop')
    command=shlex.split(start_command) if start_command else ['systemctl','start',unit]
    await run_command(command,'job_worker_start')
    async def active():
        result=await asyncio.to_thread(subprocess.run,['systemctl','is-active','--quiet',unit])
        return result.returncode==0
    await wait_until(active,120,'job_worker_active',1)


async def case_xdb02(client,args,repositories,settings):
    thread,_,job=await submit_source_job(client,args,'XDB02',protocol.provider_job(2,
        prompt='A simple blue circle centered on a light gray background'))
    async def accepted():
        value=await source_snapshot(repositories,job['id'])
        accepted_attempts=[row for row in value.attempts if row.accepted_at is not None]
        return value if (accepted_attempts and value.media and
            value.media.provider_started_at is not None) else None
    before=await wait_until(accepted,min(args.timeout,300),'media_acceptance')
    accepted_before=[row for row in before.attempts if row.accepted_at is not None]
    external_before={row.external_execution_id for row in accepted_before}
    provider_job_id=before.media.provider_job_id
    await kill_service_main(args.job_worker_unit,args.job_worker_start_command)
    completed=await protocol.wait_job(client,job['id'],args.timeout)
    if completed['state']!='SUCCEEDED':raise AssertionError('recovered_source_job_not_succeeded')
    after,outputs=await verify_source_outputs(repositories,settings,job['id'])
    accepted_after=[row for row in after.attempts if row.accepted_at is not None]
    external_after={row.external_execution_id for row in accepted_after}
    if external_after!=external_before or len(external_after)!=1:
        raise AssertionError('recovery_external_execution_changed')
    recovery_generation=max(row.generation for row in after.attempts)
    await asyncio.sleep(2)
    stable=await source_snapshot(repositories,job['id'])
    same_generation=max(row.generation for row in stable.attempts)==recovery_generation
    same_external_execution={row.external_execution_id for row in stable.attempts
        if row.accepted_at is not None}==external_after
    if not same_generation or not same_external_execution or len(after.attempts)>2:
        raise AssertionError('recovery_generation_or_execution_churn')
    return {'classification':'success','thread_id':thread['id'],'job_id':job['id'],
        'media_job_id':after.media.id,'provider_job_id':provider_job_id,
        'attempt_generations':[row.generation for row in after.attempts],
        'same_generation':same_generation,'same_external_execution':same_external_execution,
        'output_count':len(outputs)}


async def case_xdb03(client,args,repositories,settings):
    thread,turn=await protocol.origin(client,args,'XDB03')
    asset=await protocol.register_case_asset(thread['id'],turn['id'],
        ROOT/'tests/real_cases/assets/poster.png')
    response=await protocol.prepare(client,thread,turn,[protocol.decompose_job(asset.id)],'XDB03',
        suppress_continuation=True);protocol.require(response);value=response.json()
    if value.get('mode')=='draft':
        response=await client.post(f"api/job-group-drafts/{value['draft']['id']}/confirm")
        protocol.require(response);value=response.json()
    job=value['jobs'][0];completed=await protocol.wait_job(client,job['id'],args.timeout)
    if completed['state']!='SUCCEEDED':raise AssertionError('multi_output_job_not_succeeded')
    snapshot,outputs=await verify_source_outputs(repositories,settings,job['id'],minimum=2)
    if [row.position for row in snapshot.artifacts]!=list(range(len(snapshot.artifacts))):
        raise AssertionError('core_output_positions_not_contiguous')
    return {'classification':'success','thread_id':thread['id'],'job_id':job['id'],
        'media_job_id':snapshot.media.id,'output_count':len(outputs),'outputs':outputs,
        'ordered_identity':True}


async def case_xdb04(client,args,repositories,settings):
    thread,_,job=await submit_source_job(client,args,'XDB04',protocol.provider_job(4,
        prompt='A red triangle centered on a pale background'))
    async def authorized():
        value=await source_snapshot(repositories,job['id'])
        return value if value.permit and value.permit.state in {'AUTHORIZED','CONSUMED'} else None
    await wait_until(authorized,min(args.timeout,300),'media_authorization')
    response=await client.post(f"api/jobs/{job['id']}/cancel");protocol.require(response)
    terminal=await protocol.wait_job(client,job['id'],args.timeout)
    snapshot=await source_snapshot(repositories,job['id'])
    if terminal['state']!='CANCELLED' or snapshot.permit.state!='CANCELLED':
        raise AssertionError('cross_database_cancellation_not_terminal')
    if snapshot.artifacts:raise AssertionError('cancelled_output_promoted_to_core')
    return {'classification':'success','thread_id':thread['id'],'job_id':job['id'],
        'media_job_id':snapshot.media.id,'core_state':terminal['state'],
        'permit_state':snapshot.permit.state,'late_media_output_count':len(snapshot.outputs),
        'core_artifact_count':0}


async def stage_site(repositories,settings,owner_id,thread_id,identity):
    files={'index.html':f'<!doctype html><h1>{identity}</h1><script src="app.js"></script>'.encode(),
           'app.js':b'document.body.dataset.crossDatabase="verified"'}
    archive,members=build_site_archive_from_files(files);objects=object_store(settings)
    archive_ref,archive_digest=await asyncio.to_thread(objects.put,archive)
    staged=[]
    for member in members:
        reference,digest=await asyncio.to_thread(objects.put,member.data)
        if digest!=member.sha256:raise AssertionError('site_staging_digest_mismatch')
        staged.append({'path':member.path,'sha256':member.sha256,'size':member.size,
            'mime':member.mime,'object_ref':reference})
    draft,_=await repositories.commands.create_site_draft(owner_id,thread_id,'',name=identity,
        slug=identity.lower()+'-'+uuid.uuid4().hex[:8],idempotency_key='site-'+identity+'-'+uuid.uuid4().hex,
        archive_object_ref=archive_ref,bundle_sha256=archive_digest,archive_size=len(archive),
        file_count=len(staged),total_size=sum(row['size'] for row in staged),
        expires_at=now()+timedelta(hours=1),public_base_path='/sites',files=staged)
    return draft,staged,archive_digest


async def case_xdb05(client,args,repositories,settings):
    thread=await create_thread(client,args,'XDB05');core_thread=await repositories.core.get_thread(thread['id'])
    draft,staged,bundle_digest=await stage_site(repositories,settings,core_thread.owner_id,
        thread['id'],'XDB05')
    response=await client.post(f'api/sites/drafts/{draft.id}/confirm');protocol.require(response)
    deployment=response.json()
    async def deployed():
        response=await client.get(f"api/sites/jobs/{deployment['id']}");protocol.require(response)
        value=response.json();return value if value['status'] in {'succeeded','failed'} else None
    job=await wait_until(deployed,args.timeout,'site_deployment',1)
    if job['status']!='succeeded':raise AssertionError('site_deployment_not_succeeded')
    async with repositories.sites.connections() as db:
        project=await db.scalar(select(SiteProjectRow).where(SiteProjectRow.thread_id==thread['id']))
        version=await db.get(SiteVersionRow,project.current_version_id)
        files=list((await db.scalars(select(SiteFileRow).where(
            SiteFileRow.version_id==version.id).order_by(SiteFileRow.path))).all())
    wanted=sorted((row['path'],row['sha256'],row['size'],row['mime'],row['object_ref']) for row in staged)
    actual=[(row.path,row.sha256,row.size,row.mime,row.object_ref) for row in files]
    if (project.status!='active' or version.bundle_sha256!=bundle_digest or actual!=wanted
            or version.file_count!=len(staged)):
        raise AssertionError('site_manifest_identity_mismatch')
    public=await client.get(project.public_url);public.raise_for_status()
    return {'classification':'success','thread_id':thread['id'],'draft_id':draft.id,
        'site_job_id':job['id'],'project_id':project.id,'version_id':version.id,
        'version':version.version,'bundle_sha256':version.bundle_sha256,
        'file_count':len(files),'public_url':project.public_url}


async def case_xdb06(client,args,repositories,settings):
    username='xdb06.'+uuid.uuid4().hex[:16]
    response=await client.post('api/auth/register',json={'username':username,
        'password':'xdb06-'+uuid.uuid4().hex});protocol.require(response)
    thread=await create_thread(client,args,'XDB06')
    core_thread=await repositories.core.get_thread(thread['id'])
    account=await repositories.accounts.get_user_by_username(username)
    if not account or account[0].owner_id!=core_thread.owner_id:
        raise AssertionError('account_core_owner_identity_mismatch')
    response=await client.post(f"api/threads/{thread['id']}/media/drafts",json={
        'kind':'image','model':'seedream-5.0','generation_mode':'text_to_image',
        'parameters':{'prompt':'lifecycle fixture'},'references':[],
        'idempotency_key':'xdb06-media-'+uuid.uuid4().hex,'turn_id':'','output_role':'candidate'})
    protocol.require(response);media_draft=response.json()
    response=await client.post(f"api/media/drafts/{media_draft['id']}/cancel");protocol.require(response)
    site_draft,_,_=await stage_site(repositories,settings,core_thread.owner_id,thread['id'],'XDB06')
    response=await client.post(f'api/sites/drafts/{site_draft.id}/cancel');protocol.require(response)
    response=await client.delete(f"api/threads/{thread['id']}")
    if response.status_code not in {202,204}:protocol.require(response)
    if response.status_code==202:
        operation_id=response.json()['operation_id']
        async def lifecycle_done():
            value=await client.get(f'api/lifecycle/{operation_id}');protocol.require(value)
            body=value.json();return body if body['state'] in {'completed','refused'} else None
        lifecycle=await wait_until(lifecycle_done,args.timeout,'thread_lifecycle',.5)
        if lifecycle['state']!='completed':raise AssertionError('thread_delete_refused')
    async def cleaned():
        async with repositories.core.connections() as db:
            core_count=await db.scalar(select(func.count()).select_from(ThreadRow).where(
                ThreadRow.id==thread['id']))
        async with repositories.media.connections() as db:
            media_count=await db.scalar(select(func.count()).select_from(MediaDraftRow).where(
                MediaDraftRow.thread_id==thread['id']))
        async with repositories.sites.connections() as db:
            site_count=await db.scalar(select(func.count()).select_from(SiteDraftRow).where(
                SiteDraftRow.thread_id==thread['id']))
        return (core_count,media_count,site_count) if not any(
            (core_count,media_count,site_count)) else None
    counts=await wait_until(cleaned,args.timeout,'cross_database_cleanup',.5)
    async with repositories.accounts.connections() as db:
        owner_count=await db.scalar(select(func.count()).select_from(UserRow).where(
            UserRow.owner_id==core_thread.owner_id,UserRow.username==username))
    if owner_count!=1:raise AssertionError('account_principal_lost_during_thread_delete')
    return {'classification':'success','thread_id':thread['id'],'username':username,
        'owner_identity_preserved':True,'core_rows':counts[0],'media_rows':counts[1],
        'site_rows':counts[2],'orphan_count':sum(counts)}


HANDLERS={"XDB01":case_xdb01,"XDB02":case_xdb02,"XDB03":case_xdb03,
          "XDB04":case_xdb04,"XDB05":case_xdb05,"XDB06":case_xdb06}


async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--base-url',default='http://127.0.0.1:24766/')
    parser.add_argument('--case',required=True,choices=sorted(HANDLERS))
    parser.add_argument('--report',type=Path,default=EVIDENCE_ROOT/'cross-database.json')
    parser.add_argument('--timeout',type=int,default=1800)
    parser.add_argument('--harness',default='codex');parser.add_argument('--model',default='gpt-5.6-sol')
    parser.add_argument('--reasoning',default='deep')
    parser.add_argument('--job-worker-unit',default='sandbox-harness-parallel-ali-job-worker.service')
    parser.add_argument('--job-worker-start-command',default='')
    args=parser.parse_args();settings=protocol.load_platform_settings()
    repositories=Repositories.live(settings);started=time.monotonic()
    try:
        async with protocol.ResilientAsyncClient(base_url=args.base_url,timeout=None,
                trust_env=False) as client:
            session=await client.get('api/session');protocol.require(session)
            value=await HANDLERS[args.case](client,args,repositories,settings)
    except AssertionError as exc:
        value={'classification':'program_failure','reason':str(exc)[:200]}
    except protocol.JobOutcomeFailure as exc:
        value=exc.outcome
    except Exception as exc:
        value={'classification':'program_failure','reason':type(exc).__name__}
    finally:
        await repositories.close()
    value.update(case_id=args.case,elapsed_seconds=round(time.monotonic()-started,3))
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(value,ensure_ascii=False),flush=True)
    if value['classification'] in {'program_failure','optimizable_prompt'}:raise SystemExit(2)


if __name__=='__main__':asyncio.run(main())
