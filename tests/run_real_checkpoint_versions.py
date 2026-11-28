from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

import httpx
from sqlalchemy import func,select

from sandbox_harness.common.configuration.config import load_settings
from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.contracts.domain import TERMINAL
from sandbox_harness.common.contracts.job_contracts import TERMINAL_JOB_STATES
from sandbox_harness.common.db.core import CoreRepository
from sandbox_harness.common.db.models import JobRow,TurnRow
from sandbox_harness.common.db.ownership_models import SandboxOwnershipRow
from sandbox_harness.common.contracts.sandbox_contracts import SandboxHandle,SandboxNotFound
from sandbox_harness.common.sandbox.sandbox_factory import build_sandbox_backend
from sandbox_harness.common.storage.objects import object_store


TERMINAL_TURNS={state.value for state in TERMINAL}
PLATFORM_UNIT='sandbox-harness-parallel-ali-platform-worker.service'


async def wait_turn(client,turn_id,timeout=900):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        response=await client.get(f'api/turns/{turn_id}');response.raise_for_status()
        value=response.json()
        if value['state'] in TERMINAL_TURNS:return value
        await asyncio.sleep(.5)
    raise TimeoutError('checkpoint_version_turn_timeout')


async def submit(client,thread_id,message,key):
    response=await client.post(f'api/threads/{thread_id}/turns',json={
        'message':message,'attachments':[],'idempotency_key':key,
        'delivery_mode':'start'})
    response.raise_for_status();return await wait_turn(client,response.json()['id'])


async def remove_thread(client,thread_id):
    for _ in range(240):
        response=await client.delete(f'api/threads/{thread_id}')
        if response.status_code==204:return
        if response.status_code==409:
            await asyncio.sleep(.5);continue
        response.raise_for_status();value=response.json()
        if value.get('state')!='pending':return
        operation=value['operation_id']
        for _ in range(240):
            current=await client.get(f'api/lifecycle/{operation}');current.raise_for_status()
            state=current.json()['state']
            if state=='completed':return
            if state=='refused':raise RuntimeError(current.json().get('error_code'))
            await asyncio.sleep(.5)
    raise TimeoutError('checkpoint_version_cleanup_timeout')


def platform_pid():
    return int(subprocess.check_output(['systemctl','show',PLATFORM_UNIT,
        '-p','MainPID','--value'],text=True).strip() or 0)


def kill_restart_platform():
    old=platform_pid()
    if old<=0:raise RuntimeError('platform_worker_pid_missing')
    os.kill(old,signal.SIGKILL);deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        current=platform_pid()
        if current>0 and current!=old and subprocess.run(
                ['systemctl','is-active','--quiet',PLATFORM_UNIT]).returncode==0:
            return old,current
        time.sleep(.25)
    raise TimeoutError('platform_worker_restart_timeout')


async def active_counts(core,exclude_thread=''):
    async with core.connections() as db:
        turn_query=select(func.count()).select_from(TurnRow).where(
            TurnRow.state.not_in(TERMINAL_TURNS))
        if exclude_thread:turn_query=turn_query.where(TurnRow.thread_id!=exclude_thread)
        turns=int((await db.execute(turn_query)).scalar_one())
        jobs=int((await db.execute(select(func.count()).select_from(JobRow).where(
            JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES])))).scalar_one())
    return turns,jobs


async def force_fresh_sandbox(core,settings,thread_id):
    turns,jobs=await active_counts(core,thread_id)
    if turns or jobs:raise RuntimeError('shared_work_prevents_checkpoint_restart_case')
    async with core.connections() as db:
        row=(await db.scalars(select(SandboxOwnershipRow).where(
            SandboxOwnershipRow.thread_id==thread_id,
            SandboxOwnershipRow.state!='RELEASED',
            SandboxOwnershipRow.handle_json!='').order_by(
                SandboxOwnershipRow.updated_at.desc()).limit(1))).one_or_none()
    if row is None:raise RuntimeError('test_retained_sandbox_missing')
    handle=SandboxHandle.from_mapping(json.loads(row.handle_json));backend=build_sandbox_backend(settings)
    try:
        try:sandbox=await backend.get(handle)
        except SandboxNotFound:pass
        else:await backend.delete(sandbox)
    finally:await backend.aclose()
    old_pid,new_pid=await asyncio.to_thread(kill_restart_platform);await asyncio.sleep(2)
    return {'sandbox_id':handle.sandbox_id,'old_pid':old_pid,'new_pid':new_pid}


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base-url',default='http://127.0.0.1:24767/')
    parser.add_argument('--report',default='.data/evidence/checkpoint-versions-real.json')
    args=parser.parse_args();settings=load_settings();databases=DatabaseSettings().for_service('platform-worker')
    core=CoreRepository(databases['core'],settings.redis_url,settings.redis_prefix,
        namespace=settings.namespace);await core.initialize();objects=object_store(settings)
    thread_id='';results=[]
    async with httpx.AsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
        session=await client.get('api/session');session.raise_for_status()
        try:
            response=await client.post('api/threads',json={'harness':'codex','model':'gpt-5.6',
                'reasoning_level':'standard','title':'Real checkpoint version gate'})
            response.raise_for_status();thread_id=response.json()['id']
            first=await submit(client,thread_id,'不要调用工具。只回复：版本一已完成。',
                'cv01-first-'+uuid.uuid4().hex)
            second=await submit(client,thread_id,'不要调用工具。只回复：版本二已完成。',
                'cv01-second-'+uuid.uuid4().hex)
            if first['state']!='COMPLETED' or second['state']!='COMPLETED':
                raise AssertionError('cv01_turn_failed')
            versions=await core.list_checkpoint_versions(thread_id)
            active=next(row for row in versions if row['state']=='ACTIVE')
            if sum(row['state']=='SUPERSEDED' for row in versions)<1:
                raise AssertionError('cv01_version_chain_missing')
            results.append({'case':'CV01','state':'passed','active':active['id'],
                'version_count':len(versions)})

            invalid_ref,invalid_digest=objects.put(b'not-a-valid-checkpoint')
            injected=await core.commit_checkpoint_version(thread_id,invalid_ref,
                active['native_thread_id'],size=len(b'not-a-valid-checkpoint'),
                source_turn_id=uuid.uuid4().hex,verification={'fault_injection':'invalid_archive'})
            cv02_restart=await force_fresh_sandbox(core,settings,thread_id)
            failed=await submit(client,thread_id,'不要调用工具。只回复：损坏版本不应执行。',
                'cv02-invalid-'+uuid.uuid4().hex)
            if failed['state']!='FAILED' or failed['error_code']!='checkpoint_version_rolled_back':
                raise AssertionError('cv02_rollback_missing')
            after=await core.list_checkpoint_versions(thread_id)
            if (next(row for row in after if row['id']==injected['id'])['state']!='INVALID'
                    or next(row for row in after if row['state']=='ACTIVE')['object_ref']==invalid_ref):
                raise AssertionError('cv02_pointer_not_rolled_back')
            resumed=await submit(client,thread_id,'不要调用工具。只回复：回退后继续成功。',
                'cv02-resume-'+uuid.uuid4().hex)
            if resumed['state']!='COMPLETED':raise AssertionError('cv02_resume_failed')
            results.append({'case':'CV02','state':'passed','failed_turn_id':failed['id'],
                'resume_turn_id':resumed['id'],'restart':cv02_restart})

            active=next(row for row in await core.list_checkpoint_versions(thread_id)
                if row['state']=='ACTIVE')
            wrong=await core.commit_checkpoint_version(thread_id,active['object_ref'],
                'missing-native-'+uuid.uuid4().hex,size=active['size'],
                source_turn_id=uuid.uuid4().hex,verification={'fault_injection':'native_missing'})
            cv04_restart=await force_fresh_sandbox(core,settings,thread_id)
            native_failed=await submit(client,thread_id,'不要调用工具。只回复：错误 native 不应执行。',
                'cv04-native-'+uuid.uuid4().hex)
            if (native_failed['state']!='FAILED'
                    or native_failed['error_code']!='checkpoint_version_rolled_back'):
                raise AssertionError('cv04_native_rollback_missing')
            native_resumed=await submit(client,thread_id,'不要调用工具。只回复：native 回退后成功。',
                'cv04-resume-'+uuid.uuid4().hex)
            if native_resumed['state']!='COMPLETED':raise AssertionError('cv04_resume_failed')
            results.append({'case':'CV04','state':'passed','failed_version':wrong['id'],
                'resume_turn_id':native_resumed['id'],'restart':cv04_restart})

            staged_ref,_=objects.put(b'checkpoint-stage-restart')
            staged=await core.stage_checkpoint_version(thread_id,staged_ref,
                'staged-native',size=len(b'checkpoint-stage-restart'),
                source_turn_id=uuid.uuid4().hex)
            await core.verify_checkpoint_version(staged['id'],{'fault_injection':'worker_restart'})
            pointer=(await core.get_thread(thread_id)).checkpoint_ref
            turns,jobs=await active_counts(core,thread_id)
            if turns or jobs:raise RuntimeError('shared_work_prevents_checkpoint_restart_case')
            old_pid,new_pid=await asyncio.to_thread(kill_restart_platform)
            await asyncio.sleep(2)
            current=await core.get_thread(thread_id);rows=await core.list_checkpoint_versions(thread_id)
            if current.checkpoint_ref!=pointer or next(row for row in rows
                    if row['id']==staged['id'])['state']!='VERIFIED':
                raise AssertionError('cv03_active_pointer_changed')
            await core.invalidate_checkpoint_version(staged['id'],'checkpoint_staging_expired')
            results.append({'case':'CV03','state':'passed','old_pid':old_pid,'new_pid':new_pid})

            retained=[row for row in await core.list_checkpoint_versions(thread_id)
                if row['state'] in {'ACTIVE','SUPERSEDED'}]
            if len(retained)>3 or sum(row['state']=='ACTIVE' for row in retained)!=1:
                raise AssertionError('cv05_retention_invalid')
            results.append({'case':'CV05','state':'passed','retained_versions':len(retained)})
        finally:
            if thread_id:await remove_thread(client,thread_id)
            await core.close()
    report={'passed':all(row['state']=='passed' for row in results),'results':results}
    target=Path(args.report);target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':asyncio.run(main())
