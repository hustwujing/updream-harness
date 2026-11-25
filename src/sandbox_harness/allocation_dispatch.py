from __future__ import annotations

import asyncio
from dataclasses import asdict,replace
from datetime import timedelta
import hashlib
import json
import uuid

from sqlalchemy import select, or_, func

from .domain import Harness,now
from .job_adapters import PreAcceptCancelled
from .job_contracts import JobState,TERMINAL_JOB_STATES
from .observability import event
from .sandbox_contracts import (Sandbox,SandboxCreateRequest,SandboxHandle,SandboxProfile,
    SandboxScope,SandboxCapacityUnavailable,SandboxRateLimited,SandboxAuthenticationFailed,
    SandboxProfileUnavailable,SandboxCreateOutcomeUnknown,SandboxProviderError)
from .store import JobRow,JobLeaseRow,JobAttemptRow,JobCleanupRow,SandboxAllocationRow,utc_datetime


REJECTIONS={kind.code:kind for kind in (SandboxCapacityUnavailable,SandboxRateLimited,
    SandboxAuthenticationFailed,SandboxProfileUnavailable)}
ABSENT_STATES={'REJECTED','CANCELLED','RELEASED','ABSENT'}


class AllocationAbsent(RuntimeError):
    """The platform ledger proves no live allocation belongs to this request."""


def allocation_key(namespace,provider,allocation_id):
    return hashlib.sha256(json.dumps([namespace,provider,allocation_id],separators=(',',':')).encode()).hexdigest()


def encode_request(request):
    return json.dumps({'harness':request.harness.value,'profile':asdict(request.profile),
        'environment':dict(request.environment),'ttl_seconds':request.ttl_seconds,
        'allocation_id':request.allocation_id},sort_keys=True,separators=(',',':'))


def decode_request(encoded):
    value=json.loads(encoded)
    return SandboxCreateRequest(harness=Harness(value['harness']),profile=SandboxProfile(**value['profile']),
        environment=value['environment'],ttl_seconds=value['ttl_seconds'],allocation_id=value['allocation_id'])


def snapshot(row):
    if row is None:return None
    return {'id':row.id,'state':row.state,'job_id':row.job_id,'job_generation':row.job_generation,
        'request_json':row.request_json,'handle':json.loads(row.handle_json),'error_code':row.error_code,
        'reconcile_token':row.reconcile_token,'reconcile_attempts':row.reconcile_attempts}


class AllocationClient:
    def __init__(self,store,backend,*,poll_seconds=.2):
        self.store=store;self.backend=backend;self.poll_seconds=poll_seconds

    def identity(self,request):
        return allocation_key(self.backend.namespace,request.profile.provider,request.allocation_id)

    async def snapshot(self,context,request):
        return await self._snapshot_identity(self.identity(request))

    async def _snapshot_identity(self,identity):
        async with self.store.connections() as db:
            return snapshot(await db.get(SandboxAllocationRow,identity))

    async def enqueue(self,context,request):
        encoded=encode_request(request);digest=hashlib.sha256(encoded.encode()).hexdigest()
        identity=self.identity(request)
        async with self.store.connections.begin() as db:
            job=await db.get(JobRow,context.job.id,with_for_update=True)
            lease=await db.get(JobLeaseRow,context.job.id,with_for_update=True)
            if (not job or not lease or job.generation!=context.generation
                    or lease.generation!=context.generation or JobState(job.state) in TERMINAL_JOB_STATES):
                raise RuntimeError('stale_job_generation')
            if job.cancel_requested:raise PreAcceptCancelled()
            row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
            if not row or row.state in {'REJECTED','RELEASED'}:
                attempt=await db.get(JobAttemptRow,lease.attempt_id)
                if not attempt or attempt.accepted_at or attempt.outcome!='SUBMITTING':
                    raise RuntimeError('job_submission_intent_conflict')
            if row:
                if row.job_id!=job.id or row.request_digest!=digest:
                    raise RuntimeError('sandbox_allocation_request_conflict')
                if row.state in {'REJECTED','RELEASED'}:
                    row.state='PENDING';row.job_generation=context.generation
                    row.error_code='';row.handle_json='{}';row.claim_owner=''
                    row.claim_expires_at=None;row.updated_at=now()
                    row.reconcile_after=None;row.reconcile_attempts=0
                    row.reconcile_token='';row.reconcile_expires_at=None
            else:
                db.add(SandboxAllocationRow(id=identity,namespace=self.backend.namespace,
                    provider=request.profile.provider,allocation_id=request.allocation_id,
                    job_id=job.id,job_generation=context.generation,request_json=encoded,
                    request_digest=digest,state='PENDING',created_at=now(),updated_at=now()))
        return identity

    async def create(self,context,request):
        try:identity=await self.enqueue(context,request)
        except PreAcceptCancelled:raise
        except RuntimeError as exc:
            if str(exc)=='stale_job_generation':raise
            raise SandboxCreateOutcomeUnknown('Allocation enqueue requires reconciliation',
                provider=request.profile.provider) from exc
        except Exception as exc:
            raise SandboxCreateOutcomeUnknown('Allocation enqueue requires reconciliation',
                provider=request.profile.provider) from exc
        while True:
            read=asyncio.create_task(self._snapshot_identity(identity))
            try:value=await asyncio.shield(read)
            except asyncio.CancelledError:
                await asyncio.gather(read,return_exceptions=True);raise
            except Exception as exc:
                raise SandboxCreateOutcomeUnknown('Allocation status requires reconciliation',
                    provider=request.profile.provider) from exc
            owner_read=asyncio.create_task(self.store.get_job(context.job.id))
            try:current=await asyncio.shield(owner_read)
            except asyncio.CancelledError:
                await asyncio.gather(owner_read,return_exceptions=True);raise
            except Exception as exc:
                raise SandboxCreateOutcomeUnknown('Allocation owner requires reconciliation',
                    provider=request.profile.provider) from exc
            if current and current.cancel_requested:
                raise PreAcceptCancelled(retain_allocation=True)
            if value['state']=='READY':
                try:return await self.backend.get(SandboxHandle.from_mapping(value['handle']))
                except Exception as exc:
                    raise SandboxCreateOutcomeUnknown('Allocated sandbox requires reconciliation',
                        provider=request.profile.provider) from exc
            if value['state']=='REJECTED':
                kind=REJECTIONS[value['error_code']]
                raise kind('Sandbox creation was rejected',provider=request.profile.provider)
            if value['state']=='CANCELLED':raise PreAcceptCancelled()
            if value['state']=='RELEASED':raise AllocationAbsent()
            await asyncio.sleep(self.poll_seconds)

    async def resolve(self,context,receipt,*,cleanup=False):
        identity=receipt['allocation_dispatch_id']
        async with self.store.connections.begin() as db:
            job=await db.get(JobRow,context.job.id,with_for_update=True)
            if not job:raise RuntimeError('stale_job_generation')
            if cleanup:
                claim=(await db.scalars(select(JobCleanupRow).where(JobCleanupRow.job_id==job.id,
                    JobCleanupRow.generation==context.generation,
                    JobCleanupRow.state=='running').with_for_update())).one_or_none()
                if (not claim or json.loads(claim.acceptance_receipt_json).get('allocation_dispatch_id')!=identity):
                    raise RuntimeError('job_cleanup_claim_lost')
            elif JobState(job.state) not in TERMINAL_JOB_STATES:
                lease=await db.get(JobLeaseRow,job.id,with_for_update=True)
                if not lease or job.generation!=context.generation or lease.generation!=context.generation:
                    raise RuntimeError('stale_job_generation')
            row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
            if row and row.job_id!=job.id:raise RuntimeError('sandbox_allocation_owner_mismatch')
            value=snapshot(row)
        if not value or value['state'] in ABSENT_STATES:raise AllocationAbsent()
        if value['state']!='READY':return None
        request=decode_request(value['request_json']);handle=SandboxHandle.from_mapping(value['handle'])
        return Sandbox(id=handle.sandbox_id,provider=handle.provider,harness=request.harness,
            profile=request.profile,handle=handle,metadata={'sandbox-harness.io/namespace':self.backend.namespace})

    async def released(self,receipt):
        identity=receipt.get('allocation_dispatch_id')
        if not identity:return
        async with self.store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
            if not row or row.state in ABSENT_STATES:return
            if row.state!='READY' or json.loads(row.handle_json)!=receipt.get('sandbox_handle'):
                raise RuntimeError('sandbox_allocation_handle_conflict')
            row.state='RELEASED';row.updated_at=now()


class AllocationService:
    def __init__(self,store,backend,*,owner='',lease_seconds=30):
        self.store=store;self.backend=backend;self.owner=owner or 'allocator-'+uuid.uuid4().hex
        self.lease_seconds=lease_seconds

    async def _finish(self,identity,state,*,handle=None,error_code=''):
        async with self.store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
            if not row or row.claim_owner!=self.owner:
                raise RuntimeError('stale_allocation_claim')
            # Observation may win the race with the original create response.
            # Its resolved handle (or subsequent cleanup) must never regress.
            if row.state in {'READY','RELEASED'}:
                if handle and json.loads(row.handle_json)!=handle.to_mapping():
                    raise RuntimeError('sandbox_allocation_handle_conflict')
                return
            if row.state not in {'SENDING','UNKNOWN'}:
                raise RuntimeError('stale_allocation_claim')
            row.state=state;row.error_code=error_code;row.updated_at=now();row.claim_expires_at=None
            row.reconcile_after=None;row.reconcile_token='';row.reconcile_expires_at=None
            if handle:row.handle_json=json.dumps(handle.to_mapping(),sort_keys=True)

    async def _execute(self,value):
        request=decode_request(value['request_json']);identity=value['id']
        async def heartbeat():
            while True:
                await asyncio.sleep(max(.05,self.lease_seconds/3))
                async with self.store.connections.begin() as db:
                    row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
                    if not row or row.claim_owner!=self.owner or row.state!='SENDING':return
                    row.claim_expires_at=now()+timedelta(seconds=self.lease_seconds)
        task=asyncio.create_task(heartbeat())
        try:
            sandbox=await self.backend.create(request)
            if (sandbox.handle.provider!=request.profile.provider
                    or sandbox.handle.allocation_id!=request.allocation_id
                    or sandbox.compatibility_key!=request.profile.compatibility_key):
                raise RuntimeError('sandbox_allocation_identity_mismatch')
            await self._finish(identity,'READY',handle=sandbox.handle)
        except tuple(REJECTIONS.values()) as exc:
            await self._finish(identity,'REJECTED',error_code=exc.code)
        except asyncio.CancelledError:
            await asyncio.shield(self._finish(identity,'UNKNOWN'));raise
        except Exception:
            await self._finish(identity,'UNKNOWN')
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)

    async def _expire_creates(self):
        async with self.store.connections.begin() as db:
            stale=(await db.scalars(select(SandboxAllocationRow).where(
                SandboxAllocationRow.namespace==self.backend.namespace,
                SandboxAllocationRow.provider==self.backend.provider,
                SandboxAllocationRow.state=='SENDING',SandboxAllocationRow.claim_expires_at<now())
                .with_for_update(skip_locked=True))).all()
            for row in stale:
                row.state='UNKNOWN';row.updated_at=now();row.reconcile_after=None

    async def _claim_creates(self,limit):
        if limit<=0:return []
        async with self.store.connections() as db:
            candidates=(await db.scalars(select(SandboxAllocationRow.id).where(
                SandboxAllocationRow.namespace==self.backend.namespace,
                SandboxAllocationRow.provider==self.backend.provider,SandboxAllocationRow.state=='PENDING')
                .order_by(SandboxAllocationRow.created_at).limit(limit))).all()
        claimed=[]
        for identity in candidates:
            async with self.store.connections() as db:
                current=await db.get(SandboxAllocationRow,identity)
                if not current:continue
                job_id=current.job_id
            async with self.store.connections.begin() as db:
                job=await db.get(JobRow,job_id,with_for_update=True)
                row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
                if not row or row.state!='PENDING':continue
                if (not job or job.cancel_requested or JobState(job.state) in TERMINAL_JOB_STATES
                        or job.generation!=row.job_generation):
                    row.state='CANCELLED';row.updated_at=now();continue
                row.state='SENDING';row.claim_owner=self.owner
                row.claim_expires_at=now()+timedelta(seconds=self.lease_seconds);row.updated_at=now()
                claimed.append(snapshot(row))
        return claimed

    async def run_once(self,limit=8):
        await self._expire_creates()
        claimed=await self._claim_creates(limit)
        await asyncio.gather(*(self._execute(value) for value in claimed),self.reconcile_once(limit=limit))
        return len(claimed)

    async def _claim_reconciliations(self,limit):
        if limit<=0:return []
        timestamp=now()
        async with self.store.connections.begin() as db:
            rows=(await db.scalars(select(SandboxAllocationRow).where(
                SandboxAllocationRow.namespace==self.backend.namespace,
                SandboxAllocationRow.provider==self.backend.provider,SandboxAllocationRow.state=='UNKNOWN',
                or_(SandboxAllocationRow.reconcile_after.is_(None),SandboxAllocationRow.reconcile_after<=timestamp),
                or_(SandboxAllocationRow.reconcile_expires_at.is_(None),SandboxAllocationRow.reconcile_expires_at<=timestamp))
                .order_by(func.coalesce(SandboxAllocationRow.reconcile_after,SandboxAllocationRow.created_at),
                    SandboxAllocationRow.created_at,SandboxAllocationRow.id)
                .limit(limit).with_for_update(skip_locked=True))).all()
            values=[]
            for row in rows:
                row.reconcile_token=uuid.uuid4().hex
                row.reconcile_expires_at=timestamp+timedelta(seconds=self.lease_seconds)
                row.reconcile_attempts+=1
                # Persist rotation even if the observer dies during its RPC.
                row.reconcile_after=row.reconcile_expires_at
                values.append(snapshot(row))
            return values

    async def _reconcile(self,value):
        request=decode_request(value['request_json']);handle=None;error_code='allocation_not_visible'
        try:
            async with asyncio.timeout(self.lease_seconds*.8):
                sandbox=await self.backend.reconcile_create(SandboxScope(self.backend.namespace),request.allocation_id)
            if sandbox:
                if (sandbox.handle.provider==request.profile.provider
                        and sandbox.handle.allocation_id==request.allocation_id
                        and sandbox.compatibility_key==request.profile.compatibility_key):
                    handle=replace(sandbox.handle,revision=request.profile.revision)
                else:error_code='allocation_identity_mismatch'
        except (SandboxProviderError,TimeoutError):error_code='allocation_observation_unavailable'
        async with self.store.connections.begin() as db:
            row=await db.get(SandboxAllocationRow,value['id'],with_for_update=True)
            if (not row or row.state!='UNKNOWN' or row.reconcile_token!=value['reconcile_token']
                    or not row.reconcile_expires_at or utc_datetime(row.reconcile_expires_at)<=now()):return
            row.reconcile_token='';row.reconcile_expires_at=None;row.updated_at=now()
            if handle:
                row.state='READY';row.handle_json=json.dumps(handle.to_mapping(),sort_keys=True)
                row.error_code='';row.reconcile_after=None
            else:
                row.error_code=error_code
                row.reconcile_after=now()+timedelta(seconds=min(30,2**min(row.reconcile_attempts-1,5)))

    async def reconcile_once(self,limit=8):
        await self._expire_creates()
        values=await self._claim_reconciliations(limit)
        await asyncio.gather(*(self._reconcile(value) for value in values))
        return len(values)

    async def _run_lane(self,stop,*,concurrency,reconciliation):
        tasks=set()
        try:
            while not stop.is_set():
                completed={task for task in tasks if task.done()}
                tasks-=completed
                for result in await asyncio.gather(*completed,return_exceptions=True):
                    if isinstance(result,Exception):
                        event('sandbox.allocation.dispatch_failed',level=40,exception_type=type(result).__name__)
                try:
                    if reconciliation:await self._expire_creates()
                    claim=self._claim_reconciliations if reconciliation else self._claim_creates
                    execute=self._reconcile if reconciliation else self._execute
                    for value in await claim(concurrency-len(tasks)):
                        tasks.add(asyncio.create_task(execute(value)))
                except Exception as exc:
                    event('sandbox.allocation.dispatch_failed',level=40,exception_type=type(exc).__name__)
                try:await asyncio.wait_for(stop.wait(),timeout=.2)
                except TimeoutError:pass
        finally:
            for task in tasks:task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

    async def run(self,stop,*,concurrency=8):
        if concurrency<1:raise ValueError('allocation_concurrency_invalid')
        async with asyncio.TaskGroup() as group:
            group.create_task(self._run_lane(stop,concurrency=concurrency,reconciliation=False))
            group.create_task(self._run_lane(stop,concurrency=concurrency,reconciliation=True))
