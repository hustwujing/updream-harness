from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict,replace
from datetime import timedelta
import hashlib
import json
import uuid

from sqlalchemy import select, or_, func

from sandbox_harness.domain import Harness,now
from sandbox_harness.job_adapters import PreAcceptCancelled,AllocationRecoveryRequired
from sandbox_harness.job_contracts import JobState,TERMINAL_JOB_STATES
from sandbox_harness.observability import event
from sandbox_harness.sandbox_contracts import (Sandbox,SandboxCreateRequest,SandboxHandle,SandboxProfile,
    SandboxScope,SandboxCapacityUnavailable,SandboxRateLimited,SandboxAuthenticationFailed,
    SandboxProfileUnavailable,SandboxCreateOutcomeUnknown,SandboxProviderError)
from sandbox_harness.store import JobRow,JobLeaseRow,JobAttemptRow,JobCleanupRow,SandboxAllocationRow,utc_datetime


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
        'reconcile_token':row.reconcile_token,'reconcile_attempts':row.reconcile_attempts,
        'issued_at':utc_datetime(row.issued_at) if row.issued_at else None,
        'recovery_policy':json.loads(row.recovery_policy_json)}


class AllocationClient:
    def __init__(self,store,backend,*,poll_seconds=.2,recovery_seconds=90,provision_ttl_seconds=300):
        self.store=store;self.backend=backend;self.poll_seconds=poll_seconds
        self.recovery_policy={'recovery_seconds':recovery_seconds,
            'provision_ttl_seconds':provision_ttl_seconds,'max_replacements':1}

    @staticmethod
    def _check_recovery(value):
        seconds=value['recovery_policy'].get('recovery_seconds')
        if (seconds and value['issued_at'] and value['state'] in {'SENDING','UNKNOWN'}
                and (now()-value['issued_at']).total_seconds()>=seconds):
            raise AllocationRecoveryRequired(value['id'])

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
                    row.issued_at=None
            else:
                db.add(SandboxAllocationRow(id=identity,namespace=self.backend.namespace,
                    provider=request.profile.provider,allocation_id=request.allocation_id,
                    job_id=job.id,job_generation=context.generation,request_json=encoded,
                    request_digest=digest,state='PENDING',created_at=now(),updated_at=now(),
                    recovery_policy_json=json.dumps(self.recovery_policy,sort_keys=True)))
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
            self._check_recovery(value)
            if value['state']=='READY':
                try:
                    sandbox=await self.backend.get(SandboxHandle.from_mapping(value['handle']))
                    if value['recovery_policy'].get('provision_ttl_seconds'):
                        sandbox=await self.backend.renew(sandbox,force=True)
                    return sandbox
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
        if not cleanup and receipt.get('allocation_phase')=='allocating':self._check_recovery(value)
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


