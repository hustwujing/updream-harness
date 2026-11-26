from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict,replace
from datetime import timedelta
import hashlib
import json
import uuid

from sqlalchemy import select, or_, func

from sandbox_harness.common.contracts.domain import (Harness, now)
from sandbox_harness.common.execution.job_adapters import (PreAcceptCancelled, AllocationRecoveryRequired)
from sandbox_harness.common.contracts.job_contracts import (JobState, TERMINAL_JOB_STATES)
from sandbox_harness.common.observability.observability import (add_link, business_metric, event, operation)
from sandbox_harness.common.contracts.sandbox_contracts import (Sandbox, SandboxCreateRequest, SandboxHandle, SandboxProfile, SandboxScope, SandboxCapacityUnavailable, SandboxRateLimited, SandboxAuthenticationFailed, SandboxProfileUnavailable, SandboxCreateOutcomeUnknown, SandboxProviderError)
from sandbox_harness.common.db.models import (JobRow, JobLeaseRow, JobAttemptRow, JobCleanupRow,
                                              SandboxAllocationRow, utc_datetime)


from sandbox_harness.common.sandbox.allocation_client import (AllocationAbsent, AllocationClient, allocation_key, encode_request, decode_request, snapshot, REJECTIONS, ABSENT_STATES)


class AllocationService:
    def __init__(self,store,backend,*,owner='',lease_seconds=30,
                 circuit_failure_threshold=3,circuit_open_seconds=30,circuit_probe_seconds=15):
        self.store=store;self.backend=backend;self.owner=owner or 'allocator-'+uuid.uuid4().hex
        self.lease_seconds=lease_seconds
        self.circuit_failure_threshold=circuit_failure_threshold
        self.circuit_open_seconds=circuit_open_seconds
        self.circuit_probe_seconds=circuit_probe_seconds

    def _circuit_key(self,request):
        return hashlib.sha256((self.backend.provider+'\0'+
            request.profile.compatibility_key).encode()).hexdigest()

    @asynccontextmanager
    async def _transaction(self):
        # SQLite cannot upgrade concurrent local read transactions to writers.
        # PostgreSQL row locks additionally fence independent service processes.
        # No provider request or capacity wait runs inside this local lock.
        async with self.store.job_lock('allocation-ledger'):
            async with self.store.connections.begin() as db:
                yield db

    async def _finish(self,identity,state,*,handle=None,error_code=''):
        async with self._transaction() as db:
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
        provision_ttl=value['recovery_policy'].get('provision_ttl_seconds')
        if provision_ttl:request=replace(request,ttl_seconds=min(request.ttl_seconds,int(provision_ttl)))
        async def heartbeat():
            while True:
                await asyncio.sleep(max(.05,self.lease_seconds/3))
                async with self._transaction() as db:
                    row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
                    if not row or row.claim_owner!=self.owner or row.state!='SENDING':return
                    row.claim_expires_at=now()+timedelta(seconds=self.lease_seconds)
        task=asyncio.create_task(heartbeat())
        started=asyncio.get_running_loop().time()
        metric_outcome='unknown'
        event('sandbox.allocation.create.started',allocation_id=identity,job_id=value['job_id'],
            generation=value['job_generation'],provider=self.backend.provider,state_before='PENDING',
            state_after='SENDING')
        try:
            with operation('sandbox.allocation.claim',new_trace=True,
                    allocation_id=identity,generation=value['job_generation']):
                add_link(value.get('traceparent',''))
                sandbox=await self.backend.create(request)
            if (sandbox.handle.provider!=request.profile.provider
                    or sandbox.handle.allocation_id!=request.allocation_id
                    or sandbox.compatibility_key!=request.profile.compatibility_key):
                raise RuntimeError('sandbox_allocation_identity_mismatch')
            await self._finish(identity,'READY',handle=sandbox.handle)
            await self.store.record_provider_success(self._circuit_key(request),self.owner)
            metric_outcome='success'
            event('sandbox.allocation.create.completed',allocation_id=identity,
                job_id=value['job_id'],generation=value['job_generation'],provider=self.backend.provider,
                sandbox_id=sandbox.id,state_after='READY',outcome='success',
                duration_ms=round((asyncio.get_running_loop().time()-started)*1000,3))
        except tuple(REJECTIONS.values()) as exc:
            await self._finish(identity,'REJECTED',error_code=exc.code)
            if not isinstance(exc,SandboxCapacityUnavailable):
                await self.store.record_provider_failure(self._circuit_key(request),
                    threshold=self.circuit_failure_threshold,
                    open_seconds=self.circuit_open_seconds,owner=self.owner)
            metric_outcome='rejected'
            event('sandbox.allocation.create.rejected',level=30,allocation_id=identity,
                job_id=value['job_id'],generation=value['job_generation'],provider=self.backend.provider,
                state_after='REJECTED',outcome='rejected',error_code=exc.code,exc=exc,
                duration_ms=round((asyncio.get_running_loop().time()-started)*1000,3))
        except asyncio.CancelledError:
            await asyncio.shield(self._finish(identity,'UNKNOWN'))
            metric_outcome='cancelled'
            event('sandbox.allocation.create.unknown',level=30,allocation_id=identity,
                job_id=value['job_id'],generation=value['job_generation'],provider=self.backend.provider,
                state_after='UNKNOWN',outcome='cancelled');raise
        except Exception as exc:
            await self._finish(identity,'UNKNOWN')
            await self.store.record_provider_failure(self._circuit_key(request),
                threshold=self.circuit_failure_threshold,
                open_seconds=self.circuit_open_seconds,owner=self.owner)
            event('sandbox.allocation.create.unknown',level=40,allocation_id=identity,
                job_id=value['job_id'],generation=value['job_generation'],provider=self.backend.provider,
                state_after='UNKNOWN',outcome='unknown',exc=exc,
                duration_ms=round((asyncio.get_running_loop().time()-started)*1000,3))
        finally:
            business_metric('updream.sandbox.create.duration',
                (asyncio.get_running_loop().time()-started)*1000,
                provider=self.backend.provider,outcome=metric_outcome)
            task.cancel();await asyncio.gather(task,return_exceptions=True)

    async def _expire_creates(self):
        async with self._transaction() as db:
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
                request=decode_request(current.request_json)
            if not await self.store.allow_provider_create(self._circuit_key(request),self.owner,
                    probe_seconds=self.circuit_probe_seconds):
                continue
            async with self._transaction() as db:
                job=await db.get(JobRow,job_id,with_for_update=True)
                row=await db.get(SandboxAllocationRow,identity,with_for_update=True)
                if not row or row.state!='PENDING':continue
                if (not job or job.cancel_requested or JobState(job.state) in TERMINAL_JOB_STATES
                        or job.generation!=row.job_generation):
                    row.state='CANCELLED';row.updated_at=now();continue
                row.state='SENDING';row.claim_owner=self.owner
                row.issued_at=now()
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
        async with self._transaction() as db:
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
        started=asyncio.get_running_loop().time()
        event('sandbox.allocation.reconcile.started',allocation_id=value['id'],job_id=value['job_id'],
            generation=value['job_generation'],provider=self.backend.provider,
            retry_count=value['reconcile_attempts'],state_before='UNKNOWN')
        try:
            with operation('sandbox.allocation.reconcile_claim',new_trace=True,
                    allocation_id=value['id'],generation=value['job_generation']):
                add_link(value.get('traceparent',''))
                async with asyncio.timeout(self.lease_seconds*.8):
                    sandbox=await self.backend.reconcile_create(
                        SandboxScope(self.backend.namespace),request.allocation_id)
            if sandbox:
                if (sandbox.handle.provider==request.profile.provider
                        and sandbox.handle.allocation_id==request.allocation_id
                        and sandbox.compatibility_key==request.profile.compatibility_key):
                    handle=replace(sandbox.handle,revision=request.profile.revision)
                else:error_code='allocation_identity_mismatch'
        except (SandboxProviderError,TimeoutError):error_code='allocation_observation_unavailable'
        async with self._transaction() as db:
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
        event('sandbox.allocation.reconcile.completed',allocation_id=value['id'],job_id=value['job_id'],
            generation=value['job_generation'],provider=self.backend.provider,
            retry_count=value['reconcile_attempts'],state_after='READY' if handle else 'UNKNOWN',
            outcome='success' if handle else 'deferred',error_code='' if handle else error_code)
        business_metric('updream.sandbox.reconciliation.duration',
            (asyncio.get_running_loop().time()-started)*1000,provider=self.backend.provider,
            outcome='success' if handle else 'deferred')

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
                        event('sandbox.allocation.dispatch_failed',level=40,exc=result)
                try:
                    if reconciliation:await self._expire_creates()
                    claim=self._claim_reconciliations if reconciliation else self._claim_creates
                    execute=self._reconcile if reconciliation else self._execute
                    for value in await claim(concurrency-len(tasks)):
                        tasks.add(asyncio.create_task(execute(value)))
                except Exception as exc:
                    event('sandbox.allocation.dispatch_failed',level=40,exc=exc)
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
