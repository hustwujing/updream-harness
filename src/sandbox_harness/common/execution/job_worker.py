from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import replace
from datetime import datetime,timezone

import httpx

from sandbox_harness.common.contracts.domain_commands import DomainCommandValidationError
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, ExternalJobFailure, JobAdapter, JobExecutionContext, PreAcceptCancelled, PreAcceptFailure, SubmissionOutcomeUnknown, AllocationRecoveryRequired)
from sandbox_harness.common.contracts.job_contracts import (JobKind, JobState, TERMINAL_JOB_STATES)
from sandbox_harness.common.contracts.job_errors import (ArtifactCollectionCardinalityError, ArtifactValidationError, normalize_job_error)
from sandbox_harness.common.observability.observability import (add_link, business_metric, event, operation, sanitize)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxEndpointUnavailable, SandboxProviderUnavailable, SandboxRateLimited)


class JobDeadlineExceeded(TimeoutError):
    pass


class JobWorker:
    def __init__(self, store, adapters: dict[str, JobAdapter], *, node_id: str,
                 concurrency: int = 4, lease_seconds: int = 60, worker_id: str = "",
                 poll_seconds: float = 1.0, review_dispatcher=None, recovery_concurrency: int = 1,
                 owner_concurrency: int = 4):
        self.store = store
        self.adapters = dict(adapters)
        self.node_id = node_id
        self.concurrency = concurrency
        self.recovery_concurrency=max(1,recovery_concurrency)
        self.owner_concurrency=max(1,owner_concurrency)
        self.lease_seconds = lease_seconds
        self.worker_id = worker_id or "job-worker-" + uuid.uuid4().hex
        self.poll_seconds = poll_seconds
        self.stop_event = asyncio.Event()
        self.review_dispatcher = review_dispatcher

    async def _collect_result(self,adapter,context,external_id,status):
        try:result=await adapter.collect(context,external_id,status)
        except ExternalJobFailure:raise
        except ArtifactValidationError as exc:
            raise ExternalJobFailure('成果未通过格式或大小校验',
                error_code=exc.error_code,failed_phase='validation') from exc
        except ArtifactCollectionCardinalityError as exc:
            failure=ExternalJobFailure('成果文件数量不符合已冻结的产物契约',
                error_code='artifact_collection_cardinality_invalid',failed_phase='validation')
            failure.result.update(exc.result)
            raise failure from exc
        except Exception as exc:
            event('job.adapter.collection_failed',level=40,job_id=context.job.id,
                generation=context.generation,exc=exc)
            raise ExternalJobFailure('成果收集阶段发生内部错误',
                error_code='artifact_collection_failed',failed_phase='collection',
                retryable=True,retry_class='collect_same_execution',
                side_effect_state='completed') from exc
        if (self.review_dispatcher and status.state==JobState.SUCCEEDED
                and context.request.adapter!="reviewer-harness"):
            try:await self.review_dispatcher(context,result)
            except Exception as exc:
                gates=await self.store.record_policy_review_unavailable(
                    context.job.id,context.generation,type(exc).__name__)
                result={**result,"review_dispatch":{"state":"unavailable",
                    "error":type(exc).__name__,"gates":gates}}
                event("review.policy.dispatch_failed",level=40,job_id=context.job.id,
                    exc=exc)
        return result

    async def run_once(self) -> int:
        cleanups=await self.run_cleanup_once(limit=self.concurrency)
        attempts = await self.store.claim_available_jobs(
            self.worker_id, self.node_id, self.lease_seconds,
            limit=self.concurrency, adapters=set(self.adapters),
        )
        if attempts:
            await asyncio.gather(*(self.process(attempt) for attempt in attempts))
        await self.store.reconcile_terminal_group_budgets()
        return len(attempts)+cleanups

    async def run(self) -> None:
        tasks: set[asyncio.Task] = set()
        recoveries: set[asyncio.Task] = set()
        cleanup=None
        failure=None
        try:
            while not self.stop_event.is_set():
                completed={task for task in tasks|recoveries if task.done()}
                for task in completed:await task
                tasks-=completed;recoveries-=completed
                # Cleanup and accepted execution recovery must progress even when
                # every admission slot is waiting for capacity they can release.
                if cleanup is None or cleanup.done():
                    if cleanup:await cleanup
                    cleanup=asyncio.create_task(self.run_cleanup_once(limit=self.concurrency),
                        name='job-cleanup')
                recovery_capacity=max(0,self.recovery_concurrency-len(recoveries))
                if recovery_capacity:
                    recovered=await self.store.claim_available_jobs(
                        self.worker_id,self.node_id,self.lease_seconds,limit=recovery_capacity,
                        adapters=set(self.adapters),accepted_only=True)
                    recoveries.update(asyncio.create_task(self._process_isolated(attempt,
                        continuous=True,recovery_only=True),name='job-recovery-'+attempt.job_id) for attempt in recovered)
                capacity=max(0,self.concurrency-len(tasks));attempts=[]
                if capacity:
                    attempts=await self.store.claim_available_jobs(
                        self.worker_id,self.node_id,self.lease_seconds,limit=capacity,
                        adapters=set(self.adapters),owner_limit=self.owner_concurrency)
                    tasks.update(asyncio.create_task(self._process_isolated(attempt),
                        name='job-'+attempt.job_id) for attempt in attempts)
                if not attempts:
                    try:await asyncio.wait_for(self.stop_event.wait(),timeout=0.5)
                    except asyncio.TimeoutError:pass
        except BaseException as exc:
            failure=exc
        finally:
            self.stop_event.set()
            owned=list(tasks|recoveries)+([cleanup] if cleanup else [])
            joined=asyncio.gather(*owned,return_exceptions=True)
            while True:
                try:
                    results=await asyncio.shield(joined)
                    break
                except asyncio.CancelledError as exc:
                    failure=failure or exc
            for result in results:
                if isinstance(result,BaseException):failure=failure or result
        if failure:raise failure

    async def _process_isolated(self,attempt,*,continuous=True,recovery_only=False)->None:
        with operation('job.worker.claim',new_trace=True,job_id=attempt.job_id,
                generation=attempt.generation):
            add_link(attempt.traceparent)
            try:await self.process(attempt,continuous=continuous,recovery_only=recovery_only)
            except Exception as exc:
                event('job.worker.process_failed',level=40,job_id=attempt.job_id,
                    generation=attempt.generation,exc=exc)
                job=await self.store.get_job(attempt.job_id)
                if not job or job.state in TERMINAL_JOB_STATES:return
                state=getattr(exc,'terminal_state',
                    JobState.UNKNOWN if job.kind==JobKind.PROVIDER else JobState.FAILED)
                try:
                    result=getattr(exc,'result',None) or {'error_code':'worker_internal_error',
                        'exception_type':type(exc).__name__,'error_detail':str(exc)[:2000]}
                    if state in {JobState.FAILED,JobState.BLOCKED,JobState.UNKNOWN}:
                        result=normalize_job_error(result,side_effect_state=(
                            'unknown' if state==JobState.UNKNOWN else None))
                    await self.store.transition_job(job.id,state,expected_generation=attempt.generation,
                        result=result)
                except RuntimeError as stale:
                    if str(stale)!='stale_job_generation':raise
                    return
                await self.run_cleanup_once(limit=1)

    async def process(self, attempt, *, continuous: bool = False, recovery_only: bool = False) -> None:
        lost=asyncio.Event();owner=asyncio.current_task()
        async def heartbeat():
            while True:
                await asyncio.sleep(max(.1,self.lease_seconds/3))
                if not await self.store.renew_job_lease(attempt.job_id,self.worker_id,
                        attempt.generation,self.lease_seconds):
                    lost.set();owner.cancel();return
        task=asyncio.create_task(heartbeat(),name="job-heartbeat-"+attempt.job_id)
        try:
            await self._process_claimed(attempt,continuous=continuous,recovery_only=recovery_only)
        except asyncio.CancelledError:
            if not lost.is_set():raise
            event("job.worker.lease_lost",level=30,job_id=attempt.job_id,
                generation=attempt.generation)
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        await self.run_cleanup_once(limit=1)

    @staticmethod
    async def _before_deadline(awaitable,deadline):
        remaining=(deadline-datetime.now(timezone.utc)).total_seconds()
        if remaining<=0:
            if hasattr(awaitable,"close"):awaitable.close()
            raise JobDeadlineExceeded("job_timeout")
        try:return await asyncio.wait_for(awaitable,timeout=remaining)
        except TimeoutError as exc:
            if deadline<=datetime.now(timezone.utc):raise JobDeadlineExceeded("job_timeout") from exc
            raise

    async def _external_status(self,adapter,context,external_id,deadline,*,reconcile=False):
        try:
            operation=(adapter.reconcile(context,external_id) if reconcile
                else adapter.status(context,external_id))
            return await self._before_deadline(operation,deadline)
        except (ExternalJobFailure,JobDeadlineExceeded):raise
        except Exception as exc:
            transient=isinstance(exc,(httpx.TimeoutException,httpx.TransportError,
                SandboxEndpointUnavailable,SandboxProviderUnavailable,SandboxRateLimited))
            if isinstance(exc,httpx.HTTPStatusError):
                transient=exc.response.status_code in {408,429,500,502,503,504}
            if transient:
                event('job.adapter.status_retry',level=30,job_id=context.job.id,
                    generation=context.generation,exc=exc)
                return AdapterStatus(False)
            event('job.adapter.status_failed',level=40,job_id=context.job.id,
                generation=context.generation,exc=exc)
            raise ExternalJobFailure('外部任务状态查询失败',
                error_code='external_status_unavailable',failed_phase='status',
                terminal_state=JobState.UNKNOWN,retryable=True,retry_class='reconcile_only',
                side_effect_state='unknown') from exc

    async def _process_claimed(self, attempt, *, continuous: bool = False,
                               recovery_only: bool = False) -> None:
        job = await self.store.get_job(attempt.job_id)
        request = await self.store.get_job_request(attempt.job_id)
        if not job or not request:
            return
        if attempt.retry_kind in {'allocation_recovery_exhausted','allocation_capacity_exhausted'}:
            code=('allocation_replacement_capacity_unavailable'
                if attempt.retry_kind=='allocation_capacity_exhausted' else 'allocation_recovery_exhausted')
            await self.store.transition_job(job.id,JobState.CANCELLED if job.cancel_requested else JobState.FAILED,
                expected_generation=attempt.generation,result=normalize_job_error({'error_code':code},
                    side_effect_state='none'))
            return
        adapter = self.adapters.get(job.adapter)
        if not adapter:
            await self.store.finish_pre_accept_attempt(job.id, self.worker_id, attempt.generation,
                retry=False, error_code="adapter_not_registered", error_detail=job.adapter)
            return
        accepted = await self.store.latest_job_acceptance(job.id)
        if request.reconciles_job_id:
            accepted = await self.store.latest_job_acceptance(request.reconciles_job_id)
            if not accepted:
                await self.store.finish_pre_accept_attempt(job.id, self.worker_id, attempt.generation,
                    retry=False, error_code="reconciliation_handle_missing",
                    error_detail=request.reconciles_job_id)
                return
        context = JobExecutionContext(
            job=job, request=request, generation=attempt.generation,
            previous_external_execution_id=accepted.external_execution_id if accepted else "",
            previous_acceptance_receipt=accepted.acceptance_receipt if accepted else {},
            allocation_replacement=attempt.retry_kind=='allocation_replacement',
        )
        event('job.attempt.started',job_id=job.id,worker_id=self.worker_id,
            generation=attempt.generation,state_before=job.state,
            retry_count=max(0,attempt.generation-1))
        external_id = context.previous_external_execution_id
        deadline=await self.store.job_deadline(job.id,request.timeout_seconds)
        submission_started=False
        try:
            if deadline and deadline<=datetime.now(timezone.utc):raise JobDeadlineExceeded("job_timeout")
            participant = await self.store.get_pause_participant(job.thread_id,
                "child_harness", job.id)
            if participant and participant.state.value == "REQUESTED":
                await self._pause(adapter, context, external_id, participant.generation)
                return
            if (participant and participant.state.value == "RESUMED"
                    and (not accepted or accepted.accepted_at<=participant.updated_at)):
                accepted = None
                external_id = ""
                context = replace(context, previous_external_execution_id="",
                    previous_acceptance_receipt={})
            if recovery_only and not external_id:
                await self.store.release_job_lease(job.id,self.worker_id,attempt.generation)
                return
            if job.cancel_requested:
                if not external_id:
                    await self.store.transition_job(job.id, JobState.CANCELLED,
                        expected_generation=attempt.generation,
                        result={"cancelled_before_acceptance": True})
                    return
                operation=(adapter.cancel(context,external_id)
                    if request.cancellation_policy.get("supported") is not False
                    else adapter.status(context,external_id))
                status=await self._before_deadline(operation,deadline)
                await self._finish_or_defer(adapter,context,external_id,status,continuous,deadline)
                return
            if request.reconciles_job_id:
                if request.reconciliation_policy.get("supported") is not True:
                    await self.store.finish_pre_accept_attempt(job.id,self.worker_id,
                        attempt.generation,retry=False,error_code="reconciliation_not_supported",
                        error_detail="Descriptor does not support reconciliation")
                    return
                status=await self._external_status(adapter,context,external_id,deadline,reconcile=True)
                receipt = {"reconciles_job_id": request.reconciles_job_id,
                    "external_execution_id": external_id}
                await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                    external_execution_id=external_id, receipt=receipt)
                await self._finish_or_defer(adapter,context,external_id,status,continuous,deadline)
                return
            if not accepted:
                unresolved=await self.store.unresolved_job_submission(job.id,attempt.generation)
                if unresolved:
                    context=replace(context,
                        previous_external_execution_id=unresolved["external_execution_id"],
                        previous_acceptance_receipt=unresolved["acceptance_receipt"])
                    recovery=await self._before_deadline(adapter.recover_submission(context),deadline)
                    if recovery.submission is None and not recovery.safe_to_retry:
                        await self.store.transition_job(job.id,JobState.UNKNOWN,
                            expected_generation=attempt.generation,result=normalize_job_error({
                                "error_code":"submission_outcome_unknown_after_worker_loss"},
                                side_effect_state='unknown'))
                        return
                    if recovery.safe_to_retry:
                        await self.store.resolve_job_submission_absent(job.id,self.worker_id,
                            attempt.generation,unresolved["generation"])
                    submission=recovery.submission
                else:submission=None
                if submission is None:
                    await self.store.mark_job_submission_started(job.id,self.worker_id,attempt.generation)
                    submission_started=True
                    event('job.submission.started',job_id=job.id,worker_id=self.worker_id,
                        generation=attempt.generation,state_after='SUBMITTING')
                try:
                    while True:
                        try:
                            submission_started=True
                            await self._before_deadline(adapter.preflight(context),deadline)
                            submission=(submission or await self._before_deadline(
                                adapter.submit(context),deadline))
                            break
                        except AllocationRecoveryRequired:raise
                        except PreAcceptFailure as exc:
                            submission_started=False
                            maximum = int(request.retry_policy.get("max_pre_accept_attempts", 1))
                            retry = exc.retryable and (exc.retry_class=='capacity_wait'
                                or attempt.generation < max(1, maximum))
                            if context.allocation_replacement and exc.retry_class=='capacity_wait':
                                retry=False;exc.retryable=False;exc.infrastructure=False
                                exc.error_code='allocation_replacement_capacity_unavailable'
                                exc.retry_class='none';exc.failed_phase='capacity'
                            if (exc.infrastructure and exc.retryable and continuous
                                    and exc.retry_class=='capacity_wait'):
                                await self.store.mark_infrastructure_wait(job.id,self.worker_id,
                                    attempt.generation,error_detail=exc.error_detail or str(exc))
                                event('job.submission.capacity_wait',level=30,job_id=job.id,
                                    worker_id=self.worker_id,generation=attempt.generation,
                                    state_after='infrastructure_wait',error_code=exc.error_code,
                                    retry_seconds=exc.retry_after_seconds or 0)
                                if exc.retry_after_seconds:
                                    wait_started=time.monotonic()
                                    await self._before_deadline(
                                        asyncio.sleep(exc.retry_after_seconds),deadline)
                                    business_metric('updream.sandbox.capacity_wait.duration',
                                        (time.monotonic()-wait_started)*1000,
                                        provider=job.adapter[:64],outcome='resumed')
                                if not self.stop_event.is_set():
                                    await self.store.mark_job_submission_started(
                                        job.id,self.worker_id,attempt.generation)
                                    event('job.submission.resumed',job_id=job.id,
                                        worker_id=self.worker_id,generation=attempt.generation,
                                        state_before='INFRASTRUCTURE_WAIT',state_after='SUBMITTING')
                                    continue
                            elif retry and exc.retry_after_seconds:
                                await self._before_deadline(
                                    asyncio.sleep(exc.retry_after_seconds),deadline)
                            await self.store.finish_pre_accept_attempt(job.id,self.worker_id,
                                attempt.generation,retry=retry,error_code=(
                                    "infrastructure_capacity_wait" if exc.infrastructure
                                        and exc.error_code=='pre_accept_failure' else exc.error_code),
                                error_detail=(exc.error_detail or str(exc)[:2000]),error_metadata={
                                    'failed_phase':exc.failed_phase,'retry_class':exc.retry_class,
                                    'retryable':exc.retryable,
                                    'node_health_generation':exc.node_health_generation})
                            return
                except AllocationRecoveryRequired:raise
                except PreAcceptCancelled:
                    await self.store.transition_job(job.id,JobState.CANCELLED,
                        expected_generation=attempt.generation,
                        result={'cancelled_before_acceptance':True})
                    return
                except SubmissionOutcomeUnknown as exc:
                    await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                        external_execution_id=exc.external_execution_id,
                        receipt=exc.acceptance_receipt)
                    if exc.external_execution_id:
                        context=replace(context,previous_external_execution_id=exc.external_execution_id,
                            previous_acceptance_receipt=exc.acceptance_receipt)
                        status=await self._external_status(
                            adapter,context,exc.external_execution_id,deadline)
                        await self._finish_or_defer(adapter,context,exc.external_execution_id,
                            status,continuous,deadline)
                    else:
                        await self.store.transition_job(job.id, JobState.UNKNOWN,
                            expected_generation=attempt.generation,
                            result=normalize_job_error({"error_code":"submission_outcome_unknown",
                                "error_detail":str(exc)[:2000]},side_effect_state='unknown'))
                    return
                except JobDeadlineExceeded:
                    raise
                except DomainCommandValidationError as exc:
                    await self.store.finish_pre_accept_attempt(job.id,self.worker_id,
                        attempt.generation,retry=False,error_code="operation_start_failed",
                        error_detail=type(exc).__name__)
                    event('job.submission.contract_rejected',level=40,job_id=job.id,
                        worker_id=self.worker_id,generation=attempt.generation,
                        error_code='operation_start_failed')
                    return
                except Exception as exc:
                    deterministic = bool(request.retry_policy.get("deterministic"))
                    if request.kind == JobKind.OPERATION and deterministic:
                        maximum = int(request.retry_policy.get("max_pre_accept_attempts", 1))
                        detail=sanitize({"error.type":f"{type(exc).__name__}: {exc}"}).get(
                            "error.type",type(exc).__name__)
                        await self.store.finish_pre_accept_attempt(job.id, self.worker_id,
                            attempt.generation, retry=attempt.generation < max(1, maximum),
                            error_code="operation_start_failed", error_detail=detail)
                    else:
                        await self.store.record_job_acceptance(job.id, self.worker_id,
                            attempt.generation, external_execution_id="",
                            receipt={"outcome": "unknown", "exception_type": type(exc).__name__})
                        await self.store.transition_job(job.id, JobState.UNKNOWN,
                            expected_generation=attempt.generation,
                            result=normalize_job_error({"error_code":"submission_outcome_unknown"},
                                side_effect_state='unknown'))
                    return
                external_id = submission.external_execution_id
                await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                    external_execution_id=external_id, receipt=submission.acceptance_receipt)
                event('job.submission.accepted',job_id=job.id,worker_id=self.worker_id,
                    generation=attempt.generation,external_execution_id=external_id,
                    state_after=JobState.WAITING_EXTERNAL,outcome='accepted')
                context = replace(context, previous_external_execution_id=external_id,
                    previous_acceptance_receipt=submission.acceptance_receipt)
                if submission.completed:
                    result=await self._before_deadline(self._collect_result(adapter,context,external_id,AdapterStatus(
                        True,JobState.SUCCEEDED,submission.result)),deadline)
                    await self.store.transition_job(job.id, JobState.SUCCEEDED,
                        expected_generation=attempt.generation, result=result)
                    event('job.state.changed',job_id=job.id,generation=attempt.generation,
                        external_execution_id=external_id,state_before=JobState.WAITING_EXTERNAL,
                        state_after=JobState.SUCCEEDED,outcome='success')
                    return
            status=await self._external_status(adapter,context,external_id,deadline)
            await self._finish_or_defer(adapter,context,external_id,status,continuous,deadline)
        except AllocationRecoveryRequired as exc:
            try:
                result=await self.store.replace_unstarted_allocation(job.id,self.worker_id,attempt.generation,exc.identity)
            except RuntimeError as stale:
                if str(stale)!='stale_job_generation':raise
                return
            if result=='observe':
                await self.store.release_job_lease(job.id,self.worker_id,attempt.generation)
            elif result=='cancelled':
                await self.store.transition_job(job.id,JobState.CANCELLED,
                    expected_generation=attempt.generation,result={'cancelled_before_acceptance':True})
            elif result=='exhausted':
                await self.store.transition_job(job.id,JobState.FAILED,
                    expected_generation=attempt.generation,result=normalize_job_error({
                        'error_code':'allocation_recovery_exhausted'},side_effect_state='none'))
        except PreAcceptCancelled:
            await self.store.transition_job(job.id,JobState.CANCELLED,
                expected_generation=attempt.generation,
                result={'cancelled_before_acceptance':True})
        except JobDeadlineExceeded:
            await self._timeout(adapter,context,external_id,submission_started)
        except ExternalJobFailure as exc:
            if (exc.result.get('retry_class')=='collect_same_execution'
                    and request.adapter!='reviewer-harness'):
                maximum=int(request.retry_policy.get('max_collection_attempts',1))
                deferred=await self.store.defer_collection_retry(job.id,self.worker_id,
                    attempt.generation,maximum_attempts=maximum,
                    error_code=str(exc.result.get('error_code') or 'artifact_collection_failed'),
                    error_detail=str(exc)[:2000])
                if deferred:
                    event('job.collection.retry_deferred',level=30,job_id=job.id,
                        generation=attempt.generation,maximum_attempts=maximum,
                        external_execution_id=external_id)
                    return
            await self.store.transition_job(job.id,exc.terminal_state,
                expected_generation=attempt.generation,result=normalize_job_error(exc.result,
                    side_effect_state=('unknown' if exc.terminal_state==JobState.UNKNOWN else None)))
        except RuntimeError as exc:
            if str(exc) == "stale_job_generation":
                event("job.worker.stale_generation", level=30, job_id=job.id,
                    generation=attempt.generation)
                return
            raise

    async def _finish_or_defer(self,adapter:JobAdapter,context:JobExecutionContext,
                               external_id:str,status:AdapterStatus,continuous:bool,deadline)->None:
        while not status.terminal:
            if not continuous:
                await self.store.defer_accepted_job(context.job.id,self.worker_id,context.generation)
                return
            if self.stop_event.is_set():
                await self.store.release_job_lease(context.job.id,self.worker_id,context.generation)
                return
            if not await self.store.renew_job_lease(context.job.id,self.worker_id,
                    context.generation,self.lease_seconds):
                event('job.worker.stale_generation',level=30,job_id=context.job.id,
                    generation=context.generation)
                return
            try:await asyncio.wait_for(self.stop_event.wait(),timeout=self.poll_seconds)
            except asyncio.TimeoutError:pass
            if self.stop_event.is_set():
                await self.store.release_job_lease(context.job.id,self.worker_id,context.generation)
                return
            current=await self.store.get_job(context.job.id)
            participant=await self.store.get_pause_participant(context.job.thread_id,
                "child_harness",context.job.id)
            if participant and participant.state.value == "REQUESTED":
                await self._pause(adapter,context,external_id,participant.generation)
                return
            cancelling=bool(current and current.cancel_requested
                and context.request.cancellation_policy.get("supported") is not False)
            status=(await self._before_deadline(adapter.cancel(context,external_id),deadline)
                if cancelling else await self._external_status(adapter,context,external_id,deadline))
        await self._finish(adapter,context,external_id,status,deadline)

    async def _finish(self, adapter: JobAdapter, context: JobExecutionContext,
                      external_id: str, status: AdapterStatus, deadline) -> None:
        result = await self._before_deadline(self._collect_result(adapter,context,external_id,status),deadline)
        current=await self.store.get_job(context.job.id)
        if current and current.cancel_requested and context.request.cancellation_policy.get("supported") is False:
            result={**result,"soft_cancel_requested":True}
        state = status.state if status.state in TERMINAL_JOB_STATES else JobState.FAILED
        if state in {JobState.FAILED,JobState.BLOCKED,JobState.UNKNOWN}:
            result=normalize_job_error(result,side_effect_state=(
                'unknown' if state==JobState.UNKNOWN else ('accepted' if external_id else 'none')))
        await self.store.transition_job(context.job.id, state,
            expected_generation=context.generation, result=result)
        event('job.state.changed',job_id=context.job.id,generation=context.generation,
            external_execution_id=external_id,state_before=JobState.WAITING_EXTERNAL,
            state_after=state,outcome='success' if state==JobState.SUCCEEDED else 'failed',
            error_code=str(result.get('error_code') or ''))
        participant=await self.store.get_pause_participant(context.job.thread_id,
            "child_harness",context.job.id)
        if participant and participant.state.value == "REQUESTED":
            await self.store.complete_pause_participant(context.job.thread_id,
                participant.generation,"child_harness",context.job.id,
                external_execution_id=external_id,detail={"completed_at_pause_boundary":True})

    async def _pause(self,adapter:JobAdapter,context:JobExecutionContext,
                     external_id:str,generation:int)->None:
        detail={};failed=False
        if external_id:
            try:
                detail=await adapter.pause(context,external_id)
            except Exception as exc:
                failed=True;detail={"error":type(exc).__name__+": "+str(exc)}
        try:
            await self.store.complete_pause_participant(context.job.thread_id,generation,
                "child_harness",context.job.id,
                checkpoint_ref=str(detail.get("checkpoint_ref", "")),
                native_thread_id=str(detail.get("native_thread_id", "")),
                external_execution_id=external_id,detail=detail,failed=failed)
        finally:
            if external_id:
                await self.store.enqueue_job_cleanup(context.job.id,context.generation)

    async def _timeout(self,adapter,context,external_id,submission_started=False):
        if not external_id:
            provisional=await self.store.unresolved_job_submission(context.job.id,
                context.generation,include_current=True)
            phase=(provisional or {}).get('acceptance_receipt',{}).get('allocation_phase')
            if phase in {'reserving_resources','awaiting_capacity','allocating','preparing','absent'}:
                await self.store.transition_job(context.job.id,JobState.FAILED,
                    expected_generation=context.generation,result=normalize_job_error({
                        'error_code':'child_allocation_timeout' if phase!='preparing'
                            else 'child_preparation_failed','retryable':False,
                        'retry_class':'none'},side_effect_state='none'))
                return
            code=("job_timeout_submission_outcome_unknown" if submission_started
                else "job_timeout_before_submission")
            await self.store.transition_job(context.job.id,
                JobState.UNKNOWN if submission_started else JobState.FAILED,
                expected_generation=context.generation,result=normalize_job_error({
                    "error_code":code},side_effect_state='unknown' if submission_started else 'none'))
            return
        if context.request.cancellation_policy.get("supported") is not False:
            try:
                status=await asyncio.wait_for(adapter.cancel(context,external_id),timeout=5)
                if status.terminal:
                    result=await self._collect_result(adapter,context,external_id,status)
                    await self.store.transition_job(context.job.id,status.state,
                        expected_generation=context.generation,result=result);return
            except Exception:pass
        await self.store.transition_job(context.job.id,JobState.UNKNOWN,
            expected_generation=context.generation,result=normalize_job_error({
                "error_code":"job_timeout_outcome_unknown",
                "soft_cancel":context.request.cancellation_policy.get("supported") is False},
                side_effect_state='unknown'))

    async def run_cleanup_once(self,*,limit:int=10)->int:
        rows=await self.store.claim_job_cleanups(self.worker_id,self.lease_seconds,
            limit=limit,adapters=set(self.adapters))
        for row in rows:
            claim_trace=operation('job.cleanup.claim',new_trace=True,job_id=row['job_id'],
                generation=row['generation'])
            claim_trace.__enter__();add_link(row.get('traceparent',''))
            owner=asyncio.current_task();lost=asyncio.Event()
            async def heartbeat():
                while True:
                    await asyncio.sleep(max(.1,self.lease_seconds/3))
                    if not await self.store.renew_job_cleanup(row["cleanup_id"],self.worker_id,
                            self.lease_seconds):
                        lost.set();owner.cancel();return
            heartbeat_task=asyncio.create_task(heartbeat(),name="cleanup-heartbeat-"+row["job_id"])
            try:
                job=await self.store.get_job(row["job_id"]);request=await self.store.get_job_request(row["job_id"])
                adapter=self.adapters.get(row["adapter"])
                if not job or not request or not adapter:raise RuntimeError("cleanup_adapter_unavailable")
                event('job.cleanup.started',job_id=row['job_id'],generation=row['generation'],
                    external_execution_id=row['external_execution_id'],phase=row['phase'])
                context=JobExecutionContext(job=job,request=request,generation=row["generation"],
                    previous_external_execution_id=row["external_execution_id"],
                    previous_acceptance_receipt=row["acceptance_receipt"])
                if row["phase"]=="external_quiescence":
                    if context.previous_acceptance_receipt.get('allocation_phase')=='legacy_unknown':
                        raise RuntimeError('legacy_submission_requires_reconciliation')
                    resolver=getattr(adapter,'resolve_cleanup',None)
                    if resolver:
                        receipt=await resolver(context,row['external_execution_id'])
                        if receipt!=context.previous_acceptance_receipt:
                            await self.store.record_job_cleanup_receipt(row['cleanup_id'],
                                self.worker_id,receipt)
                            context=replace(context,previous_acceptance_receipt=receipt)
                    if row["external_execution_id"]:
                        cleanup_external=getattr(adapter,"cleanup_external",None)
                        if cleanup_external:await cleanup_external(context,row["external_execution_id"])
                        else:await adapter.cleanup(context,row["external_execution_id"])
                    await self.store.advance_job_cleanup(row["cleanup_id"],self.worker_id,
                        expected_phase="external_quiescence",next_phase="capacity_release")
                    row["phase"]="capacity_release"
                if row["phase"]=="capacity_release":
                    release_capacity=getattr(adapter,"release_capacity",None)
                    if release_capacity:await release_capacity(context)
                await self.store.finish_job_cleanup(row["cleanup_id"],self.worker_id,succeeded=True)
                event('job.cleanup.completed',job_id=row['job_id'],generation=row['generation'],
                    external_execution_id=row['external_execution_id'],state_after='completed',
                    outcome='success')
            except asyncio.CancelledError:
                if not lost.is_set():raise
            except Exception as exc:
                event("job.worker.cleanup_failed",level=30,job_id=row["job_id"],
                    exc=exc)
                await self.store.finish_job_cleanup(row["cleanup_id"],self.worker_id,
                    succeeded=False,error=type(exc).__name__+": "+str(exc),
                    retry_after_seconds=min(300,2**min(row["attempt_count"],9)))
            finally:
                heartbeat_task.cancel();await asyncio.gather(heartbeat_task,return_exceptions=True)
                claim_trace.__exit__(None,None,None)
        return len(rows)
