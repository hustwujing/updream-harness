from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace
from datetime import datetime,timezone

import httpx

from .job_adapters import (
    AdapterStatus, ExternalJobFailure, JobAdapter, JobExecutionContext, PreAcceptFailure,
    SubmissionOutcomeUnknown,
)
from .job_contracts import JobKind, JobState, TERMINAL_JOB_STATES
from .job_errors import normalize_job_error
from .observability import event,sanitize
from .sandbox_contracts import (
    SandboxEndpointUnavailable, SandboxProviderUnavailable, SandboxRateLimited,
)


class JobDeadlineExceeded(TimeoutError):
    pass


class JobWorker:
    def __init__(self, store, adapters: dict[str, JobAdapter], *, node_id: str,
                 concurrency: int = 4, lease_seconds: int = 60, worker_id: str = "",
                 poll_seconds: float = 1.0, review_dispatcher=None):
        self.store = store
        self.adapters = dict(adapters)
        self.node_id = node_id
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.worker_id = worker_id or "job-worker-" + uuid.uuid4().hex
        self.poll_seconds = poll_seconds
        self.stop_event = asyncio.Event()
        self.review_dispatcher = review_dispatcher

    async def _collect_result(self,adapter,context,external_id,status):
        try:result=await adapter.collect(context,external_id,status)
        except ExternalJobFailure:raise
        except Exception as exc:
            event('job.adapter.collection_failed',level=40,job_id=context.job.id,
                generation=context.generation,exception_type=type(exc).__name__,exc=exc)
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
                    exception_type=type(exc).__name__)
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
        while not self.stop_event.is_set():
            completed={task for task in tasks if task.done()}
            for task in completed:await task
            tasks-=completed
            capacity=max(0,self.concurrency-len(tasks));attempts=[]
            if capacity:
                await self.run_cleanup_once(limit=capacity)
                attempts=await self.store.claim_available_jobs(
                    self.worker_id,self.node_id,self.lease_seconds,limit=capacity,
                    adapters=set(self.adapters))
                tasks.update(asyncio.create_task(self._process_isolated(attempt),
                    name='job-'+attempt.job_id) for attempt in attempts)
            if not attempts:
                try:await asyncio.wait_for(self.stop_event.wait(),timeout=0.5)
                except asyncio.TimeoutError:pass
        await asyncio.gather(*tasks)

    async def _process_isolated(self,attempt)->None:
        try:await self.process(attempt,continuous=True)
        except Exception as exc:
            event('job.worker.process_failed',level=40,job_id=attempt.job_id,
                generation=attempt.generation,exception_type=type(exc).__name__,exc=exc)
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

    async def process(self, attempt, *, continuous: bool = False) -> None:
        lost=asyncio.Event();owner=asyncio.current_task()
        async def heartbeat():
            while True:
                await asyncio.sleep(max(.1,self.lease_seconds/3))
                if not await self.store.renew_job_lease(attempt.job_id,self.worker_id,
                        attempt.generation,self.lease_seconds):
                    lost.set();owner.cancel();return
        task=asyncio.create_task(heartbeat(),name="job-heartbeat-"+attempt.job_id)
        try:
            await self._process_claimed(attempt,continuous=continuous)
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
                generation=context.generation,exception_type=type(exc).__name__,exc=exc)
            raise ExternalJobFailure('外部任务状态查询失败',
                error_code='external_status_unavailable',failed_phase='status',
                terminal_state=JobState.UNKNOWN,retryable=True,retry_class='reconcile_only',
                side_effect_state='unknown') from exc

    async def _process_claimed(self, attempt, *, continuous: bool = False) -> None:
        job = await self.store.get_job(attempt.job_id)
        request = await self.store.get_job_request(attempt.job_id)
        if not job or not request:
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
        )
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
            if participant and participant.state.value == "RESUMED":
                accepted = None
                external_id = ""
                context = replace(context, previous_external_execution_id="",
                    previous_acceptance_receipt={})
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
                try:
                    while True:
                        try:
                            submission=(submission or await self._before_deadline(
                                adapter.submit(context),deadline))
                            break
                        except PreAcceptFailure as exc:
                            maximum = int(request.retry_policy.get("max_pre_accept_attempts", 1))
                            retry = exc.retryable and (exc.retry_class=='capacity_wait'
                                or attempt.generation < max(1, maximum))
                            if (exc.infrastructure and exc.retryable and continuous
                                    and exc.retry_class=='capacity_wait'):
                                await self.store.mark_infrastructure_wait(job.id,self.worker_id,
                                    attempt.generation,error_detail=str(exc))
                                if exc.retry_after_seconds:
                                    await self._before_deadline(
                                        asyncio.sleep(exc.retry_after_seconds),deadline)
                                if not self.stop_event.is_set():
                                    await self.store.mark_job_submission_started(
                                        job.id,self.worker_id,attempt.generation)
                                    continue
                            elif retry and exc.retry_after_seconds:
                                await self._before_deadline(
                                    asyncio.sleep(exc.retry_after_seconds),deadline)
                            await self.store.finish_pre_accept_attempt(job.id,self.worker_id,
                                attempt.generation,retry=retry,error_code=(
                                    "infrastructure_capacity_wait" if exc.infrastructure
                                        and exc.error_code=='pre_accept_failure' else exc.error_code),
                                error_detail=str(exc)[:2000],error_metadata={
                                    'failed_phase':exc.failed_phase,'retry_class':exc.retry_class,
                                    'retryable':exc.retryable,
                                    'node_health_generation':exc.node_health_generation})
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
                context = replace(context, previous_external_execution_id=external_id,
                    previous_acceptance_receipt=submission.acceptance_receipt)
                if submission.completed:
                    result=await self._before_deadline(self._collect_result(adapter,context,external_id,AdapterStatus(
                        True,JobState.SUCCEEDED,submission.result)),deadline)
                    await self.store.transition_job(job.id, JobState.SUCCEEDED,
                        expected_generation=attempt.generation, result=result)
                    return
            status=await self._external_status(adapter,context,external_id,deadline)
            await self._finish_or_defer(adapter,context,external_id,status,continuous,deadline)
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
        rows=await self.store.claim_job_cleanups(self.worker_id,self.lease_seconds,limit=limit)
        for row in rows:
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
                context=JobExecutionContext(job=job,request=request,generation=row["generation"],
                    previous_external_execution_id=row["external_execution_id"],
                    previous_acceptance_receipt=row["acceptance_receipt"])
                if row["phase"]=="external_quiescence":
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
            except asyncio.CancelledError:
                if not lost.is_set():raise
            except Exception as exc:
                event("job.worker.cleanup_failed",level=30,job_id=row["job_id"],
                    exception_type=type(exc).__name__)
                await self.store.finish_job_cleanup(row["cleanup_id"],self.worker_id,
                    succeeded=False,error=type(exc).__name__+": "+str(exc))
            finally:
                heartbeat_task.cancel();await asyncio.gather(heartbeat_task,return_exceptions=True)
        return len(rows)
