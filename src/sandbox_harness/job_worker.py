from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace

from .job_adapters import (
    AdapterStatus, JobAdapter, JobExecutionContext, PreAcceptFailure,
    SubmissionOutcomeUnknown,
)
from .job_contracts import JobKind, JobState, TERMINAL_JOB_STATES
from .observability import event


class JobWorker:
    def __init__(self, store, adapters: dict[str, JobAdapter], *, node_id: str,
                 concurrency: int = 4, lease_seconds: int = 60, worker_id: str = "",
                 poll_seconds: float = 1.0):
        self.store = store
        self.adapters = dict(adapters)
        self.node_id = node_id
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.worker_id = worker_id or "job-worker-" + uuid.uuid4().hex
        self.poll_seconds = poll_seconds
        self.stop_event = asyncio.Event()

    async def run_once(self) -> int:
        attempts = await self.store.claim_available_jobs(
            self.worker_id, self.node_id, self.lease_seconds,
            limit=self.concurrency, adapters=set(self.adapters),
        )
        if attempts:
            await asyncio.gather(*(self.process(attempt) for attempt in attempts))
        return len(attempts)

    async def run(self) -> None:
        tasks: set[asyncio.Task] = set()
        while not self.stop_event.is_set():
            completed={task for task in tasks if task.done()}
            for task in completed:await task
            tasks-=completed
            capacity=max(0,self.concurrency-len(tasks));attempts=[]
            if capacity:
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
            state=JobState.UNKNOWN if job.kind==JobKind.PROVIDER else JobState.FAILED
            try:
                await self.store.transition_job(job.id,state,expected_generation=attempt.generation,
                    result={'error_code':'worker_internal_error','exception_type':type(exc).__name__,
                        'error_detail':str(exc)[:2000]})
            except RuntimeError as stale:
                if str(stale)!='stale_job_generation':raise
                return
            accepted=await self.store.latest_job_acceptance(job.id)
            request=await self.store.get_job_request(job.id);adapter=self.adapters.get(job.adapter)
            if accepted and request and adapter:
                context=JobExecutionContext(job=job,request=request,generation=attempt.generation,
                    previous_external_execution_id=accepted.external_execution_id,
                    previous_acceptance_receipt=accepted.acceptance_receipt)
                await self._cleanup(adapter,context,accepted.external_execution_id)

    async def process(self, attempt, *, continuous: bool = False) -> None:
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
        try:
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
                status = await adapter.cancel(context, external_id)
                await self._finish_or_defer(adapter,context,external_id,status,continuous)
                return
            if request.reconciles_job_id:
                status = await adapter.reconcile(context, external_id)
                receipt = {"reconciles_job_id": request.reconciles_job_id,
                    "external_execution_id": external_id}
                await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                    external_execution_id=external_id, receipt=receipt)
                await self._finish_or_defer(adapter,context,external_id,status,continuous)
                return
            if not accepted:
                try:
                    submission = await adapter.submit(context)
                except PreAcceptFailure as exc:
                    maximum = int(request.retry_policy.get("max_pre_accept_attempts", 1))
                    retry = attempt.generation < max(1, maximum)
                    await self.store.finish_pre_accept_attempt(job.id, self.worker_id, attempt.generation,
                        retry=retry, error_code="pre_accept_failure", error_detail=str(exc)[:2000])
                    return
                except SubmissionOutcomeUnknown as exc:
                    await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                        external_execution_id=exc.external_execution_id,
                        receipt=exc.acceptance_receipt)
                    if exc.external_execution_id:
                        context=replace(context,previous_external_execution_id=exc.external_execution_id,
                            previous_acceptance_receipt=exc.acceptance_receipt)
                        status=await adapter.status(context,exc.external_execution_id)
                        await self._finish_or_defer(adapter,context,exc.external_execution_id,
                            status,continuous)
                    else:
                        await self.store.transition_job(job.id, JobState.UNKNOWN,
                            expected_generation=attempt.generation,
                            result={"error_code": "submission_outcome_unknown",
                                "error_detail": str(exc)[:2000]})
                    return
                except Exception as exc:
                    deterministic = bool(request.retry_policy.get("deterministic"))
                    if request.kind == JobKind.OPERATION and deterministic:
                        maximum = int(request.retry_policy.get("max_pre_accept_attempts", 1))
                        await self.store.finish_pre_accept_attempt(job.id, self.worker_id,
                            attempt.generation, retry=attempt.generation < max(1, maximum),
                            error_code="operation_start_failed", error_detail=type(exc).__name__)
                    else:
                        await self.store.record_job_acceptance(job.id, self.worker_id,
                            attempt.generation, external_execution_id="",
                            receipt={"outcome": "unknown", "exception_type": type(exc).__name__})
                        await self.store.transition_job(job.id, JobState.UNKNOWN,
                            expected_generation=attempt.generation,
                            result={"error_code": "submission_outcome_unknown"})
                    return
                external_id = submission.external_execution_id
                await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                    external_execution_id=external_id, receipt=submission.acceptance_receipt)
                context = replace(context, previous_external_execution_id=external_id,
                    previous_acceptance_receipt=submission.acceptance_receipt)
                if submission.completed:
                    result=await adapter.collect(context,external_id,AdapterStatus(
                        True,JobState.SUCCEEDED,submission.result))
                    await self.store.transition_job(job.id, JobState.SUCCEEDED,
                        expected_generation=attempt.generation, result=result)
                    await self._cleanup(adapter, context, external_id)
                    return
            status = await adapter.status(context, external_id)
            await self._finish_or_defer(adapter,context,external_id,status,continuous)
        except RuntimeError as exc:
            if str(exc) == "stale_job_generation":
                event("job.worker.stale_generation", level=30, job_id=job.id,
                    generation=attempt.generation)
                return
            raise

    async def _finish_or_defer(self,adapter:JobAdapter,context:JobExecutionContext,
                               external_id:str,status:AdapterStatus,continuous:bool)->None:
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
            status=(await adapter.cancel(context,external_id) if current and current.cancel_requested
                else await adapter.status(context,external_id))
        await self._finish(adapter,context,external_id,status)

    async def _finish(self, adapter: JobAdapter, context: JobExecutionContext,
                      external_id: str, status: AdapterStatus) -> None:
        result = await adapter.collect(context, external_id, status)
        state = status.state if status.state in TERMINAL_JOB_STATES else JobState.FAILED
        await self.store.transition_job(context.job.id, state,
            expected_generation=context.generation, result=result)
        await self._cleanup(adapter, context, external_id)
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
                await self._cleanup(adapter,context,external_id)

    @staticmethod
    async def _cleanup(adapter: JobAdapter, context: JobExecutionContext, external_id: str) -> None:
        try:
            await adapter.cleanup(context, external_id)
        except Exception as exc:
            event("job.worker.cleanup_failed", level=30, job_id=context.job.id,
                exception_type=type(exc).__name__)
