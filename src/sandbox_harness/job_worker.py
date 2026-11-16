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
                 concurrency: int = 4, lease_seconds: int = 60, worker_id: str = ""):
        self.store = store
        self.adapters = dict(adapters)
        self.node_id = node_id
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.worker_id = worker_id or "job-worker-" + uuid.uuid4().hex
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
        while not self.stop_event.is_set():
            count = await self.run_once()
            if count:
                continue
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

    async def process(self, attempt) -> None:
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
            if job.cancel_requested:
                if not external_id:
                    await self.store.transition_job(job.id, JobState.CANCELLED,
                        expected_generation=attempt.generation,
                        result={"cancelled_before_acceptance": True})
                    return
                status = await adapter.cancel(context, external_id)
                if not status.terminal:
                    await self.store.defer_accepted_job(job.id, self.worker_id, attempt.generation)
                    return
                await self._finish(adapter, context, external_id, status)
                return
            if request.reconciles_job_id:
                status = await adapter.reconcile(context, external_id)
                receipt = {"reconciles_job_id": request.reconciles_job_id,
                    "external_execution_id": external_id}
                await self.store.record_job_acceptance(job.id, self.worker_id, attempt.generation,
                    external_execution_id=external_id, receipt=receipt)
                if not status.terminal:
                    await self.store.defer_accepted_job(job.id, self.worker_id, attempt.generation)
                    return
                await self._finish(adapter, context, external_id, status)
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
                        await self.store.defer_accepted_job(job.id, self.worker_id, attempt.generation)
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
            if not status.terminal:
                await self.store.defer_accepted_job(job.id, self.worker_id, attempt.generation)
                return
            await self._finish(adapter, context, external_id, status)
        except RuntimeError as exc:
            if str(exc) == "stale_job_generation":
                event("job.worker.stale_generation", level=30, job_id=job.id,
                    generation=attempt.generation)
                return
            raise

    async def _finish(self, adapter: JobAdapter, context: JobExecutionContext,
                      external_id: str, status: AdapterStatus) -> None:
        result = await adapter.collect(context, external_id, status)
        state = status.state if status.state in TERMINAL_JOB_STATES else JobState.FAILED
        await self.store.transition_job(context.job.id, state,
            expected_generation=context.generation, result=result)
        await self._cleanup(adapter, context, external_id)

    @staticmethod
    async def _cleanup(adapter: JobAdapter, context: JobExecutionContext, external_id: str) -> None:
        try:
            await adapter.cleanup(context, external_id)
        except Exception as exc:
            event("job.worker.cleanup_failed", level=30, job_id=context.job.id,
                exception_type=type(exc).__name__)
