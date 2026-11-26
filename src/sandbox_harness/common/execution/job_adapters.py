from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol

from sandbox_harness.common.contracts.job_contracts import (JobRequest, JobState, JobView)
from sandbox_harness.common.contracts.job_errors import (job_error_payload)


@dataclass(frozen=True)
class JobExecutionContext:
    job: JobView
    request: JobRequest
    generation: int
    previous_external_execution_id: str = ""
    previous_acceptance_receipt: dict[str, Any] = field(default_factory=dict)
    allocation_replacement: bool = False


@dataclass(frozen=True)
class AdapterSubmission:
    external_execution_id: str
    acceptance_receipt: dict[str, Any]
    completed: bool = False
    result: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SubmissionRecovery:
    submission: AdapterSubmission | None = None
    safe_to_retry: bool = False

    def __post_init__(self):
        if self.submission is not None and self.safe_to_retry:
            raise ValueError("Submission recovery cannot be accepted and retryable")


async def reserve_job_resources(context:JobExecutionContext,scheduler,resources):
    """Persist the pre-external-call boundary before acquiring any capacity."""
    await scheduler.store.record_job_submission_allocation(context.job.id,context.generation,
        external_execution_id=context.job.id+':'+str(context.generation),
        receipt={'allocation_phase':'reserving_resources','reservation_generation':context.generation})
    try:
        return await scheduler.reserve(context.job.id,context.generation,resources)
    except RuntimeError as exc:
        if str(exc)=='no_compatible_node_capacity':
            raise PreAcceptFailure(str(exc),infrastructure=True,retry_after_seconds=1) from exc
        raise


async def recover_resource_reservation(context:JobExecutionContext,scheduler)->bool:
    receipt=context.previous_acceptance_receipt
    if receipt.get('allocation_phase') not in {'reserving_resources','awaiting_capacity','absent'}:
        return False
    await scheduler.store.release_unstarted_job_reservation(context.job.id,context.generation,
        int(receipt['reservation_generation']) if receipt.get('reservation_generation') else None,
        reservation_id=str(receipt.get('reservation_id','')))
    return True


@dataclass(frozen=True)
class AdapterStatus:
    terminal: bool
    state: JobState = JobState.WAITING_EXTERNAL
    result: dict[str, Any] = field(default_factory=dict)


class PreAcceptFailure(RuntimeError):
    def __init__(self,message:str,*,infrastructure:bool=False,retry_after_seconds:float=0,
                 error_code:str='pre_accept_failure',failed_phase:str='',
                 retry_class:str='none',retryable:bool|None=None,node_health_generation:int=0,
                 retain_capacity:bool=False):
        super().__init__(message);self.infrastructure=infrastructure
        self.retry_after_seconds=max(0,float(retry_after_seconds))
        self.error_code=error_code
        self.failed_phase=failed_phase
        self.retry_class=('capacity_wait' if infrastructure and retry_class=='none' else retry_class)
        self.retryable=True if retryable is None else retryable
        self.node_health_generation=max(0,int(node_health_generation))
        self.retain_capacity=retain_capacity


class PreAcceptCancelled(RuntimeError):
    """Durable user cancellation observed before Runtime submission."""
    def __init__(self,*,retain_allocation:bool=False):
        super().__init__();self.retain_allocation=retain_allocation


class AllocationRecoveryRequired(PreAcceptFailure):
    """An issued environment creation exceeded its frozen recovery window."""
    retain_allocation=True

    def __init__(self,identity:str):
        super().__init__('allocation_recovery_required',error_code='allocation_recovery_required',
            failed_phase='reconciliation',retryable=False,retain_capacity=True)
        self.identity=identity


class SubmissionOutcomeUnknown(RuntimeError):
    def __init__(self, message: str, *, external_execution_id: str = "",
                 acceptance_receipt: dict[str, Any] | None = None):
        super().__init__(message)
        self.external_execution_id = external_execution_id
        self.acceptance_receipt = acceptance_receipt or {"outcome": "unknown"}


class SubmissionInterrupted(asyncio.CancelledError):
    """Cancellation after submission: durable allocation still owns capacity."""

    def __init__(self,external_execution_id:str,acceptance_receipt:dict[str,Any]):
        super().__init__()
        self.external_execution_id=external_execution_id
        self.acceptance_receipt=acceptance_receipt


class ExternalJobFailure(RuntimeError):
    def __init__(self,message:str,*,error_code:str,failed_phase:str,
                 terminal_state:JobState=JobState.FAILED,retryable:bool|None=None,
                 retry_class:str|None=None,side_effect_state:str|None=None):
        super().__init__(message);self.terminal_state=terminal_state
        self.result=job_error_payload(error_code,detail=message,phase=failed_phase,
            retryable=retryable,retry_class=retry_class,side_effect_state=side_effect_state)


class JobAdapter(Protocol):
    async def submit(self, context: JobExecutionContext) -> AdapterSubmission: ...
    async def status(self, context: JobExecutionContext, external_execution_id: str) -> AdapterStatus: ...
    async def cancel(self, context: JobExecutionContext, external_execution_id: str) -> AdapterStatus: ...
    async def pause(self, context: JobExecutionContext, external_execution_id: str) -> dict[str, Any]: ...
    async def collect(self, context: JobExecutionContext, external_execution_id: str,
                      status: AdapterStatus) -> dict[str, Any]: ...
    async def reconcile(self, context: JobExecutionContext, external_execution_id: str) -> AdapterStatus: ...
    async def cleanup(self, context: JobExecutionContext, external_execution_id: str) -> None: ...
    async def cleanup_external(self, context: JobExecutionContext, external_execution_id: str) -> None: ...
    async def release_capacity(self, context: JobExecutionContext) -> None: ...


class BaseJobAdapter:
    async def recover_submission(self,context:JobExecutionContext)->SubmissionRecovery:
        return SubmissionRecovery()

    async def status(self, context: JobExecutionContext, external_execution_id: str) -> AdapterStatus:
        return AdapterStatus(terminal=False)

    async def cancel(self, context: JobExecutionContext, external_execution_id: str) -> AdapterStatus:
        return AdapterStatus(terminal=False, state=JobState.CANCELLING)

    async def pause(self, context: JobExecutionContext, external_execution_id: str) -> dict[str, Any]:
        raise RuntimeError("job_pause_not_supported")

    async def collect(self, context: JobExecutionContext, external_execution_id: str,
                      status: AdapterStatus) -> dict[str, Any]:
        return status.result

    async def reconcile(self, context: JobExecutionContext, external_execution_id: str) -> AdapterStatus:
        return await self.status(context, external_execution_id)

    async def cleanup(self, context: JobExecutionContext, external_execution_id: str) -> None:
        return None

    async def cleanup_external(self, context: JobExecutionContext, external_execution_id: str) -> None:
        await self.cleanup(context,external_execution_id)

    async def release_capacity(self, context: JobExecutionContext) -> None:
        return None
