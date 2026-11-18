from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .job_contracts import JobRequest, JobState, JobView


@dataclass(frozen=True)
class JobExecutionContext:
    job: JobView
    request: JobRequest
    generation: int
    previous_external_execution_id: str = ""
    previous_acceptance_receipt: dict[str, Any] = field(default_factory=dict)


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


@dataclass(frozen=True)
class AdapterStatus:
    terminal: bool
    state: JobState = JobState.WAITING_EXTERNAL
    result: dict[str, Any] = field(default_factory=dict)


class PreAcceptFailure(RuntimeError):
    def __init__(self,message:str,*,infrastructure:bool=False,retry_after_seconds:float=0):
        super().__init__(message);self.infrastructure=infrastructure
        self.retry_after_seconds=max(0,float(retry_after_seconds))


class SubmissionOutcomeUnknown(RuntimeError):
    def __init__(self, message: str, *, external_execution_id: str = "",
                 acceptance_receipt: dict[str, Any] | None = None):
        super().__init__(message)
        self.external_execution_id = external_execution_id
        self.acceptance_receipt = acceptance_receipt or {"outcome": "unknown"}


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
