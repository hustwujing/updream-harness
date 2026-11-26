from __future__ import annotations

from .job_adapters import AdapterStatus, AdapterSubmission, JobExecutionContext
from .job_contracts import JobState
from .common.jobs.review import (ReviewTrigger, ReviewVerdictKind, ReviewCandidate,
    ReviewTarget, ReviewRequest, ReviewSubmission, ReviewVerdict, ReviewerCoordinator)


class ReviewerJobAdapter:
    def __init__(self,delegate,store=None):self.delegate=delegate;self.store=store
    async def submit(self,context:JobExecutionContext)->AdapterSubmission:return await self.delegate.submit(context)
    async def recover_submission(self,context:JobExecutionContext):
        return await self.delegate.recover_submission(context)
    async def resolve_cleanup(self,context:JobExecutionContext,external_execution_id:str):
        resolver=getattr(self.delegate,'resolve_cleanup',None)
        return (await resolver(context,external_execution_id) if resolver
            else context.previous_acceptance_receipt)
    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.delegate.status(context,external_execution_id)
    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.delegate.cancel(context,external_execution_id)
    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.delegate.reconcile(context,external_execution_id)
    async def cleanup(self,context:JobExecutionContext,external_execution_id:str)->None:
        await self.delegate.cleanup(context,external_execution_id)
    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        await self.delegate.cleanup_external(context,external_execution_id)
    async def release_capacity(self,context:JobExecutionContext)->None:
        await self.delegate.release_capacity(context)
    async def collect(self,context:JobExecutionContext,external_execution_id:str,status:AdapterStatus)->dict:
        result=await self.delegate.collect(context,external_execution_id,status)
        if status.state!=JobState.SUCCEEDED:
            return result
        try:
            verdict=ReviewVerdict.model_validate(result.get('review',result))
            expected=tuple(context.request.parameters['review_context']['artifact_digests'])
            if verdict.artifact_digests!=expected:raise ValueError('review_artifact_digest_mismatch')
            gate=(await self.store.apply_review_gate(context.job.id,
                receipt=verdict.model_dump(mode='json')) if self.store else None)
        except Exception:raise
        return {'review':verdict.model_dump(mode='json'),**({"review_gate":gate} if gate else {})}
