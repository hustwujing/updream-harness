from __future__ import annotations

import asyncio
from dataclasses import asdict,replace
from .domain import Harness
from .job_adapters import PreAcceptCancelled,PreAcceptFailure,SubmissionInterrupted,SubmissionRecovery
from .sandbox import Sandbox,SandboxCapacityUnavailable,SandboxNotFound,SandboxRateLimited
from .sandbox_contracts import (SandboxCreateOutcomeUnknown,SandboxEndpointUnavailable,
    SandboxHandle,SandboxProfile,SandboxProfileUnavailable,SandboxProviderUnavailable)
from .sandbox_factory import SandboxProfileResolver


class SandboxAllocationOwner:
    """Provider-neutral durable allocation before any sandbox Job execution."""
    allocation_poll_seconds=1.0

    def _allocation_profile(self,reservation,harness,release):
        resolver=getattr(self.pool,'profile_resolver',None) or SandboxProfileResolver(
            provider=getattr(self.pool.backend,'provider',self.settings.backend),
            runtime_contract='sandbox-harness-runtime/v1',
            runtime_bundle_digest=self.settings.runtime_bundle_digest,
            runtime_code_bundle_digest=self.settings.runtime_code_bundle_digest)
        return resolver.resolve(harness,release.ref,profile_id=reservation.runtime_profile_id,
            revision=release.revision)


    async def _check_cancelled(self,context):
        reader=getattr(self.store,'get_job',None)
        current=await reader(context.job.id) if reader else None
        if current and current.cancel_requested:raise PreAcceptCancelled()


    @staticmethod
    def _validate_allocation(sandbox,receipt):
        profile=SandboxProfile(**receipt['allocation_profile'])
        if (sandbox.handle.provider!=receipt['allocation_provider']
                or sandbox.handle.allocation_id!=receipt['allocation_id']
                or sandbox.handle.profile_key!=profile.compatibility_key
                or sandbox.compatibility_key!=profile.compatibility_key):
            raise SandboxProfileUnavailable('Allocation identity/profile mismatch',
                provider=receipt['allocation_provider'])
        return profile


    async def _reconcile_allocation(self,context,receipt,*,ready):
        if self.pool.backend.provider!=receipt['allocation_provider']:
            raise SandboxProfileUnavailable('Allocation provider is not active',
                provider=receipt['allocation_provider'])
        while True:
            await self._check_cancelled(context)
            try:
                sandbox=await self.pool.reconcile_create(receipt['allocation_id'])
                if sandbox:
                    profile=self._validate_allocation(sandbox,receipt)
                    sandbox=replace(sandbox,profile=profile,
                        handle=replace(sandbox.handle,revision=profile.revision))
                    if ready:
                        sandbox=await self.pool.backend.get(sandbox.handle)
                        return await self.pool.adopt(sandbox,
                            thread_id=self._thread_id(context.job.id),turn_id=context.job.id,
                            expected_profile=profile)
                    return sandbox
            except (SandboxNotFound,SandboxEndpointUnavailable,SandboxProviderUnavailable,
                    SandboxRateLimited):pass
            await asyncio.sleep(self.allocation_poll_seconds)


    def _unstarted_allocation(self,context,receipt):
        handle=SandboxHandle.from_mapping(receipt['sandbox_handle'])
        profile=SandboxProfile(**receipt['allocation_profile'])
        sandbox=Sandbox(id=handle.sandbox_id,provider=handle.provider,
            harness=Harness(receipt.get('allocation_harness') or
                context.request.parameters.get('harness','codex')),
            profile=profile,handle=handle,metadata={})
        self._validate_allocation(sandbox,receipt)
        return sandbox


    async def acquire_allocation(self,context,reservation,execution_id):
        harness=Harness(reservation.harness)
        release=self._provider_release(reservation,harness);image=release.ref
        await self._check_cancelled(context)
        profile=self._allocation_profile(reservation,harness,release)
        allocation={'allocation_id':context.job.id,'allocation_provider':profile.provider,
            'allocation_harness':harness.value,
            'allocation_profile':asdict(profile),'allocation_phase':'awaiting_capacity',
            'reservation_id':reservation.id,'reservation_generation':context.generation,
            'node_id':reservation.node_id,
            'node_generation':reservation.node_generation}
        try:
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id=execution_id,receipt=allocation)
        except Exception as exc:
            if isinstance(exc,RuntimeError) and str(exc)=='stale_job_generation':raise
            raise PreAcceptFailure('Allocation intent could not be persisted',
                error_code='child_allocation_failed',failed_phase='submission',
                retryable=True,retry_class='bounded_pre_accept') from exc
        async def before_create(_request):
            await self._check_cancelled(context)
            allocation['allocation_phase']='allocating'
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id=execution_id,receipt=allocation)
        async def owns_allocation():
            owned=getattr(self.pool,'owns_allocation',None)
            return bool(owned and await owned(profile.provider,context.job.id))
        async def mark_absent():
            allocation['allocation_phase']='absent'
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id='',receipt=allocation)
        try:
            try:
                sandbox=await self.pool.acquire(thread_id=self._thread_id(context.job.id),
                    turn_id=context.job.id,harness=harness,environment={},image=image,
                    profile_id=reservation.runtime_profile_id,revision=release.revision,
                    retain_unresolved_allocation=True,before_create=before_create)
            except SandboxCreateOutcomeUnknown:
                sandbox=await self._reconcile_allocation(context,allocation,ready=True)
        except asyncio.CancelledError as exc:
            if await owns_allocation():
                raise SubmissionInterrupted(execution_id,allocation) from exc
            await mark_absent()
            raise SubmissionInterrupted(execution_id,allocation) from exc
        except PreAcceptCancelled:raise
        except (SandboxCapacityUnavailable,SandboxRateLimited):
            await mark_absent()
            raise
        except Exception as exc:
            if isinstance(exc,RuntimeError) and str(exc)=='stale_job_generation':raise
            owned=await owns_allocation()
            if not owned:await mark_absent()
            raise PreAcceptFailure('Child sandbox allocation failed before execution',
                error_code='child_allocation_failed',failed_phase='submission',
                retryable=False,retain_capacity=owned) from exc
        allocation={**allocation,'allocation_phase':'preparing',
            'sandbox_id':sandbox.id,'sandbox_handle':sandbox.handle.to_mapping()}
        try:
            await self.store.record_job_submission_allocation(context.job.id,context.generation,
                external_execution_id=execution_id,receipt=allocation)
        except asyncio.CancelledError as exc:
            raise SubmissionInterrupted(execution_id,allocation) from exc
        except Exception as exc:
            raise PreAcceptFailure('Allocated sandbox requires durable cleanup',
                error_code='child_allocation_failed',failed_phase='submission',
                retryable=False,retain_capacity=True) from exc
        return sandbox,allocation

    async def recover_allocation(self,context,reservation):
        receipt=context.previous_acceptance_receipt
        if receipt.get('allocation_phase') in {'awaiting_capacity','absent'}:
            return SubmissionRecovery(safe_to_retry=True)
        if receipt.get('allocation_phase')=='allocating':
            if not receipt.get('sandbox_handle'):
                sandbox=await self._reconcile_allocation(context,receipt,ready=False)
                await self.store.record_recovered_job_allocation(context.job.id,context.generation,
                    reservation.job_generation,sandbox.handle)
                receipt={**receipt,'sandbox_id':sandbox.id,
                    'sandbox_handle':sandbox.handle.to_mapping()}
            sandbox=self._unstarted_allocation(context,receipt)
            await self.pool.retire(sandbox)
            if await self.pool.retirement_pending(sandbox):
                raise SandboxProviderUnavailable('Allocation deletion is pending',
                    provider=sandbox.handle.provider)
            await self.store.mark_recovered_allocation_absent(context.job.id,context.generation,
                reservation.job_generation)
            return SubmissionRecovery(safe_to_retry=True)
        return None

    async def resolve_cleanup(self,context,external_execution_id,receipt):
        if receipt.get('allocation_phase')!='allocating' or receipt.get('sandbox_handle'):
            return receipt
        if self.pool.backend.provider!=receipt['allocation_provider']:
            raise SandboxProfileUnavailable('Allocation provider is not active',
                provider=receipt['allocation_provider'])
        sandbox=await self.pool.reconcile_create(receipt['allocation_id'])
        if not sandbox:
            raise SandboxCreateOutcomeUnknown('Allocation cleanup is awaiting visibility',
                provider=receipt['allocation_provider'])
        profile=self._validate_allocation(sandbox,receipt)
        sandbox=replace(sandbox,profile=profile,
            handle=replace(sandbox.handle,revision=profile.revision))
        return {**receipt,'sandbox_id':sandbox.id,'sandbox_handle':sandbox.handle.to_mapping()}
