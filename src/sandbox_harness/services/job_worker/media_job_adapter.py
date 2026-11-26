from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
from sandbox_harness.common.contracts.domain import (MediaDraftCreate, now)
from sandbox_harness.common.execution.job_adapters import (AdapterStatus, AdapterSubmission, BaseJobAdapter, JobExecutionContext, PreAcceptFailure)
from sandbox_harness.common.contracts.job_contracts import (JobState)
from sandbox_harness.common.contracts.job_errors import (normalize_job_error)
from sandbox_harness.common.catalog.provider_preflight import (CAPABILITY_SCOPES, ProviderPreflight, ProviderPreflightError)


class MediaProviderJobAdapter(BaseJobAdapter):
    def __init__(self,settings,store,objects,catalog):
        self.settings=settings;self.store=store;self.objects=objects;self.catalog=catalog
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'provider-job-artifacts',
            maximum_bytes=settings.max_artifact_bytes)
        self.preflight=ProviderPreflight(store,objects,catalog)

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        request=context.request
        try:
            validated=await self.preflight.inspect(request)
        except ProviderPreflightError as exc:
            raise PreAcceptFailure('Media Provider input validation failed',
                error_code='provider_input_invalid',failed_phase=(
                    'authorization' if exc.code=='capability_scope_invalid' else 'input_validation'),
                retryable=False) from exc
        owner_id=await self.store.owner_id_for_thread(context.job.thread_id)
        if not owner_id:raise KeyError('media_provider_thread_not_found')
        value=MediaDraftCreate(kind=validated.kind,model=validated.model,
            generation_mode=validated.generation_mode,
            parameters=validated.parameters,references=[],idempotency_key=request.idempotency_key,
            turn_id='',output_role='candidate')
        draft,_=await self.store.create_media_draft(owner_id,context.job.thread_id,value,
            expires_at=now()+timedelta(seconds=self.settings.media_draft_ttl_seconds),
            provider_spec=validated.spec,reference_snapshots=validated.references)
        media_job=await self.store.confirm_media_draft(draft.id,owner_id,provider_spec=validated.spec)
        return AdapterSubmission(external_execution_id=media_job.id,acceptance_receipt={
            'media_job_id':media_job.id,'provider':media_job.provider,'provider_adapter':media_job.adapter,
            'execution_mode':media_job.execution_mode})

    async def resolve_execution(self,context:JobExecutionContext,external_execution_id:str)->str:
        return external_execution_id

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        external_execution_id=await self.resolve_execution(context,external_execution_id)
        owner_id=await self.store.owner_id_for_thread(context.job.thread_id)
        value=await self.store.get_media_job(external_execution_id,owner_id)
        if not value:raise RuntimeError('accepted_media_job_missing')
        if value.status=='succeeded':return AdapterStatus(True,JobState.SUCCEEDED,{
            'media_job_id':value.id,'provider':value.provider,'credits_consumed':value.credits_consumed})
        if value.status=='failed':return AdapterStatus(True,JobState.FAILED,normalize_job_error({
            'media_job_id':value.id,'error_code':value.error_code,'error_detail':value.error_detail,
            'failed_phase':value.failed_phase,'retryable':value.retryable,
            'retry_class':value.retry_class,'side_effect_state':value.side_effect_state}))
        if value.status=='cancelled':return AdapterStatus(True,JobState.CANCELLED,{'media_job_id':value.id})
        if value.status=='unknown':return AdapterStatus(True,JobState.UNKNOWN,{
            'media_job_id':value.id,'error_code':value.error_code,'error_detail':value.error_detail})
        return AdapterStatus(False,JobState.CANCELLING if value.status=='cancel_requested'
            else JobState.WAITING_EXTERNAL,{'media_job_id':value.id,'phase':value.phase})

    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.status(context,external_execution_id)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        external_execution_id=await self.resolve_execution(context,external_execution_id)
        current=await self.status(context,external_execution_id)
        if current.terminal:return current
        owner_id=await self.store.owner_id_for_thread(context.job.thread_id)
        try:value=await self.store.request_media_cancel(external_execution_id,owner_id)
        except RuntimeError:return AdapterStatus(False,JobState.CANCELLING,{
            'media_job_id':external_execution_id,'cancel_refused':True})
        return AdapterStatus(False,JobState.CANCELLING,{'media_job_id':value.id})

    async def outputs(self,context:JobExecutionContext,external_execution_id:str)->list[dict]:
        output=await self.store.media_job_output_for_thread(external_execution_id,context.job.thread_id)
        return [{"role":"primary",**output}] if output else []

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict:
        if status.state!=JobState.SUCCEEDED:return status.result
        external_execution_id=await self.resolve_execution(context,external_execution_id)
        outputs=await self.outputs(context,external_execution_id)
        if not outputs:raise RuntimeError('media_job_output_unavailable')
        if len(context.request.expected_outputs)!=1:
            raise RuntimeError('media_provider_output_contract_requires_one_role')
        contract=context.request.expected_outputs[0]
        artifacts=[];existing=await self.store.list_job_artifacts(context.job.id,contract.artifact_role)
        for output in outputs:
            data=await asyncio.to_thread(self.objects.get,output['object_ref'],maximum=contract.max_item_bytes)
            async def chunks():yield data
            # Preserve identities already bound before a worker/adapter upgrade.
            previous=next((value for value in existing if value.position==len(artifacts)
                and value.filename==Path(output['filename']).name
                and value.object_ref==output['object_ref'] and value.size==len(data)
                and value.mime==output['mime']),None)
            artifact=await self.artifacts.emit(ArtifactEmission(job_id=context.job.id,
                generation=context.generation,artifact_role=contract.artifact_role,
                item_key=previous.item_key if previous else output['role'],
                position=len(artifacts),filename=Path(output['filename']).name,
                output_disposition=contract.output_disposition,declared_mime=output['mime']),chunks())
            artifacts.append(artifact.model_dump(mode='json'))
        collection=await self.store.finalize_job_artifact_collection(context.job.id,
            context.generation,contract.artifact_role)
        return {**status.result,'artifacts':artifacts,
            'collections':[collection]}
