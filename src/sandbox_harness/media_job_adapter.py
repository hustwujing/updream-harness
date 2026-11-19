from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

from .artifact_gateway import ArtifactEmission,ArtifactGateway
from .domain import MediaDraftCreate,now
from .job_adapters import (AdapterStatus,AdapterSubmission,BaseJobAdapter,JobExecutionContext,
    PreAcceptFailure)
from .job_contracts import JobState
from .media_references import inspect_reference


class MediaProviderJobAdapter(BaseJobAdapter):
    def __init__(self,settings,store,objects,catalog):
        self.settings=settings;self.store=store;self.objects=objects;self.catalog=catalog
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'provider-job-artifacts',
            maximum_bytes=settings.max_artifact_bytes)

    async def _references(self,context:JobExecutionContext)->list[dict]:
        bindings=context.request.parameters.get('reference_bindings',{})
        if set(bindings)!=set(item.artifact_id for item in context.request.input_artifacts):
            if context.request.input_artifacts:raise ValueError('media_reference_bindings_mismatch')
            return []
        values=[]
        for item in context.request.input_artifacts:
            artifact=await self.store.get_artifact(item.artifact_id)
            if not artifact:raise KeyError('media_reference_artifact_not_found')
            binding=bindings[item.artifact_id];role=str(binding.get('role',''));usage=str(binding.get('usage',''))
            data=await asyncio.to_thread(self.objects.get,artifact.object_ref,maximum=200*1024*1024)
            metadata=await asyncio.to_thread(inspect_reference,data,role,artifact.path)
            values.append({'artifact_id':artifact.id,'path':artifact.path,'version':1,
                'sha256':artifact.sha256,'size':artifact.size,'mime':metadata['mime'],
                'kind':metadata['mime'].split('/',1)[0],'object_ref':artifact.object_ref,
                'role':role,'usage':usage,'width':metadata.get('width'),'height':metadata.get('height'),
                'duration_seconds':metadata.get('duration_seconds'),'fps':metadata.get('fps'),
                'codec':metadata.get('codec',''),'audio_codec':metadata.get('audio_codec','')})
        return values

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        request=context.request;parameters=request.parameters
        kind=str(parameters.get('kind',''));model=str(parameters.get('model',''))
        generation_mode=str(parameters.get('generation_mode','')) or None
        provider_parameters=parameters.get('parameters',{})
        if not kind or not model or not isinstance(provider_parameters,dict):
            raise PreAcceptFailure('Media Provider parameters are invalid',
                error_code='provider_input_invalid',failed_phase='input_validation',retryable=False)
        try:
            spec=self.catalog.resolve(kind,model);references=await self._references(context)
            self.catalog.validate_parameters(kind,model,provider_parameters,
                generation_mode=generation_mode,references=references)
        except PreAcceptFailure:raise
        except (KeyError,ValueError) as exc:
            raise PreAcceptFailure('Media Provider input validation failed',
                error_code='provider_input_invalid',failed_phase='input_validation',
                retryable=False) from exc
        owner_id=await self.store.owner_id_for_thread(context.job.thread_id)
        if not owner_id:raise KeyError('media_provider_thread_not_found')
        value=MediaDraftCreate(kind=kind,model=model,generation_mode=generation_mode,
            parameters=provider_parameters,references=[],idempotency_key=request.idempotency_key,
            turn_id='',output_role='candidate')
        draft,_=await self.store.create_media_draft(owner_id,context.job.thread_id,value,
            expires_at=now()+timedelta(seconds=self.settings.media_draft_ttl_seconds),
            provider_spec=spec,reference_snapshots=references)
        media_job=await self.store.confirm_media_draft(draft.id,owner_id,provider_spec=spec)
        return AdapterSubmission(external_execution_id=media_job.id,acceptance_receipt={
            'media_job_id':media_job.id,'provider':media_job.provider,'provider_adapter':media_job.adapter,
            'execution_mode':media_job.execution_mode})

    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        owner_id=await self.store.owner_id_for_thread(context.job.thread_id)
        value=await self.store.get_media_job(external_execution_id,owner_id)
        if not value:raise RuntimeError('accepted_media_job_missing')
        if value.status=='succeeded':return AdapterStatus(True,JobState.SUCCEEDED,{
            'media_job_id':value.id,'provider':value.provider,'credits_consumed':value.credits_consumed})
        if value.status=='failed':return AdapterStatus(True,JobState.FAILED,{
            'media_job_id':value.id,'error_code':value.error_code,'error_detail':value.error_detail})
        if value.status=='cancelled':return AdapterStatus(True,JobState.CANCELLED,{'media_job_id':value.id})
        if value.status=='unknown':return AdapterStatus(True,JobState.UNKNOWN,{
            'media_job_id':value.id,'error_code':value.error_code,'error_detail':value.error_detail})
        return AdapterStatus(False,JobState.CANCELLING if value.status=='cancel_requested'
            else JobState.WAITING_EXTERNAL,{'media_job_id':value.id,'phase':value.phase})

    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.status(context,external_execution_id)

    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        current=await self.status(context,external_execution_id)
        if current.terminal:return current
        owner_id=await self.store.owner_id_for_thread(context.job.thread_id)
        try:value=await self.store.request_media_cancel(external_execution_id,owner_id)
        except RuntimeError:return AdapterStatus(False,JobState.CANCELLING,{
            'media_job_id':external_execution_id,'cancel_refused':True})
        return AdapterStatus(False,JobState.CANCELLING,{'media_job_id':value.id})

    async def collect(self,context:JobExecutionContext,external_execution_id:str,
                      status:AdapterStatus)->dict:
        if status.state!=JobState.SUCCEEDED:return status.result
        output=await self.store.media_job_output_for_thread(external_execution_id,context.job.thread_id)
        if not output:raise RuntimeError('media_job_output_unavailable')
        if len(context.request.expected_outputs)!=1:
            raise RuntimeError('media_provider_output_contract_requires_one_role')
        contract=context.request.expected_outputs[0]
        data=await asyncio.to_thread(self.objects.get,output['object_ref'],maximum=contract.max_item_bytes)
        async def chunks():yield data
        artifact=await self.artifacts.emit(ArtifactEmission(job_id=context.job.id,
            generation=context.generation,artifact_role=contract.artifact_role,item_key='primary',position=0,
            filename=Path(output['filename']).name,output_disposition=contract.output_disposition,
            declared_mime=output['mime']),chunks())
        collection=await self.store.finalize_job_artifact_collection(context.job.id,
            context.generation,contract.artifact_role)
        return {**status.result,'artifacts':[artifact.model_dump(mode='json')],
            'collections':[collection]}
