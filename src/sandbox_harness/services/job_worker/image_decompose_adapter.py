from __future__ import annotations

from types import SimpleNamespace

from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactGateway)
from sandbox_harness.common.execution.job_adapters import (AdapterSubmission, JobExecutionContext, PreAcceptCancelled, PreAcceptFailure, SubmissionRecovery)
from sandbox_harness.common.catalog.media_catalog import (MediaModelSpec)
from sandbox_harness.services.job_worker.media_job_adapter import (MediaProviderJobAdapter)


DECOMPOSE_SPEC=MediaModelSpec(kind="image",model="qwen-image-layered",
    provider="local-qwen-layered",adapter="image_layered_http",route="",
    execution="async",cancellation="supported",progress="provider",credential_env="")


class ImageDecomposeAdapter(MediaProviderJobAdapter):
    """Database-only bridge from an authorized Operation to MediaWorker."""

    def __init__(self,settings,store,objects,media=None):
        self.settings=settings;self.store=store;self.objects=objects
        self.media=media or store
        self.artifacts=ArtifactGateway(store,objects,settings.data_root/'tmp'/'provider-job-artifacts',
            maximum_bytes=settings.max_artifact_bytes)

    @staticmethod
    def _parameters(value:dict)->dict[str,str]:
        layer_count=int(value.get('layer_count',4));resolution=int(value.get('resolution',640))
        quality=str(value.get('quality','balanced'));seed=int(value.get('seed',777))
        if layer_count not in {3,4,6,8} or resolution not in {640,1024}:
            raise PreAcceptFailure('decompose_shape_parameters_invalid',
                error_code='provider_input_invalid',retryable=False)
        if quality not in {'balanced','fine','original'} or not 0<=seed<=2**31-1:
            raise PreAcceptFailure('decompose_quality_parameters_invalid',
                error_code='provider_input_invalid',retryable=False)
        return {'prompt':str(value.get('prompt',''))[:2000],'layer_count':str(layer_count),
            'resolution':str(resolution),'quality':quality,'seed':str(seed),
            'reconstruction_guard':str(bool(value.get('reconstruction_guard',True))).lower()}

    async def _enqueue(self,context:JobExecutionContext):
        if (len(context.request.input_artifacts)!=1
                or context.request.input_artifacts[0].artifact_role!='source'
                or len(context.request.expected_outputs)!=1):
            raise PreAcceptFailure('decompose_contract_invalid',
                error_code='provider_input_invalid',retryable=False)
        try:parameters=self._parameters(context.request.parameters)
        except (ValueError,TypeError) as exc:
            raise PreAcceptFailure('decompose_parameters_invalid',
                error_code='provider_input_invalid',retryable=False) from exc
        try:
            if hasattr(self.media,"submit"):
                references=await self.media.source_references(context)
                validated=SimpleNamespace(spec=DECOMPOSE_SPEC,references=references,
                    kind=DECOMPOSE_SPEC.kind,model=DECOMPOSE_SPEC.model,
                    generation_mode="",parameters=parameters)
                return await self.media.submit(context,validated)
            return await self.store.enqueue_job_media_execution(context.job.id,context.generation,
                request_digest=context.request.request_digest,provider_spec=DECOMPOSE_SPEC,
                parameters=parameters)
        except RuntimeError as exc:
            if str(exc)=='job_cancel_requested':raise PreAcceptCancelled() from exc
            raise

    @staticmethod
    def _submission(value,source_generation:int):
        return AdapterSubmission(value.id,{'media_job_id':value.id,'provider':value.provider,
            'provider_adapter':value.adapter,'execution_mode':value.execution_mode,
            'source_generation':source_generation})

    async def submit(self,context:JobExecutionContext)->AdapterSubmission:
        return self._submission(await self._enqueue(context),context.generation)

    async def recover_submission(self,context:JobExecutionContext)->SubmissionRecovery:
        source_generation=int(context.previous_acceptance_receipt.get(
            'source_generation') or context.generation)
        value=await self.media.media_execution_for_job(context.job.id,context.job.owner_id)
        if value:return SubmissionRecovery(submission=self._submission(value,source_generation))
        if hasattr(self.media,"submit"):
            return SubmissionRecovery(submission=self._submission(
                await self._enqueue(context),source_generation))
        return SubmissionRecovery(safe_to_retry=True)

    async def resolve_execution(self,context:JobExecutionContext,external_execution_id:str)->str:
        value=await self.media.media_execution_for_job(context.job.id,context.job.owner_id)
        if value and value.id==external_execution_id:return value.id
        raise RuntimeError('unrecorded_media_execution_identity')

    async def outputs(self,context:JobExecutionContext,external_execution_id:str)->list[dict]:
        return await self.media.media_job_outputs_for_source(external_execution_id,
            context.job.id,context.job.owner_id)
