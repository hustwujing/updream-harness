from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from sandbox_harness.common.contracts.job_contracts import (JobKind, JobRequest)
from sandbox_harness.common.catalog.media_references import (inspect_reference)


CAPABILITY_SCOPES={
    'image.generate':('image',frozenset({'text_to_image','reference_image'})),
    'video.generate':('video',frozenset({
        'text_to_video','first_frame','first_last_frame','multimodal_reference'})),
    'video.edit':('video',frozenset({'edit_video','extend_video'})),
    'audio.generate':('audio',frozenset({'text_to_audio','reference_voice'})),
    'model3d.generate':('model3d',frozenset({
        'text_to_model','image_to_model','multiview_to_model'})),
    'model3d.transform':('model3d',frozenset({
        'texture_model','decimate_model','convert_model'})),
}


class ProviderPreflightError(ValueError):
    def __init__(self,code:str):self.code=code;super().__init__('provider_preflight_invalid:'+code)


@dataclass(frozen=True)
class ProviderValidation:
    spec:object
    references:list[dict]
    kind:str
    model:str
    generation_mode:str
    parameters:dict


class ProviderPreflight:
    def __init__(self,store,objects,catalog,*,maximum_reference_bytes:int=200*1024*1024):
        self.store=store;self.objects=objects;self.catalog=catalog
        self.maximum_reference_bytes=maximum_reference_bytes

    async def _references(self,request:JobRequest)->list[dict]:
        bindings=request.parameters.get('reference_bindings',{})
        if not isinstance(bindings,dict) or set(bindings)!={
                item.artifact_id for item in request.input_artifacts}:
            raise ProviderPreflightError('reference_bindings_mismatch')
        values=[]
        for item in request.input_artifacts:
            artifact=await self.store.get_artifact(item.artifact_id)
            if not artifact:raise ProviderPreflightError('reference_artifact_not_found')
            binding=bindings[item.artifact_id]
            if not isinstance(binding,dict):raise ProviderPreflightError('reference_binding_invalid')
            role=str(binding.get('role',''));usage=str(binding.get('usage',''))
            try:
                data=await asyncio.to_thread(self.objects.get,artifact.object_ref,
                    maximum=self.maximum_reference_bytes)
                metadata=await asyncio.to_thread(inspect_reference,data,role,artifact.path)
            except (OSError,KeyError,ValueError):
                raise ProviderPreflightError('reference_media_invalid') from None
            values.append({'artifact_id':artifact.id,'path':artifact.path,'version':1,
                'sha256':artifact.sha256,'size':artifact.size,'mime':metadata['mime'],
                'kind':metadata['mime'].split('/',1)[0],'object_ref':artifact.object_ref,
                'role':role,'usage':usage,'width':metadata.get('width'),'height':metadata.get('height'),
                'duration_seconds':metadata.get('duration_seconds'),'fps':metadata.get('fps'),
                'codec':metadata.get('codec',''),'audio_codec':metadata.get('audio_codec','')})
        return values

    async def inspect(self,request:JobRequest)->ProviderValidation:
        parameters=request.parameters;kind=str(parameters.get('kind',''))
        model=str(parameters.get('model',''));mode=str(parameters.get('generation_mode',''))
        provider_parameters=parameters.get('parameters',{})
        if not kind or not model or not isinstance(provider_parameters,dict):
            raise ProviderPreflightError('parameters_invalid')
        scope=CAPABILITY_SCOPES.get(request.descriptor_id)
        if (request.kind!=JobKind.PROVIDER or not scope or kind!=scope[0]
                or mode not in scope[1] or request.adapter!=request.descriptor_id):
            raise ProviderPreflightError('capability_scope_invalid')
        try:
            spec=self.catalog.resolve(kind,model);references=await self._references(request)
            self.catalog.validate_parameters(kind,model,provider_parameters,
                generation_mode=mode,references=references)
        except ProviderPreflightError:raise
        except (KeyError,ValueError):raise ProviderPreflightError('model_input_invalid') from None
        return ProviderValidation(spec,references,kind,model,mode,provider_parameters)

    async def validate(self,request:JobRequest)->None:
        if request.kind==JobKind.PROVIDER:await self.inspect(request)
