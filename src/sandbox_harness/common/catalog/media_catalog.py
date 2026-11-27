from __future__ import annotations

import json
import re
from dataclasses import asdict,dataclass,field
from typing import Any

from sandbox_harness.common.providers import (default_media_provider_contracts)
from sandbox_harness.common.providers.contracts import (MediaProviderContractRegistry)


@dataclass(frozen=True)
class MediaModelSpec:
    kind:str;model:str;provider:str;adapter:str;route:str;execution:str
    cancellation:str;progress:str;credential_env:str;enabled:bool=True
    supported_generation_modes:tuple[str,...]=()
    guidance:dict[str,Any]=field(default_factory=dict)
    provider_options:dict[str,Any]=field(default_factory=dict)
    def public(self,contracts:MediaProviderContractRegistry)->dict[str,Any]:
        value=asdict(self);value.pop("credential_env",None);value.pop("provider_options",None)
        contract=contracts.resolve(self.adapter)
        value["reference_schema"],value["parameter_schema"]=contract.public(self)
        value.update(contract.public_metadata(self))
        value["available"],reason=contract.availability(self)
        if reason:value["availability_error"]=reason
        return value


DEFAULT_MEDIA_CATALOG=(
 MediaModelSpec("image","seedream-5.0","ark","ark_image","/v1/providers/image","sync","unsupported","none","SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY",provider_options={"sizes":["2048x2048","2560x1440","2304x1728"],"max_dimension":3584}),
 MediaModelSpec("video","sed2","ark","ark_video_v2","/v1/providers/video","async","unsupported","provider","SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY",supported_generation_modes=("text_to_video","first_frame","first_last_frame","multimodal_reference","edit_video","extend_video"),guidance={"summary":"Seedance 2.0 is the most capable all-purpose video model, with text, frame, multimodal, edit and extend modes.","best_for":["complex multimodal direction","video extension","generated audio","up to 15 second outputs"],"strengths":["widest mode coverage","strong instruction following","supports image, video and audio references"],"limitations":["references require Ark Asset activation","reference activation or generation can be rejected by Provider policy"],"selection_rules":["Choose when multimodal control, extension or generated audio is required.","After a deterministic reference-policy rejection, follow the stable error guidance instead of rerouting unchanged bytes."]}),
 MediaModelSpec("video","happyhorse-video-edit","alibaba","happyhorse_video_edit","/v1/providers/video","async","unsupported","provider","SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY",supported_generation_modes=("edit_video",),guidance={"summary":"Happyhorse edits one existing video with optional appearance or style reference images.","best_for":["prompt-based edits","lip-sync and subtle face motion","preserving original audio"],"strengths":["accepts source videos up to 60 seconds","supports up to 5 reference images","can force original-audio preservation"],"limitations":["edit_video only in this integration","no separate reference audio","provider acceptance is not proof of source authorization"],"selection_rules":["Use one authorized edit_source video; optional reference images may guide appearance or style.","Inputs longer than 15 seconds produce only the first 15 seconds."]}),
 MediaModelSpec("video","wan-2.7-video-edit","alibaba","wan27_video_edit","/v1/providers/video","async","unsupported","provider","SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY",supported_generation_modes=("edit_video",),guidance={"summary":"Wan 2.7 edits a short existing video with optional appearance or style reference images.","best_for":["prompt-based short video edits","alternate rendering style","preserving original audio"],"strengths":["supports broad input aspect ratios","supports up to 4 reference images","can force original audio preservation"],"limitations":["input and output are limited to 10 seconds","edit_video only in this integration"],"selection_rules":["Use one authorized 2 to 10 second edit_source video.","Split longer source material into semantic clips before submission."]}),
 MediaModelSpec("audio","speech-2.8-turbo","minimax","minimax_t2a","/v1/providers/audio","async","unsupported","none","SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY"),
 MediaModelSpec("model3d","v3.1-20260211","tripo","tripo_v3","/v3","async","unsupported","provider","SANDBOX_HARNESS_TRIPO_API_KEY"))


class MediaCatalog:
    def __init__(self,raw:str="",*,ark_assets_configured:bool=True,tripo_configured:bool=True,
                 contracts:MediaProviderContractRegistry|None=None):
        self.contracts=contracts or default_media_provider_contracts(
            ark_assets_configured=ark_assets_configured,tripo_configured=tripo_configured)
        self._models=self._parse(raw)
    def _parse(self,raw:str)->tuple[MediaModelSpec,...]:
        source=[asdict(item) for item in DEFAULT_MEDIA_CATALOG] if not raw else json.loads(raw)
        if not isinstance(source,list):raise ValueError("Media catalog must be a JSON array")
        defaults={(item.kind,item.model):asdict(item) for item in DEFAULT_MEDIA_CATALOG};result=[]
        for item in source:
            if not isinstance(item,dict):raise ValueError("Media catalog entries must be objects")
            values={**defaults.get((item.get("kind"),item.get("model")),{}),**item}
            values["supported_generation_modes"]=tuple(values.get("supported_generation_modes",()))
            spec=MediaModelSpec(**values)
            if spec.kind not in {"image","video","audio","model3d"}:raise ValueError("Invalid media kind")
            if spec.execution not in {"sync","async"}:raise ValueError("Invalid media execution mode")
            if spec.cancellation not in {"supported","unsupported"}:raise ValueError("Invalid media cancellation mode")
            if spec.execution=="sync" and spec.cancellation=="supported":raise ValueError("Synchronous media jobs cannot be cancellable")
            if spec.cancellation=="supported":raise ValueError("Media Provider cancellation is not implemented for this adapter")
            if not isinstance(spec.guidance,dict) or not isinstance(spec.provider_options,dict):raise ValueError("Invalid Provider model metadata")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}",spec.credential_env):raise ValueError("Invalid media credential environment")
            contract=self.contracts.resolve(spec.adapter)
            if contract.kind!=spec.kind:raise ValueError("media_provider_contract_kind_mismatch:"+spec.adapter)
            contract.validate_spec(spec);result.append(spec)
        identities=[(item.kind,item.model) for item in result]
        if len(identities)!=len(set(identities)):raise ValueError("Duplicate media catalog model")
        return tuple(result)
    def resolve(self,kind:str,model:str)->MediaModelSpec:
        for item in self._models:
            if item.enabled and item.kind==kind and item.model==model:
                if not self.contracts.resolve(item.adapter).availability(item)[0]:break
                return item
        raise KeyError("Media model is not enabled")
    @staticmethod
    def _mode(kind:str,value:str|None)->str:
        return value or {"image":"text_to_image","video":"text_to_video","audio":"text_to_audio","model3d":"text_to_model"}[kind]
    def validate_parameter_shape(self,kind:str,model:str,parameters:dict[str,Any],*,generation_mode:str|None=None)->None:
        spec=self.resolve(kind,model);self.contracts.resolve(spec.adapter).validate_shape(spec,parameters,self._mode(kind,generation_mode))
    def validate_parameters(self,kind:str,model:str,parameters:dict[str,Any],*,generation_mode:str|None=None,references:list[dict[str,Any]]|None=None)->None:
        spec=self.resolve(kind,model);self.contracts.resolve(spec.adapter).validate(spec,parameters,self._mode(kind,generation_mode),references or [])
    def provider_parameters(self,kind:str,model:str,parameters:dict[str,Any],*,generation_mode:str|None=None)->dict[str,Any]:
        spec=self.resolve(kind,model);return self.contracts.resolve(spec.adapter).provider_parameters(spec,parameters,self._mode(kind,generation_mode))
    def public(self,kind:str|None=None)->list[dict[str,Any]]:
        return sorted((item.public(self.contracts) for item in self._models if item.enabled
            and (kind is None or item.kind==kind)),key=lambda item:(item["kind"],item["model"]))
