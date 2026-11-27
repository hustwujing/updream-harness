from __future__ import annotations

import re


COMMON={"face_limit","texture","pbr","texture_quality","geometry_quality","auto_size","quad","smart_low_poly","model_seed","texture_seed","export_orientation","export_uv"}
MODE_PARAMETERS={
 "text_to_model":COMMON|{"prompt","negative_prompt","image_seed"},
 "image_to_model":COMMON|{"enable_image_autofix","texture_alignment","orientation"},
 "multiview_to_model":COMMON|{"texture_alignment","orientation"},
 "texture_model":{"input_job_id","texture_prompt","pbr","texture_seed","texture_alignment","texture_quality","bake"},
 "decimate_model":{"input_job_id","face_limit","topology_model","smart_uv","quad","bake"},
 "convert_model":{"input_job_id","target_format","bake","export_orientation"}}


class TripoContract:
    adapter="tripo_v3";kind="model3d"
    def __init__(self,configured:bool=True):self.configured=configured
    def validate_spec(self,spec):
        if spec.provider!="tripo":raise ValueError("Tripo Provider is invalid")
    def public(self,spec):
        modes={
         "text_to_model":{"references":0,"required_parameters":["prompt"],"allowed_parameters":sorted(MODE_PARAMETERS["text_to_model"]),"effects":{"quad=true":"output is FBX","texture=false":"pbr is forced false"}},
         "image_to_model":{"roles":["reference_image"],"references":1,"allowed_parameters":sorted(MODE_PARAMETERS["image_to_model"]),"effects":{"quad=true":"output is FBX","texture=false":"pbr is forced false"}},
         "multiview_to_model":{"roles":["front_view","left_view","back_view","right_view"],"min_references":2,"max_references":4,"required_roles":["front_view"],"allowed_parameters":sorted(MODE_PARAMETERS["multiview_to_model"]),"effects":{"quad=true":"output is FBX","texture=false":"pbr is forced false"}},
         "texture_model":{"references":0,"required_parameters":["input_job_id"],"allowed_parameters":sorted(MODE_PARAMETERS["texture_model"])},
         "decimate_model":{"references":0,"required_parameters":["input_job_id","face_limit"],"allowed_parameters":sorted(MODE_PARAMETERS["decimate_model"]),"defaults":{"topology_model":"v3.0"},"effects":{"quad=true":"output is FBX"}},
         "convert_model":{"references":0,"required_parameters":["input_job_id","target_format"],"allowed_parameters":sorted(MODE_PARAMETERS["convert_model"])}}
        return ({"roles":["reference_image","front_view","left_view","back_view","right_view"],"usages":["subject","style"],"recommended_usage":{"reference_image":"subject","front_view":"subject","left_view":"subject","back_view":"subject","right_view":"subject"}},
            {"required":[],"optional":{"prompt":"string <=1024","negative_prompt":"string <=255","face_limit":"integer 500..2000000","texture":"boolean","pbr":"boolean","texture_quality":["standard","detailed","extreme"],"geometry_quality":["standard","detailed"],"auto_size":"boolean","quad":"boolean","smart_low_poly":"boolean","model_seed":"integer","image_seed":"integer","texture_seed":"integer","enable_image_autofix":"boolean","texture_alignment":["original_image","geometry"],"orientation":["default","align_image"],"input_job_id":"platform Job ID","texture_prompt":"string <=1024","target_format":["GLTF","FBX","USDZ","OBJ","STL","3MF"],"topology_model":["v1.0","v2.0","v3.0"],"smart_uv":"boolean","bake":"boolean","export_orientation":["+x","-x","+y","-y"],"export_uv":"boolean"},
             "mode_parameter_policy":"Use only allowed_parameters for the selected generation mode","generation_modes":modes,
             "limits":{"max_image_bytes":20*1024*1024,"max_model_bytes":150*1024*1024,"reference_image_mimes":["image/png","image/jpeg","image/webp"],"face_limit":{"standard_triangle":1_500_000,"detailed_triangle":2_000_000,"quad":150_000,"smart_low_poly_triangle":20_000,"smart_low_poly_quad":10_000}}})
    def public_metadata(self,spec):return {"reference_inputs_available":True}
    def availability(self,spec):return (True,"") if self.configured else (False,"tripo_configuration_missing")
    def validate_shape(self,spec,p,mode):
        if not isinstance(p,dict):raise ValueError("Media parameters must be an object")
        allowed=MODE_PARAMETERS.get(mode)
        if allowed is None:raise ValueError("Invalid 3D generation mode")
        unknown=sorted(set(p)-allowed)
        if unknown:raise ValueError(f"Unsupported parameter(s) for {mode}: "+", ".join(unknown))
        prompt=p.get("prompt")
        if prompt is not None and (not isinstance(prompt,str) or not prompt.strip() or len(prompt)>1024):raise ValueError("3D prompt must be a non-empty string up to 1024 characters")
        negative=p.get("negative_prompt")
        if negative is not None and (not isinstance(negative,str) or len(negative)>255):raise ValueError("3D negative prompt must be a string up to 255 characters")
        face=p.get("face_limit")
        if face is not None and (isinstance(face,bool) or not isinstance(face,int) or not 500<=face<=2_000_000):raise ValueError("3D face_limit must be an integer from 500 to 2000000")
        for key in ("texture","pbr","auto_size","quad","smart_low_poly","enable_image_autofix","smart_uv","bake","export_uv"):
            if key in p and not isinstance(p[key],bool):raise ValueError(f"{key} must be boolean")
        if p.get("texture_quality","standard") not in {"standard","detailed","extreme"}:raise ValueError("Invalid 3D texture quality")
        if p.get("geometry_quality","standard") not in {"standard","detailed"}:raise ValueError("Invalid 3D geometry quality")
        for key in ("model_seed","image_seed","texture_seed"):
            if key in p and (isinstance(p[key],bool) or not isinstance(p[key],int)):raise ValueError(f"{key} must be integer")
        texture_prompt=p.get("texture_prompt")
        if texture_prompt is not None and (not isinstance(texture_prompt,str) or not texture_prompt.strip() or len(texture_prompt)>1024):raise ValueError("3D texture_prompt must be a non-empty string up to 1024 characters")
        if "topology_model" in p and p["topology_model"] not in {"v1.0","v2.0","v3.0"}:raise ValueError("Invalid 3D topology model")
        if "target_format" in p and p["target_format"] not in {"GLTF","FBX","USDZ","OBJ","STL","3MF"}:raise ValueError("Invalid 3D target format")
        if "texture_alignment" in p and p["texture_alignment"] not in {"original_image","geometry"}:raise ValueError("Invalid 3D texture alignment")
        if "orientation" in p and p["orientation"] not in {"default","align_image"}:raise ValueError("Invalid 3D orientation")
        if "export_orientation" in p and p["export_orientation"] not in {"+x","-x","+y","-y"}:raise ValueError("Invalid 3D export orientation")
        if "input_job_id" in p and (not isinstance(p["input_job_id"],str) or not re.fullmatch(r"[0-9a-f]{32}",p["input_job_id"])):raise ValueError("input_job_id must be a platform Job ID")
    def validate(self,spec,p,mode,references):
        self.validate_shape(spec,p,mode);roles=[item.get("role") for item in references]
        if mode=="text_to_model":
            if references or not p.get("prompt"):raise ValueError("Text-to-3D requires prompt and no references")
        elif mode=="image_to_model":
            if roles!=["reference_image"]:raise ValueError("Image-to-3D requires exactly one reference image")
        elif mode=="multiview_to_model":
            allowed={"front_view","left_view","back_view","right_view"}
            if not 2<=len(roles)<=4 or "front_view" not in roles or len(roles)!=len(set(roles)) or any(role not in allowed for role in roles):raise ValueError("Multiview-to-3D requires 2..4 unique views including front_view")
        elif mode in {"texture_model","decimate_model","convert_model"}:
            if references or not p.get("input_job_id"):raise ValueError(f"{mode} requires input_job_id and no references")
            if mode=="decimate_model" and "face_limit" not in p:raise ValueError("decimate_model requires face_limit")
            if mode=="convert_model" and p.get("target_format") not in {"GLTF","FBX","USDZ","OBJ","STL","3MF"}:raise ValueError("convert_model requires a supported target_format")
        if mode in {"text_to_model","image_to_model","multiview_to_model"}:
            face=p.get("face_limit")
            if face is not None:
                maximum=(10_000 if p.get("quad") else 20_000) if p.get("smart_low_poly") else 150_000 if p.get("quad") else 2_000_000 if p.get("geometry_quality")=="detailed" else 1_500_000
                if face>maximum:raise ValueError(f"3D face_limit exceeds the selected topology limit of {maximum}")
            if p.get("texture") is False:
                if p.get("pbr") is True:raise ValueError("pbr=true conflicts with texture=false")
                irrelevant=sorted(set(p)&{"texture_quality","texture_seed","texture_alignment","orientation"})
                if irrelevant:raise ValueError("texture=false does not accept: "+", ".join(irrelevant))
        if mode=="decimate_model":
            topology=p.get("topology_model","v3.0")
            if p.get("smart_uv") and topology!="v3.0":raise ValueError("smart_uv requires topology_model=v3.0")
            if "bake" in p and topology=="v1.0":raise ValueError("bake is unsupported with topology_model=v1.0")
        for item in references:
            if item.get("size",0)>20*1024*1024 or item.get("mime") not in {"image/png","image/jpeg","image/webp"}:raise ValueError("Tripo reference images must be PNG, JPEG or WebP at most 20 MB")
    def provider_parameters(self,spec,p,mode):self.validate_shape(spec,p,mode);return dict(p)
