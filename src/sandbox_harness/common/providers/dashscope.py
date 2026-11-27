from __future__ import annotations

import re


CONTRACTS={
 "happyhorse_video_edit":{"max_images":5,"video_duration":(3,60),"video_dimensions":(360,4096),"video_ratio":(1/2.5,2.5),"video_fps":(8,60),"image_dimensions":(300,None),"image_ratio":(1/2.5,2.5),"image_mimes":{"image/jpeg","image/png","image/webp"},"prompt_cjk_limit":2500,"prompt_other_limit":5000},
 "wan27_video_edit":{"max_images":4,"video_duration":(2,10),"video_dimensions":(240,4096),"video_ratio":(1/8,8),"video_fps":None,"image_dimensions":(240,8000),"image_ratio":(1/8,8),"image_mimes":{"image/jpeg","image/png","image/bmp","image/webp"},"prompt_cjk_limit":5000,"prompt_other_limit":5000}}


class DashscopeVideoContract:
    kind="video"
    def __init__(self,adapter):self.adapter=adapter
    @property
    def values(self):return CONTRACTS[self.adapter]
    def validate_spec(self,spec):
        if tuple(spec.supported_generation_modes)!=("edit_video",):raise ValueError("DashScope video supports edit_video only")
    def public(self,spec):
        c=self.values;minimum,maximum=c["video_duration"]
        return ({"roles":["reference_image","reference_video"],"usages":["subject","style","composition","edit_source"],"recommended_usage":{"reference_image":"subject","reference_video":"edit_source"}},
            {"required":["prompt"],"optional":{"duration":"fixed -1 (preserve source within Provider output limit)","resolution":["720p","1080p"],"aspect_ratio":["adaptive"]},
             "defaults":{"resolution":"1080p","duration":-1,"aspect_ratio":"adaptive"},
             "mode_parameter_policy":"Use only allowed_parameters for the selected generation mode",
             "generation_modes":{"edit_video":{"roles":["reference_image","reference_video"],"required_usage":"edit_source","max_images":c["max_images"],"references":f"one edit_source video and 0..{c['max_images']} reference images","allowed_parameters":["prompt","duration","resolution","aspect_ratio"],"fixed_parameters":{"aspect_ratio":"adaptive","duration":-1}}},
             "limits":{"input_video_duration_seconds":f"{minimum}..{maximum}","output_video_duration_seconds":"3..15" if self.adapter=="happyhorse_video_edit" else "2..10","max_video_bytes":100*1024*1024,"max_reference_image_bytes":20*1024*1024,"max_reference_images":c["max_images"]}})
    def public_metadata(self,spec):return {"reference_inputs_available":True}
    def availability(self,spec):return True,""
    def validate_shape(self,spec,p,mode):
        if not isinstance(p,dict):raise ValueError("Media parameters must be an object")
        if mode!="edit_video":raise ValueError(f"{spec.model} does not support generation mode {mode}")
        unsupported=sorted(set(p)-{"prompt","duration","resolution","aspect_ratio"})
        if unsupported:raise ValueError("DashScope video edit does not accept: "+", ".join(unsupported))
        prompt=p.get("prompt")
        if not isinstance(prompt,str) or not prompt.strip():raise ValueError("prompt must be non-empty")
        limit=self.values["prompt_cjk_limit"] if re.search(r"[\u3400-\u9fff]",prompt) else self.values["prompt_other_limit"]
        if len(prompt)>limit:raise ValueError(f"DashScope video edit prompt exceeds {limit} characters")
        if p.get("duration",-1)!=-1 or p.get("aspect_ratio","adaptive")!="adaptive":raise ValueError("DashScope video edit preserves source duration/aspect; use duration=-1 and aspect_ratio=adaptive")
        if p.get("resolution","1080p") not in {"720p","1080p"}:raise ValueError("DashScope video edit resolution must be 720p or 1080p")
    def validate(self,spec,p,mode,references):
        self.validate_shape(spec,p,mode);c=self.values
        images=[item for item in references if item.get("role")=="reference_image"]
        videos=[item for item in references if item.get("role")=="reference_video"]
        if len(videos)!=1 or any(item.get("role") not in {"reference_image","reference_video"} for item in references) or len(images)>c["max_images"]:
            raise ValueError(f"DashScope video edit requires one edit_source video, up to {c['max_images']} reference images, and no reference audio")
        source=videos[0]
        if source.get("usage")!="edit_source":raise ValueError("DashScope source video must use edit_source")
        duration,width,height,fps,size=source.get("duration_seconds"),source.get("width"),source.get("height"),source.get("fps"),source.get("size",0)
        minimum,maximum=c["video_duration"]
        if not duration or not minimum<=duration<=maximum or size<=0 or size>100*1024*1024 or source.get("mime") not in {"video/mp4","video/quicktime"}:raise ValueError(f"DashScope edit_source must be MP4/MOV, {minimum}..{maximum} seconds, at most 100 MB")
        low,high=c["video_dimensions"];rmin,rmax=c["video_ratio"]
        if not isinstance(width,int) or not isinstance(height,int) or min(width,height)<low or max(width,height)>high or not rmin<=width/height<=rmax:raise ValueError("DashScope edit_source dimensions are outside Provider limits")
        if c["video_fps"] and (fps is None or not c["video_fps"][0]<=fps<=c["video_fps"][1]):raise ValueError("Happyhorse edit_source frame rate must be 8..60 FPS")
        ilow,ihigh=c["image_dimensions"];irmin,irmax=c["image_ratio"]
        for image in images:
            width,height,size=image.get("width"),image.get("height"),image.get("size",0)
            if image.get("mime") not in c["image_mimes"] or size<=0 or size>20*1024*1024 or not isinstance(width,int) or not isinstance(height,int) or min(width,height)<ilow or ihigh is not None and max(width,height)>ihigh or not irmin<=width/height<=irmax:raise ValueError("DashScope reference image is outside Provider format, size, dimension or aspect-ratio limits")
    def provider_parameters(self,spec,p,mode):self.validate_shape(spec,p,mode);return dict(p)


class HappyhorseVideoContract(DashscopeVideoContract):
    def __init__(self):super().__init__("happyhorse_video_edit")


class WanVideoContract(DashscopeVideoContract):
    def __init__(self):super().__init__("wan27_video_edit")
