from __future__ import annotations

import re
from typing import Any


IMAGE_ROLES={"reference_image","first_frame","last_frame"}
VIDEO_ROLES={"reference_video"}
AUDIO_ROLES={"reference_audio"}
MIN_IMAGE_PIXELS=2560*1440
MAX_IMAGE_PIXELS=4_624_220
DEPLOYED_IMAGE_SIZES=("2048x2048","2560x1440","2304x1728")


def _prompt(parameters:dict[str,Any])->None:
    value=parameters.get("prompt")
    if not isinstance(value,str) or not value.strip() or len(value)>9_999:
        raise ValueError("prompt must be a non-empty string shorter than 10000 characters")


def _images(values:list[dict[str,Any]],*,seedream:bool)->None:
    for item in values:
        width,height,size=item.get("width"),item.get("height"),item.get("size",0)
        if not isinstance(width,int) or not isinstance(height,int) or width<=0 or height<=0:
            raise ValueError("Reference image dimensions are unavailable")
        if size<=0 or size>30*1024*1024:raise ValueError("Reference image must be smaller than 30 MB")
        ratio=width/height
        if seedream:
            if width<=14 or height<=14 or width*height>36_000_000 or not 1/16<=ratio<=16:
                raise ValueError("Seedream reference image dimensions are outside Provider limits")
        elif not 300<width<6000 or not 300<height<6000 or not .4<ratio<2.5:
            raise ValueError("Seedance reference image dimensions are outside Provider limits")


class ArkImageContract:
    adapter="ark_image";kind="image"

    def validate_spec(self,spec)->None:
        options=spec.provider_options
        sizes=options.get("sizes")
        maximum=options.get("max_dimension")
        if not isinstance(sizes,list) or not sizes or len(sizes)!=len(set(sizes)):
            raise ValueError("Ark image sizes are invalid")
        if not isinstance(maximum,int) or not 2048<=maximum<=4096:
            raise ValueError("Ark image maximum dimension is invalid")
        for value in sizes:
            match=re.fullmatch(r"([1-9][0-9]{1,3})x([1-9][0-9]{1,3})",value) if isinstance(value,str) else None
            if not match:raise ValueError("Ark image sizes must use WIDTHxHEIGHT")
            width,height=(int(part) for part in match.groups())
            if max(width,height)>maximum or not MIN_IMAGE_PIXELS<=width*height<=MAX_IMAGE_PIXELS:
                raise ValueError("Ark image size exceeds deployed limits")

    def public(self,spec):
        options=spec.provider_options
        return ({"roles":["reference_image"],"usages":["subject","style","composition"],
            "recommended_usage":{"reference_image":"subject"}},
            {"required":["prompt"],"optional":{"size":[*options["sizes"],"explicit WIDTHxHEIGHT"]},
             "generation_modes":{"text_to_image":{"references":0},
                "reference_image":{"roles":["reference_image"],"max_references":14}},
             "limits":{"min_total_pixels":MIN_IMAGE_PIXELS,"max_total_pixels":MAX_IMAGE_PIXELS,
                "max_dimension":options["max_dimension"],"aspect_ratio":"1/16..16"}})
    def public_metadata(self,spec):return {"reference_inputs_available":True}
    def availability(self,spec):return True,""

    def validate_shape(self,spec,parameters,mode):
        if not isinstance(parameters,dict):raise ValueError("Media parameters must be an object")
        unknown=sorted(set(parameters)-{"prompt","size"})
        if unknown:raise ValueError("Unsupported image parameter(s): "+", ".join(unknown))
        _prompt(parameters);size=parameters.get("size","2048x2048")
        match=re.fullmatch(r"([1-9][0-9]{1,3})x([1-9][0-9]{1,3})",size) if isinstance(size,str) else None
        if not match:raise ValueError("Image size must be explicit WIDTHxHEIGHT")
        width,height=(int(value) for value in match.groups());pixels=width*height
        if (not MIN_IMAGE_PIXELS<=pixels<=MAX_IMAGE_PIXELS or max(width,height)>spec.provider_options["max_dimension"]
                or not 1/16<=width/height<=16):
            raise ValueError("Seedream image size exceeds the deployed Provider limits")

    def validate(self,spec,parameters,mode,references):
        self.validate_shape(spec,parameters,mode)
        if mode=="text_to_image":
            if references:raise ValueError("Text-to-image does not accept references")
            return
        if mode!="reference_image" or not 1<=len(references)<=14 or any(
                item.get("role")!="reference_image" for item in references):
            raise ValueError("Seedream reference image mode requires 1..14 reference images")
        _images(references,seedream=True)
        if sum(item["size"] for item in references)>45*1024*1024:
            raise ValueError("Seedream inline references may total at most 45 MB")

    def provider_parameters(self,spec,parameters,mode):
        self.validate_shape(spec,parameters,mode)
        return dict(parameters)


class ArkVideoContract:
    adapter="ark_video_v2";kind="video"
    def __init__(self,assets_configured:bool=True):self.assets_configured=assets_configured

    def validate_spec(self,spec)->None:
        allowed={"text_to_video","first_frame","first_last_frame","multimodal_reference",
            "edit_video","extend_video"}
        if not spec.supported_generation_modes or not set(spec.supported_generation_modes)<=allowed:
            raise ValueError("Ark video generation modes are invalid")

    def public(self,spec):
        modes={
            "text_to_video":{"references":0},"first_frame":{"roles":["first_frame"],"references":1},
            "first_last_frame":{"roles":["first_frame","last_frame"],"references":2},
            "multimodal_reference":{"roles":["reference_image","reference_video","reference_audio"],"max_images":9,"max_videos":3,"max_audio":3},
            "edit_video":{"roles":["reference_image","reference_video","reference_audio"],"required_usage":"edit_source","fixed_parameters":{"aspect_ratio":"adaptive","duration":-1}},
            "extend_video":{"roles":["reference_image","reference_video","reference_audio"],"required_usage":"extend_source","required_parameters":["extend_direction"],"fixed_parameters":{"aspect_ratio":"adaptive"}}}
        return ({"roles":["reference_image","reference_video","reference_audio","first_frame","last_frame"],
            "usages":["subject","style","composition","action","camera","audio","edit_source","extend_source"],
            "recommended_usage":{"first_frame":"composition","last_frame":"composition","reference_image":"subject","reference_video":"action","reference_audio":"audio"}},
            {"required":["prompt"],"optional":{"duration":"integer 4..15 or -1","resolution":["480p","720p","1080p","4k"],
                "aspect_ratio":["adaptive","21:9","16:9","9:16","1:1","4:3","3:4"],"generate_audio":"boolean","extend_direction":["forward","backward"]},
             "generation_modes":{mode:modes[mode] for mode in spec.supported_generation_modes}})
    def public_metadata(self,spec):return ({"reference_inputs_available":True} if self.assets_configured
        else {"reference_inputs_available":False,"reference_inputs_error":"ark_asset_configuration_missing"})
    def availability(self,spec):return True,""

    def validate_shape(self,spec,parameters,mode):
        if not isinstance(parameters,dict):raise ValueError("Media parameters must be an object")
        unknown=sorted(set(parameters)-{"prompt","duration","resolution","aspect_ratio","generate_audio","extend_direction"})
        if unknown:raise ValueError("Unsupported video parameter(s): "+", ".join(unknown)+
            "; allowed: aspect_ratio, duration, extend_direction, generate_audio, prompt, resolution")
        _prompt(parameters);duration=parameters.get("duration",5)
        if isinstance(duration,bool) or not isinstance(duration,int) or duration!=-1 and not 4<=duration<=15:
            raise ValueError("Video duration must be -1 or an integer from 4 to 15 seconds")
        if parameters.get("resolution","480p") not in {"480p","720p","1080p","4k"}:raise ValueError("Invalid video resolution")
        if parameters.get("aspect_ratio","16:9") not in {"adaptive","21:9","16:9","9:16","1:1","4:3","3:4"}:raise ValueError("Invalid video aspect ratio")
        if not isinstance(parameters.get("generate_audio",False),bool):raise ValueError("generate_audio must be boolean")
        if parameters.get("extend_direction") is not None and parameters["extend_direction"] not in {"forward","backward"}:raise ValueError("Invalid video extension direction")

    def validate(self,spec,parameters,mode,references):
        self.validate_shape(spec,parameters,mode)
        if mode not in spec.supported_generation_modes:raise ValueError(f"{spec.model} does not support generation mode {mode}")
        if references and not self.assets_configured:raise ValueError("Seedance reference inputs require Ark Asset credentials and group configuration")
        roles=[item.get("role") for item in references]
        if mode=="text_to_video":
            if references:raise ValueError("Text-to-video does not accept references")
            return
        if mode=="first_frame":
            if roles!=["first_frame"]:raise ValueError("First-frame mode requires exactly one first frame")
        elif mode=="first_last_frame":
            if len(roles)!=2 or sorted(roles)!=["first_frame","last_frame"]:raise ValueError("First/last-frame mode requires exactly one first and one last frame")
        elif mode in {"multimodal_reference","edit_video","extend_video"}:
            if not references or any(role not in IMAGE_ROLES|VIDEO_ROLES|AUDIO_ROLES or role in {"first_frame","last_frame"} for role in roles):
                raise ValueError("Multimodal modes require reference_image/reference_video/reference_audio roles")
            usage={"edit_video":"edit_source","extend_video":"extend_source"}.get(mode)
            if usage:
                sources=[item for item in references if item.get("usage")==usage]
                if len(sources)!=1 or sources[0].get("role")!="reference_video":raise ValueError(f"{mode} requires exactly one reference video marked {usage}")
                if parameters.get("aspect_ratio","adaptive")!="adaptive":raise ValueError("Video edit/extend modes require adaptive aspect ratio")
                if mode=="edit_video" and parameters.get("duration",-1)!=-1:raise ValueError("Video edit mode requires duration -1")
                if mode=="extend_video" and parameters.get("extend_direction") not in {"forward","backward"}:raise ValueError("Video extension requires forward or backward direction")
        else:raise ValueError("Invalid video generation mode")
        images=[item for item in references if item.get("role") in IMAGE_ROLES]
        videos=[item for item in references if item.get("role") in VIDEO_ROLES]
        audios=[item for item in references if item.get("role") in AUDIO_ROLES]
        if len(images)>9 or len(videos)>3 or len(audios)>3:raise ValueError("Seedance supports at most 9 images, 3 videos and 3 audio references")
        if mode in {"multimodal_reference","edit_video","extend_video"} and audios and not (images or videos):raise ValueError("Seedance audio references require an image or video reference")
        _images(images,seedream=False)
        if sum(item["size"] for item in images+audios)>45*1024*1024:raise ValueError("Seedance inline image/audio references may total at most 45 MB")
        for item in videos:
            duration,width,height,fps,size=(item.get("duration_seconds"),item.get("width"),item.get("height"),item.get("fps"),item.get("size",0))
            if not duration or not 2<=duration<=15 or size<=0 or size>200*1024*1024:raise ValueError("Seedance reference video must be 2..15 seconds and at most 200 MB")
            if not width or not height or not 300<=width<=6000 or not 300<=height<=6000 or not .4<=width/height<=2.5 or not 407_696<=width*height<=8_295_044:raise ValueError("Seedance reference video dimensions are outside Provider limits")
            if fps is None or not 24<=fps<=60:raise ValueError("Seedance reference video frame rate must be 24..60 FPS")
            if item.get("codec") not in {"h264","hevc"}:raise ValueError("Seedance reference video codec must be H.264 or H.265")
            if item.get("audio_codec") and item.get("audio_codec") not in {"aac","mp3"}:raise ValueError("Seedance reference video audio codec must be AAC or MP3")
        if sum(float(item["duration_seconds"]) for item in videos)>15:raise ValueError("Seedance reference videos may total at most 15 seconds")
        for item in audios:
            duration,size=item.get("duration_seconds"),item.get("size",0)
            if not duration or not 2<=duration<=15 or size<=0 or size>15*1024*1024:raise ValueError("Seedance reference audio must be 2..15 seconds and at most 15 MB")
            if item.get("codec") not in {"mp3","pcm_s16le","pcm_s24le","pcm_f32le"}:raise ValueError("Seedance reference audio codec must be MP3 or PCM WAV")
        if sum(float(item["duration_seconds"]) for item in audios)>15:raise ValueError("Seedance reference audio may total at most 15 seconds")

    def provider_parameters(self,spec,parameters,mode):
        self.validate_shape(spec,parameters,mode);return dict(parameters)
