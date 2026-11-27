from __future__ import annotations

import re
from typing import Any

SUPPORTED_VOICES=("male-qn-qingse",)
MAX_CHARACTERS=9_999
MAX_ESTIMATED_SECONDS=180


def estimate_audio_duration_seconds(text:str,speed:float=1)->float:
    intervals=sum(float(value) for value in re.findall(r"<#([0-9]+(?:\.[0-9]{1,2})?)#>",text))
    spoken=re.sub(r"<#[0-9]+(?:\.[0-9]{1,2})?#>","",text)
    cjk=len(re.findall(r"[\u3400-\u9fff]",spoken))
    words=len(re.findall(r"[A-Za-z0-9]+(?:['’-][A-Za-z0-9]+)*",spoken))
    punctuation=len(re.findall(r"[，。！？；：,.!?;:]",spoken))
    return round((cjk/4.5+words/2.5+punctuation*.08)/speed+intervals,1)


class MiniMaxAudioContract:
    adapter="minimax_t2a";kind="audio"

    def validate_spec(self,spec)->None:
        if spec.provider!="minimax":raise ValueError("MiniMax audio Provider is invalid")

    def public(self,spec):
        return ({"roles":["reference_audio"],"usages":["audio"],
            "recommended_usage":{"reference_audio":"audio"}},
            {"required":["text"],"optional":{"voice_id":list(SUPPORTED_VOICES),
                "speed":"number 0.5..2","audio_format":["mp3","wav","flac"]},
             "mode_parameter_policy":"Use only allowed_parameters for the selected generation mode",
             "generation_modes":{"text_to_audio":{"references":0,
                    "allowed_parameters":["text","voice_id","speed","audio_format"]},
                "reference_voice":{"roles":["reference_audio"],"references":1,
                    "required_usage":"audio","forbidden_parameters":["voice_id"],
                    "allowed_parameters":["text","speed","audio_format"],
                    "reference_limits":{"formats":["mp3","m4a","wav"],
                        "duration_seconds":"10..300","max_bytes":20*1024*1024},
                    "effect":"clone the reference voice, then synthesize text"}},
             "limits":{"max_characters":MAX_CHARACTERS,
                "max_estimated_duration_seconds":MAX_ESTIMATED_SECONDS}})
    def public_metadata(self,spec):return {"reference_inputs_available":True}
    def availability(self,spec):return True,""

    @staticmethod
    def _allowed(mode):
        return ({"text","voice_id","speed","audio_format"} if mode=="text_to_audio"
            else {"text","speed","audio_format"} if mode=="reference_voice" else None)

    def validate_shape(self,spec,parameters,mode):
        if not isinstance(parameters,dict):raise ValueError("Media parameters must be an object")
        if mode=="reference_voice" and "voice_id" in parameters:
            raise ValueError("reference_voice does not accept voice_id; the host derives it from reference_audio")
        allowed=self._allowed(mode)
        if allowed is None:raise ValueError("Invalid audio generation mode")
        unknown=sorted(set(parameters)-allowed)
        if unknown:raise ValueError(f"Unsupported parameter(s) for {mode}: "+", ".join(unknown)+
            "; allowed: "+", ".join(sorted(allowed)))
        text=parameters.get("text")
        if not isinstance(text,str) or not text.strip() or len(text)>MAX_CHARACTERS:
            raise ValueError("text must be a non-empty string shorter than 10000 characters")
        voice=parameters.get("voice_id","male-qn-qingse")
        if mode=="text_to_audio" and voice not in SUPPORTED_VOICES:raise ValueError("Invalid audio voice")
        speed=parameters.get("speed",1)
        if isinstance(speed,bool) or not isinstance(speed,(int,float)) or not .5<=speed<=2:raise ValueError("Invalid audio speed")
        estimated=estimate_audio_duration_seconds(text,float(speed))
        if estimated>MAX_ESTIMATED_SECONDS:raise ValueError(f"Audio estimated duration is {estimated:.1f}s; split text into segments of at most 180s")
        if parameters.get("audio_format","mp3") not in {"mp3","wav","flac"}:raise ValueError("Invalid audio format")

    def validate(self,spec,parameters,mode,references):
        self.validate_shape(spec,parameters,mode)
        if mode=="text_to_audio":
            if references:raise ValueError("text_to_audio does not accept references; use reference_voice")
            return
        if len(references)!=1 or any(item.get("role")!="reference_audio" or item.get("usage")!="audio" for item in references):
            raise ValueError("reference_voice requires exactly one reference_audio with audio usage")
        item=references[0];duration,size,mime=item.get("duration_seconds"),item.get("size",0),item.get("mime")
        if not duration or not 10<=duration<=300 or size<=0 or size>20*1024*1024 or mime not in {"audio/mpeg","audio/mp4","audio/wav"}:
            raise ValueError("Voice cloning reference must be MP3, M4A or WAV, 10..300 seconds, at most 20 MB")

    def provider_parameters(self,spec,parameters,mode):
        self.validate_shape(spec,parameters,mode);return dict(parameters)
