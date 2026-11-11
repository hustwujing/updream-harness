from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class MediaModelSpec:
    kind: str
    model: str
    provider: str
    adapter: str
    route: str
    execution: str
    cancellation: str
    progress: str
    credential_env: str
    enabled: bool = True

    def public(self) -> dict[str, Any]:
        value = asdict(self)
        value.pop("credential_env", None)
        value["parameter_schema"] = {
            "image": {"required": ["prompt"], "optional": {"size": "256x256..4096x4096"}},
            "video": {"required": ["prompt"], "optional": {"duration": "integer 1..12",
                "resolution": ["480p", "720p", "1080p"],
                "aspect_ratio": ["16:9", "9:16", "1:1", "4:3", "3:4"],
                "generate_audio": "boolean"}},
            "audio": {"required": ["text"], "optional": {"voice_id": "string",
                "speed": "number 0.5..2", "audio_format": ["mp3", "wav", "flac"]}},
        }[self.kind]
        return value


DEFAULT_MEDIA_CATALOG = (
    MediaModelSpec("image", "seedream-5.0", "ark", "ark_image", "/v1/providers/image", "sync", "unsupported", "none", "SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY"),
    MediaModelSpec("video", "sed2", "ark", "ark_video_v2", "/v1/providers/video", "async", "unsupported", "provider", "SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY"),
    MediaModelSpec("audio", "speech-2.8-turbo", "minimax", "minimax_t2a", "/v1/providers/audio", "async", "unsupported", "none", "SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY"),
)


class MediaCatalog:
    def __init__(self, raw: str = ""):
        self._models = self._parse(raw)

    @staticmethod
    def _parse(raw: str) -> tuple[MediaModelSpec, ...]:
        if not raw:
            return DEFAULT_MEDIA_CATALOG
        value = json.loads(raw)
        if not isinstance(value, list):
            raise ValueError("Media catalog must be a JSON array")
        result: list[MediaModelSpec] = []
        for item in value:
            if not isinstance(item, dict):
                raise ValueError("Media catalog entries must be objects")
            spec = MediaModelSpec(**item)
            if spec.kind not in {"image", "video", "audio"}:
                raise ValueError("Invalid media kind")
            if spec.execution not in {"sync", "async"}:
                raise ValueError("Invalid media execution mode")
            if spec.cancellation not in {"supported", "unsupported"}:
                raise ValueError("Invalid media cancellation mode")
            if spec.execution == "sync" and spec.cancellation == "supported":
                raise ValueError("Synchronous media jobs cannot be cancellable")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", spec.credential_env):
                raise ValueError("Invalid media credential environment")
            result.append(spec)
        identities = [(item.kind, item.model) for item in result]
        if len(identities) != len(set(identities)):
            raise ValueError("Duplicate media catalog model")
        return tuple(result)

    def resolve(self, kind: str, model: str) -> MediaModelSpec:
        for item in self._models:
            if item.enabled and item.kind == kind and item.model == model:
                return item
        raise KeyError("Media model is not enabled")

    def validate_parameters(self, kind: str, model: str, parameters: dict[str, Any]) -> None:
        self.resolve(kind, model)
        if not isinstance(parameters, dict):
            raise ValueError("Media parameters must be an object")
        allowed = {
            "image": {"prompt", "size"},
            "video": {"prompt", "duration", "resolution", "aspect_ratio", "generate_audio"},
            "audio": {"text", "voice_id", "speed", "audio_format"},
        }[kind]
        if set(parameters) - allowed:
            raise ValueError("Unsupported media parameter")
        text_key = "text" if kind == "audio" else "prompt"
        text = parameters.get(text_key)
        if not isinstance(text, str) or not text.strip() or len(text) > 10_000:
            raise ValueError(f"{text_key} must be a non-empty string of at most 10000 characters")
        if kind == "image":
            size = parameters.get("size", "2048x2048")
            match = re.fullmatch(r"([1-9][0-9]{2,3})x([1-9][0-9]{2,3})", size) if isinstance(size, str) else None
            if not match or any(not 256 <= int(value) <= 4096 for value in match.groups()):
                raise ValueError("Invalid image size")
        elif kind == "video":
            duration = parameters.get("duration", 5)
            if isinstance(duration, bool) or not isinstance(duration, int) or not 1 <= duration <= 12:
                raise ValueError("Invalid video duration")
            if parameters.get("resolution", "480p") not in {"480p", "720p", "1080p"}:
                raise ValueError("Invalid video resolution")
            if parameters.get("aspect_ratio", "16:9") not in {"16:9", "9:16", "1:1", "4:3", "3:4"}:
                raise ValueError("Invalid video aspect ratio")
            if not isinstance(parameters.get("generate_audio", False), bool):
                raise ValueError("generate_audio must be boolean")
        else:
            voice = parameters.get("voice_id", "male-qn-qingse")
            if not isinstance(voice, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", voice):
                raise ValueError("Invalid audio voice")
            speed = parameters.get("speed", 1)
            if isinstance(speed, bool) or not isinstance(speed, (int, float)) or not 0.5 <= speed <= 2:
                raise ValueError("Invalid audio speed")
            if parameters.get("audio_format", "mp3") not in {"mp3", "wav", "flac"}:
                raise ValueError("Invalid audio format")

    def public(self, kind: str | None = None) -> list[dict[str, Any]]:
        return [item.public() for item in self._models if item.enabled and (kind is None or item.kind == kind)]
