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

    def public(self, kind: str | None = None) -> list[dict[str, Any]]:
        return [item.public() for item in self._models if item.enabled and (kind is None or item.kind == kind)]
