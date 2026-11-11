from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any


SUPPORTED_AUDIO_VOICES = ("male-qn-qingse",)
MAX_AUDIO_CHARACTERS = 9_999
MAX_AUDIO_ESTIMATED_SECONDS = 180
MIN_SEEDREAM_LITE_PIXELS = 2560 * 1440
MAX_SEEDREAM_LITE_PIXELS = 4096 * 4096
SEEDREAM_LITE_SIZE_TIERS = ("2K", "3K", "4K")
IMAGE_REFERENCE_ROLES = {"reference_image", "first_frame", "last_frame"}
VIDEO_REFERENCE_ROLES = {"reference_video"}
AUDIO_REFERENCE_ROLES = {"reference_audio"}


def estimate_audio_duration_seconds(text: str, speed: float = 1) -> float:
    intervals = sum(float(value) for value in re.findall(r"<#([0-9]+(?:\.[0-9]{1,2})?)#>", text))
    spoken = re.sub(r"<#[0-9]+(?:\.[0-9]{1,2})?#>", "", text)
    cjk = len(re.findall(r"[\u3400-\u9fff]", spoken))
    words = len(re.findall(r"[A-Za-z0-9]+(?:['’-][A-Za-z0-9]+)*", spoken))
    punctuation = len(re.findall(r"[，。！？；：,.!?;:]", spoken))
    return round((cjk / 4.5 + words / 2.5 + punctuation * 0.08) / speed + intervals, 1)


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
        value["reference_schema"] = {
            "image": {"roles": ["reference_image"],
                "usages": ["subject", "style", "composition"],
                "recommended_usage": {"reference_image": "subject"}},
            "video": {"roles": ["reference_image", "reference_video", "reference_audio",
                    "first_frame", "last_frame"],
                "usages": ["subject", "style", "composition", "action", "camera", "audio",
                    "edit_source", "extend_source"],
                "recommended_usage": {"first_frame": "composition", "last_frame": "composition",
                    "reference_image": "subject", "reference_video": "action",
                    "reference_audio": "audio"}},
            "audio": {"roles": [], "usages": [], "recommended_usage": {}},
        }[self.kind]
        value["parameter_schema"] = {
            "image": {"required": ["prompt"], "optional": {
                "size": [*SEEDREAM_LITE_SIZE_TIERS, "explicit WIDTHxHEIGHT"]},
                "generation_modes": {"text_to_image": {"references": 0},
                    "reference_image": {"roles": ["reference_image"], "max_references": 14}},
                "limits": {"min_total_pixels": MIN_SEEDREAM_LITE_PIXELS,
                    "max_total_pixels": MAX_SEEDREAM_LITE_PIXELS,
                    "aspect_ratio": "1/16..16"}},
            "video": {"required": ["prompt"], "optional": {"duration": "integer 4..15 or -1",
                "resolution": ["480p", "720p", "1080p", "4k"],
                "aspect_ratio": ["adaptive", "21:9", "16:9", "9:16", "1:1", "4:3", "3:4"],
                "generate_audio": "boolean", "extend_direction": ["forward", "backward"]},
                "generation_modes": {
                    "text_to_video": {"references": 0},
                    "first_frame": {"roles": ["first_frame"], "references": 1},
                    "first_last_frame": {"roles": ["first_frame", "last_frame"], "references": 2},
                    "multimodal_reference": {"roles": ["reference_image", "reference_video", "reference_audio"],
                        "max_images": 9, "max_videos": 3, "max_audio": 3},
                    "edit_video": {"roles": ["reference_image", "reference_video", "reference_audio"],
                        "required_usage": "edit_source"},
                    "extend_video": {"roles": ["reference_image", "reference_video", "reference_audio"],
                        "required_usage": "extend_source"}}},
            "audio": {"required": ["text"], "optional": {"voice_id": list(SUPPORTED_AUDIO_VOICES),
                "speed": "number 0.5..2", "audio_format": ["mp3", "wav", "flac"]},
                "generation_modes": {"text_to_audio": {"references": 0}},
                "limits": {"max_characters": MAX_AUDIO_CHARACTERS,
                    "max_estimated_duration_seconds": MAX_AUDIO_ESTIMATED_SECONDS}},
        }[self.kind]
        return value


DEFAULT_MEDIA_CATALOG = (
    MediaModelSpec("image", "seedream-5.0", "ark", "ark_image", "/v1/providers/image", "sync", "unsupported", "none", "SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY"),
    MediaModelSpec("video", "sed2", "ark", "ark_video_v2", "/v1/providers/video", "async", "unsupported", "provider", "SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY"),
    MediaModelSpec("audio", "speech-2.8-turbo", "minimax", "minimax_t2a", "/v1/providers/audio", "async", "unsupported", "none", "SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY"),
)


class MediaCatalog:
    def __init__(self, raw: str = "", *, ark_assets_configured: bool = True):
        self._models = self._parse(raw)
        self.ark_assets_configured = ark_assets_configured

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

    def validate_parameters(self, kind: str, model: str, parameters: dict[str, Any], *,
                            generation_mode: str | None = None,
                            references: list[dict[str, Any]] | None = None) -> None:
        self.resolve(kind, model)
        generation_mode = generation_mode or {"image": "text_to_image", "video": "text_to_video",
            "audio": "text_to_audio"}[kind]
        references = references or []
        if kind == "video" and references and not self.ark_assets_configured:
            raise ValueError("Seedance reference inputs require Ark Asset credentials and group configuration")
        if not isinstance(parameters, dict):
            raise ValueError("Media parameters must be an object")
        allowed = {
            "image": {"prompt", "size"},
            "video": {"prompt", "duration", "resolution", "aspect_ratio", "generate_audio", "extend_direction"},
            "audio": {"text", "voice_id", "speed", "audio_format"},
        }[kind]
        if set(parameters) - allowed:
            raise ValueError("Unsupported media parameter")
        text_key = "text" if kind == "audio" else "prompt"
        text = parameters.get(text_key)
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_AUDIO_CHARACTERS:
            raise ValueError(f"{text_key} must be a non-empty string shorter than 10000 characters")
        if kind == "image":
            size = parameters.get("size", "2048x2048")
            if size not in SEEDREAM_LITE_SIZE_TIERS:
                match = re.fullmatch(r"([1-9][0-9]{1,3})x([1-9][0-9]{1,3})", size) if isinstance(size, str) else None
                if not match:
                    raise ValueError("Image size must be 2K, 3K, 4K, or explicit WIDTHxHEIGHT")
                width, height = (int(value) for value in match.groups())
                pixels, ratio = width * height, width / height
                if not MIN_SEEDREAM_LITE_PIXELS <= pixels <= MAX_SEEDREAM_LITE_PIXELS or not 1 / 16 <= ratio <= 16:
                    raise ValueError("Seedream 5.0 image size must have 3686400..16777216 total pixels and aspect ratio 1/16..16; use 2K or at least 2560x1440")
            if generation_mode == "text_to_image" and references:
                raise ValueError("Text-to-image does not accept references")
            if generation_mode == "reference_image":
                if not 1 <= len(references) <= 14 or any(item.get("role") != "reference_image" for item in references):
                    raise ValueError("Seedream reference image mode requires 1..14 reference images")
                self._validate_image_references(references, seedream=True)
                if sum(item["size"] for item in references) > 45 * 1024 * 1024:
                    raise ValueError("Seedream inline references may total at most 45 MB")
            elif generation_mode != "text_to_image":
                raise ValueError("Invalid image generation mode")
        elif kind == "video":
            duration = parameters.get("duration", 5)
            if isinstance(duration, bool) or not isinstance(duration, int) or duration != -1 and not 4 <= duration <= 15:
                raise ValueError("Video duration must be -1 or an integer from 4 to 15 seconds")
            if parameters.get("resolution", "480p") not in {"480p", "720p", "1080p", "4k"}:
                raise ValueError("Invalid video resolution")
            if parameters.get("aspect_ratio", "16:9") not in {"adaptive", "21:9", "16:9", "9:16", "1:1", "4:3", "3:4"}:
                raise ValueError("Invalid video aspect ratio")
            if not isinstance(parameters.get("generate_audio", False), bool):
                raise ValueError("generate_audio must be boolean")
            direction = parameters.get("extend_direction")
            if direction is not None and direction not in {"forward", "backward"}:
                raise ValueError("Invalid video extension direction")
            self._validate_video_mode(generation_mode, parameters, references)
        else:
            voice = parameters.get("voice_id", "male-qn-qingse")
            if voice not in SUPPORTED_AUDIO_VOICES:
                raise ValueError("Invalid audio voice")
            speed = parameters.get("speed", 1)
            if isinstance(speed, bool) or not isinstance(speed, (int, float)) or not 0.5 <= speed <= 2:
                raise ValueError("Invalid audio speed")
            estimated = estimate_audio_duration_seconds(text, float(speed))
            if estimated > MAX_AUDIO_ESTIMATED_SECONDS:
                raise ValueError(f"Audio estimated duration is {estimated:.1f}s; split text into segments of at most 180s")
            if parameters.get("audio_format", "mp3") not in {"mp3", "wav", "flac"}:
                raise ValueError("Invalid audio format")
            if generation_mode != "text_to_audio" or references:
                raise ValueError("Audio generation does not accept media references")

    @staticmethod
    def _validate_image_references(references: list[dict[str, Any]], *, seedream: bool = False) -> None:
        for item in references:
            width, height, size = item.get("width"), item.get("height"), item.get("size", 0)
            if not isinstance(width, int) or not isinstance(height, int) or width <= 0 or height <= 0:
                raise ValueError("Reference image dimensions are unavailable")
            if size <= 0 or size > 30 * 1024 * 1024:
                raise ValueError("Reference image must be smaller than 30 MB")
            ratio = width / height
            if seedream:
                if width <= 14 or height <= 14 or width * height > 36_000_000 or not 1 / 16 <= ratio <= 16:
                    raise ValueError("Seedream reference image dimensions are outside Provider limits")
            elif (size >= 30 * 1024 * 1024 or not 300 < width < 6000 or not 300 < height < 6000
                    or not .4 < ratio < 2.5):
                raise ValueError("Seedance reference image dimensions are outside Provider limits")

    @classmethod
    def _validate_video_mode(cls, mode: str, parameters: dict[str, Any],
                             references: list[dict[str, Any]]) -> None:
        roles = [item.get("role") for item in references]
        if mode == "text_to_video":
            if references: raise ValueError("Text-to-video does not accept references")
            return
        if mode == "first_frame":
            if roles != ["first_frame"]: raise ValueError("First-frame mode requires exactly one first frame")
        elif mode == "first_last_frame":
            if len(roles) != 2 or sorted(roles) != ["first_frame", "last_frame"]:
                raise ValueError("First/last-frame mode requires exactly one first and one last frame")
        elif mode in {"multimodal_reference", "edit_video", "extend_video"}:
            if not references or any(role not in IMAGE_REFERENCE_ROLES | VIDEO_REFERENCE_ROLES | AUDIO_REFERENCE_ROLES
                    or role in {"first_frame", "last_frame"} for role in roles):
                raise ValueError("Multimodal modes require reference_image/reference_video/reference_audio roles")
            source_usage = {"edit_video": "edit_source", "extend_video": "extend_source"}.get(mode)
            if source_usage:
                sources = [item for item in references if item.get("usage") == source_usage]
                if len(sources) != 1 or sources[0].get("role") != "reference_video":
                    raise ValueError(f"{mode} requires exactly one reference video marked {source_usage}")
                if parameters.get("aspect_ratio", "adaptive") != "adaptive":
                    raise ValueError("Video edit/extend modes require adaptive aspect ratio")
                if mode == "edit_video" and parameters.get("duration", -1) != -1:
                    raise ValueError("Video edit mode requires duration -1")
                if mode == "extend_video" and parameters.get("extend_direction") not in {"forward", "backward"}:
                    raise ValueError("Video extension requires forward or backward direction")
        else:
            raise ValueError("Invalid video generation mode")
        images = [item for item in references if item.get("role") in IMAGE_REFERENCE_ROLES]
        videos = [item for item in references if item.get("role") in VIDEO_REFERENCE_ROLES]
        audios = [item for item in references if item.get("role") in AUDIO_REFERENCE_ROLES]
        if len(images) > 9 or len(videos) > 3 or len(audios) > 3:
            raise ValueError("Seedance supports at most 9 images, 3 videos and 3 audio references")
        if mode in {"multimodal_reference", "edit_video", "extend_video"} and audios and not (images or videos):
            raise ValueError("Seedance 2.0 audio references require an image or video reference")
        cls._validate_image_references(images)
        if sum(item["size"] for item in images + audios) > 45 * 1024 * 1024:
            raise ValueError("Seedance inline image/audio references may total at most 45 MB")
        for item in videos:
            duration, width, height, fps, size = (item.get("duration_seconds"), item.get("width"),
                item.get("height"), item.get("fps"), item.get("size", 0))
            if not duration or not 2 <= duration <= 15 or size <= 0 or size > 200 * 1024 * 1024:
                raise ValueError("Seedance reference video must be 2..15 seconds and at most 200 MB")
            if (not width or not height or not 300 <= width <= 6000 or not 300 <= height <= 6000
                    or not .4 <= width / height <= 2.5 or not 407_696 <= width * height <= 8_295_044):
                raise ValueError("Seedance reference video dimensions are outside Provider limits")
            if fps is None or not 24 <= fps <= 60:
                raise ValueError("Seedance reference video frame rate must be 24..60 FPS")
            if item.get("codec") not in {"h264", "hevc"}:
                raise ValueError("Seedance reference video codec must be H.264 or H.265")
            if item.get("audio_codec") and item.get("audio_codec") not in {"aac", "mp3"}:
                raise ValueError("Seedance reference video audio codec must be AAC or MP3")
        if sum(float(item["duration_seconds"]) for item in videos) > 15:
            raise ValueError("Seedance reference videos may total at most 15 seconds")
        for item in audios:
            duration, size = item.get("duration_seconds"), item.get("size", 0)
            if not duration or not 2 <= duration <= 15 or size <= 0 or size > 15 * 1024 * 1024:
                raise ValueError("Seedance reference audio must be 2..15 seconds and at most 15 MB")
            if item.get("codec") not in {"mp3", "pcm_s16le", "pcm_s24le", "pcm_f32le"}:
                raise ValueError("Seedance reference audio codec must be MP3 or PCM WAV")
        if sum(float(item["duration_seconds"]) for item in audios) > 15:
            raise ValueError("Seedance reference audio may total at most 15 seconds")

    def public(self, kind: str | None = None) -> list[dict[str, Any]]:
        values = [item.public() for item in self._models if item.enabled and (kind is None or item.kind == kind)]
        for value in values:
            if value["kind"] == "video":
                value["reference_inputs_available"] = self.ark_assets_configured
                if not self.ark_assets_configured:
                    value["reference_inputs_error"] = "ark_asset_configuration_missing"
        return values
