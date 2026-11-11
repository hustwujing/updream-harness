from __future__ import annotations

import asyncio
import io
from pathlib import PurePosixPath
from typing import Any

import av
from PIL import Image

from .domain import MediaReferenceInput


MAX_REFERENCE_BYTES = 200 * 1024 * 1024


def normalize_project_path(value: str) -> str:
    path = value.strip().replace("\\", "/")
    prefix = "/workspace/project/"
    if path.startswith(prefix):
        path = path[len(prefix):]
    if path.startswith("./"):
        path = path[2:]
    normalized = PurePosixPath(path)
    if not path or normalized.is_absolute() or ".." in normalized.parts:
        raise ValueError("Invalid reference project path")
    return normalized.as_posix()


def _duration(container: av.container.InputContainer, stream: Any) -> float | None:
    if container.duration is not None:
        return round(float(container.duration / av.time_base), 3)
    if stream.duration is not None and stream.time_base is not None:
        return round(float(stream.duration * stream.time_base), 3)
    return None


def inspect_reference(data: bytes, role: str, path: str) -> dict[str, Any]:
    if not data:
        raise ValueError("Reference file is empty")
    if role in {"reference_image", "first_frame", "last_frame"}:
        try:
            with Image.open(io.BytesIO(data)) as image:
                image.verify()
                image_format = (image.format or "").upper()
            with Image.open(io.BytesIO(data)) as image:
                width, height = image.size
            mime = {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp",
                "BMP": "image/bmp", "TIFF": "image/tiff", "GIF": "image/gif",
                "HEIF": "image/heif", "HEIC": "image/heic"}.get(image_format)
            if not mime:
                raise ValueError("Unsupported reference image format")
            return {"mime": mime, "width": width, "height": height}
        except (Image.UnidentifiedImageError, OSError):
            try:
                with av.open(io.BytesIO(data)) as container:
                    stream = next((item for item in container.streams if item.type == "video"), None)
                    if not stream or not stream.width or not stream.height:
                        raise ValueError("Unsupported reference image format")
                    suffix = PurePosixPath(path).suffix.lower()
                    mime = "image/heic" if suffix == ".heic" else "image/heif" if suffix == ".heif" else ""
                    if not mime:
                        raise ValueError("Unsupported reference image format")
                    return {"mime": mime, "width": stream.width, "height": stream.height}
            except (av.error.FFmpegError, StopIteration) as exc:
                raise ValueError("Reference image cannot be decoded") from exc
    try:
        with av.open(io.BytesIO(data)) as container:
            stream_type = "video" if role == "reference_video" else "audio"
            stream = next((item for item in container.streams if item.type == stream_type), None)
            if not stream:
                raise ValueError(f"Reference file has no {stream_type} stream")
            duration = _duration(container, stream)
            if role == "reference_video":
                suffix = PurePosixPath(path).suffix.lower()
                if suffix not in {".mp4", ".mov"}:
                    raise ValueError("Reference video must be MP4 or MOV")
                fps = float(stream.average_rate) if stream.average_rate else None
                audio_stream = next((item for item in container.streams if item.type == "audio"), None)
                return {"mime": "video/quicktime" if suffix == ".mov" else "video/mp4",
                    "width": stream.width, "height": stream.height,
                    "duration_seconds": duration, "fps": round(fps, 3) if fps else None,
                    "codec": stream.codec_context.name or "",
                    "audio_codec": audio_stream.codec_context.name if audio_stream else ""}
            suffix = PurePosixPath(path).suffix.lower()
            if suffix not in {".mp3", ".wav"}:
                raise ValueError("Reference audio must be MP3 or WAV")
            return {"mime": "audio/wav" if suffix == ".wav" else "audio/mpeg",
                "duration_seconds": duration, "codec": stream.codec_context.name or ""}
    except av.error.FFmpegError as exc:
        raise ValueError("Reference media cannot be decoded") from exc


async def resolve_media_references(store, objects, thread_id: str, owner_id: str,
                                   references: list[MediaReferenceInput]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    inspected: dict[str, dict[str, Any]] = {}
    seen: set[tuple[str, str, str]] = set()
    for reference in references:
        path = normalize_project_path(reference.project_path) if reference.project_path else ""
        project_file = await store.resolve_project_file_reference(thread_id, owner_id,
            file_id=reference.project_file_id, path=path)
        if not project_file:
            raise ValueError("Reference project file is missing, stale, or not owned by this Thread")
        identity = (project_file.id, reference.role, reference.usage)
        if identity in seen:
            raise ValueError("Duplicate media reference")
        seen.add(identity)
        if project_file.object_ref not in inspected:
            data = await asyncio.to_thread(objects.get, project_file.object_ref,
                maximum=MAX_REFERENCE_BYTES)
            inspected[project_file.object_ref] = await asyncio.to_thread(
                inspect_reference, data, reference.role, project_file.path)
        metadata = inspected[project_file.object_ref]
        expected = {"reference_image": "image/", "first_frame": "image/", "last_frame": "image/",
            "reference_video": "video/", "reference_audio": "audio/"}[reference.role]
        if not metadata["mime"].startswith(expected):
            raise ValueError("Reference role does not match file media type")
        snapshots.append({"project_file_id": project_file.id, "path": project_file.path,
            "version": project_file.version, "sha256": project_file.sha256,
            "size": project_file.size, "mime": metadata["mime"],
            "kind": metadata["mime"].split("/", 1)[0],
            "object_ref": project_file.object_ref, "role": reference.role,
            "usage": reference.usage, "width": metadata.get("width"),
            "height": metadata.get("height"), "duration_seconds": metadata.get("duration_seconds"),
            "fps": metadata.get("fps"), "codec": metadata.get("codec", ""),
            "audio_codec": metadata.get("audio_codec", "")})
    return snapshots
