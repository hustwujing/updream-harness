from __future__ import annotations

import hashlib
import json
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path,PurePosixPath
from tempfile import NamedTemporaryFile
from typing import AsyncIterable

from defusedxml import ElementTree

from sandbox_harness.common.contracts.job_contracts import (OutputDisposition)
from sandbox_harness.common.contracts.job_errors import (ArtifactValidationError)


MIME_ALIASES = {
    "audio/x-wav": "audio/wav",
    "image/jpg": "image/jpeg",
    "text/x-markdown": "text/markdown",
    "text/x-sh": "text/x-shellscript",
    "application/x-sh": "text/x-shellscript",
    "application/x-pdf": "application/pdf",
    "application/x-gzip": "application/gzip",
    "application/x-tar": "application/gzip",
}


class MimeRegistry:
    @staticmethod
    def _ebml_document_type(head: bytes) -> bytes | None:
        """Read a bounded EBML header's DocType, never a match inside payload data."""
        def vint(offset: int, limit: int, *, identifier: bool = False):
            if offset>=limit or head[offset]==0:raise ValueError('invalid_ebml_vint')
            width=9-head[offset].bit_length()
            if width>(4 if identifier else 8) or offset+width>limit:
                raise ValueError('truncated_ebml_vint')
            value=int.from_bytes(head[offset:offset+width],'big')
            if not identifier:
                mask=(1<<(7*width))-1;value&=mask
                if value==mask:raise ValueError('unbounded_ebml_header')
            return value,offset+width
        try:
            size,offset=vint(4,len(head));end=offset+size
            if end>len(head):return None
            document=None
            while offset<end:
                identity,offset=vint(offset,end,identifier=True)
                size,offset=vint(offset,end)
                if offset+size>end:return None
                if identity==0x4282:
                    if document is not None:return None
                    document=head[offset:offset+size].rstrip(b'\x00')
                offset+=size
            return document
        except (ValueError,IndexError):return None

    @staticmethod
    def normalize(value: str) -> str:
        mime = value.split(";", 1)[0].strip().lower()
        return MIME_ALIASES.get(mime, mime or "application/octet-stream")

    @classmethod
    def detect(cls, path: Path, declared: str = "") -> str:
        normalized = cls.normalize(declared)
        with path.open("rb") as stream:
            head = stream.read(4096)
        if head.startswith(b"\x89PNG\r\n\x1a\n"): detected = "image/png"
        elif head.startswith(b"\xff\xd8\xff"): detected = "image/jpeg"
        elif head.startswith((b"GIF87a", b"GIF89a")): detected = "image/gif"
        elif head.startswith(b"8BPS"):detected="image/vnd.adobe.photoshop"
        elif head.startswith((b"II*\x00",b"MM\x00*",b"II+\x00",b"MM\x00+")):
            detected="image/tiff"
        elif head.startswith(b"%PDF-"): detected = "application/pdf"
        elif head.startswith(b"RIFF") and head[8:12] == b"WEBP": detected = "image/webp"
        elif head.startswith(b"RIFF") and head[8:12] == b"WAVE": detected = "audio/wav"
        elif head.startswith(b"ID3") or len(head)>=2 and head[0]==0xff and head[1]&0xe0==0xe0:
            detected = "audio/mpeg"
        elif len(head) >= 12 and head[4:8] == b"ftyp":
            detected = "audio/mp4" if normalized=="audio/mp4" else "video/mp4"
        elif head.startswith(b'\x1a\x45\xdf\xa3'):
            document=cls._ebml_document_type(head)
            family={b'matroska':'x-matroska',b'webm':'webm'}.get(document)
            detected=(('audio/' if normalized=='audio/'+family else 'video/')+family
                if family else 'application/octet-stream')
        elif head.startswith(b"glTF"):detected="model/gltf-binary"
        elif head.startswith(b"Kaydara FBX Binary"):detected="application/vnd.autodesk.fbx"
        elif head.startswith(b"\x1f\x8b"):detected="application/gzip"
        elif head.startswith(b"PK\x03\x04"):
            try:
                with zipfile.ZipFile(path) as archive:
                    names=set(archive.namelist())
                    safe=all(not PurePosixPath(name).is_absolute() and ".." not in PurePosixPath(name).parts
                        for name in names)
                    openraster=False
                    if (safe and {'mimetype','stack.xml'}<=names
                            and archive.getinfo('mimetype').file_size<=128):
                        with archive.open('mimetype') as metadata:
                            openraster=metadata.read(129).strip()==b'image/openraster'
                model_archive=(normalized=="model/obj+zip" and any(name.lower().endswith(".obj") for name in names)
                    or normalized=="model/gltf+zip" and any(name.lower().endswith((".gltf",".glb")) for name in names)
                    or normalized=="model/vnd.usdz+zip" and any(name.lower().endswith((".usd",".usda",".usdc")) for name in names)
                    or normalized=="model/3mf" and "[Content_Types].xml" in names
                        and any(name.lower().endswith(".model") for name in names))
                detected = ("image/openraster" if openraster else
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    if "word/document.xml" in names else
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                    if "ppt/presentation.xml" in names else
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    if "xl/workbook.xml" in names else normalized if safe and model_archive
                    else "application/zip")
            except zipfile.BadZipFile:
                detected = "application/octet-stream"
        else:
            try:
                text = path.read_text(encoding="utf-8")
                if all(character in "\n\r\t" or ord(character) >= 32 for character in text):
                    try:is_json=isinstance(json.loads(text),(dict,list))
                    except (json.JSONDecodeError,ValueError):is_json=False
                    try:root=ElementTree.fromstring(text)
                    except (ElementTree.ParseError,ValueError):root=None
                    detected=("text/x-shellscript" if normalized=="text/x-shellscript" and re.match(
                        r"^#![^\n]*(?:/|\b)(?:ba|da|k|z)?sh(?:\s|$)",text) else
                        "model/obj" if normalized=="model/obj" and re.search(
                        r"(?m)^\s*(?:v|f)\s+",text) else
                        "model/gltf+json" if normalized=="model/gltf+json" and is_json
                            and isinstance(json.loads(text),dict) and isinstance(json.loads(text).get("asset"),dict) else
                        "model/stl" if normalized=="model/stl" and text.lstrip().startswith("solid ") else
                        "application/json" if is_json else "image/svg+xml" if root is not None and
                        root.tag in {"svg","{http://www.w3.org/2000/svg}svg"} else "text/plain")
                else:detected="application/octet-stream"
            except UnicodeDecodeError:
                detected = "application/octet-stream"
        if normalized not in {"", "application/octet-stream"} and detected != "application/octet-stream":
            if normalized != detected and not (normalized == "text/markdown" and detected == "text/plain"):
                raise ArtifactValidationError('artifact_mime_mismatch',"Declared MIME does not match content")
            return normalized
        return detected

    @classmethod
    def matches(cls, mime: str, patterns: tuple[str, ...]) -> bool:
        actual=cls.normalize(mime)
        return not patterns or any(cls.normalize(pattern) == actual or (
            pattern.endswith("/*") and actual.startswith(pattern[:-1].lower())) for pattern in patterns)


@dataclass(frozen=True)
class ArtifactEmission:
    job_id: str
    generation: int
    artifact_role: str
    item_key: str
    position: int
    filename: str
    output_disposition: OutputDisposition
    declared_mime: str = "application/octet-stream"

    def validate(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{32}", self.job_id) or self.generation < 1:
            raise ValueError("Invalid Job artifact identity")
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", self.artifact_role):
            raise ValueError("Invalid artifact role")
        if not self.item_key or self.position < 0 or Path(self.filename).name != self.filename:
            raise ValueError("Invalid artifact collection item")


class ArtifactGateway:
    def __init__(self, store, objects, staging_root: Path, *, maximum_bytes: int):
        self.store = store
        self.objects = objects
        self.staging_root = staging_root
        self.maximum_bytes = maximum_bytes
        self.staging_root.mkdir(parents=True, exist_ok=True)

    async def emit(self, value: ArtifactEmission, chunks: AsyncIterable[bytes]):
        return await self._emit(value,chunks)

    async def emit_matching(self,value:ArtifactEmission,chunks:AsyncIterable[bytes],*,
                            mime_patterns:tuple[str,...]):
        """Select optional source formats without widening the frozen output contract."""
        return await self._emit(value,chunks,mime_patterns=mime_patterns)

    async def _emit(self,value:ArtifactEmission,chunks:AsyncIterable[bytes],*,
                    mime_patterns:tuple[str,...]|None=None):
        value.validate()
        with NamedTemporaryFile(dir=self.staging_root, prefix=".job-artifact-", delete=False) as stream:
            path = Path(stream.name);digest = hashlib.sha256();size = 0
            try:
                async for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise ValueError("Artifact stream chunks must be bytes")
                    size += len(chunk)
                    if size > self.maximum_bytes:
                        raise ArtifactValidationError('artifact_item_too_large',"Artifact exceeds byte limit")
                    digest.update(chunk);stream.write(chunk)
                stream.flush();os.fsync(stream.fileno())
            except BaseException:
                path.unlink(missing_ok=True)
                raise
        try:
            mime = MimeRegistry.detect(path, value.declared_mime)
            if mime_patterns is not None and not MimeRegistry.matches(mime,mime_patterns):
                return None
            identity = digest.hexdigest()
            object_ref, _ = self.objects.put_file(path, digest=identity, size=size)
            return await self.store.bind_job_artifact(value, sha256=identity, size=size,
                mime=mime, object_ref=object_ref)
        finally:
            path.unlink(missing_ok=True)
