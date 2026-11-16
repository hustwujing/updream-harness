from __future__ import annotations

import hashlib
import json
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import AsyncIterable

from defusedxml import ElementTree

from .job_contracts import OutputDisposition


MIME_ALIASES = {
    "audio/x-wav": "audio/wav",
    "image/jpg": "image/jpeg",
    "text/x-markdown": "text/markdown",
    "application/x-pdf": "application/pdf",
}


class MimeRegistry:
    @staticmethod
    def normalize(value: str) -> str:
        mime = value.split(";", 1)[0].strip().lower()
        return MIME_ALIASES.get(mime, mime or "application/octet-stream")

    @classmethod
    def detect(cls, path: Path, declared: str = "") -> str:
        with path.open("rb") as stream:
            head = stream.read(4096)
        if head.startswith(b"\x89PNG\r\n\x1a\n"): detected = "image/png"
        elif head.startswith(b"\xff\xd8\xff"): detected = "image/jpeg"
        elif head.startswith((b"GIF87a", b"GIF89a")): detected = "image/gif"
        elif head.startswith(b"%PDF-"): detected = "application/pdf"
        elif head.startswith(b"RIFF") and head[8:12] == b"WAVE": detected = "audio/wav"
        elif len(head) >= 12 and head[4:8] == b"ftyp": detected = "video/mp4"
        elif head.startswith(b"PK\x03\x04"):
            try:
                with zipfile.ZipFile(path) as archive:
                    names = set(archive.namelist())
                detected = ("application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    if "word/document.xml" in names else
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                    if "ppt/presentation.xml" in names else
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    if "xl/workbook.xml" in names else "application/zip")
            except (OSError, zipfile.BadZipFile):
                detected = "application/octet-stream"
        else:
            try:
                text = path.read_text(encoding="utf-8")
                if all(character in "\n\r\t" or ord(character) >= 32 for character in text):
                    try:root=ElementTree.fromstring(text)
                    except (ElementTree.ParseError,ValueError):root=None
                    detected=("image/svg+xml" if root is not None and
                        root.tag in {"svg","{http://www.w3.org/2000/svg}svg"} else "text/plain")
                else:detected="application/octet-stream"
            except (OSError, UnicodeDecodeError):
                detected = "application/octet-stream"
        normalized = cls.normalize(declared)
        if normalized not in {"", "application/octet-stream"} and detected != "application/octet-stream":
            if normalized != detected and not (normalized == "text/markdown" and detected == "text/plain"):
                raise ValueError("Declared MIME does not match content")
            return normalized
        return detected

    @staticmethod
    def matches(mime: str, patterns: tuple[str, ...]) -> bool:
        return not patterns or any(pattern == mime or (
            pattern.endswith("/*") and mime.startswith(pattern[:-1])) for pattern in patterns)


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
        value.validate()
        with NamedTemporaryFile(dir=self.staging_root, prefix=".job-artifact-", delete=False) as stream:
            path = Path(stream.name);digest = hashlib.sha256();size = 0
            try:
                async for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise ValueError("Artifact stream chunks must be bytes")
                    size += len(chunk)
                    if size > self.maximum_bytes:
                        raise ValueError("Artifact exceeds byte limit")
                    digest.update(chunk);stream.write(chunk)
                stream.flush();os.fsync(stream.fileno())
            except BaseException:
                path.unlink(missing_ok=True)
                raise
        try:
            mime = MimeRegistry.detect(path, value.declared_mime)
            identity = digest.hexdigest()
            object_ref, _ = self.objects.put_file(path, digest=identity, size=size)
            return await self.store.bind_job_artifact(value, sha256=identity, size=size,
                mime=mime, object_ref=object_ref)
        finally:
            path.unlink(missing_ok=True)
