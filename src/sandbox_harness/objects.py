from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


class ObjectStore:
    """Content-addressed local backend for development."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, digest: str) -> Path:
        return self.root / digest[:2] / digest[2:]

    def put(self, data: bytes) -> tuple[str, str]:
        digest = hashlib.sha256(data).hexdigest()
        target = self._path(digest)
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(dir=target.parent, delete=False) as stream:
                stream.write(data)
                temporary = Path(stream.name)
            os.chmod(temporary, 0o600)
            temporary.replace(target)
        return "sha256:" + digest, digest

    def get(self, reference: str, *, maximum: int) -> bytes:
        if not reference.startswith("sha256:"):
            raise ValueError("Unknown object reference")
        digest = reference.removeprefix("sha256:")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Invalid object digest")
        path = self._path(digest)
        if path.stat().st_size > maximum:
            raise ValueError("Object exceeds byte limit")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Object integrity check failed")
        return data


class S3ObjectStore:
    """Content-addressed BOSS/S3 backend scoped to one immutable key prefix."""

    def __init__(self, *, bucket: str, prefix: str, endpoint: str = "", client: Any = None):
        if not bucket:
            raise ValueError("S3 object backend requires a bucket")
        normalized = prefix.strip("/")
        if not normalized or ".." in normalized.split("/"):
            raise ValueError("S3 object prefix must be a non-empty safe path")
        self.bucket = bucket
        self.prefix = normalized
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint or None,
                config=Config(
                    signature_version="s3v4",
                    connect_timeout=5,
                    read_timeout=30,
                    retries={"max_attempts": 2},
                    s3={"addressing_style": "path"},
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                ),
            )
        self.client = client

    def _key(self, digest: str) -> str:
        return f"{self.prefix}/objects/{digest[:2]}/{digest[2:]}"

    def put(self, data: bytes) -> tuple[str, str]:
        digest = hashlib.sha256(data).hexdigest()
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(digest),
                Body=data,
                IfNoneMatch="*",
            )
        except Exception as exc:
            response = getattr(exc, "response", {})
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (409, 412):
                raise
        return "sha256:" + digest, digest

    def get(self, reference: str, *, maximum: int) -> bytes:
        if not reference.startswith("sha256:"):
            raise ValueError("Unknown object reference")
        digest = reference.removeprefix("sha256:")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Invalid object digest")
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(digest))
        body = response["Body"]
        try:
            size = int(response.get("ContentLength", 0))
            if size > maximum:
                raise ValueError("Object exceeds byte limit")
            data = body.read(maximum + 1)
        finally:
            body.close()
        if len(data) > maximum:
            raise ValueError("Object exceeds byte limit")
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Object integrity check failed")
        return data


def object_store(settings):
    if settings.object_backend == "s3":
        return S3ObjectStore(
            bucket=settings.object_bucket,
            prefix=settings.object_prefix,
            endpoint=settings.object_endpoint,
        )
    return ObjectStore(settings.data_root / "objects")
