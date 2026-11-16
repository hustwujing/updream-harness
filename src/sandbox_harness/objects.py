from __future__ import annotations

from .observability import observed_sync

import hashlib
import os
import re
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import quote, urlsplit


MEDIA_EXTENSIONS = {
    "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif",
    "image/bmp": "bmp", "image/tiff": "tiff", "image/heic": "heic", "image/heif": "heif",
    "video/mp4": "mp4", "video/quicktime": "mov", "audio/mpeg": "mp3", "audio/wav": "wav",
}


class ObjectStore:
    """Content-addressed local backend for development."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, digest: str) -> Path:
        return self.root / digest[:2] / digest[2:]

    @staticmethod
    def _digest(reference: str) -> str:
        if not reference.startswith("sha256:"):
            raise ValueError("Unknown object reference")
        digest = reference.removeprefix("sha256:")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Invalid object digest")
        return digest

    @observed_sync("object.put")
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

    @observed_sync("object.put_file")
    def put_file(self, source: Path, *, digest: str, size: int) -> tuple[str, str]:
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or source.stat().st_size != size:
            raise ValueError("Invalid staged object identity")
        target = self._path(digest)
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(dir=target.parent, delete=False) as stream:
                temporary = Path(stream.name)
                with source.open("rb") as incoming:
                    shutil.copyfileobj(incoming, stream, length=1024 * 1024)
            os.chmod(temporary, 0o600)
            temporary.replace(target)
        return "sha256:" + digest, digest

    @observed_sync("object.get")
    def get(self, reference: str, *, maximum: int) -> bytes:
        digest = self._digest(reference)
        path = self._path(digest)
        if path.stat().st_size > maximum:
            raise ValueError("Object exceeds byte limit")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Object integrity check failed")
        return data

    def size(self, reference: str, *, maximum: int) -> int:
        size = self._path(self._digest(reference)).stat().st_size
        if size > maximum:
            raise ValueError("Object exceeds byte limit")
        return size

    def iter_bytes(self, reference: str, *, maximum: int, start: int = 0,
                   end: int | None = None, chunk_size: int = 1024 * 1024):
        digest = self._digest(reference); size = self.size(reference, maximum=maximum)
        end = size - 1 if end is None else end
        if start < 0 or end < start or end >= size:
            raise ValueError("Invalid object byte range")
        full = start == 0 and end == size - 1; hasher = hashlib.sha256() if full else None
        remaining = end - start + 1
        with self._path(digest).open("rb") as stream:
            stream.seek(start)
            while remaining:
                data = stream.read(min(chunk_size, remaining))
                if not data:
                    raise ValueError("Object ended before the declared range")
                remaining -= len(data)
                if hasher: hasher.update(data)
                yield data
        if hasher and hasher.hexdigest() != digest:
            raise ValueError("Object integrity check failed")

    def prepare_media_upload(self, **_):
        raise RuntimeError("Public media upload requires the configured BOSS backend")


class S3ObjectStore:
    """Content-addressed BOSS/S3 backend scoped to one immutable key prefix."""

    def __init__(self, *, bucket: str, prefix: str, endpoint: str = "", region: str = "updream",
                 public_endpoint: str = "https://boss-shjd.biliapi.net",
                 media_prefix: str = "aniforge", client: Any = None, public_client: Any = None):
        if not bucket:
            raise ValueError("S3 object backend requires a bucket")
        normalized = prefix.strip("/")
        if not normalized or ".." in normalized.split("/"):
            raise ValueError("S3 object prefix must be a non-empty safe path")
        self.bucket = bucket
        self.prefix = normalized
        self.region = region
        self.public_endpoint = public_endpoint.rstrip("/")
        self.media_prefix = media_prefix.strip("/")
        parsed = urlsplit(self.public_endpoint)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path:
            raise ValueError("BOSS public endpoint must be an HTTPS origin")
        if self.media_prefix != "aniforge":
            raise ValueError("Media reference prefix must be aniforge")
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint or None,
                region_name=region,
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
            public_client = boto3.client("s3", endpoint_url=self.public_endpoint,
                region_name=region, config=Config(signature_version="s3v4",
                    connect_timeout=5, read_timeout=30, retries={"max_attempts": 2},
                    s3={"addressing_style": "path"}))
        self.client = client
        self.public_client = public_client or client

    def _key(self, digest: str) -> str:
        return f"{self.prefix}/objects/{digest[:2]}/{digest[2:]}"

    @staticmethod
    def _digest(reference: str) -> str:
        return ObjectStore._digest(reference)

    @observed_sync("object.put")
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

    @observed_sync("object.put_file")
    def put_file(self, source: Path, *, digest: str, size: int) -> tuple[str, str]:
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or source.stat().st_size != size:
            raise ValueError("Invalid staged object identity")
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(digest))
        except Exception as exc:
            response = getattr(exc, "response", {})
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (404,):
                raise
            with source.open("rb") as stream:
                self.client.upload_fileobj(stream, self.bucket, self._key(digest))
        return "sha256:" + digest, digest

    @observed_sync("object.get")
    def get(self, reference: str, *, maximum: int) -> bytes:
        digest = self._digest(reference)
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

    def size(self, reference: str, *, maximum: int) -> int:
        digest = self._digest(reference)
        size = int(self.client.head_object(Bucket=self.bucket,
            Key=self._key(digest)).get("ContentLength", 0))
        if size > maximum:
            raise ValueError("Object exceeds byte limit")
        return size

    def iter_bytes(self, reference: str, *, maximum: int, start: int = 0,
                   end: int | None = None, chunk_size: int = 1024 * 1024):
        digest = self._digest(reference); size = self.size(reference, maximum=maximum)
        end = size - 1 if end is None else end
        if start < 0 or end < start or end >= size:
            raise ValueError("Invalid object byte range")
        full = start == 0 and end == size - 1; hasher = hashlib.sha256() if full else None
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(digest),
            Range=f"bytes={start}-{end}")
        body = response["Body"]; remaining = end - start + 1
        try:
            while remaining:
                data = body.read(min(chunk_size, remaining))
                if not data:
                    raise ValueError("Object ended before the declared range")
                remaining -= len(data)
                if hasher: hasher.update(data)
                yield data
        finally:
            body.close()
        if hasher and hasher.hexdigest() != digest:
            raise ValueError("Object integrity check failed")

    def media_object_key(self, kind: str, digest: str, mime: str) -> str:
        if kind not in {"image", "video", "audio"} or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Invalid media object identity")
        extension = MEDIA_EXTENSIONS.get(mime)
        if not extension or not mime.startswith(kind + "/"):
            raise ValueError("Unsupported media reference MIME")
        return f"{self.media_prefix}/{kind}/{digest[:2]}/{digest}.{extension}"

    def media_public_url(self, object_key: str) -> str:
        if not object_key.startswith(self.media_prefix + "/") or ".." in object_key.split("/"):
            raise ValueError("Invalid media object key")
        return f"{self.public_endpoint}/{quote(self.bucket, safe='')}/{quote(object_key, safe='/')}"

    def prepare_media_upload(self, *, kind: str, digest: str, mime: str, size: int,
                             expires_seconds: int = 900) -> dict[str, Any]:
        if size <= 0 or size > 200 * 1024 * 1024 or not 60 <= expires_seconds <= 3600:
            raise ValueError("Invalid media upload size or expiration")
        object_key = self.media_object_key(kind, digest, mime)
        metadata = {"sha256": digest, "size": str(size), "kind": kind}
        parameters = {"Bucket": self.bucket, "Key": object_key, "ContentType": mime,
            "Metadata": metadata, "IfNoneMatch": "*"}
        put_url = self.public_client.generate_presigned_url("put_object", Params=parameters,
            ExpiresIn=expires_seconds)
        return {"object_key": object_key, "public_url": self.media_public_url(object_key),
            "put_url": put_url, "headers": {"Content-Type": mime, "x-amz-meta-sha256": digest,
                "x-amz-meta-size": str(size), "x-amz-meta-kind": kind, "If-None-Match": "*"}}

    def head_media_upload(self, object_key: str) -> dict[str, Any]:
        if not object_key.startswith(self.media_prefix + "/") or ".." in object_key.split("/"):
            raise ValueError("Invalid media object key")
        value = self.client.head_object(Bucket=self.bucket, Key=object_key)
        return {"size": int(value.get("ContentLength", 0)),
            "mime": str(value.get("ContentType", "")).split(";", 1)[0].lower(),
            "metadata": {str(key).lower(): str(item) for key, item in (value.get("Metadata") or {}).items()}}

    def verify_media_upload(self, object_key: str, *, digest: str, expected_size: int) -> None:
        if (not object_key.startswith(self.media_prefix + "/") or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or expected_size <= 0 or expected_size > 200 * 1024 * 1024):
            raise ValueError("Invalid media upload verification")
        response = self.client.get_object(Bucket=self.bucket, Key=object_key)
        body = response["Body"]; hasher = hashlib.sha256(); size = 0
        try:
            while chunk := body.read(1024 * 1024):
                size += len(chunk)
                if size > expected_size:
                    raise ValueError("Media upload size mismatch")
                hasher.update(chunk)
        finally:
            body.close()
        if size != expected_size or hasher.hexdigest() != digest:
            raise ValueError("Media upload digest mismatch")

    def promote_media_upload(self, object_key: str, digest: str) -> str:
        if not object_key.startswith(self.media_prefix + "/") or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Invalid media object promotion")
        target = self._key(digest)
        try:
            self.client.head_object(Bucket=self.bucket, Key=target)
        except Exception as exc:
            response = getattr(exc, "response", {})
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (404,):
                raise
            self.client.copy_object(Bucket=self.bucket, Key=target,
                CopySource={"Bucket": self.bucket, "Key": object_key}, MetadataDirective="COPY")
        return "sha256:" + digest


def object_store(settings):
    if settings.object_backend == "s3":
        return S3ObjectStore(
            bucket=settings.object_bucket,
            prefix=settings.object_prefix,
            endpoint=settings.object_endpoint,
            region=settings.object_region,
            public_endpoint=settings.object_public_endpoint,
            media_prefix=settings.media_reference_prefix,
        )
    return ObjectStore(settings.data_root / "objects")
