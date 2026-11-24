from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import zlib

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization


MAX_BUNDLE_BYTES = 64 * 1024 * 1024
MAX_BUNDLE_FILES = 10_000
MAX_BUNDLE_FILE_BYTES = 8 * 1024 * 1024


def decode_runtime_bundle_transport(data: bytes, content_encoding: str) -> bytes:
    if not content_encoding:
        return data
    if content_encoding.lower().strip() != "gzip":
        raise ValueError("runtime_bundle_content_encoding_unsupported")
    if len(data) > MAX_BUNDLE_BYTES:
        raise ValueError("runtime_bundle_too_large")
    try:
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        decoded = decoder.decompress(data, MAX_BUNDLE_BYTES + 1)
    except zlib.error as exc:
        raise ValueError("runtime_bundle_transport_invalid") from exc
    if len(decoded) > MAX_BUNDLE_BYTES:
        raise ValueError("runtime_bundle_too_large")
    if not decoder.eof or decoder.unconsumed_tail or decoder.unused_data:
        raise ValueError("runtime_bundle_transport_invalid")
    return decoded


def verify_runtime_bundle_signature(data: bytes, signature: bytes) -> None:
    public_key_path = Path(os.environ.get("SANDBOX_RUNTIME_BUNDLE_PUBLIC_KEY",
        "/opt/bootstrap/bundle_public_key.pem"))
    try:
        key = serialization.load_pem_public_key(public_key_path.read_bytes())
        key.verify(signature, data)
    except (OSError, ValueError, TypeError, InvalidSignature) as exc:
        raise ValueError("runtime_bundle_signature_invalid") from exc


def _allowed(path: str) -> bool:
    value = PurePosixPath(path)
    return (path in {"manifest.json", "instructions.txt", "runtime_guidance.txt"}
        or len(value.parts) >= 2 and value.parts[0] == "skills")


def activate_runtime_bundle(data: bytes, expected_digest: str, run_root: Path) -> dict:
    if len(data) > MAX_BUNDLE_BYTES:
        raise ValueError("runtime_bundle_too_large")
    digest = hashlib.sha256(data).hexdigest()
    if digest != expected_digest or len(expected_digest) != 64:
        raise ValueError("runtime_bundle_digest_mismatch")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        members = archive.getmembers()
        if len(members) > MAX_BUNDLE_FILES:
            raise ValueError("runtime_bundle_file_limit")
        payloads: dict[str, bytes] = {}
        for member in members:
            path = PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or ".." in path.parts
                    or not _allowed(member.name) or member.size > MAX_BUNDLE_FILE_BYTES):
                raise ValueError("runtime_bundle_member_invalid")
            stream = archive.extractfile(member)
            payloads[member.name] = stream.read() if stream else b""
    try:
        manifest = json.loads(payloads.pop("manifest.json"))
        if manifest.get("format_version") != 1 or not isinstance(manifest.get("files"), list):
            raise ValueError
        declared = {row["path"]: row for row in manifest["files"]}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("runtime_bundle_manifest_invalid") from exc
    if set(declared) != set(payloads):
        raise ValueError("runtime_bundle_manifest_mismatch")
    for path, content in payloads.items():
        row = declared[path]
        if row.get("size") != len(content) or row.get("sha256") != hashlib.sha256(content).hexdigest():
            raise ValueError("runtime_bundle_file_digest_mismatch")
    if "skills/manifest.json" not in payloads or "instructions.txt" not in payloads:
        raise ValueError("runtime_bundle_required_file_missing")
    root = run_root / "runtime-bundles"; root.mkdir(parents=True, exist_ok=True)
    target = root / digest
    if not target.exists():
        temporary = root / ("." + digest + ".tmp")
        shutil.rmtree(temporary, ignore_errors=True); temporary.mkdir(parents=True, mode=0o700)
        try:
            for path, content in payloads.items():
                destination = temporary.joinpath(*PurePosixPath(path).parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content); destination.chmod(0o444)
            for directory in sorted((value for value in temporary.rglob("*") if value.is_dir()), reverse=True):
                directory.chmod(0o555)
            temporary.chmod(0o555); temporary.replace(target)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)
    current = root / "current"; replacement = root / ".current.tmp"
    replacement.unlink(missing_ok=True)
    replacement.symlink_to(target.name, target_is_directory=True); replacement.replace(current)
    os.environ["SANDBOX_RUNTIME_BUNDLE_ROOT"] = str(target)
    return {"sha256": digest, "bundle_version": str(manifest.get("bundle_version") or ""),
        "files": len(payloads), "root": str(target)}
