#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import subprocess


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sandbox_runtime.instructions import RUNTIME_GUIDANCE, RUNTIME_INSTRUCTIONS  # noqa: E402


def add_file(archive: tarfile.TarFile, path: str, data: bytes) -> None:
    info = tarfile.TarInfo(path)
    info.size = len(data); info.mode = 0o444; info.mtime = 0; info.uid = 0; info.gid = 0
    info.uname = ""; info.gname = ""
    archive.addfile(info, io.BytesIO(data))


def build(output_dir: Path, signing_key: Path) -> tuple[Path, Path, str, dict]:
    files = {
        "instructions.txt": RUNTIME_INSTRUCTIONS.encode(),
        "runtime_guidance.txt": RUNTIME_GUIDANCE.encode(),
    }
    skills = ROOT / "runtime" / "skills"
    for source in sorted(skills.rglob("*"), key=lambda value: value.as_posix()):
        if source.is_file() and not source.is_symlink():
            files["skills/" + source.relative_to(skills).as_posix()] = source.read_bytes()
    public_key = subprocess.run(["openssl", "pkey", "-in", str(signing_key), "-pubout"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout
    manifest = {
        "format_version": 1,
        "bundle_version": json.loads(files["skills/manifest.json"])["bundle_version"],
        "signing_key_id": hashlib.sha256(public_key).hexdigest()[:16],
        "files": [{"path": path, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for path, data in sorted(files.items())],
    }
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w", format=tarfile.PAX_FORMAT) as archive:
        add_file(archive, "manifest.json", json.dumps(manifest, sort_keys=True,
            separators=(",", ":")).encode() + b"\n")
        for path, data in sorted(files.items()):
            add_file(archive, path, data)
    data = payload.getvalue(); digest = hashlib.sha256(data).hexdigest()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.chmod(0o755)
    target = output_dir / f"{digest}.tar"
    if not target.exists():
        temporary = output_dir / f".{digest}.tmp"
        temporary.write_bytes(data); temporary.chmod(0o444); temporary.replace(target)
    signature_path = output_dir / f"{digest}.sig"
    signature = subprocess.run(["openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(signing_key),
        "-in", str(target)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout
    if not signature_path.exists():
        temporary_signature = output_dir / f".{digest}.sig.tmp"
        temporary_signature.write_bytes(signature); temporary_signature.chmod(0o444)
        temporary_signature.replace(signature_path)
    elif signature_path.read_bytes() != signature:
        raise RuntimeError("Existing Runtime Bundle signature does not match signing key")
    return target, signature_path, digest, manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=ROOT / ".data" / "runtime-bundles")
    parser.add_argument("--signing-key", type=Path,
        default=ROOT / ".data" / "config" / "runtime-bundle-signing.pem")
    args = parser.parse_args()
    path, signature, digest, manifest = build(args.output_dir, args.signing_key)
    print(json.dumps({"path": str(path), "signature_path": str(signature), "sha256": digest,
        "files": len(manifest["files"]), "bundle_version": manifest["bundle_version"]}, sort_keys=True))


if __name__ == "__main__":
    main()
