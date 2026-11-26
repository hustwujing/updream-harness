#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


ROOT = Path(__file__).resolve().parents[1]
SHARED_MODULES = ("common/contracts/domain.py", "common/contracts/job_contracts.py",
    "common/catalog/media_catalog.py", "common/observability/observability.py",
    "common/sites/site_bundle.py")


def add_file(archive: tarfile.TarFile, path: str, data: bytes) -> None:
    info = tarfile.TarInfo(path); info.size = len(data); info.mode = 0o444
    info.mtime = 0; info.uid = 0; info.gid = 0; info.uname = ""; info.gname = ""
    archive.addfile(info, io.BytesIO(data))


def build(output_dir: Path, signing_key: Path) -> tuple[Path, Path, str, dict]:
    files: dict[str, bytes] = {}
    for source in sorted((ROOT / "src/sandbox_runtime").rglob("*.py")):
        files["site-packages/sandbox_runtime/" + source.relative_to(ROOT / "src/sandbox_runtime").as_posix()] = source.read_bytes()
    shared = set(SHARED_MODULES)
    for name in SHARED_MODULES:
        shared.update(str(parent / "__init__.py") for parent in Path(name).parents)
    for name in sorted(shared):
        source = ROOT / "src/sandbox_harness" / name
        files["site-packages/sandbox_harness/" + name] = source.read_bytes()
    editppt = ROOT / "runtime/skills/image-to-editable-ppt/cli/editppt"
    for source in sorted(editppt.rglob("*.py")):
        files["site-packages/editppt/" + source.relative_to(editppt).as_posix()] = source.read_bytes()
    public_key = subprocess.run(["openssl", "pkey", "-in", str(signing_key), "-pubout"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout
    dependency_lock = hashlib.sha256((ROOT / "runtime/requirements-runtime.txt").read_bytes()).hexdigest()
    manifest = {"format_version": 1, "bootstrap_protocol": 1,
        "python_abi": "cp313", "dependency_lock_digest": dependency_lock,
        "signing_key_id": hashlib.sha256(public_key).hexdigest()[:16],
        "entry_module": "sandbox_runtime.main",
        "files": [{"path": path, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for path, data in sorted(files.items())]}
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w", format=tarfile.PAX_FORMAT) as archive:
        add_file(archive, "manifest.json", json.dumps(manifest, sort_keys=True,
            separators=(",", ":")).encode() + b"\n")
        for path, data in sorted(files.items()): add_file(archive, path, data)
    data = payload.getvalue(); digest = hashlib.sha256(data).hexdigest()
    output_dir.mkdir(parents=True, exist_ok=True); output_dir.chmod(0o755)
    target = output_dir / f"{digest}.tar"
    if not target.exists():
        temporary = output_dir / f".{digest}.tmp"
        temporary.write_bytes(data); temporary.chmod(0o444); temporary.replace(target)
    signature = subprocess.run(["openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(signing_key),
        "-in", str(target)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout
    signature_path = output_dir / f"{digest}.sig"
    if not signature_path.exists():
        temporary = output_dir / f".{digest}.sig.tmp"
        temporary.write_bytes(signature); temporary.chmod(0o444); temporary.replace(signature_path)
    elif signature_path.read_bytes() != signature:
        raise RuntimeError("Existing Runtime Code Bundle signature does not match signing key")
    return target, signature_path, digest, manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=ROOT / ".data/runtime-code-bundles")
    parser.add_argument("--signing-key", type=Path,
        default=ROOT / ".data/config/runtime-bundle-signing.pem")
    args = parser.parse_args()
    path, signature, digest, manifest = build(args.output_dir, args.signing_key)
    print(json.dumps({"path": str(path), "signature_path": str(signature), "sha256": digest,
        "files": len(manifest["files"]), "dependency_lock_digest": manifest["dependency_lock_digest"]},
        sort_keys=True))


if __name__ == "__main__": main()
