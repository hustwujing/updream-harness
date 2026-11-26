"""Build deterministic source/resource artifacts from an explicit module closure.

Dependency environments and external runtime receipts are separate launch gates;
an artifact created here alone is deliberately marked source-only.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile


def _tool(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("module_build_" + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


manifest_tools = _tool("module_manifest")
boundary_tools = _tool("module_boundaries")


def _encode(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _module_file(root: Path, name: str) -> Path | None:
    path = root / "src" / name.replace(".", "/")
    for candidate in (path.with_suffix(".py"), path / "__init__.py"):
        if candidate.is_file():
            return manifest_tools._contained(root, candidate.relative_to(root).as_posix())
    return None


def source_closure(root: Path, manifest: dict) -> list[Path]:
    root = root.resolve()
    seeds = manifest_tools.module_sources(root, manifest)
    result = set()
    pending = list(seeds)
    service = manifest["id"].replace("-", "_")
    while pending:
        path = pending.pop()
        if path in result:
            continue
        result.add(path)
        if path.suffix != ".py" or not path.is_relative_to(root / "src"):
            continue
        name = boundary_tools.module_name(root, path)
        if name.startswith("sandbox_harness.services."):
            owner = name.split(".")[2]
            if owner != service:
                raise ValueError("cross_service_import:" + name)
        for parent in path.parents:
            if parent == root / "src":
                break
            initializer = parent / "__init__.py"
            if initializer.is_file():
                pending.append(manifest_tools._contained(root, initializer.relative_to(root).as_posix()))
        for dependency in boundary_tools.imports(root, path):
            source = _module_file(root, dependency)
            if source:
                pending.append(source)
            elif dependency.startswith(("sandbox_harness", "sandbox_runtime", "sandbox_bootstrap", "controlled_runtime")):
                # A namespace package with no Python source is permitted, never an
                # absent concrete implementation silently resolved from the host.
                package = root / "src" / dependency.replace(".", "/")
                if not package.is_dir():
                    raise ValueError("module_dependency_missing:" + dependency)
    selected = {boundary_tools.module_name(root, p) for p in result
                if p.suffix == ".py" and p.is_relative_to(root / "src")}
    for finding in boundary_tools.violations(root):
        if finding["source"] in selected:
            raise ValueError(finding["rule"] + ":" + finding["source"])
    return sorted(result)


def _resource_files(root: Path, name: str) -> list[Path]:
    path = manifest_tools._contained(root, name)
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise ValueError("module_resource_missing:" + name)
    result = []
    for child in sorted(path.rglob("*")):
        child = manifest_tools._contained(root, child.relative_to(root).as_posix())
        if child.is_file():
            result.append(child)
    return result


def verify_artifact(manifest: Path) -> dict:
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError("artifact_manifest_invalid")
    value = json.loads(manifest.read_text())
    identity = value.get("artifact_digest", "")
    payload = {key: item for key, item in value.items() if key != "artifact_digest"}
    if hashlib.sha256(_encode(payload)).hexdigest() != identity or manifest.parent.name != identity:
        raise ValueError("artifact_manifest_digest_mismatch")
    if value.get("schema_version") != 1 or value.get("state") != "source-only":
        raise ValueError("artifact_manifest_invalid")
    root = manifest.parent.resolve()
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ValueError("source_symlink_forbidden")
    seen = set()
    for row in value["files"]:
        if row["path"] in seen:
            raise ValueError("artifact_duplicate_path")
        seen.add(row["path"])
        path = manifest_tools._contained(root, row["path"])
        if not path.is_file():
            raise ValueError("artifact_file_missing")
        data = path.read_bytes()
        if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
            raise ValueError("artifact_file_digest_mismatch")
        if path.stat().st_mode & 0o777 != row["mode"]:
            raise ValueError("artifact_file_mode_mismatch")
    actual = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual != seen | {"manifest.json"}:
        raise ValueError("artifact_unlisted_file")
    return value


def build_module(root: Path, module_id: str, output_root: Path) -> Path:
    root = root.resolve()
    manifests = manifest_tools.load_manifests(root)
    if module_id not in manifests:
        raise ValueError("module_unknown")
    module = manifests[module_id]
    payloads = {}
    for path in source_closure(root, module):
        payloads[path.relative_to(root).as_posix()] = (path.read_bytes(), 0o555 if path.stat().st_mode & 0o111 else 0o444)
    for name in module["resources"]:
        for path in _resource_files(root, name):
            relative = "resources/" + path.relative_to(root).as_posix()
            payloads[relative] = (path.read_bytes(), 0o555 if path.stat().st_mode & 0o111 else 0o444)
    rows = [{"path": name, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data), "mode": mode}
            for name, (data, mode) in sorted(payloads.items())]
    value = {"schema_version": 1, "module": module, "files": rows, "state": "source-only"}
    digest = hashlib.sha256(_encode(value)).hexdigest()
    value["artifact_digest"] = digest
    parent = output_root.resolve() / module_id
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / digest
    manifest = target / "manifest.json"
    if target.exists():
        verify_artifact(manifest)
        return manifest
    staging = Path(tempfile.mkdtemp(prefix=".building-", dir=parent))
    try:
        for name, (data, mode) in payloads.items():
            path = staging / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(mode)
        (staging / "manifest.json").write_bytes(_encode(value))
        (staging / "manifest.json").chmod(0o444)
        for directory in sorted((p for p in staging.rglob("*") if p.is_dir()), reverse=True):
            directory.chmod(0o555)
        staging.chmod(0o555)
        try:
            staging.rename(target)
        except OSError:
            if not target.exists():
                raise
            verify_artifact(manifest)
        verify_artifact(manifest)
        return manifest
    finally:
        if staging.exists():
            staging.chmod(0o755)
            for directory in staging.rglob("*"):
                if directory.is_dir():
                    directory.chmod(0o755)
            shutil.rmtree(staging)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--module", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = build_module(args.root, args.module, args.output)
    print(json.dumps({"manifest": str(manifest), "state": "source-only"}))


if __name__ == "__main__":
    main()
