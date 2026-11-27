"""Build, select and resolve immutable independently deployable service releases."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone


HOST_SERVICES = frozenset({
    "api", "platform-worker", "sandbox-gateway", "job-worker", "allocation-worker",
    "controlled-worker", "media-worker", "site-worker",
})


def _load_tool(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("service_release_" + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


module_builder = _load_tool("build_module")


def _encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe(root, value):
    root, value = root.resolve(), Path(value)
    if value.is_symlink():
        raise ValueError("release_symlink_forbidden")
    resolved = value.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("release_path_escape")
    return resolved


def _tree_rows(root, *, omit=frozenset()):
    rows = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if (relative in omit or any(part == "__pycache__" for part in path.parts)
                or path.name.startswith("_editable_impl_sandbox_harness_platform_parallel")
                or any(part.startswith("sandbox_harness_platform_parallel-")
                       and part.endswith(".dist-info") for part in path.parts)):
            continue
        if path.is_symlink():
            target = os.readlink(path)
            rows.append({"path":relative,"kind":"symlink","target":target})
        elif path.is_file() and path.suffix != ".pyc":
            rows.append({"path":relative,"kind":"file","size":path.stat().st_size,
                         "sha256":_digest_file(path),"mode":path.stat().st_mode & 0o777})
    return rows


def _verify_rows(root, rows):
    for row in rows:
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("release_path_escape")
        path = root / relative
        if row["kind"] == "symlink":
            if not path.is_symlink() or os.readlink(path) != row["target"]:
                raise ValueError("release_file_digest_mismatch")
        elif (not path.is_file() or path.stat().st_size != row["size"]
              or _digest_file(path) != row["sha256"]
              or path.stat().st_mode & 0o777 != row["mode"]):
            raise ValueError("release_file_digest_mismatch")
    expected={row["path"] for row in rows}
    actual={path.relative_to(root).as_posix() for path in root.rglob("*")
            if ((path.is_file() or path.is_symlink()) and path.name != "manifest.json"
                and path.suffix != ".pyc" and "__pycache__" not in path.parts)}
    if actual != expected:
        raise ValueError("release_file_set_mismatch")


def _manifest_identity(value):
    payload = {key:item for key,item in value.items() if key != "digest"}
    return hashlib.sha256(_encode(payload)).hexdigest()


def _publish_release_tree(root):
    for path in sorted((value for value in root.rglob("*") if value.is_dir()),reverse=True):
        path.chmod(0o555)
    root.chmod(0o555)


def freeze_environment(source: Path, output_root: Path) -> Path:
    source = source.resolve()
    if not (source / "bin/python").exists() or not (source / "pyvenv.cfg").is_file():
        raise ValueError("dependency_environment_invalid")
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    output_root.chmod(0o755)
    rows = _tree_rows(source)
    for row in rows:
        if row["kind"] == "file":
            row["mode"] = 0o755 if row["mode"] & 0o111 else 0o644
    payload = {"schema_version":1,"kind":"python-environment","files":rows}
    digest = hashlib.sha256(_encode(payload)).hexdigest()
    payload["digest"] = digest
    target = output_root / digest
    manifest = target / "manifest.json"
    if target.exists():
        verify_environment(manifest)
        target.chmod(0o555);manifest.chmod(0o444)
        return manifest
    staging = Path(tempfile.mkdtemp(prefix=".environment-", dir=output_root))
    try:
        shutil.rmtree(staging)
        subprocess.run(["cp", "--reflink=auto", "-a", str(source), str(staging)], check=True)
        for path in list(staging.rglob("__pycache__")):
            shutil.rmtree(path)
        for path in list(staging.rglob("*.pyc")):
            path.unlink()
        for path in list(staging.rglob("_editable_impl_sandbox_harness_platform_parallel*.pth")):
            path.unlink()
        for path in list(staging.rglob("sandbox_harness_platform_parallel-*.dist-info")):
            shutil.rmtree(path)
        for path in staging.rglob("*"):
            if path.is_dir():
                path.chmod(0o755)
        for row in rows:
            if row["kind"] == "file":
                (staging / row["path"]).chmod(row["mode"])
        (staging / "manifest.json").write_bytes(_encode(payload))
        (staging / "manifest.json").chmod(0o444);staging.chmod(0o555)
        staging.rename(target)
        verify_environment(manifest)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def verify_environment(manifest: Path):
    manifest = manifest.resolve()
    value = json.loads(manifest.read_text())
    if (value.get("schema_version") != 1 or value.get("kind") != "python-environment"
            or value.get("digest") != _manifest_identity(value)
            or manifest.parent.name != value.get("digest")):
        raise ValueError("dependency_environment_manifest_invalid")
    _verify_rows(manifest.parent, value["files"])
    return value


def _native_receipts(module):
    result = []
    for item in module["native_requirements"]:
        path = item.get("path")
        if not path:
            raise ValueError("native_requirement_path_missing")
        source = Path(path)
        if not source.is_absolute() or not source.exists() or not os.access(source, os.X_OK):
            raise ValueError("native_requirement_unavailable")
        resolved = source.resolve()
        result.append({"name":item["name"],"path":str(source),"resolved":str(resolved),
                       "sha256":_digest_file(resolved)})
    return result


def build_service_release(root: Path, service: str, output_root: Path,
                          environment_manifest: Path, external_receipts=None) -> Path:
    root, output_root = root.resolve(), output_root.resolve()
    if service not in HOST_SERVICES:
        raise ValueError("service_release_unknown")
    environment = verify_environment(environment_manifest)
    manifests = module_builder.manifest_tools.load_manifests(root)
    module = manifests[service]
    if module["kind"] != "service" or module["location"] != "host":
        raise ValueError("service_release_module_invalid")
    supplied = external_receipts or {}
    external = []
    for item in module["external_inputs"]:
        receipt = supplied.get(item["id"])
        if not receipt or receipt.get("identity") != item["identity"] or not receipt.get("digest"):
            raise ValueError("external_input_receipt_missing")
        external.append({"id":item["id"],"identity":receipt["identity"],
                         "digest":receipt["digest"]})
    source_manifest = module_builder.build_module(root, service, output_root / ".source-artifacts")
    source = module_builder.verify_artifact(source_manifest)
    native = _native_receipts(module)
    source_relative = "source/" + source["artifact_digest"]
    payload = {"schema_version":1,"service":service,"module_id":service,
        "source_digest":source["artifact_digest"],"environment_digest":environment["digest"],
        "source_root":source_relative,
        "environment_manifest":str(environment_manifest.resolve()),
        "entry_points":module["entry_points"],"native":native,"external_inputs":external,
        "compatibility":module["compatibility"]}
    digest = hashlib.sha256(_encode(payload)).hexdigest()
    payload["digest"] = digest
    parent = output_root / service
    parent.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True,exist_ok=True);output_root.chmod(0o755);parent.chmod(0o755)
    target, manifest = parent / digest, parent / digest / "manifest.json"
    if target.exists():
        verify_service_release(manifest, output_root=output_root)
        source_root=target/json.loads(manifest.read_text())["source_root"]
        (source_root/"resources").mkdir(exist_ok=True)
        _publish_release_tree(target);manifest.chmod(0o444)
        return manifest
    staging = Path(tempfile.mkdtemp(prefix=".release-", dir=parent))
    try:
        source_target = staging / source_relative
        source_target.parent.mkdir(parents=True)
        shutil.copytree(source_manifest.parent, source_target)
        (source_target/"resources").mkdir(exist_ok=True)
        payload.update({"source_root":str(source_target.relative_to(staging)),
                        "environment_manifest":str(environment_manifest.resolve())})
        payload["digest"] = hashlib.sha256(_encode(
            {key:item for key,item in payload.items() if key != "digest"})).hexdigest()
        if payload["digest"] != digest:
            digest = payload["digest"]
            target, manifest = parent / digest, parent / digest / "manifest.json"
        (staging / "manifest.json").write_bytes(_encode(payload))
        (staging / "manifest.json").chmod(0o444);_publish_release_tree(staging)
        staging.rename(target)
        verify_service_release(manifest, output_root=output_root)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def verify_service_release(manifest: Path, *, output_root: Path,
                           require_compatibility: bool = True):
    output_root = output_root.resolve()
    manifest = _safe(output_root, manifest)
    value = json.loads(manifest.read_text())
    if (value.get("schema_version") != 1 or value.get("service") not in HOST_SERVICES
            or value.get("service") != value.get("module_id")
            or value.get("digest") != _manifest_identity(value)
            or manifest.parent.name != value.get("digest")
            or manifest.parent.parent.name != value.get("service")):
        raise ValueError("service_release_manifest_invalid")
    compatibility=value.get("compatibility")
    if (require_compatibility and (not isinstance(compatibility,dict)
            or set(compatibility)!={"wire_protocol","schema","baseline"}
            or any(not isinstance(item,str) or not item for item in compatibility.values()))):
        raise ValueError("service_release_compatibility_invalid")
    source = _safe(manifest.parent, manifest.parent / value["source_root"])
    source_manifest = source / "manifest.json"
    if module_builder.verify_artifact(source_manifest)["artifact_digest"] != value["source_digest"]:
        raise ValueError("service_release_source_mismatch")
    environment_manifest = Path(value["environment_manifest"])
    if verify_environment(environment_manifest)["digest"] != value["environment_digest"]:
        raise ValueError("service_release_environment_mismatch")
    return value


def select_service_release(data_root: Path, manifest: Path):
    data_root = data_root.resolve()
    releases = data_root / "service-releases"
    value = verify_service_release(manifest, output_root=releases)
    current = data_root / "service-current"
    current.mkdir(parents=True, exist_ok=True)
    current.chmod(0o755)
    descriptor = {"schema_version":1,"service":value["service"],"digest":value["digest"],
                  "manifest":str(Path(manifest).resolve()),
                  "activated_at":datetime.now(timezone.utc).isoformat()}
    target = current / (value["service"] + ".json")
    rollback=data_root/"service-rollback"/(value["service"]+".json")
    if target.exists():
        previous=json.loads(target.read_text())
        verify_service_release(Path(previous["manifest"]),output_root=releases,
            require_compatibility=False)
        rollback.parent.mkdir(parents=True,exist_ok=True);rollback.parent.chmod(0o755)
        rollback_stage=rollback.with_suffix(".json.tmp")
        rollback_stage.write_bytes(_encode(previous));rollback_stage.chmod(0o444)
        os.replace(rollback_stage,rollback)
    staging = target.with_suffix(".json.tmp")
    staging.write_bytes(_encode(descriptor));staging.chmod(0o444);os.replace(staging, target)
    return target


def resolve_service(data_root: Path, service: str):
    if service not in HOST_SERVICES:
        raise ValueError("service_release_unknown")
    data_root = data_root.resolve()
    descriptor_path = _safe(data_root / "service-current", data_root / "service-current" / (service + ".json"))
    descriptor = json.loads(descriptor_path.read_text())
    if (descriptor.get("schema_version") != 1 or descriptor.get("service") != service
            or any(key not in {"schema_version","service","digest","manifest","activated_at"}
                   for key in descriptor)):
        raise ValueError("service_descriptor_invalid")
    value = verify_service_release(Path(descriptor["manifest"]),
                                   output_root=data_root / "service-releases")
    if value["digest"] != descriptor["digest"] or value["service"] != service:
        raise ValueError("service_descriptor_mismatch")
    release = Path(descriptor["manifest"]).resolve().parent
    source = release / value["source_root"]
    environment = Path(value["environment_manifest"]).resolve().parent
    return {"python":str(environment / "bin/python"),"pythonpath":str(source / "src"),
            "resource_root":str(source / "resources"),"digest":value["digest"]}


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    freeze = sub.add_parser("freeze-environment")
    freeze.add_argument("--source", type=Path, required=True);freeze.add_argument("--output", type=Path, required=True)
    build = sub.add_parser("build")
    build.add_argument("--root", type=Path, required=True);build.add_argument("--service", required=True)
    build.add_argument("--output", type=Path, required=True);build.add_argument("--environment", type=Path, required=True)
    select = sub.add_parser("select")
    select.add_argument("--data-root", type=Path, required=True);select.add_argument("--manifest", type=Path, required=True)
    resolve = sub.add_parser("resolve")
    resolve.add_argument("--data-root", type=Path, required=True);resolve.add_argument("--service", required=True)
    args = parser.parse_args()
    if args.action == "freeze-environment":
        print(freeze_environment(args.source,args.output))
    elif args.action == "build":
        print(build_service_release(args.root,args.service,args.output,args.environment))
    elif args.action == "select":
        print(select_service_release(args.data_root,args.manifest))
    else:
        value=resolve_service(args.data_root,args.service)
        print(value["python"]);print(value["pythonpath"]);print(value["resource_root"]);print(value["digest"])


if __name__ == "__main__":
    main()
