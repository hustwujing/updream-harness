"""Validate module ownership before building any deployable artifact."""
from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import re


KINDS = {"service", "runtime", "bootstrap", "content", "images", "controlled", "web"}
LOCATIONS = {"host", "sandbox", "controlled", "browser", "registry"}
IGNORED = {"__pycache__", ".pytest_cache", "node_modules", ".git"}
FIELDS = {
    "schema_version", "id", "kind", "location", "sources", "source_roots",
    "entry_points", "common_modules", "python_dependencies", "native_requirements",
    "external_inputs", "resources", "host_credentials_allowed", "verification",
    "compatibility", "migration_stage",
}


def _source_path(value: str) -> str:
    if (not isinstance(value, str) or not value or "\\" in value
            or ":" in value or "\x00" in value):
        raise ValueError("source_path_invalid")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("source_path_invalid")
    return value


def validate_manifest(value: dict) -> None:
    if not isinstance(value, dict) or set(value) != FIELDS or value.get("schema_version") != 1:
        raise ValueError("module_manifest_schema_invalid")
    if not re.fullmatch(r"[a-z][a-z0-9-]{1,63}", value.get("id", "")):
        raise ValueError("module_identity_invalid")
    if value["kind"] not in KINDS or value["location"] not in LOCATIONS:
        raise ValueError("module_location_invalid")
    for key in ("sources", "source_roots", "resources"):
        if not isinstance(value[key], list):
            raise ValueError("module_source_list_invalid")
        for path in value[key]:
            _source_path(path)
    for key in ("common_modules", "python_dependencies", "native_requirements", "external_inputs", "verification"):
        if not isinstance(value[key], list):
            raise ValueError("module_list_invalid")
    if not isinstance(value["entry_points"], dict) or not isinstance(value["compatibility"], dict):
        raise ValueError("module_contract_invalid")
    if value["migration_stage"] not in {"inventory", "isolated"}:
        raise ValueError("module_migration_stage_invalid")
    if not isinstance(value["host_credentials_allowed"], bool):
        raise ValueError("module_credentials_policy_invalid")
    if value["location"] in {"sandbox", "controlled", "browser"} and value["host_credentials_allowed"]:
        raise ValueError("module_credentials_policy_invalid")
    for row in value["native_requirements"]:
        if not isinstance(row, dict) or not row.get("name") or not row.get("requirement"):
            raise ValueError("native_requirement_invalid")
    for row in value["external_inputs"]:
        if (not isinstance(row, dict) or not row.get("id") or not row.get("source")
                or row.get("identity") not in {"sha256", "manifest-sha256"}):
            raise ValueError("external_input_identity_required")


def load_manifests(root: Path) -> dict[str, dict]:
    result = {}
    for path in sorted((root / "modules").rglob("module.json")):
        if path.is_symlink():
            raise ValueError("manifest_symlink_forbidden")
        value = json.loads(path.read_text())
        validate_manifest(value)
        if value["id"] in result:
            raise ValueError("duplicate_module_identity")
        result[value["id"]] = value
    return result


def _contained(root: Path, relative: str) -> Path:
    path = root / _source_path(relative)
    for ancestor in (path, *path.parents):
        if ancestor == root:
            break
        if ancestor.is_symlink():
            raise ValueError("source_symlink_forbidden")
    if not path.resolve().is_relative_to(root):
        raise ValueError("source_outside_root")
    return path


def module_sources(root: Path, manifest: dict) -> list[Path]:
    validate_manifest(manifest)
    root = root.resolve()
    sources = set()
    for name in manifest["sources"]:
        path = _contained(root, name)
        if not path.is_file():
            raise ValueError("module_source_missing:" + name)
        sources.add(path)
    for name in manifest["source_roots"]:
        directory = _contained(root, name)
        if not directory.is_dir():
            raise ValueError("module_source_root_missing:" + name)
        for path in directory.rglob("*"):
            if any(part in IGNORED for part in path.relative_to(directory).parts) or path.suffix in {".pyc", ".pyo"}:
                continue
            path = _contained(root, path.relative_to(root).as_posix())
            if path.is_file():
                sources.add(path)
    return sorted(sources)


def validate_source_ownership(root: Path, ownership: dict) -> None:
    declared = ownership.get("modules", {})
    if not isinstance(declared, dict):
        raise ValueError("source_ownership_invalid")
    actual = {path.relative_to(root).as_posix() for path in (root / "src").rglob("*.py")
              if not any(part in IGNORED for part in path.parts)}
    missing = actual - set(declared)
    if missing:
        raise ValueError("unowned_source:" + ",".join(sorted(missing)))
    stale = set(declared) - actual
    if stale:
        raise ValueError("stale_source_ownership:" + ",".join(sorted(stale)))
    for name, value in declared.items():
        _source_path(name)
        if not isinstance(value, dict) or not value.get("owner") or not value.get("target"):
            raise ValueError("source_owner_invalid")
        _source_path(value["target"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    manifests = load_manifests(args.root)
    ownership = json.loads((args.root / "modules/ownership.json").read_text())
    validate_source_ownership(args.root, ownership)
    print(json.dumps({"modules": len(manifests), "sources": {
        name: len(module_sources(args.root, manifest)) for name, manifest in manifests.items()
    }}, sort_keys=True))


if __name__ == "__main__":
    main()
