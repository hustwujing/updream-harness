from __future__ import annotations

import json
import os
import shutil
from pathlib import Path


def bundle_root() -> Path:
    runtime_bundle = os.environ.get("SANDBOX_RUNTIME_BUNDLE_ROOT")
    if runtime_bundle and (Path(runtime_bundle) / "skills").is_dir():
        return Path(runtime_bundle) / "skills"
    configured = os.environ.get("SANDBOX_RUNTIME_SKILL_ROOT")
    if configured:
        return Path(configured)
    installed = Path("/opt/platform/runtime/skills")
    if installed.is_dir():
        return installed
    return Path(__file__).resolve().parents[2] / "runtime" / "skills"


def destination_root(harness: str, workspace: Path) -> Path:
    roots = {
        "codex": workspace / "state" / "codex" / "skills",
        "claude": workspace / "state" / "claude" / "skills",
        "deepseek": workspace / "state" / "dsh" / "skills",
    }
    try:
        return roots[harness]
    except KeyError as exc:
        raise ValueError("Unsupported Harness Skill root") from exc


def remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


def materialize_skills(harness: str, workspace: Path) -> dict:
    source = bundle_root()
    manifest = json.loads((source / "manifest.json").read_text())
    destination = destination_root(harness, workspace)
    destination.mkdir(parents=True, exist_ok=True)
    marker = destination / ".image-bundle.json"
    selected = [row for row in manifest["skills"] if harness in row["supported_harnesses"]]
    current = {row["id"] for row in selected}
    for skill_id in manifest.get("retired_skills", []):
        remove_path(destination / skill_id)
    for row in selected:
        target = source / row["id"]
        link = destination / row["id"]
        if link.is_symlink() and link.resolve() == target.resolve():
            continue
        remove_path(link)
        link.symlink_to(target, target_is_directory=True)
    temporary = destination / ".image-bundle.json.tmp"
    temporary.write_text(json.dumps({
        "bundle_version": manifest["bundle_version"],
        "skills": sorted(current),
    }, separators=(",", ":")) + "\n")
    temporary.replace(marker)
    return {"bundle_version": manifest["bundle_version"], "skill_count": len(selected),
        "root": str(destination)}
