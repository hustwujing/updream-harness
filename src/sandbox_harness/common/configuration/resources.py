"""Resolve declared release resources without depending on process cwd."""
from __future__ import annotations

import os
from pathlib import Path, PurePosixPath


def resource_root() -> Path:
    configured = os.environ.get("SANDBOX_HARNESS_RESOURCE_ROOT", "")
    if configured:
        path = Path(configured)
        if not path.is_absolute() or not path.is_dir():
            raise ValueError("release_resource_root_invalid")
        return path.resolve()
    source = Path(__file__).resolve()
    root = source.parents[4]
    if (source.relative_to(root).as_posix() == "src/sandbox_harness/common/configuration/resources.py"
            and (root / "pyproject.toml").is_file()):
        return root
    raise ValueError("release_resource_root_required")


def resource_path(relative: str) -> Path:
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts or "\\" in relative:
        raise ValueError("release_resource_path_invalid")
    root = resource_root()
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("release_resource_path_invalid")
    return resolved


def controlled_program_path(name: str) -> Path:
    if name not in {"sam3_job.py", "transcribe_job.py"}:
        raise ValueError("controlled_program_unknown")
    configured = os.environ.get("SANDBOX_HARNESS_CONTROLLED_PROGRAM_ROOT", "")
    root = Path(configured) if configured else resource_path("src/controlled_runtime")
    if not root.is_absolute() or not root.is_dir():
        raise ValueError("controlled_program_root_invalid")
    path = root / name
    if path.is_symlink() or not path.is_file():
        raise ValueError("controlled_program_unavailable")
    return path
