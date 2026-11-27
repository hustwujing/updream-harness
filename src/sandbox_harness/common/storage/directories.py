"""Fail-closed recreation for disposable writable directories."""
from __future__ import annotations

import os
from pathlib import Path
import stat


def ensure_private_directory(path: Path, *, boundary: Path | None = None) -> bool:
    target=Path(path).absolute();base=Path(boundary or target.parent).absolute()
    if base.is_symlink():raise ValueError('writable_directory_boundary_symlink')
    base_missing=not base.exists();base.mkdir(parents=True,exist_ok=True,mode=0o700)
    base_metadata=base.lstat()
    if not stat.S_ISDIR(base_metadata.st_mode) or stat.S_ISLNK(base_metadata.st_mode):
        raise ValueError('writable_directory_boundary_invalid')
    if base_missing:os.chmod(base,0o700)
    try:target.relative_to(base)
    except ValueError as exc:raise ValueError('writable_directory_outside_boundary') from exc
    current=base
    for part in target.relative_to(base).parts:
        current=current/part
        if current.is_symlink():raise ValueError('writable_directory_symlink')
    missing=not target.exists();target.mkdir(parents=True,exist_ok=True,mode=0o700)
    metadata=target.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError('writable_directory_invalid')
    if missing:os.chmod(target,0o700)
    if metadata.st_uid!=os.geteuid():
        raise PermissionError('writable_directory_owner_mismatch')
    if not os.access(target,os.W_OK|os.X_OK):
        raise PermissionError('writable_directory_not_writable')
    return missing
