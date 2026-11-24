from __future__ import annotations

import os
from pathlib import Path


def runtime_state_dir() -> Path:
    configured = os.environ.get("SANDBOX_RUNTIME_STATE_DIR", "")
    root = (Path(configured) if configured else
        Path(os.environ.get("TMPDIR", "/tmp")) / f"sandbox-harness-runtime-{os.geteuid()}")
    try:
        if root.is_symlink():
            raise RuntimeError("runtime_state_directory_unavailable")
        root.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = root.stat()
        if (not root.is_dir() or info.st_uid != os.geteuid()
                or not os.access(root, os.W_OK | os.X_OK)):
            raise RuntimeError("runtime_state_directory_unavailable")
        root.chmod(0o700)
    except OSError as exc:
        raise RuntimeError("runtime_state_directory_unavailable") from exc
    return root


def capability_token_file() -> Path:
    configured = os.environ.get("HARNESS_CAPABILITY_TOKEN_FILE", "")
    return Path(configured) if configured else runtime_state_dir() / "capability-token"
