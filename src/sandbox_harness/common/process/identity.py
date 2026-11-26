"""PID start-time fencing without importing a Runtime HTTP client."""
from __future__ import annotations

from pathlib import Path

def _process_start(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/stat").read_text().split()[21]
    except (OSError, IndexError):
        return ""

