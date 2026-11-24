from __future__ import annotations

from datetime import datetime, timezone
import json
import sys


IDENTITY_FIELDS = ("sandbox_id", "thread_id", "turn_id", "job_id", "execution_id")


def bootstrap_event(event: str, *, service: str, level: str = "INFO", stream=None) -> None:
    target = stream or sys.stdout
    row = {
        "schema_version": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        "service": service,
        **{field: "" for field in IDENTITY_FIELDS},
    }
    target.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    target.flush()


def configure_bootstrap_logging(service: str, *, stream=None) -> None:
    bootstrap_event("service.started", service=service, stream=stream)
