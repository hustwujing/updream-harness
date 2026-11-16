from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import httpx

from sandbox_harness.job_contracts import JobGroupCreate


def _json_value(raw: str):
    stripped = raw.lstrip()
    if stripped.startswith(("{", "[")):
        source = raw
    else:
        path = Path(raw)
        source = path.read_text() if path.is_file() else raw
    return json.loads(source)


def _value(raw: str) -> dict:
    value = _json_value(raw)
    if not isinstance(value, dict):
        raise ValueError("JobGroup payload must be a JSON object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sandbox-job")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="Validate and submit one complete parallel JobGroup")
    prepare.add_argument("--request", required=True, help="JSON object or path to a JSON file")
    edit = sub.add_parser("edit", help="Replace the editable members of one JobGroupDraft")
    edit.add_argument("--draft-id", required=True)
    edit.add_argument("--expected-digest", required=True)
    edit.add_argument("--jobs", required=True, help="JSON array or path to a JSON file")
    for name in ("draft",):
        command = sub.add_parser(name)
        command.add_argument("--draft-id", required=True)
    group = sub.add_parser("group"); group.add_argument("--group-id", required=True)
    job = sub.add_parser("job"); job.add_argument("--job-id", required=True)
    cancel_job = sub.add_parser("cancel-job"); cancel_job.add_argument("--job-id", required=True)
    cancel_group = sub.add_parser("cancel-group"); cancel_group.add_argument("--group-id", required=True)
    args = parser.parse_args(argv)
    base = os.environ.get("HARNESS_GATEWAY_URL", "").rstrip("/")
    token_file = Path(os.environ.get("HARNESS_CAPABILITY_TOKEN_FILE", "/run/harness/capability-token"))
    if not base or not token_file.is_file():
        print(json.dumps({"error": "Harness Gateway capability is not configured"}))
        return 2
    headers = {"Authorization": "Bearer " + token_file.read_text().strip()}
    try:
        if args.command == "prepare":
            payload = _value(args.request)
            JobGroupCreate.model_validate(payload)
            response = httpx.post(base + "/v1/job-group-drafts", headers=headers, json=payload, timeout=30)
        elif args.command == "edit":
            jobs_value = _json_value(args.jobs)
            jobs = (jobs_value.get("jobs")
                if isinstance(jobs_value, dict) and set(jobs_value) == {"jobs"} else jobs_value)
            if not isinstance(jobs, list):
                raise ValueError("--jobs must contain a JSON array or {\"jobs\": [...]} object")
            response = httpx.patch(base + f"/v1/job-group-drafts/{args.draft_id}", headers=headers,
                json={"expected_digest": args.expected_digest, "jobs": jobs}, timeout=30)
        elif args.command == "draft":
            response = httpx.get(base + f"/v1/job-group-drafts/{args.draft_id}", headers=headers, timeout=20)
        elif args.command in {"group", "cancel-group"}:
            path = f"/v1/job-groups/{args.group_id}" + ("/cancel" if args.command == "cancel-group" else "")
            response = (httpx.post if args.command == "cancel-group" else httpx.get)(
                base + path, headers=headers, timeout=20)
        else:
            path = f"/v1/jobs/{args.job_id}" + ("/cancel" if args.command == "cancel-job" else "")
            response = (httpx.post if args.command == "cancel-job" else httpx.get)(
                base + path, headers=headers, timeout=20)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    print(response.text)
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
