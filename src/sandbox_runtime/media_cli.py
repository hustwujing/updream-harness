from __future__ import annotations

import argparse
import json
import os
import sys

import httpx


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sandbox-media")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="Emit a canonical confirmation payload")
    prepare.add_argument("kind", choices=("image", "video", "audio"))
    prepare.add_argument("--model", required=True)
    prepare.add_argument("--parameters", default="{}")
    status = sub.add_parser("status")
    status.add_argument("--job-id", required=True)
    cancel = sub.add_parser("cancel")
    cancel.add_argument("--job-id", required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        try:
            parameters = json.loads(args.parameters)
        except ValueError as exc:
            parser.error(f"--parameters must be JSON: {exc}")
        print(json.dumps({"action": "prepare", "kind": args.kind, "model": args.model,
                          "parameters": parameters, "requires_confirmation": True}, ensure_ascii=False))
        return 0
    base = os.environ.get("HARNESS_MEDIA_API_URL", "").rstrip("/")
    token_file = os.environ.get("HARNESS_CAPABILITY_TOKEN_FILE", "/run/harness/capability-token")
    if not base:
        print(json.dumps({"error": "HARNESS_MEDIA_API_URL is not configured"}), ensure_ascii=False)
        return 2
    token = open(token_file, encoding="utf-8").read().strip()
    path = f"/api/media/jobs/{args.job_id}" + ("/cancel" if args.command == "cancel" else "")
    response = httpx.post(base + path, headers={"Authorization": "Bearer " + token}, timeout=20)
    print(response.text)
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
