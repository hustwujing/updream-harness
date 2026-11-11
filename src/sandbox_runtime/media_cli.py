from __future__ import annotations

import argparse
import json
import os
import uuid

import httpx


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sandbox-media")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="Emit a canonical confirmation payload")
    prepare.add_argument("kind", choices=("image", "video", "audio"))
    prepare.add_argument("--model", required=True)
    prepare.add_argument("--parameters", default="{}")
    prepare.add_argument("--idempotency-key", default="")
    catalog = sub.add_parser("catalog")
    catalog.add_argument("--kind", choices=("image", "video", "audio"), default="")
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
    base = os.environ.get("HARNESS_GATEWAY_URL", "").rstrip("/")
    token_file = os.environ.get("HARNESS_CAPABILITY_TOKEN_FILE", "/run/harness/capability-token")
    if not base:
        print(json.dumps({"error": "HARNESS_GATEWAY_URL is not configured"}), ensure_ascii=False)
        return 2
    token = open(token_file, encoding="utf-8").read().strip()
    if args.command == "catalog":
        response = httpx.get(base + "/v1/media/catalog", headers={"Authorization": "Bearer " + token},
            params={"kind": args.kind} if args.kind else None, timeout=20)
        print(response.text)
        return 0 if response.is_success else 1
    if args.command == "prepare":
        response = httpx.post(base + "/v1/media/drafts", headers={"Authorization": "Bearer " + token},
            json={"kind": args.kind, "model": args.model, "parameters": parameters,
                  "idempotency_key": args.idempotency_key or "sandbox-media-" + uuid.uuid4().hex}, timeout=20)
        print(response.text)
        return 0 if response.is_success else 1
    path = f"/v1/media/jobs/{args.job_id}" + ("/cancel" if args.command == "cancel" else "")
    request = httpx.get if args.command == "status" else httpx.post
    response = request(base + path, headers={"Authorization": "Bearer " + token}, timeout=20)
    print(response.text)
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
