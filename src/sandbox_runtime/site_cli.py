from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import uuid

import httpx

from sandbox_harness.site_bundle import build_site_archive, normalize_site_slug


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sandbox-site")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="Validate a static site and create a deployment draft")
    prepare.add_argument("--source", required=True)
    prepare.add_argument("--name", required=True)
    prepare.add_argument("--slug", required=True)
    prepare.add_argument("--idempotency-key", default="")
    status = sub.add_parser("status", help="Read a deployment job")
    status.add_argument("--job-id", required=True)
    args = parser.parse_args(argv)
    base = os.environ.get("HARNESS_GATEWAY_URL", "").rstrip("/")
    token_file = os.environ.get("HARNESS_CAPABILITY_TOKEN_FILE", "/run/harness/capability-token")
    if not base:
        print(json.dumps({"error":"HARNESS_GATEWAY_URL is not configured"}, ensure_ascii=False))
        return 2
    token = Path(token_file).read_text(encoding="utf-8").strip()
    headers = {"Authorization":"Bearer " + token}
    if args.command == "status":
        response = httpx.get(base + f"/v1/sites/jobs/{args.job_id}", headers=headers, timeout=20)
        print(response.text)
        return 0 if response.is_success else 1
    source = Path(args.source).resolve()
    allowed = Path(os.environ.get("SANDBOX_SITE_OUTPUT_ROOT", "/workspace/outputs")).resolve()
    if source != allowed and allowed not in source.parents:
        parser.error("site source must be inside /workspace/outputs")
    if not 1 <= len(args.name.strip()) <= 120:
        parser.error("site name must contain 1-120 characters")
    try:
        slug = normalize_site_slug(args.slug)
        archive, files = build_site_archive(source)
    except ValueError as exc:
        parser.error(str(exc))
    params = {"name":args.name.strip(), "slug":slug,
        "idempotency_key":args.idempotency_key or "sandbox-site-" + uuid.uuid4().hex}
    response = httpx.post(base + "/v1/sites/drafts", headers={**headers,
        "Content-Type":"application/x-tar"}, params=params, content=archive, timeout=120)
    if response.is_success:
        value = response.json(); value["validated_files"] = len(files)
        print(json.dumps(value, ensure_ascii=False))
    else:
        print(response.text)
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
