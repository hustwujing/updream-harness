from __future__ import annotations

import argparse
import json
import os
import uuid

import httpx
from pydantic import ValidationError

from sandbox_harness.domain import MediaDraftCreate
from sandbox_harness.media_catalog import MediaCatalog
from .state import capability_token_file


def validation_message(error: ValidationError) -> str:
    return "; ".join((".".join(str(item) for item in value.get("loc", ())) + ": "
        + value.get("msg", "Invalid value")) for value in error.errors(
        include_url=False, include_context=False, include_input=False)[:12])


def canonical_parameters(kind: str, generation_mode: str, parameters: dict) -> dict:
    value = dict(parameters)
    if kind == "video" and generation_mode == "edit_video":
        value.update({"aspect_ratio": "adaptive", "duration": -1})
    elif kind == "video" and generation_mode == "extend_video":
        value["aspect_ratio"] = "adaptive"
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sandbox-media")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare", help="Emit a canonical confirmation payload")
    prepare.add_argument("kind", choices=("image", "video", "audio", "model3d"))
    prepare.add_argument("--model", required=True)
    prepare.add_argument("--parameters", default="{}")
    prepare.add_argument("--generation-mode", default="")
    prepare.add_argument("--reference", action="append", default=[],
        help=('Repeatable JSON with a protocol role and semantic usage, for example '
            '{"project_path":"assets/input.png","role":"first_frame","usage":"composition"}. '
            'Read allowed roles/usages from sandbox-media catalog.'))
    prepare.add_argument("--idempotency-key", default="")
    prepare.add_argument("--output-role", choices=("candidate", "workspace", "delivery"), default="candidate")
    sub.add_parser("commit", help="Seal this Turn's complete media draft batch")
    catalog = sub.add_parser("catalog")
    catalog.add_argument("--kind", choices=("image", "video", "audio", "model3d"), default="")
    status = sub.add_parser("status")
    status.add_argument("--job-id", required=True)
    cancel = sub.add_parser("cancel")
    cancel.add_argument("--job-id", required=True)
    fetch = sub.add_parser("fetch", help="Download a completed media candidate for inspection")
    fetch.add_argument("--job-id", required=True)
    fetch.add_argument("--output", required=True)
    promote = sub.add_parser("promote", help="Keep a completed candidate in the persistent project")
    promote.add_argument("--job-id", required=True)
    promote.add_argument("--output-role", choices=("workspace", "delivery"), required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        try:
            parameters = canonical_parameters(args.kind, args.generation_mode, json.loads(args.parameters))
            references = [json.loads(value) for value in args.reference]
            if not all(isinstance(value, dict) for value in references):
                raise ValueError("references must be JSON objects")
            for reference in references:
                path = reference.get("project_path")
                if isinstance(path, str) and path.startswith("/workspace/project/"):
                    reference["project_path"] = path.removeprefix("/workspace/project/")
        except ValueError as exc:
            parser.error(f"media draft JSON is invalid: {exc}")
        payload = {"kind": args.kind, "model": args.model, "parameters": parameters,
            "references": references,
            "idempotency_key": args.idempotency_key or "sandbox-media-" + uuid.uuid4().hex,
            "output_role": args.output_role}
        if args.generation_mode:
            payload["generation_mode"] = args.generation_mode
        try:
            MediaDraftCreate.model_validate(payload)
            MediaCatalog.validate_parameter_shape(args.kind, parameters)
            if args.kind == "model3d":
                MediaCatalog.validate_model3d_parameter_names(
                    args.generation_mode or "text_to_model", parameters)
            elif args.kind == "audio":
                MediaCatalog.validate_audio_parameter_names(
                    args.generation_mode or "text_to_audio", parameters)
        except ValidationError as exc:
            parser.error("invalid media draft: " + validation_message(exc))
        except ValueError as exc:
            parser.error("invalid media parameters: " + str(exc))
    base = os.environ.get("HARNESS_GATEWAY_URL", "").rstrip("/")
    if not base:
        print(json.dumps({"error": "HARNESS_GATEWAY_URL is not configured"}), ensure_ascii=False)
        return 2
    token = capability_token_file().read_text(encoding="utf-8").strip()
    if args.command == "catalog":
        response = httpx.get(base + "/v1/media/catalog", headers={"Authorization": "Bearer " + token},
            params={"kind": args.kind} if args.kind else None, timeout=20)
        print(response.text)
        return 0 if response.is_success else 1
    if args.command == "commit":
        response = httpx.post(base + "/v1/media/batch/commit",
            headers={"Authorization": "Bearer " + token}, timeout=20)
        print(response.text)
        return 0 if response.is_success else 1
    if args.command == "fetch":
        response = httpx.get(base + f"/v1/media/jobs/{args.job_id}/content",
            headers={"Authorization": "Bearer " + token}, timeout=120)
        if not response.is_success:
            print(response.text)
            return 1
        output = os.path.abspath(args.output)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(output, "wb") as handle:
            handle.write(response.content)
        print(json.dumps({"job_id": args.job_id, "path": output, "size": len(response.content)}, ensure_ascii=False))
        return 0
    if args.command == "promote":
        response = httpx.post(base + f"/v1/media/jobs/{args.job_id}/promote",
            headers={"Authorization": "Bearer " + token},
            json={"output_role": args.output_role}, timeout=20)
        print(response.text)
        return 0 if response.is_success else 1
    if args.command == "prepare":
        response = httpx.post(base + "/v1/media/drafts", headers={"Authorization": "Bearer " + token},
            json=payload, timeout=120)
        print(response.text)
        return 0 if response.is_success else 1
    path = f"/v1/media/jobs/{args.job_id}" + ("/cancel" if args.command == "cancel" else "")
    request = httpx.get if args.command == "status" else httpx.post
    response = request(base + path, headers={"Authorization": "Bearer " + token}, timeout=20)
    print(response.text)
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
