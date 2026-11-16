from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import uuid

import httpx

from sandbox_harness.sessions import COOKIE


PROJECT_FILES = {
    "codex-media": "process_video.py",
    "claude-images": "batch_images.py",
    "deepseek-repair": "process_audio.py",
}


async def wait_for_turn(client: httpx.AsyncClient, turn_id: str) -> dict:
    for _ in range(900):
        response = await client.get(f"api/turns/{turn_id}")
        response.raise_for_status()
        value = response.json()
        if value["state"] in {"COMPLETED", "FAILED", "CANCELLED", "WAITING_USER"}:
            return value
        await asyncio.sleep(1)
    raise TimeoutError("Checkpoint resume Turn did not finish")


async def verify(client: httpx.AsyncClient, original: dict) -> dict:
    case = original["case"]
    project_file = PROJECT_FILES[case]
    output = "checkpoint-" + original["harness"] + ".json"
    prompt = f"""This is a checkpoint recovery verification. Do not recreate {project_file} and do not use files from /workspace/inbox.
Read the already existing /workspace/project/{project_file}, compute its SHA-256 and byte size, and write /workspace/outputs/{output} as JSON with exactly these keys: path, exists, bytes, sha256. The path must be /workspace/project/{project_file}, exists must be true, and the hash and size must come from the restored file. Verify the JSON and reply briefly."""
    response = await client.post(f"api/threads/{original['thread_id']}/turns", json={
        "message": prompt,
        "attachments": [],
        "idempotency_key": "checkpoint-resume-" + uuid.uuid4().hex,
        "delivery_mode": "start",
    })
    response.raise_for_status()
    turn = response.json()
    final = await wait_for_turn(client, turn["id"])
    if final["state"] != "COMPLETED":
        raise AssertionError(f"{case} resume failed: {final.get('error_code')} {final.get('error_detail')}")
    if final["sandbox_id"] == original.get("sandbox_id"):
        raise AssertionError(case + " reused the old sandbox instead of restoring a checkpoint")
    artifact = next((item for item in final["artifacts"] if item["path"] == output), None)
    if not artifact:
        raise AssertionError(case + " did not export " + output)
    response = await client.get(f"api/artifacts/{artifact['id']}")
    response.raise_for_status()
    value = response.json()
    assert value["path"] == "/workspace/project/" + project_file
    assert value["exists"] is True and value["bytes"] > 0
    assert len(value["sha256"]) == 64
    return {
        "case": case,
        "state": final["state"],
        "previous_turn_id": original["turn_id"],
        "resume_turn_id": final["id"],
        "previous_sandbox_id": original.get("sandbox_id", ""),
        "resume_sandbox_id": final["sandbox_id"],
        "artifact": output,
        "project_file": value,
        "artifact_sha256": hashlib.sha256(response.content).hexdigest(),
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:24766/")
    parser.add_argument("--report", default=".data/first-capability-report.json")
    parser.add_argument("--case", choices=[*PROJECT_FILES, "all"], default="all")
    args = parser.parse_args()
    path = Path(args.report)
    report = json.loads(path.read_text())
    selected = report["results"] if args.case == "all" else [result for result in report["results"] if result["case"] == args.case]
    async with httpx.AsyncClient(base_url=args.base_url, timeout=None, trust_env=False) as client:
        session_file = path.with_suffix(".session")
        if session_file.is_file():
            client.cookies.set(COOKIE, session_file.read_text().strip())
        for result in selected:
            response = await client.get(f"api/turns/{result['turn_id']}")
            response.raise_for_status()
            result["sandbox_id"] = response.json()["sandbox_id"]
        resumed = await asyncio.gather(*(verify(client, result) for result in selected), return_exceptions=True)
    failed = False
    outcomes = {result["case"]: result for result in report.get("checkpoint_resume", [])}
    for original, result in zip(selected, resumed, strict=True):
        if isinstance(result, Exception):
            failed = True
            outcomes[original["case"]] = {
                "case": original["case"], "state": "FAILED", "error": type(result).__name__ + ": " + str(result),
            }
        else:
            outcomes[original["case"]] = result
    report["checkpoint_resume"] = [outcomes[result["case"]] for result in report["results"] if result["case"] in outcomes]
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"checkpoint_resume": report["checkpoint_resume"]}, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
