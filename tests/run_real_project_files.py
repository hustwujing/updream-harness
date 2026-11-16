from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import subprocess
import uuid

import httpx


TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "WAITING_USER"}


async def wait_turn(client: httpx.AsyncClient, turn_id: str) -> dict:
    for _ in range(600):
        try:
            response = await client.get(f"api/turns/{turn_id}")
            if response.status_code == 200:
                value = response.json()
                if value["state"] in TERMINAL:
                    return value
        except httpx.TransportError:
            pass
        await asyncio.sleep(1)
    raise TimeoutError("Turn did not finish")


async def wait_health(client: httpx.AsyncClient) -> None:
    for _ in range(120):
        try:
            response = await client.get("api/health")
            if response.status_code == 200:
                return
        except httpx.TransportError:
            pass
        await asyncio.sleep(1)
    raise TimeoutError("API did not become healthy")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:24766/")
    parser.add_argument("--report", default=".data/project-files-report.json")
    args = parser.parse_args()
    async with httpx.AsyncClient(base_url=args.base_url, timeout=None, trust_env=False) as client:
        payload = b"persistent-reference-v1\n"
        response = await client.post("api/uploads", params={"name":"reference.txt", "mime":"text/plain"},
            content=payload)
        response.raise_for_status(); attachment = response.json()
        response = await client.post("api/threads", json={"harness":"claude", "model":"claude-sonnet-5-5",
            "reasoning_level":"standard", "title":"Cross-sandbox project files"})
        response.raise_for_status(); thread = response.json()
        first_prompt = """Work directly in the sandbox. Read /workspace/inbox/reference.txt and verify it contains exactly persistent-reference-v1 followed by a newline. Write /workspace/outputs/first.json with exactly {"asset":"persistent-reference-v1","stage":1} using Python, parse it to verify, and reply briefly."""
        response = await client.post(f"api/threads/{thread['id']}/turns", json={"message":first_prompt,
            "attachments":[attachment], "idempotency_key":"project-files-first-"+uuid.uuid4().hex,
            "delivery_mode":"start"})
        response.raise_for_status(); first = await wait_turn(client, response.json()["id"])
        if first["state"] != "COMPLETED":
            raise AssertionError(f"First Turn failed: {first.get('error_code')} {first.get('error_detail')}")
        response = await client.get(f"api/threads/{thread['id']}/project-files")
        response.raise_for_status(); first_files = response.json()
        if {item["path"] for item in first_files} != {"assets/reference.txt", "artifacts/first.json"}:
            raise AssertionError("First project manifest is incomplete")

        subprocess.run(["sudo", "-n", "systemctl", "restart", "sandbox-harness-parallel-api.service"], check=True)
        await wait_health(client)
        second_prompt = """Do not use /workspace/inbox. Read /workspace/project/assets/reference.txt and /workspace/project/artifacts/first.json. Verify the text and JSON agree, then write /workspace/outputs/second.json with exactly {"asset":"persistent-reference-v1","previous_stage":1,"stage":2} using Python. Parse it to verify and reply briefly."""
        response = await client.post(f"api/threads/{thread['id']}/turns", json={"message":second_prompt,
            "attachments":[], "idempotency_key":"project-files-second-"+uuid.uuid4().hex,
            "delivery_mode":"start"})
        response.raise_for_status(); second = await wait_turn(client, response.json()["id"])
        if second["state"] != "COMPLETED":
            raise AssertionError(f"Second Turn failed: {second.get('error_code')} {second.get('error_detail')}")
        if second["sandbox_id"] == first["sandbox_id"]:
            raise AssertionError("Second Turn did not use a new Sandbox")
        response = await client.get(f"api/threads/{thread['id']}/project-files")
        response.raise_for_status(); final_files = response.json()
        paths = {item["path"] for item in final_files}
        if paths != {"assets/reference.txt", "artifacts/first.json", "artifacts/second.json"}:
            raise AssertionError("Final project manifest is incomplete: " + repr(paths))
        second_file = next(item for item in final_files if item["path"] == "artifacts/second.json")
        response = await client.get(f"api/project-files/{second_file['id']}")
        response.raise_for_status(); value = response.json()
        expected = {"asset":"persistent-reference-v1", "previous_stage":1, "stage":2}
        if value != expected:
            raise AssertionError("Second project artifact content is wrong")
        report = {"thread_id":thread["id"], "first_turn_id":first["id"], "second_turn_id":second["id"],
            "first_sandbox_id":first["sandbox_id"], "second_sandbox_id":second["sandbox_id"],
            "files":[{"path":item["path"], "version":item["version"], "sha256":item["sha256"]}
                for item in final_files], "second_artifact":value}
        target = Path(args.report); target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
