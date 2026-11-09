from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import subprocess
import time
import uuid

import httpx


TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "WAITING_USER"}


async def request(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> httpx.Response:
    last = None
    for _ in range(120):
        try:
            response = await client.request(method, path, **kwargs)
            if response.status_code < 500:
                return response
        except httpx.TransportError as exc:
            last = exc
        await asyncio.sleep(1)
    raise RuntimeError("API did not recover") from last


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:22766/")
    parser.add_argument("--report", default=".data/broker-recovery-report.json")
    args = parser.parse_args()
    async with httpx.AsyncClient(base_url=args.base_url, timeout=None, trust_env=False) as client:
        response = await request(client, "POST", "api/threads", json={
            "harness": "claude", "model": "claude-sonnet-5-5", "reasoning_level": "standard",
            "title": "Broker SIGKILL recovery",
        })
        response.raise_for_status(); thread = response.json()
        prompt = """Work directly in the sandbox. Create /workspace/project/recovery_probe.py. The program must atomically create /workspace/project/recovery-once.marker using exclusive creation mode, print STARTED with flush=True, sleep for exactly 35 seconds, write /workspace/outputs/broker-recovery.json containing {"executions":1,"result":"recovered"}, then print FINISHED with flush=True. Execute the program in the foreground exactly once, read and validate the JSON, then reply briefly. Never delete or overwrite recovery-once.marker."""
        response = await request(client, "POST", f"api/threads/{thread['id']}/turns", json={
            "message": prompt, "attachments": [], "idempotency_key": "broker-recovery-" + uuid.uuid4().hex,
            "delivery_mode": "start",
        })
        response.raise_for_status(); initial = response.json(); turn_id = initial["id"]
        before = None
        for _ in range(180):
            response = await request(client, "GET", f"api/turns/{turn_id}")
            response.raise_for_status(); before = response.json()
            if before["state"] == "RUNNING" and before.get("runtime_execution_id") and before.get("runtime_sequence", 0) >= 2:
                break
            if before["state"] in TERMINAL:
                raise AssertionError("Turn finished before fault injection: " + before["state"])
            await asyncio.sleep(.5)
        else:
            raise TimeoutError("Turn did not enter an observable Runtime execution")

        old_pid = subprocess.check_output(["systemctl", "show", "-p", "MainPID", "--value",
            "sandbox-harness-api.service"], text=True).strip()
        subprocess.run(["sudo", "-n", "systemctl", "kill", "--kill-who=main", "--signal=SIGKILL",
            "sandbox-harness-api.service"], check=True)
        killed_at = time.monotonic()

        final = None
        for _ in range(240):
            response = await request(client, "GET", f"api/turns/{turn_id}")
            response.raise_for_status(); final = response.json()
            if final["state"] in TERMINAL:
                break
            await asyncio.sleep(1)
        else:
            raise TimeoutError("Recovered Turn did not finish")
        new_pid = subprocess.check_output(["systemctl", "show", "-p", "MainPID", "--value",
            "sandbox-harness-api.service"], text=True).strip()
        if final["state"] != "COMPLETED":
            raise AssertionError(f"Recovered Turn ended {final['state']}: {final.get('error_code')} {final.get('error_detail')}")
        if new_pid == old_pid or final["sandbox_id"] != before["sandbox_id"]:
            raise AssertionError("Broker or Sandbox identity did not match the recovery contract")
        if final["recovery_generation"] <= before["recovery_generation"]:
            raise AssertionError("Recovery generation did not advance")
        artifact = next((item for item in final["artifacts"] if item["path"] == "broker-recovery.json"), None)
        if not artifact:
            raise AssertionError("Recovered Turn did not export broker-recovery.json")
        response = await request(client, "GET", f"api/artifacts/{artifact['id']}")
        response.raise_for_status()
        artifact_value = response.json()
        if artifact_value != {"executions": 1, "result": "recovered"}:
            raise AssertionError("Recovery artifact proves an unexpected execution count")
        response = await request(client, "GET", f"api/turns/{turn_id}/events")
        response.raise_for_status()
        event_text = response.text
        if "turn_runtime_reattached" not in event_text:
            raise AssertionError("Recovery observation event was not emitted")
        report = {
            "turn_id": turn_id, "thread_id": thread["id"], "state": final["state"],
            "sandbox_id": final["sandbox_id"], "old_pid": old_pid, "new_pid": new_pid,
            "generation_before": before["recovery_generation"],
            "generation_after": final["recovery_generation"],
            "runtime_sequence_before": before["runtime_sequence"],
            "runtime_sequence_after": final["runtime_sequence"],
            "recovery_seconds": round(time.monotonic() - killed_at, 3),
            "artifact": artifact_value,
        }
        target = Path(args.report); target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
