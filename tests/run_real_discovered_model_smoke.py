from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import uuid

import httpx


MODELS = {
    "codex": "gpt-6-sol",
    "claude": "claude-sonnet-5-5",
    "deepseek": "deepseek-v4-pro",
}


async def wait(client: httpx.AsyncClient, turn_id: str) -> dict:
    for _ in range(900):
        response = await client.get(f"api/turns/{turn_id}")
        response.raise_for_status()
        value = response.json()
        if value["state"] in {"COMPLETED", "FAILED", "CANCELLED", "WAITING_USER"}:
            return value
        await asyncio.sleep(1)
    raise TimeoutError("Model smoke Turn did not finish")


async def run_model(client: httpx.AsyncClient, harness: str, model: str) -> dict:
    expected = f"{harness}:{model}:ok"
    response = await client.post("api/threads", json={
        "harness": harness, "model": model, "reasoning_level": "standard", "title": f"Discovered model smoke {harness} {model}",
    })
    response.raise_for_status()
    thread = response.json()
    prompt = f"""Work in the sandbox. Write /workspace/project/model_smoke.py using only Python's standard library. The program must create /workspace/outputs/model-smoke.json with exactly this JSON object: {{"harness":"{harness}","model":"{model}","result":"{expected}"}}. Execute the program, parse the resulting JSON to verify every value, then reply briefly. Do not merely describe code."""
    response = await client.post(f"api/threads/{thread['id']}/turns", json={
        "message": prompt, "attachments": [], "idempotency_key": "model-smoke-" + uuid.uuid4().hex,
        "harness": harness, "model": model, "reasoning_level": "standard", "delivery_mode": "start",
    })
    response.raise_for_status()
    final = await wait(client, response.json()["id"])
    if final["state"] != "COMPLETED":
        raise AssertionError(f"{harness}/{model}: {final['state']} {final.get('error_code')} {final.get('error_detail')}")
    artifact = next((item for item in final["artifacts"] if item["path"] == "model-smoke.json"), None)
    if not artifact:
        raise AssertionError(f"{harness}/{model} did not export model-smoke.json")
    response = await client.get(f"api/artifacts/{artifact['id']}")
    response.raise_for_status()
    assert response.json() == {"harness": harness, "model": model, "result": expected}
    response = await client.get(f"api/threads/{thread['id']}/items")
    response.raise_for_status()
    items = response.json()
    return {
        "harness": harness,
        "model": model,
        "state": final["state"],
        "thread_id": thread["id"],
        "turn_id": final["id"],
        "sandbox_id": final["sandbox_id"],
        "tool_items": sum(item["type"] == "tool" for item in items),
        "agent_messages": sum(item["type"] == "agent_message" for item in items),
        "artifact": "model-smoke.json",
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:22766/")
    parser.add_argument("--report", default=".data/discovered-model-smoke.json")
    args = parser.parse_args()
    async with httpx.AsyncClient(base_url=args.base_url, timeout=None, trust_env=False) as client:
        response = await client.get("api/session")
        response.raise_for_status()
        results = await asyncio.gather(*(run_model(client, harness, model) for harness, model in MODELS.items()), return_exceptions=True)
    report = {"results": []}
    failed = False
    for (harness, model), result in zip(MODELS.items(), results, strict=True):
        if isinstance(result, Exception):
            failed = True
            report["results"].append({"harness": harness, "model": model, "state": "FAILED", "error": type(result).__name__ + ": " + str(result)})
        else:
            report["results"].append(result)
    target = Path(args.report)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
