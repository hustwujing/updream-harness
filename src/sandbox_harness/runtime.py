from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import httpx

from .domain import Attachment, RuntimeEvent, RuntimeResult
from .sandbox import Sandbox


class RuntimeClient:
    def __init__(self, *, timeout_seconds: int = 3600, transport: httpx.AsyncBaseTransport | None = None):
        self.timeout = httpx.Timeout(timeout_seconds, connect=20)
        self.transport = transport

    async def inject(self, sandbox: Sandbox, *, capability_token: str, gateway_url: str, proxy_url: str, harness: str, model: str,
                     model_start_timeout_seconds: float = 150, event_idle_timeout_seconds: float = 900) -> None:
        payload = {"capability_token": capability_token, "gateway_url": gateway_url,
            "harness": harness, "model": model, "proxy_url": proxy_url,
            "model_start_timeout_seconds": model_start_timeout_seconds,
            "event_idle_timeout_seconds": event_idle_timeout_seconds}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=20,
            trust_env=False, transport=self.transport) as client:
            for attempt in range(8):
                try:
                    response = await client.post("/v1/configure", json=payload)
                    if response.status_code not in {502, 503, 504}:
                        response.raise_for_status()
                        return
                    if attempt == 7:
                        response.raise_for_status()
                except (httpx.TimeoutException, httpx.TransportError):
                    if attempt == 7:
                        raise
                await asyncio.sleep(0.25 * (attempt + 1))

    async def restore(self, sandbox: Sandbox, archive: bytes) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=120, trust_env=False) as client:
            response = await client.put("/v1/checkpoint", content=archive, headers={"Content-Type": "application/x-tar"})
            response.raise_for_status()

    async def put_attachment(self, sandbox: Sandbox, name: str, data: bytes, mime: str) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=300, trust_env=False) as client:
            response = await client.put("/v1/attachments/file", params={"name": name}, content=data,
                headers={"Content-Type": mime})
            response.raise_for_status()

    async def put_project_file(self, sandbox: Sandbox, path: str, data: bytes, mime: str) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=300, trust_env=False) as client:
            response = await client.put("/v1/project-files/file", params={"path": path}, content=data,
                headers={"Content-Type": mime})
            response.raise_for_status()

    async def reset_project_files(self, sandbox: Sandbox) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=60, trust_env=False) as client:
            response = await client.post("/v1/project-files/reset")
            response.raise_for_status()

    async def run(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], native_thread_id: str, reasoning_effort: str,
                  execution_id: str = "legacy", input_id: str = "initial", after_sequence: int = 0) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": native_thread_id, "reasoning_effort": reasoning_effort,
                "execution_id": execution_id, "input_id": input_id, "after_sequence": after_sequence}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=self.timeout, trust_env=False) as client:
            async with client.stream("POST", "/v1/turns", json=body) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    value = json.loads(line)
                    if value.get("type") == "result":
                        yield RuntimeResult.model_validate({**value["result"], "runtime_sequence": value.get("runtime_sequence", 0)})
                    else:
                        yield RuntimeEvent.model_validate(value)

    async def append_input(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], reasoning_effort: str,
                           execution_id: str = "legacy", input_id: str = "initial") -> str:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": "", "reasoning_effort": reasoning_effort,
                "execution_id": execution_id, "input_id": input_id}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=20, trust_env=False) as client:
            response = await client.post("/v1/inputs", json=body)
            if response.status_code == 409:
                return ""
            response.raise_for_status()
            value = response.json()
            if not value.get("accepted"):
                return ""
            return {"codex": "native_steer", "claude": "native_stream",
                "deepseek": "native_safe_point"}[sandbox.harness.value]

    async def execution_status(self, sandbox: Sandbox, execution_id: str) -> dict | None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=20, trust_env=False) as client:
            response = await client.get("/v1/executions/" + execution_id)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()

    async def respond_interaction(self, sandbox: Sandbox, interaction_id: str, response_value: dict) -> bool:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/interactions/{interaction_id}", json=response_value)
            if response.status_code == 409:
                return False
            response.raise_for_status()
            return bool(response.json().get("accepted"))

    async def background_jobs(self, sandbox: Sandbox) -> list[dict]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=20, trust_env=False) as client:
            response = await client.get("/v1/background-jobs")
            response.raise_for_status()
            return response.json()["items"]

    async def cancel_background_job(self, sandbox: Sandbox, job_id: str) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/background-jobs/{job_id}/cancel")
            if response.status_code == 404:
                raise KeyError("background_job_not_found")
            response.raise_for_status()
            return response.json()

    async def resume(self, sandbox: Sandbox, execution_id: str, after_sequence: int) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=self.timeout, trust_env=False) as client:
            async with client.stream("GET", f"/v1/executions/{execution_id}/events",
                params={"after": after_sequence}) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    value = json.loads(line)
                    if value.get("type") == "result":
                        yield RuntimeResult.model_validate({**value["result"], "runtime_sequence": value.get("runtime_sequence", 0)})
                    else:
                        yield RuntimeEvent.model_validate(value)

    async def checkpoint(self, sandbox: Sandbox) -> bytes:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=300, trust_env=False) as client:
            response = await client.get("/v1/checkpoint")
            response.raise_for_status()
            return response.content

    async def artifacts(self, sandbox: Sandbox) -> list[dict]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=60, trust_env=False) as client:
            response = await client.get("/v1/artifacts")
            response.raise_for_status()
            return response.json()["items"]

    async def artifact(self, sandbox: Sandbox, path: str) -> bytes:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=300, trust_env=False) as client:
            response = await client.get("/v1/artifacts/file", params={"path": path})
            response.raise_for_status()
            return response.content

    async def cancel(self, sandbox: Sandbox) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=10, trust_env=False) as client:
            response = await client.post("/v1/cancel")
            response.raise_for_status()
