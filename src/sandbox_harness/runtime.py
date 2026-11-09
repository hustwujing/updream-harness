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

    async def inject(self, sandbox: Sandbox, *, capability_token: str, gateway_url: str, proxy_url: str, harness: str, model: str) -> None:
        payload = {"capability_token": capability_token, "gateway_url": gateway_url,
            "harness": harness, "model": model, "proxy_url": proxy_url}
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

    async def run(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], native_thread_id: str, reasoning_effort: str) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": native_thread_id, "reasoning_effort": reasoning_effort}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers, timeout=self.timeout, trust_env=False) as client:
            async with client.stream("POST", "/v1/turns", json=body) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    value = json.loads(line)
                    if value.get("type") == "result":
                        yield RuntimeResult.model_validate(value["result"])
                    else:
                        yield RuntimeEvent.model_validate(value)

    async def append_input(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], reasoning_effort: str) -> bool:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": "", "reasoning_effort": reasoning_effort}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers=sandbox.runtime_headers,
            timeout=20, trust_env=False) as client:
            response = await client.post("/v1/inputs", json=body)
            if response.status_code == 409:
                return False
            response.raise_for_status()
            return bool(response.json().get("accepted"))

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
