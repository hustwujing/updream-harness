from __future__ import annotations

from .observability import observed, event, enrich, carrier, add_link, telemetry

import asyncio
import json
import logging
from collections.abc import AsyncIterator

import httpx

from .domain import Attachment, RuntimeEvent, RuntimeResult
from .sandbox import Sandbox


logger = logging.getLogger(__name__)


class RuntimeStreamInterrupted(RuntimeError):
    """The existing execution could not be reattached; never replay its input."""


class RuntimeClient:
    def __init__(self, *, timeout_seconds: int = 3600, transport: httpx.AsyncBaseTransport | None = None):
        self.timeout = httpx.Timeout(timeout_seconds, connect=20)
        self.transport = transport

    @observed("runtime.configure")
    async def inject(self, sandbox: Sandbox, *, capability_token: str, gateway_url: str, proxy_url: str, harness: str, model: str,
                     model_start_timeout_seconds: float = 150, event_idle_timeout_seconds: float = 900) -> None:
        payload = {"capability_token": capability_token, "gateway_url": gateway_url,
            "harness": harness, "model": model, "proxy_url": proxy_url,
            "model_start_timeout_seconds": model_start_timeout_seconds,
            "event_idle_timeout_seconds": event_idle_timeout_seconds,
            "thread_id":sandbox.thread_id or sandbox.metadata.get("sandbox-harness.io/thread", ""),
            "turn_id":sandbox.turn_id or sandbox.metadata.get("sandbox-harness.io/turn", ""),"sandbox_id":sandbox.id}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=20,
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

    @observed("workspace.restore")
    async def restore(self, sandbox: Sandbox, archive: bytes) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=120, trust_env=False) as client:
            response = await client.put("/v1/checkpoint", content=archive, headers={"Content-Type": "application/x-tar"})
            response.raise_for_status()

    async def put_attachment(self, sandbox: Sandbox, name: str, data: bytes, mime: str) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=300, trust_env=False) as client:
            response = await client.put("/v1/attachments/file", params={"name": name}, content=data,
                headers={"Content-Type": mime})
            response.raise_for_status()

    async def put_project_file(self, sandbox: Sandbox, path: str, data: bytes, mime: str) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=300, trust_env=False) as client:
            response = await client.put("/v1/project-files/file", params={"path": path}, content=data,
                headers={"Content-Type": mime})
            response.raise_for_status()

    async def reset_project_files(self, sandbox: Sandbox) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=60, trust_env=False) as client:
            response = await client.post("/v1/project-files/reset")
            response.raise_for_status()

    async def run(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], native_thread_id: str, reasoning_effort: str,
                  execution_id: str = "legacy", input_id: str = "initial", after_sequence: int = 0) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": native_thread_id, "reasoning_effort": reasoning_effort,
                "execution_id": execution_id, "input_id": input_id, "after_sequence": after_sequence}
        async for value in self._execution_stream(sandbox, execution_id, after_sequence, body=body):
            yield value

    async def _execution_stream(self, sandbox: Sandbox, execution_id: str, after_sequence: int,
                                *, body: dict | None = None):
        cursor = after_sequence
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=self.timeout, trust_env=False, transport=self.transport) as client:
            for attempt in range(4):
                try:
                    # A lost POST reply is an unknown submission. Every retry only reads
                    # the same execution; it must never start another Harness invocation.
                    method = "POST" if attempt == 0 and body is not None else "GET"
                    path = "/v1/turns" if method == "POST" else f"/v1/executions/{execution_id}/events"
                    kwargs = {"json": body} if method == "POST" else {"params": {"after": cursor}}
                    async with client.stream(method, path, **kwargs) as response:
                        response.raise_for_status()
                        async for line in response.aiter_lines():
                            if not line:
                                continue
                            value = json.loads(line)
                            if value.get("type") == "heartbeat":
                                continue
                            sequence = value.get("runtime_sequence", 0)
                            if value.get("type") == "result":
                                yield RuntimeResult.model_validate({**value["result"], "runtime_sequence": sequence})
                                return
                            if sequence and sequence <= cursor:
                                continue
                            yield RuntimeEvent.model_validate(value)
                            cursor = max(cursor, sequence)
                    # The final frame may already have been consumed by a prior Broker.
                    status = await client.get(f"/v1/executions/{execution_id}")
                    if status.status_code == 200:
                        snapshot = status.json()
                        if (snapshot.get("result") is not None
                                and int(snapshot.get("last_sequence", 0)) <= cursor):
                            yield RuntimeResult.model_validate({**snapshot["result"], "runtime_sequence": cursor})
                            return
                except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code not in {404, 408, 429, 500, 502, 503, 504}:
                        raise
                    event("runtime.transport.interrupted",level=logging.WARNING,execution_id=execution_id,attempt=attempt,runtime_sequence=cursor,exc=exc)
                    logger.warning("Runtime transport interrupted execution=%s cursor=%s attempt=%s error=%s",
                        execution_id, cursor, attempt + 1, type(exc).__name__)
                if attempt < 3:
                    event("runtime.stream.reattach",level=logging.WARNING,execution_id=execution_id,attempt=attempt,runtime_sequence=cursor)
                    logger.warning("Reattaching runtime stream execution=%s cursor=%s attempt=%s",
                        execution_id, cursor, attempt + 1)
                    await asyncio.sleep(0.2 * (attempt + 1))
        raise RuntimeStreamInterrupted("Runtime connection ended without a result; reattachment exhausted")

    @observed("runtime.input")
    async def append_input(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], reasoning_effort: str,
                           execution_id: str = "legacy", input_id: str = "initial") -> str:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": "", "reasoning_effort": reasoning_effort,
                "execution_id": execution_id, "input_id": input_id}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
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
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.get("/v1/executions/" + execution_id)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()

    @observed("runtime.interaction")
    async def respond_interaction(self, sandbox: Sandbox, interaction_id: str, response_value: dict) -> bool:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/interactions/{interaction_id}", json=response_value)
            if response.status_code == 409:
                return False
            response.raise_for_status()
            return bool(response.json().get("accepted"))

    async def background_jobs(self, sandbox: Sandbox) -> list[dict]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.get("/v1/background-jobs")
            response.raise_for_status()
            return response.json()["items"]

    async def cancel_background_job(self, sandbox: Sandbox, job_id: str) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/background-jobs/{job_id}/cancel")
            if response.status_code == 404:
                raise KeyError("background_job_not_found")
            response.raise_for_status()
            return response.json()

    async def resume(self, sandbox: Sandbox, execution_id: str, after_sequence: int) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        async for value in self._execution_stream(sandbox, execution_id, after_sequence):
            yield value

    @observed("workspace.checkpoint")
    async def checkpoint(self, sandbox: Sandbox) -> bytes:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=300, trust_env=False) as client:
            response = await client.get("/v1/checkpoint")
            response.raise_for_status()
            return response.content

    async def artifacts(self, sandbox: Sandbox) -> list[dict]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=60, trust_env=False) as client:
            response = await client.get("/v1/artifacts")
            response.raise_for_status()
            return response.json()["items"]

    @observed("artifact.download")
    async def artifact(self, sandbox: Sandbox, path: str) -> bytes:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=300, trust_env=False) as client:
            response = await client.get("/v1/artifacts/file", params={"path": path})
            response.raise_for_status()
            return response.content

    async def diagnostics(self, sandbox: Sandbox, after: int = 0) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,headers={**sandbox.runtime_headers,**carrier()},
            timeout=5,trust_env=False) as client:
            response=await client.get("/v1/diagnostics",params={"after":after,"limit":500})
            response.raise_for_status()
            if len(response.content)>4*1024*1024:raise ValueError("telemetry_snapshot_too_large")
            return response.json()

    @observed("runtime.cancel")
    async def cancel(self, sandbox: Sandbox) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=10, trust_env=False) as client:
            response = await client.post("/v1/cancel")
            response.raise_for_status()
