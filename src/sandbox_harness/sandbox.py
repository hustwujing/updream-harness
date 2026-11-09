from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from .domain import Harness, identifier


@dataclass
class Sandbox:
    id: str
    harness: Harness
    image: str
    runtime_url: str
    metadata: dict[str, str]
    runtime_headers: dict[str, str] = field(default_factory=dict)
    warm: bool = False
    thread_id: str = ""
    turn_id: str = ""


class SandboxBackend(Protocol):
    async def create(self, harness: Harness, image: str, metadata: dict[str, str], environment: dict[str, str]) -> Sandbox: ...
    async def bind(self, sandbox: Sandbox, thread_id: str, turn_id: str) -> None: ...
    async def delete(self, sandbox: Sandbox) -> None: ...
    async def find_by_turn(self, turn_id: str) -> list[Sandbox]: ...


class OpenSandboxBackend:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: int, namespace: str, gateway_url: str = "http://172.31.0.1:28600", proxy_port: int = 28601):
        headers = {"OPEN-SANDBOX-API-KEY": api_key} if api_key else {}
        self.client = httpx.AsyncClient(base_url=base_url.rstrip("/"), headers=headers, timeout=30, trust_env=False)
        self.timeout_seconds = timeout_seconds
        self.namespace = namespace
        gateway = urlsplit(gateway_url)
        if gateway.scheme != "http" or not gateway.hostname or not gateway.port or gateway.path not in ("", "/"):
            raise ValueError("Sandbox Gateway must be a fixed HTTP origin")
        self.gateway_host = gateway.hostname
        self.gateway_port = gateway.port
        self.proxy_port = proxy_port

    @staticmethod
    def _sandbox(payload: dict[str, Any], harness: Harness, image: str, metadata: dict[str, str]) -> Sandbox:
        sandbox_id = payload.get("id") or payload.get("sandboxId")
        endpoints = payload.get("endpoints") or {}
        runtime_url = payload.get("runtime_url") or endpoints.get("runtime") or endpoints.get("8080")
        if not isinstance(sandbox_id, str):
            raise RuntimeError("OpenSandbox response has no sandbox ID")
        return Sandbox(id=sandbox_id, harness=harness, image=image, runtime_url=runtime_url or "", metadata=metadata)

    async def _ready(self, sandbox: Sandbox) -> Sandbox:
        for _ in range(80):
            status = await self.client.get(f"/v1/sandboxes/{sandbox.id}")
            status.raise_for_status()
            state = ((status.json().get("status") or {}).get("state") or "").lower()
            if state in {"failed", "terminated"}:
                raise RuntimeError("OpenSandbox failed before its runtime endpoint became ready")
            if state == "running":
                endpoint = await self.client.get(
                    f"/v1/sandboxes/{sandbox.id}/endpoints/8080",
                    params={"use_server_proxy": "true"},
                )
                if endpoint.status_code == 200 and endpoint.json().get("endpoint"):
                    value = endpoint.json()["endpoint"]
                    sandbox.runtime_url = value if "://" in value else "http://" + value
                    sandbox.runtime_headers = dict(self.client.headers)
                    return sandbox
            await asyncio.sleep(0.25)
        raise RuntimeError("OpenSandbox runtime endpoint did not become ready")

    async def create(self, harness: Harness, image: str, metadata: dict[str, str], environment: dict[str, str]) -> Sandbox:
        if "@sha256:" not in image:
            raise ValueError("Runtime image must use an immutable manifest digest")
        metadata = {**metadata, "sandbox-harness.io/namespace": self.namespace}
        body = {
            "image": {"uri": image},
            "timeout": self.timeout_seconds,
            "resourceLimits": {"cpu": "4", "memory": "8Gi"},
            "metadata": metadata,
            "env": environment,
            "entrypoint": ["sandbox-harness-runtime"],
        }
        request_id = metadata["sandbox-harness.io/turn"]
        try:
            response = await self.client.post("/v1/sandboxes", json=body, headers={"X-Request-ID": request_id})
            response.raise_for_status()
            return await self._ready(self._sandbox(response.json(), harness, image, metadata))
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            matches = await self.find_by_turn(request_id)
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise RuntimeError("Duplicate sandboxes found for one Turn") from exc
            raise RuntimeError("OpenSandbox create outcome is unknown") from exc

    async def find_by_turn(self, turn_id: str) -> list[Sandbox]:
        metadata_filter = f"sandbox-harness.io/namespace={self.namespace}&sandbox-harness.io/turn={turn_id}"
        response = await self.client.get("/v1/sandboxes", params={"metadata": metadata_filter, "pageSize": 100})
        response.raise_for_status()
        rows = response.json().get("items") or response.json().get("sandboxes") or []
        result = []
        for row in rows:
            metadata = row.get("metadata") or {}
            if metadata.get("sandbox-harness.io/namespace") != self.namespace:
                continue
            harness = Harness(metadata.get("sandbox-harness.io/harness", "codex"))
            result.append(await self._ready(self._sandbox(row, harness, (row.get("image") or {}).get("uri", ""), metadata)))
        return result

    async def bind(self, sandbox: Sandbox, thread_id: str, turn_id: str) -> None:
        patch = {
            "sandbox-harness.io/pool": None,
            "sandbox-harness.io/thread": thread_id,
            "sandbox-harness.io/turn": turn_id,
        }
        response = await self.client.patch(f"/v1/sandboxes/{sandbox.id}/metadata", json=patch)
        response.raise_for_status()
        sandbox.metadata.pop("sandbox-harness.io/pool", None)
        sandbox.metadata.update({
            "sandbox-harness.io/thread": thread_id,
            "sandbox-harness.io/turn": turn_id,
        })

    async def delete(self, sandbox: Sandbox) -> None:
        response = await self.client.delete(f"/v1/sandboxes/{sandbox.id}")
        if response.status_code not in (204, 404):
            response.raise_for_status()
        for _ in range(40):
            check = await self.client.get(f"/v1/sandboxes/{sandbox.id}")
            if check.status_code == 404:
                return
            await asyncio.sleep(0.25)
        raise RuntimeError("Sandbox deletion did not converge")


class FakeSandboxBackend:
    """Explicit test backend. It is never selected as a production fallback."""

    def __init__(self, runtime_url: str = "http://fake-runtime", namespace: str = "sandbox-harness"):
        self.runtime_url = runtime_url
        self.namespace = namespace
        self.items: dict[str, Sandbox] = {}

    async def create(self, harness: Harness, image: str, metadata: dict[str, str], environment: dict[str, str]) -> Sandbox:
        metadata = {**metadata, "sandbox-harness.io/namespace": self.namespace}
        value = Sandbox(id=identifier(), harness=harness, image=image or "fake@sha256:" + "0" * 64, runtime_url=self.runtime_url, metadata=metadata)
        self.items[value.id] = value
        return value

    async def find_by_turn(self, turn_id: str) -> list[Sandbox]:
        return [
            item for item in self.items.values()
            if item.metadata.get("sandbox-harness.io/namespace") == self.namespace
            and item.metadata.get("sandbox-harness.io/turn") == turn_id
        ]

    async def bind(self, sandbox: Sandbox, thread_id: str, turn_id: str) -> None:
        sandbox.metadata.pop("sandbox-harness.io/pool", None)
        sandbox.metadata.update({
            "sandbox-harness.io/thread": thread_id,
            "sandbox-harness.io/turn": turn_id,
        })

    async def delete(self, sandbox: Sandbox) -> None:
        self.items.pop(sandbox.id, None)
