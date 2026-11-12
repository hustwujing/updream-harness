from __future__ import annotations

from .observability import observed, event, enrich, carrier, add_link, telemetry

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import time
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from .domain import Harness, identifier


class SandboxNotFound(RuntimeError):
    pass


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
    reused: bool = False
    expires_at_monotonic: float = 0


class SandboxBackend(Protocol):
    @observed("sandbox.create")
    async def create(self, harness: Harness, image: str, metadata: dict[str, str], environment: dict[str, str]) -> Sandbox: ...
    @observed("sandbox.bind")
    async def bind(self, sandbox: Sandbox, thread_id: str, turn_id: str) -> None: ...
    async def renew(self, sandbox: Sandbox, *, force: bool = False) -> None: ...
    @observed("sandbox.delete")
    async def delete(self, sandbox: Sandbox) -> None: ...
    async def find_by_turn(self, turn_id: str) -> list[Sandbox]: ...
    async def list_namespace(self) -> list[Sandbox]: ...
    async def get(self, sandbox_id: str) -> Sandbox: ...
    async def endpoint(self, sandbox: Sandbox, port: int) -> tuple[str, dict[str, str]]: ...


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
            if status.status_code == 404:
                raise SandboxNotFound("OpenSandbox no longer has sandbox " + sandbox.id)
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
                    try:
                        health = await self.client.get(sandbox.runtime_url.rstrip("/") + "/health")
                        if health.status_code == 200 and health.json().get("status") == "ok":
                            return sandbox
                    except (httpx.TimeoutException, httpx.TransportError, ValueError):
                        pass
            await asyncio.sleep(0.25)
        raise RuntimeError("OpenSandbox runtime endpoint did not become ready")

    @observed("sandbox.create")
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
            sandbox = await self._ready(self._sandbox(response.json(), harness, image, metadata))
            sandbox.expires_at_monotonic = time.monotonic() + self.timeout_seconds
            return sandbox
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
            if (metadata.get("sandbox-harness.io/namespace") != self.namespace
                    or metadata.get("sandbox-harness.io/turn") != turn_id):
                continue
            harness = Harness(metadata.get("sandbox-harness.io/harness", "codex"))
            result.append(await self._ready(self._sandbox(row, harness, (row.get("image") or {}).get("uri", ""), metadata)))
        return result

    async def list_namespace(self) -> list[Sandbox]:
        response = await self.client.get("/v1/sandboxes", params={
            "metadata": f"sandbox-harness.io/namespace={self.namespace}", "pageSize": 100,
        })
        response.raise_for_status()
        rows = response.json().get("items") or response.json().get("sandboxes") or []
        result = []
        for row in rows:
            metadata = row.get("metadata") or {}
            if metadata.get("sandbox-harness.io/namespace") != self.namespace:
                continue
            try:
                harness = Harness(metadata.get("sandbox-harness.io/harness", "codex"))
                sandbox = self._sandbox(row, harness, (row.get("image") or {}).get("uri", ""), metadata)
                status = await self.client.get(f"/v1/sandboxes/{sandbox.id}")
                if status.status_code == 404:
                    continue
                status.raise_for_status()
                result.append(sandbox)
            except RuntimeError:
                continue
        return result

    async def get(self, sandbox_id: str) -> Sandbox:
        response = await self.client.get(f"/v1/sandboxes/{sandbox_id}")
        if response.status_code == 404:
            raise SandboxNotFound("OpenSandbox no longer has sandbox " + sandbox_id)
        response.raise_for_status()
        row = response.json(); metadata = row.get("metadata") or {}
        if metadata.get("sandbox-harness.io/namespace") != self.namespace:
            raise SandboxNotFound("Sandbox is outside this product namespace")
        harness = Harness(metadata.get("sandbox-harness.io/harness", "codex"))
        return await self._ready(self._sandbox(row, harness, (row.get("image") or {}).get("uri", ""), metadata))

    async def endpoint(self, sandbox: Sandbox, port: int) -> tuple[str, dict[str, str]]:
        if not 1 <= port <= 65535:
            raise ValueError("Invalid sandbox port")
        response = await self.client.get(f"/v1/sandboxes/{sandbox.id}/endpoints/{port}",
            params={"use_server_proxy": "true"})
        if response.status_code == 404:
            raise SandboxNotFound("OpenSandbox endpoint is unavailable")
        response.raise_for_status()
        value = response.json().get("endpoint")
        if not isinstance(value, str) or not value:
            raise RuntimeError("OpenSandbox returned no endpoint")
        return (value if "://" in value else "http://" + value), dict(self.client.headers)

    @observed("sandbox.bind")
    async def bind(self, sandbox: Sandbox, thread_id: str, turn_id: str) -> None:
        patch = {
            "sandbox-harness.io/pool": None,
            "sandbox-harness.io/thread": thread_id,
            "sandbox-harness.io/turn": turn_id,
        }
        response = await self.client.patch(f"/v1/sandboxes/{sandbox.id}/metadata", json=patch)
        if response.status_code == 404:
            raise SandboxNotFound("OpenSandbox no longer has sandbox " + sandbox.id)
        response.raise_for_status()
        await self.renew(sandbox, force=True)
        sandbox.metadata.pop("sandbox-harness.io/pool", None)
        sandbox.metadata.update({
            "sandbox-harness.io/thread": thread_id,
            "sandbox-harness.io/turn": turn_id,
        })

    @observed("sandbox.renew")
    async def renew(self, sandbox: Sandbox, *, force: bool = False) -> None:
        if (not force and sandbox.expires_at_monotonic
                and sandbox.expires_at_monotonic - time.monotonic() > max(60, self.timeout_seconds / 2)):
            return
        target = datetime.now(timezone.utc) + timedelta(seconds=self.timeout_seconds)
        response = await self.client.post(f"/v1/sandboxes/{sandbox.id}/renew-expiration",
            json={"expiresAt": target.isoformat()})
        if response.status_code == 404:
            raise SandboxNotFound("OpenSandbox no longer has sandbox " + sandbox.id)
        response.raise_for_status()
        sandbox.expires_at_monotonic = time.monotonic() + self.timeout_seconds

    @observed("sandbox.delete")
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
        self.renewed: list[str] = []

    @observed("sandbox.create")
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

    async def list_namespace(self) -> list[Sandbox]:
        return [item for item in self.items.values()
            if item.metadata.get("sandbox-harness.io/namespace") == self.namespace]

    async def get(self, sandbox_id: str) -> Sandbox:
        value = self.items.get(sandbox_id)
        if not value:
            raise SandboxNotFound("Fake backend no longer has sandbox " + sandbox_id)
        return value

    async def endpoint(self, sandbox: Sandbox, port: int) -> tuple[str, dict[str, str]]:
        if sandbox.id not in self.items:
            raise SandboxNotFound("Fake backend no longer has sandbox " + sandbox.id)
        return self.runtime_url.rstrip("/") + f"/ports/{port}", {}

    @observed("sandbox.bind")
    async def bind(self, sandbox: Sandbox, thread_id: str, turn_id: str) -> None:
        if sandbox.id not in self.items:
            raise SandboxNotFound("Fake backend no longer has sandbox " + sandbox.id)
        sandbox.metadata.pop("sandbox-harness.io/pool", None)
        sandbox.metadata.update({
            "sandbox-harness.io/thread": thread_id,
            "sandbox-harness.io/turn": turn_id,
        })
        await self.renew(sandbox, force=True)

    async def renew(self, sandbox: Sandbox, *, force: bool = False) -> None:
        if sandbox.id not in self.items:
            raise SandboxNotFound("Fake backend no longer has sandbox " + sandbox.id)
        self.renewed.append(sandbox.id)

    @observed("sandbox.delete")
    async def delete(self, sandbox: Sandbox) -> None:
        self.items.pop(sandbox.id, None)
