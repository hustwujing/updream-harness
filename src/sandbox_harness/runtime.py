from __future__ import annotations

from .observability import observed, event, enrich, carrier, add_link, telemetry

import asyncio
import hashlib
import json
import logging
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

import httpx

from .domain import Attachment, RuntimeEvent, RuntimeResult
from .sandbox import Sandbox


logger = logging.getLogger(__name__)


class RuntimeStreamInterrupted(RuntimeError):
    """The existing execution could not be reattached; never replay its input."""


class RuntimeTransferError(ValueError):
    """A Runtime object could not cross the bounded durability boundary."""

    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


@dataclass(frozen=True)
class StagedRuntimeObject:
    path: Path
    size: int
    sha256: str
    content_type: str = "application/octet-stream"

    def remove(self) -> None:
        self.path.unlink(missing_ok=True)


def _process_start(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/stat").read_text().split()[21]
    except (OSError, IndexError):
        return ""


def _reap_stale_downloads(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for path in root.glob(".runtime-download-*-*-*"):
        parts = path.name.split("-", 4)
        try:
            pid = int(parts[2]); started = parts[3]
        except (ValueError, IndexError):
            continue
        if _process_start(pid) != started:
            path.unlink(missing_ok=True)


class RuntimeClient:
    def __init__(self, *, timeout_seconds: int = 3600, reattach_attempts: int = 8,
                 reattach_max_delay_seconds: float = 4,
                 bundle_dir: Path | None = None, default_bundle_digest: str = "",
                 code_bundle_dir: Path | None = None, default_code_bundle_digest: str = "",
                 transport: httpx.AsyncBaseTransport | None = None,
                 staging_root: Path | None = None):
        self.timeout = httpx.Timeout(timeout_seconds, connect=20)
        self.reattach_attempts = reattach_attempts
        self.reattach_max_delay_seconds = reattach_max_delay_seconds
        self.bundle_dir = bundle_dir
        self.default_bundle_digest = default_bundle_digest
        self.code_bundle_dir = code_bundle_dir
        self.default_code_bundle_digest = default_code_bundle_digest
        self.transport = transport
        self.staging_root = (staging_root or Path.cwd() / ".data" / "tmp" / "runtime-downloads").resolve()
        _reap_stale_downloads(self.staging_root)
        self.process_start = _process_start(os.getpid()) or "unknown"

    async def _download(self, sandbox: Sandbox, path: str, *, maximum: int,
                        error_code: str, params: dict | None = None) -> StagedRuntimeObject:
        if maximum < 0:
            raise RuntimeTransferError(error_code)
        temporary: Path | None = None
        try:
            async with httpx.AsyncClient(base_url=sandbox.runtime_url,
                    headers={**sandbox.runtime_headers, **carrier()}, timeout=300,
                    trust_env=False, transport=self.transport) as client:
                async with client.stream("GET", path, params=params) as response:
                    response.raise_for_status()
                    raw_length = response.headers.get("content-length")
                    declared = None
                    if raw_length is not None:
                        try:
                            declared = int(raw_length)
                        except ValueError as exc:
                            raise RuntimeTransferError("runtime_content_length_invalid") from exc
                        if declared < 0:
                            raise RuntimeTransferError("runtime_content_length_invalid")
                        if declared > maximum:
                            raise RuntimeTransferError(error_code)
                    with NamedTemporaryFile(dir=self.staging_root,
                            prefix=f".runtime-download-{os.getpid()}-{self.process_start}-",
                            delete=False) as stream:
                        temporary = Path(stream.name); size = 0; digest = hashlib.sha256()
                        async for chunk in response.aiter_raw():
                            if size + len(chunk) > maximum:
                                raise RuntimeTransferError(error_code)
                            stream.write(chunk); digest.update(chunk); size += len(chunk)
                        if declared is not None and size != declared:
                            raise RuntimeTransferError("runtime_transfer_size_mismatch")
                        stream.flush(); os.fsync(stream.fileno())
                    return StagedRuntimeObject(path=temporary, size=size,
                        sha256=digest.hexdigest(),
                        content_type=response.headers.get("content-type", "application/octet-stream"))
        except BaseException:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise

    @observed("runtime.configure")
    async def inject(self, sandbox: Sandbox, *, capability_token: str, gateway_url: str, proxy_url: str, harness: str, model: str,
                     model_start_timeout_seconds: float = 150, event_idle_timeout_seconds: float = 900,
                     defer_skill_install: bool = False, runtime_bundle_digest: str = "",
                     runtime_code_bundle_digest: str = "", runtime_profile_id: str = "",
                     skill_ids: list[str] | None = None, skill_digests: dict[str, str] | None = None,
                     child_mode: bool = False, delegation_depth: int = 0) -> None:
        bundle_digest = runtime_bundle_digest or self.default_bundle_digest
        code_digest = runtime_code_bundle_digest or self.default_code_bundle_digest
        payload = {"capability_token": capability_token, "gateway_url": gateway_url,
            "harness": harness, "model": model, "proxy_url": proxy_url,
            "model_start_timeout_seconds": model_start_timeout_seconds,
            "event_idle_timeout_seconds": event_idle_timeout_seconds,
            "defer_skill_install": defer_skill_install,
            "runtime_bundle_digest": bundle_digest,
            "runtime_profile_id":runtime_profile_id,"skill_ids":skill_ids or [],
            "skill_digests":skill_digests or {},"child_mode":child_mode,
            "delegation_depth":delegation_depth,
            "thread_id":sandbox.thread_id or sandbox.metadata.get("sandbox-harness.io/thread", ""),
            "turn_id":sandbox.turn_id or sandbox.metadata.get("sandbox-harness.io/turn", ""),"sandbox_id":sandbox.id}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=20,
            trust_env=False, transport=self.transport) as client:
            if code_digest:
                if not self.code_bundle_dir:
                    raise RuntimeError("runtime_code_bundle_directory_missing")
                code_path = self.code_bundle_dir / (code_digest + ".tar")
                code = await asyncio.to_thread(code_path.read_bytes)
                signature = await asyncio.to_thread(
                    (self.code_bundle_dir / (code_digest + ".sig")).read_bytes)
                if hashlib.sha256(code).hexdigest() != code_digest:
                    raise RuntimeError("runtime_code_bundle_digest_mismatch")
                status = await client.get("/bootstrap/code-bundle")
                status.raise_for_status()
                if status.json().get("sha256") != code_digest:
                    response = await client.put("/bootstrap/code-bundle", params={"sha256": code_digest},
                        content=code, headers={"Content-Type": "application/x-tar",
                            "X-Runtime-Bundle-Signature": signature.hex()})
                    response.raise_for_status()
            if bundle_digest:
                if not self.bundle_dir:
                    raise RuntimeError("runtime_bundle_directory_missing")
                bundle_path = self.bundle_dir / (bundle_digest + ".tar")
                bundle = await asyncio.to_thread(bundle_path.read_bytes)
                signature = await asyncio.to_thread(
                    (self.bundle_dir / (bundle_digest + ".sig")).read_bytes)
                if hashlib.sha256(bundle).hexdigest() != bundle_digest:
                    raise RuntimeError("runtime_bundle_digest_mismatch")
                status = await client.get("/v1/runtime-bundle")
                status.raise_for_status()
                if status.json().get("sha256") != bundle_digest:
                    response = await client.put("/v1/runtime-bundle", params={"sha256": bundle_digest},
                        content=bundle, headers={"Content-Type": "application/x-tar",
                            "X-Runtime-Bundle-Signature": signature.hex()})
                    response.raise_for_status()
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
    async def restore(self, sandbox: Sandbox, archive, *, size: int) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers,**carrier()}, timeout=120,
                trust_env=False, transport=self.transport) as client:
            response = await client.put("/v1/checkpoint", content=archive,
                headers={"Content-Type": "application/x-tar", "Content-Length": str(size)})
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

    async def put_project_file_stream(self, sandbox: Sandbox, path: str, content,
                                      *, size: int, mime: str) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers,**carrier()},timeout=300,
                trust_env=False,transport=self.transport) as client:
            response=await client.put("/v1/project-files/file",params={"path":path},content=content,
                headers={"Content-Type":mime,"Content-Length":str(size)})
            response.raise_for_status()
            return response.json()

    async def reset_project_files(self, sandbox: Sandbox) -> None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=60, trust_env=False) as client:
            response = await client.post("/v1/project-files/reset")
            response.raise_for_status()

    async def project_file_manifest(self, sandbox: Sandbox) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=300,
            trust_env=False, transport=self.transport) as client:
            response = await client.get("/v1/project-files/manifest")
            response.raise_for_status()
            return response.json()

    async def managed_project_file(self, sandbox: Sandbox, path: str, *,
                                   maximum: int) -> StagedRuntimeObject:
        return await self._download(sandbox, "/v1/project-files/file", params={"path": path},
            maximum=maximum, error_code="artifact_too_large")

    async def preserve_project_files(self, sandbox: Sandbox, paths: list[str], destination: str) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=300, trust_env=False) as client:
            response = await client.post("/v1/project-files/preserve",
                json={"paths": paths, "destination": destination})
            response.raise_for_status()
            return response.json()

    async def run(self, sandbox: Sandbox, *, message: str, attachments: list[Attachment], native_thread_id: str, reasoning_effort: str,
                  execution_id: str = "legacy", input_id: str = "initial", after_sequence: int = 0) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": native_thread_id, "reasoning_effort": reasoning_effort,
                "execution_id": execution_id, "input_id": input_id, "after_sequence": after_sequence}
        async for value in self._execution_stream(sandbox, execution_id, after_sequence, body=body):
            yield value

    async def start_execution(self, sandbox: Sandbox, *, message: str,
                              attachments: list[Attachment], native_thread_id: str,
                              reasoning_effort: str, execution_id: str,
                              input_id: str = "initial") -> dict:
        body = {"message":message,"attachments":[item.model_dump(mode="json") for item in attachments],
            "native_thread_id":native_thread_id,"reasoning_effort":reasoning_effort,
            "execution_id":execution_id,"input_id":input_id,"after_sequence":0}
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers,**carrier()},timeout=30,trust_env=False,
            transport=self.transport) as client:
            response=await client.post("/v1/executions/start",json=body)
            response.raise_for_status();return response.json()

    async def _execution_stream(self, sandbox: Sandbox, execution_id: str, after_sequence: int,
                                *, body: dict | None = None):
        cursor = after_sequence
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=self.timeout, trust_env=False, transport=self.transport) as client:
            for attempt in range(self.reattach_attempts):
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
                    status_code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
                    event("runtime.transport.interrupted",level=logging.WARNING,execution_id=execution_id,
                        attempt=attempt,runtime_sequence=cursor,http_status_code=status_code,exc=exc)
                    logger.warning("Runtime transport interrupted execution=%s cursor=%s attempt=%s error=%s",
                        execution_id, cursor, attempt + 1, type(exc).__name__)
                if attempt < self.reattach_attempts - 1:
                    event("runtime.stream.reattach",level=logging.WARNING,execution_id=execution_id,attempt=attempt,runtime_sequence=cursor)
                    logger.warning("Reattaching runtime stream execution=%s cursor=%s attempt=%s",
                        execution_id, cursor, attempt + 1)
                    await asyncio.sleep(min(0.5 * (2 ** attempt), self.reattach_max_delay_seconds))
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
    async def respond_interaction(self, sandbox: Sandbox, interaction_id: str,
                                  response_value: dict) -> bool | None:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/interactions/{interaction_id}", json=response_value)
            if response.status_code == 409:
                try:detail=response.json().get("detail")
                except ValueError:detail=""
                if detail=="interaction_delivery_unknown":return None
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

    async def inspect_overview(self, sandbox: Sandbox) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/overview")

    async def inspect_files(self, sandbox: Sandbox, path: str, depth: int = 1) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/files",
            params={"path": path, "depth": depth})

    async def inspect_processes(self, sandbox: Sandbox) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/processes")

    async def stop_inspect_process(self, sandbox: Sandbox, pid: int) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/inspect/processes/{pid}/stop")
            response.raise_for_status()
            return response.json()

    async def inspect_ports(self, sandbox: Sandbox) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/ports")

    async def inspect_file(self, sandbox: Sandbox, path: str) -> tuple[bytes, str]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=30, trust_env=False) as client:
            response = await client.get("/v1/inspect/file", params={"path": path})
            response.raise_for_status()
            return response.content, response.headers.get("content-type", "application/octet-stream")

    async def write_inspect_file(self, sandbox: Sandbox, path: str, data: bytes,
                                 mime: str = "text/plain") -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=30, trust_env=False) as client:
            response = await client.put("/v1/inspect/file", params={"path": path}, content=data,
                headers={"Content-Type": mime})
            response.raise_for_status()
            return response.json()

    async def _inspect_json(self, sandbox: Sandbox, path: str, *, params: dict | None = None) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=20, trust_env=False) as client:
            response = await client.get(path, params=params)
            response.raise_for_status()
            return response.json()

    async def resume(self, sandbox: Sandbox, execution_id: str, after_sequence: int) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        async for value in self._execution_stream(sandbox, execution_id, after_sequence):
            yield value

    @observed("workspace.checkpoint")
    async def checkpoint(self, sandbox: Sandbox, *, maximum: int) -> StagedRuntimeObject:
        return await self._download(sandbox, "/v1/checkpoint", maximum=maximum,
            error_code="checkpoint_too_large")

    async def artifacts(self, sandbox: Sandbox, expected_batch_id: str) -> list[dict]:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=60, trust_env=False, transport=self.transport) as client:
            response = await client.get("/v1/artifacts")
            if response.status_code == 413:
                try:
                    detail = response.json().get("detail")
                except (ValueError, AttributeError):
                    detail = ""
                if detail == "artifact_count_limit_exceeded":
                    raise RuntimeTransferError("artifact_count_limit_exceeded")
            response.raise_for_status()
            value = response.json()
            if value.get("batch_id") != expected_batch_id:
                raise ValueError("Runtime output batch does not belong to this Turn")
            if value.get("truncated") is not False:
                raise RuntimeTransferError("artifact_count_limit_exceeded")
            if not isinstance(value.get("items"), list) or len(value["items"]) > 50_000:
                raise RuntimeTransferError("artifact_count_limit_exceeded")
            return value["items"]

    @observed("artifact.download")
    async def artifact(self, sandbox: Sandbox, path: str, *, maximum: int) -> StagedRuntimeObject:
        return await self._download(sandbox, "/v1/artifacts/file", params={"path": path},
            maximum=maximum, error_code="artifact_too_large")

    async def media_render(self,sandbox:Sandbox,value:dict)->dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers,**carrier()},timeout=None,trust_env=False,
            transport=self.transport) as client:
            response=await client.post('/v1/operations/media-render',json=value)
            response.raise_for_status();return response.json()

    async def diagnostics(self, sandbox: Sandbox, after: int = 0) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,headers={**sandbox.runtime_headers,**carrier()},
            timeout=5,trust_env=False) as client:
            response=await client.get("/v1/diagnostics",params={"after":after,"limit":500})
            response.raise_for_status()
            if len(response.content)>4*1024*1024:raise ValueError("telemetry_snapshot_too_large")
            return response.json()

    @observed("runtime.cancel")
    async def cancel(self, sandbox: Sandbox) -> dict:
        async with httpx.AsyncClient(base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=10, trust_env=False) as client:
            response = await client.post("/v1/cancel")
            response.raise_for_status()
            return response.json()
