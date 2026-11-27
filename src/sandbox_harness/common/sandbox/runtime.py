from __future__ import annotations

from sandbox_harness.common.observability.observability import (observed, event, enrich, carrier, add_link, telemetry)
from sandbox_harness.common.observability.http import (observed_async_client)

import asyncio
import gzip
import hashlib
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

import httpx

from sandbox_harness.common.process.identity import (_process_start)
from sandbox_harness.common.contracts.domain import (Attachment, RuntimeEvent, RuntimeResult)
from sandbox_harness.common.sandbox.sandbox import (Sandbox)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxBackend)
from sandbox_harness.common.storage.directories import (ensure_private_directory)
from sandbox_harness.common.storage.objects import (ReplayableObjectChunks)


logger = logging.getLogger(__name__)
PROJECT_UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024


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




def _reap_stale_downloads(root: Path) -> None:
    ensure_private_directory(root,boundary=root.parent)
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
                 request_retry_attempts: int = 4,
                 request_retry_initial_delay_seconds: float = 1,
                 request_retry_backoff_multiplier: float = 2,
                 request_retry_max_delay_seconds: float = 10,
                 bundle_dir: Path | None = None, default_bundle_digest: str = "",
                 code_bundle_dir: Path | None = None, default_code_bundle_digest: str = "",
                 transport: httpx.AsyncBaseTransport | None = None,
                 staging_root: Path | None = None,backend:SandboxBackend|None=None):
        self.timeout = httpx.Timeout(timeout_seconds, connect=20)
        self.reattach_attempts = reattach_attempts
        self.reattach_max_delay_seconds = reattach_max_delay_seconds
        self.request_retry_attempts=max(1,int(request_retry_attempts))
        self.request_retry_initial_delay_seconds=max(0.1,float(
            request_retry_initial_delay_seconds))
        self.request_retry_backoff_multiplier=max(1,float(request_retry_backoff_multiplier))
        self.request_retry_max_delay_seconds=max(0.1,float(request_retry_max_delay_seconds))
        self.bundle_dir = bundle_dir
        self.default_bundle_digest = default_bundle_digest
        self.code_bundle_dir = code_bundle_dir
        self.default_code_bundle_digest = default_code_bundle_digest
        self.transport = transport
        self.backend=backend
        self._endpoint_refresh:set[str]=set()
        self.staging_root = (staging_root or Path.cwd() / ".data" / "tmp" / "runtime-downloads").resolve()
        self.process_start = _process_start(os.getpid()) or "unknown"

    async def _resolve_endpoint(self,sandbox:Sandbox)->None:
        if self.backend is None:return
        force_refresh=sandbox.id in self._endpoint_refresh
        endpoint=await self.backend.endpoint(sandbox,8080,force_refresh=force_refresh)
        self._endpoint_refresh.discard(sandbox.id)
        sandbox.runtime_url=endpoint.url;sandbox.runtime_headers=dict(endpoint.headers)

    def _raise_for_status(self,sandbox:Sandbox,response:httpx.Response)->None:
        if self.backend is not None and response.status_code in {401,403}:
            self._endpoint_refresh.add(sandbox.id)
        response.raise_for_status()

    def _request_retry_delay(self,attempt:int)->float:
        return min(self.request_retry_max_delay_seconds,
            self.request_retry_initial_delay_seconds*(
                self.request_retry_backoff_multiplier**attempt))

    async def _request_with_startup_retry(self,client:httpx.AsyncClient,method:str,
                                          path:str,**kwargs)->httpx.Response:
        """Retry idempotent Runtime reads/uploads, never execution submission."""
        retryable={429,502,503,504}
        for attempt in range(self.request_retry_attempts):
            try:
                response=await client.request(method,path,**kwargs)
                if (response.status_code not in retryable
                        or attempt==self.request_retry_attempts-1):return response
            except (httpx.TimeoutException,httpx.TransportError):
                if attempt==self.request_retry_attempts-1:raise
            await asyncio.sleep(self._request_retry_delay(attempt))
        raise RuntimeError("unreachable")

    async def _download(self, sandbox: Sandbox, path: str, *, maximum: int,
                        error_code: str, params: dict | None = None) -> StagedRuntimeObject:
        await self._resolve_endpoint(sandbox)
        if maximum < 0:
            raise RuntimeTransferError(error_code)
        _reap_stale_downloads(self.staging_root)
        for attempt in range(self.request_retry_attempts):
            temporary:Path|None=None
            try:
                async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                        headers={**sandbox.runtime_headers, **carrier()}, timeout=300,
                        trust_env=False, transport=self.transport) as client:
                    async with client.stream("GET", path, params=params) as response:
                        self._raise_for_status(sandbox,response)
                        raw_length=response.headers.get("content-length");declared=None
                        if raw_length is not None:
                            try:declared=int(raw_length)
                            except ValueError as exc:
                                raise RuntimeTransferError("runtime_content_length_invalid") from exc
                            if declared<0:raise RuntimeTransferError("runtime_content_length_invalid")
                            if declared>maximum:raise RuntimeTransferError(error_code)
                        with NamedTemporaryFile(dir=self.staging_root,
                                prefix=f".runtime-download-{os.getpid()}-{self.process_start}-",
                                delete=False) as stream:
                            temporary=Path(stream.name);size=0;digest=hashlib.sha256()
                            async for chunk in response.aiter_raw():
                                if size+len(chunk)>maximum:raise RuntimeTransferError(error_code)
                                stream.write(chunk);digest.update(chunk);size+=len(chunk)
                            if declared is not None and size!=declared:
                                raise RuntimeTransferError("runtime_transfer_size_mismatch")
                            stream.flush();os.fsync(stream.fileno())
                        return StagedRuntimeObject(path=temporary,size=size,
                            sha256=digest.hexdigest(),content_type=response.headers.get(
                                "content-type","application/octet-stream"))
            except (httpx.TimeoutException,httpx.TransportError):
                if temporary is not None:temporary.unlink(missing_ok=True)
                if attempt==self.request_retry_attempts-1:raise
                await asyncio.sleep(self._request_retry_delay(attempt))
            except BaseException:
                if temporary is not None:temporary.unlink(missing_ok=True)
                raise
        raise RuntimeError("unreachable")

    async def _install_bundles(self, sandbox: Sandbox, client: httpx.AsyncClient, *,
                               code_digest: str, bundle_digest: str) -> None:
        if code_digest:
            if not self.code_bundle_dir:
                raise RuntimeError("runtime_code_bundle_directory_missing")
            code_path = self.code_bundle_dir / (code_digest + ".tar")
            code = await asyncio.to_thread(code_path.read_bytes)
            signature = await asyncio.to_thread(
                (self.code_bundle_dir / (code_digest + ".sig")).read_bytes)
            if hashlib.sha256(code).hexdigest() != code_digest:
                raise RuntimeError("runtime_code_bundle_digest_mismatch")
            status = await self._request_with_startup_retry(client,"GET","/bootstrap/code-bundle")
            self._raise_for_status(sandbox,status)
            if status.json().get("sha256") != code_digest:
                response = await self._request_with_startup_retry(client,"PUT","/bootstrap/code-bundle", params={"sha256": code_digest},
                    content=code, headers={"Content-Type": "application/x-tar",
                        "X-Runtime-Bundle-Signature": signature.hex()})
                self._raise_for_status(sandbox,response)
        if bundle_digest:
            if not self.bundle_dir:
                raise RuntimeError("runtime_bundle_directory_missing")
            bundle_path = self.bundle_dir / (bundle_digest + ".tar")
            bundle = await asyncio.to_thread(bundle_path.read_bytes)
            signature = await asyncio.to_thread(
                (self.bundle_dir / (bundle_digest + ".sig")).read_bytes)
            if hashlib.sha256(bundle).hexdigest() != bundle_digest:
                raise RuntimeError("runtime_bundle_digest_mismatch")
            status = await self._request_with_startup_retry(client,"GET","/v1/runtime-bundle")
            self._raise_for_status(sandbox,status)
            if status.json().get("sha256") != bundle_digest:
                compressed = await asyncio.to_thread(
                    gzip.compress, bundle, 6, mtime=0)
                response = await self._request_with_startup_retry(client,"PUT","/v1/runtime-bundle", params={"sha256": bundle_digest},
                    content=compressed, headers={"Content-Type": "application/x-tar",
                        "Content-Encoding": "gzip",
                        "X-Uncompressed-Content-Length": str(len(bundle)),
                        "X-Runtime-Bundle-Signature": signature.hex()})
                self._raise_for_status(sandbox,response)

    @observed("runtime.prepare")
    async def prepare(self, sandbox: Sandbox) -> None:
        """Activate signed Runtime code/content without granting execution access."""
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers, **carrier()}, timeout=20,
                trust_env=False, transport=self.transport) as client:
            await self._install_bundles(sandbox, client,
                code_digest=self.default_code_bundle_digest,
                bundle_digest=self.default_bundle_digest)

    @observed("runtime.configure")
    async def inject(self, sandbox: Sandbox, *, capability_token: str, gateway_url: str, proxy_url: str, harness: str, model: str,
                     model_start_timeout_seconds: float = 150, event_idle_timeout_seconds: float = 900,
                     defer_skill_install: bool = False, runtime_bundle_digest: str = "",
                     runtime_code_bundle_digest: str = "", runtime_profile_id: str = "",
                     skill_ids: list[str] | None = None, skill_digests: dict[str, str] | None = None,
                     child_mode: bool = False, delegation_depth: int = 0,
                     telemetry_metrics_enabled: bool = False,
                     stderr_log_max_bytes: int = 1024,
                     logical_thread_id: str = "", logical_turn_id: str = "",
                     logical_job_id: str = "") -> None:
        await self._resolve_endpoint(sandbox)
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
            "telemetry_metrics_enabled":telemetry_metrics_enabled,
            "stderr_log_max_bytes":stderr_log_max_bytes,
            "thread_id":logical_thread_id or sandbox.thread_id,
            "turn_id":logical_turn_id or sandbox.turn_id,"sandbox_id":sandbox.id,
            "job_id":logical_job_id}
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=20,
            trust_env=False, transport=self.transport) as client:
            await self._install_bundles(sandbox, client,
                code_digest=code_digest, bundle_digest=bundle_digest)
            response = await self._request_with_startup_retry(
                client,"POST","/v1/configure",json=payload)
            self._raise_for_status(sandbox,response)

    @observed("workspace.restore")
    async def restore(self, sandbox: Sandbox, archive, *, size: int) -> None:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers,**carrier()}, timeout=120,
                trust_env=False, transport=self.transport) as client:
            response = await self._request_with_startup_retry(client,"PUT","/v1/checkpoint",
                content=archive,
                headers={"Content-Type": "application/x-tar", "Content-Length": str(size)})
            self._raise_for_status(sandbox,response)

    async def put_attachment(self, sandbox: Sandbox, name: str, data: bytes, mime: str) -> None:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=300, trust_env=False,transport=self.transport) as client:
            response = await self._request_with_startup_retry(client,"PUT","/v1/attachments/file",
                params={"name": name}, content=data,
                headers={"Content-Type": mime})
            self._raise_for_status(sandbox,response)

    async def put_project_file(self, sandbox: Sandbox, path: str, data: bytes, mime: str) -> None:
        if len(data) > PROJECT_UPLOAD_CHUNK_BYTES:
            async def chunks():
                for offset in range(0, len(data), PROJECT_UPLOAD_CHUNK_BYTES):
                    yield data[offset:offset + PROJECT_UPLOAD_CHUNK_BYTES]
            return await self._put_project_chunks(sandbox, path, chunks(), size=len(data), mime=mime)
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=300, trust_env=False,transport=self.transport) as client:
            response = await self._request_with_startup_retry(client,"PUT","/v1/project-files/file", params={"path": path,"size":len(data)},
                content=data,headers={"Content-Type": mime})
            self._raise_for_status(sandbox,response)

    async def put_project_file_stream(self, sandbox: Sandbox, path: str, content,
                                      *, size: int, mime: str) -> None:
        if size > PROJECT_UPLOAD_CHUNK_BYTES:
            return await self._put_project_chunks(sandbox, path, content, size=size, mime=mime)
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers,**carrier()},timeout=300,
                trust_env=False,transport=self.transport) as client:
            request=(self._request_with_startup_retry(client,"PUT","/v1/project-files/file",
                params={"path":path,"size":size},content=content,
                headers={"Content-Type":mime,"Content-Length":str(size)})
                if isinstance(content,ReplayableObjectChunks) else
                client.put("/v1/project-files/file",params={"path":path,"size":size},content=content,
                    headers={"Content-Type":mime,"Content-Length":str(size)}))
            response=await request
            self._raise_for_status(sandbox,response)
            return response.json()

    async def _put_project_chunks(self, sandbox: Sandbox, path: str, content,
                                  *, size: int, mime: str) -> dict:
        await self._resolve_endpoint(sandbox)
        upload_path = "/v1/project-files/uploads/" + uuid.uuid4().hex
        issued_at = int(time.time())
        digest = hashlib.sha256(); offset = 0; pending = bytearray()
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers, **carrier()}, timeout=300,
                trust_env=False, transport=self.transport) as client:
            async def send_chunk(body):
                nonlocal offset
                response = await self._request_with_startup_retry(client, "PUT",
                    upload_path + "/chunks/" + str(offset), content=body,
                    headers={"Content-Type": mime})
                self._raise_for_status(sandbox, response)
                if response.json() != {"bytes": len(body), "offset": offset}:
                    raise RuntimeTransferError("runtime_upload_chunk_receipt_mismatch")
                digest.update(body); offset += len(body)
            try:
                response = await self._request_with_startup_retry(client, "GET",
                    "/v1/project-files/uploads")
                self._raise_for_status(sandbox, response)
                epoch = response.json().get("epoch")
                if not isinstance(epoch, str) or len(epoch) != 32:
                    raise RuntimeTransferError("runtime_upload_epoch_invalid")
                async with asyncio.timeout(60):
                    response = await self._request_with_startup_retry(client, "PUT", upload_path,
                        params={"path": path, "size": size, "issued_at": issued_at, "epoch": epoch})
                self._raise_for_status(sandbox, response)
                chunk_size = response.json().get("chunk_bytes")
                if not isinstance(chunk_size, int) or not 0 < chunk_size <= PROJECT_UPLOAD_CHUNK_BYTES:
                    raise RuntimeTransferError("runtime_upload_chunk_limit_invalid")
                async for part in content:
                    if offset + len(pending) + len(part) > size:
                        raise RuntimeTransferError("runtime_transfer_size_mismatch")
                    view = memoryview(part)
                    while view:
                        length = min(chunk_size - len(pending), len(view))
                        pending.extend(view[:length]); view = view[length:]
                        if len(pending) == chunk_size:
                            await send_chunk(bytes(pending)); pending.clear()
                if pending:
                    await send_chunk(bytes(pending))
                if offset != size:
                    raise RuntimeTransferError("runtime_transfer_size_mismatch")
                response = await self._request_with_startup_retry(client, "POST",
                    upload_path + "/commit", params={"sha256": digest.hexdigest()})
                self._raise_for_status(sandbox, response)
                result = response.json()
                if result.get("bytes") != size or result.get("sha256") != digest.hexdigest():
                    raise RuntimeTransferError("runtime_upload_receipt_mismatch")
                return result
            finally:
                # Abort is idempotent and leaves an already committed file intact.
                # Bound cleanup separately from the transfer timeout.
                try:
                    await client.delete(upload_path, timeout=5)
                except httpx.HTTPError:
                    pass

    async def reset_project_files(self, sandbox: Sandbox) -> None:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=60, trust_env=False,transport=self.transport) as client:
            response = await self._request_with_startup_retry(
                client,"POST","/v1/project-files/reset")
            self._raise_for_status(sandbox,response)

    async def project_file_manifest(self, sandbox: Sandbox) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=300,
            trust_env=False, transport=self.transport) as client:
            response = await self._request_with_startup_retry(
                client,"GET","/v1/project-files/manifest")
            self._raise_for_status(sandbox,response)
            return response.json()

    async def managed_project_file(self, sandbox: Sandbox, path: str, *,
                                   maximum: int) -> StagedRuntimeObject:
        return await self._download(sandbox, "/v1/project-files/file", params={"path": path},
            maximum=maximum, error_code="artifact_too_large")

    async def preserve_project_files(self, sandbox: Sandbox, paths: list[str], destination: str) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=300, trust_env=False,
            transport=self.transport) as client:
            response = await self._request_with_startup_retry(client,"POST",
                "/v1/project-files/preserve",
                json={"paths": paths, "destination": destination})
            self._raise_for_status(sandbox,response)
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
        await self._resolve_endpoint(sandbox)
        body = {"message":message,"attachments":[item.model_dump(mode="json") for item in attachments],
            "native_thread_id":native_thread_id,"reasoning_effort":reasoning_effort,
            "execution_id":execution_id,"input_id":input_id,"after_sequence":0}
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers,**carrier()},timeout=30,trust_env=False,
            transport=self.transport) as client:
            response=await client.post("/v1/executions/start",json=body)
            self._raise_for_status(sandbox,response);return response.json()

    async def _execution_stream(self, sandbox: Sandbox, execution_id: str, after_sequence: int,
                                *, body: dict | None = None):
        await self._resolve_endpoint(sandbox)
        cursor = after_sequence
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=self.timeout, trust_env=False, transport=self.transport) as client:
            for attempt in range(self.reattach_attempts):
                try:
                    # A lost POST reply is an unknown submission. Every retry only reads
                    # the same execution; it must never start another Harness invocation.
                    method = "POST" if attempt == 0 and body is not None else "GET"
                    path = "/v1/turns" if method == "POST" else f"/v1/executions/{execution_id}/events"
                    kwargs = {"json": body} if method == "POST" else {"params": {"after": cursor}}
                    async with client.stream(method, path, **kwargs) as response:
                        self._raise_for_status(sandbox,response)
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
                        attempt=attempt,runtime_sequence=cursor,exc=exc,
                        **{'http.response.status_code':status_code})
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
        await self._resolve_endpoint(sandbox)
        body = {"message": message, "attachments": [item.model_dump(mode="json") for item in attachments],
                "native_thread_id": "", "reasoning_effort": reasoning_effort,
                "execution_id": execution_id, "input_id": input_id}
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.post("/v1/inputs", json=body)
            if response.status_code == 409:
                return ""
            self._raise_for_status(sandbox,response)
            value = response.json()
            if not value.get("accepted"):
                return ""
            return {"codex": "native_steer", "claude": "native_stream",
                "deepseek": "native_safe_point"}[sandbox.harness.value]

    async def execution_status(self, sandbox: Sandbox, execution_id: str) -> dict | None:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False,transport=self.transport) as client:
            response = await client.get("/v1/executions/" + execution_id)
            if response.status_code == 404:
                return None
            self._raise_for_status(sandbox,response)
            return response.json()

    @observed("runtime.interaction")
    async def respond_interaction(self, sandbox: Sandbox, interaction_id: str,
                                  response_value: dict) -> bool | None:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/interactions/{interaction_id}", json=response_value)
            if response.status_code == 409:
                try:detail=response.json().get("detail")
                except ValueError:detail=""
                if detail=="interaction_delivery_unknown":return None
                return False
            self._raise_for_status(sandbox,response)
            return bool(response.json().get("accepted"))

    async def background_jobs(self, sandbox: Sandbox) -> list[dict]:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.get("/v1/background-jobs")
            self._raise_for_status(sandbox,response)
            return response.json()["items"]

    async def cancel_background_job(self, sandbox: Sandbox, job_id: str) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/background-jobs/{job_id}/cancel")
            if response.status_code == 404:
                raise KeyError("background_job_not_found")
            self._raise_for_status(sandbox,response)
            return response.json()

    async def inspect_overview(self, sandbox: Sandbox) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/overview")

    async def inspect_files(self, sandbox: Sandbox, path: str, depth: int = 1) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/files",
            params={"path": path, "depth": depth})

    async def inspect_processes(self, sandbox: Sandbox) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/processes")

    async def stop_inspect_process(self, sandbox: Sandbox, pid: int) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=20, trust_env=False) as client:
            response = await client.post(f"/v1/inspect/processes/{pid}/stop")
            self._raise_for_status(sandbox,response)
            return response.json()

    async def inspect_ports(self, sandbox: Sandbox) -> dict:
        return await self._inspect_json(sandbox, "/v1/inspect/ports")

    async def create_terminal(self, sandbox: Sandbox, *, cwd: str,
                              command: str) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers, **carrier()}, timeout=20,
                trust_env=False, transport=self.transport) as client:
            response = await client.post("/v1/pty",
                json={"cwd": cwd, "command": command})
            self._raise_for_status(sandbox, response)
            return response.json()

    async def terminal_status(self, sandbox: Sandbox,
                              session_id: str) -> dict | None:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers, **carrier()}, timeout=20,
                trust_env=False, transport=self.transport) as client:
            response = await client.get(f"/v1/pty/{session_id}")
            if response.status_code == 404:
                return None
            self._raise_for_status(sandbox, response)
            return response.json()

    async def delete_terminal(self, sandbox: Sandbox, session_id: str) -> bool:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
                headers={**sandbox.runtime_headers, **carrier()}, timeout=20,
                trust_env=False, transport=self.transport) as client:
            response = await client.delete(f"/v1/pty/{session_id}")
            if response.status_code == 404:
                return False
            self._raise_for_status(sandbox, response)
            return True

    async def terminal_endpoint(self, sandbox: Sandbox, session_id: str, *,
                                since: int) -> tuple[str, dict[str, str]]:
        await self._resolve_endpoint(sandbox)
        endpoint = sandbox.runtime_url
        websocket_base = ("wss://" if endpoint.startswith("https://") else "ws://")
        websocket_base += endpoint.split("://", 1)[1]
        url = urljoin(websocket_base.rstrip("/") + "/",
            f"v1/pty/{session_id}/ws")
        return url + "?since=" + str(max(0, since)), dict(sandbox.runtime_headers)

    async def inspect_file(self, sandbox: Sandbox, path: str) -> tuple[bytes, str]:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=30, trust_env=False) as client:
            response = await client.get("/v1/inspect/file", params={"path": path})
            self._raise_for_status(sandbox,response)
            return response.content, response.headers.get("content-type", "application/octet-stream")

    async def write_inspect_file(self, sandbox: Sandbox, path: str, data: bytes,
                                 mime: str = "text/plain") -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=30, trust_env=False) as client:
            response = await client.put("/v1/inspect/file", params={"path": path}, content=data,
                headers={"Content-Type": mime})
            self._raise_for_status(sandbox,response)
            return response.json()

    async def _inspect_json(self, sandbox: Sandbox, path: str, *, params: dict | None = None) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers, **carrier()}, timeout=20, trust_env=False) as client:
            response = await client.get(path, params=params)
            self._raise_for_status(sandbox,response)
            return response.json()

    async def resume(self, sandbox: Sandbox, execution_id: str, after_sequence: int) -> AsyncIterator[RuntimeEvent | RuntimeResult]:
        async for value in self._execution_stream(sandbox, execution_id, after_sequence):
            yield value

    @observed("workspace.checkpoint")
    async def checkpoint(self, sandbox: Sandbox, *, maximum: int) -> StagedRuntimeObject:
        return await self._download(sandbox, "/v1/checkpoint", maximum=maximum,
            error_code="checkpoint_too_large")

    async def artifacts(self, sandbox: Sandbox, expected_batch_id: str) -> list[dict]:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()},
            timeout=60, trust_env=False, transport=self.transport) as client:
            response = await client.get("/v1/artifacts")
            if response.status_code == 413:
                try:
                    detail = response.json().get("detail")
                except (ValueError, AttributeError):
                    detail = ""
                if detail == "artifact_count_limit_exceeded":
                    raise RuntimeTransferError("artifact_count_limit_exceeded")
            self._raise_for_status(sandbox,response)
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
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,
            headers={**sandbox.runtime_headers,**carrier()},timeout=None,trust_env=False,
            transport=self.transport) as client:
            response=await client.post('/v1/operations/media-render',json=value)
            self._raise_for_status(sandbox,response);return response.json()

    async def diagnostics(self, sandbox: Sandbox, after: int = 0) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url,headers={**sandbox.runtime_headers,**carrier()},
            timeout=5,trust_env=False) as client:
            response=await client.get("/v1/diagnostics",params={"after":after,"limit":500})
            self._raise_for_status(sandbox,response)
            if len(response.content)>4*1024*1024:raise ValueError("telemetry_snapshot_too_large")
            return response.json()

    @observed("runtime.cancel")
    async def cancel(self, sandbox: Sandbox) -> dict:
        await self._resolve_endpoint(sandbox)
        async with observed_async_client('sandbox-runtime',base_url=sandbox.runtime_url, headers={**sandbox.runtime_headers,**carrier()}, timeout=10, trust_env=False) as client:
            response = await client.post("/v1/cancel")
            self._raise_for_status(sandbox,response)
            return response.json()
