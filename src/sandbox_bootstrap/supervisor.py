from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import sys
import tarfile

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
from starlette.background import BackgroundTask


PUBLIC_KEY = Path("/opt/bootstrap/bundle_public_key.pem")
DEPENDENCY_LOCK = Path("/opt/bootstrap/requirements-runtime.txt")
CHILD_PORT = 18080
MAX_CODE_BUNDLE_BYTES = 16 * 1024 * 1024
MAX_CODE_FILES = 512
MAX_CODE_FILE_BYTES = 2 * 1024 * 1024
CLI_MODULES = {"sandbox-media": "sandbox_runtime.media_cli", "sandbox-site": "sandbox_runtime.site_cli",
    "sandbox-background": "sandbox_runtime.background_cli", "sandbox-interact": "sandbox_runtime.interaction_cli",
    "sandbox-job":"sandbox_runtime.job_cli","sandbox-artifact":"sandbox_runtime.artifact_cli",
    "sandbox-review":"sandbox_runtime.review_cli","editppt": "editppt.cli"}


def runtime_state_dir() -> Path:
    configured = os.environ.get("SANDBOX_RUNTIME_STATE_DIR", "")
    root = (Path(configured) if configured else
        Path(os.environ.get("TMPDIR", "/tmp")) / f"sandbox-harness-runtime-{os.geteuid()}")
    try:
        if root.is_symlink():
            raise RuntimeError("runtime_state_directory_unavailable")
        root.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = root.stat()
        if (not root.is_dir() or info.st_uid != os.geteuid()
                or not os.access(root, os.W_OK | os.X_OK)):
            raise RuntimeError("runtime_state_directory_unavailable")
        root.chmod(0o700)
    except OSError as exc:
        raise RuntimeError("runtime_state_directory_unavailable") from exc
    return root


def verify_signature(data: bytes, signature: bytes) -> None:
    try:
        key = serialization.load_pem_public_key(PUBLIC_KEY.read_bytes()); key.verify(signature, data)
    except (OSError, ValueError, TypeError, InvalidSignature) as exc:
        raise ValueError("runtime_code_bundle_signature_invalid") from exc


def allowed_code_path(path: str) -> bool:
    value = PurePosixPath(path)
    return (len(value.parts) >= 3 and value.parts[0] == "site-packages"
        and value.parts[1] in {"sandbox_runtime", "sandbox_harness", "editppt"}
        and value.suffix == ".py")


def activate_code_bundle(data: bytes, expected_digest: str, *, state_dir: Path | None = None) -> dict:
    if len(data) > MAX_CODE_BUNDLE_BYTES: raise ValueError("runtime_code_bundle_too_large")
    digest = hashlib.sha256(data).hexdigest()
    if digest != expected_digest or len(expected_digest) != 64:
        raise ValueError("runtime_code_bundle_digest_mismatch")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        members = archive.getmembers()
        if len(members) > MAX_CODE_FILES: raise ValueError("runtime_code_bundle_file_limit")
        payloads = {}
        for member in members:
            path = PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or ".." in path.parts
                    or member.name != "manifest.json" and not allowed_code_path(member.name)
                    or member.size > MAX_CODE_FILE_BYTES):
                raise ValueError("runtime_code_bundle_member_invalid")
            stream = archive.extractfile(member); payloads[member.name] = stream.read() if stream else b""
    try:
        manifest = json.loads(payloads.pop("manifest.json"))
        declared = {row["path"]: row for row in manifest["files"]}
        if (manifest.get("format_version") != 1 or manifest.get("bootstrap_protocol") != 1
                or manifest.get("python_abi") != "cp313"
                or manifest.get("dependency_lock_digest") != hashlib.sha256(DEPENDENCY_LOCK.read_bytes()).hexdigest()):
            raise ValueError
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("runtime_code_bundle_manifest_invalid") from exc
    if set(declared) != set(payloads): raise ValueError("runtime_code_bundle_manifest_mismatch")
    for path, content in payloads.items():
        row = declared[path]
        if row.get("size") != len(content) or row.get("sha256") != hashlib.sha256(content).hexdigest():
            raise ValueError("runtime_code_bundle_file_digest_mismatch")
    root = (state_dir or runtime_state_dir()) / "code-releases"
    root.mkdir(parents=True, exist_ok=True)
    target = root / digest
    if not target.exists():
        temporary = root / ("." + digest + ".tmp")
        shutil.rmtree(temporary, ignore_errors=True); temporary.mkdir(parents=True, mode=0o700)
        try:
            for path, content in payloads.items():
                destination = temporary.joinpath(*PurePosixPath(path).parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content); destination.chmod(0o444)
            bin_dir = temporary / "bin"; bin_dir.mkdir()
            for name, module in CLI_MODULES.items():
                shim = bin_dir / name
                shim.write_text("#!/bin/sh\nrelease=$(CDPATH= cd -- \"$(dirname -- \"$0\")/..\" && pwd)\n"
                    f"PYTHONPATH=\"$release/site-packages\" exec {sys.executable} -m {module} \"$@\"\n")
                shim.chmod(0o555)
            (temporary / "manifest.json").write_text(json.dumps(manifest, sort_keys=True));
            (temporary / "manifest.json").chmod(0o444)
            for directory in sorted((item for item in temporary.rglob("*") if item.is_dir()), reverse=True):
                directory.chmod(0o555)
            temporary.chmod(0o555); temporary.replace(target)
        finally: shutil.rmtree(temporary, ignore_errors=True)
    current = root / "current"; replacement = root / ".current.tmp"
    replacement.unlink(missing_ok=True); replacement.symlink_to(target.name, target_is_directory=True)
    replacement.replace(current)
    return {"sha256": digest, "root": str(target),
        "dependency_lock_digest": manifest["dependency_lock_digest"], "files": len(payloads)}


class Supervisor:
    def __init__(self):
        self.state_dir = runtime_state_dir()
        self.process: asyncio.subprocess.Process | None = None
        self.digest = ""; self.lock = asyncio.Lock(); self.monitor: asyncio.Task | None = None
        self.transitioning = False; self.closed = False

    async def child_health(self) -> dict | None:
        if not self.process or self.process.returncode is not None: return None
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{CHILD_PORT}", timeout=2,
                    trust_env=False) as client:
                response = await client.get("/health")
                return response.json() if response.is_success else None
        except (httpx.HTTPError, ValueError): return None

    async def stop(self) -> None:
        self.transitioning = True
        monitor = self.monitor; self.monitor = None
        if monitor and monitor is not asyncio.current_task():
            monitor.cancel(); await asyncio.gather(monitor, return_exceptions=True)
        process = self.process; self.process = None
        try:
            if process and process.returncode is None:
                try: os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError: return
                try: await asyncio.wait_for(process.wait(), 10)
                except asyncio.TimeoutError:
                    try: os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    await process.wait()
        finally: self.transitioning = False

    async def _monitor(self, process: asyncio.subprocess.Process, release: Path, digest: str) -> None:
        await process.wait()
        if self.closed or self.transitioning or self.process is not process: return
        self.process = None
        await asyncio.sleep(.5)
        async with self.lock:
            if not self.closed and self.digest == digest and self.process is None:
                await self._spawn(release, digest)

    async def _spawn(self, release: Path, digest: str) -> None:
        environment = {**os.environ, "PYTHONPATH": str(release / "site-packages"),
            "PATH": str(release / "bin") + ":" + os.environ.get("PATH", ""),
            "SANDBOX_RUNTIME_PORT": str(CHILD_PORT), "SANDBOX_RUNTIME_CODE_DIGEST": digest,
            "SANDBOX_RUNTIME_STATE_DIR":str(self.state_dir)}
        process = await asyncio.create_subprocess_exec(sys.executable, "-m", "sandbox_runtime.main",
            env=environment, start_new_session=True)
        self.process = process
        for _ in range(100):
            if process.returncode is not None: raise RuntimeError("runtime_child_exited")
            if await self.child_health(): break
            await asyncio.sleep(.05)
        else: raise RuntimeError("runtime_child_start_timeout")
        self.monitor = asyncio.create_task(self._monitor(process, release, digest),
            name="runtime-child-monitor")

    async def start(self, release: Path, digest: str) -> None:
        await self.stop()
        self.digest = digest
        await self._spawn(release, digest)

    async def close(self) -> None:
        self.closed = True; await self.stop()

    async def activate(self, data: bytes, digest: str, signature: bytes) -> dict:
        async with self.lock:
            if digest == self.digest and await self.child_health():
                return {"sha256": digest, "reused": True}
            health = await self.child_health()
            if health and health.get("execution") == "running":
                raise RuntimeError("runtime_execution_active")
            verify_signature(data, signature)
            value = await asyncio.to_thread(activate_code_bundle, data, digest,
                state_dir=self.state_dir)
            await self.start(Path(value["root"]), digest)
            return {**value, "reused": False}

    async def restore(self) -> None:
        current = self.state_dir / "code-releases/current"
        if current.is_symlink() and current.resolve().is_dir():
            await self.start(current.resolve(), current.resolve().name)


def create_app() -> FastAPI:
    supervisor = Supervisor()
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await supervisor.restore(); yield; await supervisor.close()
    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health():
        child = await supervisor.child_health()
        return ({**child, "runtime_code_bundle_digest": supervisor.digest} if child else
            {"status": "bootstrap", "runtime_code_bundle_digest": supervisor.digest})

    @app.get("/bootstrap/code-bundle")
    async def code_bundle_status(): return {"sha256": supervisor.digest}

    @app.put("/bootstrap/code-bundle")
    async def install_code_bundle(request: Request, sha256: str):
        if len(sha256) != 64 or any(value not in "0123456789abcdef" for value in sha256):
            raise HTTPException(400, "Invalid Runtime Code Bundle digest")
        data = await request.body()
        if len(data) > MAX_CODE_BUNDLE_BYTES: raise HTTPException(413, "Runtime Code Bundle too large")
        try:
            signature = bytes.fromhex(request.headers.get("X-Runtime-Bundle-Signature", ""))
            return await supervisor.activate(data, sha256, signature)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc))

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
    async def proxy(path: str, request: Request):
        if not await supervisor.child_health():
            return JSONResponse({"detail": "Runtime Code Bundle is not active"}, status_code=503)
        headers = {key: value for key, value in request.headers.items()
            if key.lower() not in {"host", "content-length", "connection"}}
        async def body():
            async for chunk in request.stream(): yield chunk
        client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{CHILD_PORT}", timeout=None, trust_env=False)
        upstream = client.build_request(request.method, "/" + path,
            params=request.query_params, headers=headers, content=body())
        try: response = await client.send(upstream, stream=True)
        except Exception:
            await client.aclose(); raise
        returned_headers = {key: value for key, value in response.headers.items()
            if key.lower() not in {"content-length", "connection", "transfer-encoding"}}
        async def close(): await response.aclose(); await client.aclose()
        return StreamingResponse(response.aiter_raw(), status_code=response.status_code,
            headers=returned_headers, background=BackgroundTask(close))
    app.state.supervisor = supervisor
    return app
