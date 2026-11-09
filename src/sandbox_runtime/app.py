from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import os
from pathlib import Path, PurePosixPath
import signal
import tarfile
from tempfile import NamedTemporaryFile
from urllib.parse import quote, urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from .adapters import command, normalize


WORKSPACE = Path("/workspace")
PERSISTENT = ("project", "state")
OUTPUTS = WORKSPACE / "outputs"
RUN = Path("/run/harness")


class Configure(BaseModel):
    capability_token: str = Field(min_length=20)
    gateway_url: str = Field(pattern=r"^https?://")
    harness: str = Field(pattern=r"^(codex|claude|deepseek)$")
    model: str = Field(min_length=1, max_length=160)
    proxy_url: str = Field(pattern=r"^http://")


class TurnRequest(BaseModel):
    message: str = Field(min_length=1, max_length=200_000)
    attachments: list[dict] = Field(default_factory=list, max_length=64)
    native_thread_id: str = ""
    reasoning_effort: str = Field(default="high", pattern=r"^(minimal|low|medium|high|xhigh)$")


def safe_relative(value: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in ("", ".", "..") for part in pure.parts):
        raise ValueError("Unsafe artifact path")
    return Path(*pure.parts)


def create_app() -> FastAPI:
    app = FastAPI(title="Sandbox Harness Runtime")
    app.state.process = None
    for name in (*PERSISTENT, "inbox", "outputs", "tmp"):
        (WORKSPACE / name).mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/v1/configure")
    async def configure(value: Configure):
        token = RUN / "capability-token"
        temporary = RUN / ".capability-token.tmp"
        temporary.write_text(value.capability_token)
        os.chmod(temporary, 0o400)
        temporary.replace(token)
        os.environ["HARNESS_GATEWAY_URL"] = value.gateway_url
        os.environ["HARNESS_KIND"] = value.harness
        os.environ["HARNESS_MODEL"] = value.model
        proxy = urlsplit(value.proxy_url)
        authenticated_proxy = f"{proxy.scheme}://{quote(value.capability_token, safe='')}:@{proxy.netloc}"
        os.environ["HTTP_PROXY"] = authenticated_proxy
        os.environ["HTTPS_PROXY"] = authenticated_proxy
        os.environ["http_proxy"] = authenticated_proxy
        os.environ["https_proxy"] = authenticated_proxy
        no_proxy = ",".join(filter(None, {"127.0.0.1", "localhost", proxy.hostname, urlsplit(value.gateway_url).hostname}))
        os.environ["NO_PROXY"] = no_proxy
        os.environ["no_proxy"] = no_proxy
        return {"configured": True}

    @app.put("/v1/checkpoint")
    async def restore(request: Request):
        data = await request.body()
        if len(data) > 4 * 1024 * 1024 * 1024:
            raise HTTPException(413, "Checkpoint too large")
        with NamedTemporaryFile() as stream:
            stream.write(data); stream.flush()
            with tarfile.open(stream.name, "r:*") as archive:
                for member in archive.getmembers():
                    path = safe_relative(member.name)
                    if path.parts[0] not in PERSISTENT or member.issym() or member.islnk() or member.isdev():
                        raise HTTPException(400, "Invalid checkpoint member")
                archive.extractall(WORKSPACE, filter="data")
        return {"restored": True}

    @app.put("/v1/attachments/file")
    async def put_attachment(request: Request, name: str):
        if not name or Path(name).name != name or len(name) > 255:
            raise HTTPException(400, "Invalid attachment name")
        data = await request.body()
        if len(data) > 512 * 1024 * 1024:
            raise HTTPException(413, "Attachment too large")
        target = WORKSPACE / "inbox" / name
        temporary = target.with_name("." + target.name + ".tmp")
        temporary.write_bytes(data)
        os.chmod(temporary, 0o600)
        temporary.replace(target)
        return {"path": "/workspace/inbox/" + name, "bytes": len(data)}

    @app.get("/v1/checkpoint")
    async def checkpoint():
        target = WORKSPACE / "tmp" / "checkpoint.tar"
        with tarfile.open(target, "w") as archive:
            for name in PERSISTENT:
                archive.add(WORKSPACE / name, arcname=name, recursive=True)
        return FileResponse(target, media_type="application/x-tar")

    @app.post("/v1/turns")
    async def run_turn(value: TurnRequest):
        if app.state.process and app.state.process.returncode is None:
            raise HTTPException(409, "Harness is already running")
        harness = os.environ.get("HARNESS_KIND", "claude")
        model = os.environ.get("HARNESS_MODEL", "default")
        os.environ["HARNESS_REASONING_EFFORT"] = value.reasoning_effort
        selected = command(harness, model, value.native_thread_id)
        environment = {**os.environ, **selected.environment, "HOME": "/workspace/state/home", "PWD": "/workspace/project"}
        Path(environment["HOME"]).mkdir(parents=True, exist_ok=True)
        prompt = value.message
        if value.attachments:
            prompt += "\n\nAttached files are available in /workspace/inbox. Their immutable records are listed in attachments.json."
            (WORKSPACE / "inbox" / "attachments.json").write_text(json.dumps(value.attachments, ensure_ascii=False))

        async def stream():
            process = await asyncio.create_subprocess_exec(*selected.argv, cwd=WORKSPACE / "project", env=environment,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            app.state.process = process
            process.stdin.write(prompt.encode()); await process.stdin.drain(); process.stdin.close()
            native_thread_id = value.native_thread_id
            while line := await process.stdout.readline():
                event = normalize(harness, line.decode(errors="replace").rstrip("\n"))
                native_thread_id = event.get("payload", {}).get("thread_id") or native_thread_id
                yield json.dumps(event, ensure_ascii=False) + "\n"
            code = await process.wait()
            app.state.process = None
            result = {"status": "completed" if code == 0 else "failed", "native_thread_id": native_thread_id,
                      "error_code": "" if code == 0 else "harness_exit", "error_detail": "" if code == 0 else f"Harness exited with {code}"}
            yield json.dumps({"type": "result", "result": result}, ensure_ascii=False) + "\n"
        return StreamingResponse(stream(), media_type="application/x-ndjson")

    @app.post("/v1/cancel")
    async def cancel():
        process = app.state.process
        if process and process.returncode is None:
            process.send_signal(signal.SIGTERM)
        return {"cancelled": True}

    @app.get("/v1/artifacts")
    async def artifacts():
        items = []
        for path in OUTPUTS.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(OUTPUTS).as_posix()
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            items.append({"path": relative, "size": path.stat().st_size, "sha256": digest, "mime": mimetypes.guess_type(path.name)[0] or "application/octet-stream"})
        return {"items": items[:50_000]}

    @app.get("/v1/artifacts/file")
    async def artifact(path: str):
        try:
            target = (OUTPUTS / safe_relative(path)).resolve()
            if not target.is_relative_to(OUTPUTS.resolve()) or not target.is_file() or target.is_symlink():
                raise ValueError
        except ValueError:
            raise HTTPException(404, "Artifact not found")
        return FileResponse(target)

    return app
