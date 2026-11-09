from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import os
from pathlib import Path, PurePosixPath
import signal
import tarfile
from dataclasses import dataclass, field
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import quote, urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from .adapters import command, control_user_message, normalize, streaming_user_message


WORKSPACE = Path("/workspace")
PERSISTENT = ("project", "state")
OUTPUTS = WORKSPACE / "outputs"
RUN = Path("/run/harness")
HARNESS_JSONL_LIMIT = 16 * 1024 * 1024
RUNTIME_EVENT_JOURNAL_LIMIT = 256 * 1024 * 1024


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
    execution_id: str = Field(default="legacy", min_length=1, max_length=160)
    input_id: str = Field(default="initial", min_length=1, max_length=160)
    after_sequence: int = Field(default=0, ge=0)


@dataclass
class Execution:
    id: str
    process: asyncio.subprocess.Process
    selected: Any
    native_thread_id: str
    events: list[dict] = field(default_factory=list)
    sequence: int = 0
    pending_input_ids: list[str] = field(default_factory=list)
    completed_input_ids: list[str] = field(default_factory=list)
    received_input_ids: set[str] = field(default_factory=set)
    result: dict | None = None
    saw_text_delta: bool = False
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    reader_task: asyncio.Task | None = None
    journal_bytes: int = 0


def safe_relative(value: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in ("", ".", "..") for part in pure.parts):
        raise ValueError("Unsafe artifact path")
    return Path(*pure.parts)


def create_app() -> FastAPI:
    app = FastAPI(title="Sandbox Harness Runtime")
    app.state.process = None
    app.state.process_command = None
    app.state.pending_inputs = 0
    app.state.execution = None
    app.state.write_lock = asyncio.Lock()
    for name in (*PERSISTENT, "inbox", "outputs", "tmp"):
        (WORKSPACE / name).mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)

    async def stop_process() -> None:
        process = app.state.process
        app.state.process = None
        app.state.process_command = None
        app.state.pending_inputs = 0
        if not process or process.returncode is not None:
            return
        if process.stdin and not process.stdin.is_closing():
            process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), 5)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    async def publish(execution: Execution, value: dict) -> None:
        async with execution.condition:
            execution.sequence += 1
            row = {**value, "runtime_sequence": execution.sequence}
            size = len(json.dumps(row, ensure_ascii=False).encode()) + 1
            if execution.journal_bytes + size > RUNTIME_EVENT_JOURNAL_LIMIT and value.get("type") != "result":
                raise RuntimeError("Runtime event journal exceeded its byte limit")
            execution.journal_bytes += size
            execution.events.append(row)
            execution.condition.notify_all()

    async def finish_execution(execution: Execution, result: dict) -> None:
        if execution.result is not None:
            return
        execution.result = result
        await publish(execution, {"type": "result", "result": result})

    async def read_execution(execution: Execution, harness: str) -> None:
        process = execution.process
        try:
            while line := await process.stdout.readline():
                decoded = line.decode(errors="replace").rstrip("\n")
                try:
                    native = json.loads(decoded)
                except (TypeError, ValueError):
                    native = {}
                event = normalize(harness, decoded)
                terminal_input = (
                    native.get("type") == "harness.input.completed"
                    if execution.selected.control_input else native.get("type") == "result"
                )
                if event is not None:
                    execution.native_thread_id = event.get("payload", {}).get("thread_id") or execution.native_thread_id
                    if event.get("kind") != "text_delta" or not (
                        event.get("payload", {}).get("snapshot") and execution.saw_text_delta
                    ):
                        execution.saw_text_delta |= event.get("kind") == "text_delta"
                        await publish(execution, event)
                if execution.selected.streaming_input and terminal_input:
                    if execution.selected.batched_completion:
                        completed = list(execution.pending_input_ids)
                        execution.pending_input_ids.clear()
                    else:
                        completed = execution.pending_input_ids[:1]
                        del execution.pending_input_ids[:1]
                    execution.completed_input_ids.extend(completed)
                    app.state.pending_inputs = len(execution.pending_input_ids)
                    if not execution.pending_input_ids:
                        await stop_process()
                        await finish_execution(execution, {
                            "status": "completed", "native_thread_id": execution.native_thread_id,
                            "error_code": "", "error_detail": "", "input_ids": execution.completed_input_ids,
                        })
                        return
            code = await process.wait()
            app.state.process = None
            app.state.process_command = None
            app.state.pending_inputs = 0
            await finish_execution(execution, {
                "status": "completed" if code == 0 else "failed",
                "native_thread_id": execution.native_thread_id,
                "error_code": "" if code == 0 else "harness_exit",
                "error_detail": "" if code == 0 else f"Harness exited with {code}",
                "input_ids": list(execution.pending_input_ids),
            })
            execution.pending_input_ids.clear()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await finish_execution(execution, {
                "status": "failed", "native_thread_id": execution.native_thread_id,
                "error_code": type(exc).__name__, "error_detail": str(exc),
                "input_ids": list(execution.pending_input_ids),
            })

    async def stream_execution(execution: Execution, after: int):
        sequence = after
        while True:
            async with execution.condition:
                rows = [row for row in execution.events if row["runtime_sequence"] > sequence]
                if not rows:
                    if execution.result is not None and sequence >= execution.sequence:
                        return
                    await execution.condition.wait()
                    continue
            for row in rows:
                sequence = row["runtime_sequence"]
                yield json.dumps(row, ensure_ascii=False) + "\n"
                if row.get("type") == "result":
                    return

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
        os.environ["HARNESS_CAPABILITY_TOKEN_FILE"] = str(token)
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

        execution = app.state.execution
        if execution is not None and execution.id != value.execution_id and execution.result is None:
            raise HTTPException(409, "Another Harness execution is already running")
        if execution is None or execution.id != value.execution_id:
            process = await asyncio.create_subprocess_exec(*selected.argv, cwd=WORKSPACE / "project", env=environment,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                limit=HARNESS_JSONL_LIMIT)
            execution = Execution(id=value.execution_id, process=process, selected=selected,
                native_thread_id=value.native_thread_id)
            app.state.execution = execution
            app.state.process = process
            app.state.process_command = selected
            execution.reader_task = asyncio.create_task(read_execution(execution, harness),
                name="harness-reader-" + value.execution_id)
        if value.input_id not in execution.received_input_ids:
            async with app.state.write_lock:
                execution.process.stdin.write(control_user_message(prompt) if selected.control_input else
                    streaming_user_message(prompt) if selected.streaming_input else prompt.encode())
                await execution.process.stdin.drain()
                execution.received_input_ids.add(value.input_id)
                execution.pending_input_ids.append(value.input_id)
                app.state.pending_inputs += 1
                if not selected.streaming_input:
                    execution.process.stdin.close()
        return StreamingResponse(stream_execution(execution, value.after_sequence), media_type="application/x-ndjson")

    @app.get("/v1/executions/{execution_id}")
    async def execution_status(execution_id: str):
        execution = app.state.execution
        if execution is None or execution.id != execution_id:
            raise HTTPException(404, "Execution not found")
        return {"execution_id": execution.id, "state": "completed" if execution.result else "running",
            "last_sequence": execution.sequence, "result": execution.result}

    @app.get("/v1/executions/{execution_id}/events")
    async def execution_events(execution_id: str, after: int = 0):
        execution = app.state.execution
        if execution is None or execution.id != execution_id:
            raise HTTPException(404, "Execution not found")
        return StreamingResponse(stream_execution(execution, after), media_type="application/x-ndjson")

    @app.post("/v1/inputs")
    async def append_input(value: TurnRequest):
        process = app.state.process
        selected = app.state.process_command
        if not process or process.returncode is not None or not selected or not selected.streaming_input:
            raise HTTPException(409, "Harness does not have a live input stream")
        prompt = value.message
        input_id = value.input_id
        execution = app.state.execution
        if input_id == "initial" and execution and input_id in execution.received_input_ids:
            input_id = "append-" + __import__("uuid").uuid4().hex
        if value.attachments:
            prompt += "\n\nAttached files are available in /workspace/inbox. Their immutable records are listed in attachments.json."
            (WORKSPACE / "inbox" / "attachments.json").write_text(json.dumps(value.attachments, ensure_ascii=False))
        async with app.state.write_lock:
            execution = app.state.execution
            if execution and input_id not in execution.received_input_ids:
                execution.received_input_ids.add(input_id)
                execution.pending_input_ids.append(input_id)
            process.stdin.write(control_user_message(prompt) if selected.control_input else streaming_user_message(prompt)); await process.stdin.drain()
            app.state.pending_inputs += 1
        mode = "native_steer" if os.environ.get("HARNESS_KIND") == "codex" else (
            "native_safe_point" if os.environ.get("HARNESS_KIND") == "deepseek" else "native_stream")
        return {"accepted": True, "mode": mode}

    @app.post("/v1/cancel")
    async def cancel():
        process = app.state.process
        if process and process.returncode is None:
            process.send_signal(signal.SIGTERM)
        await stop_process()
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
