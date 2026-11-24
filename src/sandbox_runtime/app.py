from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import mimetypes
import os
import pwd
import re
from pathlib import Path, PurePosixPath
import psutil
import signal
import shutil
import stat
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile
from typing import Any,Literal
import uuid
from urllib.parse import quote, urlsplit

import httpx

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from sandbox_harness.observability import RequestLoggingMiddleware, observed, event, enrich, telemetry, ToolObservations

from .adapters import (command,control_interaction_response,control_user_message,normalize,
    normalize_error_code,streaming_user_message)
from .instructions import RUNTIME_GUIDANCE, runtime_guidance
from .runtime_bundle import (MAX_BUNDLE_BYTES,activate_runtime_bundle,
    decode_runtime_bundle_transport,verify_runtime_bundle_signature)
from .skills import clear_materialized_skills, materialize_skills
from .state import runtime_state_dir


WORKSPACE = Path("/workspace")
PERSISTENT = ("project", "state")
OUTPUTS = WORKSPACE / "outputs"
INSPECT_ROOTS = {
    "project": WORKSPACE / "project",
    "inbox": WORKSPACE / "inbox",
    "outputs": OUTPUTS,
}
INSPECT_TEXT_LIMIT = 2 * 1024 * 1024
INSPECT_INTERNAL_PORTS = {8080, 18080, 29880, 29881, 44772}
RUN = runtime_state_dir()
OUTPUT_BATCH_MARKER = RUN / "output-batch"
HARNESS_JSONL_LIMIT = 16 * 1024 * 1024
RUNTIME_EVENT_JOURNAL_LIMIT = 256 * 1024 * 1024
MAX_OUTPUT_ARTIFACTS = 50_000
MAX_CHECKPOINT_BYTES = 4 * 1024 * 1024 * 1024


def require_output_artifact_count(count: int) -> None:
    if count > MAX_OUTPUT_ARTIFACTS:
        raise HTTPException(413, "artifact_count_limit_exceeded")


def reset_outputs(batch_id: str) -> None:
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    for path in OUTPUTS.iterdir():
        if path.is_symlink() or not path.is_dir():
            path.unlink(missing_ok=True)
        else:
            shutil.rmtree(path)
    OUTPUT_BATCH_MARKER.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_BATCH_MARKER.write_text(batch_id)


async def stage_checkpoint_request(request: Request) -> Path:
    raw_length = request.headers.get("content-length")
    if raw_length is not None:
        try:
            declared = int(raw_length)
        except ValueError as exc:
            raise HTTPException(400, "checkpoint_content_length_invalid") from exc
        if declared < 0:
            raise HTTPException(400, "checkpoint_content_length_invalid")
        if declared > MAX_CHECKPOINT_BYTES:
            raise HTTPException(413, "checkpoint_too_large")
    RUN.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(dir=RUN, prefix=".checkpoint-restore-", delete=False) as stream:
            temporary = Path(stream.name); size = 0
            async for chunk in request.stream():
                if size + len(chunk) > MAX_CHECKPOINT_BYTES:
                    raise HTTPException(413, "checkpoint_too_large")
                stream.write(chunk); size += len(chunk)
            if raw_length is not None and size != declared:
                raise HTTPException(400, "checkpoint_size_mismatch")
            stream.flush(); os.fsync(stream.fileno())
        return temporary
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise
class Configure(BaseModel):
    thread_id: str = Field(default="",max_length=160)
    turn_id: str = Field(default="",max_length=160)
    sandbox_id: str = Field(default="",max_length=200)
    job_id: str = Field(default="",max_length=160)
    capability_token: str = Field(min_length=20)
    gateway_url: str = Field(pattern=r"^https?://")
    harness: str = Field(pattern=r"^(codex|claude|deepseek)$")
    model: str = Field(min_length=1, max_length=160)
    proxy_url: str = Field(pattern=r"^http://")
    defer_skill_install: bool = False
    model_start_timeout_seconds: float = Field(default=150, ge=10, le=900)
    event_idle_timeout_seconds: float = Field(default=900, ge=60, le=3600)
    runtime_bundle_digest: str = Field(default="", pattern=r"^$|^[0-9a-f]{64}$")
    runtime_profile_id: str = Field(default="", max_length=100)
    skill_ids: list[str] = Field(default_factory=list, max_length=64)
    skill_digests: dict[str, str] = Field(default_factory=dict)
    child_mode: bool = False
    delegation_depth: int = Field(default=0, ge=0, le=4)
    background_job_max_seconds: float = Field(default=3600, ge=1, le=86400)


class MediaRenderOperation(BaseModel):
    operation: Literal["image.resize","video.transcode","audio.process","media.concat_sequence"]
    input_path: str = Field(default="",max_length=2000)
    source_path: str = Field(default="",max_length=2000)
    music_path: str = Field(default="",max_length=2000)
    output_filename: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
    width: int = Field(default=0,ge=0,le=8192)
    height: int = Field(default=0,ge=0,le=8192)
    fps: int = Field(default=24,ge=1,le=120)
    format: Literal["wav","mp3","m4a"] = "wav"
    gain_db: float = Field(default=0,ge=-60,le=24)
    target_lufs: float | None = Field(default=None,ge=-40,le=-5)
    true_peak_db: float = Field(default=-1.5,ge=-9,le=0)
    fade_in: float = Field(default=0,ge=0,le=30)
    fade_out: float = Field(default=0,ge=0,le=30)
    highpass_hz: float = Field(default=0,ge=0,le=300)
    denoise: bool = False
    segment_count: int = Field(default=1,ge=1,le=100)
    segment_seconds: float = Field(default=2,ge=.2,le=60)


class JobArtifactStage(BaseModel):
    job_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1)
    path: str = Field(min_length=1, max_length=2000)
    artifact_role: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    item_key: str = Field(min_length=1, max_length=160)
    position: int = Field(ge=0)
    filename: str = Field(min_length=1, max_length=255)
    output_disposition: str = Field(pattern=r"^(candidate|workspace|delivery)$")
    mime: str = Field(default="application/octet-stream", max_length=200)


def stage_regular_file(source: Path, staging_root: Path, maximum: int, after_copy=None) -> Path:
    initial = source.lstat()
    if source.is_symlink() or not stat.S_ISREG(initial.st_mode):
        raise ValueError("Artifact source must be a regular file")
    resolved = source.resolve(strict=True)
    output_root = OUTPUTS.resolve()
    if resolved != output_root and output_root not in resolved.parents:
        raise ValueError("Artifact source must be inside /workspace/outputs")
    descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    staging_root.mkdir(parents=True, exist_ok=True)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("Artifact source must be a regular file")
        if before.st_size > maximum:
            raise ValueError("Artifact exceeds byte limit")
        with NamedTemporaryFile(dir=staging_root, prefix=".artifact-stage-", delete=False) as target:
            staged = Path(target.name);copied = 0
            while True:
                chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - copied))
                if not chunk:break
                copied += len(chunk)
                if copied > maximum:raise ValueError("Artifact exceeds byte limit")
                target.write(chunk)
            target.flush();os.fsync(target.fileno())
        if after_copy:after_copy()
        after = os.fstat(descriptor)
        current = source.stat(follow_symlinks=False)
        identity = lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns)
        if identity(before) != identity(after) or identity(after) != identity(current) or copied != before.st_size:
            staged.unlink(missing_ok=True)
            raise RuntimeError("Artifact source changed during staging")
        os.chmod(staged,0o600)
        return staged
    finally:
        os.close(descriptor)


class TurnRequest(BaseModel):
    message: str = Field(min_length=1, max_length=200_000)
    attachments: list[dict] = Field(default_factory=list, max_length=64)
    native_thread_id: str = ""
    reasoning_effort: str = Field(default="high", pattern=r"^(minimal|low|medium|high|xhigh)$")
    execution_id: str = Field(default="legacy", min_length=1, max_length=160)
    input_id: str = Field(default="initial", min_length=1, max_length=160)
    after_sequence: int = Field(default=0, ge=0)


class ManagedPreserveRequest(BaseModel):
    paths: list[str] = Field(min_length=1, max_length=10_000)
    destination: str = Field(pattern=r"^work(?:/[^/]+)*$")


class BackgroundJobCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    argv: list[str] = Field(min_length=1, max_length=256)
    cwd: str = Field(default="/workspace/project", min_length=1, max_length=1000)
    replaces: str = Field(default="", pattern=r"^$|^[0-9a-f]{32}$")
    timeout_seconds: float = Field(default=1200, ge=.1, le=86400)
    terminate_grace_seconds: float = Field(default=2, ge=.01, le=10)


class NativeInteractionCreate(BaseModel):
    kind: str = Field(pattern=r"^(approval|question)$")
    prompt: str = Field(min_length=1, max_length=4000)
    questions: list[dict] = Field(default_factory=list, max_length=20)
    options: list[str] = Field(default_factory=list, max_length=50)


@dataclass
class BackgroundJob:
    id: str
    name: str
    argv: list[str]
    cwd: str
    execution_id: str
    replaces: str = ""
    status: str = "running"
    pid: int = 0
    returncode: int | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: str = ""
    log_path: str = ""
    pid_start_time: str = ""
    boot_id: str = ""
    recovery_detail: str = ""
    supervisor_receipt: str = ""
    timeout_seconds: float = 1200
    terminate_grace_seconds: float = 2
    deadline_at: float = 0
    termination_detail: dict = field(default_factory=dict)
    process: asyncio.subprocess.Process | None = None
    task: asyncio.Task | None = None
    done_event: asyncio.Event = field(default_factory=asyncio.Event)

    def public(self, *, log_tail: str = "") -> dict:
        return {"id": self.id, "name": self.name, "argv": self.argv, "cwd": self.cwd,
            "execution_id": self.execution_id, "status": self.status, "pid": self.pid,
            "returncode": self.returncode, "created_at": self.created_at,
            "completed_at": self.completed_at, "replaces": self.replaces,
            "timeout_seconds": self.timeout_seconds,
            "terminate_grace_seconds": self.terminate_grace_seconds,
            "deadline_at": self.deadline_at,"termination_detail":self.termination_detail,
            "log_tail": log_tail}


def boot_identity() -> str:
    try:return Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    except OSError:return ""


def process_start_identity(pid: int) -> str:
    try:
        value=Path(f'/proc/{pid}/stat').read_text()
        return value[value.rfind(') ')+2:].split()[19]
    except (OSError,IndexError):return ""


def recorded_process_alive(value: dict) -> bool:
    pid=int(value.get('pid') or 0)
    if not (pid>1 and value.get('boot_id') and value.get('boot_id')==boot_identity()
            and value.get('pid_start_time')):return False
    try:
        raw=Path(f'/proc/{pid}/stat').read_text();parts=raw[raw.rfind(') ')+2:].split()
        return parts[0]!='Z' and value.get('pid_start_time')==parts[19]
    except (OSError,IndexError):return False


def quiesce_recorded_group(value: dict) -> str:
    if not recorded_process_alive(value):return 'process_absent_or_identity_changed'
    pid=int(value['pid'])
    try:
        if os.getpgid(pid)!=pid:return 'process_group_identity_changed'
    except ProcessLookupError:return 'process_already_exited'
    grace=min(max(float(value.get('terminate_grace_seconds') or 2),.01),10)
    for sig,deadline in ((signal.SIGTERM,grace+1.2),(signal.SIGKILL,.8)):
        try:os.killpg(pid,sig)
        except ProcessLookupError:return 'process_quiesced'
        end=time.monotonic()+deadline
        while time.monotonic()<end:
            if not recorded_process_alive(value):return 'process_quiesced'
            time.sleep(.02)
    return 'process_quiesce_unconfirmed'


def atomic_json(path: Path, value: dict) -> None:
    temporary=path.with_suffix(path.suffix+'.tmp')
    with temporary.open('w') as target:
        target.write(json.dumps(value,ensure_ascii=False))
        target.flush();os.fsync(target.fileno())
    temporary.replace(path)
    descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def unresolved_background_jobs(jobs: list[BackgroundJob]) -> list[BackgroundJob]:
    replacements: dict[str, list[BackgroundJob]] = {}
    for job in jobs:
        if job.replaces:
            replacements.setdefault(job.replaces, []).append(job)

    def recovered(job_id: str, seen: set[str]) -> bool:
        if job_id in seen:
            return False
        return any(child.status == "completed" or recovered(child.id, seen | {job_id})
            for child in replacements.get(job_id, []))

    return [job for job in jobs if job.status != "completed" and not recovered(job.id, set())]


def unresolved_replacement_targets(jobs:list[BackgroundJob])->list[BackgroundJob]:
    unresolved=unresolved_background_jobs(jobs)
    parents={job.replaces for job in jobs if job.replaces}
    return [job for job in unresolved if job.id not in parents]


def background_failure_detail(jobs:list[BackgroundJob],log_tails:dict[str,str])->str:
    values=[{'id':job.id,'name':job.name,'status':job.status,'returncode':job.returncode,
        'log_tail':str(log_tails.get(job.id) or '')[-2000:]} for job in jobs]
    return json.dumps({'background_jobs':values},ensure_ascii=False)[:4000]


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
    input_fingerprints: dict[str, str] = field(default_factory=dict)
    interaction_ids: dict[str, str] = field(default_factory=dict)
    interaction_responses: dict[str, dict] = field(default_factory=dict)
    stop_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    cancel_requested: bool = False
    accepting_inputs: bool = True
    result: dict | None = None
    saw_text_delta: bool = False
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    reader_task: asyncio.Task | None = None
    tools: ToolObservations = field(default_factory=ToolObservations)
    journal_bytes: int = 0
    protocol_state: dict[str, Any] = field(default_factory=dict)
    project_output_snapshot: dict[str, tuple[int, int]] = field(default_factory=dict)


def execution_input_ids(execution: Execution) -> list[str]:
    return list(dict.fromkeys([*execution.completed_input_ids,*execution.pending_input_ids]))


def safe_relative(value: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in ("", ".", "..") for part in pure.parts):
        raise ValueError("Unsafe artifact path")
    return Path(*pure.parts)


def project_output_snapshot() -> dict[str, tuple[int, int]]:
    root = WORKSPACE / "project" / "outputs"
    if not root.is_dir() or root.is_symlink():
        return {}
    result = {}
    for path in root.rglob("*"):
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                continue
            relative = path.relative_to(root).as_posix()
            safe_relative(relative)
            if not path.resolve().is_relative_to(root.resolve()):
                continue
            result[relative] = (info.st_size, info.st_mtime_ns)
        except (OSError, ValueError):
            continue
        if len(result) >= 50_000:
            break
    return result


def promote_project_outputs(baseline: dict[str, tuple[int, int]]) -> tuple[int, int]:
    root = WORKSPACE / "project" / "outputs"
    current = project_output_snapshot()
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    count = total = 0
    for relative, identity in current.items():
        if baseline.get(relative) == identity:
            continue
        source = root / safe_relative(relative)
        target = OUTPUTS / safe_relative(relative)
        if target.exists() or target.is_symlink():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.parent.resolve().is_relative_to(OUTPUTS.resolve()):
            continue
        temporary = target.with_name("." + target.name + ".publish.tmp")
        try:
            shutil.copyfile(source, temporary)
            os.chmod(temporary, 0o600)
            temporary.replace(target)
            count += 1
            total += identity[0]
        finally:
            temporary.unlink(missing_ok=True)
    return count, total


def create_app() -> FastAPI:
    app = FastAPI(title="Sandbox Harness Runtime")
    app.add_middleware(RequestLoggingMiddleware,service="runtime")
    app.state.log_context = {}
    app.state.process = None
    app.state.process_command = None
    app.state.pending_inputs = 0
    app.state.execution = None
    app.state.runtime_bundle_digest = ""
    app.state.background_jobs = {}
    app.state.interaction_waiters = {}
    app.state.interaction_response_lock = asyncio.Lock()
    app.state.model_start_timeout_seconds = 150.0
    app.state.event_idle_timeout_seconds = 900.0
    app.state.harness = ""
    app.state.runtime_profile_id = ""
    app.state.skill_ids = []
    app.state.skill_digests = {}
    app.state.capability_change_frame = None
    app.state.child_mode = False
    app.state.delegation_depth = 0
    app.state.stream_heartbeat_seconds = 15.0
    app.state.background_job_max_seconds = 3600.0
    jobs_root = WORKSPACE / "state" / "background-jobs"
    jobs_root.mkdir(parents=True, exist_ok=True)
    def load_jobs() -> None:
        for record in jobs_root.glob("*.json"):
            if record.name.endswith(".supervisor.json"):
                continue
            try:
                value = json.loads(record.read_text())
                receipt=Path(str(value.get('supervisor_receipt') or ''))
                if not value.get('pid') and value.get('status')=='starting' and str(receipt):
                    for _ in range(100):
                        if receipt.is_file():break
                        time.sleep(.01)
                supervisor={}
                if receipt.is_file():
                    try:supervisor=json.loads(receipt.read_text())
                    except (OSError,ValueError,TypeError):pass
                if not value.get('pid') and supervisor:
                    for key in ('pid','pid_start_time','boot_id','deadline_at'):
                        if key in supervisor:value[key]=supervisor[key]
                supervisor_status=str(supervisor.get('status') or '')
                if supervisor_status in {"completed","failed","cancelled","timed_out"}:
                    value['status']=supervisor_status
                    value['returncode']=supervisor.get('returncode')
                    value['completed_at']=str(supervisor.get('completed_at') or
                        datetime.now(timezone.utc).isoformat())
                    value['termination_detail']={key:supervisor[key] for key in (
                        'reason','sigterm_sent','sigkill_sent') if key in supervisor}
                    atomic_json(record,value)
                elif value.get("status") in {"starting", "running", "cancelling", "timing_out"}:
                    value["recovery_detail"] = quiesce_recorded_group(value)
                    value["status"] = "interrupted"
                    value["completed_at"] = datetime.now(timezone.utc).isoformat()
                    atomic_json(record,value)
                job = BackgroundJob(**{key: value[key] for key in BackgroundJob.__dataclass_fields__
                    if key in value and key not in {"process", "task", "done_event"}})
                if job.status in {"completed","failed","cancelled","interrupted","timed_out"}:
                    job.done_event.set()
                app.state.background_jobs[job.id] = job
            except (OSError, ValueError, TypeError, KeyError):
                continue
    load_jobs()
    app.state.write_lock = asyncio.Lock()
    app.state.execution_lock = asyncio.Lock()
    for name in (*PERSISTENT, "inbox", "outputs", "tmp"):
        (WORKSPACE / name).mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)

    async def terminate_group(process, *, grace: float = 2.0) -> None:
        # A shell may exit while its descendants still own stdout or mutate files.
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                break
            if sig == signal.SIGTERM:
                try:
                    await asyncio.wait_for(asyncio.shield(process.wait()), grace)
                except asyncio.TimeoutError:
                    pass
        await process.wait()

    async def stop_process(execution: Execution) -> None:
        async with execution.stop_lock:
            process = execution.process
            if process.stdin and not process.stdin.is_closing():
                process.stdin.close()
            if process.returncode is None:
                try:
                    await asyncio.wait_for(asyncio.shield(process.wait()), 1)
                except asyncio.TimeoutError:
                    pass
            await terminate_group(process)
            if app.state.process is process:
                app.state.process = None
                app.state.process_command = None
                app.state.pending_inputs = 0

    async def stop_background_supervisor(job: BackgroundJob) -> None:
        process=job.process
        if not process or process.returncode is not None:return
        try:process.send_signal(signal.SIGTERM)
        except ProcessLookupError:return
        try:
            await asyncio.wait_for(asyncio.shield(process.wait()),job.terminate_grace_seconds+2)
        except asyncio.TimeoutError:
            await terminate_group(process,grace=.5)

    async def cancel_job(job: BackgroundJob) -> None:
        if job.status not in {"running", "cancelling"} or not job.process:
            return
        job.status = "cancelling"
        persist_job(job)
        await stop_background_supervisor(job)
        if job.task:
            await asyncio.shield(job.task)

    async def abort_execution(execution: Execution) -> None:
        async with app.state.write_lock:
            execution.accepting_inputs = False
        await stop_process(execution)
        await asyncio.gather(*(cancel_job(job) for job in list(app.state.background_jobs.values())
            if job.execution_id == execution.id))

    def append_event_locked(execution: Execution, value: dict) -> None:
        row = {**value, "runtime_sequence": execution.sequence + 1}
        size = len(json.dumps(row, ensure_ascii=False).encode()) + 1
        if execution.journal_bytes + size > RUNTIME_EVENT_JOURNAL_LIMIT and value.get("type") != "result":
            raise RuntimeError("Runtime event journal exceeded its byte limit")
        execution.sequence += 1
        execution.journal_bytes += size
        execution.events.append(row)
        execution.condition.notify_all()

    async def publish(execution: Execution, value: dict) -> None:
        async with execution.condition:
            if value.get("kind") == "waiting_user":
                if execution.result is not None or not execution.accepting_inputs:
                    raise HTTPException(409, "Execution is no longer accepting interactions")
                payload = value["payload"]
                native_id = str(payload.get("interaction_id") or "")
                if not native_id:
                    raise ValueError("Native interaction has no ID")
                identity = "runtime-" + hashlib.sha256(json.dumps([execution.id, native_id]).encode()).hexdigest()
                execution.interaction_ids[identity] = native_id
                value = {**value, "payload": {**payload, "interaction_id": identity}}
            append_event_locked(execution, value)

    async def finish_execution(execution: Execution, result: dict) -> None:
        async with execution.condition:
            if execution.result is not None:
                return
        if result.get("status") != "cancelled":
            try:
                count, size = await asyncio.to_thread(
                    promote_project_outputs, execution.project_output_snapshot)
                if count:
                    event("runtime.project_outputs.promoted",execution_id=execution.id,
                        count=count,bytes=size)
            except Exception as exc:
                event("runtime.project_outputs.promotion_failed",level=logging.WARNING,
                    execution_id=execution.id,exc=exc)
        async with execution.condition:
            if execution.result is not None:
                return
            execution.accepting_inputs = False
            if execution.cancel_requested:
                result = {**result, "status": "cancelled", "error_code": "cancelled"}
            append_event_locked(execution, {"type": "result", "result": result})
            execution.result = result
            execution.tools.close()
            event("runtime.execution.terminal",execution_id=execution.id,state=result.get("status"),error_code=result.get("error_code"),runtime_sequence=execution.sequence)
            for native_id in execution.interaction_ids.values():
                waiter = app.state.interaction_waiters.get(native_id)
                if waiter is not None and not waiter.done():
                    waiter.set_exception(HTTPException(409, "Execution ended before the interaction was answered"))

    def persist_job(job: BackgroundJob) -> None:
        target = jobs_root / (job.id + ".json")
        atomic_json(target,{**job.public(),"log_path":job.log_path,
            "pid_start_time":job.pid_start_time,"boot_id":job.boot_id,
            "recovery_detail":job.recovery_detail,
            "supervisor_receipt":job.supervisor_receipt})

    def job_log_tail(job: BackgroundJob) -> str:
        try:
            data = Path(job.log_path).read_bytes()[-8192:]
            return data.decode(errors="replace")
        except OSError:
            return ""

    async def watch_job(job: BackgroundJob) -> None:
        written = 0
        with Path(job.log_path).open("wb") as log:
            while chunk := await job.process.stdout.read(65536):
                if written < 16 * 1024 * 1024:
                    value = chunk[:16 * 1024 * 1024 - written]
                    log.write(value); log.flush(); written += len(value)
        supervisor_returncode = await job.process.wait()
        receipt={}
        try:receipt=json.loads(Path(job.supervisor_receipt).read_text())
        except (OSError,ValueError,TypeError):pass
        receipt_status=str(receipt.get("status") or "")
        if receipt_status in {"completed","failed","cancelled","timed_out"}:
            job.status=receipt_status
            job.returncode=receipt.get("returncode")
            job.termination_detail={key:receipt[key] for key in (
                "reason","sigterm_sent","sigkill_sent") if key in receipt}
        elif job.status == "cancelling":
            job.status = "cancelled";job.returncode=supervisor_returncode
        else:
            job.returncode=supervisor_returncode
            job.status = "completed" if supervisor_returncode == 0 else "failed"
        job.completed_at = str(receipt.get("completed_at") or datetime.now(timezone.utc).isoformat())
        persist_job(job);job.done_event.set()
        event("runtime.background.finished",job_id=job.id,execution_id=job.execution_id,state=job.status,exit_code=job.returncode)
        execution = app.state.execution
        if execution and execution.id == job.execution_id:
            await publish(execution, {"kind": "step_finished", "payload": {
                "name": "后台任务 · " + job.name, "background_job_id": job.id,
                "status": job.status, "returncode": job.returncode, "replaces": job.replaces,
            }})

    async def wait_background(execution: Execution) -> list[BackgroundJob]:
        jobs = [job for job in app.state.background_jobs.values()
            if job.execution_id == execution.id and job.status in {"running", "cancelling"}]
        if jobs:
            await publish(execution, {"kind": "step_progress", "payload": {
                "name": "等待后台任务", "jobs": [job.id for job in jobs],
            }})
            await asyncio.gather(*(job.task for job in jobs))
        return [job for job in app.state.background_jobs.values() if job.execution_id == execution.id]

    @observed("runtime.execution")
    async def read_execution(execution: Execution, harness: str) -> None:
        process = execution.process
        substantive = False
        try:
            while True:
                timeout = (app.state.event_idle_timeout_seconds if substantive
                    else app.state.model_start_timeout_seconds)
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=timeout)
                except TimeoutError:
                    await abort_execution(execution)
                    await finish_execution(execution, {"status": "failed",
                        "native_thread_id": execution.native_thread_id,
                        "error_code": "harness_idle_timeout" if substantive else "model_response_timeout",
                        "error_detail": ("Harness produced no events before the idle timeout" if substantive
                            else "The model produced no response before the start timeout"),
                        "input_ids": execution_input_ids(execution)})
                    execution.pending_input_ids.clear()
                    return
                if not line:
                    break
                decoded = line.decode(errors="replace").rstrip("\n")
                try:
                    native = json.loads(decoded)
                except (TypeError, ValueError):
                    native = {}
                if not isinstance(native, dict):
                    native = {}
                if native.get("type")=="harness.interaction.applied":
                    native_id=str(native.get("id") or "")
                    public_id=next((public for public,value in execution.interaction_ids.items()
                        if value==native_id),"")
                    if public_id:
                        recorded=execution.interaction_responses.get(public_id)
                        if recorded is not None:recorded["state"]="delivered"
                        native={**native,"id":public_id}
                event = normalize(harness, decoded, execution.protocol_state)
                if native.get("type")=="harness.interaction.applied":
                    event=normalize(harness,json.dumps(native),execution.protocol_state)
                terminal_input = (
                    native.get("type") == "harness.input.completed"
                    if execution.selected.control_input else native.get("type") == "result"
                )
                if event is not None:
                    execution.tools.observe(event.get("kind"),event.get("payload",{}),execution.id)
                    execution.native_thread_id = event.get("payload", {}).get("thread_id") or execution.native_thread_id
                    if event.get("kind") != "text_delta" or not (
                        event.get("payload", {}).get("snapshot") and execution.saw_text_delta
                    ):
                        execution.saw_text_delta |= event.get("kind") == "text_delta"
                        await publish(execution, event)
                    substantive = substantive or event.get("kind") in {
                        "text_delta", "tool_started", "tool_finished", "step_progress",
                        "waiting_user", "error", "warning",
                    }
                substantive = substantive or terminal_input
                if execution.selected.streaming_input and terminal_input:
                    async with app.state.write_lock:
                        if execution.selected.control_input:
                            ids = set(native.get("input_ids") or [])
                            completed = [identity for identity in execution.pending_input_ids if identity in ids]
                            execution.pending_input_ids[:] = [identity for identity in execution.pending_input_ids if identity not in ids]
                        elif execution.selected.batched_completion:
                            completed = list(execution.pending_input_ids)
                            execution.pending_input_ids.clear()
                        else:
                            completed = execution.pending_input_ids[:1]
                            del execution.pending_input_ids[:1]
                        execution.completed_input_ids.extend(completed)
                        app.state.pending_inputs = len(execution.pending_input_ids)
                        done = not execution.pending_input_ids
                        if done:
                            execution.accepting_inputs = False
                    execution.saw_text_delta = False
                    if not done:
                        substantive = False
                    if done:
                        await stop_process(execution)
                        background = await wait_background(execution)
                        unresolved=unresolved_background_jobs(background)
                        failed = (
                            native.get("status") == "failed"
                            or bool(native.get("is_error"))
                            or native.get("subtype") in {"error", "error_max_turns", "error_during_execution"}
                            or bool(unresolved)
                        )
                        detail = (native.get("error_detail") or native.get("result")
                            or native.get("message") or background_failure_detail(unresolved,
                                {job.id:job_log_tail(job) for job in unresolved}))
                        await finish_execution(execution, {
                            "status": "failed" if failed else "completed",
                            "native_thread_id": execution.native_thread_id,
                            "error_code": (normalize_error_code(native.get("error_code")) if native.get("error_code") else (
                                "background_job_failed" if unresolved
                                else "harness_failed")) if failed else "",
                            "error_detail": str(detail)[:4000] if failed else "",
                            "input_ids": execution.completed_input_ids,
                        })
                        return
            code = await process.wait()
            execution.accepting_inputs = False
            missing_result = execution.selected.streaming_input
            if missing_result or code != 0 or execution.cancel_requested:
                await abort_execution(execution)
            else:
                await stop_process(execution)
            if missing_result or execution.cancel_requested:
                await finish_execution(execution, {
                    "status": "cancelled" if execution.cancel_requested else "failed",
                    "native_thread_id": execution.native_thread_id,
                    "error_code": "cancelled" if execution.cancel_requested else "harness_result_missing",
                    "error_detail": "Harness stopped before acknowledging all pending inputs",
                    "input_ids": execution_input_ids(execution),
                })
                return
            background = await wait_background(execution)
            unresolved=unresolved_background_jobs(background);background_failed=bool(unresolved)
            await finish_execution(execution, {
                "status": "completed" if code == 0 and not background_failed else "failed",
                "native_thread_id": execution.native_thread_id,
                "error_code": "" if code == 0 and not background_failed else (
                    "background_job_failed" if background_failed else "harness_exit"),
                "error_detail": "" if code == 0 and not background_failed else (
                    background_failure_detail(unresolved,{job.id:job_log_tail(job) for job in unresolved})
                    if background_failed else f"Harness exited with {code}"),
                "input_ids": execution_input_ids(execution),
            })
            execution.pending_input_ids.clear()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await abort_execution(execution)
            await finish_execution(execution, {
                "status": "failed", "native_thread_id": execution.native_thread_id,
                "error_code": "harness_protocol_error", "error_detail": f"{type(exc).__name__}: {exc}",
                "input_ids": execution_input_ids(execution),
            })

    async def stream_execution(execution: Execution, after: int):
        sequence = after
        while True:
            heartbeat = False
            async with execution.condition:
                rows = [row for row in execution.events if row["runtime_sequence"] > sequence]
                if not rows:
                    if execution.result is not None and sequence >= execution.sequence:
                        return
                    try:
                        await asyncio.wait_for(execution.condition.wait(), app.state.stream_heartbeat_seconds)
                    except TimeoutError:
                        heartbeat = True
                    if not heartbeat:
                        continue
            if heartbeat:
                yield json.dumps({"type": "heartbeat"}) + "\n"
                continue
            for row in rows:
                sequence = row["runtime_sequence"]
                yield json.dumps(row, ensure_ascii=False) + "\n"
                if row.get("type") == "result":
                    return

    def inspect_target(value: str, *, require_file: bool = False) -> tuple[str, Path]:
        path = PurePosixPath((value or "project").strip().replace("\\", "/"))
        if path.is_absolute() or not path.parts or path.parts[0] not in INSPECT_ROOTS or any(
                part in {"", ".", ".."} for part in path.parts):
            raise HTTPException(400, "Invalid sandbox workspace path")
        root_name = path.parts[0]
        root = INSPECT_ROOTS[root_name]
        target = root.joinpath(*path.parts[1:])
        current = root
        for part in path.parts[1:]:
            current = current / part
            if current.is_symlink():
                raise HTTPException(400, "Sandbox workspace links are not inspectable")
        try:
            if not target.resolve().is_relative_to(root.resolve()):
                raise HTTPException(400, "Invalid sandbox workspace path")
            if require_file and (not target.is_file() or target.is_symlink()):
                raise HTTPException(404, "Sandbox file not found")
        except OSError:
            raise HTTPException(404, "Sandbox path not found")
        return root_name, target

    @app.get("/v1/inspect/overview")
    async def inspect_overview():
        def workspace_bytes() -> int:
            total = 0; seen = set()
            for root, directories, files in os.walk(WORKSPACE, followlinks=False):
                directories[:] = [name for name in directories if not (Path(root) / name).is_symlink()]
                for name in files:
                    try:
                        path = Path(root) / name; info = path.stat(follow_symlinks=False)
                        identity = (info.st_dev, info.st_ino)
                        if stat.S_ISREG(info.st_mode) and identity not in seen:
                            seen.add(identity); total += info.st_size
                    except OSError:
                        continue
            return total
        memory = psutil.virtual_memory()
        disk_used = await asyncio.to_thread(workspace_bytes)
        execution = app.state.execution
        return {"runtime": "running", "execution": (
            "running" if execution and execution.result is None else "idle"),
            "username": pwd.getpwuid(os.getuid()).pw_name,
            "cpu_percent": round(psutil.cpu_percent(interval=None), 2),
            "cpu_count": psutil.cpu_count() or 1,
            "memory_used": int(memory.used), "memory_total": int(memory.total),
            "disk_used": disk_used, "disk_total": 0, "disk_limited": False,
            "process_count": len(psutil.pids()),
            "timestamp": datetime.now(timezone.utc).isoformat()}

    @app.get("/v1/inspect/files")
    async def inspect_files(path: str = "project", depth: int = 1):
        root_name, target = inspect_target(path)
        if not target.exists() or not target.is_dir():
            raise HTTPException(404, "Sandbox directory not found")
        depth = min(max(depth, 1), 4)
        items = []
        for candidate in sorted(target.iterdir(), key=lambda item: (not item.is_dir(), item.name.casefold())):
            try:
                info = candidate.lstat()
            except OSError:
                continue
            kind = "symlink" if candidate.is_symlink() else "directory" if candidate.is_dir() else "file"
            relative = candidate.relative_to(INSPECT_ROOTS[root_name]).as_posix()
            first = PurePosixPath(relative).parts[0] if relative != "." else ""
            policy = ("managed_input" if root_name == "project" and first == "assets" else
                "managed_artifact" if root_name == "project" and first == "artifacts" else
                "checkpoint" if root_name == "project" else
                "turn_input" if root_name == "inbox" else "turn_output")
            items.append({"path": root_name + ("/" + relative if relative != "." else ""),
                "name": candidate.name, "type": kind,
                "size": int(info.st_size) if kind == "file" else 0,
                "mime": mimetypes.guess_type(candidate.name)[0] or "application/octet-stream",
                "modified_at": datetime.fromtimestamp(info.st_mtime, timezone.utc).isoformat(),
                "policy": policy, "writable": policy == "checkpoint"})
            if len(items) >= 10_000:
                break
        return {"root": root_name, "path": path, "items": items, "truncated": len(items) >= 10_000}

    @app.get("/v1/inspect/file")
    async def inspect_file(path: str):
        _, target = inspect_target(path, require_file=True)
        info = target.stat()
        if info.st_size > INSPECT_TEXT_LIMIT:
            raise HTTPException(413, "Sandbox file is too large for inline inspection")
        data = target.read_bytes()
        mime = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        return Response(data, media_type=mime,
            headers={"X-Sandbox-File-Size": str(info.st_size), "Cache-Control": "no-store"})

    @app.put("/v1/inspect/file")
    async def write_inspect_file(path: str, request: Request):
        root_name, target = inspect_target(path)
        relative = PurePosixPath(path).parts[1:]
        if root_name != "project" or relative and relative[0] in {"assets", "artifacts"}:
            raise HTTPException(403, "Only unmanaged project files may be edited")
        data = await request.body()
        if len(data) > INSPECT_TEXT_LIMIT:
            raise HTTPException(413, "Sandbox file is too large for inline editing")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name("." + target.name + ".inspector.tmp")
        temporary.write_bytes(data); os.chmod(temporary, 0o600); temporary.replace(target)
        return {"path": path, "size": len(data), "updated": True}

    @app.get("/v1/inspect/processes")
    async def inspect_processes():
        values = []
        for process in psutil.process_iter(["pid", "ppid", "name", "status", "create_time",
                "memory_info", "cpu_percent", "username"]):
            try:
                info = process.info
                values.append({"pid": info["pid"], "ppid": info["ppid"],
                    "name": (info.get("name") or "")[:160], "status": info.get("status") or "unknown",
                    "cpu_percent": round(float(info.get("cpu_percent") or 0), 2),
                    "memory_bytes": int(info["memory_info"].rss if info.get("memory_info") else 0),
                    "started_at": datetime.fromtimestamp(info.get("create_time") or 0,
                        timezone.utc).isoformat() if info.get("create_time") else ""})
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return {"items": sorted(values, key=lambda value: value["pid"])[:5000]}

    @app.post("/v1/inspect/processes/{pid}/stop")
    async def stop_inspect_process(pid: int):
        protected = {1, os.getpid(), os.getppid()}
        if pid in protected or pid <= 1:
            raise HTTPException(403, "Protected sandbox process cannot be stopped")
        try:
            process = psutil.Process(pid)
            if process.uids().real != os.getuid():
                raise HTTPException(403, "Sandbox process is owned by another identity")
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, 3)
            except psutil.TimeoutExpired:
                process.kill()
            return {"pid": pid, "stopped": True}
        except psutil.NoSuchProcess:
            raise HTTPException(404, "Sandbox process not found")
        except psutil.AccessDenied:
            raise HTTPException(403, "Sandbox process cannot be stopped")

    @app.get("/v1/inspect/ports")
    async def inspect_ports():
        values = {}
        try:
            connections = psutil.net_connections(kind="inet")
        except psutil.AccessDenied:
            connections = []
        for connection in connections:
            if connection.status != psutil.CONN_LISTEN or not connection.laddr:
                continue
            port = int(connection.laddr.port)
            if port in INSPECT_INTERNAL_PORTS or port < 1024:
                continue
            name = ""
            if connection.pid:
                try: name = psutil.Process(connection.pid).name()[:160]
                except (psutil.NoSuchProcess, psutil.AccessDenied): pass
            values[port] = {"port": port, "address": connection.laddr.ip,
                "pid": connection.pid or 0, "process": name, "protocol": "http"}
        return {"items": [values[port] for port in sorted(values)]}

    @app.get("/v1/diagnostics")
    async def diagnostics(after: int = 0, limit: int = 500):
        if telemetry():await asyncio.to_thread(telemetry().flush,1)
        return telemetry().snapshot(max(0,after),min(max(limit,1),500)) if telemetry() else {"records":[],"last_sequence":0}

    @app.get("/health")
    async def health():
        execution = app.state.execution
        return {"status": "ok", "runtime_bundle_digest": app.state.runtime_bundle_digest,
            "runtime_code_bundle_digest": os.environ.get("SANDBOX_RUNTIME_CODE_DIGEST", ""),
            "execution": "running" if execution and execution.result is None else "idle"}

    @app.get("/v1/runtime-bundle")
    async def runtime_bundle_status():
        return {"sha256": app.state.runtime_bundle_digest}

    @app.put("/v1/runtime-bundle")
    async def install_runtime_bundle(request: Request, sha256: str):
        if not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise HTTPException(400, "Invalid Runtime Bundle digest")
        if app.state.execution is not None and app.state.execution.result is None:
            raise HTTPException(409, "Cannot replace Runtime Bundle during an active execution")
        encoded = await request.body()
        try:
            data = decode_runtime_bundle_transport(encoded,
                request.headers.get("content-encoding", ""))
            if len(data) > MAX_BUNDLE_BYTES:
                raise ValueError("runtime_bundle_too_large")
            signature = bytes.fromhex(request.headers.get("X-Runtime-Bundle-Signature", ""))
            verify_runtime_bundle_signature(data, signature)
            value = await asyncio.to_thread(activate_runtime_bundle, data, sha256, RUN)
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc))
        app.state.runtime_bundle_digest = value["sha256"]
        event("runtime.bundle.activated", runtime_bundle_digest=value["sha256"],
            bundle_version=value["bundle_version"], file_count=value["files"])
        return value

    @app.post("/v1/configure")
    async def configure(value: Configure, request: Request = None):
        if value.runtime_bundle_digest and value.runtime_bundle_digest != app.state.runtime_bundle_digest:
            raise HTTPException(409, "Required Runtime Bundle is not active")
        app.state.log_context={"thread_id":value.thread_id,"turn_id":value.turn_id,
            "sandbox_id":value.sandbox_id,"job_id":value.job_id,
            "harness":value.harness,"model":value.model}
        if request is not None:
            request.state.log_context.clear();request.state.log_context.update(app.state.log_context)
        enrich(**app.state.log_context)
        if telemetry():
            await asyncio.to_thread(telemetry().set_remote,value.gateway_url.rstrip("/")+"/v1/telemetry",{"Authorization":"Bearer "+value.capability_token})
        token = RUN / "capability-token"
        temporary = RUN / ".capability-token.tmp"
        temporary.write_text(value.capability_token)
        os.chmod(temporary, 0o400)
        temporary.replace(token)
        os.environ["HARNESS_CAPABILITY_TOKEN_FILE"] = str(token)
        os.environ["HARNESS_GATEWAY_URL"] = value.gateway_url
        os.environ["HARNESS_KIND"] = value.harness
        os.environ["HARNESS_MODEL"] = value.model
        os.environ["HARNESS_THREAD_ID"] = value.thread_id
        os.environ["HARNESS_TURN_ID"] = value.turn_id
        os.environ["HARNESS_SANDBOX_ID"] = value.sandbox_id
        os.environ["HARNESS_JOB_ID"] = value.job_id
        os.environ["HARNESS_RUNTIME_PROFILE_ID"] = value.runtime_profile_id
        os.environ["HARNESS_CHILD_MODE"] = "1" if value.child_mode else "0"
        app.state.harness = value.harness
        previous_profile = app.state.runtime_profile_id
        previous_skills = set(app.state.skill_ids)
        app.state.runtime_profile_id = value.runtime_profile_id
        app.state.skill_ids = list(value.skill_ids)
        app.state.skill_digests = dict(value.skill_digests)
        app.state.child_mode = value.child_mode
        app.state.delegation_depth = value.delegation_depth
        runtime_home = WORKSPACE / "state" / "home"
        runtime_home.mkdir(parents=True, exist_ok=True)
        os.environ["HOME"] = str(runtime_home)
        os.environ["PIP_INDEX_URL"] = "http://mirrors.cloud.aliyuncs.com/pypi/simple/"
        os.environ["PIP_TRUSTED_HOST"] = "mirrors.cloud.aliyuncs.com"
        os.environ["npm_config_registry"] = "https://registry.npmmirror.com"
        app.state.model_start_timeout_seconds = value.model_start_timeout_seconds
        app.state.event_idle_timeout_seconds = value.event_idle_timeout_seconds
        app.state.background_job_max_seconds = value.background_job_max_seconds
        proxy = urlsplit(value.proxy_url)
        authenticated_proxy = f"{proxy.scheme}://{quote(value.capability_token, safe='')}:@{proxy.netloc}"
        os.environ["HTTP_PROXY"] = authenticated_proxy
        os.environ["HTTPS_PROXY"] = authenticated_proxy
        os.environ["http_proxy"] = authenticated_proxy
        os.environ["https_proxy"] = authenticated_proxy
        no_proxy = ",".join(filter(None, {"127.0.0.1", "localhost", proxy.hostname, urlsplit(value.gateway_url).hostname}))
        os.environ["NO_PROXY"] = no_proxy
        os.environ["no_proxy"] = no_proxy
        skills = None
        if value.defer_skill_install:
            clear_materialized_skills(value.harness, WORKSPACE)
        else:
            skills = materialize_skills(value.harness, WORKSPACE, value.skill_ids,
                value.skill_digests or None)
            event("runtime.skills.materialized", harness=value.harness,
                bundle_version=skills["bundle_version"], skill_count=skills["skill_count"])
        app.state.capability_change_frame = {
            "type": "platform_capability_change",
            "runtime_profile_id": value.runtime_profile_id,
            "previous_runtime_profile_id": previous_profile,
            "skill_ids": sorted(value.skill_ids),
            "skill_digests": dict(sorted(value.skill_digests.items())),
            "added_skill_ids": sorted(set(value.skill_ids) - previous_skills),
            "revoked_skill_ids": sorted(previous_skills - set(value.skill_ids)),
        }
        return {"configured": True, "skills": skills, "skills_deferred": value.defer_skill_install,
            "capability_change": app.state.capability_change_frame}

    @app.put("/v1/checkpoint")
    async def restore(request: Request):
        staged = await stage_checkpoint_request(request)
        try:
            def safe_checkpoint_member(member, _destination):
                try:
                    path = safe_relative(member.name)
                except ValueError as exc:
                    raise tarfile.FilterError("Invalid checkpoint member") from exc
                if path.parts[0] not in PERSISTENT:
                    raise tarfile.FilterError("Invalid checkpoint member")
                if member.issym() or member.islnk() or member.isdev():
                    return None
                current = WORKSPACE
                for part in path.parts[:-1]:
                    current = current / part
                    if current.is_symlink():
                        raise tarfile.FilterError("Checkpoint member traverses a link")
                return member
            try:
                with tarfile.open(staged, "r:*") as archive:
                    archive.extractall(WORKSPACE, filter=safe_checkpoint_member)
            except (tarfile.TarError, OSError) as exc:
                raise HTTPException(400, "checkpoint_archive_invalid") from exc
        finally:
            staged.unlink(missing_ok=True)
        if app.state.harness:
            skills = materialize_skills(app.state.harness, WORKSPACE, app.state.skill_ids,
                app.state.skill_digests or None)
            event("runtime.skills.materialized", harness=app.state.harness,
                bundle_version=skills["bundle_version"], skill_count=skills["skill_count"])
        load_jobs()
        return {"restored": True}

    @app.post("/v1/job-artifacts/stage")
    async def stage_job_artifact(value: JobArtifactStage):
        token_path = Path(os.environ.get("HARNESS_CAPABILITY_TOKEN_FILE", RUN / "capability-token"))
        gateway_url = os.environ.get("HARNESS_GATEWAY_URL", "").rstrip("/")
        if not token_path.is_file() or not gateway_url:
            raise HTTPException(409, "Job capability is not configured")
        token = token_path.read_text().strip()
        try:
            encoded = token.split(".", 1)[0]
            claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise HTTPException(409, "Job capability token is malformed") from exc
        if (claims.get("scope") != "job" or claims.get("job_id") != value.job_id
                or int(claims.get("generation") or 0) != value.generation):
            raise HTTPException(403, "Artifact identity is outside Job capability")
        try:
            staged = await asyncio.to_thread(stage_regular_file, Path(value.path),
                RUN / "artifact-stage", 2 * 1024 * 1024 * 1024)
        except (OSError, ValueError, RuntimeError) as exc:
            raise HTTPException(422, str(exc)) from exc
        headers = {
            "Authorization": "Bearer " + token,
            "X-Updream-Thread-Id": str(claims.get("thread_id", "")),
            "X-Updream-Turn-Id": str(claims.get("turn_id", "")),
            "X-Updream-Group-Id": str(claims.get("group_id", "")),
            "X-Updream-Job-Id": value.job_id,
            "X-Updream-Job-Generation": str(value.generation),
            "X-Updream-Source-Sandbox": str(claims.get("source_identity", "")),
            "X-Updream-Descriptor-Id": str(claims.get("descriptor_id", "")),
            "X-Updream-Descriptor-Version": str(claims.get("descriptor_version", "")),
            "X-Updream-Descriptor-Digest": str(claims.get("descriptor_digest", "")),
            "X-Updream-Operation": "emit",
            "X-Updream-Artifact-Ids": json.dumps(claims.get("artifact_ids", [])),
            "X-Updream-Skill-Ids": json.dumps(claims.get("skill_ids", [])),
            "X-Updream-Capability-Id": str((claims.get("capability_ids") or [""])[0]),
            "X-Updream-Operation-Id": str((claims.get("operation_ids") or [""])[0]),
            "X-Updream-Output-Scope": f"{value.artifact_role}:{value.output_disposition}",
            "X-Updream-Child-Profile-Id": str(claims.get("child_profile_id", "")),
            "X-Updream-Runtime-Profile-Id": str(claims.get("runtime_profile_id", "")),
        }
        async def content():
            with staged.open("rb") as stream:
                while chunk := await asyncio.to_thread(stream.read, 1024 * 1024):
                    yield chunk
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(3600, connect=10), trust_env=False) as client:
                response = await client.post(gateway_url + "/v1/job-artifacts/emit", headers=headers,
                    params={"artifact_role":value.artifact_role,"item_key":value.item_key,
                        "position":value.position,"filename":value.filename,
                        "output_disposition":value.output_disposition,"mime":value.mime},
                    content=content())
            if not response.is_success:
                raise HTTPException(response.status_code, response.text[:2000])
            return response.json()
        finally:
            staged.unlink(missing_ok=True)

    @app.put("/v1/attachments/file")
    async def put_attachment(request: Request, name: str):
        if (not name or name in {".", ".."} or Path(name).name != name or len(name) > 255
                or any(character in name for character in '/\\"\r\n\0')
                or any(ord(character) < 32 or ord(character) == 127 for character in name)):
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

    @app.put("/v1/project-files/file")
    async def put_project_file(request: Request, path: str, size: int):
        try:
            relative = safe_relative(path)
            if relative.parts[0] not in {"assets", "artifacts"}:
                raise ValueError
            project = (WORKSPACE / "project").resolve()
            target = project / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.parent.resolve().is_relative_to(project):
                raise ValueError
        except ValueError:
            raise HTTPException(400, "Invalid managed project path")
        maximum=2*1024*1024*1024
        if size<0 or size>maximum:raise HTTPException(413,"Project file too large")
        if "content-length" in request.headers:
            try:declared=int(request.headers["content-length"])
            except ValueError:raise HTTPException(400,"Invalid Content-Length")
            if declared!=size:raise HTTPException(400,"Project file length mismatch")
        temporary = target.with_name("." + target.name + ".tmp")
        written=0;digest=hashlib.sha256()
        try:
            with temporary.open("wb") as stream:
                async for data in request.stream():
                    written+=len(data)
                    if written>size or written>maximum:raise HTTPException(413,"Project file too large")
                    digest.update(data);stream.write(data)
            if written!=size:raise HTTPException(400,"Project file length mismatch")
            os.chmod(temporary, 0o600);temporary.replace(target)
        finally:temporary.unlink(missing_ok=True)
        return {"path": "/workspace/project/" + relative.as_posix(), "bytes": written,
            "sha256":digest.hexdigest()}

    def managed_project_target(path: str) -> Path:
        relative = safe_relative(path)
        if relative.parts[0] not in {"assets", "artifacts"}:
            raise ValueError("Invalid managed project path")
        project = (WORKSPACE / "project").resolve()
        target = project / relative
        current = project
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("Managed project path traverses a link")
        if not target.resolve().is_relative_to(project):
            raise ValueError("Invalid managed project path")
        return target

    @app.get("/v1/project-files/file")
    async def get_project_file(path: str):
        try:
            target = managed_project_target(path)
            info = target.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("Managed project file is not regular")
        except (ValueError, OSError):
            raise HTTPException(404, "Managed project file not found")
        return FileResponse(target, media_type=mimetypes.guess_type(target.name)[0] or "application/octet-stream")

    def build_project_file_manifest() -> dict:
        project = (WORKSPACE / "project").resolve(); items = []
        for root_name in ("assets", "artifacts"):
            root = project / root_name
            if not root.is_dir() or root.is_symlink():
                continue
            for candidate in sorted(root.rglob("*"), key=lambda value: value.as_posix()):
                try:
                    relative = candidate.relative_to(project).as_posix()
                    target = managed_project_target(relative); info = target.stat()
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        continue
                    with target.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    items.append({"path": relative, "size": info.st_size, "sha256": digest,
                        "mime": mimetypes.guess_type(target.name)[0] or "application/octet-stream"})
                except (ValueError, OSError):
                    continue
                if len(items) >= 50_000:
                    return {"items": items, "truncated": True}
        return {"items": items, "truncated": False}

    @app.get("/v1/project-files/manifest")
    async def project_file_manifest():
        return await asyncio.to_thread(build_project_file_manifest)

    @app.post("/v1/project-files/preserve")
    async def preserve_project_files(value: ManagedPreserveRequest):
        def copy_files() -> dict:
            project = (WORKSPACE / "project").resolve()
            destination = project / safe_relative(value.destination)
            if not destination.resolve().is_relative_to((project / "work").resolve()):
                raise ValueError("Managed edits must be preserved under project/work")
            copied = []
            for path in value.paths:
                try:
                    source = managed_project_target(path); info = source.stat()
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        continue
                    target = destination / safe_relative(path)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
                    copied.append({"source": path, "path": target.relative_to(project).as_posix(),
                        "size": info.st_size})
                except (ValueError, OSError):
                    continue
            return {"destination": value.destination, "items": copied}
        try:
            return await asyncio.to_thread(copy_files)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.post("/v1/project-files/reset")
    async def reset_project_files():
        project = (WORKSPACE / "project").resolve()
        for name in ("assets", "artifacts"):
            target = project / name
            if target.is_symlink():
                target.unlink()
            elif target.exists():
                shutil.rmtree(target)
            target.mkdir(parents=True, exist_ok=True)
        return {"reset": True}

    @app.get("/v1/checkpoint")
    async def checkpoint():
        target = WORKSPACE / "tmp" / "checkpoint.tar"
        def include_safe(member: tarfile.TarInfo):
            if member.issym() or member.islnk() or member.isdev():
                return None
            parts = PurePosixPath(member.name).parts
            if len(parts) >= 2 and parts[0] == "project" and parts[1] in {"assets", "artifacts", "outputs"}:
                return None
            return member
        with tarfile.open(target, "w") as archive:
            for name in PERSISTENT:
                archive.add(WORKSPACE / name, arcname=name, recursive=True,
                    filter=include_safe)
        return FileResponse(target, media_type="application/x-tar")

    def fingerprint(value: TurnRequest) -> str:
        body = value.model_dump(exclude={"execution_id", "input_id", "after_sequence"})
        return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

    async def submit_input(execution: Execution, value: TurnRequest, *, initial: bool) -> bool:
        async with app.state.write_lock:
            digest = fingerprint(value)
            if value.input_id in execution.received_input_ids:
                if execution.input_fingerprints.get(value.input_id) != digest:
                    raise HTTPException(409, "Input ID already has different content")
                event("runtime.input.replayed",execution_id=execution.id,input_id=value.input_id)
                return False
            if (not execution.accepting_inputs or execution.result is not None
                    or execution.process.returncode is not None):
                raise HTTPException(409, "Execution is no longer accepting input")
            if not initial and not execution.selected.streaming_input:
                raise HTTPException(409, "Harness does not have a live input stream")
            change_frame = app.state.capability_change_frame
            prefix = ((json.dumps(change_frame, ensure_ascii=False, sort_keys=True) + "\n\n")
                if change_frame else "")
            prompt = prefix + (runtime_guidance() if initial else "") + value.message
            if value.attachments:
                prompt += "\n\nAttached files are available in /workspace/inbox. Their immutable records are listed in attachments.json."
                (WORKSPACE / "inbox" / "attachments.json").write_text(json.dumps(value.attachments, ensure_ascii=False))
            selected = execution.selected
            payload = (control_user_message(prompt, value.input_id) if selected.control_input else
                streaming_user_message(prompt) if selected.streaming_input else prompt.encode())
            # Reserve before drain: a failed transport acknowledgement cannot authorize replay.
            execution.process.stdin.write(payload)
            execution.received_input_ids.add(value.input_id)
            execution.input_fingerprints[value.input_id] = digest
            execution.pending_input_ids.append(value.input_id)
            app.state.pending_inputs = len(execution.pending_input_ids)
            await execution.process.stdin.drain()
            if change_frame is app.state.capability_change_frame:
                app.state.capability_change_frame = None
            event("runtime.input.submitted",execution_id=execution.id,input_id=value.input_id)
            if not selected.streaming_input:
                execution.process.stdin.close()
            return True

    async def start_execution_locked(value: TurnRequest) -> tuple[Execution, bool, bool]:
        execution = app.state.execution
        if execution is not None and execution.id != value.execution_id and execution.result is None:
            raise HTTPException(409, "Another Harness execution is already running")
        created = execution is None or execution.id != value.execution_id
        harness = os.environ.get("HARNESS_KIND", "claude")
        if created:
            reset_outputs(value.execution_id.split(":", 1)[0])
            model = os.environ.get("HARNESS_MODEL", "default")
            os.environ["HARNESS_REASONING_EFFORT"] = value.reasoning_effort
            selected = command(harness, model, value.native_thread_id)
            environment = {**os.environ, **selected.environment, "HOME": "/workspace/state/home", "PWD": "/workspace/project"}
            Path(environment["HOME"]).mkdir(parents=True, exist_ok=True)
            process = await asyncio.create_subprocess_exec(*selected.argv, cwd=WORKSPACE / "project", env=environment,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                start_new_session=True, limit=HARNESS_JSONL_LIMIT)
            execution = Execution(id=value.execution_id, process=process, selected=selected,
                native_thread_id=value.native_thread_id,
                project_output_snapshot=project_output_snapshot())
            app.state.execution = execution
            app.state.process = process
            app.state.process_command = selected
        try:
            submitted = await submit_input(execution, value, initial=created)
        except Exception:
            if created:
                await abort_execution(execution)
                await finish_execution(execution, {"status": "failed", "error_code": "input_delivery_failed",
                    "error_detail": "Initial input could not be delivered", "input_ids": execution_input_ids(execution)})
            raise
        if created:
            execution.reader_task = asyncio.create_task(read_execution(execution, harness),
                name="harness-reader-" + value.execution_id)
        return execution, created, submitted

    @app.post("/v1/executions/start")
    async def start_execution(value: TurnRequest):
        async with app.state.execution_lock:
            execution, created, submitted = await start_execution_locked(value)
            return {"execution_id":execution.id,"accepted":True,"created":created,
                "replayed":not submitted,"input_id":value.input_id,
                "native_thread_id":execution.native_thread_id}

    @app.post("/v1/turns")
    async def run_turn(value: TurnRequest):
        async with app.state.execution_lock:
            execution, _, _ = await start_execution_locked(value)
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
        if app.state.child_mode:
            raise HTTPException(409, "Child Harness does not accept steer input")
        async with app.state.execution_lock:
            execution = app.state.execution
            if not execution or execution.id != value.execution_id:
                raise HTTPException(409, "Execution is no longer accepting input")
            submitted = await submit_input(execution, value, initial=False)
            mode = "native_steer" if os.environ.get("HARNESS_KIND") == "codex" else (
                "native_safe_point" if os.environ.get("HARNESS_KIND") == "deepseek" else "native_stream")
            return {"accepted": True, "mode": mode, "replayed": not submitted}

    @app.post("/v1/interactions/{interaction_id}")
    async def respond_interaction(interaction_id: str, request: Request):
        response = await request.json()
        if not isinstance(response, dict) or response.get("action") not in {"approve", "reject", "answer"}:
            raise HTTPException(422, "Invalid interaction action")
        async with app.state.execution_lock, app.state.interaction_response_lock:
            execution = app.state.execution
            if not execution or interaction_id not in execution.interaction_ids:
                raise HTTPException(409, "Interaction does not belong to the current execution")
            native_id = execution.interaction_ids[interaction_id]
            recorded = execution.interaction_responses.get(interaction_id)
            if recorded is not None:
                if recorded["response"] != response:
                    raise HTTPException(409, "Interaction already has a different response")
                if recorded["state"] == "delivered":
                    return {"accepted": True, "replayed": True}
                raise HTTPException(409, "interaction_delivery_unknown")
            if execution.result is not None or not execution.accepting_inputs:
                raise HTTPException(409, "Execution is no longer accepting responses")
            waiter = app.state.interaction_waiters.get(native_id)
            if waiter is not None:
                if not waiter.done():
                    waiter.set_result(response)
                execution.interaction_responses[interaction_id] = {
                    "response":response,"state":"delivered"}
                return {"accepted": True}
            process, selected = execution.process, execution.selected
            if process.returncode is not None or not selected.control_input:
                raise HTTPException(409, "Harness does not support a live native interaction")
            async with app.state.write_lock:
                if not execution.accepting_inputs:
                    raise HTTPException(409, "Execution is no longer accepting responses")
                execution.interaction_responses[interaction_id] = {
                    "response":response,"state":"reserved"}
                try:
                    process.stdin.write(control_interaction_response(native_id, response))
                except BaseException:
                    execution.interaction_responses.pop(interaction_id,None)
                    raise
                try:
                    await process.stdin.drain()
                except BaseException as exc:
                    execution.interaction_responses[interaction_id]["state"]="unknown"
                    raise HTTPException(409,"interaction_delivery_unknown") from exc
                execution.interaction_responses[interaction_id]["state"]="delivered"
            return {"accepted": True}

    @app.post("/v1/native-interactions")
    async def start_native_interaction(value: NativeInteractionCreate):
        execution = app.state.execution
        if execution is None or execution.result is not None or not execution.accepting_inputs:
            raise HTTPException(409, "No active Harness execution")
        if app.state.child_mode:
            await finish_execution(execution, {"status":"blocked",
                "native_thread_id":execution.native_thread_id,
                "error_code":"child_requires_parent_input",
                "error_detail":value.prompt,
                "missing_fields":[question.get("id","") for question in value.questions],
                "questions":[question.get("question",question.get("text","")) for question in value.questions],
                "input_ids":execution_input_ids(execution)})
            await abort_execution(execution)
            return {"interaction_id":"","action":"blocked","blocked":True}
        identity = "runtime-" + uuid.uuid4().hex
        waiter = asyncio.get_running_loop().create_future()
        app.state.interaction_waiters[identity] = waiter
        try:
            await publish(execution, {"kind": "waiting_user", "payload": {
                "interaction_id": identity, "interaction_kind": value.kind,
                "prompt": value.prompt, "questions": value.questions, "options": value.options,
            }})
            response = await waiter
            return {"interaction_id": identity, **response}
        finally:
            app.state.interaction_waiters.pop(identity, None)

    def background_problem(status:int,code:str,message:str,*,retryable:bool=False,
                           allowed_actions:list[str]|None=None,**detail):
        raise HTTPException(status,{"code":code,"message":message,"retryable":retryable,
            "allowed_actions":allowed_actions or [],**detail})

    @app.post("/v1/background-jobs")
    async def start_background_job(value: BackgroundJobCreate):
        project = (WORKSPACE / "project").resolve()
        cwd = Path(value.cwd).resolve()
        if not cwd.is_relative_to(project) or not cwd.is_dir():
            background_problem(422,"background_cwd_invalid",
                "Background job cwd must be an existing directory inside /workspace/project")
        async with app.state.write_lock:
            execution = app.state.execution
            if execution is None or execution.result is not None or not execution.accepting_inputs:
                background_problem(409,"background_execution_inactive",
                    "No active Harness execution can accept a Sandbox-local background job")
            execution_jobs=[job for job in app.state.background_jobs.values()
                if job.execution_id==execution.id]
            replacement_targets=unresolved_replacement_targets(execution_jobs)
            if replacement_targets and value.replaces not in {job.id for job in replacement_targets}:
                background_problem(409,"background_replacement_required",
                    "Replace the latest unresolved failed background job before starting unrelated work",
                    allowed_actions=["replace","status"],
                    replacement_target_ids=[job.id for job in replacement_targets])
            if value.replaces:
                previous = app.state.background_jobs.get(value.replaces)
                if not previous or previous.execution_id != execution.id:
                    background_problem(422,"replacement_target_invalid",
                        "Replacement job must belong to the active execution")
                if previous.status not in {"failed", "cancelled", "interrupted", "timed_out"}:
                    background_problem(422,"replacement_target_not_terminal",
                        "Replacement target must be a terminal unsuccessful job",
                        allowed_actions=["wait","cancel"],job_status=previous.status)
            identity = uuid.uuid4().hex
            timeout_seconds=min(value.timeout_seconds,app.state.background_job_max_seconds)
            deadline_at=time.time()+timeout_seconds
            job = BackgroundJob(id=identity, name=value.name, argv=value.argv, cwd=str(cwd),
                execution_id=execution.id, replaces=value.replaces, status="starting",
                log_path=str(jobs_root / (identity + ".log")),
                supervisor_receipt=str(jobs_root/(identity+'.supervisor.json')),
                timeout_seconds=timeout_seconds,
                terminate_grace_seconds=value.terminate_grace_seconds,
                deadline_at=deadline_at)
            app.state.background_jobs[job.id] = job
            try:persist_job(job)
            except BaseException:
                app.state.background_jobs.pop(job.id,None)
                raise
            process=None
            try:
                spawn=asyncio.create_task(asyncio.create_subprocess_exec(sys.executable,
                    '-m','sandbox_runtime.background_supervisor','--receipt',
                    job.supervisor_receipt,'--deadline-at',str(job.deadline_at),
                    '--terminate-grace',str(job.terminate_grace_seconds),'--',*value.argv,
                    cwd=cwd,env=os.environ.copy(),stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,start_new_session=True,
                    limit=HARNESS_JSONL_LIMIT),name="background-spawn-"+job.id)
                try:
                    process=await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    process=await spawn
                    raise
                job.process=process;job.pid=process.pid
                receipt={}
                for _ in range(200):
                    try:receipt=json.loads(Path(job.supervisor_receipt).read_text())
                    except (OSError,ValueError):await asyncio.sleep(.01);continue
                    break
                if not receipt:raise RuntimeError('background_supervisor_receipt_missing')
                if int(receipt.get('pid') or 0)!=process.pid:
                    raise RuntimeError('background_supervisor_identity_mismatch')
                job.pid_start_time=str(receipt.get('pid_start_time') or '')
                job.boot_id=str(receipt.get('boot_id') or '');job.status="running"
                persist_job(job)
                job.task=asyncio.create_task(watch_job(job),
                    name="background-job-"+job.id)
                event("runtime.background.started",job_id=job.id,
                    execution_id=job.execution_id)
                await publish(execution,{"kind":"step_started","payload":{
                    "name":"后台任务 · "+job.name,"background_job_id":job.id,
                    "pid":job.pid,"replaces":job.replaces}})
                return job.public()
            except BaseException as exc:
                if process and process.returncode is None:
                    job.status="cancelling"
                    try:persist_job(job)
                    except OSError:pass
                    job.process=process
                    await stop_background_supervisor(job)
                if job.task:
                    await asyncio.gather(job.task,return_exceptions=True)
                if job.status not in {"completed","failed","cancelled","timed_out"}:
                    job.status="failed";job.returncode=(process.returncode if process else None)
                    job.completed_at=datetime.now(timezone.utc).isoformat()
                    job.recovery_detail="start_failed:"+type(exc).__name__
                    try:persist_job(job)
                    except OSError:pass
                raise

    @app.get("/v1/background-jobs")
    async def list_background_jobs():
        jobs=list(app.state.background_jobs.values())
        return {"items":[job.public(log_tail=job_log_tail(job)) for job in jobs],
            "unresolved":[job.public(log_tail=job_log_tail(job))
                for job in unresolved_background_jobs(jobs)],
            "replacement_targets":[job.public(log_tail=job_log_tail(job))
                for job in unresolved_replacement_targets(jobs)]}

    @app.get("/v1/background-jobs/{job_id}/wait")
    async def wait_background_job(job_id:str,timeout_seconds:float|None=None,
                                  heartbeat_seconds:float=10):
        job=app.state.background_jobs.get(job_id)
        if not job:
            background_problem(404,"background_job_not_found","Background job not found")
        if timeout_seconds is not None and timeout_seconds<=0:
            background_problem(422,"wait_timeout_invalid","Wait timeout must be greater than zero")
        heartbeat_seconds=min(max(heartbeat_seconds,.01),30)
        async def rows():
            started=time.monotonic()
            while True:
                current=job.public(log_tail=job_log_tail(job))
                if job.status in {"completed","failed","cancelled","interrupted","timed_out"}:
                    yield json.dumps({"type":"terminal","job":current},ensure_ascii=False)+"\n"
                    return
                remaining=(None if timeout_seconds is None else
                    max(0,timeout_seconds-(time.monotonic()-started)))
                if remaining is not None and remaining<=0:
                    yield json.dumps({"type":"wait_timeout","job":current},ensure_ascii=False)+"\n"
                    return
                interval=heartbeat_seconds if remaining is None else min(heartbeat_seconds,remaining)
                try:await asyncio.wait_for(job.done_event.wait(),interval)
                except asyncio.TimeoutError:
                    current=job.public(log_tail=job_log_tail(job))
                    if job.status in {"completed","failed","cancelled","interrupted","timed_out"}:
                        yield json.dumps({"type":"terminal","job":current},ensure_ascii=False)+"\n"
                        return
                    if timeout_seconds is not None and time.monotonic()-started>=timeout_seconds:
                        yield json.dumps({"type":"wait_timeout","job":current},ensure_ascii=False)+"\n"
                        return
                    yield json.dumps({"type":"heartbeat","job":current},ensure_ascii=False)+"\n"
        return StreamingResponse(rows(),media_type="application/x-ndjson")

    @app.post("/v1/background-jobs/{job_id}/cancel")
    async def cancel_background_job(job_id: str):
        job = app.state.background_jobs.get(job_id)
        if not job:
            background_problem(404,"background_job_not_found","Background job not found")
        await cancel_job(job)
        return job.public(log_tail=job_log_tail(job))

    @app.post("/v1/cancel")
    async def cancel():
        async with app.state.execution_lock:
            execution = app.state.execution
            event("runtime.cancel.requested",execution_id=execution.id if execution else "")
            if execution and execution.result is None:
                execution.cancel_requested = True
                await abort_execution(execution)
                await finish_execution(execution, {"status": "cancelled", "error_code": "cancelled",
                    "native_thread_id": execution.native_thread_id,
                    "input_ids": execution_input_ids(execution)})
            return {"cancelled": True,
                "native_thread_id": execution.native_thread_id if execution else "",
                "execution_id": execution.id if execution else ""}

    @app.post("/v1/operations/media-render")
    async def media_render_operation(value:MediaRenderOperation):
        if not app.state.child_mode or app.state.runtime_profile_id not in {"media-cpu","primary-full-cpu"}:
            raise HTTPException(403,"Controlled media Operation requires an authorized media Runtime Profile")
        def frozen_input(raw:str)->Path:
            relative=safe_relative(raw)
            if len(relative.parts)<3 or relative.parts[:2]!=("assets","inputs"):
                raise HTTPException(422,"Operation input must be a frozen Job Artifact")
            source=WORKSPACE/"project"/relative
            try:
                if (not source.is_file() or source.is_symlink()
                        or not source.resolve().is_relative_to((WORKSPACE/"project/assets/inputs").resolve())):
                    raise ValueError
            except (OSError,ValueError):raise HTTPException(404,"Operation input is unavailable")
            return source
        if value.operation=="media.concat_sequence":
            source=frozen_input(value.source_path);music=frozen_input(value.music_path)
        else:source=frozen_input(value.input_path)
        target=OUTPUTS/value.output_filename;target.unlink(missing_ok=True)
        if value.operation in {"image.resize","video.transcode"} and (not value.width or not value.height):
            raise HTTPException(422,"Image and video Operations require non-zero dimensions")
        if value.operation=="media.concat_sequence":
            work=WORKSPACE/"tmp"/("concat-sequence-"+uuid.uuid4().hex);work.mkdir(parents=True)
            try:
                probe=await asyncio.create_subprocess_exec("ffprobe","-v","error","-show_entries",
                    "format=duration","-of","json",str(source),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                probe_out,_=await probe.communicate();source_duration=float(
                    json.loads(probe_out).get("format",{}).get("duration") or 0)
                if probe.returncode or source_duration<value.segment_seconds:
                    raise HTTPException(422,"Sequence source is shorter than one segment")
                segments=[];font="/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                for index in range(value.segment_count):
                    segment=work/f"segment_{index+1:03d}.mp4";offset=(index*value.segment_seconds)%source_duration
                    label=f"{index+1:02d}";filter_value=(
                        f"scale={value.width}:{value.height}:force_original_aspect_ratio=decrease,"
                        f"pad={value.width}:{value.height}:(ow-iw)/2:(oh-ih)/2,setsar=1,"
                        f"drawbox=x=24:y=24:w=100:h=64:color=black@0.65:t=fill,"
                        f"drawtext=fontfile={font}:text='{label}':x=44:y=32:fontsize=42:fontcolor=white")
                    process=await asyncio.create_subprocess_exec("ffmpeg","-nostdin","-v","error","-y",
                        "-ss",f"{offset:.6f}","-i",str(source),"-t",f"{value.segment_seconds:.6f}",
                        "-an","-vf",filter_value,"-r",str(value.fps),"-c:v","libx264","-pix_fmt","yuv420p",
                        str(segment),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                    output,_=await asyncio.wait_for(process.communicate(),timeout=1800)
                    if process.returncode or not segment.is_file():
                        raise HTTPException(422,"Sequence segment render failed")
                    segments.append(segment)
                concat=work/"concat.txt";concat.write_text("".join(
                    "file '"+path.name+"'\n" for path in segments))
                total=value.segment_count*value.segment_seconds
                process=await asyncio.create_subprocess_exec("ffmpeg","-nostdin","-v","error","-y",
                    "-f","concat","-safe","0","-i",str(concat),"-stream_loop","-1","-i",str(music),
                    "-t",f"{total:.6f}","-map","0:v:0","-map","1:a:0","-c:v","copy","-c:a","aac",
                    "-shortest",str(target),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                output,_=await asyncio.wait_for(process.communicate(),timeout=7200)
                if process.returncode or not target.is_file():raise HTTPException(422,"Sequence concat failed")
                probe=await asyncio.create_subprocess_exec("ffprobe","-v","error","-show_streams",
                    "-show_format","-of","json",str(target),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                probe_out,_=await probe.communicate();metadata=json.loads(probe_out)
                streams=metadata.get("streams",[]);video=next((row for row in streams
                    if row.get("codec_type")=="video"),{});audio=next((row for row in streams
                    if row.get("codec_type")=="audio"),{})
                output_duration=float(metadata.get("format",{}).get("duration") or 0)
                if (probe.returncode or abs(output_duration-total)>.2 or int(video.get("width") or 0)!=value.width
                        or int(video.get("height") or 0)!=value.height or not audio):
                    raise HTTPException(422,"Sequence output contract failed")
                with target.open("rb") as stream:digest=hashlib.file_digest(stream,"sha256").hexdigest()
                report={"schema":1,"status":"succeeded","segment_count":value.segment_count,
                    "segment_seconds":value.segment_seconds,"source_duration":source_duration,
                    "sequence":[{"position":index+1,"label":f"{index+1:02d}",
                        "source_offset":round((index*value.segment_seconds)%source_duration,6)}
                        for index in range(value.segment_count)],
                    "output":{"name":target.name,"duration":output_duration,"width":value.width,
                        "height":value.height,"fps":value.fps,"audio":True,"sha256":digest,
                        "size":target.stat().st_size}}
                report_path=OUTPUTS/"compiler-report.json"
                report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
                with report_path.open("rb") as stream:report_digest=hashlib.file_digest(stream,"sha256").hexdigest()
                return {"outputs":[{"role":"rendered_media","path":target.name,
                    "size":target.stat().st_size,"sha256":digest,"mime":"video/mp4",
                    "validation_receipts":[{"validator":"video","status":"pass",
                        "artifact_digests":[digest],"evidence":report["output"]}]},
                    {"role":"compiler_report","path":report_path.name,"size":report_path.stat().st_size,
                        "sha256":report_digest,"mime":"application/json"}]}
            finally:shutil.rmtree(work,ignore_errors=True)
        if value.operation=="image.resize":
            argv=["ffmpeg","-hide_banner","-loglevel","error","-y","-i",str(source),
                "-vf",f"scale={value.width}:{value.height}:force_original_aspect_ratio=decrease,"
                f"pad={value.width}:{value.height}:(ow-iw)/2:(oh-ih)/2:color=black@0","-frames:v","1",str(target)]
        elif value.operation=="video.transcode":
            argv=["ffmpeg","-hide_banner","-loglevel","error","-y","-i",str(source),
                "-vf",f"scale={value.width}:{value.height}:force_original_aspect_ratio=decrease,"
                f"pad={value.width}:{value.height}:(ow-iw)/2:(oh-ih)/2,setsar=1","-r",str(value.fps),
                "-c:v","libx264","-pix_fmt","yuv420p","-c:a","aac",str(target)]
        else:
            if target.suffix.lower()!="."+value.format:
                raise HTTPException(422,"Audio output filename must match format")
            probe=await asyncio.create_subprocess_exec("ffprobe","-v","error","-select_streams","a:0",
                "-show_entries","format=duration:stream=sample_rate,channels,channel_layout",
                "-of","json",str(source),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            probe_out,probe_err=await probe.communicate()
            if probe.returncode:raise HTTPException(422,"Audio input probe failed")
            source_probe=json.loads(probe_out);stream=(source_probe.get("streams") or [None])[0]
            if not stream:raise HTTPException(422,"Audio input has no audio stream")
            metadata={"duration":float((source_probe.get("format") or {}).get("duration") or 0),
                "sample_rate":int(stream["sample_rate"]),"channels":int(stream["channels"]),
                "channel_layout":stream.get("channel_layout") or ""}
            filters=[]
            if value.highpass_hz:filters.append(f"highpass=f={value.highpass_hz:g}")
            if value.denoise:filters.append("afftdn=nf=-25")
            if value.gain_db:filters.append(f"volume={value.gain_db:g}dB")
            if value.fade_in:filters.append(f"afade=t=in:st=0:d={min(metadata['duration'],value.fade_in):g}")
            if value.fade_out:filters.append(f"afade=t=out:st={max(0,metadata['duration']-value.fade_out):g}:d={min(metadata['duration'],value.fade_out):g}")
            measurement=None
            if value.target_lufs is not None:
                first=",".join([*filters,
                    f"loudnorm=I={value.target_lufs:g}:TP={value.true_peak_db:g}:LRA=11:print_format=json"])
                measured=await asyncio.create_subprocess_exec("ffmpeg","-nostdin","-hide_banner","-v","info",
                    "-i",str(source),"-map","0:a:0","-af",first,"-f","null","-",
                    stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
                _,measured_err=await asyncio.wait_for(measured.communicate(),timeout=1800)
                matches=re.findall(rb"\{[^{}]+\}",measured_err,re.S)
                if measured.returncode or not matches:raise HTTPException(422,"Loudness measurement failed")
                measurement=json.loads(matches[-1]);required=("input_i","input_tp","input_lra",
                    "input_thresh","target_offset")
                if not all(key in measurement for key in required):
                    raise HTTPException(422,"Loudness measurement is incomplete")
                filters.append(f"loudnorm=I={value.target_lufs:g}:TP={value.true_peak_db:g}:LRA=11:"
                    f"measured_I={measurement['input_i']}:measured_TP={measurement['input_tp']}:"
                    f"measured_LRA={measurement['input_lra']}:measured_thresh={measurement['input_thresh']}:"
                    f"offset={measurement['target_offset']}:linear=true")
                measurement={key:measurement[key] for key in required}
            codec={"wav":["-c:a","pcm_s16le"],"mp3":["-c:a","libmp3lame","-b:a","192k"],
                "m4a":["-c:a","aac","-b:a","192k"]}[value.format]
            argv=["ffmpeg","-nostdin","-hide_banner","-loglevel","error","-y","-i",str(source),
                "-map","0:a:0","-vn"]
            if filters:argv += ["-af",",".join(filters)]
            argv += codec+["-ar",str(metadata["sample_rate"]),"-ac",str(metadata["channels"]),str(target)]
        process=await asyncio.create_subprocess_exec(*argv,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        try:output,_=await asyncio.wait_for(process.communicate(),timeout=7200)
        except asyncio.TimeoutError:
            process.kill();await process.wait();raise HTTPException(504,"Controlled media Operation timed out")
        if process.returncode or not target.is_file():
            event("runtime.operation.failed",operation=value.operation,returncode=process.returncode)
            raise HTTPException(422,"Controlled media Operation failed")
        with target.open("rb") as stream:digest=hashlib.file_digest(stream,"sha256").hexdigest()
        if value.operation=="audio.process":
            output_probe=await asyncio.create_subprocess_exec("ffprobe","-v","error","-select_streams","a:0",
                "-show_entries","format=duration:stream=sample_rate,channels,channel_layout",
                "-of","json",str(target),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            probe_out,_=await output_probe.communicate();result_probe=json.loads(probe_out)
            result_stream=(result_probe.get("streams") or [None])[0]
            output_metadata={"duration":float((result_probe.get("format") or {}).get("duration") or 0),
                "sample_rate":int(result_stream["sample_rate"]),"channels":int(result_stream["channels"]),
                "channel_layout":result_stream.get("channel_layout") or ""}
            if abs(output_metadata["duration"]-metadata["duration"])>.2:
                raise HTTPException(422,"Audio Operation changed duration")
            report={"schema":1,"status":"succeeded","parameters":value.model_dump(exclude={"input_path"}),
                "source":metadata,"filters":filters,"measurement":measurement,
                "output":{**output_metadata,"name":target.name,"size":target.stat().st_size,"sha256":digest},
                "normalization_applied":measurement is not None,"channel_policy":"preserve",
                "sample_rate_policy":"preserve"}
            report_path=OUTPUTS/"audio-process-report.json"
            report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
            with report_path.open("rb") as stream:report_digest=hashlib.file_digest(stream,"sha256").hexdigest()
            return {"outputs":[{"role":"processed_audio","path":target.name,"size":target.stat().st_size,
                "sha256":digest,"mime":mimetypes.guess_type(target.name)[0] or "application/octet-stream",
                "validation_receipts":[{"validator":"audio","status":"pass",
                    "artifact_digests":[digest],"evidence":output_metadata}]},
                {"role":"measurement_report","path":report_path.name,"size":report_path.stat().st_size,
                "sha256":report_digest,"mime":"application/json"}]}
        return {"path":value.output_filename,"size":target.stat().st_size,"sha256":digest,
            "mime":mimetypes.guess_type(target.name)[0] or "application/octet-stream"}

    def artifact_path(value: str) -> Path:
        relative = safe_relative(value)
        target = OUTPUTS / relative
        # Check the lexical path before resolve() so links cannot disappear from
        # validation. Special files and hard links are never exported.
        current = OUTPUTS
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("Artifact links are not allowed")
        info = target.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("Artifact is not a regular independent file")
        if not target.resolve().is_relative_to(OUTPUTS.resolve()):
            raise ValueError("Artifact is outside output directory")
        return target

    @app.get("/v1/artifacts")
    async def artifacts():
        paths = []
        for path in sorted(OUTPUTS.rglob("*"), key=lambda value: value.as_posix()):
            if path == OUTPUTS / ".sandbox-output-batch":
                continue
            try:
                relative = path.relative_to(OUTPUTS).as_posix()
                path = artifact_path(relative)
            except (ValueError, OSError):
                continue
            paths.append((relative, path))
            require_output_artifact_count(len(paths))
        items = []
        for relative, path in paths:
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            items.append({"path": relative, "size": path.stat().st_size, "sha256": digest, "mime": mimetypes.guess_type(path.name)[0] or "application/octet-stream"})
        return {"batch_id": OUTPUT_BATCH_MARKER.read_text().strip()
            if OUTPUT_BATCH_MARKER.is_file() else "",
            "items": items, "truncated": False}

    @app.get("/v1/artifacts/file")
    async def artifact(path: str):
        try:
            target = artifact_path(path)
        except (ValueError, OSError):
            raise HTTPException(404, "Artifact not found")
        return FileResponse(target)

    return app
