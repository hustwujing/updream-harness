from __future__ import annotations

import asyncio
import base64
from datetime import timedelta
import hashlib
import hmac
import json
import mimetypes
import time
from typing import Any

import httpx
from websockets.asyncio.client import connect as websocket_connect

from .capability import CapabilityCodec
from .domain import TERMINAL, identifier, now
from .observability import event
from .sandbox import Sandbox, SandboxNotFound
from .sandbox_contracts import SandboxHandle
from .objects import object_chunks


EXECD_PORT = 44772
BLOCKED_PREVIEW_PORTS = {22, 8080, 18080, 29880, 29881, EXECD_PORT}
WEBSOCKET_HOP_HEADERS = {
    "connection", "host", "upgrade", "proxy-connection", "keep-alive",
    "transfer-encoding", "sec-websocket-key", "sec-websocket-version",
    "sec-websocket-extensions", "sec-websocket-protocol",
}


def websocket_endpoint_headers(headers: dict[str, str]) -> dict[str, str]:
    """Keep endpoint authentication without conflicting with the WS handshake."""
    return {key: value for key, value in headers.items()
        if key.lower() not in WEBSOCKET_HOP_HEADERS}


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class TerminalTicketCodec:
    def __init__(self, key: str):
        self.key = hashlib.sha256(("terminal:" + key).encode()).digest()

    def issue(self, *, owner_id: str, thread_id: str, sandbox_id: str,
              pty_session_id: str, ttl_seconds: int) -> str:
        payload = json.dumps({"owner_id": owner_id, "thread_id": thread_id,
            "sandbox_id": sandbox_id, "pty_session_id": pty_session_id,
            "expires_at": int(time.time()) + ttl_seconds},
            separators=(",", ":"), sort_keys=True).encode()
        return _encode(payload) + "." + _encode(hmac.new(self.key, payload, hashlib.sha256).digest())

    def verify(self, token: str) -> dict:
        try:
            payload_text, signature_text = token.split(".", 1)
            payload = _decode(payload_text); signature = _decode(signature_text)
            value = json.loads(payload)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid_terminal_ticket") from exc
        if not hmac.compare_digest(signature, hmac.new(self.key, payload, hashlib.sha256).digest()):
            raise ValueError("invalid_terminal_ticket")
        if int(value.get("expires_at") or 0) <= int(time.time()):
            raise ValueError("terminal_ticket_expired")
        return value


class SandboxInspector:
    def __init__(self, settings, store, objects, pool, runtime, broker):
        self.settings = settings
        self.store = store
        self.objects = objects
        self.pool = pool
        self.runtime = runtime
        self.broker = broker
        self.capabilities = CapabilityCodec(settings.gateway_signing_key)
        self.tickets = TerminalTicketCodec(settings.session_signing_key)
        self.locks: dict[str, asyncio.Lock] = {}
        self.cleanup_task: asyncio.Task | None = None
        self.stop_event = asyncio.Event()
        self.cleanup_status = "stopped"
        self.cleanup_last_error = ""
        self.cleanup_failures = 0

    def _lock(self, thread_id: str) -> asyncio.Lock:
        return self.locks.setdefault(thread_id, asyncio.Lock())

    async def start(self) -> None:
        self.cleanup_status = "running"
        self.cleanup_task = asyncio.create_task(self._cleanup_loop(), name="sandbox-inspector-cleanup")

    async def close(self) -> None:
        self.stop_event.set()
        if self.cleanup_task:
            self.cleanup_task.cancel()
            await asyncio.gather(self.cleanup_task, return_exceptions=True)
        self.cleanup_status = "stopped"

    async def _cleanup_loop(self) -> None:
        delay = float(getattr(self.settings,"supervisor_retry_initial_seconds",1.0))
        maximum_delay = float(getattr(self.settings,"supervisor_retry_max_seconds",30.0))
        while not self.stop_event.is_set():
            try:
                controls = await self.store.expired_sandbox_controls()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.cleanup_status = "degraded"; self.cleanup_last_error = type(exc).__name__
                self.cleanup_failures += 1
                event("sandbox.control.expiry_scan_failed", level=40,
                    exception_type=type(exc).__name__, failures=self.cleanup_failures)
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, maximum_delay)
                continue
            self.cleanup_status = "running"; self.cleanup_last_error = ""
            delay = float(getattr(self.settings,"supervisor_retry_initial_seconds",1.0))
            for control in controls:
                try:
                    await self.release(control["thread_id"], control["owner_id"], expired=True)
                except Exception as exc:
                    event("sandbox.control.expiry_failed", level=40,
                        thread_id=control["thread_id"], sandbox_id=control["sandbox_id"],
                        exception_type=type(exc).__name__)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def _thread(self, thread_id: str, owner_id: str):
        value = await self.store.get_thread(thread_id, owner_id)
        if not value:
            raise KeyError("thread_not_found")
        return value

    async def _active_sandbox(self, thread_id: str) -> Sandbox | None:
        for sandbox in self.broker.active_sandboxes.values():
            if sandbox.thread_id == thread_id:
                return sandbox
        return await self.pool.get_attached(thread_id)

    async def _controlled_sandbox(self, thread_id: str, owner_id: str) -> tuple[Sandbox | None, dict | None]:
        control = await self.store.get_sandbox_control(thread_id, owner_id)
        if not control or control["state"] not in {"user_controlled", "releasing"}:
            return None, control
        if control["state"]=="user_controlled" and control["expires_at"]<=now():
            return None,control
        try:
            handle=SandboxHandle.from_mapping(control["sandbox_handle"])
            sandbox = await self.pool.get_attached(thread_id)
            if sandbox and sandbox.handle==handle:
                return sandbox, control
            sandbox = await self.pool.backend.get(handle)
            sandbox.thread_id = thread_id; sandbox.turn_id = "inspect-" + thread_id
            return sandbox, control
        except (SandboxNotFound, RuntimeError, httpx.HTTPError):
            return None, control

    async def live_sandbox(self, thread_id: str, owner_id: str) -> Sandbox | None:
        await self._thread(thread_id, owner_id)
        active = await self._active_sandbox(thread_id)
        if active:
            return active
        controlled, _ = await self._controlled_sandbox(thread_id, owner_id)
        return controlled

    async def heartbeat(self,thread_id:str,owner_id:str)->dict|None:
        control=await self.store.get_sandbox_control(thread_id,owner_id)
        if not control or control["state"]!="user_controlled":return control
        if control["expires_at"]<=now():return control
        return await self.store.touch_sandbox_control(thread_id,owner_id,
            now()+timedelta(seconds=self.settings.sandbox_control_ttl_seconds))

    async def _materialize_managed(self, thread, sandbox: Sandbox) -> None:
        await self.runtime.reset_project_files(sandbox)
        for value in await self.store.current_project_files(thread.id, thread.owner_id):
            size=await asyncio.to_thread(self.objects.size,value.object_ref,
                maximum=self.settings.max_artifact_bytes)
            if size!=value.size:
                raise ValueError("Project file does not match its immutable record")
            iterator=self.objects.iter_bytes(value.object_ref,maximum=self.settings.max_artifact_bytes,chunk_size=1024*1024)
            sentinel=object()
            async def chunks(iterator=iterator,sentinel=sentinel):
                while True:
                    chunk=await asyncio.to_thread(next,iterator,sentinel)
                    if chunk is sentinel:return
                    yield chunk
            result=await self.runtime.put_project_file_stream(sandbox,value.path,chunks(),
                size=value.size,mime=value.mime)
            if result.get("bytes")!=value.size or result.get("sha256")!=value.sha256:
                raise ValueError("Project file does not match its immutable record")

    async def _restore_new(self, thread, sandbox: Sandbox) -> None:
        if thread.checkpoint_ref and not sandbox.reused:
            archive_size = await asyncio.to_thread(self.objects.size, thread.checkpoint_ref,
                maximum=self.settings.max_checkpoint_bytes)
            await self.runtime.restore(sandbox, object_chunks(self.objects, thread.checkpoint_ref,
                maximum=self.settings.max_checkpoint_bytes), size=archive_size)
        await self._materialize_managed(thread, sandbox)

    async def restore(self, thread_id: str, owner_id: str) -> Sandbox:
        async with self._lock(thread_id):
            thread = await self._thread(thread_id, owner_id)
            turns = await self.store.list_turns(thread_id, owner_id)
            if any(turn.state not in TERMINAL for turn in turns):
                active = await self._active_sandbox(thread_id)
                if active:
                    return active
                raise RuntimeError("active_sandbox_not_ready")
            existing = await self.live_sandbox(thread_id, owner_id)
            if existing:
                return existing
            sandbox = await self.pool.acquire(thread_id=thread_id,
                turn_id="inspect-" + thread_id, harness=thread.harness, environment={})
            try:
                await self._restore_new(thread, sandbox)
            except Exception:
                await self.pool.release(sandbox, keep_attached=False)
                raise
            await self.pool.release(sandbox, keep_attached=True,
                keepalive_seconds=self.settings.session_keepalive_seconds)
            await self.store.add_sandbox_audit(thread_id, owner_id, sandbox.id, "sandbox.restored",
                {"checkpoint": bool(thread.checkpoint_ref)})
            event("sandbox.inspector.restored", thread_id=thread_id, sandbox_id=sandbox.id)
            return sandbox

    async def status(self, thread_id: str, owner_id: str) -> dict:
        thread = await self._thread(thread_id, owner_id)
        control = await self.heartbeat(thread_id,owner_id)
        sandbox = await self.live_sandbox(thread_id, owner_id)
        turns = await self.store.list_turns(thread_id, owner_id)
        active = next((turn for turn in reversed(turns) if turn.state not in TERMINAL), None)
        overview = None
        if sandbox:
            try:
                overview = await self.runtime.inspect_overview(sandbox)
            except (httpx.HTTPError, ValueError):
                overview = None
        files = await self.store.current_project_files(thread_id, owner_id)
        return {"state": "live" if overview else "unavailable" if sandbox or control else "stopped",
            "sandbox_id": sandbox.id if sandbox else "", "harness": thread.harness.value,
            "image": sandbox.image.rsplit("@", 1)[-1] if sandbox else "",
            "turn_state": active.state.value if active else "idle",
            "control_state": ("expired" if control and control["state"]=="user_controlled"
                and control["expires_at"]<=now() else control["state"] if control else "observer"),
            "release_phase":control.get("release_phase","") if control else "",
            "release_action":control.get("release_action","") if control else "",
            "control_expires_at": control["expires_at"] if control else None,
            "checkpoint_available": bool(thread.checkpoint_ref),
            "persistent_file_count": len(files), "overview": overview,
            "capabilities": {"files": bool(overview), "processes": bool(overview),
                "metrics": bool(overview), "ports": bool(overview),
                "terminal": bool(control and control["state"] == "user_controlled"
                    and control["expires_at"]>now())}}

    async def files(self, thread_id: str, owner_id: str, path: str, depth: int) -> dict:
        await self.heartbeat(thread_id,owner_id)
        sandbox = await self.live_sandbox(thread_id, owner_id)
        if sandbox:
            return await self.runtime.inspect_files(sandbox, path, depth)
        if path != "project":
            return {"root": path.split("/", 1)[0], "path": path, "items": [],
                "truncated": False, "persistent": True}
        values = await self.store.current_project_files(thread_id, owner_id)
        return {"root": "project", "path": path, "persistent": True, "truncated": False,
            "items": [{"path": "project/" + value.path, "name": value.path.rsplit("/", 1)[-1],
                "type": "file", "size": value.size, "mime": value.mime,
                "modified_at": value.created_at} for value in values]}

    async def file(self, thread_id: str, owner_id: str, path: str) -> tuple[bytes, str]:
        await self.heartbeat(thread_id,owner_id)
        sandbox = await self.live_sandbox(thread_id, owner_id)
        if not sandbox:
            raise RuntimeError("sandbox_not_running")
        return await self.runtime.inspect_file(sandbox, path)

    async def write_file(self, thread_id: str, owner_id: str, path: str,
                         data: bytes, mime: str) -> dict:
        await self.heartbeat(thread_id,owner_id)
        if len(data) > self.settings.sandbox_inspect_max_file_bytes:
            raise ValueError("sandbox_file_too_large")
        normalized = path.strip().replace("\\", "/").strip("/")
        if normalized in {"project/assets", "project/artifacts"} or normalized.startswith(
                ("project/assets/", "project/artifacts/")):
            raise RuntimeError("managed_project_file_read_only")
        sandbox, control = await self._controlled_sandbox(thread_id, owner_id)
        if not sandbox or not control or control["state"] != "user_controlled":
            raise RuntimeError("sandbox_takeover_required")
        value = await self.runtime.write_inspect_file(sandbox, path, data, mime)
        await self.store.add_sandbox_audit(thread_id, owner_id, sandbox.id, "file.updated",
            {"path_hash": hashlib.sha256(path.encode()).hexdigest()[:16], "bytes": len(data)})
        return value

    async def processes(self, thread_id: str, owner_id: str) -> dict:
        await self.heartbeat(thread_id,owner_id)
        sandbox = await self.live_sandbox(thread_id, owner_id)
        if not sandbox:
            return {"items": []}
        return await self.runtime.inspect_processes(sandbox)

    async def stop_process(self, thread_id: str, owner_id: str, pid: int) -> dict:
        await self.heartbeat(thread_id,owner_id)
        sandbox, control = await self._controlled_sandbox(thread_id, owner_id)
        if not sandbox or not control or control["state"] != "user_controlled":
            raise RuntimeError("sandbox_takeover_required")
        value = await self.runtime.stop_inspect_process(sandbox, pid)
        await self.store.add_sandbox_audit(thread_id, owner_id, sandbox.id,
            "process.stopped", {"pid": pid})
        return value

    async def ports(self, thread_id: str, owner_id: str) -> dict:
        await self.heartbeat(thread_id,owner_id)
        sandbox = await self.live_sandbox(thread_id, owner_id)
        if not sandbox:
            return {"items": []}
        return await self.runtime.inspect_ports(sandbox)

    async def managed_changes(self, thread_id: str, owner_id: str) -> dict:
        await self.heartbeat(thread_id,owner_id)
        thread = await self._thread(thread_id, owner_id)
        sandbox = await self.live_sandbox(thread_id, owner_id)
        if not sandbox:
            return {"dirty": False, "items": [], "sandbox_id": ""}
        try:
            value = await self.runtime.project_file_manifest(sandbox)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            event("sandbox.managed_manifest.unavailable", level=30,
                thread_id=thread_id, sandbox_id=sandbox.id, reason="legacy_runtime")
            return {"dirty": False, "items": [], "sandbox_id": sandbox.id,
                "supported": False}
        if value.get("truncated"):
            raise RuntimeError("managed_file_manifest_truncated")
        expected = {item.path: item for item in await self.store.current_project_files(
            thread.id, owner_id)}
        actual = {str(item["path"]): item for item in value.get("items", [])}
        changes = []
        for path in sorted(set(expected) | set(actual)):
            project_file = expected.get(path); observed = actual.get(path)
            if project_file is None:
                changes.append({"path": path, "kind": "untracked",
                    "source_type": "upload" if path.startswith("assets/") else "artifact",
                    "expected_version": None, "expected_sha256": "",
                    "actual_sha256": observed["sha256"], "size": observed["size"],
                    "mime": observed.get("mime") or "application/octet-stream"})
            elif observed is None:
                changes.append({"path": path, "kind": "missing",
                    "source_type": project_file.source_type,
                    "expected_version": project_file.version,
                    "expected_sha256": project_file.sha256, "actual_sha256": "",
                    "size": project_file.size, "mime": project_file.mime})
            elif observed["sha256"] != project_file.sha256 or observed["size"] != project_file.size:
                changes.append({"path": path, "kind": "modified",
                    "source_type": project_file.source_type,
                    "expected_version": project_file.version,
                    "expected_sha256": project_file.sha256,
                    "actual_sha256": observed["sha256"], "size": observed["size"],
                    "mime": observed.get("mime") or project_file.mime})
        return {"dirty": bool(changes), "items": changes, "sandbox_id": sandbox.id,
            "supported": True}

    async def _resolve_managed_changes(self, thread, sandbox: Sandbox, owner_id: str,
                                       changes: dict, action: str, generation: int) -> dict:
        if not changes.get("supported", True):
            return {"action": "managed_manifest_unavailable", "count": 0, "destination": ""}
        if not changes["dirty"]:
            return {"action": "none", "count": 0, "destination": ""}
        if not action:
            raise RuntimeError("managed_files_modified")
        destination = ""
        existing_paths = [item["path"] for item in changes["items"] if item["kind"] != "missing"]
        if action == "preserve_work":
            destination = f"work/recovered-managed/control-{generation}"
            if existing_paths:
                await self.runtime.preserve_project_files(sandbox, existing_paths, destination)
        elif action == "save_versions":
            updates = []
            for item in changes["items"]:
                if item["kind"] == "missing":
                    updates.append(item)
                    continue
                staged = await self.runtime.managed_project_file(sandbox, item["path"],
                    maximum=self.settings.max_artifact_bytes)
                try:
                    if staged.sha256 != item["actual_sha256"] or staged.size != item["size"]:
                        raise RuntimeError("managed_file_changed_during_release")
                    object_ref, stored_digest = await asyncio.to_thread(self.objects.put_file,
                        staged.path, digest=staged.sha256, size=staged.size)
                    if stored_digest != staged.sha256:
                        raise RuntimeError("managed_file_object_identity_mismatch")
                    updates.append({**item,"sha256":staged.sha256,"size":staged.size,
                        "object_ref":object_ref})
                finally:
                    staged.remove()
            await self.store.apply_managed_file_versions(thread.id, owner_id, updates)
        elif action != "discard":
            raise ValueError("invalid_managed_file_action")
        await self._materialize_managed(thread, sandbox)
        return {"action": action, "count": len(changes["items"]), "destination": destination}

    async def takeover(self, thread_id: str, owner_id: str, *, network_access: str,
                       interrupt: bool = False) -> dict:
        async with self._lock(thread_id):
            thread = await self._thread(thread_id, owner_id)
            turns = await self.store.list_turns(thread_id, owner_id)
            active = next((turn for turn in reversed(turns) if turn.state not in TERMINAL), None)
            if active and not interrupt:
                raise RuntimeError("active_turn_prevents_sandbox_takeover")
            if active:
                await self.broker.cancel(active.id)
                for _ in range(120):
                    current = await self.store.get_turn(active.id)
                    if current and current.state in TERMINAL:
                        break
                    await asyncio.sleep(0.5)
                else:
                    raise RuntimeError("sandbox_takeover_waiting_for_turn_cleanup")
            control = await self.store.get_sandbox_control(thread_id, owner_id)
            if control and control["state"]=="releasing":
                raise RuntimeError("sandbox_release_in_progress")
            if control and control["state"] == "user_controlled" and control["expires_at"] > now():
                return control
            sandbox = await self.pool.acquire(thread_id=thread_id,
                turn_id="inspect-" + thread_id, harness=thread.harness, environment={})
            try:
                token, claims = self.capabilities.issue(thread_id=thread_id,
                    turn_id=thread_id, sandbox_id=sandbox.id, harness=thread.harness,
                    model=thread.model, routes=["/v1/telemetry"], network_access=network_access,
                    ttl_seconds=self.settings.sandbox_control_ttl_seconds,
                    max_requests=1, max_public_requests=self.settings.capability_public_request_limit,
                    token_budget=1)
                route=self.pool.backend.runtime_route(sandbox)
                await self.runtime.inject(sandbox, capability_token=token,
                    gateway_url=route.gateway_url, proxy_url=route.proxy_url,
                    harness=thread.harness.value, model=thread.model,
                    defer_skill_install=bool(thread.checkpoint_ref and not sandbox.reused))
                if not sandbox.reused:
                    await self._restore_new(thread, sandbox)
                else:
                    await self._materialize_managed(thread, sandbox)
                control = await self.store.begin_sandbox_control(thread_id, owner_id, sandbox.id,
                    sandbox.handle.to_mapping(),claims.nonce,
                    now() + timedelta(seconds=self.settings.sandbox_control_ttl_seconds))
            except Exception:
                await self.pool.release(sandbox, keep_attached=False)
                raise
            await self.store.add_sandbox_audit(thread_id, owner_id, sandbox.id,
                "control.started", {"network_access": network_access,
                    "interrupted_turn": bool(active)})
            event("sandbox.control.started", thread_id=thread_id, sandbox_id=sandbox.id)
            return control

    async def _revoke(self, nonce: str, thread_id: str, sandbox_id: str) -> None:
        if not nonce:
            return
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            response = await client.post(self.settings.gateway_url.rstrip("/") + "/internal/revoke/" + nonce,
                headers={"X-Sandbox-Harness-Control": self.settings.gateway_control_key,
                    "X-Updream-Turn-ID": thread_id, "X-Updream-Thread-ID": thread_id,
                    "X-Updream-Sandbox-ID": sandbox_id})
            response.raise_for_status()

    async def release(self, thread_id: str, owner_id: str, *, expired: bool = False,
                      managed_action: str = "", allow_checkpoint_loss:bool=False) -> dict:
        async with self._lock(thread_id):
            raw = await self.store.get_sandbox_control(thread_id, owner_id)
            if not raw:
                return {"released": True, "checkpointed": False}
            thread = await self._thread(thread_id, owner_id)
            handle=SandboxHandle.from_mapping(raw["sandbox_handle"])
            sandbox = await self.pool.get_attached(thread_id)
            if not sandbox or sandbox.handle!=handle:
                try:
                    candidate = await self.pool.backend.get(handle)
                    candidate.thread_id = thread_id; candidate.turn_id = "inspect-" + thread_id
                    sandbox = await self.pool.adopt(candidate, thread_id=thread_id,
                        turn_id="inspect-" + thread_id)
                except SandboxNotFound:
                    sandbox = None
                except (RuntimeError,httpx.HTTPError) as exc:
                    event("sandbox.control.handle_lookup_failed",level=40,
                        thread_id=thread_id,sandbox_id=raw["sandbox_id"],
                        exception_type=type(exc).__name__)
                    raise RuntimeError("sandbox_release_cleanup_pending") from exc
            action=raw.get("release_action") or managed_action or ("preserve_work" if expired else "")
            changes=None
            if sandbox and raw["state"]!="releasing":
                changes=await self.managed_changes(thread_id,owner_id)
                if changes["dirty"] and not action:raise RuntimeError("managed_files_modified")
            control=await self.store.mark_sandbox_control_releasing(thread_id,owner_id,action)
            phase=control.get("release_phase") or "starting"
            change_result={"action":action or "none","count":0,"destination":""}
            if phase=="starting":
                if sandbox and control.get("pty_session_id"):
                    await self.delete_terminal(sandbox,control["pty_session_id"])
                if sandbox:
                    changes=changes or await self.managed_changes(thread_id,owner_id)
                    change_result=await self._resolve_managed_changes(thread,sandbox,owner_id,
                        changes,action,raw["generation"])
                control=await self.store.advance_sandbox_release_phase(thread_id,owner_id,
                    "starting","managed_resolved");phase="managed_resolved"
            checkpointed=phase in {"checkpointed","revoked"}
            if phase=="managed_resolved" and sandbox:
                try:
                    archive = await self.runtime.checkpoint(sandbox,
                        maximum=self.settings.max_checkpoint_bytes)
                    try:
                        checkpoint_ref, _ = await asyncio.to_thread(self.objects.put_file,
                            archive.path, digest=archive.sha256, size=archive.size)
                    finally:
                        archive.remove()
                    await self.store.update_thread_checkpoint_idle(thread_id, owner_id, checkpoint_ref)
                    checkpointed = True
                    await self.pool.release(sandbox, keep_attached=True,
                        keepalive_seconds=self.settings.session_keepalive_seconds)
                except SandboxNotFound:
                    control=await self.store.advance_sandbox_release_phase(thread_id,owner_id,
                        "managed_resolved","checkpoint_unavailable");phase="checkpoint_unavailable"
                    sandbox=None
                except Exception as exc:
                    event("sandbox.control.checkpoint_failed",level=40,thread_id=thread_id,
                        sandbox_id=control["sandbox_id"],exception_type=type(exc).__name__)
                    raise RuntimeError("sandbox_checkpoint_retryable") from exc
                if phase=="managed_resolved":
                    control=await self.store.advance_sandbox_release_phase(thread_id,owner_id,
                        "managed_resolved","checkpointed");phase="checkpointed"
            elif phase=="managed_resolved":
                control=await self.store.advance_sandbox_release_phase(thread_id,owner_id,
                    "managed_resolved","checkpoint_unavailable");phase="checkpoint_unavailable"
            if phase=="checkpoint_unavailable":
                if not allow_checkpoint_loss:
                    raise RuntimeError("sandbox_checkpoint_unavailable")
                control=await self.store.advance_sandbox_release_phase(thread_id,owner_id,
                    "checkpoint_unavailable","checkpointed");phase="checkpointed"
            if phase=="checkpointed":
                nonce=await self.store.sandbox_control_nonce(thread_id,owner_id)
                await self._revoke(nonce,thread_id,control["sandbox_id"])
                control=await self.store.advance_sandbox_release_phase(thread_id,owner_id,
                    "checkpointed","revoked");phase="revoked"
            await self.store.add_sandbox_audit(thread_id, owner_id, control["sandbox_id"],
                "control.expired" if expired else "control.released", {"checkpointed": checkpointed,
                    "checkpoint_unavailable":not checkpointed,
                    "managed_action": change_result["action"], "managed_change_count": change_result["count"]})
            await self.store.finish_sandbox_control(thread_id, owner_id)
            event("sandbox.control.released", thread_id=thread_id,
                sandbox_id=control["sandbox_id"], checkpointed=checkpointed, expired=expired)
            return {"released": True, "checkpointed": checkpointed,
                "checkpoint_unavailable":not checkpointed,
                "managed_changes": change_result}

    async def terminal_ticket(self, thread_id: str, owner_id: str) -> dict:
        sandbox, control = await self._controlled_sandbox(thread_id, owner_id)
        if not sandbox or not control or control["state"] != "user_controlled":
            raise RuntimeError("sandbox_takeover_required")
        session_id = control.get("pty_session_id") or ""
        if session_id:
            status = await self.runtime.terminal_status(sandbox, session_id)
            if status is None or not status.get("running"):
                if status is not None:
                    await self.runtime.delete_terminal(sandbox, session_id)
                session_id = ""
        if not session_id:
            response = await self.runtime.create_terminal(sandbox,
                cwd="/workspace/project", command="/bin/bash")
            session_id = str(response.get("session_id") or "")
            if not session_id:
                raise RuntimeError("terminal_session_missing")
            await self.store.set_sandbox_terminal(thread_id, owner_id, sandbox.id, session_id)
            await self.store.add_sandbox_audit(thread_id, owner_id, sandbox.id, "terminal.created")
        await self.store.touch_sandbox_control(thread_id, owner_id,
            now() + timedelta(seconds=self.settings.sandbox_control_ttl_seconds))
        ticket = self.tickets.issue(owner_id=owner_id, thread_id=thread_id,
            sandbox_id=sandbox.id, pty_session_id=session_id,
            ttl_seconds=self.settings.sandbox_terminal_ticket_seconds)
        return {"ticket": ticket, "session_id": session_id,
            "expires_in": self.settings.sandbox_terminal_ticket_seconds}

    async def delete_terminal(self, sandbox: Sandbox, session_id: str) -> None:
        await self.runtime.delete_terminal(sandbox, session_id)

    async def terminal_connection(self, ticket: str, owner_id: str, since: int):
        value = self.tickets.verify(ticket)
        if value.get("owner_id") != owner_id:
            raise ValueError("terminal_ticket_owner_mismatch")
        control = await self.store.get_sandbox_control(value["thread_id"], owner_id)
        if (not control or control["state"] != "user_controlled"
                or control["sandbox_id"] != value["sandbox_id"]
                or control["pty_session_id"] != value["pty_session_id"]):
            raise ValueError("terminal_session_not_active")
        sandbox = await self.pool.backend.get(SandboxHandle.from_mapping(
            control["sandbox_handle"]))
        ws_url, headers = await self.runtime.terminal_endpoint(sandbox,
            value["pty_session_id"], since=max(0, since))
        await self.store.touch_sandbox_control(value["thread_id"], owner_id,
            now() + timedelta(seconds=self.settings.sandbox_control_ttl_seconds))
        return websocket_connect(ws_url, additional_headers=websocket_endpoint_headers(headers),
            max_size=64 * 1024 * 1024,
            open_timeout=20, ping_interval=20, ping_timeout=20)

    async def create_port_grant(self, thread_id: str, owner_id: str, port: int) -> dict:
        await self.heartbeat(thread_id,owner_id)
        if port in BLOCKED_PREVIEW_PORTS or not 1024 <= port <= 65535:
            raise ValueError("sandbox_port_not_previewable")
        sandbox = await self.live_sandbox(thread_id, owner_id)
        if not sandbox:
            raise RuntimeError("sandbox_not_running")
        detected = await self.runtime.inspect_ports(sandbox)
        if port not in {int(item["port"]) for item in detected.get("items", [])}:
            raise ValueError("sandbox_port_not_listening")
        value = await self.store.create_sandbox_port_grant(thread_id, owner_id,
            sandbox.id,sandbox.handle.to_mapping(),port,
            now() + timedelta(seconds=self.settings.sandbox_port_grant_ttl_seconds))
        await self.store.add_sandbox_audit(thread_id, owner_id, sandbox.id,
            "port.preview_created", {"port": port})
        return value

    async def port_endpoint(self, grant_id: str, owner_id: str) -> tuple[dict, str, dict[str, str]]:
        grant = await self.store.get_sandbox_port_grant(grant_id, owner_id)
        if not grant:
            raise KeyError("sandbox_port_grant_not_found")
        try:sandbox = await self.pool.backend.get(SandboxHandle.from_mapping(
            grant["sandbox_handle"]))
        except SandboxNotFound as exc:
            await self.store.revoke_sandbox_port_grant(grant_id,owner_id)
            raise KeyError("sandbox_port_preview_expired") from exc
        attached=await self.pool.get_attached(grant["thread_id"])
        control=await self.store.get_sandbox_control(grant["thread_id"],owner_id)
        controlled=bool(control and control.get("state") in {"user_controlled","releasing"}
            and control.get("sandbox_id")==grant["sandbox_id"])
        attached_matches=bool(attached and attached.id==grant["sandbox_id"]
            and attached.thread_id==grant["thread_id"])
        if not attached_matches and not controlled:
            await self.store.revoke_sandbox_port_grant(grant_id,owner_id)
            raise KeyError("sandbox_port_preview_expired")
        endpoint, headers = await self.pool.backend.endpoint(sandbox, grant["port"])
        return grant, endpoint, headers
