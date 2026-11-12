from __future__ import annotations

import httpx
import asyncio
import hashlib
import json
import logging
import time
import uuid
from botocore.exceptions import ClientError
from datetime import timedelta
from collections import defaultdict, deque
from contextlib import suppress
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .broker import Broker
from .auth import AUTH_COOKIE, AccountAuth, normalize_username
from .catalog import HarnessCatalog
from .config import Settings, load_settings, validate_service_settings
from .observability import RequestLoggingMiddleware, telemetry, event
from .domain import Attachment, ChangePasswordRequest, ChangeUsernameRequest, Harness, InteractionResponse, LoginRequest, RegisterRequest, UpdateAccountRequest, MediaDraftCreate, MediaDraftUpdate, MediaReferenceUploadCreate, ProjectFileRename, ThreadCreate, ThreadRead, TurnCreate, TurnInputCreate, valid_attachment_name, now
from .document_preview import is_presentation, presentation_pdf
from .model_discovery import ModelDiscovery
from .media_catalog import MediaCatalog
from .media_references import inspect_reference, resolve_media_references
from .objects import object_store
from .pool import SandboxPool
from .runtime import RuntimeClient
from .sandbox import FakeSandboxBackend, OpenSandboxBackend
from .sessions import COOKIE, Sessions
from .store import Store

logger = logging.getLogger(__name__)

SAFE_INLINE_MIMES = {
    "image/png", "image/jpeg", "image/gif", "image/webp", "image/avif", "image/bmp", "image/svg+xml",
    "text/plain", "application/pdf",
}
MAX_AVATAR_BYTES = 5 * 1024 * 1024


def avatar_mime(data: bytes) -> str:
    if data.startswith(b"\x89PNG\r\n\x1a\n"): return "image/png"
    if data.startswith(b"\xff\xd8\xff"): return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")): return "image/gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP": return "image/webp"
    raise ValueError("Avatar must be PNG, JPEG, WebP or GIF")


def media_group_has_executed_job(items: list[dict]) -> bool:
    return any(item.get("job_id") for item in items)


def media_group_needs_agent_continuation(items: list[dict]) -> bool:
    return media_group_has_executed_job(items) or any(item.get("status") == "expired" for item in items)


def valid_filename(value: str) -> bool:
    return valid_attachment_name(value)


def content_disposition(filename: str, *, inline: bool) -> str:
    fallback = "".join(character if character.isascii() and (character.isalnum() or character in "._-")
        else "_" for character in filename).strip("._") or "download"
    kind = "inline" if inline else "attachment"
    return f"{kind}; filename=\"{fallback}\"; filename*=UTF-8''{quote(filename, safe='')}"


async def limited_body(request: Request, maximum: int) -> bytes:
    raw_length = request.headers.get("content-length")
    if raw_length:
        try:
            if int(raw_length) > maximum:
                raise HTTPException(413, "Request body exceeds byte limit")
        except ValueError as exc:
            raise HTTPException(400, "Invalid Content-Length") from exc
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > maximum:
            raise HTTPException(413, "Request body exceeds byte limit")
        data.extend(chunk)
    return bytes(data)


def managed_project_path(value: str, *, upload: bool = False) -> str:
    path = PurePosixPath(value)
    allowed = {"assets"} if upload else {"assets", "artifacts"}
    if path.is_absolute() or len(path.parts) < 2 or path.parts[0] not in allowed or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise ValueError("Invalid managed project path")
    return path.as_posix()


def components(settings: Settings):
    store = Store(settings.database_url, settings.redis_url, settings.redis_prefix)
    objects = object_store(settings)
    backend = (
        FakeSandboxBackend(namespace=settings.namespace)
        if settings.backend == "fake"
        else OpenSandboxBackend(
            settings.opensandbox_url,
            settings.opensandbox_api_key,
            settings.sandbox_timeout_seconds,
            settings.namespace,
            settings.gateway_url,
            settings.gateway_proxy_port,
        )
    )
    images = {harness: settings.image_for(harness.value) for harness in Harness}
    targets = {
        Harness.CODEX: settings.warm_pool_codex,
        Harness.CLAUDE: settings.warm_pool_claude,
        Harness.DEEPSEEK: settings.warm_pool_deepseek,
    }
    if sum(targets.values()) >= settings.max_sandboxes:
        raise ValueError("max_sandboxes must exceed the total warm pool target")
    pool = SandboxPool(backend, images, targets, max_sandboxes=settings.max_sandboxes)
    runtime = RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds,
        reattach_attempts=settings.runtime_stream_reattach_attempts,
        reattach_max_delay_seconds=settings.runtime_stream_reattach_max_delay_seconds)
    catalog = HarnessCatalog(settings.harness_catalog_json or None)
    media_catalog = MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key))
    discovery = ModelDiscovery(settings, catalog)
    if settings.model_catalog_discovery:
        discovery.load_snapshot()
    return store, objects, pool, catalog, discovery, media_catalog, Broker(settings, store, objects, pool, runtime, catalog)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    validate_service_settings(settings)
    store, objects, pool, catalog, discovery, media_catalog, broker = components(settings)
    sessions = Sessions(settings.session_signing_key)
    account_auth = AccountAuth(store, settings.auth_session_ttl_seconds)
    login_failures: dict[str, deque[float]] = defaultdict(deque)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.initialize()
        await broker.start()
        media_outbox_stop = asyncio.Event()
        media_outbox_owner = "media-outbox-" + uuid.uuid4().hex
        async def continue_media(batch: dict):
            turn_id = batch.get("turn_id") or ""
            if not turn_id:
                return
            ready, jobs = await store.media_continuation_group(turn_id)
            if not ready:
                raise RuntimeError("media_group_pending")
            if not media_group_needs_agent_continuation(jobs):
                event("media.batch.ended_without_jobs", turn_id=turn_id,
                    cancelled=sum(item["status"] == "cancelled" for item in jobs),
                    expired=sum(item["status"] == "expired" for item in jobs))
                return
            results = []
            for item in jobs:
                candidate = (item["status"] == "succeeded" and item["output_role"] == "candidate"
                    and bool(item["job_id"]))
                results.append({"draft_id": item["draft_id"], "job_id": item["job_id"],
                    "kind": item["kind"], "model": item["model"], "status": item["status"],
                    "output_role": item["output_role"],
                    "path": "/workspace/project/" + item["project_path"] if item["project_path"] else "",
                    "inspect_command": f"sandbox-media fetch --job-id {item['job_id']} --output /workspace/outputs/{item['job_id']}"
                        if candidate else "",
                    "error": item["error_code"] or ""})
            expired_only = bool(results) and all(item["status"] == "expired" for item in results)
            if expired_only:
                message = ("[media_group_expired] The media drafts expired before confirmation. "
                    "Recreate the same validated drafts, commit the batch once, and tell the user to use the media confirmation cards. "
                    "Do not use sandbox-interact approval as a substitute or claim that a Provider Job was submitted. Expired drafts: "
                    + json.dumps(results, ensure_ascii=False, sort_keys=True))
            else:
                message = ("[media_group_completed] Continue the original user task. Use only results with status=succeeded and a non-empty path or inspect_command; never invent a path for a failed result. Verify input files exist before composition. Media results: "
                    + json.dumps(results, ensure_ascii=False, sort_keys=True))
            origin = await store.get_turn(turn_id)
            await broker.submit(batch["thread_id"], TurnCreate(message=message,
                idempotency_key=f"media-continuation-{turn_id}", delivery_mode="queue"),
                network_access=origin.network_access if origin else "restricted", internal=True)
        async def dispatch_media_outbox():
            while not media_outbox_stop.is_set():
                for batch in await store.claim_media_batches(media_outbox_owner):
                    try:
                        await continue_media(batch)
                    except Exception as exc:
                        await store.finish_media_batch(batch["turn_id"], media_outbox_owner,
                            delivered=False, error=type(exc).__name__)
                    else:
                        await store.finish_media_batch(batch["turn_id"], media_outbox_owner, delivered=True)
                try:
                    await asyncio.wait_for(media_outbox_stop.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
        media_outbox_task = asyncio.create_task(dispatch_media_outbox(), name="media-outbox-dispatcher")
        pool_task = None
        catalog_task = None
        if settings.model_catalog_discovery:
            async def refresh_catalog():
                while True:
                    try:
                        await discovery.refresh()
                    except Exception:
                        logger.exception("Model catalog refresh failed; retaining the previous catalog")
                    await asyncio.sleep(settings.model_catalog_refresh_seconds)

            catalog_task = asyncio.create_task(refresh_catalog(), name="model-catalog-refresh")
        if settings.backend == "opensandbox":
            app.state.pool_status = "starting"

            async def fill_pool():
                try:
                    await pool.fill()
                    app.state.pool_status = "ready"
                except Exception:
                    app.state.pool_status = "error"
                    logger.exception("OpenSandbox warm pool initialization failed")

            pool_task = asyncio.create_task(fill_pool(), name="warm-pool-fill")
        else:
            app.state.pool_status = "disabled"
        yield
        if catalog_task:
            catalog_task.cancel()
            with suppress(asyncio.CancelledError):
                await catalog_task
        if pool_task and not pool_task.done():
            pool_task.cancel()
            with suppress(asyncio.CancelledError):
                await pool_task
        media_outbox_stop.set()
        await media_outbox_task
        await broker.close()
        await pool.close(preserve_sandbox_ids={sandbox.id for sandbox in broker.active_sandboxes.values()})
        await store.close()

    app = FastAPI(title="Sandbox Harness Platform", lifespan=lifespan)
    app.add_middleware(RequestLoggingMiddleware,service="api")
    app.state.settings = settings
    app.state.store = store
    app.state.objects = objects
    app.state.pool = pool
    app.state.broker = broker
    app.state.account_auth = account_auth

    def cookie_path(request: Request) -> str:
        prefix = request.headers.get("x-forwarded-prefix", "").rstrip("/")
        return (prefix or "") + "/"

    def secure_cookie(request: Request) -> bool:
        return request.headers.get("x-forwarded-proto", request.url.scheme) == "https"

    @app.middleware("http")
    async def browser_identity(request: Request, call_next):
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            origin = request.headers.get("origin")
            if origin and urlsplit(origin).netloc != request.headers.get("host"):
                return JSONResponse({"detail": "Cross-site operations are not accepted"}, status_code=403)
        ingress = request.headers.get("x-updream-ingress", "direct").strip().lower()
        if ingress not in {"direct", "frpc"}:ingress = "direct"
        account, auth_token_hash = await account_auth.resolve(request.cookies.get(AUTH_COOKIE))
        if request.url.path == "/api/health":owner_id, token, new_session = "", "", False
        elif account:owner_id, token, new_session = account.owner_id, "", False
        elif ingress == "frpc":owner_id, token, new_session = "", "", False
        else:owner_id, token, new_session = sessions.resolve(request.cookies.get(COOKIE))
        request.state.ingress = ingress
        request.state.account = account
        request.state.auth_token_hash = auth_token_hash
        request.state.owner_id = owner_id
        public_auth = request.url.path in {"/api/health", "/api/session", "/api/auth/login", "/api/auth/register"}
        if ingress == "frpc" and not account and request.url.path.startswith("/api/") and not public_auth:
            return JSONResponse({"detail":"authentication_required"},status_code=401,
                headers={"Cache-Control":"no-store"})
        response = await call_next(request)
        if new_session:
            response.set_cookie(COOKIE, token, httponly=True, samesite="lax", secure=secure_cookie(request),
                max_age=365 * 86400, path=cookie_path(request))
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        elif request.url.path == "/":
            response.headers["Cache-Control"] = "no-store, max-age=0"
        elif request.url.path.startswith("/assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response

    @app.get("/api/health")
    async def health():
        pool_status = app.state.pool_status
        payload = {"status": "ok" if pool_status in {"ready", "disabled"} else "degraded",
                   "backend": settings.backend, "warm_pool": pool_status, "broker": broker.status(),
                   "model_catalog": discovery.status(),"observability":telemetry().status() if telemetry() else {"enabled":False}}
        return JSONResponse(payload, status_code=200 if payload["status"] == "ok" else 503)


    @app.get("/api/catalog/harnesses")
    async def harness_catalog():
        return catalog.public()

    @app.get("/api/catalog/media")
    async def media_catalog_route(kind: str | None = None):
        if kind is not None and kind not in {"image", "video", "audio", "model3d"}:
            raise HTTPException(422, "Invalid media kind")
        return {"models": media_catalog.public(kind)}

    @app.post("/api/threads/{thread_id}/media/reference-uploads")
    async def create_media_reference_upload(thread_id: str, value: MediaReferenceUploadCreate,
                                            request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        try:
            prepared = await asyncio.to_thread(objects.prepare_media_upload, kind=value.kind,
                digest=value.sha256, mime=value.mime, size=value.size, expires_seconds=900)
            upload = await store.create_media_reference_upload(request.state.owner_id, thread_id, value,
                object_key=prepared["object_key"], public_url=prepared["public_url"],
                expires_at=now() + timedelta(minutes=15))
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(422, str(exc))
        return {**upload.model_dump(mode="json"), "upload": {"method": "PUT",
            "url": prepared["put_url"], "headers": prepared["headers"]}}

    @app.post("/api/media/reference-uploads/{upload_id}/complete")
    async def complete_media_reference_upload(upload_id: str, request: Request):
        upload = await store.get_media_reference_upload(upload_id, request.state.owner_id)
        if not upload:
            raise HTTPException(404, "Media reference upload not found")
        if upload.status == "completed":
            return upload
        if upload.status != "pending":
            raise HTTPException(409, "Media reference upload expired")
        try:
            head = await asyncio.to_thread(objects.head_media_upload, upload.object_key)
            metadata = head["metadata"]
            if (head["size"] != upload.size or head["mime"] != upload.mime
                    or metadata.get("sha256") != upload.sha256
                    or metadata.get("size") != str(upload.size) or metadata.get("kind") != upload.kind):
                raise ValueError("Uploaded BOSS object metadata does not match the upload ticket")
            await asyncio.to_thread(objects.verify_media_upload, upload.object_key,
                digest=upload.sha256, expected_size=upload.size)
            object_ref = await asyncio.to_thread(objects.promote_media_upload,
                upload.object_key, upload.sha256)
            data = await asyncio.to_thread(objects.get, object_ref, maximum=upload.size)
            role = {"image":"reference_image","video":"reference_video","audio":"reference_audio"}[upload.kind]
            inspected = await asyncio.to_thread(inspect_reference, data, role, upload.name)
            if inspected["mime"] != upload.mime:
                raise ValueError("Uploaded media content does not match the declared MIME")
            project_file, _ = await store.register_project_file(upload.thread_id,
                path="assets/" + upload.name, source_type="upload", source_turn_id="",
                sha256=upload.sha256, size=upload.size, mime=upload.mime, object_ref=object_ref)
            await store.ensure_media_reference_asset({"sha256": upload.sha256, "kind": upload.kind,
                "mime": upload.mime, "size": upload.size, "object_ref": object_ref},
                object_key=upload.object_key, public_url=upload.public_url, boss_status="active")
            return await store.finish_media_reference_upload(upload.id, request.state.owner_id,
                project_file.id)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            raise HTTPException(409 if status == 404 else 502,
                "BOSS upload is not available" if status == 404 else "BOSS verification failed")

    @app.post("/api/threads/{thread_id}/media/drafts")
    async def create_media_draft(thread_id: str, value: MediaDraftCreate, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        try:
            spec = media_catalog.resolve(value.kind, value.model)
            references = await resolve_media_references(store, objects, thread_id,
                request.state.owner_id, value.references)
            media_catalog.validate_parameters(value.kind, value.model, value.parameters,
                generation_mode=value.generation_mode, references=references)
        except KeyError:
            raise HTTPException(422, "Media model is not enabled")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        try:
            result, _ = await store.create_media_draft(request.state.owner_id, thread_id, value,
                expires_at=now() + timedelta(seconds=settings.media_draft_ttl_seconds), provider_spec=spec,
                reference_snapshots=references)
            return result
        except KeyError:
            raise HTTPException(404, "Media Turn not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/media/drafts/{draft_id}")
    async def get_media_draft(draft_id: str, request: Request):
        value = await store.get_media_draft(draft_id, request.state.owner_id)
        if not value: raise HTTPException(404, "Media draft not found")
        return value

    @app.get("/api/threads/{thread_id}/media/drafts")
    async def list_media_drafts(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_media_drafts(thread_id, request.state.owner_id)

    @app.patch("/api/media/drafts/{draft_id}")
    async def update_media_draft(draft_id: str, value: MediaDraftUpdate, request: Request):
        current = await store.get_media_draft(draft_id, request.state.owner_id)
        if not current: raise HTTPException(404, "Media draft not found")
        snapshot = await store.get_media_draft_snapshot(draft_id, request.state.owner_id)
        model = value.model or current.model
        generation_mode = value.generation_mode or current.generation_mode
        parameters = value.parameters if value.parameters is not None else current.parameters
        try:
            references = (await resolve_media_references(store, objects, current.thread_id,
                request.state.owner_id, value.references)) if value.references is not None else snapshot["references"]
            media_catalog.validate_parameters(current.kind, model, parameters,
                generation_mode=generation_mode, references=references)
        except (KeyError, ValueError) as exc:
            raise HTTPException(422, str(exc))
        try:
            return await store.update_media_draft(draft_id, request.state.owner_id,
                model=value.model, generation_mode=value.generation_mode, parameters=value.parameters,
                reference_snapshots=references if value.references is not None else None)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/api/media/drafts/{draft_id}/confirm")
    async def confirm_media_draft(draft_id: str, request: Request):
        current = await store.get_media_draft(draft_id, request.state.owner_id)
        if not current: raise HTTPException(404, "Media draft not found")
        try:
            spec = media_catalog.resolve(current.kind, current.model)
            snapshot = await store.get_media_draft_snapshot(draft_id, request.state.owner_id)
            media_catalog.validate_parameters(current.kind, current.model, current.parameters,
                generation_mode=current.generation_mode, references=snapshot["references"])
            return await store.confirm_media_draft(draft_id, request.state.owner_id, provider_spec=spec)
        except (KeyError, ValueError) as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/api/media/drafts/{draft_id}/cancel")
    async def cancel_media_draft(draft_id: str, request: Request):
        try:
            return await store.cancel_media_draft(draft_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Media draft not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/media/jobs/{job_id}")
    async def get_media_job(job_id: str, request: Request):
        value = await store.get_media_job(job_id, request.state.owner_id)
        if not value: raise HTTPException(404, "Media job not found")
        return value

    @app.get("/api/threads/{thread_id}/media/jobs")
    async def list_media_jobs(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_media_jobs(thread_id, request.state.owner_id)

    @app.post("/api/media/jobs/{job_id}/cancel")
    async def cancel_media_job(job_id: str, request: Request):
        try:
            return await store.request_media_cancel(job_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Media job not found")
        except RuntimeError as exc:
            if str(exc) == "media_cancel_not_supported":
                raise HTTPException(409, str(exc))
            raise HTTPException(409, str(exc))

    @app.get("/api/session")
    async def session_ready(request: Request):
        account = request.state.account
        return {"ready": True, "ingress": request.state.ingress,
            "authentication_required": request.state.ingress == "frpc" and not account,
            "authenticated": bool(account),
            "user": account.model_dump(mode="json") if account else None}

    @app.post("/api/auth/login")
    async def login(value: LoginRequest, request: Request):
        source = request.headers.get("x-real-ip", "") if request.state.ingress == "frpc" else ""
        source = source or (request.client.host if request.client else "unknown")
        identity = hashlib.sha256((source + "\0" + value.username.strip().casefold()).encode()).hexdigest()
        current = time.monotonic()
        if len(login_failures) > 10_000:
            for key, values in list(login_failures.items()):
                if not values or values[-1] < current - 300:login_failures.pop(key,None)
            if len(login_failures) > 10_000:raise HTTPException(429,"login_rate_limited")
        failures = login_failures[identity]
        while failures and failures[0] < current - 300:failures.popleft()
        if len(failures) >= 5:raise HTTPException(429,"login_rate_limited")
        account = await account_auth.authenticate(value.username, value.password)
        if not account:
            failures.append(current)
            raise HTTPException(401,"invalid_credentials")
        login_failures.pop(identity,None)
        token, _ = await account_auth.create_session(account.id)
        response = JSONResponse({"authenticated":True,"user":account.model_dump(mode="json")})
        response.set_cookie(AUTH_COOKIE, token, httponly=True, samesite="lax", secure=secure_cookie(request),
            max_age=settings.auth_session_ttl_seconds, path=cookie_path(request))
        return response

    @app.post("/api/auth/register")
    async def register(value: RegisterRequest, request: Request):
        if request.state.ingress == "frpc":
            raise HTTPException(403, "registration_not_available")
        if request.state.account:
            raise HTTPException(409, "already_authenticated")
        try:
            username = normalize_username(value.username)
            password_hash = await account_auth.hash_password(value.password)
            account = await store.register_user(username, password_hash, request.state.owner_id)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        token, _ = await account_auth.create_session(account.id)
        response = JSONResponse({"authenticated": True, "user": account.model_dump(mode="json")})
        response.delete_cookie(COOKIE, path=cookie_path(request))
        response.set_cookie(AUTH_COOKIE, token, httponly=True, samesite="lax", secure=secure_cookie(request),
            max_age=settings.auth_session_ttl_seconds, path=cookie_path(request))
        return response

    @app.post("/api/auth/logout")
    async def logout(request: Request):
        if request.state.auth_token_hash:await store.delete_auth_session(request.state.auth_token_hash)
        response = JSONResponse({"authenticated":False})
        response.delete_cookie(AUTH_COOKIE,path=cookie_path(request))
        return response

    @app.post("/api/auth/change-username")
    async def change_username(value: ChangeUsernameRequest, request: Request):
        if request.state.ingress == "frpc":
            raise HTTPException(403, "account_action_not_available")
        account = request.state.account
        if not account:raise HTTPException(401,"authentication_required")
        if not await account_auth.verify_account(account,value.current_password):
            raise HTTPException(401,"invalid_credentials")
        try:username = normalize_username(value.username)
        except ValueError as exc:raise HTTPException(422,str(exc))
        try:updated = await store.update_username(account.id,username)
        except RuntimeError as exc:raise HTTPException(409,str(exc))
        return {"authenticated":True,"user":updated.model_dump(mode="json")}

    @app.post("/api/auth/change-password")
    async def change_password(value: ChangePasswordRequest, request: Request):
        account = request.state.account
        if not account:raise HTTPException(401,"authentication_required")
        if not await account_auth.verify_account(account,value.current_password):
            raise HTTPException(401,"invalid_credentials")
        try:password_hash = await account_auth.hash_password(value.new_password)
        except ValueError as exc:raise HTTPException(422,str(exc))
        await store.update_password(account.id,password_hash,keep_token_hash=request.state.auth_token_hash)
        return {"changed":True}

    @app.post("/api/auth/profile")
    async def update_profile(value: UpdateAccountRequest, request: Request):
        account = request.state.account
        if not account: raise HTTPException(401, "authentication_required")
        if request.state.ingress == "frpc" and value.username is not None:
            raise HTTPException(403, "account_action_not_available")
        if not await account_auth.verify_account(account, value.current_password):
            raise HTTPException(401, "invalid_credentials")
        try:
            username = normalize_username(value.username) if value.username is not None else None
            password_hash = await account_auth.hash_password(value.new_password) if value.new_password else None
            updated = await store.update_account(account.id, username=username, password_hash=password_hash,
                keep_token_hash=request.state.auth_token_hash)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        return {"authenticated": True, "user": updated.model_dump(mode="json")}

    @app.get("/api/auth/avatar")
    async def get_avatar(request: Request):
        account = request.state.account
        if not account: raise HTTPException(401, "authentication_required")
        value = await store.get_user_avatar(account.id)
        if not value: raise HTTPException(404, "avatar_not_found")
        object_ref, mime = value
        data = await asyncio.to_thread(objects.get, object_ref, maximum=MAX_AVATAR_BYTES)
        return Response(data, media_type=mime, headers={"Cache-Control":"private, no-store",
            "Content-Security-Policy":"sandbox; default-src 'none'"})

    @app.post("/api/auth/avatar")
    async def set_avatar(request: Request):
        account = request.state.account
        if not account: raise HTTPException(401, "authentication_required")
        if request.state.ingress == "frpc": raise HTTPException(403, "account_action_not_available")
        data = await limited_body(request, MAX_AVATAR_BYTES)
        try: mime = avatar_mime(data)
        except ValueError as exc: raise HTTPException(422, str(exc))
        object_ref, _ = await asyncio.to_thread(objects.put, data)
        updated = await store.set_user_avatar(account.id, object_ref, mime)
        return {"authenticated": True, "user": updated.model_dump(mode="json")}

    @app.delete("/api/auth/avatar")
    async def delete_avatar(request: Request):
        account = request.state.account
        if not account: raise HTTPException(401, "authentication_required")
        if request.state.ingress == "frpc": raise HTTPException(403, "account_action_not_available")
        updated = await store.set_user_avatar(account.id)
        return {"authenticated": True, "user": updated.model_dump(mode="json")}

    @app.post("/api/threads")
    async def create_thread(value: ThreadCreate, request: Request):
        try:
            resolved = catalog.resolve(value.harness, value.model, value.reasoning_level)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        return await store.create_thread(value, resolved, request.state.owner_id)

    @app.get("/api/threads")
    async def list_threads(request: Request, archived: bool = False):
        return await store.list_thread_summaries(request.state.owner_id, archived=archived)

    @app.get("/api/threads/{thread_id}")
    async def get_thread(thread_id: str, request: Request):
        value = await store.get_thread(thread_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Thread not found")
        return value

    @app.post("/api/threads/{thread_id}/read")
    async def mark_thread_read(thread_id: str, value: ThreadRead, request: Request):
        try:
            turn_id = await store.mark_thread_read(thread_id, value.turn_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        return {"read_turn_id": turn_id}

    @app.post("/api/threads/{thread_id}/archive")
    async def archive_thread(thread_id: str, request: Request):
        try:
            return await broker.set_thread_archived(thread_id, request.state.owner_id, archived=True)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/api/threads/{thread_id}/unarchive")
    async def unarchive_thread(thread_id: str, request: Request):
        try:
            return await broker.set_thread_archived(thread_id, request.state.owner_id, archived=False)
        except KeyError:
            raise HTTPException(404, "Thread not found")

    @app.delete("/api/threads/{thread_id}", status_code=204)
    async def delete_thread(thread_id: str, request: Request):
        try:
            await broker.delete_thread(thread_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        return Response(status_code=204)

    @app.post("/api/threads/{thread_id}/turns")
    async def create_turn(thread_id: str, value: TurnCreate, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        if not await store.attachments_owned(request.state.owner_id, value.attachments):
            raise HTTPException(404, "Attachment not found")
        try:
            network_access = "public" if request.state.account else "restricted"
            result = await broker.submit(thread_id, value, network_access=network_access)
            request.state.log_context.update({"thread_id":thread_id,"turn_id":result.id})
            return result
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/threads/{thread_id}/turns")
    async def list_turns(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_turns(thread_id, request.state.owner_id)

    @app.get("/api/threads/{thread_id}/items")
    async def list_items(thread_id: str, request: Request, after: int = 0, limit: int = 200):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.items_after(thread_id, after, min(max(limit, 1), 500), request.state.owner_id)

    @app.get("/api/threads/{thread_id}/project-files")
    async def list_project_files(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.current_project_files(thread_id, request.state.owner_id)

    @app.post("/api/threads/{thread_id}/project-files")
    async def upload_project_file(thread_id: str, request: Request, path: str, mime: str = "application/octet-stream"):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        try:
            path = managed_project_path(path, upload=True)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        data = await limited_body(request, settings.max_attachment_bytes)
        object_ref, digest = await asyncio.to_thread(objects.put, data)
        value, _ = await store.register_project_file(thread_id, path=path, source_type="upload",
            source_turn_id="", sha256=digest, size=len(data), mime=mime, object_ref=object_ref)
        return value

    @app.get("/api/threads/{thread_id}/project-files/history")
    async def project_file_history(thread_id: str, path: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        try:
            path = managed_project_path(path)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        return await store.project_file_history(thread_id, path, request.state.owner_id)

    @app.get("/api/threads/{thread_id}/interactions")
    async def list_interactions(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_interactions(thread_id, request.state.owner_id)

    @app.post("/api/interactions/{interaction_id}/respond")
    async def respond_interaction(interaction_id: str, value: InteractionResponse, request: Request):
        try:
            return await broker.respond_interaction(interaction_id, request.state.owner_id, value)
        except KeyError:
            raise HTTPException(404, "Interaction not found")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/turns/{turn_id}")
    async def get_turn(turn_id: str, request: Request):
        value = await store.get_turn_for_owner(turn_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Turn not found")
        return {**value.model_dump(mode="json"), "artifacts": [item.model_dump(mode="json") for item in await store.artifacts(turn_id)]}

    @app.post("/api/turns/{turn_id}/cancel")
    async def cancel_turn(turn_id: str, request: Request):
        if not await store.get_turn_for_owner(turn_id, request.state.owner_id):
            raise HTTPException(404, "Turn not found")
        try:
            await broker.cancel(turn_id)
        except KeyError:
            raise HTTPException(404, "Turn not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        return {"accepted": True}

    @app.post("/api/turns/{turn_id}/inputs")
    async def append_turn_input(turn_id: str, value: TurnInputCreate, request: Request):
        turn = await store.get_turn_for_owner(turn_id, request.state.owner_id)
        if not turn:
            raise HTTPException(404, "Turn not found")
        if not await store.attachments_owned(request.state.owner_id, value.attachments):
            raise HTTPException(404, "Attachment not found")
        if sum(item.size for item in value.attachments) > settings.max_attachment_bytes:
            raise HTTPException(413, "Attachments exceed the input byte limit")
        try:
            result, created = await store.create_turn_input(turn_id, value, request.state.owner_id)
            request.state.log_context.update({"turn_id":turn_id,"thread_id":turn.thread_id,"input_id":result.id})
            event("input.accepted",turn_id=turn_id,thread_id=turn.thread_id,input_id=result.id,reused=not created)
            if created:
                try:
                    await broker.try_deliver_input(result)
                except httpx.TransportError:
                    # The Runtime may have accepted the input before the reply
                    # was lost. Do not retry it as a fresh execution.
                    await store.set_input_status(result.id, "unknown", "delivery_unconfirmed")
                    event("input.delivery.unknown",level=logging.WARNING,turn_id=turn_id,input_id=result.id)
                except Exception as exc:
                    await store.set_input_status(result.id, "failed", "delivery_failed")
                    raise HTTPException(409, "input_delivery_failed") from exc
            return await store.get_turn_input(result.id)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/turns/{turn_id}/inputs")
    async def list_turn_inputs(turn_id: str, request: Request):
        if not await store.get_turn_for_owner(turn_id, request.state.owner_id):
            raise HTTPException(404, "Turn not found")
        return await store.list_turn_inputs(turn_id)

    @app.get("/api/turns/{turn_id}/background-jobs")
    async def list_background_jobs(turn_id: str, request: Request):
        try:
            return await broker.background_jobs(turn_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Turn not found")

    @app.post("/api/turns/{turn_id}/background-jobs/{job_id}/cancel")
    async def cancel_background_job(turn_id: str, job_id: str, request: Request):
        try:
            return await broker.cancel_background_job(turn_id, job_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Turn or background job not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/turns/{turn_id}/events")
    async def events(turn_id: str, request: Request, after: int = 0):
        if not await store.get_turn_for_owner(turn_id, request.state.owner_id):
            raise HTTPException(404, "Turn not found")
        try:
            after = max(after, int(request.headers.get("last-event-id", "0")))
        except ValueError:
            raise HTTPException(422, "Invalid event cursor")
        async def stream():
            cursor = after
            while not await request.is_disconnected():
                rows = await store.events_after(turn_id, cursor, 1000)
                for event in rows:
                    cursor = event.sequence
                    yield f"id: {event.sequence}\nevent: {event.kind.value}\ndata: {json.dumps(event.payload, ensure_ascii=False)}\n\n"
                turn = await store.get_turn(turn_id)
                if turn and turn.state.value in {"COMPLETED", "WAITING_USER", "FAILED", "CANCELLED"} and not rows:
                    return
                if store.events:
                    await store.events.wait(turn_id, cursor)
                    if not store.events.available:
                        await asyncio.sleep(1)
                else:
                    await asyncio.sleep(0.25)
        return StreamingResponse(stream(), media_type="text/event-stream")

    @app.get("/api/turns/{turn_id}/events/history")
    async def event_history(turn_id: str, request: Request, after: int = 0, limit: int = 500):
        if not await store.get_turn_for_owner(turn_id, request.state.owner_id):
            raise HTTPException(404, "Turn not found")
        return await store.events_after(turn_id, after, min(max(limit, 1), 1000))

    @app.post("/api/uploads")
    async def upload(request: Request, name: str, mime: str = "application/octet-stream"):
        if not valid_filename(name):
            raise HTTPException(400, "Invalid filename")
        data = await limited_body(request, settings.max_attachment_bytes)
        object_ref, digest = await asyncio.to_thread(objects.put, data)
        attachment = Attachment(name=name, object_ref=object_ref, sha256=digest, size=len(data), mime=mime)
        await store.record_upload(request.state.owner_id, attachment)
        return attachment

    @app.get("/api/artifacts/{artifact_id}")
    async def artifact(artifact_id: str, request: Request, inline: bool = False):
        value = await store.get_artifact_for_owner(artifact_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Artifact not found")
        data = await asyncio.to_thread(objects.get, value.object_ref, maximum=settings.max_artifact_bytes)
        filename = Path(value.path).name
        safe_inline = inline and (value.mime in SAFE_INLINE_MIMES or value.mime.startswith(("video/", "audio/")))
        return Response(data, media_type=value.mime if safe_inline or not inline else "application/octet-stream",
            headers={"Content-Disposition": content_disposition(filename, inline=safe_inline),
                     "Content-Security-Policy": "sandbox; default-src 'none'"})

    @app.get("/api/project-files/{file_id}")
    async def project_file(file_id: str, request: Request, inline: bool = False):
        value = await store.get_project_file_for_owner(file_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Project file not found")
        maximum = settings.max_attachment_bytes if value.source_type == "upload" else settings.max_artifact_bytes
        data = await asyncio.to_thread(objects.get, value.object_ref, maximum=maximum)
        filename = Path(value.path).name
        safe_inline = inline and (value.mime in SAFE_INLINE_MIMES or value.mime.startswith(("video/", "audio/")))
        return Response(data, media_type=value.mime if safe_inline or not inline else "application/octet-stream",
            headers={"Content-Disposition": content_disposition(filename, inline=safe_inline),
                     "Content-Security-Policy": "sandbox; default-src 'none'"})

    @app.get("/api/previews/{source}/{file_id}")
    async def document_preview(source: str, file_id: str, request: Request):
        if source == "artifact":
            value = await store.get_artifact_for_owner(file_id, request.state.owner_id)
            maximum = settings.max_artifact_bytes
            filename = Path(value.path).name if value else ""
        elif source == "project-file":
            value = await store.get_project_file_for_owner(file_id, request.state.owner_id)
            maximum = (settings.max_attachment_bytes if value and value.source_type == "upload"
                else settings.max_artifact_bytes)
            filename = Path(value.path).name if value else ""
        else:
            raise HTTPException(404, "Preview source not found")
        if not value:
            raise HTTPException(404, "Preview file not found")
        if not is_presentation(filename, value.mime):
            raise HTTPException(415, "Preview format is not supported by this endpoint")
        data = await asyncio.to_thread(objects.get, value.object_ref, maximum=maximum)
        try:
            pdf = await asyncio.to_thread(presentation_pdf, data, filename=filename, mime=value.mime,
                digest=value.sha256, cache_root=settings.data_root / "preview-cache", maximum=maximum)
        except (ValueError, RuntimeError) as exc:
            event("document.preview.failed", level=40, source=source, file_id=file_id,
                exception_type=type(exc).__name__)
            raise HTTPException(422, str(exc))
        return Response(pdf, media_type="application/pdf",
            headers={"Content-Disposition": content_disposition(Path(filename).stem + ".pdf", inline=True),
                     "Content-Security-Policy": "sandbox; default-src 'none'",
                     "Cache-Control": "private, max-age=3600"})

    @app.post("/api/project-files/{file_id}/rename")
    async def rename_project_file(file_id: str, value: ProjectFileRename, request: Request):
        current = await store.get_project_file_for_owner(file_id, request.state.owner_id)
        if not current:
            raise HTTPException(404, "Project file not found")
        try:
            path = managed_project_path(value.path)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        if path.split("/", 1)[0] != current.path.split("/", 1)[0]:
            raise HTTPException(422, "Project file category cannot change during rename")
        try:
            return await store.rename_project_file(file_id, request.state.owner_id, path)
        except KeyError:
            raise HTTPException(404, "Project file not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.delete("/api/project-files/{file_id}")
    async def delete_project_file(file_id: str, request: Request):
        try:
            return await store.delete_project_file(file_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Project file not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    web_root = Path(__file__).resolve().parents[2] / "web"
    static = web_root / "dist" if (web_root / "dist" / "index.html").is_file() else web_root
    if (static / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return (static / "index.html").read_text()

    return app
