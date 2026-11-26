from __future__ import annotations

from sandbox_harness.common.configuration.resources import resource_path

import httpx
import asyncio
import difflib
import hashlib
import json
import logging
import re
import time
import uuid
from botocore.exceptions import ClientError
from datetime import timedelta
from collections import defaultdict, deque
from contextlib import suppress
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin, urlsplit
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from websockets.asyncio.client import connect as websocket_connect

from sandbox_harness.services.api.broker import (Broker)
from sandbox_harness.common.security.auth import (AUTH_COOKIE, AccountAuth, normalize_username)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.catalog.job_catalog import (JobCatalog)
from sandbox_harness.common.catalog.profiles import (ProfileResolver)
from sandbox_harness.common.jobs.job_coordinator import (JobCoordinator, JobGroupDraftUpdate)
from sandbox_harness.services.api.job_continuation import (ContinuationDispatcher)
from sandbox_harness.services.api.execution_graph import (ExecutionGraphProjector)
from sandbox_harness.common.catalog.execution_readiness import (ExecutionReadiness)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate)
from sandbox_harness.common.configuration.config import (Settings, load_settings, validate_service_settings)
from sandbox_harness.common.configuration.databases import (DatabaseSettings)
from sandbox_harness.common.observability.observability import (RequestLoggingMiddleware, telemetry, event)
from sandbox_harness.common.contracts.domain import (Attachment, ChangePasswordRequest, ChangeUsernameRequest, Harness, InteractionResponse, LoginRequest, RegisterRequest, UpdateAccountRequest, MediaDraftCreate, MediaDraftUpdate, MediaReferenceUploadCreate, ProjectFileRename, SandboxReleaseRequest, ThreadCreate, ThreadRead, TurnCreate, TurnInputCreate, TERMINAL, valid_attachment_name, now)
from sandbox_harness.services.api.document_preview import (is_presentation, presentation_pdf)
from sandbox_harness.services.api.model_discovery import (ModelDiscovery)
from sandbox_harness.common.catalog.media_catalog import (MediaCatalog)
from sandbox_harness.common.catalog.media_references import (inspect_reference, resolve_media_references)
from sandbox_harness.common.catalog.provider_preflight import (ProviderPreflight)
from sandbox_harness.common.storage.objects import (object_store)
from sandbox_harness.common.sandbox.pool import (SandboxPool)
from sandbox_harness.common.sandbox.ownership_cleanup import (OwnershipCleanup)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
import sandbox_harness.common.sandbox.sandbox_factory as sandbox_factory
from sandbox_harness.services.api.sandbox_inspector import (SandboxInspector, websocket_endpoint_headers)
from sandbox_harness.common.sites.site_bundle import (safe_site_path)
from sandbox_harness.services.api.sessions import (COOKIE, Sessions)
from sandbox_harness.common.db.accounts import (AccountsRepository)
from sandbox_harness.common.db.api_repositories import (ApiRepositories)
from sandbox_harness.common.db.core import (CoreRepository)
from sandbox_harness.common.db.media_repository import (MediaRepository)
from sandbox_harness.common.db.site_repository import (SiteRepository)

logger = logging.getLogger(__name__)


def web_distribution_digest(root: Path) -> str:
    files=sorted(path for path in root.rglob("*") if path.is_file())
    if not files:raise ValueError("Frontend distribution is empty")
    digest=hashlib.sha256()
    for path in files:
        relative=path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(4,"big"));digest.update(relative)
        size=path.stat().st_size;digest.update(size.to_bytes(8,"big"))
        with path.open("rb") as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b""):digest.update(chunk)
    return digest.hexdigest()

SAFE_INLINE_MIMES = {
    "image/png", "image/jpeg", "image/gif", "image/webp", "image/avif", "image/bmp", "image/svg+xml",
    "text/plain", "application/pdf", "model/gltf-binary",
}
MAX_AVATAR_BYTES = 5 * 1024 * 1024
ISOLATED_SITE_STORAGE_SHIM = b'''<script>(()=>{const makeStorage=()=>{const values=Object.create(null);return Object.freeze({getItem:key=>Object.prototype.hasOwnProperty.call(values,String(key))?values[String(key)]:null,setItem:(key,value)=>{values[String(key)]=String(value)},removeItem:key=>{delete values[String(key)]},clear:()=>{for(const key of Object.keys(values))delete values[key]},key:index=>Object.keys(values)[Number(index)]??null,get length(){return Object.keys(values).length}})};for(const name of ["localStorage","sessionStorage"]){try{window[name].getItem("__updream_storage_probe__")}catch{Object.defineProperty(window,name,{value:makeStorage(),configurable:false})}}window.__updreamStorageMode="isolated-memory"})();</script>'''


def isolated_site_html(data: bytes) -> bytes:
    return ISOLATED_SITE_STORAGE_SHIM + data


def isolated_site_etag(source_sha256: str) -> str:
    return hashlib.sha256((source_sha256 + ":isolated-storage").encode()).hexdigest()


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


def sandbox_preview_target(endpoint: str, path: str, query: str = "") -> str:
    target = urljoin(endpoint.rstrip("/") + "/", path.lstrip("/"))
    return target + ("?" + query if query else "")


def requested_byte_range(value: str, size: int) -> tuple[int, int] | None:
    if not value:
        return None
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", value.strip())
    if not match or size <= 0:
        raise ValueError("invalid_byte_range")
    first, last = match.groups()
    if not first:
        suffix = int(last or 0)
        if suffix <= 0:
            raise ValueError("invalid_byte_range")
        return max(0, size - suffix), size - 1
    start = int(first); end = int(last) if last else size - 1
    if start >= size or end < start:
        raise ValueError("invalid_byte_range")
    return start, min(end, size - 1)


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
    try:
        databases = DatabaseSettings().for_service("api")
    except ValueError:
        if settings.backend != "fake":
            raise
        databases = {
            "accounts": "sqlite+aiosqlite:///" + str(settings.data_root / "api-accounts.db"),
            "core": settings.database_url,
            "media": "sqlite+aiosqlite:///" + str(settings.data_root / "api-media.db"),
            "sites": "sqlite+aiosqlite:///" + str(settings.data_root / "api-sites.db"),
        }
    core = CoreRepository(databases["core"], settings.redis_url, settings.redis_prefix,
        namespace=settings.namespace)
    store = ApiRepositories(AccountsRepository(databases["accounts"]), core,
        MediaRepository(databases["media"]), SiteRepository(databases["sites"]))
    objects = object_store(settings)
    backend=sandbox_factory.build_sandbox_registry(settings)
    releases=sandbox_factory.primary_provider_releases(settings,backend.provider)
    profile_resolver=sandbox_factory.build_profile_resolver(settings,backend,releases)
    images = {harness: releases[harness].ref for harness in Harness}
    targets = {
        Harness.CODEX: settings.warm_pool_codex,
        Harness.CLAUDE: settings.warm_pool_claude,
        Harness.DEEPSEEK: settings.warm_pool_deepseek,
    }
    if sum(targets.values()) >= settings.max_sandboxes:
        raise ValueError("max_sandboxes must exceed the total warm pool target")
    pool=SandboxPool(backend,images,targets,max_sandboxes=settings.max_sandboxes,
        profile_resolver=profile_resolver,ttl_seconds=settings.sandbox_timeout_seconds,
        ownership=core.ownership)
    runtime = RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds,
        reattach_attempts=settings.runtime_stream_reattach_attempts,
        reattach_max_delay_seconds=settings.runtime_stream_reattach_max_delay_seconds,
        bundle_dir=settings.runtime_bundle_dir,
        default_bundle_digest=settings.runtime_bundle_digest,
        code_bundle_dir=settings.runtime_code_bundle_dir,
        default_code_bundle_digest=settings.runtime_code_bundle_digest,
        staging_root=settings.data_root / "tmp" / "runtime-downloads",backend=backend)
    catalog = HarnessCatalog(settings.harness_catalog_json or None)
    media_catalog = MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key))
    discovery = ModelDiscovery(settings, catalog)
    if settings.model_catalog_discovery:
        discovery.load_snapshot()
    return store, objects, pool, catalog, discovery, media_catalog, Broker(settings, store, objects, pool, runtime, catalog)


async def supervise_warm_pool(app, pool, *, initial_delay: float = .5,
                               maximum_delay: float = 30,
                               probe_interval: float = 30) -> None:
    delay = initial_delay
    while True:
        app.state.pool_attempts += 1
        fill_error = None
        try:
            await pool.fill()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            fill_error = exc
            logger.exception("Sandbox warm pool convergence failed; retrying")
        try:
            available = await pool.execution_available()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            app.state.execution_backend_available = False
            app.state.pool_status = "unavailable"
            app.state.pool_last_error = "probe:" + type(exc).__name__
            logger.exception("Sandbox availability probe failed; retrying")
            await asyncio.sleep(delay)
            delay = min(delay * 2, maximum_delay)
            continue
        app.state.execution_backend_available = available
        if fill_error:
            app.state.pool_status = "degraded" if available else "unavailable"
            app.state.pool_last_error = type(fill_error).__name__
            await asyncio.sleep(delay)
            delay = min(delay * 2, maximum_delay)
        else:
            app.state.pool_status = "ready" if available else "unavailable"
            app.state.pool_last_error = "" if available else "execution_unavailable"
            delay = initial_delay
            await asyncio.sleep(probe_interval if available else delay)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    validate_service_settings(settings)
    store, objects, pool, catalog, discovery, media_catalog, broker = components(settings)
    job_catalog_raw = settings.job_catalog_path.read_text()
    job_catalog = JobCatalog.from_json(job_catalog_raw,
        expected_digest=(settings.job_catalog_digest or hashlib.sha256(job_catalog_raw.encode()).hexdigest()))
    profile_catalog_raw = settings.profile_catalog_path.read_text()
    skill_manifest = json.loads(resource_path("runtime/skills/manifest.json").read_text())
    profile_resolver = ProfileResolver.from_json(profile_catalog_raw,
        expected_digest=(settings.profile_catalog_digest or hashlib.sha256(profile_catalog_raw.encode()).hexdigest()),
        skill_manifest=skill_manifest,
        provider=sandbox_factory.release_provider(settings.backend))
    catalog.job_catalog = job_catalog
    catalog.profile_resolver = profile_resolver
    readiness=ExecutionReadiness.from_settings(settings,job_catalog)
    provider_preflight=ProviderPreflight(store,objects,media_catalog)
    job_coordinator = JobCoordinator(store, job_catalog, profile_resolver=profile_resolver,
        draft_ttl_seconds=settings.media_draft_ttl_seconds,readiness=readiness,
        provider_preflight=provider_preflight)
    continuation_dispatcher = ContinuationDispatcher(store, broker)
    execution_graph = ExecutionGraphProjector(store)
    sessions = Sessions(settings.session_signing_key)
    account_auth = AccountAuth(store, settings.auth_session_ttl_seconds)
    inspector = SandboxInspector(settings, store, objects, pool, broker.runtime, broker)
    login_failures: dict[str, deque[float]] = defaultdict(deque)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.initialize()
        await pool.restore_allocation_capacity(await store.core.ownership.capacity_obligations())
        await broker.start()
        await inspector.start()
        protocol_stop = asyncio.Event()
        ownership_cleanup = OwnershipCleanup(store.core.ownership, pool.backend, pool)

        async def drive_domain_protocols():
            delay = settings.supervisor_retry_initial_seconds
            while not protocol_stop.is_set():
                try:
                    await store.relay_pending()
                    await store.advance_lifecycle_operations()
                    await ownership_cleanup.run_once()
                    app.state.domain_protocol_status = "running"
                    app.state.domain_protocol_last_error = ""
                    delay = settings.supervisor_retry_initial_seconds
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    app.state.domain_protocol_status = "degraded"
                    app.state.domain_protocol_last_error = type(exc).__name__
                    event("domain.protocol.failed", level=40, exception_type=type(exc).__name__)
                    delay = min(delay * 2, settings.supervisor_retry_max_seconds)
                try:
                    await asyncio.wait_for(protocol_stop.wait(), timeout=max(.1, delay))
                except asyncio.TimeoutError:
                    pass

        app.state.domain_protocol_status = "running"
        app.state.domain_protocol_last_error = ""
        protocol_task = asyncio.create_task(drive_domain_protocols(), name="domain-protocols")
        continuation_task = asyncio.create_task(continuation_dispatcher.run(),
            name="job-continuation-dispatcher")
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
            initial_delay=settings.supervisor_retry_initial_seconds
            delay = initial_delay
            while not media_outbox_stop.is_set():
                try:
                    batches = await store.claim_media_batches(media_outbox_owner)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    app.state.media_outbox_status="degraded"
                    app.state.media_outbox_last_error=type(exc).__name__
                    app.state.media_outbox_failures+=1
                    event("media.outbox.claim_failed",level=40,
                        exception_type=type(exc).__name__,failures=app.state.media_outbox_failures)
                    try:await asyncio.wait_for(media_outbox_stop.wait(),timeout=delay)
                    except asyncio.TimeoutError:pass
                    delay=min(delay*2,settings.supervisor_retry_max_seconds)
                    continue
                iteration_error=None
                for batch in batches:
                    try:
                        try:
                            await continue_media(batch)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            permanent=isinstance(exc,RuntimeError) and str(exc)=="thread_archived"
                            await store.finish_media_batch(batch["turn_id"], media_outbox_owner,
                                delivered=False, error=type(exc).__name__,permanent=permanent)
                        else:
                            await store.finish_media_batch(batch["turn_id"], media_outbox_owner, delivered=True)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        iteration_error=exc;break
                if iteration_error:
                    app.state.media_outbox_status="degraded"
                    app.state.media_outbox_last_error=type(iteration_error).__name__
                    app.state.media_outbox_failures+=1
                    event("media.outbox.finish_failed",level=40,
                        exception_type=type(iteration_error).__name__,
                        failures=app.state.media_outbox_failures)
                    try:await asyncio.wait_for(media_outbox_stop.wait(),timeout=delay)
                    except asyncio.TimeoutError:pass
                    delay=min(delay*2,settings.supervisor_retry_max_seconds)
                    continue
                app.state.media_outbox_status="running";app.state.media_outbox_last_error="";delay=initial_delay
                try:
                    await asyncio.wait_for(media_outbox_stop.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
        app.state.media_outbox_status="running";app.state.media_outbox_last_error=""
        app.state.media_outbox_failures=0
        media_outbox_task = asyncio.create_task(dispatch_media_outbox(), name="media-outbox-dispatcher")
        draft_expiry_stop=asyncio.Event()
        async def expire_job_group_drafts():
            while not draft_expiry_stop.is_set():
                try:await store.expire_job_group_drafts()
                except Exception as exc:event("job_group_draft.expiry_failed",level=40,
                    exception_type=type(exc).__name__)
                try:await asyncio.wait_for(draft_expiry_stop.wait(),timeout=5)
                except asyncio.TimeoutError:pass
        draft_expiry_task=asyncio.create_task(expire_job_group_drafts(),
            name="job-group-draft-expiry")
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
        if pool.backend.provider != "fake":
            app.state.pool_status = "starting"
            app.state.execution_backend_available = False
            app.state.pool_attempts = 0
            app.state.pool_last_error = ""

            async def fill_pool():
                await supervise_warm_pool(app,pool)

            pool_task = asyncio.create_task(fill_pool(), name="warm-pool-fill")
        else:
            app.state.pool_status = "disabled"
            app.state.execution_backend_available = True
            app.state.pool_attempts = 0
            app.state.pool_last_error = ""
        app.state.required_tasks={"job_continuation":continuation_task,
            "media_outbox":media_outbox_task,"sandbox_expiry":inspector.cleanup_task,
            "turn_recovery":broker.recovery_task,"orphan_reaper":broker.reaper_task,
            "job_group_draft_expiry":draft_expiry_task,"domain_protocols":protocol_task}
        if pool_task:app.state.required_tasks["warm_pool_supervisor"]=pool_task
        yield
        if catalog_task:
            catalog_task.cancel()
            with suppress(asyncio.CancelledError):
                await catalog_task
        if pool_task and not pool_task.done():
            pool_task.cancel()
            with suppress(asyncio.CancelledError):
                await pool_task
        protocol_stop.set()
        await protocol_task
        media_outbox_stop.set()
        await media_outbox_task
        draft_expiry_stop.set();await draft_expiry_task
        continuation_dispatcher.stop_event.set()
        await continuation_task
        await inspector.close()
        preserve_sandbox_keys = await broker.close()
        await pool.close(preserve_sandbox_keys=(preserve_sandbox_keys
            | await store.active_sandbox_keys()))
        await pool.backend.aclose()
        await store.close()

    app = FastAPI(title="Sandbox Harness Platform", lifespan=lifespan)
    app.add_middleware(RequestLoggingMiddleware,service="api")
    app.state.settings = settings
    app.state.store = store
    app.state.objects = objects
    app.state.pool = pool
    app.state.broker = broker
    app.state.account_auth = account_auth
    app.state.inspector = inspector
    app.state.job_coordinator = job_coordinator
    app.state.continuation_dispatcher = continuation_dispatcher
    app.state.execution_graph = execution_graph

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
        if request.url.path == "/api/health" or request.url.path.startswith("/sites/"):
            owner_id, token, new_session = "", "", False
        elif account:owner_id, token, new_session = account.owner_id, "", False
        elif ingress == "frpc":owner_id, token, new_session = "", "", False
        else:
            owner_id, token, new_session = sessions.resolve(request.cookies.get(COOKIE))
            if not await store.accounts.guest_owner_available(owner_id):
                new_session = False
                request.state.guest_principal_claimed = True
        request.state.ingress = ingress
        request.state.account = account
        request.state.auth_token_hash = auth_token_hash
        request.state.owner_id = owner_id
        public_auth = request.url.path in {"/api/health", "/api/session", "/api/auth/login", "/api/auth/register"}
        if (getattr(request.state, "guest_principal_claimed", False)
                and request.url.path.startswith("/api/") and not public_auth
                and not request.url.path.startswith("/api/auth/registrations/")):
            return JSONResponse({"detail":"anonymous_principal_claimed"}, status_code=401,
                headers={"Cache-Control":"no-store"})
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
            response.headers.setdefault("Cache-Control", "no-store")
        elif request.url.path == "/":
            response.headers["Cache-Control"] = "no-store, max-age=0"
        elif request.url.path.startswith("/assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response

    @app.get("/api/health")
    async def health():
        pool_status = app.state.pool_status
        available=app.state.execution_backend_available
        try:
            await asyncio.wait_for(store.healthcheck(),timeout=1)
            database={"available":True,"error":""}
        except Exception as exc:
            database={"available":False,"error":type(exc).__name__}
        supervisors={name:bool(task and not task.done())
            for name,task in getattr(app.state,"required_tasks",{}).items()}
        supervisors_ready=bool(supervisors) and all(supervisors.values())
        ready=available and database["available"] and supervisors_ready
        payload = {"status": "ok" if ready else "degraded",
                   "backend": settings.backend, "execution_backend_available":available,
                   "web_dist_digest":app.state.web_dist_digest,
                   "database":database,"supervisors":supervisors,
                   "media_outbox":{"status":app.state.media_outbox_status,
                       "last_error":app.state.media_outbox_last_error,
                       "failures":app.state.media_outbox_failures},
                   "sandbox_expiry":{"status":inspector.cleanup_status,
                       "last_error":inspector.cleanup_last_error,
                       "failures":inspector.cleanup_failures},
                   "warm_pool": pool_status,
                   "warm_pool_optional_degraded": pool_status == "degraded" and available,
                   "warm_pool_attempts": app.state.pool_attempts,
                   "warm_pool_last_error": app.state.pool_last_error,
                   "broker": broker.status(),
                   "model_catalog": discovery.status(),"observability":telemetry().status() if telemetry() else {"enabled":False}}
        return JSONResponse(payload,status_code=200 if ready else 503)


    @app.get("/api/catalog/harnesses")
    async def harness_catalog():
        return catalog.public()

    @app.get("/api/catalog/media")
    async def media_catalog_route(kind: str | None = None):
        if kind is not None and kind not in {"image", "video", "audio", "model3d"}:
            raise HTTPException(422, "Invalid media kind")
        return {"models": media_catalog.public(kind)}

    @app.get("/api/catalog/execution")
    async def execution_catalog():
        return catalog.public_execution_metadata(await readiness.snapshot())

    @app.post("/api/job-group-drafts")
    async def prepare_job_group(value: JobGroupCreate, request: Request):
        thread = await store.get_thread(value.thread_id, request.state.owner_id)
        turn = await store.get_turn_for_owner(value.origin_turn_id, request.state.owner_id)
        if not thread or not turn or turn.thread_id != thread.id:
            raise HTTPException(404, "Thread or origin Turn not found")
        try:
            return await job_coordinator.prepare_group(request.state.owner_id, value)
        except (KeyError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/job-group-drafts/{draft_id}")
    async def get_job_group_draft(draft_id: str, request: Request):
        value = await store.get_job_group_draft(draft_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "JobGroupDraft not found")
        frozen = await store.get_job_group_draft_request(draft_id, request.state.owner_id)
        return {**value.model_dump(mode="json"), "jobs": [job.model_dump(mode="json") for job in frozen.jobs]}

    @app.get("/api/threads/{thread_id}/job-group-drafts")
    async def list_job_group_drafts(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        result = []
        for draft in await store.list_job_group_drafts(thread_id, request.state.owner_id):
            frozen = await store.get_job_group_draft_request(draft.id, request.state.owner_id)
            result.append({**draft.model_dump(mode="json"),
                "jobs": [job.model_dump(mode="json") for job in frozen.jobs]})
        return result

    @app.patch("/api/job-group-drafts/{draft_id}")
    async def edit_job_group_draft(draft_id: str, value: JobGroupDraftUpdate, request: Request):
        try:
            return await job_coordinator.edit_group(draft_id, request.state.owner_id, value)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/job-group-drafts/{draft_id}/confirm")
    async def confirm_job_group_draft(draft_id: str, request: Request):
        try:
            return await job_coordinator.confirm_group(draft_id, request.state.owner_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/job-group-drafts/{draft_id}/reject")
    async def reject_job_group_draft(draft_id: str, request: Request):
        try:
            return await job_coordinator.reject_group(draft_id, request.state.owner_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/job-groups/{group_id}")
    async def get_job_group(group_id: str, request: Request):
        group = await store.get_job_group_for_owner(group_id, request.state.owner_id)
        if not group:
            raise HTTPException(404, "JobGroup not found")
        return {"group": group, "jobs": await store.list_jobs_for_group(group.id, request.state.owner_id)}

    @app.post("/api/job-groups/{group_id}/cancel")
    async def cancel_job_group(group_id: str, request: Request):
        try:
            return {"jobs": await job_coordinator.cancel_group(group_id, request.state.owner_id)}
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/api/jobs/{job_id}")
    async def get_job(job_id: str, request: Request):
        value = await store.get_job_for_owner(job_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Job not found")
        return {**value.model_dump(mode="json"),"cleanup":await store.get_job_cleanup(job_id)}

    @app.post("/api/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str, request: Request):
        try:
            return await job_coordinator.cancel_job(job_id, request.state.owner_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

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

    @app.post("/api/turns/{turn_id}/media/drafts/confirm")
    async def confirm_media_batch(turn_id:str,request:Request):
        drafts=await store.list_media_drafts_for_turn(turn_id,request.state.owner_id)
        if not drafts:raise HTTPException(404,"Media batch not found")
        specs={}
        try:
            for draft in drafts:
                if draft.status!="awaiting_confirmation":continue
                spec=media_catalog.resolve(draft.kind,draft.model)
                snapshot=await store.get_media_draft_snapshot(draft.id,request.state.owner_id)
                media_catalog.validate_parameters(draft.kind,draft.model,draft.parameters,
                    generation_mode=draft.generation_mode,references=snapshot["references"])
                specs[draft.id]=spec
            return await store.confirm_media_batch(turn_id,request.state.owner_id,
                provider_specs=specs)
        except (KeyError,ValueError) as exc:
            raise HTTPException(422,str(exc))
        except RuntimeError as exc:
            raise HTTPException(409,str(exc))

    @app.post("/api/turns/{turn_id}/media/drafts/cancel")
    async def cancel_media_batch(turn_id:str,request:Request):
        try:return await store.cancel_media_batch(turn_id,request.state.owner_id)
        except KeyError:raise HTTPException(404,"Media batch not found")
        except RuntimeError as exc:raise HTTPException(409,str(exc))

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

    @app.post("/api/media/jobs/{job_id}/refresh")
    async def refresh_media_job(job_id: str, request: Request):
        try:
            value = await store.request_media_refresh(job_id, request.state.owner_id)
            event("media.job.refresh_requested", job_id=job_id, status=value.status,
                phase=value.phase)
            return value
        except KeyError:
            raise HTTPException(404, "Media job not found")

    @app.get("/api/threads/{thread_id}/sites/drafts")
    async def list_site_drafts(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_site_drafts(thread_id, request.state.owner_id)

    @app.post("/api/sites/drafts/{draft_id}/confirm")
    async def confirm_site_draft(draft_id: str, request: Request):
        try:
            return await store.confirm_site_draft(draft_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Site draft not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/api/sites/drafts/{draft_id}/cancel")
    async def cancel_site_draft(draft_id: str, request: Request):
        try:
            return await store.cancel_site_draft(draft_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Site draft not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    async def site_draft_preview_response(draft_id: str, path: str, request: Request):
        try:
            path = safe_site_path(path.strip("/") or "index.html")
            file = await store.get_site_draft_file(draft_id, path, request.state.owner_id)
        except ValueError:
            file = None
        if file is None:
            raise HTTPException(404, "Site draft preview file not found")
        data = await asyncio.to_thread(objects.get, file["object_ref"], maximum=file["size"])
        if len(data) != file["size"] or hashlib.sha256(data).hexdigest() != file["sha256"]:
            raise HTTPException(409, "Site draft preview identity mismatch")
        headers = {"Cache-Control":"private, no-cache", "ETag":'"' + file["sha256"] + '"'}
        if file["mime"] == "text/html":
            data = isolated_site_html(data)
            headers["ETag"] = '"' + isolated_site_etag(file["sha256"]) + '"'
            headers["Content-Security-Policy"] = ("sandbox allow-scripts allow-forms allow-modals "
                "allow-pointer-lock allow-downloads; default-src 'self' data: blob:; "
                "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; "
                "style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; "
                "media-src 'self' data: blob:; connect-src 'self'; worker-src 'self' blob:")
        return Response(data, media_type=file["mime"], headers=headers)

    @app.get("/api/sites/drafts/{draft_id}/preview/")
    async def site_draft_preview_index(draft_id: str, request: Request):
        return await site_draft_preview_response(draft_id, "index.html", request)

    @app.get("/api/sites/drafts/{draft_id}/preview/{path:path}")
    async def site_draft_preview_file(draft_id: str, path: str, request: Request):
        return await site_draft_preview_response(draft_id, path, request)

    @app.get("/api/threads/{thread_id}/sites/jobs")
    async def list_site_jobs(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_site_jobs(thread_id, request.state.owner_id)

    @app.get("/api/sites/jobs/{job_id}")
    async def get_site_job(job_id: str, request: Request):
        value = await store.get_site_job(job_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Site deployment job not found")
        return value

    @app.post("/api/sites/jobs/{job_id}/retry")
    async def retry_site_job(job_id: str, request: Request):
        try:
            return await store.retry_site_job(job_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Site deployment job not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/threads/{thread_id}/sites")
    async def list_site_projects(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_site_projects(thread_id, request.state.owner_id)

    @app.post("/api/sites/{project_id}/versions/{version_id}/activate")
    async def activate_site_version(project_id: str, version_id: str, request: Request):
        try:
            return await store.activate_site_version(project_id, version_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Site version not found")

    @app.post("/api/sites/{project_id}/unpublish")
    async def unpublish_site(project_id: str, request: Request):
        try:
            return await store.unpublish_site(project_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Site project not found")

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
        try: canonical_username = normalize_username(value.username)
        except ValueError: canonical_username = "invalid-user"
        identity = hashlib.sha256((source + "\0" + canonical_username).encode()).hexdigest()
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
            operation_id = hashlib.sha256(("registration\0" + request.state.owner_id
                + "\0" + username).encode()).hexdigest()
            try:
                existing = await store.accounts.registration_result(
                    operation_id, request.state.owner_id)
            except KeyError:
                existing = None
            if existing:
                if existing["state"] == "pending":
                    await store.drive_lifecycle(operation_id)
                    existing = await store.accounts.registration_result(
                        operation_id, request.state.owner_id)
                if existing["state"] == "refused":
                    raise RuntimeError(existing["error_code"])
                if existing["state"] == "pending":
                    return JSONResponse({"operation_id":operation_id,"state":"pending"},
                        status_code=202)
                replay = await store.accounts.registration_replay_user(
                    request.state.owner_id, username)
                account = await account_auth.authenticate(value.username, value.password)
                if not replay or not account or account.id != replay.id:
                    raise RuntimeError("registration_identity_conflict")
                token, _ = await account_auth.create_session(account.id)
            else:
                token, digest, expires_at = account_auth.new_session()
                result = await store.begin_registration(operation_id, request.state.owner_id,
                    username, password_hash, session_token_hash=digest,
                    session_expires_at=expires_at)
                if result["state"] == "refused":
                    raise RuntimeError(result["error_code"])
                if result["state"] == "pending":
                    response = JSONResponse({"operation_id":operation_id,"state":"pending"},
                        status_code=202)
                    response.set_cookie(AUTH_COOKIE, token, httponly=True, samesite="lax",
                        secure=secure_cookie(request), max_age=settings.auth_session_ttl_seconds,
                        path=cookie_path(request))
                    return response
                account = await store.accounts.get_user(result["user_id"])
                if account is None:
                    raise RuntimeError("registration_result_invalid")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        response = JSONResponse({"authenticated": True, "user": account.model_dump(mode="json")})
        response.delete_cookie(COOKIE, path=cookie_path(request))
        response.set_cookie(AUTH_COOKIE, token, httponly=True, samesite="lax", secure=secure_cookie(request),
            max_age=settings.auth_session_ttl_seconds, path=cookie_path(request))
        return response

    @app.get("/api/auth/registrations/{operation_id}")
    async def registration_status(operation_id: str, request: Request):
        try:
            await store.drive_lifecycle(operation_id)
            return await store.accounts.registration_result(operation_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "registration_not_found")

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
            result = await broker.set_thread_archived(thread_id, request.state.owner_id, archived=True)
            if getattr(result, "state", "") == "pending":
                return JSONResponse({"operation_id":result.operation_id,"state":"pending"},
                    status_code=202)
            return result
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/api/threads/{thread_id}/unarchive")
    async def unarchive_thread(thread_id: str, request: Request):
        try:
            result = await broker.set_thread_archived(thread_id, request.state.owner_id, archived=False)
            if getattr(result, "state", "") == "pending":
                return JSONResponse({"operation_id":result.operation_id,"state":"pending"},
                    status_code=202)
            return result
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.delete("/api/threads/{thread_id}")
    async def delete_thread(thread_id: str, request: Request):
        try:
            result = await broker.delete_thread(thread_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        if result.state == "pending":
            return JSONResponse({"operation_id":result.operation_id,"state":"pending"},
                status_code=202)
        return Response(status_code=204)

    @app.get("/api/lifecycle/{operation_id}")
    async def lifecycle_status(operation_id: str, request: Request):
        try:
            result = await store.lifecycle_status(operation_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "lifecycle_operation_not_found")
        return {"operation_id":result.operation_id,"state":result.state,
                "error_code":result.error_code}

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

    @app.get("/api/threads/{thread_id}/execution-graph")
    async def thread_execution_graph(thread_id: str, request: Request, cursor: str = "",
                                     include_history: bool = False, history_before: str = ""):
        try:
            return await execution_graph.snapshot(thread_id, request.state.owner_id,
                cursor=cursor, include_history=include_history,history_before=history_before)
        except KeyError:
            raise HTTPException(404, "Thread not found")

    @app.get("/api/threads/{thread_id}/job-waits")
    async def thread_job_waits(thread_id: str, request: Request):
        try:
            return await execution_graph.waits(thread_id,request.state.owner_id)
        except KeyError:
            raise HTTPException(404,"Thread not found")

    @app.get("/api/jobs/{job_id}/activities")
    async def job_activities(job_id: str, request: Request, after: int = 0, limit: int = 500,
                             view: str = "display"):
        job = await store.get_job_for_owner(job_id, request.state.owner_id)
        if not job:
            raise HTTPException(404, "Job not found")
        if view=="raw":return await store.list_job_activities(job_id,after,limit)
        if view!="display":raise HTTPException(422,"Invalid activity view")
        return await store.list_job_display_activities(job_id,after,limit)

    @app.get("/api/threads/{thread_id}/project-files")
    async def list_project_files(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return await store.current_project_files(thread_id, request.state.owner_id)

    async def ensure_project_files_mutable(thread_id: str, owner_id: str) -> None:
        if await store.get_sandbox_control(thread_id, owner_id):
            raise HTTPException(409, "sandbox_takeover_active")

    @app.post("/api/threads/{thread_id}/project-files")
    async def upload_project_file(thread_id: str, request: Request, path: str, mime: str = "application/octet-stream"):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        await ensure_project_files_mutable(thread_id, request.state.owner_id)
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

    @app.get("/api/turns/{turn_id}/project-files")
    async def turn_project_files(turn_id: str, request: Request):
        if not await store.get_turn_for_owner(turn_id, request.state.owner_id):
            raise HTTPException(404, "Turn not found")
        return await store.turn_project_files(turn_id, request.state.owner_id)

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

    @app.get("/api/threads/{thread_id}/pause")
    async def thread_pause_status(thread_id: str, request: Request):
        try:
            control = await store.get_thread_control(thread_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        participants = await store.list_pause_participants(thread_id, control.generation)
        return {**control.model_dump(mode="json"),
            "participants": [value.model_dump(mode="json") for value in participants]}

    @app.post("/api/threads/{thread_id}/pause")
    async def pause_thread_work(thread_id: str, request: Request):
        try:
            control = await broker.pause_thread(thread_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        participants = await store.list_pause_participants(thread_id, control.generation)
        return {**control.model_dump(mode="json"),
            "participants": [value.model_dump(mode="json") for value in participants]}

    @app.post("/api/threads/{thread_id}/resume")
    async def resume_thread_work(thread_id: str, request: Request):
        try:
            return await store.resume_thread(thread_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/api/threads/{thread_id}/stop-all")
    async def stop_all_thread_work(thread_id: str, request: Request):
        thread = await store.get_thread(thread_id, request.state.owner_id)
        if not thread:
            raise HTTPException(404, "Thread not found")
        jobs = await store.stop_all_jobs(thread_id, request.state.owner_id)
        if thread.active_turn_id:
            active = await store.get_turn(thread.active_turn_id)
            if active and active.state not in TERMINAL:
                try:
                    await broker.cancel(active.id)
                except RuntimeError:
                    pass
        return {"accepted": True, "jobs": jobs}

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
                    await store.set_input_status(result.id, "unknown", "delivery_unconfirmed",
                        execution_id=turn.runtime_execution_id)
                    event("input.delivery.unknown",level=logging.WARNING,turn_id=turn_id,input_id=result.id)
                except Exception as exc:
                    await store.set_input_status(result.id, "failed", "delivery_failed",
                        execution_id=turn.runtime_execution_id)
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
                if turn and turn.state.value in {"COMPLETED", "WAITING_USER", "FAILED", "CANCELLED", "PAUSED"} and not rows:
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

    async def sandbox_api(call):
        try:
            return await call
        except KeyError:
            raise HTTPException(404, "Sandbox or Thread not found")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            raise HTTPException(404 if status == 404 else 502, "Sandbox runtime is unavailable")
        except httpx.HTTPError:
            raise HTTPException(502, "Sandbox runtime is unavailable")

    @app.get("/api/threads/{thread_id}/sandbox")
    async def sandbox_status(thread_id: str, request: Request):
        return await sandbox_api(inspector.status(thread_id, request.state.owner_id))

    @app.post("/api/threads/{thread_id}/sandbox/restore")
    async def sandbox_restore(thread_id: str, request: Request):
        await sandbox_api(inspector.restore(thread_id, request.state.owner_id))
        return await sandbox_api(inspector.status(thread_id, request.state.owner_id))

    @app.get("/api/threads/{thread_id}/sandbox/files")
    async def sandbox_files(thread_id: str, request: Request, path: str = "project", depth: int = 2):
        return await sandbox_api(inspector.files(thread_id, request.state.owner_id, path, depth))

    @app.get("/api/threads/{thread_id}/sandbox/file")
    async def sandbox_file(thread_id: str, request: Request, path: str):
        data, mime = await sandbox_api(inspector.file(thread_id, request.state.owner_id, path))
        safe_inline = mime.startswith("text/") or mime in {"application/json", "application/xml"}
        return Response(data, media_type=mime if safe_inline else "application/octet-stream",
            headers={"Content-Disposition": content_disposition(Path(path).name, inline=safe_inline),
                "Content-Security-Policy": "sandbox; default-src 'none'"})

    @app.put("/api/threads/{thread_id}/sandbox/file")
    async def sandbox_write_file(thread_id: str, request: Request, path: str):
        data = await limited_body(request, settings.sandbox_inspect_max_file_bytes)
        return await sandbox_api(inspector.write_file(thread_id, request.state.owner_id,
            path, data, request.headers.get("content-type", "text/plain").split(";", 1)[0]))

    @app.get("/api/threads/{thread_id}/sandbox/processes")
    async def sandbox_processes(thread_id: str, request: Request):
        return await sandbox_api(inspector.processes(thread_id, request.state.owner_id))

    @app.post("/api/threads/{thread_id}/sandbox/processes/{pid}/stop")
    async def sandbox_stop_process(thread_id: str, pid: int, request: Request):
        return await sandbox_api(inspector.stop_process(thread_id, request.state.owner_id, pid))

    @app.get("/api/threads/{thread_id}/sandbox/ports")
    async def sandbox_ports(thread_id: str, request: Request):
        return await sandbox_api(inspector.ports(thread_id, request.state.owner_id))

    @app.get("/api/threads/{thread_id}/sandbox/audit")
    async def sandbox_audit(thread_id: str, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        return {"items": await store.list_sandbox_audit(thread_id, request.state.owner_id)}

    @app.get("/api/threads/{thread_id}/sandbox/managed-changes")
    async def sandbox_managed_changes(thread_id: str, request: Request):
        return await sandbox_api(inspector.managed_changes(thread_id, request.state.owner_id))

    @app.post("/api/threads/{thread_id}/sandbox/takeover")
    async def sandbox_takeover(thread_id: str, request: Request, interrupt: bool = False):
        network_access = "public" if request.state.account else "restricted"
        return await sandbox_api(inspector.takeover(thread_id, request.state.owner_id,
            network_access=network_access, interrupt=interrupt))

    @app.post("/api/threads/{thread_id}/sandbox/release")
    async def sandbox_release(thread_id: str, request: Request,
                              value: SandboxReleaseRequest = SandboxReleaseRequest()):
        return await sandbox_api(inspector.release(thread_id, request.state.owner_id,
            managed_action=value.managed_action,allow_checkpoint_loss=value.allow_checkpoint_loss))

    @app.post("/api/threads/{thread_id}/sandbox/terminal-ticket")
    async def sandbox_terminal_ticket(thread_id: str, request: Request):
        return await sandbox_api(inspector.terminal_ticket(thread_id, request.state.owner_id))

    @app.post("/api/threads/{thread_id}/sandbox/ports/{port}/grant")
    async def sandbox_port_grant(thread_id: str, port: int, request: Request):
        value = await sandbox_api(inspector.create_port_grant(thread_id,
            request.state.owner_id, port))
        return {**value, "preview_url": f"/api/sandbox-preview/{value['id']}/"}

    @app.delete("/api/sandbox-preview/{grant_id}")
    async def revoke_sandbox_port_grant(grant_id: str, request: Request):
        if not await store.revoke_sandbox_port_grant(grant_id, request.state.owner_id):
            raise HTTPException(404, "Sandbox port grant not found")
        return {"revoked": True}

    def filtered_proxy_headers(headers, *, request_headers: bool) -> dict[str, str]:
        blocked = {"connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade",
            "host", "cookie", "authorization", "proxy-authorization"}
        if not request_headers:
            blocked |= {"content-length", "content-encoding", "set-cookie", "x-frame-options"}
        return {key: value for key, value in headers.items() if key.lower() not in blocked}

    @app.api_route("/api/sandbox-preview/{grant_id}/{path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    async def sandbox_port_preview(grant_id: str, path: str, request: Request):
        try:
            grant, endpoint, endpoint_headers = await inspector.port_endpoint(
                grant_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Sandbox port preview not found")
        except (httpx.HTTPError, RuntimeError):
            raise HTTPException(502, "Sandbox port is unavailable")
        target = sandbox_preview_target(endpoint, path, request.url.query)
        body = await limited_body(request, 16 * 1024 * 1024)
        client = httpx.AsyncClient(headers=endpoint_headers, timeout=60,
            follow_redirects=False, trust_env=False)
        try:
            upstream = await client.send(client.build_request(request.method, target,
                headers=filtered_proxy_headers(request.headers, request_headers=True),
                content=body), stream=True)
        except Exception:
            await client.aclose()
            raise HTTPException(502, "Sandbox port is unavailable")
        content_type = upstream.headers.get("content-type", "application/octet-stream")
        prefix = f"/api/sandbox-preview/{grant_id}/"
        if request.method != "HEAD" and content_type.startswith(("text/html", "text/css")):
            data = await upstream.aread(); await upstream.aclose(); await client.aclose()
            if len(data) > 8 * 1024 * 1024:
                raise HTTPException(413, "Sandbox preview document is too large")
            text = data.decode(errors="replace")
            if content_type.startswith("text/html"):
                base = f'<base href="{prefix}">'
                text = text.replace("<head>", "<head>" + base, 1) if "<head>" in text else base + text
                for attribute in ("src", "href", "action"):
                    text = text.replace(f'{attribute}="/', f'{attribute}="{prefix}')
                    text = text.replace(f"{attribute}='/", f"{attribute}='{prefix}")
            else:
                text = text.replace("url(/", f"url({prefix}")
            headers = filtered_proxy_headers(upstream.headers, request_headers=False)
            headers["Content-Security-Policy"] = ("default-src 'self' data: blob:; script-src 'self' 'unsafe-inline' "
                "'unsafe-eval' blob:; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; frame-ancestors 'self'")
            return Response(text, status_code=upstream.status_code,
                media_type=content_type.split(";", 1)[0], headers=headers)

        async def stream_upstream():
            try:
                async for chunk in upstream.aiter_bytes():
                    yield chunk
            except Exception as exc:
                event("sandbox.port_stream.failed", level=logging.WARNING, exc=exc,
                    reason=type(exc).__name__)
                logger.exception("Sandbox port response stream failed", extra={"grant_id": grant_id})
                raise
            finally:
                await upstream.aclose(); await client.aclose()
        headers = filtered_proxy_headers(upstream.headers, request_headers=False)
        location = upstream.headers.get("location")
        if location:
            parsed_location = urlsplit(location)
            redirected = parsed_location.path.lstrip("/")
            if parsed_location.query:
                redirected += "?" + parsed_location.query
            headers["Location"] = prefix + redirected
        return StreamingResponse(stream_upstream(), status_code=upstream.status_code,
            media_type=content_type.split(";", 1)[0], headers=headers)

    async def websocket_owner(websocket: WebSocket) -> str:
        origin = websocket.headers.get("origin")
        if origin and urlsplit(origin).netloc != websocket.headers.get("host"):
            raise ValueError("cross_site_websocket_rejected")
        ingress = websocket.headers.get("x-updream-ingress", "direct").strip().lower()
        account, _ = await account_auth.resolve(websocket.cookies.get(AUTH_COOKIE))
        if account:
            return account.owner_id
        if ingress == "frpc":
            raise ValueError("authentication_required")
        owner_id, _, new_session = sessions.resolve(websocket.cookies.get(COOKIE))
        if new_session:
            raise ValueError("browser_session_required")
        return owner_id

    async def relay_websockets(browser: WebSocket, upstream) -> None:
        async def to_browser():
            async for message in upstream:
                if isinstance(message, bytes):
                    await browser.send_bytes(message)
                else:
                    await browser.send_text(message)

        async def to_upstream():
            while True:
                message = await browser.receive()
                if message["type"] == "websocket.disconnect":
                    return
                if message.get("bytes") is not None:
                    await upstream.send(message["bytes"])
                elif message.get("text") is not None:
                    await upstream.send(message["text"])

        first = asyncio.create_task(to_browser())
        second = asyncio.create_task(to_upstream())
        done, pending = await asyncio.wait({first, second}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exception = task.exception() if not task.cancelled() else None
            if exception and not isinstance(exception, WebSocketDisconnect):
                raise exception

    @app.websocket("/api/threads/{thread_id}/sandbox/terminal")
    async def sandbox_terminal(websocket: WebSocket, thread_id: str, ticket: str, since: int = 0):
        try:
            owner_id = await websocket_owner(websocket)
            ticket_value = inspector.tickets.verify(ticket)
            if ticket_value.get("thread_id") != thread_id or ticket_value.get("owner_id") != owner_id:
                raise ValueError("terminal_ticket_mismatch")
            connection = await inspector.terminal_connection(ticket, owner_id, since)
            async with connection as upstream:
                await websocket.accept()
                async def heartbeat():
                    while True:
                        await asyncio.sleep(max(1,settings.sandbox_control_ttl_seconds/3))
                        await inspector.heartbeat(thread_id,owner_id)
                heartbeat_task=asyncio.create_task(heartbeat(),name="sandbox-terminal-heartbeat")
                try:await relay_websockets(websocket, upstream)
                finally:
                    heartbeat_task.cancel()
                    await asyncio.gather(heartbeat_task,return_exceptions=True)
        except (ValueError, KeyError) as exc:
            event("sandbox.terminal.rejected", level=logging.WARNING,
                thread_id=thread_id, reason=str(exc))
            await websocket.close(code=4403)
        except WebSocketDisconnect:
            return
        except Exception as exc:
            event("sandbox.terminal.relay_failed", level=logging.WARNING,
                thread_id=thread_id, exc=exc)
            with suppress(Exception):
                await websocket.close(code=1011)

    @app.websocket("/api/sandbox-preview/{grant_id}/{path:path}")
    async def sandbox_port_preview_websocket(websocket: WebSocket, grant_id: str, path: str):
        try:
            owner_id = await websocket_owner(websocket)
            _, endpoint, endpoint_headers = await inspector.port_endpoint(grant_id, owner_id)
            target = ("wss://" if endpoint.startswith("https://") else "ws://") + endpoint.split("://", 1)[1]
            target = urljoin(target.rstrip("/") + "/", path or "")
            if websocket.url.query:
                target += "?" + websocket.url.query
            requested = [value.strip() for value in websocket.headers.get(
                "sec-websocket-protocol", "").split(",") if value.strip()]
            async with websocket_connect(target,
                    additional_headers=websocket_endpoint_headers(endpoint_headers),
                    subprotocols=requested or None, max_size=64 * 1024 * 1024,
                    open_timeout=20, ping_interval=20, ping_timeout=20) as upstream:
                await websocket.accept(subprotocol=upstream.subprotocol)
                await relay_websockets(websocket, upstream)
        except (ValueError, KeyError) as exc:
            event("sandbox.port_websocket.rejected", level=logging.WARNING,
                reason=str(exc))
            await websocket.close(code=4403)
        except WebSocketDisconnect:
            return
        except Exception as exc:
            event("sandbox.port_websocket.relay_failed", level=logging.WARNING,
                exception_type=type(exc).__name__)
            with suppress(Exception):
                await websocket.close(code=1011)

    @app.post("/api/uploads")
    async def upload(request: Request, name: str, mime: str = "application/octet-stream"):
        if not valid_filename(name):
            raise HTTPException(400, "Invalid filename")
        data = await limited_body(request, settings.max_attachment_bytes)
        object_ref, digest = await asyncio.to_thread(objects.put, data)
        attachment = Attachment(name=name, object_ref=object_ref, sha256=digest, size=len(data), mime=mime)
        await store.record_upload(request.state.owner_id, attachment)
        return attachment

    async def immutable_object_response(value, *, maximum: int, request: Request,
                                        filename: str, inline: bool):
        size = await asyncio.to_thread(objects.size, value.object_ref, maximum=maximum)
        etag = '"' + value.sha256 + '"'
        cache_headers = {"ETag":etag,"Accept-Ranges":"bytes",
            "Cache-Control":"private, max-age=31536000, immutable"}
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=cache_headers)
        try:
            selected = requested_byte_range(request.headers.get("range", ""), size)
        except ValueError:
            raise HTTPException(416, "invalid_byte_range", headers={"Content-Range":f"bytes */{size}"})
        start, end = selected or (0, max(0, size - 1))
        safe_inline = inline and (value.mime in SAFE_INLINE_MIMES or
            value.mime.startswith(("video/", "audio/")))
        if size == 0:
            headers = {**cache_headers,
                "Content-Disposition":content_disposition(filename, inline=safe_inline),
                "Content-Security-Policy":"sandbox; default-src 'none'",
                "Content-Length":"0"}
            media_type = value.mime if safe_inline or not inline else "application/octet-stream"
            return Response(b"", media_type=media_type, headers=headers)
        headers = {**cache_headers,
            "Content-Disposition":content_disposition(filename, inline=safe_inline),
            "Content-Security-Policy":"sandbox; default-src 'none'",
            "Content-Length":str(end - start + 1)}
        if selected:
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        media_type = value.mime if safe_inline or not inline else "application/octet-stream"
        return StreamingResponse(objects.iter_bytes(value.object_ref, maximum=maximum,
            start=start, end=end), status_code=206 if selected else 200,
            media_type=media_type, headers=headers)

    @app.get("/api/artifacts/{artifact_id}")
    async def artifact(artifact_id: str, request: Request, inline: bool = False):
        value = await store.get_artifact_for_owner(artifact_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Artifact not found")
        filename = Path(value.path).name
        return await immutable_object_response(value, maximum=settings.max_artifact_bytes,
            request=request, filename=filename, inline=inline)

    async def artifact_site_preview_response(artifact_id: str, path: str, request: Request):
        try:
            path = safe_site_path(path.strip("/") or "index.html")
            value = await store.artifact_site_file(artifact_id, path, request.state.owner_id)
        except ValueError:
            value = None
        if not value:
            raise HTTPException(404, "Site preview file not found")
        data = await asyncio.to_thread(objects.get, value["object_ref"], maximum=value["size"])
        if len(data) != value["size"] or hashlib.sha256(data).hexdigest() != value["sha256"]:
            raise HTTPException(409, "Site preview identity mismatch")
        headers = {"Cache-Control":"private, no-cache", "ETag":'"' + value["sha256"] + '"'}
        if value["mime"] == "text/html":
            data = isolated_site_html(data)
            headers["ETag"] = '"' + isolated_site_etag(value["sha256"]) + '"'
            headers["Content-Security-Policy"] = ("sandbox allow-scripts allow-forms allow-modals "
                "allow-pointer-lock allow-downloads; default-src 'self' data: blob:; "
                "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; "
                "style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; "
                "font-src 'self' data:; media-src 'self' data: blob:; connect-src 'self'; "
                "worker-src 'self' blob:")
        return Response(data, media_type=value["mime"], headers=headers)

    @app.get("/api/artifacts/{artifact_id}/site-preview/")
    async def artifact_site_preview_index(artifact_id: str, request: Request):
        return await artifact_site_preview_response(artifact_id, "index.html", request)

    @app.get("/api/artifacts/{artifact_id}/site-preview/{path:path}")
    async def artifact_site_preview_file(artifact_id: str, path: str, request: Request):
        return await artifact_site_preview_response(artifact_id, path, request)

    @app.get("/api/project-files/{file_id}")
    async def project_file(file_id: str, request: Request, inline: bool = False):
        value = await store.get_project_file_for_owner(file_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Project file not found")
        maximum = settings.max_attachment_bytes if value.source_type == "upload" else settings.max_artifact_bytes
        filename = Path(value.path).name
        return await immutable_object_response(value, maximum=maximum, request=request,
            filename=filename, inline=inline)

    async def public_site_response(slug: str, path: str, request: Request):
        try:
            path = path.strip("/") or "index.html"
            if path.endswith("/"):
                path += "index.html"
            path = safe_site_path(path)
            value = await store.public_site_file(slug, path)
        except ValueError:
            value = None
        if not value:
            raise HTTPException(404, "Published site file not found")
        source_size = value["size"]
        try:
            if await asyncio.to_thread(objects.size,value["object_ref"],maximum=settings.max_artifact_bytes)!=source_size:
                raise ValueError("published_site_object_size_mismatch")
        except Exception:
            raise HTTPException(503,"Published site file is temporarily unavailable")
        is_html = value["mime"] == "text/html"
        size = source_size + (len(ISOLATED_SITE_STORAGE_SHIM) if is_html else 0)
        etag_value = isolated_site_etag(value["sha256"]) if is_html else value["sha256"]
        etag = '"' + etag_value + '"'
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={"ETag":etag, "Cache-Control":"no-cache"})
        try:
            selected = None if is_html else requested_byte_range(request.headers.get("range", ""), size)
        except ValueError:
            raise HTTPException(416, "invalid_byte_range", headers={"Content-Range":f"bytes */{size}"})
        start, end = selected or (0, size - 1)
        headers = {"ETag":etag, "Accept-Ranges":"bytes", "Cache-Control":"no-cache",
            "Content-Length":str(end - start + 1), "X-Published-Site-Version":value["version_id"]}
        if selected:
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        if is_html:
            headers["Content-Security-Policy"] = ("sandbox allow-scripts allow-forms allow-modals "
                "allow-pointer-lock allow-downloads; default-src 'self' data: blob:; "
                "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; "
                "style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; "
                "font-src 'self' data:; media-src 'self' data: blob:; "
                "connect-src 'self'; worker-src 'self' blob:; frame-ancestors *")
        def content():
            if is_html:
                yield ISOLATED_SITE_STORAGE_SHIM
                yield from objects.iter_bytes(value["object_ref"], maximum=settings.max_artifact_bytes,
                    start=0, end=source_size - 1)
            else:
                yield from objects.iter_bytes(value["object_ref"], maximum=settings.max_artifact_bytes,
                    start=start, end=end)
        return StreamingResponse(content(), status_code=206 if selected else 200,
            media_type=value["mime"], headers=headers)

    @app.get("/sites/{slug}/")
    async def public_site_index(slug: str, request: Request):
        return await public_site_response(slug, "index.html", request)

    @app.get("/sites/{slug}/{path:path}")
    async def public_site_file(slug: str, path: str, request: Request):
        return await public_site_response(slug, path, request)

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
        await ensure_project_files_mutable(current.thread_id, request.state.owner_id)
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

    @app.post("/api/project-files/{file_id}/restore")
    async def restore_project_file(file_id: str, request: Request):
        try:
            current = await store.get_project_file_for_owner(file_id, request.state.owner_id)
            if not current:
                raise KeyError("project_file_not_found")
            await ensure_project_files_mutable(current.thread_id, request.state.owner_id)
            value, created = await store.restore_project_file_version(file_id, request.state.owner_id)
            return {"file": value, "created": created}
        except KeyError:
            raise HTTPException(404, "Project file not found")

    @app.get("/api/project-file-versions/compare")
    async def compare_project_files(left_id: str, right_id: str, request: Request):
        left = await store.get_project_file_for_owner(left_id, request.state.owner_id)
        right = await store.get_project_file_for_owner(right_id, request.state.owner_id)
        if not left or not right:
            raise HTTPException(404, "Project file not found")
        if left.thread_id != right.thread_id or left.path != right.path:
            raise HTTPException(422, "Project file versions must belong to the same path")
        result = {"left": left, "right": right, "diff": "", "diff_available": False,
            "reason": "binary_or_large"}
        textual = (left.mime.startswith("text/") or left.mime in {"application/json", "application/xml"})
        if textual and max(left.size, right.size) <= 1024 * 1024:
            left_data = await asyncio.to_thread(objects.get, left.object_ref, maximum=1024 * 1024)
            right_data = await asyncio.to_thread(objects.get, right.object_ref, maximum=1024 * 1024)
            if (hashlib.sha256(left_data).hexdigest() != left.sha256
                    or hashlib.sha256(right_data).hexdigest() != right.sha256):
                raise HTTPException(409, "Project file object identity mismatch")
            diff = "".join(difflib.unified_diff(left_data.decode(errors="replace").splitlines(True),
                right_data.decode(errors="replace").splitlines(True),
                fromfile=f"v{left.version}", tofile=f"v{right.version}"))
            result.update({"diff": diff[:200_000], "diff_available": True, "reason": ""})
        return result

    @app.delete("/api/project-files/{file_id}")
    async def delete_project_file(file_id: str, request: Request):
        try:
            current = await store.get_project_file_for_owner(file_id, request.state.owner_id)
            if not current:
                raise KeyError("project_file_not_found")
            await ensure_project_files_mutable(current.thread_id, request.state.owner_id)
            return await store.delete_project_file(file_id, request.state.owner_id)
        except KeyError:
            raise HTTPException(404, "Project file not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    web_root = resource_path("web")
    static = web_root / "dist" if (web_root / "dist" / "index.html").is_file() else web_root
    served_web_digest=web_distribution_digest(static)
    if settings.web_dist_digest and served_web_digest!=settings.web_dist_digest:
        raise ValueError("Configured frontend distribution digest does not match served files")
    app.state.web_dist_digest=served_web_digest
    if (static / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return (static / "index.html").read_text()

    return app
