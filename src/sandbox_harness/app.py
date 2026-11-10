from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from contextlib import suppress
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .broker import Broker
from .catalog import HarnessCatalog
from .config import Settings, load_settings
from .domain import Attachment, Harness, InteractionResponse, ProjectFileRename, ThreadCreate, TurnCreate, TurnInputCreate
from .model_discovery import ModelDiscovery
from .objects import object_store
from .pool import SandboxPool
from .runtime import RuntimeClient
from .sandbox import FakeSandboxBackend, OpenSandboxBackend
from .sessions import COOKIE, Sessions
from .store import Store

logger = logging.getLogger(__name__)


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
    runtime = RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds)
    catalog = HarnessCatalog(settings.harness_catalog_json or None)
    discovery = ModelDiscovery(settings, catalog)
    if settings.model_catalog_discovery:
        discovery.load_snapshot()
    return store, objects, pool, catalog, discovery, Broker(settings, store, objects, pool, runtime, catalog)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    store, objects, pool, catalog, discovery, broker = components(settings)
    sessions = Sessions(settings.session_signing_key)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.initialize()
        await broker.start()
        pool_task = None
        catalog_task = None
        if settings.model_catalog_discovery:
            await discovery.refresh()

            async def refresh_catalog():
                while True:
                    await asyncio.sleep(settings.model_catalog_refresh_seconds)
                    await discovery.refresh()

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
        await broker.close()
        await pool.close(preserve_sandbox_ids={sandbox.id for sandbox in broker.active_sandboxes.values()})
        await store.close()

    app = FastAPI(title="Sandbox Harness Platform", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.objects = objects
    app.state.pool = pool
    app.state.broker = broker

    @app.middleware("http")
    async def browser_identity(request: Request, call_next):
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            origin = request.headers.get("origin")
            if origin and urlsplit(origin).netloc != request.headers.get("host"):
                return JSONResponse({"detail": "Cross-site operations are not accepted"}, status_code=403)
        if request.url.path == "/api/health":
            owner_id, token, new_session = "", "", False
        else:
            owner_id, token, new_session = sessions.resolve(request.cookies.get(COOKIE))
        request.state.owner_id = owner_id
        response = await call_next(request)
        if new_session:
            prefix = request.headers.get("x-forwarded-prefix", "").rstrip("/")
            response.set_cookie(COOKIE, token, httponly=True, samesite="lax", secure=request.url.scheme == "https",
                max_age=365 * 86400, path=(prefix or "") + "/")
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
                   "backend": settings.backend, "warm_pool": pool_status, "broker": broker.status()}
        return JSONResponse(payload, status_code=200 if payload["status"] == "ok" else 503)

    @app.get("/api/catalog/harnesses")
    async def harness_catalog():
        return catalog.public()

    @app.get("/api/session")
    async def session_ready():
        return {"ready": True}

    @app.post("/api/threads")
    async def create_thread(value: ThreadCreate, request: Request):
        try:
            resolved = catalog.resolve(value.harness, value.model, value.reasoning_level)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        return await store.create_thread(value, resolved, request.state.owner_id)

    @app.get("/api/threads")
    async def list_threads(request: Request):
        return await store.list_thread_summaries(request.state.owner_id)

    @app.get("/api/threads/{thread_id}")
    async def get_thread(thread_id: str, request: Request):
        value = await store.get_thread(thread_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Thread not found")
        return value

    @app.post("/api/threads/{thread_id}/turns")
    async def create_turn(thread_id: str, value: TurnCreate, request: Request):
        if not await store.get_thread(thread_id, request.state.owner_id):
            raise HTTPException(404, "Thread not found")
        if not await store.attachments_owned(request.state.owner_id, value.attachments):
            raise HTTPException(404, "Attachment not found")
        try:
            return await broker.submit(thread_id, value)
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
        data = await request.body()
        if len(data) > settings.max_attachment_bytes:
            raise HTTPException(413, "Project asset exceeds byte limit")
        object_ref, digest = objects.put(data)
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
            if created:
                await broker.try_deliver_input(result)
            return result
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
        async def stream():
            cursor = after
            while not await request.is_disconnected():
                rows = await store.events_after(turn_id, cursor)
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
    async def event_history(turn_id: str, request: Request, after: int = 0):
        if not await store.get_turn_for_owner(turn_id, request.state.owner_id):
            raise HTTPException(404, "Turn not found")
        return await store.events_after(turn_id, after)

    @app.post("/api/uploads")
    async def upload(request: Request, name: str, mime: str = "application/octet-stream"):
        if not name or len(name) > 255 or Path(name).name != name:
            raise HTTPException(400, "Invalid filename")
        data = await request.body()
        if len(data) > settings.max_attachment_bytes:
            raise HTTPException(413, "Attachment too large")
        object_ref, digest = objects.put(data)
        attachment = Attachment(name=name, object_ref=object_ref, sha256=digest, size=len(data), mime=mime)
        await store.record_upload(request.state.owner_id, attachment)
        return attachment

    @app.get("/api/artifacts/{artifact_id}")
    async def artifact(artifact_id: str, request: Request):
        value = await store.get_artifact_for_owner(artifact_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Artifact not found")
        data = objects.get(value.object_ref, maximum=settings.max_artifact_bytes)
        return Response(data, media_type=value.mime, headers={"Content-Disposition": f'attachment; filename="{Path(value.path).name}"'})

    @app.get("/api/project-files/{file_id}")
    async def project_file(file_id: str, request: Request):
        value = await store.get_project_file_for_owner(file_id, request.state.owner_id)
        if not value:
            raise HTTPException(404, "Project file not found")
        maximum = settings.max_attachment_bytes if value.source_type == "upload" else settings.max_artifact_bytes
        data = objects.get(value.object_ref, maximum=maximum)
        return Response(data, media_type=value.mime,
            headers={"Content-Disposition": f'attachment; filename="{Path(value.path).name}"'})

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
