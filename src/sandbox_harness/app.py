from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from contextlib import suppress
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .broker import Broker
from .catalog import HarnessCatalog
from .config import Settings, load_settings
from .domain import Attachment, Harness, ThreadCreate, TurnCreate
from .model_discovery import ModelDiscovery
from .objects import object_store
from .pool import SandboxPool
from .runtime import RuntimeClient
from .sandbox import FakeSandboxBackend, OpenSandboxBackend
from .store import Store

logger = logging.getLogger(__name__)


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
    pool = SandboxPool(backend, images, targets)
    runtime = RuntimeClient(timeout_seconds=settings.sandbox_timeout_seconds)
    catalog = HarnessCatalog(settings.harness_catalog_json or None)
    discovery = ModelDiscovery(settings, catalog)
    if settings.model_catalog_discovery:
        discovery.load_snapshot()
    return store, objects, pool, catalog, discovery, Broker(settings, store, objects, pool, runtime, catalog)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    store, objects, pool, catalog, discovery, broker = components(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.initialize()
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
        await pool.close()
        await store.close()

    app = FastAPI(title="Sandbox Harness Platform", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.objects = objects
    app.state.pool = pool
    app.state.broker = broker

    @app.get("/api/health")
    async def health():
        pool_status = app.state.pool_status
        payload = {"status": "ok" if pool_status in {"ready", "disabled"} else "degraded",
                   "backend": settings.backend, "warm_pool": pool_status}
        return JSONResponse(payload, status_code=200 if payload["status"] == "ok" else 503)

    @app.get("/api/catalog/harnesses")
    async def harness_catalog():
        return catalog.public()

    @app.post("/api/threads")
    async def create_thread(value: ThreadCreate):
        try:
            resolved = catalog.resolve(value.harness, value.model, value.reasoning_level)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        return await store.create_thread(value, resolved)

    @app.get("/api/threads")
    async def list_threads():
        return await store.list_threads()

    @app.get("/api/threads/{thread_id}")
    async def get_thread(thread_id: str):
        value = await store.get_thread(thread_id)
        if not value:
            raise HTTPException(404, "Thread not found")
        return value

    @app.post("/api/threads/{thread_id}/turns")
    async def create_turn(thread_id: str, value: TurnCreate):
        try:
            return await broker.submit(thread_id, value)
        except KeyError:
            raise HTTPException(404, "Thread not found")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/api/threads/{thread_id}/turns")
    async def list_turns(thread_id: str):
        if not await store.get_thread(thread_id):
            raise HTTPException(404, "Thread not found")
        return await store.list_turns(thread_id)

    @app.get("/api/threads/{thread_id}/items")
    async def list_items(thread_id: str, after: int = 0, limit: int = 200):
        if not await store.get_thread(thread_id):
            raise HTTPException(404, "Thread not found")
        return await store.items_after(thread_id, after, min(max(limit, 1), 500))

    @app.get("/api/turns/{turn_id}")
    async def get_turn(turn_id: str):
        value = await store.get_turn(turn_id)
        if not value:
            raise HTTPException(404, "Turn not found")
        return {**value.model_dump(mode="json"), "artifacts": [item.model_dump(mode="json") for item in await store.artifacts(turn_id)]}

    @app.post("/api/turns/{turn_id}/cancel")
    async def cancel_turn(turn_id: str):
        try:
            await broker.cancel(turn_id)
        except KeyError:
            raise HTTPException(404, "Turn not found")
        return {"accepted": True}

    @app.get("/api/turns/{turn_id}/events")
    async def events(turn_id: str, request: Request, after: int = 0):
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
    async def event_history(turn_id: str, after: int = 0):
        if not await store.get_turn(turn_id):
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
        return Attachment(name=name, object_ref=object_ref, sha256=digest, size=len(data), mime=mime)

    @app.get("/api/artifacts/{artifact_id}")
    async def artifact(artifact_id: str):
        value = await store.get_artifact(artifact_id)
        if not value:
            raise HTTPException(404, "Artifact not found")
        data = objects.get(value.object_ref, maximum=settings.max_artifact_bytes)
        return Response(data, media_type=value.mime, headers={"Content-Disposition": f'attachment; filename="{Path(value.path).name}"'})

    web_root = Path(__file__).resolve().parents[2] / "web"
    static = web_root / "dist" if (web_root / "dist" / "index.html").is_file() else web_root
    if (static / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return (static / "index.html").read_text()

    return app
