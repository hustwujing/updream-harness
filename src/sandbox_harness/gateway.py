from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
import os
import time
import json
import logging

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis
import uvicorn

from .capability import CapabilityCodec
from .config import Settings, load_settings, validate_service_settings
from .domain import Harness
from .model_routes import ModelRoutes
from .public_proxy import start_proxy


logger = logging.getLogger(__name__)


HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length"}

CONSUME_CAPABILITY = """
if redis.call('EXISTS', KEYS[1]) == 1 then return -1 end
local count = redis.call('INCR', KEYS[2])
if count == 1 then redis.call('EXPIRE', KEYS[2], ARGV[1]) end
return count
"""


def token_from(request: Request) -> str:
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return request.headers.get("x-api-key") or request.headers.get("anthropic-api-key") or ""


def upstream_suffix(route: str, prefix: str) -> str:
    suffix = route[len(prefix):] or "/"
    if suffix == "/v1" or suffix.startswith("/v1/"):
        suffix = suffix[3:] or "/"
    return suffix


async def send_with_start_timeout(client, request, timeout_seconds: float):
    try:
        return await asyncio.wait_for(client.send(request, stream=True), timeout=timeout_seconds)
    except TimeoutError as exc:
        raise HTTPException(504, "Model upstream did not start responding before the timeout") from exc


async def bytes_with_idle_timeout(response, timeout_seconds: float):
    iterator = response.aiter_bytes().__aiter__()
    while True:
        try:
            yield await asyncio.wait_for(iterator.__anext__(), timeout=timeout_seconds)
        except StopAsyncIteration:
            return
        except TimeoutError:
            return


def create_gateway(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    validate_service_settings(settings)
    codec = CapabilityCodec(settings.gateway_signing_key)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    active: dict[str, set[httpx.Response]] = defaultdict(set)
    public_connections: dict[str, set[asyncio.Task]] = defaultdict(set)
    clients: dict[str, httpx.AsyncClient] = {}
    model_routes = ModelRoutes(settings)
    upstreams = {
        "/v1/models/codex": (settings.upstream_codex, os.environ.get("SANDBOX_HARNESS_UPSTREAM_CODEX_KEY", "")),
        "/v1/models/claude": (settings.upstream_claude, os.environ.get("SANDBOX_HARNESS_UPSTREAM_CLAUDE_KEY", "")),
        "/v1/models/deepseek": (settings.upstream_deepseek, os.environ.get("SANDBOX_HARNESS_UPSTREAM_DEEPSEEK_KEY", "")),
        "/v1/providers/image": (settings.upstream_image, os.environ.get("SANDBOX_HARNESS_UPSTREAM_IMAGE_KEY", "")),
        "/v1/providers/video": (settings.upstream_video, os.environ.get("SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY", "")),
        "/v1/providers/audio": (settings.upstream_audio, os.environ.get("SANDBOX_HARNESS_UPSTREAM_AUDIO_KEY", "")),
    }

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await redis.ping()

        async def authorize_public(token: str):
            try:
                claims = codec.verify(token, route="/v1/public-http")
            except ValueError as exc:
                raise ValueError("Invalid proxy capability") from exc
            remaining = max(1, claims.expires_at - int(time.time()))
            count = await redis.eval(
                CONSUME_CAPABILITY,
                2,
                settings.redis_prefix + "capability:revoked:" + claims.nonce,
                settings.redis_prefix + "capability:public-usage:" + claims.nonce,
                remaining,
            )
            if count == -1 or count > claims.max_public_requests:
                raise ValueError("Proxy capability is revoked or exhausted")
            return claims

        proxy = await start_proxy(settings.gateway_bind_host, settings.gateway_proxy_port, authorize_public, connections=public_connections)
        try:
            yield
        finally:
            proxy.close()
            await proxy.wait_closed()
            await redis.aclose()
            await asyncio.gather(*(client.aclose() for client in clients.values()))

    app = FastAPI(title="Sandbox Capability Gateway", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/internal/revoke/{nonce}")
    async def revoke(nonce: str, request: Request):
        if request.headers.get("x-sandbox-harness-control") != settings.gateway_control_key:
            raise HTTPException(403, "Invalid control credential")
        await redis.set(
            settings.redis_prefix + "capability:revoked:" + nonce,
            "1",
            ex=settings.capability_ttl_seconds,
        )
        tunnels = list(public_connections.pop(nonce, set()))
        for task in tunnels:
            task.cancel()
        if tunnels:
            await asyncio.gather(*tunnels, return_exceptions=True)
        responses = list(active.pop(nonce, set()))
        if responses:
            await asyncio.gather(*(response.aclose() for response in responses), return_exceptions=True)
        return {"revoked": True}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request):
        route = "/" + path
        token = token_from(request)
        try:
            claims = codec.verify(token, route=route)
        except ValueError as exc:
            raise HTTPException(401, str(exc))
        remaining = max(1, claims.expires_at - int(time.time()))
        count = await redis.eval(
            CONSUME_CAPABILITY,
            2,
            settings.redis_prefix + "capability:revoked:" + claims.nonce,
            settings.redis_prefix + "capability:usage:" + claims.nonce,
            remaining,
        )
        if count == -1:
            raise HTTPException(401, "Capability token revoked")
        if count > claims.max_requests:
            raise HTTPException(429, "Capability request limit reached")
        prefix = next((candidate for candidate in sorted(upstreams, key=len, reverse=True) if route == candidate or route.startswith(candidate + "/")), None)
        if not prefix:
            raise HTTPException(404, "Gateway route is not configured")
        base_url, credential = upstreams[prefix]
        if prefix.startswith("/v1/models/"):
            harness = Harness(prefix.rsplit("/", 1)[-1])
            selected_route = model_routes.resolve(harness, claims.model)
            base_url, credential = selected_route.base_url, selected_route.credential
        if not base_url:
            raise HTTPException(503, "Gateway upstream is not configured")
        client_key = prefix + "\0" + base_url
        client = clients.get(client_key)
        if client is None:
            client = httpx.AsyncClient(base_url=base_url.rstrip("/"),
                timeout=httpx.Timeout(None, connect=20), trust_env=False)
            clients[client_key] = client
        suffix = upstream_suffix(route, prefix)
        headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS | {"authorization", "x-api-key", "anthropic-api-key"}}
        if credential:
            headers["authorization"] = "Bearer " + credential
            if prefix.endswith(("claude", "deepseek")):
                headers["x-api-key"] = credential
        body = await request.body()
        if prefix.startswith("/v1/models/"):
            try:
                payload = json.loads(body)
            except (TypeError, ValueError):
                raise HTTPException(422, "Model request must be JSON")
            if not isinstance(payload, dict):
                raise HTTPException(422, "Model request must be an object")
            payload["model"] = claims.model
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        upstream = client.build_request(request.method, suffix, params=request.query_params, headers=headers, content=body)
        started = time.monotonic()
        response = await send_with_start_timeout(client, upstream, settings.gateway_model_start_timeout_seconds)
        log = logger.warning if response.status_code >= 400 else logger.info
        log("Upstream response turn=%s model=%s status=%s header_seconds=%.3f request_bytes=%s request_id=%s",
            claims.turn_id, claims.model, response.status_code, time.monotonic() - started,
            len(body), response.headers.get("x-request-id") or response.headers.get("request-id", ""))
        if (int(time.time()) >= claims.expires_at or
                await redis.exists(settings.redis_prefix + "capability:revoked:" + claims.nonce)):
            await response.aclose()
            raise HTTPException(401, "Capability token expired or revoked")
        active[claims.nonce].add(response)

        async def content():
            try:
                async with asyncio.timeout(max(0, claims.expires_at - time.time())):
                    async for chunk in bytes_with_idle_timeout(response, settings.gateway_model_idle_timeout_seconds):
                        yield chunk
            finally:
                active[claims.nonce].discard(response)
                if not active[claims.nonce]:
                    active.pop(claims.nonce, None)
                await response.aclose()

        response_headers = {key: value for key, value in response.headers.items() if key.lower() not in HOP_HEADERS | {"content-encoding"}}
        return StreamingResponse(content(), status_code=response.status_code, headers=response_headers, media_type=response.headers.get("content-type"))

    return app


def run() -> None:
    settings = load_settings()
    uvicorn.run(create_gateway(settings), host=settings.gateway_bind_host, port=settings.gateway_bind_port)


if __name__ == "__main__":
    run()
