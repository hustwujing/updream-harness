from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
import os
import time
import json
import logging
import re

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis
import uvicorn

from .observability import configure, RequestLoggingMiddleware, event, enrich, add_link, telemetry, operation, carrier
from .capability import CapabilityCodec
from .config import Settings, load_settings, validate_service_settings
from .domain import Harness
from .model_routes import ModelRoutes
from .model_activity import ModelActivity
from .store import Store
from .public_proxy import DestinationPolicy, start_proxy


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
    public_policy = DestinationPolicy(settings.public_proxy_allowlist, settings.public_proxy_denylist)
    unrestricted_public_policy = DestinationPolicy([], settings.public_proxy_denylist)
    codec = CapabilityCodec(settings.gateway_signing_key)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    active: dict[str, set[httpx.Response]] = defaultdict(set)
    public_connections: dict[str, set[asyncio.Task]] = defaultdict(set)
    timing_finalizers: set[asyncio.Task] = set()
    clients: dict[str, httpx.AsyncClient] = {}
    model_routes = ModelRoutes(settings)
    activity_store = Store(settings.database_url, settings.redis_url, settings.redis_prefix)
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
        await activity_store.initialize()

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

        proxy = await start_proxy(settings.gateway_bind_host, settings.gateway_proxy_port, authorize_public,
            connections=public_connections, policy=public_policy,
            policy_selector=lambda claims: unrestricted_public_policy
                if claims.network_access == "public" else public_policy)
        try:
            yield
        finally:
            proxy.close()
            await proxy.wait_closed()
            await redis.aclose()
            if timing_finalizers:
                await asyncio.gather(*timing_finalizers, return_exceptions=True)
            await activity_store.close()
            await asyncio.gather(*(client.aclose() for client in clients.values()))

    app = FastAPI(title="Sandbox Capability Gateway", lifespan=lifespan)
    app.add_middleware(RequestLoggingMiddleware,service="gateway")

    @app.get("/health")
    async def health():
        return {"status": "ok","observability":telemetry().status() if telemetry() else {"enabled":False},
            "public_proxy_policy":{"restricted_mode":"allowlist" if public_policy.allowlist else "public",
                "frpc_mode":"public", "allowlist_rules":len(public_policy.allowlist),
                "denylist_rules":len(public_policy.denylist)}}

    @app.post("/internal/revoke/{nonce}")
    async def revoke(nonce: str, request: Request):
        if request.headers.get("x-sandbox-harness-control") != settings.gateway_control_key:
            raise HTTPException(403, "Invalid control credential")
        fields={}
        for key in ("turn_id","thread_id","sandbox_id"):
            value=request.headers.get("x-updream-"+key.replace("_","-"),"")
            if re.fullmatch(r"[0-9a-f]{32}|[0-9a-f-]{36}",value):fields[key]=value
        if not hasattr(request.state,"log_context"):request.state.log_context={}
        request.state.log_context.update(fields);enrich(**fields)
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
        event("capability.revoked",count=len(tunnels)+len(responses))
        return {"revoked": True}

    @app.post("/v1/telemetry/{signal}")
    async def ingest_telemetry(signal: str, request: Request):
        from .telemetry_ingest import decode_otlp
        try:claims=codec.verify(token_from(request),route="/v1/telemetry")
        except ValueError:raise HTTPException(401,"Invalid telemetry capability")
        if await redis.exists(settings.redis_prefix+"capability:revoked:"+claims.nonce):
            raise HTTPException(401,"Telemetry capability revoked")
        key=settings.redis_prefix+"telemetry:rate:"+claims.nonce+":"+str(int(time.time())//60)
        count=await redis.eval("local n=redis.call('INCR',KEYS[1]);redis.call('EXPIRE',KEYS[1],120);return n",1,key)
        if count>600:raise HTTPException(429,"Telemetry rate exceeded")
        body=bytearray()
        async for part in request.stream():
            body.extend(part)
            if len(body)>4*1024*1024:raise HTTPException(413,"Telemetry batch too large")
        if request.headers.get("content-encoding") not in (None,"identity"):
            raise HTTPException(415,"Telemetry compression is not supported")
        try:rows=decode_otlp(signal,bytes(body),claims)
        except Exception:raise HTTPException(422,"Invalid telemetry batch")
        target=telemetry()
        if target is None:raise HTTPException(503,"Telemetry receiver unavailable")
        rejected=sum(not target.put(kind,row) for kind,row in rows)
        from fastapi.responses import Response
        if signal=="logs":
            from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceResponse
            result=ExportLogsServiceResponse()
            if rejected:result.partial_success.rejected_log_records=rejected
        else:
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse
            result=ExportTraceServiceResponse()
            if rejected:result.partial_success.rejected_spans=rejected
        if rejected:result.partial_success.error_message="Telemetry queue capacity exceeded"
        return Response(content=result.SerializeToString(),status_code=503 if rows and rejected==len(rows) else 200,media_type="application/x-protobuf")

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request):
        route = "/" + path
        token = token_from(request)
        try:
            claims = codec.verify(token, route=route)
        except ValueError as exc:
            raise HTTPException(401, str(exc))
        if not hasattr(request.state,"log_context"):request.state.log_context={}
        request.state.log_context.update({"turn_id":claims.turn_id,"thread_id":claims.thread_id,"sandbox_id":claims.sandbox_id,"model":claims.model,"harness":claims.harness})
        enrich(**request.state.log_context);add_link(claims.traceparent)
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
        observe_model = prefix.startswith("/v1/models/") and suffix.rstrip("/") in {
            "/responses", "/messages", "/chat/completions"}
        activity = ModelActivity(activity_store, claims) if observe_model else None
        if activity:
            await activity.start()
        upstream.headers.update(carrier(getattr(activity,"span",None)))
        started = time.monotonic()
        try:
            response = await send_with_start_timeout(client, upstream, settings.gateway_model_start_timeout_seconds)
        except BaseException as exc:
            if activity:
                await activity.finish(error_code=type(exc).__name__)
            raise
        if activity:
            activity.header_ms = round((time.monotonic() - started) * 1000)
        event("upstream.response.headers",level=logging.WARNING if response.status_code>=400 else logging.INFO,
            turn_id=claims.turn_id,model=claims.model,header_ms=round((time.monotonic()-started)*1000,3),
            **{"http.response.status_code":response.status_code},request_bytes=len(body),
            upstream_request_id=response.headers.get("x-request-id") or response.headers.get("request-id", ""))
        if (int(time.time()) >= claims.expires_at or
                await redis.exists(settings.redis_prefix + "capability:revoked:" + claims.nonce)):
            await response.aclose()
            if activity:
                await activity.finish(error_code="capability_revoked")
            raise HTTPException(401, "Capability token expired or revoked")
        active[claims.nonce].add(response)

        async def content():
            stream_error = ""
            try:
                async with asyncio.timeout(max(0, claims.expires_at - time.time())):
                    async for chunk in bytes_with_idle_timeout(response, settings.gateway_model_idle_timeout_seconds):
                        if activity:
                            await activity.observe(chunk)
                            if activity.probe.completed or activity.probe.failed:
                                # Commit before forwarding the terminal frame: native
                                # clients may close the HTTP stream immediately after it.
                                await activity.finish(response.status_code)
                        yield chunk
            except BaseException as exc:
                stream_error = "" if activity and activity.probe.completed and isinstance(exc, asyncio.CancelledError) else type(exc).__name__
                raise
            finally:
                if activity and response.status_code == 200 and "text/event-stream" in response.headers.get("content-type", "") and not activity.probe.completed:
                    stream_error = stream_error or "stream_incomplete"
                async def finalize():
                    try:
                        await response.aclose()
                    finally:
                        active[claims.nonce].discard(response)
                        if not active[claims.nonce]:
                            active.pop(claims.nonce, None)
                        if activity:
                            await activity.finish(response.status_code, stream_error)
                # ASGI disconnect cancellation must not cancel the durable receipt.
                finalizer = asyncio.create_task(finalize())
                timing_finalizers.add(finalizer)
                finalizer.add_done_callback(timing_finalizers.discard)
                await asyncio.shield(finalizer)

        response_headers = {key: value for key, value in response.headers.items() if key.lower() not in HOP_HEADERS | {"content-encoding"}}
        return StreamingResponse(content(), status_code=response.status_code, headers=response_headers, media_type=response.headers.get("content-type"))

    return app


def run() -> None:
    settings = load_settings()
    configure("gateway", settings.data_root / "logs")
    uvicorn.run(create_gateway(settings), host=settings.gateway_bind_host, port=settings.gateway_bind_port, log_config=None, access_log=False)


if __name__ == "__main__":
    run()
