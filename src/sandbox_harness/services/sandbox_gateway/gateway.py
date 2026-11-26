from __future__ import annotations

from sandbox_harness.common.configuration.resources import resource_path

import asyncio
import base64
from collections import defaultdict, OrderedDict
from contextlib import asynccontextmanager
from datetime import timedelta
import os
import time
import json
import hashlib
import logging
import math
import re
import struct
from tempfile import NamedTemporaryFile
from pathlib import Path
from urllib.parse import quote
from pydantic import ValidationError

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from redis.asyncio import Redis
import uvicorn

from sandbox_harness.common.observability.observability import (configure, RequestLoggingMiddleware, event, enrich, add_link, telemetry, operation, carrier)
from sandbox_harness.common.observability.http import (observed_async_client)
from sandbox_harness.common.security.capability import (CapabilityCodec)
from sandbox_harness.common.security.grants import (GrantAuthority)
from sandbox_harness.common.artifacts.artifact_gateway import (ArtifactEmission, ArtifactGateway)
from sandbox_harness.common.contracts.job_contracts import (OutputDisposition)
from sandbox_harness.common.catalog.job_catalog import (JobCatalog)
from sandbox_harness.common.jobs.job_coordinator import (JobCoordinator, JobGroupDraftUpdate)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate)
from sandbox_harness.common.catalog.profiles import (ProfileResolver)
from sandbox_harness.common.sandbox.sandbox_factory import (release_provider)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.catalog.execution_readiness import (ExecutionReadiness)
from sandbox_harness.common.jobs.review import (ReviewSubmission, ReviewerCoordinator)
from sandbox_harness.common.configuration.config import (Settings, load_settings, validate_service_settings)
from sandbox_harness.common.storage.directories import (ensure_private_directory)
from sandbox_harness.common.configuration.databases import (DatabaseSettings)
from sandbox_harness.common.contracts.domain import (Harness, MediaDraftCreate, now)
from sandbox_harness.common.models.model_routes import (ModelRoutes)
from sandbox_harness.common.models.model_activity import (ModelActivity)
from sandbox_harness.common.catalog.media_catalog import (MediaCatalog)
from sandbox_harness.common.catalog.media_references import (resolve_media_references)
from sandbox_harness.common.catalog.provider_preflight import (ProviderPreflight)
from sandbox_harness.common.storage.objects import (object_store)
from sandbox_harness.common.sites.site_bundle import (MAX_SITE_ARCHIVE_BYTES, normalize_site_slug, stage_site_archive)
from sandbox_harness.common.db.core import (CoreRepository)
from sandbox_harness.common.db.gateway_repositories import (GatewayRepositories)
from sandbox_harness.common.db.media_repository import (MediaRepository)
from sandbox_harness.common.db.site_repository import (SiteRepository)
from sandbox_harness.services.sandbox_gateway.public_proxy import (DestinationPolicy, start_proxy)


logger = logging.getLogger(__name__)


HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length"}


def fallback_rate_count(state: OrderedDict, nonce: str, window: int, capacity: int) -> int:
    for key,(stored_window,_) in list(state.items()):
        if stored_window!=window:state.pop(key,None)
    previous=state.pop(nonce,None)
    if previous is None and len(state)>=capacity:state.popitem(last=False)
    count=(previous[1]+1) if previous and previous[0]==window else 1
    state[nonce]=(window,count)
    return count


def validation_detail(error: ValidationError) -> dict:
    return {"code": "invalid_media_draft", "errors": [{
        "path": ".".join(str(item) for item in value.get("loc", ())),
        "message": value.get("msg", "Invalid value"),
    } for value in error.errors(include_url=False, include_context=False, include_input=False)[:12]]}

class ModelUpstreamIdleTimeout(TimeoutError):
    pass


async def bounded_request_body(request: Request, maximum: int, idle_timeout: float,
                               total_timeout: float) -> bytes:
    raw_length = request.headers.get("content-length")
    if raw_length is not None:
        try:length=int(raw_length)
        except ValueError as exc:raise HTTPException(400,"request_content_length_invalid") from exc
        if length < 0:raise HTTPException(400,"request_content_length_invalid")
        if length > maximum:raise HTTPException(413,"request_too_large")
    body=bytearray();iterator=request.stream().__aiter__()
    try:
        async with asyncio.timeout(total_timeout):
            while True:
                try:chunk=await asyncio.wait_for(iterator.__anext__(),timeout=idle_timeout)
                except StopAsyncIteration:break
                if len(body)+len(chunk)>maximum:raise HTTPException(413,"request_too_large")
                body.extend(chunk)
    except TimeoutError as exc:
        raise HTTPException(408,"request_body_timeout") from exc
    if raw_length is not None and len(body)!=length:raise HTTPException(400,"request_size_mismatch")
    request._body=bytes(body)
    return request._body


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


MODEL_ENDPOINTS={
    Harness.CODEX:{"/responses":"generation","/responses/compact":"compaction"},
    Harness.CLAUDE:{"/messages":"generation","/messages/count_tokens":"token_count"},
    Harness.DEEPSEEK:{"/messages":"generation","/messages/count_tokens":"token_count"},
}


def model_endpoint_kind(harness:Harness|str,suffix:str)->str|None:
    try:value=harness if isinstance(harness,Harness) else Harness(str(harness))
    except ValueError:return None
    normalized=suffix.rstrip("/") or "/"
    return MODEL_ENDPOINTS[value].get(normalized)


ADAPTIVE_ONLY_CLAUDE_MODELS={
    "claude-fable-5","claude-fable-5-1","claude-haiku-5-5","claude-opus-4-7",
    "claude-opus-4-8","claude-opus-5","claude-opus-5-5","claude-sonnet-5-5",
}


def normalize_model_request(payload:dict,*,harness:Harness|str,model:str,suffix:str)->dict:
    value=dict(payload)
    harness_id=harness.value if isinstance(harness,Harness) else str(harness)
    model_id=model.rsplit("/",1)[-1]
    thinking=value.get("thinking")
    if (harness_id!="claude" or suffix.rstrip("/")!="/messages"
            or model_id not in ADAPTIVE_ONLY_CLAUDE_MODELS
            or not isinstance(thinking,dict) or thinking.get("type")=="adaptive"):
        return value
    budget=thinking.get("budget_tokens")
    effort=("high" if not isinstance(budget,int) else "low" if budget<=4096
        else "medium" if budget<=16384 else "high" if budget<=65536 else "max")
    output_config=dict(value.get("output_config") or {})
    output_config.setdefault("effort",effort)
    value["thinking"]={"type":"adaptive"}
    value["output_config"]=output_config
    return value


def _image_dimensions(data:bytes,mime:str)->tuple[int,int] | None:
    if mime=='image/png' and len(data)>=24 and data.startswith(b'\x89PNG\r\n\x1a\n'):
        return struct.unpack('>II',data[16:24])
    if mime=='image/gif' and len(data)>=10 and data[:6] in {b'GIF87a',b'GIF89a'}:
        return struct.unpack('<HH',data[6:10])
    if mime=='image/webp' and len(data)>=30 and data[:4]==b'RIFF' and data[8:12]==b'WEBP':
        if data[12:16]==b'VP8X':
            return (1+int.from_bytes(data[24:27],'little'),
                1+int.from_bytes(data[27:30],'little'))
    if mime in {'image/jpeg','image/jpg'} and data.startswith(b'\xff\xd8'):
        offset=2
        while offset+9<=len(data):
            if data[offset]!=0xff:offset+=1;continue
            marker=data[offset+1];offset+=2
            if marker in {0xd8,0xd9} or 0xd0<=marker<=0xd7:continue
            if offset+2>len(data):break
            length=int.from_bytes(data[offset:offset+2],'big')
            if length<2 or offset+length>len(data):break
            if marker in {0xc0,0xc1,0xc2,0xc3,0xc5,0xc6,0xc7,0xc9,0xca,0xcb,0xcd,0xce,0xcf} and length>=7:
                return (int.from_bytes(data[offset+5:offset+7],'big'),
                    int.from_bytes(data[offset+3:offset+5],'big'))
            offset+=length
    return None


def _visual_token_estimate(data:bytes,mime:str)->int:
    dimensions=_image_dimensions(data,mime)
    if not dimensions:return 16_384
    width,height=dimensions
    if width<1 or height<1 or width>100_000 or height>100_000:return 16_384
    tiles=math.ceil(min(width,8192)/512)*math.ceil(min(height,8192)/512)
    return min(65_536,max(2_048,tiles*1_024))


def estimate_model_input_tokens(payload:dict,raw_size:int)->int:
    """Conservative transport-independent estimate for model input preflight."""
    visited=0
    def text_tokens(value:str)->int:return max(1,math.ceil(len(value.encode())/3))
    def walk(value,key:str='',depth:int=0)->int:
        nonlocal visited
        visited+=1
        if depth>64 or visited>100_000:raise ValueError('model_input_structure_too_complex')
        if value is None:return 1
        if isinstance(value,bool):return 1
        if isinstance(value,(int,float)):return text_tokens(str(value))
        if isinstance(value,str):
            if value.startswith('data:'):
                header,separator,encoded=value.partition(',')
                if not separator or ';base64' not in header:raise ValueError('model_data_uri_invalid')
                mime=header[5:].split(';',1)[0].lower()
                try:data=base64.b64decode(encoded,validate=True)
                except Exception as exc:raise ValueError('model_data_uri_invalid') from exc
                return (_visual_token_estimate(data,mime) if mime.startswith('image/')
                    else max(1,math.ceil(len(data)/3)))
            if key in {'image_url','url'} and value.startswith(('http://','https://','file://')):
                return 8_192
            return text_tokens(value)
        if isinstance(value,list):return 2+sum(walk(item,depth=depth+1) for item in value)
        if isinstance(value,dict):
            media_type=str(value.get('media_type') or '').lower()
            if (value.get('type')=='base64' and media_type.startswith('image/')
                    and isinstance(value.get('data'),str)):
                try:data=base64.b64decode(value['data'],validate=True)
                except Exception as exc:raise ValueError('model_data_uri_invalid') from exc
                return _visual_token_estimate(data,media_type)+8
            return 2+sum(text_tokens(str(name))+walk(item,str(name),depth+1)
                for name,item in value.items())
        raise ValueError('model_input_value_invalid')
    try:estimate=walk(payload)+512
    except (ValueError,TypeError,OverflowError):return max(1,raw_size)
    return max(1,min(max(1,raw_size),estimate))


def enforce_model_output_budget(payload: dict, suffix: str, remaining: int,
                                input_token_ceiling: int = 0) -> int:
    field = "max_output_tokens" if suffix.rstrip("/") == "/responses" else "max_tokens"
    # Cache outcome is known only after Provider usage is returned. Reserve
    # output here; weighted input is settled after completion.
    available=remaining
    if available<1:raise HTTPException(429,"model_token_budget_exhausted")
    value = payload.get(field)
    if value is None:
        value = min(8192, available)
        payload[field] = value
    if not isinstance(value,int) or isinstance(value,bool) or value < 1:
        raise HTTPException(422,"model_output_token_limit_invalid")
    if value > available:
        raise HTTPException(429,"model_token_budget_exhausted")
    return value


def model_token_reservation(payload:dict,suffix:str,kind:str,remaining:int,
                            input_token_ceiling:int)->int:
    if kind=="token_count":return 0
    if kind=="generation":
        return enforce_model_output_budget(payload,suffix,remaining,input_token_ceiling)
    available=remaining-max(0,input_token_ceiling)
    if available<1:raise HTTPException(429,"model_token_budget_exhausted")
    # Standalone compaction does not accept a generation output limit. The
    # ledger still serializes the request and settles Provider-reported usage.
    return min(8192,available)


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
        except TimeoutError as exc:
            raise ModelUpstreamIdleTimeout("model_upstream_idle_timeout") from exc


def gateway_repositories(settings):
    try:
        databases=DatabaseSettings().for_service("sandbox-gateway")
    except ValueError:
        if settings.backend!="fake":raise
        databases={"core":settings.database_url,
            "media":"sqlite+aiosqlite:///"+str(settings.data_root/"gateway-media.db"),
            "sites":"sqlite+aiosqlite:///"+str(settings.data_root/"gateway-sites.db")}
    return GatewayRepositories(
        CoreRepository(databases["core"],settings.redis_url,settings.redis_prefix,
            namespace=settings.namespace),
        MediaRepository(databases["media"]),SiteRepository(databases["sites"]))


def create_gateway(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    validate_service_settings(settings)
    public_policy = DestinationPolicy(settings.public_proxy_allowlist, settings.public_proxy_denylist)
    unrestricted_public_policy = DestinationPolicy([], settings.public_proxy_denylist)
    codec = CapabilityCodec(settings.gateway_signing_key)
    grant_authority = GrantAuthority(settings.gateway_signing_key)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    active: dict[str, set[httpx.Response]] = defaultdict(set)
    public_connections: dict[str, set[asyncio.Task]] = defaultdict(set)
    timing_finalizers: set[asyncio.Task] = set()
    clients: dict[str, httpx.AsyncClient] = {}
    telemetry_fallback: OrderedDict[str, tuple[int,int]] = OrderedDict()
    model_routes = ModelRoutes(settings)
    activity_store = gateway_repositories(settings)
    job_catalog_raw = settings.job_catalog_path.read_text()
    job_catalog = JobCatalog.from_json(job_catalog_raw,
        expected_digest=(settings.job_catalog_digest or hashlib.sha256(job_catalog_raw.encode()).hexdigest()))
    profile_raw=settings.profile_catalog_path.read_text()
    skill_manifest=json.loads(resource_path('runtime/skills/manifest.json').read_text())
    profile_resolver=ProfileResolver.from_json(profile_raw,
        expected_digest=(settings.profile_catalog_digest or hashlib.sha256(profile_raw.encode()).hexdigest()),
        skill_manifest=skill_manifest,
        provider=release_provider(settings.backend))
    objects = object_store(settings)
    artifact_gateway = ArtifactGateway(activity_store, objects,
        settings.data_root / "tmp" / "job-artifacts", maximum_bytes=settings.max_artifact_bytes)
    media_catalog = MediaCatalog(settings.media_catalog_json,
        ark_assets_configured=bool(settings.ark_access_key and settings.ark_secret_key
            and settings.ark_asset_group_id and settings.ark_project_name),
        tripo_configured=bool(settings.tripo_api_key))
    readiness=ExecutionReadiness.from_settings(settings,job_catalog)
    provider_preflight=ProviderPreflight(activity_store,objects,media_catalog)
    job_coordinator = JobCoordinator(activity_store, job_catalog, profile_resolver=profile_resolver,
        draft_ttl_seconds=settings.media_draft_ttl_seconds,readiness=readiness,
        provider_preflight=provider_preflight)
    execution_catalog=HarnessCatalog(job_catalog=job_catalog,profile_resolver=profile_resolver)
    reviewer=ReviewerCoordinator(job_coordinator)
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
        try:
            await asyncio.wait_for(redis.ping(),timeout=1)
            app.state.redis_available=True;app.state.redis_last_error=""
        except Exception as exc:
            app.state.redis_available=False;app.state.redis_last_error=type(exc).__name__
            event("gateway.redis.degraded_start",level=logging.WARNING,
                exc=exc)
        await activity_store.initialize()

        async def authorize_public(token: str):
            try:
                claims = codec.verify(token, route="/v1/public-http")
            except ValueError as exc:
                raise ValueError("Invalid proxy capability") from exc
            try:
                await activity_store.authorize_capability(claims,"public")
            except ValueError as exc:
                raise ValueError(str(exc)) from exc
            except Exception as exc:
                raise ValueError("capability_authority_unavailable") from exc
            return claims

        proxy = await start_proxy(settings.gateway_bind_host, settings.gateway_proxy_port, authorize_public,
            connections=public_connections, policy=public_policy,
            policy_selector=lambda claims: unrestricted_public_policy
                if claims.network_access == "public" else public_policy,
            maximum_body=settings.public_proxy_request_max_bytes,
            connect_timeout=settings.public_proxy_connect_timeout_seconds,
            body_idle_timeout=settings.public_proxy_body_idle_timeout_seconds,
            body_total_timeout=settings.public_proxy_body_total_timeout_seconds)
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
    app.state.store=activity_store
    app.state.redis=redis
    app.state.telemetry_fallback=telemetry_fallback

    @app.get("/health")
    async def health():
        try:
            await asyncio.wait_for(activity_store.healthcheck(),timeout=1)
            database={"available":True,"error":""}
        except Exception as exc:
            database={"available":False,"error":type(exc).__name__}
        try:
            await asyncio.wait_for(redis.ping(),timeout=1)
            app.state.redis_available=True;app.state.redis_last_error="";telemetry_fallback.clear()
        except Exception as exc:
            app.state.redis_available=False;app.state.redis_last_error=type(exc).__name__
        payload={"status": "ok" if database["available"] and app.state.redis_available else "degraded",
            "ready":database["available"],"database":database,
            "redis":{"available":app.state.redis_available,"required_for_readiness":False,
                "error":app.state.redis_last_error},
            "observability":telemetry().status() if telemetry() else {"enabled":False},
            "public_proxy_policy":{"restricted_mode":"allowlist" if public_policy.allowlist else "public",
                "frpc_mode":"public", "allowlist_rules":len(public_policy.allowlist),
                "denylist_rules":len(public_policy.denylist)}}
        return JSONResponse(payload,status_code=200 if database["available"] else 503)

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
        await activity_store.revoke_capability(nonce,
            int(time.time())+settings.capability_ttl_seconds)
        try:
            await redis.set(settings.redis_prefix + "capability:revoked:" + nonce,
                "1",ex=settings.capability_ttl_seconds)
        except Exception as exc:
            event("capability.revocation_cache_failed",level=logging.WARNING,exc=exc)
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
        from sandbox_harness.common.observability.telemetry_ingest import (decode_otlp)
        try:claims=codec.verify(token_from(request),route="/v1/telemetry")
        except ValueError:raise HTTPException(401,"Invalid telemetry capability")
        try:await activity_store.authorize_capability(claims,"check")
        except ValueError:raise HTTPException(401,"Telemetry capability revoked")
        except Exception:raise HTTPException(503,"capability_authority_unavailable")
        key=settings.redis_prefix+"telemetry:rate:"+claims.nonce+":"+str(int(time.time())//60)
        try:
            count=await redis.eval("local n=redis.call('INCR',KEYS[1]);redis.call('EXPIRE',KEYS[1],120);return n",1,key)
            app.state.redis_available=True;app.state.redis_last_error="";telemetry_fallback.clear()
        except Exception as exc:
            app.state.redis_available=False;app.state.redis_last_error=type(exc).__name__
            window=int(time.time())//60
            count=fallback_rate_count(telemetry_fallback,claims.nonce,window,
                settings.gateway_telemetry_fallback_capacity)
            event("telemetry.rate_limit_fallback",level=logging.WARNING,
                exc=exc)
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

    async def authorize_runtime_request(request: Request, route: str, *, streaming_body: bool = False):
        try:
            claims = codec.verify(token_from(request), route=route)
        except ValueError as exc:
            raise HTTPException(401, str(exc))
        if not hasattr(request.state, "log_context"):
            request.state.log_context = {}
        request.state.log_context.update({"turn_id": claims.turn_id, "thread_id": claims.thread_id,
            "sandbox_id": claims.sandbox_id, "model": claims.model, "harness": claims.harness})
        enrich(**request.state.log_context); add_link(claims.traceparent)
        try:
            security = await activity_store.authorize_capability(claims,"request")
        except ValueError as exc:
            code=str(exc)
            if code in {"capability_revoked","capability_expired"}:
                raise HTTPException(401,code)
            if code in {"capability_request_limit","capability_public_request_limit"}:
                raise HTTPException(429,code)
            raise HTTPException(403,code)
        except Exception as exc:
            raise HTTPException(503,"capability_authority_unavailable") from exc
        request.state.capability_security=security
        if (not streaming_body and request.method in {"POST","PUT","PATCH"}
                and not hasattr(request,"_body")):
            await bounded_request_body(request,settings.gateway_request_max_bytes,
                settings.gateway_body_idle_timeout_seconds,
                settings.gateway_body_total_timeout_seconds)
        if claims.scope == "job":
            if route.startswith("/v1/models/"):
                if "model" not in claims.operations:
                    raise HTTPException(403, "Model call is outside Job grant scope")
                return claims
            try:
                def required(name: str) -> str:
                    value = request.headers.get(name, "")
                    if not value:
                        raise ValueError("Missing Job grant header: " + name)
                    return value
                def values(name: str) -> set[str]:
                    raw = request.headers.get(name, "[]")
                    parsed = json.loads(raw)
                    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
                        raise ValueError("Invalid Job grant header: " + name)
                    return set(parsed)
                operation_name = required("x-updream-operation")
                parameter_digest = claims.parameter_digest
                if operation_name == "submit":
                    if streaming_body:
                        raise ValueError("Submit parameter validation cannot use an unbounded streaming body")
                    body = await bounded_request_body(request,settings.gateway_request_max_bytes,
                        settings.gateway_body_idle_timeout_seconds,
                        settings.gateway_body_total_timeout_seconds)
                    if body:
                        try:
                            body_value = json.loads(body)
                        except (ValueError, TypeError):
                            body_value = {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}
                    else:
                        body_value = None
                    parameter_digest = hashlib.sha256(json.dumps({
                        "query": sorted(request.query_params.multi_items()), "body": body_value,
                    }, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
                grant_authority.verify_call(
                    token_from(request), route=route,
                    thread_id=required("x-updream-thread-id"),
                    turn_id=required("x-updream-turn-id"),
                    group_id=required("x-updream-group-id"),
                    job_id=required("x-updream-job-id"),
                    generation=int(required("x-updream-job-generation")),
                    sandbox_id=required("x-updream-source-sandbox"),
                    source_identity=required("x-updream-source-sandbox"),
                    descriptor_id=required("x-updream-descriptor-id"),
                    descriptor_version=required("x-updream-descriptor-version"),
                    descriptor_digest=required("x-updream-descriptor-digest"),
                    operation=operation_name,
                    artifact_ids=values("x-updream-artifact-ids"),
                    skill_ids=values("x-updream-skill-ids"),
                    capability_id=request.headers.get("x-updream-capability-id", ""),
                    operation_id=request.headers.get("x-updream-operation-id", ""),
                    network_origin=request.headers.get("x-updream-network-origin", ""),
                    output_scope=request.headers.get("x-updream-output-scope", ""),
                    child_profile_id=request.headers.get("x-updream-child-profile-id", ""),
                    runtime_profile_id=request.headers.get("x-updream-runtime-profile-id", ""),
                    parameter_digest=parameter_digest,
                )
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                raise HTTPException(403, str(exc)) from exc
        return claims

    @app.get("/v1/media/jobs/{job_id}")
    async def runtime_media_job(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/media/jobs")
        value = await activity_store.get_media_job_for_thread(job_id, claims.thread_id)
        if not value:
            raise HTTPException(404, "Media job not found")
        return value

    @app.post("/v1/job-artifacts/emit")
    async def runtime_emit_job_artifact(request: Request, artifact_role: str, item_key: str,
                                        position: int, filename: str,
                                        output_disposition: OutputDisposition,
                                        mime: str = "application/octet-stream"):
        claims = await authorize_runtime_request(request, "/v1/job-artifacts/emit",
            streaming_body=True)
        value = ArtifactEmission(
            job_id=claims.job_id, generation=claims.generation,
            artifact_role=artifact_role, item_key=item_key, position=position,
            filename=filename, output_disposition=output_disposition, declared_mime=mime,
        )
        try:
            return await artifact_gateway.emit(value, request.stream())
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/v1/job-artifacts/{artifact_id}/content")
    async def runtime_fetch_job_artifact(artifact_id:str,request:Request):
        claims=await authorize_runtime_request(request,"/v1/job-artifacts")
        if claims.scope!='turn':raise HTTPException(404,"Job Artifact not found")
        artifact=await activity_store.get_reviewed_job_artifact_for_thread(
            artifact_id,claims.thread_id)
        if not artifact:raise HTTPException(404,"Job Artifact not found")
        try:size=await asyncio.to_thread(objects.size,artifact.object_ref,maximum=artifact.size)
        except Exception as exc:
            raise HTTPException(503,"Job Artifact content is temporarily unavailable") from exc
        if size!=artifact.size:
            raise HTTPException(503,"Job Artifact content is temporarily unavailable")
        filename=Path(artifact.filename).name or "artifact.bin"
        fallback=re.sub(r'[^A-Za-z0-9._-]','_',filename)[:160] or 'artifact.bin'
        headers={"Content-Length":str(artifact.size),
            "X-Updream-Artifact-Sha256":artifact.sha256,
            "Content-Disposition":f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(filename)}"}
        return StreamingResponse(objects.iter_bytes(artifact.object_ref,maximum=artifact.size),
            media_type=artifact.mime,headers=headers)

    @app.post("/v1/job-group-drafts")
    async def runtime_prepare_job_group(value: JobGroupCreate, request: Request):
        claims = await authorize_runtime_request(request, "/v1/job-group-drafts")
        try:value=JobCoordinator.scope_child_group(value,claims)
        except ValueError as exc:raise HTTPException(403,str(exc)) from exc
        if value.thread_id != claims.thread_id or value.origin_turn_id != claims.turn_id:
            raise HTTPException(403, "JobGroup identity is outside Turn capability")
        try:
            owner_id = await activity_store.owner_id_for_thread(claims.thread_id)
            return await job_coordinator.prepare_group(owner_id, value)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/v1/execution/catalog")
    async def runtime_execution_catalog(request:Request):
        claims=await authorize_runtime_request(request,"/v1/execution/catalog")
        owner_id=await activity_store.owner_id_for_thread(claims.thread_id)
        inputs=await activity_store.turn_input_artifacts(claims.turn_id,owner_id)
        metadata=execution_catalog.public_execution_metadata(await readiness.snapshot())
        artifacts=[{
            "artifact_id":item["artifact"].id,"project_file_id":item["project_file_id"],
            "project_path":item["project_path"],"source_type":item["source_type"],
            "sha256":item["artifact"].sha256,"size":item["artifact"].size,
            "mime":item["artifact"].mime} for item in inputs]
        if claims.scope!='job':
            return {**metadata,"job_group_schema":JobGroupCreate.model_json_schema(),
                "input_artifacts":artifacts,"media_models":media_catalog.public()}
        allowed_operations=set(claims.operation_ids)
        allowed_capabilities=set(claims.capability_ids)
        allowed_skills=set(claims.skill_ids)
        allowed_artifacts=set(claims.artifact_ids)
        return {"job_descriptor":{"id":claims.descriptor_id,"version":claims.descriptor_version,
                "digest":claims.descriptor_digest},
            "operations":[row for row in metadata.get("operations",[])
                if row.get("id") in allowed_operations],
            "capabilities":[row for row in metadata.get("capabilities",[])
                if row.get("id") in allowed_capabilities],
            "skills":[row for row in metadata.get("skills",[]) if row.get("id") in allowed_skills],
            "child_execution_profiles":[],
            "input_artifacts":[row for row in artifacts if row["artifact_id"] in allowed_artifacts],
            "media_models":[]}

    @app.get("/v1/operations/artifact.inspect")
    async def runtime_inspect_artifact(artifact_id:str,request:Request):
        claims=await authorize_runtime_request(request,"/v1/operations/artifact.inspect")
        if claims.scope!='job' or 'artifact.inspect' not in claims.operation_ids:
            raise HTTPException(403,'artifact.inspect is outside Job grant scope')
        if artifact_id not in claims.artifact_ids:
            raise HTTPException(403,'Artifact is outside Job grant scope')
        artifact=await activity_store.get_artifact(artifact_id)
        if not artifact:raise HTTPException(404,'Artifact not found')
        return {'artifact_id':artifact.id,'path':artifact.path,'sha256':artifact.sha256,
            'size':artifact.size,'mime':artifact.mime}

    @app.post("/v1/reviews")
    async def runtime_prepare_review(value:ReviewSubmission,request:Request):
        claims=await authorize_runtime_request(request,"/v1/reviews")
        owner_id=await activity_store.owner_id_for_thread(claims.thread_id)
        if not owner_id:raise HTTPException(404,"Thread not found")
        try:return await reviewer.prepare_submission(owner_id,claims.thread_id,claims.turn_id,value)
        except KeyError as exc:raise HTTPException(404,str(exc)) from exc
        except ValueError as exc:raise HTTPException(422,str(exc)) from exc
        except RuntimeError as exc:raise HTTPException(409,str(exc)) from exc

    async def runtime_draft_owner(draft_id: str, claims):
        value = await activity_store.get_job_group_draft(draft_id)
        if not value or value.thread_id != claims.thread_id:
            raise HTTPException(404, "JobGroupDraft not found")
        return value.owner_id

    @app.get("/v1/job-group-drafts/{draft_id}")
    async def runtime_get_job_group_draft(draft_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/job-group-drafts")
        owner_id = await runtime_draft_owner(draft_id, claims)
        return await activity_store.get_job_group_draft(draft_id, owner_id)

    @app.patch("/v1/job-group-drafts/{draft_id}")
    async def runtime_edit_job_group_draft(draft_id: str, value: JobGroupDraftUpdate, request: Request):
        claims = await authorize_runtime_request(request, "/v1/job-group-drafts")
        owner_id = await runtime_draft_owner(draft_id, claims)
        try:
            return await job_coordinator.edit_group(draft_id, owner_id, value)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/v1/job-groups/{group_id}")
    async def runtime_get_job_group(group_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/job-groups")
        group = await activity_store.get_job_group(group_id)
        if not group or group.thread_id != claims.thread_id:
            raise HTTPException(404, "JobGroup not found")
        return {"group": group, "jobs": await activity_store.list_jobs_for_group(group.id)}

    @app.post("/v1/job-groups/{group_id}/cancel")
    async def runtime_cancel_job_group(group_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/job-groups")
        group = await activity_store.get_job_group(group_id)
        if not group or group.thread_id != claims.thread_id:
            raise HTTPException(404, "JobGroup not found")
        return {"jobs": await job_coordinator.cancel_group(group_id, group.owner_id)}

    @app.get("/v1/jobs/{job_id}")
    async def runtime_get_job(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/jobs")
        value = await activity_store.get_job(job_id)
        if not value or value.thread_id != claims.thread_id:
            raise HTTPException(404, "Job not found")
        return {**value.model_dump(mode="json"),"cleanup":await activity_store.get_job_cleanup(job_id)}

    @app.post("/v1/jobs/{job_id}/cancel")
    async def runtime_cancel_job(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/jobs")
        value = await activity_store.get_job(job_id)
        if not value or value.thread_id != claims.thread_id:
            raise HTTPException(404, "Job not found")
        return await job_coordinator.cancel_job(job_id, value.owner_id)

    @app.get("/v1/media/catalog")
    async def runtime_media_catalog(request: Request, kind: str | None = None):
        await authorize_runtime_request(request, "/v1/media/catalog")
        if kind is not None and kind not in {"image", "video", "audio", "model3d"}:
            raise HTTPException(422, "Invalid media kind")
        return {"models": media_catalog.public(kind)}

    @app.post("/v1/media/drafts")
    async def runtime_create_media_draft(request: Request):
        claims = await authorize_runtime_request(request, "/v1/media/drafts")
        try:
            raw = await request.json()
            value = MediaDraftCreate.model_validate({**raw, "turn_id": claims.turn_id})
        except ValidationError as exc:
            raise HTTPException(422, validation_detail(exc))
        except (ValueError, TypeError):
            raise HTTPException(422, "Invalid media draft")
        try:
            spec = media_catalog.resolve(value.kind, value.model)
        except KeyError:
            raise HTTPException(422, "Media model is not enabled")
        try:
            owner_id = await activity_store.owner_id_for_thread(claims.thread_id)
            if not owner_id:
                raise HTTPException(404, "Thread not found")
            references = await resolve_media_references(activity_store, objects, claims.thread_id,
                owner_id, value.references)
            media_catalog.validate_parameters(value.kind, value.model, value.parameters,
                generation_mode=value.generation_mode, references=references)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        try:
            draft, _ = await activity_store.create_media_draft(owner_id, claims.thread_id, value,
                expires_at=now() + timedelta(seconds=settings.media_draft_ttl_seconds), provider_spec=spec,
                reference_snapshots=references)
        except KeyError:
            raise HTTPException(404, "Media Turn not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        return {**draft.model_dump(mode="json"), "requires_confirmation": True}

    @app.post("/v1/media/batch/commit")
    async def runtime_commit_media_batch(request: Request):
        claims = await authorize_runtime_request(request, "/v1/media/drafts")
        owner_id = await activity_store.owner_id_for_thread(claims.thread_id)
        if not owner_id:
            raise HTTPException(404, "Thread not found")
        try:
            return await activity_store.seal_media_batch(claims.turn_id, claims.thread_id, owner_id)
        except KeyError:
            raise HTTPException(404, "Media Turn not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/v1/media/jobs/{job_id}/cancel")
    async def runtime_cancel_media_job(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/media/jobs")
        try:
            return await activity_store.request_media_cancel_for_thread(job_id, claims.thread_id)
        except KeyError:
            raise HTTPException(404, "Media job not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/v1/media/jobs/{job_id}/content")
    async def runtime_media_job_content(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/media/jobs")
        output = await activity_store.media_job_output_for_thread(job_id, claims.thread_id)
        if not output:
            raise HTTPException(404, "Media output not found")
        try:
            data = await asyncio.to_thread(objects.get, output["object_ref"], maximum=settings.max_artifact_bytes)
        except (KeyError, ValueError, OSError):
            raise HTTPException(404, "Media output not found")
        except Exception as exc:
            event("media.output.read_failed", level=40, job_id=job_id, exc=exc)
            raise HTTPException(502, "Media output is temporarily unavailable")
        if len(data) != output["size"]:
            raise HTTPException(409, "Media output size mismatch")
        return Response(data, media_type=output["mime"] or "application/octet-stream",
            headers={"X-Media-Filename": output["filename"]})

    @app.post("/v1/media/jobs/{job_id}/promote")
    async def runtime_promote_media_job(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/media/jobs")
        try:
            raw = await request.json()
            return await activity_store.promote_media_job_for_thread(job_id, claims.thread_id,
                str(raw.get("output_role") or ""))
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except KeyError:
            raise HTTPException(404, "Media Job not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.post("/v1/sites/drafts")
    async def runtime_create_site_draft(request: Request, name: str, slug: str,
                                        idempotency_key: str):
        claims = await authorize_runtime_request(request, "/v1/sites/drafts")
        if not 1 <= len(name.strip()) <= 120 or not 8 <= len(idempotency_key) <= 160:
            raise HTTPException(422, "invalid_site_draft")
        try:
            slug = normalize_site_slug(slug)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        declared=request.headers.get("content-length")
        if declared:
            try:declared_size=int(declared)
            except ValueError:raise HTTPException(400,"invalid_content_length")
            if declared_size>MAX_SITE_ARCHIVE_BYTES:raise HTTPException(413,"site_archive_too_large")
        staging_root=settings.data_root/"tmp"/"site-stages"
        ensure_private_directory(staging_root,boundary=settings.data_root/"tmp")
        package=None;archive_path=None
        try:
            with NamedTemporaryFile(dir=staging_root,
                    prefix=f"site-ingress-{os.getpid()}-",delete=False) as stream:
                archive_path=Path(stream.name);digest_builder=hashlib.sha256();archive_size=0
                async for chunk in request.stream():
                    archive_size+=len(chunk)
                    if archive_size>MAX_SITE_ARCHIVE_BYTES:raise HTTPException(413,"site_archive_too_large")
                    digest_builder.update(chunk);stream.write(chunk)
            os.chmod(archive_path,0o600);digest=digest_builder.hexdigest()
            package=await asyncio.to_thread(stage_site_archive,archive_path,staging_root)
            files=package.files
            object_ref,stored_digest=await asyncio.to_thread(objects.put_file,archive_path,
                digest=digest,size=archive_size)
            if stored_digest!=digest:raise ValueError("site_archive_identity_mismatch")
            staged = []
            for value in files:
                file_ref, file_digest = await asyncio.to_thread(objects.put_file,value.source,
                    digest=value.sha256,size=value.size)
                if file_digest != value.sha256:
                    raise ValueError("site_file_identity_mismatch")
                staged.append({"path":value.path, "sha256":value.sha256, "size":value.size,
                    "mime":value.mime, "object_ref":file_ref})
            owner_id = await activity_store.owner_id_for_thread(claims.thread_id)
            if not owner_id:
                raise HTTPException(404, "Thread not found")
            draft, _ = await activity_store.create_site_draft(owner_id, claims.thread_id,
                claims.turn_id, name=name.strip(), slug=slug, idempotency_key=idempotency_key,
                archive_object_ref=object_ref, bundle_sha256=digest, archive_size=archive_size,
                file_count=len(files), total_size=sum(value.size for value in files),
                expires_at=now() + timedelta(seconds=settings.site_draft_ttl_seconds),
                public_base_path=settings.site_public_base_path, files=staged)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        except KeyError:
            raise HTTPException(404, "Site Turn not found")
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))
        finally:
            if package:package.remove()
            if archive_path:archive_path.unlink(missing_ok=True)
        return {**draft.model_dump(mode="json"), "requires_confirmation":True}

    @app.get("/v1/sites/jobs/{job_id}")
    async def runtime_site_job(job_id: str, request: Request):
        claims = await authorize_runtime_request(request, "/v1/sites/jobs")
        value = await activity_store.get_site_job_for_thread(job_id, claims.thread_id)
        if not value:
            raise HTTPException(404, "Site deployment job not found")
        return value

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request):
        route = "/" + path
        # Native model history can contain multiple inline image previews. Read
        # it under its own bound after authorization and activity creation.
        claims = await authorize_runtime_request(request, route,
            streaming_body=route.startswith("/v1/models/"))
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
            client = observed_async_client('model-provider',base_url=base_url.rstrip("/"),
                timeout=httpx.Timeout(None, connect=20), trust_env=False)
            clients[client_key] = client
        suffix = upstream_suffix(route, prefix)
        headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS | {"authorization", "x-api-key", "anthropic-api-key"}}
        if credential:
            headers["authorization"] = "Bearer " + credential
            if prefix.endswith(("claude", "deepseek")):
                headers["x-api-key"] = credential
        endpoint_kind=(model_endpoint_kind(harness,suffix)
            if prefix.startswith("/v1/models/") else None)
        if prefix.startswith("/v1/models/") and endpoint_kind is None:
            raise HTTPException(404,"model_route_not_supported")
        observe_model=endpoint_kind in {"generation","compaction"}
        activity = ModelActivity(activity_store, claims) if observe_model else None
        reservation_id = ""
        if activity:await activity.start()
        try:
            body = await bounded_request_body(request,
                settings.gateway_model_request_max_bytes if prefix.startswith("/v1/models/")
                    else settings.gateway_request_max_bytes,
                settings.gateway_body_idle_timeout_seconds,
                settings.gateway_body_total_timeout_seconds)
            if prefix.startswith("/v1/models/"):
                try:
                    payload = json.loads(body)
                except (TypeError, ValueError):
                    raise HTTPException(422, "Model request must be JSON")
                if not isinstance(payload, dict):
                    raise HTTPException(422, "Model request must be an object")
                payload["model"] = claims.model
                payload=normalize_model_request(payload,harness=harness,model=claims.model,suffix=suffix)
        except BaseException as exc:
            status=exc.status_code if isinstance(exc,HTTPException) else None
            code=("model_request_too_large" if status==413 and prefix.startswith("/v1/models/")
                else "model_request_invalid" if status in {400,422}
                else "model_request_body_timeout" if status==408 else type(exc).__name__)
            if activity:await activity.finish(status,error_code=code)
            if code=="model_request_too_large":raise HTTPException(413,code) from exc
            raise
        if prefix.startswith("/v1/models/"):
            try:
                requested_output = model_token_reservation(payload,suffix,endpoint_kind,
                    int(request.state.capability_security["token_remaining"]),
                    estimate_model_input_tokens(payload,len(body)))
            except HTTPException as exc:
                if activity:await activity.finish(exc.status_code,error_code=str(exc.detail))
                raise
            if activity:
                reservation_id=activity.request_id
                try:
                    await activity_store.reserve_model_tokens(claims,reservation_id,requested_output)
                except ValueError as exc:
                    code=str(exc)
                    if code=="model_token_budget_exhausted":status=429
                    elif code in {"capability_revoked","capability_expired","capability_identity_conflict",
                            "model_token_reservation_invalid","model_token_reservation_conflict"}:status=409
                    else:status=503;code="capability_authority_unavailable"
                    await activity.finish(status,error_code=code)
                    raise HTTPException(status,code) from exc
                except Exception as exc:
                    await activity.finish(503,error_code="capability_authority_unavailable")
                    raise HTTPException(503,"capability_authority_unavailable") from exc
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        upstream = client.build_request(request.method, suffix, params=request.query_params, headers=headers, content=body)
        upstream.headers.update(carrier(getattr(activity,"span",None)))
        started = time.monotonic()
        try:
            response = await send_with_start_timeout(client, upstream, settings.gateway_model_start_timeout_seconds)
        except BaseException as exc:
            if activity:
                await activity.finish(error_code=type(exc).__name__)
            if reservation_id:
                await activity_store.finalize_model_tokens(reservation_id,None)
            raise
        if activity:
            activity.header_ms = round((time.monotonic() - started) * 1000)
        event("upstream.response.headers",level=logging.WARNING if response.status_code>=400 else logging.INFO,
            turn_id=claims.turn_id,model=claims.model,header_ms=round((time.monotonic()-started)*1000,3),
            **{"http.response.status_code":response.status_code},request_bytes=len(body),
            upstream_request_id=response.headers.get("x-request-id") or response.headers.get("request-id", ""))
        try:
            await activity_store.authorize_capability(claims,"check")
        except ValueError:
            await response.aclose()
            if activity:
                await activity.finish(error_code="capability_revoked")
            if reservation_id:
                await activity_store.finalize_model_tokens(reservation_id,None)
            raise HTTPException(401, "Capability token expired or revoked")
        except Exception as exc:
            await response.aclose()
            if activity:await activity.finish(error_code="capability_authority_unavailable")
            if reservation_id:await activity_store.finalize_model_tokens(reservation_id,None)
            raise HTTPException(503,"capability_authority_unavailable") from exc
        iterator=response.aiter_bytes().__aiter__();first_chunk=None
        try:
            first_chunk=await asyncio.wait_for(iterator.__anext__(),
                timeout=settings.gateway_model_idle_timeout_seconds)
        except StopAsyncIteration:
            if activity and "text/event-stream" in response.headers.get("content-type",""):
                await response.aclose();await activity.finish(error_code="stream_incomplete")
                if reservation_id:await activity_store.finalize_model_tokens(reservation_id,None)
                raise HTTPException(502,"model_stream_incomplete")
        except TimeoutError as exc:
            await response.aclose()
            if activity:await activity.finish(error_code="model_response_idle_timeout")
            if reservation_id:await activity_store.finalize_model_tokens(reservation_id,None)
            raise HTTPException(504,"model_response_idle_timeout") from exc
        active[claims.nonce].add(response)

        token_finalized=False
        async def finalize_tokens():
            nonlocal token_finalized
            if token_finalized or not reservation_id:return
            token_finalized=True
            usage=(activity.probe.token_usage.budget_charge(
                    settings.model_cache_read_budget_percent,
                    settings.model_cache_write_budget_percent)
                if activity and activity.probe.input_tokens is not None
                and activity.probe.output_tokens is not None else None)
            await activity_store.finalize_model_tokens(reservation_id,usage)

        async def observe_chunk(chunk):
            if activity:
                await activity.observe(chunk)
                if activity.probe.completed or activity.probe.failed:
                    await activity.finish(response.status_code)
                    await finalize_tokens()

        async def content():
            stream_error = ""
            try:
                async with asyncio.timeout(max(0, claims.expires_at - time.time())):
                    if first_chunk is not None:
                        await observe_chunk(first_chunk)
                        yield first_chunk
                    while True:
                        try:
                            chunk=await asyncio.wait_for(iterator.__anext__(),
                                timeout=settings.gateway_model_idle_timeout_seconds)
                        except StopAsyncIteration:break
                        except TimeoutError as exc:
                            if activity and activity.probe.completed:return
                            raise ModelUpstreamIdleTimeout("model_response_idle_timeout") from exc
                        await observe_chunk(chunk)
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
                        await finalize_tokens()
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
    configure("gateway", settings.data_root / "logs", settings)
    uvicorn.run(create_gateway(settings), host=settings.gateway_bind_host, port=settings.gateway_bind_port, log_config=None, access_log=False)


if __name__ == "__main__":
    run()
