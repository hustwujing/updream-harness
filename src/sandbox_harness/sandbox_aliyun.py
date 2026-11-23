from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import time
from typing import Any, Mapping, Protocol
import weakref

import httpx

from .domain import Harness
from .observability import observed
from .sandbox_contracts import (
    Sandbox,
    SandboxAuthenticationFailed,
    SandboxCapabilities,
    SandboxCreateOutcomeUnknown,
    SandboxCreateRequest,
    SandboxEndpoint,
    SandboxEndpointUnavailable,
    SandboxHandle,
    SandboxNotFound,
    SandboxProfile,
    SandboxProfileUnavailable,
    SandboxProviderError,
    SandboxProviderUnavailable,
    SandboxRateLimited,
    SandboxRuntimeRoute,
    SandboxScope,
    SandboxState,
)


_PROFILE_ID_KEY = "sandbox-harness.io/profile"
_PROFILE_COMPATIBILITY_KEY = "sandbox-harness.io/profile-key"
_PROFILE_PROVIDER_REF_KEY="sandbox-harness.io/provider-ref"


class _Paginator(Protocol):
    @property
    def has_next(self) -> bool: ...

    async def next_items(self) -> list[Any]: ...


class _AliyunClient(Protocol):
    async def create(self, *, template: str, timeout: int, metadata: Mapping[str, str],
                     envs: Mapping[str, str]) -> Any: ...
    async def get_info(self, sandbox_id: str) -> Any: ...
    async def connect(self, sandbox_id: str) -> Any: ...
    def list(self, metadata: Mapping[str, str]) -> _Paginator: ...
    async def set_timeout(self, sandbox_id: str, timeout: int) -> None: ...
    async def kill(self, sandbox_id: str) -> bool: ...


class _E2BClient:
    def __init__(self, *, api_key: str, api_url: str, domain: str,
                 request_timeout_seconds: float):
        from e2b import AsyncSandbox
        from e2b.sandbox.sandbox_api import SandboxQuery

        self._sandbox = AsyncSandbox
        self._query = SandboxQuery
        self._opts = {
            "api_key": api_key,
            "api_url": api_url,
            "domain": domain,
            "request_timeout": request_timeout_seconds,
        }

    async def create(self, *, template: str, timeout: int, metadata: Mapping[str, str],
                     envs: Mapping[str, str]) -> Any:
        return await self._sandbox.create(template=template, timeout=timeout,
            metadata=dict(metadata), envs=dict(envs), secure=True,
            network={"allow_public_traffic":False}, **self._opts)

    async def get_info(self, sandbox_id: str) -> Any:
        return await self._sandbox.get_info(sandbox_id, **self._opts)

    async def connect(self, sandbox_id: str) -> Any:
        return await self._sandbox.connect(sandbox_id, **self._opts)

    def list(self, metadata: Mapping[str, str]) -> _Paginator:
        return self._sandbox.list(query=self._query(metadata=dict(metadata)), **self._opts)

    async def set_timeout(self, sandbox_id: str, timeout: int) -> None:
        await self._sandbox.set_timeout(sandbox_id, timeout, **self._opts)

    async def kill(self, sandbox_id: str) -> bool:
        return await self._sandbox.kill(sandbox_id, **self._opts)


class AliyunSandboxBackend:
    provider = "aliyun"
    capabilities=SandboxCapabilities(native_idempotency=False,scoped_inventory=True,
        arbitrary_ports=True,ttl_renewal=True,authenticated_runtime_http=True,
        delete_convergence=True,pause_resume=True)

    def __init__(self, *, api_key: str, api_url: str, domain: str,
                 timeout_seconds: int, namespace: str,
                 request_timeout_seconds: float = 60,
                 client: _AliyunClient | None = None,
                 health_transport: httpx.AsyncBaseTransport | None = None,
                 reconciliation_attempts:int=12,reconciliation_interval:float=.25,
                 gateway_url:str="http://10.194.49.8:29882",
                 proxy_url:str="http://10.194.49.8:29883"):
        self.api_url = api_url.rstrip("/")
        self.domain = domain
        self.timeout_seconds = timeout_seconds
        self.namespace = namespace
        self.reconciliation_attempts=max(1,reconciliation_attempts)
        self.reconciliation_interval=max(0,reconciliation_interval)
        self.route=SandboxRuntimeRoute(gateway_url,proxy_url)
        self.client = client or _E2BClient(api_key=api_key, api_url=self.api_url,
            domain=domain, request_timeout_seconds=request_timeout_seconds)
        self.health_client = httpx.AsyncClient(timeout=10, trust_env=False,
            transport=health_transport)
        self._profiles: dict[str, SandboxProfile] = {}
        self._handles: dict[str, Any] = {}
        self._endpoints: dict[tuple[str, int], SandboxEndpoint] = {}
        self._endpoint_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._create_locks: weakref.WeakValueDictionary[str,asyncio.Lock] = (
            weakref.WeakValueDictionary())
        self._allocations:dict[str,str]={}

    @staticmethod
    def _profile_metadata(profile: SandboxProfile) -> dict[str, str]:
        return {
            _PROFILE_ID_KEY: profile.profile_id,
            _PROFILE_COMPATIBILITY_KEY: profile.compatibility_key,
            _PROFILE_PROVIDER_REF_KEY:profile.provider_ref,
        }

    @staticmethod
    def _state(value: Any) -> SandboxState:
        native = str(getattr(value, "value", value)).lower().rsplit(".", 1)[-1]
        return {
            "running": SandboxState.READY,
            "paused": SandboxState.STOPPED,
            "starting": SandboxState.STARTING,
            "provisioning": SandboxState.PROVISIONING,
            "stopping": SandboxState.STOPPING,
            "killed": SandboxState.STOPPED,
            "failed": SandboxState.FAILED,
        }.get(native, SandboxState.UNKNOWN)

    def _map_error(self, exc: BaseException, *, endpoint: bool = False) -> SandboxProviderError:
        name = type(exc).__name__
        if name in {"KeyError", "SandboxNotFoundException", "NotFoundException"}:
            return SandboxNotFound("Aliyun sandbox is unavailable", provider=self.provider)
        if name == "AuthenticationException":
            return SandboxAuthenticationFailed("Aliyun sandbox authentication failed",
                provider=self.provider)
        if name == "RateLimitException":
            return SandboxRateLimited("Aliyun sandbox rate limit reached", provider=self.provider)
        if name in {"TemplateException", "InvalidArgumentException"}:
            return SandboxProfileUnavailable("Aliyun sandbox profile is unavailable",
                provider=self.provider)
        if endpoint:
            return SandboxEndpointUnavailable("Aliyun sandbox endpoint is unavailable",
                provider=self.provider)
        return SandboxProviderUnavailable("Aliyun sandbox request failed", provider=self.provider)

    def _profile(self, info: Any) -> SandboxProfile:
        sandbox_id = str(info.sandbox_id)
        if sandbox_id in self._profiles:
            return self._profiles[sandbox_id]
        metadata = dict(getattr(info, "metadata", {}) or {})
        provider_ref=str(metadata.get(_PROFILE_PROVIDER_REF_KEY)
            or getattr(info,"template_id","") or "provider-recovered")
        profile = SandboxProfile(
            profile_id=str(metadata.get(_PROFILE_ID_KEY) or "provider-recovered"),
            revision=provider_ref,
            provider=self.provider,
            provider_ref=provider_ref,
            runtime_contract="sandbox-harness-runtime/v1",
            runtime_bundle_digest="provider-recovered",
            runtime_code_bundle_digest="provider-recovered",
            compatibility_key=str(metadata.get(_PROFILE_COMPATIBILITY_KEY) or ""),
        )
        self._profiles[sandbox_id] = profile
        return profile

    @staticmethod
    def _aware(value: Any) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    def _sandbox(self, info: Any) -> Sandbox:
        metadata = dict(getattr(info, "metadata", {}) or {})
        try:
            harness = Harness(metadata.get("sandbox-harness.io/harness", Harness.CODEX.value))
        except ValueError:
            harness = Harness.CODEX
        state = self._state(getattr(info, "state", "unknown"))
        profile = self._profile(info)
        return Sandbox(id=str(info.sandbox_id), provider=self.provider, harness=harness,
            profile=profile, metadata=metadata, state=state,
            expires_at=self._aware(getattr(info, "end_at", None)),
            provider_details={"template_id": profile.provider_ref})

    async def _info(self, sandbox_id: str) -> Any:
        try:
            return await self.client.get_info(sandbox_id)
        except Exception as exc:
            raise self._map_error(exc) from exc

    async def _owned_info(self,sandbox_id:str)->Any:
        info=await self._info(sandbox_id)
        metadata=dict(getattr(info,"metadata",{}) or {})
        if metadata.get("sandbox-harness.io/namespace")!=self.namespace:
            raise SandboxNotFound("Aliyun sandbox is outside this namespace",
                provider=self.provider)
        return info

    async def _inventory(self, metadata: Mapping[str, str]) -> list[Sandbox]:
        namespace_key="sandbox-harness.io/namespace"
        if namespace_key in metadata and metadata[namespace_key]!=self.namespace:return []
        query={namespace_key:self.namespace,
            **{key:value for key,value in metadata.items() if key!=namespace_key}}
        try:
            paginator = self.client.list(query)
            rows: list[Any] = []
            while paginator.has_next:
                rows.extend(await paginator.next_items())
        except Exception as exc:
            raise self._map_error(exc) from exc
        result = []
        for row in rows:
            values = dict(getattr(row, "metadata", {}) or {})
            if all(values.get(key) == value for key, value in query.items()):
                result.append(self._sandbox(row))
        return result

    async def reconcile_create(self,scope:SandboxScope,allocation_id:str)->Sandbox|None:
        if scope.namespace!=self.namespace:return None
        rows=await self._inventory({"sandbox-harness.io/idempotency-key":allocation_id})
        if len(rows)>1:raise SandboxCreateOutcomeUnknown(
            "Duplicate Aliyun allocations found",provider=self.provider)
        return rows[0] if rows else None

    async def list_owned(self, scope: SandboxScope) -> list[Sandbox]:
        if scope.namespace != self.namespace:
            return []
        return await self._inventory({})

    async def _handle(self, sandbox_id: str, *, force_refresh: bool = False) -> Any:
        current = self._handles.get(sandbox_id)
        if current is not None and not force_refresh:
            return current
        try:
            current = await self.client.connect(sandbox_id)
        except Exception as exc:
            raise self._map_error(exc, endpoint=True) from exc
        self._handles[sandbox_id] = current
        return current

    async def endpoint(self, sandbox: Sandbox, port: int, *,
                       force_refresh: bool = False) -> SandboxEndpoint:
        if not 1 <= port <= 65535:
            raise ValueError("Invalid sandbox port")
        if sandbox.metadata.get("sandbox-harness.io/namespace")!=self.namespace:
            raise SandboxNotFound("Aliyun sandbox is outside this namespace",
                provider=self.provider)
        key = (sandbox.id, port)
        current = self._endpoints.get(key)
        now = time.monotonic()
        if not force_refresh and current and (current.expires_at_monotonic is None or
                current.expires_at_monotonic - now > 30):
            return current
        lock = self._endpoint_locks.setdefault(key, asyncio.Lock())
        async with lock:
            current = self._endpoints.get(key)
            now = time.monotonic()
            if not force_refresh and current and (current.expires_at_monotonic is None or
                    current.expires_at_monotonic - now > 30):
                return current
            await self._owned_info(sandbox.id)
            handle = await self._handle(sandbox.id,force_refresh=True)
            try:
                host = handle.get_host(port)
                token = getattr(handle, "traffic_access_token", None)
            except Exception as exc:
                raise self._map_error(exc, endpoint=True) from exc
            if not isinstance(host, str) or not host:
                raise SandboxEndpointUnavailable("Aliyun sandbox returned no endpoint",
                    provider=self.provider)
            if not isinstance(token,str) or not token:
                raise SandboxEndpointUnavailable(
                    "Aliyun sandbox returned no authenticated traffic token",
                    provider=self.provider)
            headers = {"e2b-traffic-access-token":token}
            endpoint = SandboxEndpoint(url="https://" + host, headers=headers,
                expires_at_monotonic=time.monotonic() + 60)
            self._endpoints[key] = endpoint
            sandbox.runtime_url = endpoint.url
            sandbox.runtime_headers = dict(endpoint.headers)
            sandbox.runtime_endpoint_refresh_at_monotonic = endpoint.expires_at_monotonic or 0
            return endpoint

    async def _ready(self, sandbox: Sandbox) -> Sandbox:
        for _ in range(80):
            info = await self._owned_info(sandbox.id)
            state = self._state(getattr(info, "state", "unknown"))
            sandbox.state = state
            sandbox.expires_at = self._aware(getattr(info, "end_at", None))
            if state == SandboxState.FAILED:
                raise SandboxProviderUnavailable("Aliyun sandbox failed before Runtime was ready",
                    provider=self.provider)
            if state == SandboxState.STOPPED:
                await self._handle(sandbox.id,force_refresh=True)
                sandbox.state=SandboxState.STARTING
            if state == SandboxState.READY:
                try:
                    endpoint = await self.endpoint(sandbox, 8080, force_refresh=True)
                    response = await self.health_client.get(endpoint.url.rstrip("/") + "/health",
                        headers=dict(endpoint.headers))
                    if response.status_code == 200 and response.json().get("status") in {"ok", "bootstrap"}:
                        sandbox.health_state = "ready"
                        sandbox.health_error = ""
                        return sandbox
                except (httpx.HTTPError, ValueError, SandboxEndpointUnavailable):
                    pass
            sandbox.health_state = "starting"
            await asyncio.sleep(.25)
        raise SandboxProviderUnavailable("Aliyun Runtime endpoint did not become ready",
            provider=self.provider)

    @observed("sandbox.create")
    async def create(self, request: SandboxCreateRequest) -> Sandbox:
        if request.profile.provider!=self.provider or not request.profile.provider_ref:
            raise SandboxProfileUnavailable("Aliyun requires a template profile",
                provider=self.provider)
        lock=self._create_locks.get(request.allocation_id)
        if lock is None:
            lock=asyncio.Lock();self._create_locks[request.allocation_id]=lock
        async with lock:
            cached=self._allocations.get(request.allocation_id)
            if cached:
                try:existing=await self.get(cached)
                except SandboxNotFound:self._allocations.pop(request.allocation_id,None)
                else:
                    self._require_compatible_allocation(existing,request.profile)
                    return existing
            existing=await self._reconcile_allocation(request.allocation_id)
            if existing:
                self._require_compatible_allocation(existing,request.profile)
                self._profiles[existing.id]=request.profile
                self._allocations[request.allocation_id]=existing.id
                return await self._ready(existing)
            return await self._create_once(request)

    def _require_compatible_allocation(self,sandbox:Sandbox,profile:SandboxProfile)->None:
        if (sandbox.compatibility_key!=profile.compatibility_key
                or sandbox.profile.provider_ref!=profile.provider_ref):
            raise SandboxProfileUnavailable(
                "Aliyun idempotency key conflicts with another profile",provider=self.provider)

    async def _reconcile_allocation(self,idempotency_key:str)->Sandbox|None:
        for attempt in range(self.reconciliation_attempts):
            matches=await self._inventory({"sandbox-harness.io/idempotency-key":idempotency_key})
            if len(matches)>1:
                raise SandboxCreateOutcomeUnknown(
                    "Duplicate Aliyun sandboxes found for one create request",
                    provider=self.provider)
            if matches:return matches[0]
            if attempt+1<self.reconciliation_attempts:
                await asyncio.sleep(self.reconciliation_interval)
        return None

    async def _create_once(self, request: SandboxCreateRequest) -> Sandbox:
        metadata = {
            **self._profile_metadata(request.profile),
            "sandbox-harness.io/namespace": self.namespace,
            "sandbox-harness.io/idempotency-key": request.allocation_id,
            "sandbox-harness.io/harness": request.harness.value,
        }
        try:
            handle = await self.client.create(template=request.profile.provider_ref,
                timeout=request.ttl_seconds, metadata=metadata, envs=request.environment)
        except Exception as exc:
            mapped = self._map_error(exc)
            if isinstance(mapped, (SandboxAuthenticationFailed, SandboxRateLimited,
                    SandboxProfileUnavailable)):
                raise mapped from exc
            for attempt in range(3):
                try:
                    match=await self._reconcile_allocation(request.allocation_id)
                except SandboxProviderError:
                    match=None
                if match:
                    self._require_compatible_allocation(match,request.profile)
                    self._profiles[match.id]=request.profile
                    self._allocations[request.allocation_id]=match.id
                    return await self._ready(match)
                if attempt<2:await asyncio.sleep(self.reconciliation_interval)
            raise SandboxCreateOutcomeUnknown("Aliyun sandbox create outcome is unknown",
                provider=self.provider) from exc
        sandbox_id = str(getattr(handle, "sandbox_id", ""))
        if not sandbox_id:
            raise SandboxCreateOutcomeUnknown("Aliyun sandbox create returned no ID",
                provider=self.provider)
        self._handles[sandbox_id] = handle
        self._profiles[sandbox_id] = request.profile
        self._allocations[request.allocation_id]=sandbox_id
        sandbox = Sandbox(id=sandbox_id, provider=self.provider, harness=request.harness,
            profile=request.profile, metadata=metadata, state=SandboxState.PROVISIONING,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=request.ttl_seconds),
            provider_details={"template_id": request.profile.provider_ref})
        try:
            return await self._ready(sandbox)
        except SandboxProviderError as exc:
            raise SandboxCreateOutcomeUnknown(
                "Aliyun accepted the sandbox but Runtime readiness is unresolved",
                provider=self.provider, request_id=exc.request_id) from exc

    async def get(self,handle:SandboxHandle|str)->Sandbox:
        if isinstance(handle,SandboxHandle):
            if handle.provider!=self.provider:raise SandboxNotFound(
                "Aliyun provider mismatch",provider=self.provider)
            sandbox_id=handle.sandbox_id
        else:sandbox_id=handle
        sandbox = self._sandbox(await self._owned_info(sandbox_id))
        return await self._ready(sandbox)

    async def _renew_ttl(self,sandbox:Sandbox,ttl_seconds:int)->Sandbox:
        info = await self._owned_info(sandbox.id)
        provider_metadata = dict(getattr(info, "metadata", {}) or {})
        try:await self.client.set_timeout(sandbox.id,ttl_seconds)
        except Exception as exc:raise self._map_error(exc) from exc
        sandbox.expires_at=datetime.now(timezone.utc)+timedelta(seconds=ttl_seconds)
        sandbox.metadata = provider_metadata
        return sandbox

    async def renew(self, sandbox: Sandbox, *, force: bool = False) -> Sandbox:
        remaining=((sandbox.expires_at-datetime.now(timezone.utc)).total_seconds()
            if sandbox.expires_at else 0)
        if force or remaining<=max(60,self.timeout_seconds/2):
            return await self._renew_ttl(sandbox,self.timeout_seconds)
        return sandbox

    def runtime_route(self,sandbox:Sandbox)->SandboxRuntimeRoute:return self.route

    @observed("sandbox.delete")
    async def delete(self, sandbox: Sandbox) -> None:
        try:
            await self._owned_info(sandbox.id)
        except SandboxNotFound:
            self._forget(sandbox.id);return
        try:
            await self.client.kill(sandbox.id)
        except Exception as exc:
            mapped = self._map_error(exc)
            if not isinstance(mapped, SandboxNotFound):
                raise mapped from exc
        for attempt in range(30):
            try:await self._owned_info(sandbox.id)
            except SandboxNotFound:
                self._forget(sandbox.id);return
            if attempt<29:await asyncio.sleep(.1)
        raise SandboxProviderUnavailable("Aliyun sandbox deletion did not converge",
            provider=self.provider)

    def _forget(self,sandbox_id:str)->None:
        self._handles.pop(sandbox_id,None);self._profiles.pop(sandbox_id,None)
        for key in [key for key,value in self._allocations.items() if value==sandbox_id]:
            self._allocations.pop(key,None)
        for key in [key for key in self._endpoints if key[0]==sandbox_id]:
            self._endpoints.pop(key,None)

    async def aclose(self) -> None:
        self._handles.clear()
        self._profiles.clear()
        self._endpoints.clear()
        self._endpoint_locks.clear()
        self._allocations.clear()
        await self.health_client.aclose()
