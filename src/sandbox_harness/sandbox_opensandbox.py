from __future__ import annotations

import asyncio
from datetime import datetime,timedelta,timezone
import time
from typing import Any,Mapping
from urllib.parse import urlsplit

import httpx

from .domain import Harness
from .observability import event,observed
from .sandbox_contracts import (
    Sandbox,SandboxAuthenticationFailed,SandboxCapabilities,SandboxCapacityUnavailable,
    SandboxCreateOutcomeUnknown,SandboxCreateRequest,SandboxEndpoint,
    SandboxEndpointUnavailable,SandboxNotFound,SandboxProfile,SandboxProfileUnavailable,
    SandboxProviderUnavailable,SandboxRateLimited,SandboxScope,SandboxState,SandboxUpdate)


_PROFILE_ID_KEY="sandbox-harness.io/profile"
_PROFILE_COMPATIBILITY_KEY="sandbox-harness.io/profile-key"


class OpenSandboxBackend:
    provider="opensandbox"
    capabilities=SandboxCapabilities(metadata_update=True,metadata_filter=True,
        arbitrary_ports=True,ttl_renewal=True,idempotent_create=True,
        authenticated_runtime_http=True)

    def __init__(self,base_url:str,api_key:str,timeout_seconds:int,namespace:str,
                 gateway_url:str="http://172.31.0.1:29880",proxy_port:int=29881,
                 *,transport:httpx.AsyncBaseTransport|None=None):
        headers={"OPEN-SANDBOX-API-KEY":api_key} if api_key else {}
        self.client=httpx.AsyncClient(base_url=base_url.rstrip("/"),headers=headers,
            timeout=30,trust_env=False,transport=transport)
        self.timeout_seconds=timeout_seconds;self.namespace=namespace
        gateway=urlsplit(gateway_url)
        if gateway.scheme!="http" or not gateway.hostname or not gateway.port or gateway.path not in ("","/"):
            raise ValueError("Sandbox Gateway must be a fixed HTTP origin")
        self.gateway_host=gateway.hostname;self.gateway_port=gateway.port;self.proxy_port=proxy_port
        self._profiles:dict[str,SandboxProfile]={}
        self._endpoints:dict[tuple[str,int],SandboxEndpoint]={}
        self._endpoint_locks:dict[tuple[str,int],asyncio.Lock]={}

    @staticmethod
    def legacy_profile(harness:Harness,image:str)->SandboxProfile:
        value=image or "fake@sha256:"+"0"*64
        return SandboxProfile(profile_id=f"legacy-{harness.value}",revision=value,
            provider="opensandbox",provider_ref=value,
            runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="legacy",
            runtime_code_bundle_digest="legacy")

    def _legacy_request(self,harness:Harness,image:str,metadata:Mapping[str,str],
                        environment:Mapping[str,str])->SandboxCreateRequest:
        key=str(metadata.get("sandbox-harness.io/turn") or metadata.get(
            "sandbox-harness.io/idempotency-key") or "legacy")
        return SandboxCreateRequest(harness=harness,profile=self.legacy_profile(harness,image),
            environment=environment,metadata=metadata,ttl_seconds=self.timeout_seconds,
            idempotency_key=key[:64])

    @staticmethod
    def _profile_metadata(profile:SandboxProfile)->dict[str,str]:
        identity=profile.profile_id[:63].strip("-_.")
        if not identity or any(value not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_." for value in identity):
            identity="profile-"+profile.compatibility_key[:40]
        return {_PROFILE_ID_KEY:identity,_PROFILE_COMPATIBILITY_KEY:profile.compatibility_key}

    def _profile(self,sandbox_id:str,harness:Harness,image:str,metadata:Mapping[str,str])->SandboxProfile:
        existing=self._profiles.get(sandbox_id)
        if existing:return existing
        profile=SandboxProfile(profile_id=str(metadata.get(_PROFILE_ID_KEY) or "legacy"),
            revision=image or "legacy",provider="opensandbox",provider_ref=image or "legacy",
            runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="provider-recovered",
            runtime_code_bundle_digest="provider-recovered",
            compatibility_key=str(metadata.get(_PROFILE_COMPATIBILITY_KEY) or ""))
        self._profiles[sandbox_id]=profile;return profile

    def _sandbox(self,payload:dict[str,Any],harness:Harness,image:str,
                 metadata:Mapping[str,str])->Sandbox:
        sandbox_id=payload.get("id") or payload.get("sandboxId")
        if not isinstance(sandbox_id,str):raise SandboxProviderUnavailable(
            "OpenSandbox response has no sandbox ID",provider=self.provider)
        profile=self._profile(sandbox_id,harness,image,metadata)
        return Sandbox(id=sandbox_id,provider=self.provider,harness=harness,profile=profile,
            metadata=dict(metadata),state=SandboxState.INVENTORY,
            provider_details={"image":image},image=image)

    def _raise_status(self,response:httpx.Response,*,profile:bool=False,endpoint:bool=False)->None:
        request_id=response.headers.get("x-request-id","")
        if response.status_code in {401,403}:raise SandboxAuthenticationFailed(
            "OpenSandbox authentication failed",provider=self.provider,request_id=request_id)
        if response.status_code==404:
            kind=SandboxEndpointUnavailable if endpoint else SandboxProfileUnavailable if profile else SandboxNotFound
            raise kind("OpenSandbox resource is unavailable",provider=self.provider,request_id=request_id)
        if response.status_code==429:raise SandboxRateLimited(
            "OpenSandbox rate limit reached",provider=self.provider,request_id=request_id)
        try:response.raise_for_status()
        except httpx.HTTPStatusError as exc:raise SandboxProviderUnavailable(
            "OpenSandbox request failed",provider=self.provider,request_id=request_id) from exc

    async def _fetch_endpoint(self,sandbox:Sandbox,port:int)->SandboxEndpoint:
        try:response=await self.client.get(f"/v1/sandboxes/{sandbox.id}/endpoints/{port}",
            params={"use_server_proxy":"true"})
        except (httpx.TimeoutException,httpx.TransportError) as exc:
            raise SandboxEndpointUnavailable("OpenSandbox endpoint request failed",
                provider=self.provider) from exc
        self._raise_status(response,endpoint=True);value=response.json().get("endpoint")
        if not isinstance(value,str) or not value:raise SandboxEndpointUnavailable(
            "OpenSandbox returned no endpoint",provider=self.provider)
        endpoint=SandboxEndpoint(url=value if "://" in value else "http://"+value,
            headers=dict(self.client.headers),expires_at_monotonic=time.monotonic()+60)
        self._endpoints[(sandbox.id,port)]=endpoint
        sandbox.provider_details["runtime_url"]=endpoint.url
        sandbox.provider_details["runtime_headers"]=dict(endpoint.headers)
        sandbox.runtime_url=endpoint.url;sandbox.runtime_headers=dict(endpoint.headers)
        sandbox.runtime_endpoint_refresh_at_monotonic=endpoint.expires_at_monotonic or 0
        return endpoint

    async def endpoint(self,sandbox:Sandbox,port:int,*,force_refresh:bool=False)->SandboxEndpoint:
        if not 1<=port<=65535:raise ValueError("Invalid sandbox port")
        key=(sandbox.id,port);current=self._endpoints.get(key);now=time.monotonic()
        if not force_refresh and current and (current.expires_at_monotonic is None
                or current.expires_at_monotonic-now>30):return current
        lock=self._endpoint_locks.setdefault(key,asyncio.Lock())
        async with lock:
            current=self._endpoints.get(key);now=time.monotonic()
            if not force_refresh and current and (current.expires_at_monotonic is None
                    or current.expires_at_monotonic-now>30):return current
            return await self._fetch_endpoint(sandbox,port)

    async def _ready(self,sandbox:Sandbox)->Sandbox:
        for _ in range(80):
            response=await self.client.get(f"/v1/sandboxes/{sandbox.id}")
            if response.status_code==404:raise SandboxNotFound(
                "OpenSandbox no longer has sandbox "+sandbox.id,provider=self.provider)
            self._raise_status(response);state=((response.json().get("status") or {}).get("state") or "").lower()
            if state in {"failed","terminated"}:
                sandbox.state=SandboxState.FAILED;sandbox.health_state="failed";sandbox.health_error="opensandbox_"+state
                raise SandboxProviderUnavailable("OpenSandbox failed before Runtime became ready",
                    provider=self.provider)
            if state=="running":
                try:
                    endpoint=await self.endpoint(sandbox,8080,force_refresh=True)
                    health=await self.client.get(endpoint.url.rstrip("/")+"/health",headers=dict(endpoint.headers))
                    if health.status_code==200 and health.json().get("status") in {"ok","bootstrap"}:
                        sandbox.state=SandboxState.READY;sandbox.health_state="ready";sandbox.health_error="";return sandbox
                except (httpx.HTTPError,ValueError,SandboxEndpointUnavailable):pass
            sandbox.state=SandboxState.PROVISIONING;sandbox.health_state="starting"
            await asyncio.sleep(.25)
        raise SandboxProviderUnavailable("OpenSandbox Runtime endpoint did not become ready",
            provider=self.provider)

    @observed("sandbox.create")
    async def create(self,request:SandboxCreateRequest|Harness,image:str|None=None,
                     metadata:Mapping[str,str]|None=None,
                     environment:Mapping[str,str]|None=None)->Sandbox:
        if isinstance(request,Harness):
            request=self._legacy_request(request,image or "",metadata or {},environment or {})
        if request.profile.provider!=self.provider or "@sha256:" not in request.profile.provider_ref:
            raise SandboxProfileUnavailable("OpenSandbox requires an immutable image profile",
                provider=self.provider)
        metadata={**request.metadata,**self._profile_metadata(request.profile),
            "sandbox-harness.io/namespace":self.namespace,
            "sandbox-harness.io/idempotency-key":request.idempotency_key}
        body={"image":{"uri":request.profile.provider_ref},"timeout":request.ttl_seconds,
            "resourceLimits":{"cpu":"4","memory":"8Gi"},"metadata":metadata,
            "env":dict(request.environment),"entrypoint":["sandbox-harness-runtime"]}
        try:
            response=await self.client.post("/v1/sandboxes",json=body,
                headers={"X-Request-ID":request.idempotency_key})
            if response.status_code==429:raise SandboxCapacityUnavailable(
                "OpenSandbox capacity is temporarily unavailable",provider=self.provider,
                request_id=response.headers.get("x-request-id",""))
            self._raise_status(response,profile=True)
            sandbox=self._sandbox(response.json(),request.harness,request.profile.provider_ref,metadata)
            self._profiles[sandbox.id]=request.profile
            sandbox.expires_at=datetime.now(timezone.utc)+timedelta(seconds=request.ttl_seconds)
            sandbox.expires_at_monotonic=time.monotonic()+request.ttl_seconds
            return await self._ready(sandbox)
        except (httpx.TimeoutException,httpx.TransportError) as exc:
            for attempt in range(20):
                matches=await self.find({"sandbox-harness.io/idempotency-key":request.idempotency_key})
                ready=[value for value in matches if value.state==SandboxState.READY]
                if len(ready)==1:return ready[0]
                if len(matches)>1:raise SandboxCreateOutcomeUnknown(
                    "Duplicate sandboxes found for one create request",provider=self.provider) from exc
                if attempt<19:await asyncio.sleep(.25)
            raise SandboxCreateOutcomeUnknown("OpenSandbox create outcome is unknown",
                provider=self.provider) from exc

    async def _inventory(self,metadata:Mapping[str,str])->list[Sandbox]:
        query={"sandbox-harness.io/namespace":self.namespace,**metadata};token="";rows=[]
        while True:
            params={"metadata":"&".join(f"{key}={value}" for key,value in query.items()),
                "pageSize":100}
            if token:params["pageToken"]=token
            response=await self.client.get("/v1/sandboxes",params=params);self._raise_status(response)
            payload=response.json();rows.extend(payload.get("items") or payload.get("sandboxes") or [])
            token=str(payload.get("nextPageToken") or "")
            if not token:break
        result=[]
        for row in rows:
            values=row.get("metadata") or {}
            if any(values.get(key)!=value for key,value in query.items()):continue
            try:
                harness=Harness(values.get("sandbox-harness.io/harness","codex"))
                sandbox=self._sandbox(row,harness,(row.get("image") or {}).get("uri",""),values)
                status=await self.client.get(f"/v1/sandboxes/{sandbox.id}")
                if status.status_code==404:continue
                self._raise_status(status);state=((status.json().get("status") or {}).get("state") or "").lower()
                sandbox.state=(SandboxState.READY if state=="running" else
                    SandboxState.FAILED if state in {"failed","terminated"} else SandboxState.PROVISIONING)
                sandbox.health_state=("ready" if sandbox.state==SandboxState.READY else
                    "failed" if sandbox.state==SandboxState.FAILED else "starting")
                if sandbox.state==SandboxState.READY:
                    try:
                        endpoint=await self.endpoint(sandbox,8080)
                        health=await self.client.get(endpoint.url.rstrip("/")+"/health",headers=dict(endpoint.headers))
                        if health.status_code!=200:
                            sandbox.state=SandboxState.UNHEALTHY;sandbox.health_state="unhealthy"
                    except Exception as exc:
                        sandbox.state=SandboxState.UNHEALTHY;sandbox.health_state="unhealthy"
                        sandbox.health_error=type(exc).__name__
                result.append(sandbox)
            except (ValueError,SandboxProviderUnavailable):continue
        return result

    async def find(self,metadata:Mapping[str,str])->list[Sandbox]:return await self._inventory(metadata)
    async def list(self,scope:SandboxScope)->list[Sandbox]:
        if scope.namespace!=self.namespace:return []
        return await self._inventory({})
    async def find_by_turn(self,turn_id:str)->list[Sandbox]:
        return await self.find({"sandbox-harness.io/turn":turn_id})
    async def list_namespace(self)->list[Sandbox]:return await self.list(SandboxScope(self.namespace))

    async def get(self,sandbox_id:str)->Sandbox:
        response=await self.client.get(f"/v1/sandboxes/{sandbox_id}")
        if response.status_code==404:raise SandboxNotFound(
            "OpenSandbox no longer has sandbox "+sandbox_id,provider=self.provider)
        self._raise_status(response);row=response.json();metadata=row.get("metadata") or {}
        if metadata.get("sandbox-harness.io/namespace")!=self.namespace:raise SandboxNotFound(
            "Sandbox is outside this product namespace",provider=self.provider)
        harness=Harness(metadata.get("sandbox-harness.io/harness","codex"))
        return await self._ready(self._sandbox(row,harness,(row.get("image") or {}).get("uri",""),metadata))

    async def update(self,sandbox:Sandbox,update:SandboxUpdate)->Sandbox:
        if update.metadata:
            response=await self.client.patch(f"/v1/sandboxes/{sandbox.id}/metadata",
                json=dict(update.metadata))
            if response.status_code==404:raise SandboxNotFound(
                "OpenSandbox no longer has sandbox "+sandbox.id,provider=self.provider)
            self._raise_status(response)
            for key,value in update.metadata.items():
                if value is None:sandbox.metadata.pop(key,None)
                else:sandbox.metadata[key]=value
        if update.ttl_seconds is not None:
            target=datetime.now(timezone.utc)+timedelta(seconds=update.ttl_seconds)
            response=await self.client.post(f"/v1/sandboxes/{sandbox.id}/renew-expiration",
                json={"expiresAt":target.isoformat()})
            if response.status_code==404:raise SandboxNotFound(
                "OpenSandbox no longer has sandbox "+sandbox.id,provider=self.provider)
            self._raise_status(response);sandbox.expires_at=target
            sandbox.expires_at_monotonic=time.monotonic()+update.ttl_seconds
        return sandbox

    @observed("sandbox.bind")
    async def bind(self,sandbox:Sandbox,thread_id:str,turn_id:str)->None:
        await self.update(sandbox,SandboxUpdate(metadata={"sandbox-harness.io/pool":None,
            "sandbox-harness.io/thread":thread_id,"sandbox-harness.io/turn":turn_id},
            ttl_seconds=self.timeout_seconds))
        await self.endpoint(sandbox,8080,force_refresh=True)

    @observed("sandbox.renew")
    async def renew(self,sandbox:Sandbox,*,force:bool=False)->Sandbox:
        current=datetime.now(timezone.utc)
        monotonic=time.monotonic()
        if force or (sandbox.expires_at is None and not sandbox.expires_at_monotonic) or (
                sandbox.expires_at is not None and (sandbox.expires_at-current).total_seconds()<=max(
                    60,self.timeout_seconds/2)) or (sandbox.expires_at_monotonic and
                    sandbox.expires_at_monotonic-monotonic<=max(60,self.timeout_seconds/2)):
            await self.update(sandbox,SandboxUpdate(ttl_seconds=self.timeout_seconds))
        if force or sandbox.runtime_endpoint_refresh_at_monotonic<=monotonic:
            await self.endpoint(sandbox,8080,force_refresh=True)
        return sandbox

    @observed("sandbox.delete")
    async def delete(self,sandbox:Sandbox)->None:
        response=await self.client.delete(f"/v1/sandboxes/{sandbox.id}")
        if response.status_code not in {204,404}:self._raise_status(response)
        self._profiles.pop(sandbox.id,None)
        for key in [key for key in self._endpoints if key[0]==sandbox.id]:self._endpoints.pop(key,None)
        if response.status_code==404:
            sandbox.state=SandboxState.STOPPED;sandbox.health_state="stopped";return
        for _ in range(40):
            check=await self.client.get(f"/v1/sandboxes/{sandbox.id}")
            if check.status_code==404:
                sandbox.state=SandboxState.STOPPED;sandbox.health_state="stopped";return
            await asyncio.sleep(.25)
        raise SandboxProviderUnavailable("Sandbox deletion did not converge",provider=self.provider)

    async def aclose(self)->None:await self.client.aclose()
