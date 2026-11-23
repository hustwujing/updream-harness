from __future__ import annotations

from datetime import datetime,timedelta,timezone
from typing import Mapping

from .domain import Harness,identifier
from .observability import observed
from .sandbox_contracts import (
    Sandbox,SandboxAuthenticationFailed,SandboxBackend,SandboxCapabilities,
    SandboxCapacityUnavailable,SandboxCreateOutcomeUnknown,SandboxCreateRequest,
    SandboxEndpoint,SandboxEndpointUnavailable,SandboxNotFound,SandboxProfile,
    SandboxProfileUnavailable,SandboxProviderUnavailable,SandboxRateLimited,SandboxScope,
    SandboxState,SandboxUpdate)
from .sandbox_opensandbox import OpenSandboxBackend
from .sandbox_aliyun import AliyunSandboxBackend


class FakeSandboxBackend:
    """Explicit test backend. It is never selected as a production fallback."""
    provider="fake"
    capabilities=SandboxCapabilities(metadata_update=True,metadata_filter=True,
        arbitrary_ports=True,ttl_renewal=True,idempotent_create=True,
        authenticated_runtime_http=True)

    def __init__(self,runtime_url:str="http://fake-runtime",namespace:str="sandbox-harness"):
        self.runtime_url=runtime_url;self.namespace=namespace
        self.items:dict[str,Sandbox]={};self.renewed:list[str]=[]

    @staticmethod
    def legacy_profile(harness:Harness,image:str)->SandboxProfile:
        value=image or f"fake:default:{harness.value}"
        return SandboxProfile(profile_id=f"primary-{harness.value}",revision=value,provider="fake",
            provider_ref=value,runtime_contract="sandbox-harness-runtime/v1",
            runtime_bundle_digest="legacy",runtime_code_bundle_digest="legacy")

    @observed("sandbox.create")
    async def create(self,request:SandboxCreateRequest|Harness,image:str|None=None,
                     metadata:Mapping[str,str]|None=None,
                     environment:Mapping[str,str]|None=None)->Sandbox:
        if isinstance(request,Harness):
            values=dict(metadata or {});key=str(values.get(
                "sandbox-harness.io/idempotency-key") or identifier())[:64]
            request=SandboxCreateRequest(harness=request,profile=self.legacy_profile(request,image or ""),
                environment=environment or {},metadata=values,ttl_seconds=3600,
                idempotency_key=key)
        values={**request.metadata,"sandbox-harness.io/namespace":self.namespace,
            "sandbox-harness.io/idempotency-key":request.idempotency_key,
            "sandbox-harness.io/harness":request.harness.value}
        sandbox=Sandbox(id=identifier(),provider=self.provider,harness=request.harness,
            profile=request.profile,metadata=values,state=SandboxState.READY,
            expires_at=datetime.now(timezone.utc)+timedelta(seconds=request.ttl_seconds),
            provider_details={"image":request.profile.provider_ref,"runtime_url":self.runtime_url,
                "runtime_headers":{}})
        self.items[sandbox.id]=sandbox;return sandbox

    async def find(self,metadata:Mapping[str,str])->list[Sandbox]:
        return [item for item in self.items.values()
            if item.metadata.get("sandbox-harness.io/namespace")==self.namespace
            and all(item.metadata.get(key)==value for key,value in metadata.items())]

    async def list(self,scope:SandboxScope)->list[Sandbox]:
        return list(self.items.values()) if scope.namespace==self.namespace else []
    async def list_namespace(self)->list[Sandbox]:return await self.list(SandboxScope(self.namespace))

    async def get(self,sandbox_id:str)->Sandbox:
        value=self.items.get(sandbox_id)
        if not value:raise SandboxNotFound("Fake backend no longer has sandbox "+sandbox_id,
            provider=self.provider)
        return value

    async def endpoint(self,sandbox:Sandbox,port:int,*,force_refresh:bool=False)->SandboxEndpoint:
        if sandbox.id not in self.items:raise SandboxNotFound(
            "Fake backend no longer has sandbox "+sandbox.id,provider=self.provider)
        return SandboxEndpoint(self.runtime_url.rstrip("/")+f"/ports/{port}")

    async def update(self,sandbox:Sandbox,update:SandboxUpdate)->Sandbox:
        if sandbox.id not in self.items:raise SandboxNotFound(
            "Fake backend no longer has sandbox "+sandbox.id,provider=self.provider)
        for key,value in update.metadata.items():
            if value is None:sandbox.metadata.pop(key,None)
            else:sandbox.metadata[key]=value
        if update.ttl_seconds is not None:
            sandbox.expires_at=datetime.now(timezone.utc)+timedelta(seconds=update.ttl_seconds)
        return sandbox

    async def renew(self,sandbox:Sandbox,*,force:bool=False)->Sandbox:
        if sandbox.id not in self.items:raise SandboxNotFound(
            "Fake backend no longer has sandbox "+sandbox.id,provider=self.provider)
        self.renewed.append(sandbox.id);return sandbox

    @observed("sandbox.delete")
    async def delete(self,sandbox:Sandbox)->None:
        self.items.pop(sandbox.id,None)

    async def aclose(self)->None:return None


__all__=["AliyunSandboxBackend","FakeSandboxBackend","OpenSandboxBackend","Sandbox","SandboxBackend",
    "SandboxNotFound","SandboxCapacityUnavailable","SandboxCreateOutcomeUnknown",
    "SandboxRateLimited","SandboxAuthenticationFailed","SandboxProfileUnavailable",
    "SandboxEndpointUnavailable","SandboxProviderUnavailable"]
