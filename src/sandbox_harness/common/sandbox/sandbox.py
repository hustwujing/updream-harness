from __future__ import annotations

from datetime import datetime,timedelta,timezone
from typing import Mapping

from sandbox_harness.common.contracts.domain import (Harness, identifier)
from sandbox_harness.common.observability.observability import (observed)
from sandbox_harness.common.contracts.sandbox_contracts import (Sandbox, SandboxAuthenticationFailed, SandboxBackend, SandboxCapabilities, SandboxCapacityUnavailable, SandboxCreateOutcomeUnknown, SandboxCreateRequest, SandboxEndpoint, SandboxEndpointUnavailable, SandboxHandle, SandboxNotFound, SandboxProfile, SandboxProfileUnavailable, SandboxProviderUnavailable, SandboxRateLimited, SandboxScope, SandboxRuntimeRoute, SandboxState)
from sandbox_harness.common.sandbox.sandbox_opensandbox import (OpenSandboxBackend)
from sandbox_harness.common.sandbox.sandbox_aliyun import (AliyunSandboxBackend)


class FakeSandboxBackend:
    """Explicit test backend. It is never selected as a production fallback."""
    provider="fake"
    capabilities=SandboxCapabilities(native_idempotency=True,scoped_inventory=True,
        arbitrary_ports=True,ttl_renewal=True,authenticated_runtime_http=True,
        delete_convergence=True,pause_resume=False)

    def validate_release(self,provider_ref:str,artifact_digest:str)->None:return None

    def __init__(self,runtime_url:str="http://fake-runtime",namespace:str="sandbox-harness",
                 gateway_url:str="http://127.0.0.1:29882",proxy_url:str="http://127.0.0.1:29883"):
        self.runtime_url=runtime_url;self.namespace=namespace
        self.route=SandboxRuntimeRoute(gateway_url,proxy_url)
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
                "sandbox-harness.io/idempotency-key") or identifier())[:160]
            request=SandboxCreateRequest(harness=request,profile=self.legacy_profile(request,image or ""),
                environment=environment or {},ttl_seconds=3600,allocation_id=key)
        existing=await self.reconcile_create(SandboxScope(self.namespace),request.allocation_id)
        if existing is not None:
            if (existing.harness!=request.harness
                    or existing.compatibility_key!=request.profile.compatibility_key):
                raise SandboxProfileUnavailable(
                    "Fake sandbox allocation conflicts with another profile",
                    provider=self.provider)
            return existing
        values={"sandbox-harness.io/namespace":self.namespace,
            "sandbox-harness.io/idempotency-key":request.allocation_id,
            "sandbox-harness.io/harness":request.harness.value}
        sandbox=Sandbox(id=identifier(),provider=self.provider,harness=request.harness,
            profile=request.profile,metadata=values,state=SandboxState.READY,
            expires_at=datetime.now(timezone.utc)+timedelta(seconds=request.ttl_seconds),
            provider_details={"image":request.profile.provider_ref,"runtime_url":self.runtime_url,
                "runtime_headers":{}})
        sandbox.handle=SandboxHandle(provider=self.provider,sandbox_id=sandbox.id,
            allocation_id=request.allocation_id,profile_key=request.profile.compatibility_key,
            revision=request.profile.revision)
        self.items[sandbox.id]=sandbox;return sandbox

    async def _inventory(self,metadata:Mapping[str,str])->list[Sandbox]:
        return [item for item in self.items.values()
            if item.metadata.get("sandbox-harness.io/namespace")==self.namespace
            and all(item.metadata.get(key)==value for key,value in metadata.items())]

    async def reconcile_create(self,scope:SandboxScope,allocation_id:str)->Sandbox|None:
        rows=await self._inventory({"sandbox-harness.io/idempotency-key":allocation_id})
        if scope.namespace!=self.namespace:return None
        if len(rows)>1:raise SandboxCreateOutcomeUnknown(
            "Fake backend has duplicate allocation",provider=self.provider)
        return rows[0] if rows else None

    async def list_owned(self,scope:SandboxScope)->list[Sandbox]:
        return list(self.items.values()) if scope.namespace==self.namespace else []

    async def get(self,handle:SandboxHandle|str)->Sandbox:
        if isinstance(handle,SandboxHandle):
            if handle.provider!=self.provider:raise SandboxNotFound(
                "Fake backend provider mismatch",provider=self.provider)
            sandbox_id=handle.sandbox_id
        else:sandbox_id=handle
        value=self.items.get(sandbox_id)
        if not value:raise SandboxNotFound("Fake backend no longer has sandbox "+sandbox_id,
            provider=self.provider)
        if isinstance(handle,SandboxHandle) and value.handle!=handle:
            raise SandboxProfileUnavailable("Fake sandbox handle fence mismatch",
                provider=self.provider)
        return value

    async def endpoint(self,sandbox:Sandbox,port:int,*,force_refresh:bool=False)->SandboxEndpoint:
        if sandbox.id not in self.items:raise SandboxNotFound(
            "Fake backend no longer has sandbox "+sandbox.id,provider=self.provider)
        return SandboxEndpoint(self.runtime_url.rstrip("/")+f"/ports/{port}")

    async def renew(self,sandbox:Sandbox,*,force:bool=False)->Sandbox:
        if sandbox.id not in self.items:raise SandboxNotFound(
            "Fake backend no longer has sandbox "+sandbox.id,provider=self.provider)
        self.renewed.append(sandbox.id)
        sandbox.expires_at=datetime.now(timezone.utc)+timedelta(seconds=3600)
        return sandbox

    def runtime_route(self,sandbox:Sandbox)->SandboxRuntimeRoute:return self.route

    @observed("sandbox.delete")
    async def delete(self,sandbox:Sandbox)->None:
        self.items.pop(sandbox.id,None)

    async def aclose(self)->None:return None


__all__=["AliyunSandboxBackend","FakeSandboxBackend","OpenSandboxBackend","Sandbox","SandboxBackend",
    "SandboxNotFound","SandboxCapacityUnavailable","SandboxCreateOutcomeUnknown",
    "SandboxRateLimited","SandboxAuthenticationFailed","SandboxProfileUnavailable",
    "SandboxEndpointUnavailable","SandboxProviderUnavailable"]
