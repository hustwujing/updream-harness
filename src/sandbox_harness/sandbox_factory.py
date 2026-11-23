from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .domain import Harness
from .sandbox import FakeSandboxBackend
from .sandbox_contracts import SandboxBackend,SandboxProfile
from .sandbox_contracts import (Sandbox,SandboxCreateRequest,SandboxEndpoint,SandboxHandle,
    SandboxRuntimeRoute,SandboxScope)
from .sandbox_aliyun import AliyunSandboxBackend
from .sandbox_opensandbox import OpenSandboxBackend


@dataclass(frozen=True)
class SandboxProfileResolver:
    provider:str
    runtime_contract:str
    runtime_bundle_digest:str
    runtime_code_bundle_digest:str

    def resolve(self,harness:Harness,provider_ref:str,*,profile_id:str|None=None,
                revision:str|None=None)->SandboxProfile:
        provider_ref=provider_ref or f"{self.provider}:default:{harness.value}"
        if revision is None and "@sha256:" in provider_ref:
            revision="sha256:"+provider_ref.rsplit("@sha256:",1)[1]
        return SandboxProfile(profile_id=profile_id or f"primary-{harness.value}",
            revision=revision or provider_ref,provider=self.provider,provider_ref=provider_ref,
            runtime_contract=self.runtime_contract,
            runtime_bundle_digest=self.runtime_bundle_digest or "legacy",
            runtime_code_bundle_digest=self.runtime_code_bundle_digest or "legacy")


class SandboxBackendRegistry:
    def __init__(self,backends:list[SandboxBackend],active_provider:str):
        self.backends={backend.provider:backend for backend in backends}
        if len(self.backends)!=len(backends):raise ValueError("duplicate_sandbox_provider")
        if active_provider not in self.backends:raise ValueError("active_sandbox_provider_missing")
        namespaces={getattr(backend,"namespace","") for backend in backends}
        if len(namespaces)!=1:raise ValueError("sandbox_provider_namespace_mismatch")
        self.active_provider=active_provider;self.namespace=namespaces.pop()

    @property
    def active(self)->SandboxBackend:return self.backends[self.active_provider]
    @property
    def provider(self)->str:return self.active_provider
    @property
    def capabilities(self):return self.active.capabilities

    def backend_for(self,handle:SandboxHandle)->SandboxBackend:
        try:return self.backends[handle.provider]
        except KeyError as exc:raise ValueError(
            "sandbox_provider_not_registered:"+handle.provider) from exc

    def set_active(self,provider:str,*,active_handles:int=0)->None:
        if active_handles:raise RuntimeError("sandbox_provider_switch_has_active_work")
        if provider not in self.backends:raise ValueError("active_sandbox_provider_missing")
        self.active_provider=provider

    async def create(self,request:SandboxCreateRequest)->Sandbox:
        if request.profile.provider!=self.active_provider:
            raise ValueError("sandbox_create_provider_not_active")
        return await self.active.create(request)

    async def get(self,handle:SandboxHandle)->Sandbox:
        if not isinstance(handle,SandboxHandle):raise TypeError("sandbox_handle_required")
        return await self.backend_for(handle).get(handle)

    async def reconcile_create(self,scope:SandboxScope,allocation_id:str)->Sandbox|None:
        return await self.active.reconcile_create(scope,allocation_id)

    async def list_owned(self,scope:SandboxScope)->list[Sandbox]:
        rows=[]
        for backend in self.backends.values():rows.extend(await backend.list_owned(scope))
        return rows

    async def renew(self,sandbox:Sandbox,*,force:bool=False)->Sandbox:
        return await self.backend_for(sandbox.handle).renew(sandbox,force=force)

    async def delete(self,sandbox:Sandbox)->None:
        await self.backend_for(sandbox.handle).delete(sandbox)

    async def endpoint(self,sandbox:Sandbox,port:int,*,force_refresh:bool=False)->SandboxEndpoint:
        return await self.backend_for(sandbox.handle).endpoint(
            sandbox,port,force_refresh=force_refresh)

    def runtime_route(self,sandbox:Sandbox)->SandboxRuntimeRoute:
        return self.backend_for(sandbox.handle).runtime_route(sandbox)

    async def aclose(self)->None:
        for backend in self.backends.values():await backend.aclose()


def build_sandbox_backend(settings:Settings)->SandboxBackend:
    if settings.backend=="fake":return FakeSandboxBackend(namespace=settings.namespace)
    if settings.backend=="opensandbox":
        backend=OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,
            settings.sandbox_timeout_seconds,settings.namespace,settings.gateway_url,
            settings.gateway_proxy_port)
        backend.capabilities.require_primary_runtime();return backend
    if settings.backend=="aliyun":
        backend=AliyunSandboxBackend(api_key=settings.aliyun_sandbox_api_key,
            api_url=settings.aliyun_sandbox_api_url,domain=settings.aliyun_sandbox_domain,
            timeout_seconds=settings.sandbox_timeout_seconds,namespace=settings.namespace,
            request_timeout_seconds=settings.aliyun_sandbox_request_timeout_seconds)
        backend.capabilities.require_primary_runtime();return backend
    raise ValueError("unsupported_sandbox_backend:"+str(settings.backend))


def build_sandbox_registry(settings:Settings)->SandboxBackendRegistry:
    backend=build_sandbox_backend(settings)
    return SandboxBackendRegistry([backend],backend.provider)


def build_profile_resolver(settings:Settings,backend:SandboxBackend)->SandboxProfileResolver:
    return SandboxProfileResolver(provider=backend.provider,
        runtime_contract="sandbox-harness-runtime/v1",
        runtime_bundle_digest=settings.runtime_bundle_digest,
        runtime_code_bundle_digest=settings.runtime_code_bundle_digest)
