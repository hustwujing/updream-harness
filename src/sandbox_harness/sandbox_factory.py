from __future__ import annotations

from dataclasses import dataclass,field
import hashlib
import json
from typing import Mapping

from .config import Settings
from .domain import Harness
from .sandbox import FakeSandboxBackend
from .sandbox_contracts import SandboxBackend,SandboxProfile
from .sandbox_contracts import (Sandbox,SandboxCreateRequest,SandboxEndpoint,SandboxHandle,
    SandboxRuntimeRoute,SandboxScope)
from .sandbox_aliyun import AliyunSandboxBackend
from .sandbox_opensandbox import OpenSandboxBackend
from .profiles import ProfileCatalogPayload,ProviderRelease


@dataclass(frozen=True)
class SandboxProfileResolver:
    provider:str
    runtime_contract:str
    runtime_bundle_digest:str
    runtime_code_bundle_digest:str
    releases:Mapping[Harness,ProviderRelease]=field(default_factory=dict)

    def resolve(self,harness:Harness,provider_ref:str,*,profile_id:str|None=None,
                revision:str|None=None)->SandboxProfile:
        provider_ref=provider_ref or f"{self.provider}:default:{harness.value}"
        release=self.releases.get(harness)
        if release and provider_ref==release.ref:revision=revision or release.revision
        return SandboxProfile(profile_id=profile_id or f"primary-{harness.value}",
            revision=revision or provider_ref,provider=self.provider,provider_ref=provider_ref,
            runtime_contract=self.runtime_contract,
            runtime_bundle_digest=self.runtime_bundle_digest or "legacy",
            runtime_code_bundle_digest=self.runtime_code_bundle_digest or "legacy")


def release_provider(provider:str)->str:
    return "opensandbox" if provider=="fake" else provider


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

    def validate_release(self,provider_ref:str,artifact_digest:str)->None:
        self.active.validate_release(provider_ref,artifact_digest)

    def backend_for(self,handle:SandboxHandle)->SandboxBackend:
        try:return self.backends[handle.provider]
        except KeyError as exc:raise ValueError(
            "sandbox_provider_not_registered:"+handle.provider) from exc

    def validate_active_providers(self,providers:set[str])->None:
        missing=providers-set(self.backends)
        if missing:raise RuntimeError("sandbox_active_provider_not_registered:"+",".join(sorted(missing)))
        if providers and providers!={self.active_provider}:
            raise RuntimeError("sandbox_provider_switch_has_active_work")

    def set_active(self,provider:str,*,active_providers:set[str]|None=None)->None:
        self.validate_active_providers(active_providers or set())
        if provider not in self.backends:raise ValueError("active_sandbox_provider_missing")
        if active_providers and provider not in active_providers:
            raise RuntimeError("sandbox_provider_switch_has_active_work")
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
        if not settings.aliyun_sandbox_gateway_url or not settings.aliyun_sandbox_proxy_url:
            raise ValueError("aliyun_sandbox_runtime_route_required")
        backend=AliyunSandboxBackend(api_key=settings.aliyun_sandbox_api_key,
            api_url=settings.aliyun_sandbox_api_url,domain=settings.aliyun_sandbox_domain,
            timeout_seconds=settings.sandbox_timeout_seconds,namespace=settings.namespace,
            request_timeout_seconds=settings.aliyun_sandbox_request_timeout_seconds,
            gateway_url=settings.aliyun_sandbox_gateway_url,
            proxy_url=settings.aliyun_sandbox_proxy_url)
        backend.capabilities.require_primary_runtime();return backend
    raise ValueError("unsupported_sandbox_backend:"+str(settings.backend))


def build_sandbox_registry(settings:Settings)->SandboxBackendRegistry:
    backend=build_sandbox_backend(settings)
    return SandboxBackendRegistry([backend],backend.provider)


def primary_provider_releases(settings:Settings,provider:str)->dict[Harness,ProviderRelease]:
    selected=release_provider(provider)
    raw=settings.profile_catalog_path.read_text();actual=hashlib.sha256(raw.encode()).hexdigest()
    if settings.profile_catalog_digest and actual!=settings.profile_catalog_digest:
        raise ValueError("profile catalog digest mismatch")
    payload=ProfileCatalogPayload.model_validate(json.loads(raw))
    try:profile=next(value for value in payload.runtime_dependency_profiles
        if value.id=="primary-full-cpu")
    except StopIteration as exc:raise ValueError("primary sandbox profile unavailable") from exc
    releases=profile.provider_releases.get(selected,{})
    if set(releases)!={harness.value for harness in Harness}:
        raise ValueError("primary provider release matrix is incomplete")
    return {Harness(harness):release for harness,release in releases.items()}


def build_profile_resolver(settings:Settings,backend:SandboxBackend,
                           releases:Mapping[Harness,ProviderRelease]|None=None)->SandboxProfileResolver:
    return SandboxProfileResolver(provider=backend.provider,
        runtime_contract="sandbox-harness-runtime/v1",
        runtime_bundle_digest=settings.runtime_bundle_digest,
        runtime_code_bundle_digest=settings.runtime_code_bundle_digest,
        releases=releases or {})
