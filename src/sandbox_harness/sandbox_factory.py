from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .domain import Harness
from .sandbox import FakeSandboxBackend
from .sandbox_contracts import SandboxBackend,SandboxProfile
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
        return SandboxProfile(profile_id=profile_id or f"primary-{harness.value}",
            revision=revision or provider_ref,provider=self.provider,provider_ref=provider_ref,
            runtime_contract=self.runtime_contract,
            runtime_bundle_digest=self.runtime_bundle_digest or "legacy",
            runtime_code_bundle_digest=self.runtime_code_bundle_digest or "legacy")


def build_sandbox_backend(settings:Settings)->SandboxBackend:
    if settings.backend=="fake":return FakeSandboxBackend(namespace=settings.namespace)
    if settings.backend=="opensandbox":
        backend=OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,
            settings.sandbox_timeout_seconds,settings.namespace,settings.gateway_url,
            settings.gateway_proxy_port)
        backend.capabilities.require_primary_runtime();return backend
    raise ValueError("unsupported_sandbox_backend:"+str(settings.backend))


def build_profile_resolver(settings:Settings,backend:SandboxBackend)->SandboxProfileResolver:
    return SandboxProfileResolver(provider=backend.provider,
        runtime_contract="sandbox-harness-runtime/v1",
        runtime_bundle_digest=settings.runtime_bundle_digest,
        runtime_code_bundle_digest=settings.runtime_code_bundle_digest)
