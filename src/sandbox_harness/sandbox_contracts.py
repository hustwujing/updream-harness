from __future__ import annotations

from dataclasses import dataclass,field
from datetime import datetime
from enum import StrEnum
import hashlib
import json
import re
from types import MappingProxyType
from typing import Mapping,Protocol
from urllib.parse import urlsplit

from .domain import Harness


_METADATA_KEY=re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
_PROVIDER=re.compile(r"^[a-z][a-z0-9_-]{1,31}$")


def _mapping(value:Mapping[str,str],*,name:str,maximum:int)->Mapping[str,str]:
    rows=dict(value)
    if len(rows)>maximum:raise ValueError(f"{name}_too_many_entries")
    for key,item in rows.items():
        if not isinstance(key,str) or not isinstance(item,str):
            raise ValueError(f"{name}_must_contain_strings")
    return MappingProxyType(rows)


def _metadata(value:Mapping[str,str|None],*,allow_delete:bool=False)->Mapping[str,str|None]:
    rows=dict(value)
    if len(rows)>64:raise ValueError("metadata_too_many_entries")
    for key,item in rows.items():
        if not isinstance(key,str) or not _METADATA_KEY.fullmatch(key):
            raise ValueError("metadata_key_invalid")
        if item is None and allow_delete:continue
        if not isinstance(item,str) or len(item)>512:
            raise ValueError("metadata_value_invalid")
    return MappingProxyType(rows)


@dataclass(frozen=True)
class SandboxProfile:
    profile_id:str
    revision:str
    provider:str
    provider_ref:str
    runtime_contract:str
    runtime_bundle_digest:str
    runtime_code_bundle_digest:str
    compatibility_key:str=""

    def __post_init__(self)->None:
        values=(self.profile_id,self.revision,self.provider,self.provider_ref,self.runtime_contract,
            self.runtime_bundle_digest,self.runtime_code_bundle_digest)
        if any(not isinstance(value,str) or not value or len(value)>512 for value in values):
            raise ValueError("sandbox_profile_identity_invalid")
        if self.compatibility_key:
            if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_-]{18,61}[A-Za-z0-9])",self.compatibility_key):
                raise ValueError("sandbox_profile_compatibility_key_invalid")
            return
        payload=json.dumps(values,separators=(",",":"),ensure_ascii=True).encode()
        object.__setattr__(self,"compatibility_key",hashlib.sha256(payload).hexdigest()[:63])


@dataclass(frozen=True)
class SandboxCreateRequest:
    harness:Harness
    profile:SandboxProfile
    environment:Mapping[str,str]
    ttl_seconds:int
    allocation_id:str

    def __post_init__(self)->None:
        if not 30<=self.ttl_seconds<=86400:raise ValueError("sandbox_ttl_invalid")
        if not isinstance(self.allocation_id,str) or not 1<=len(self.allocation_id)<=160:
            raise ValueError("sandbox_allocation_id_invalid")
        object.__setattr__(self,"environment",_mapping(self.environment,name="environment",maximum=256))


@dataclass(frozen=True)
class SandboxEndpoint:
    url:str
    headers:Mapping[str,str]=field(default_factory=dict,repr=False)
    expires_at_monotonic:float|None=None

    def __post_init__(self)->None:
        parsed=urlsplit(self.url)
        if parsed.scheme not in {"http","https"} or not parsed.hostname:
            raise ValueError("sandbox_endpoint_invalid")
        if self.expires_at_monotonic is not None and self.expires_at_monotonic<=0:
            raise ValueError("sandbox_endpoint_expiry_invalid")
        object.__setattr__(self,"headers",_mapping(self.headers,name="endpoint_headers",maximum=64))

    def __iter__(self):
        # Transitional tuple compatibility for inspector callers migrated in Task 3.
        yield self.url
        yield dict(self.headers)


@dataclass(frozen=True)
class SandboxHandle:
    provider:str
    sandbox_id:str
    allocation_id:str
    profile_key:str
    revision:str

    def __post_init__(self)->None:
        if not isinstance(self.provider,str) or not _PROVIDER.fullmatch(self.provider):
            raise ValueError("sandbox_handle_provider_invalid")
        for name,value,maximum in (
            ("sandbox_id",self.sandbox_id,512),("allocation_id",self.allocation_id,160),
            ("profile_key",self.profile_key,160),("revision",self.revision,512)):
            if not isinstance(value,str) or not value or len(value)>maximum:
                raise ValueError("sandbox_handle_"+name+"_invalid")

    def to_mapping(self)->dict[str,str]:
        return {"provider":self.provider,"sandbox_id":self.sandbox_id,
            "allocation_id":self.allocation_id,"profile_key":self.profile_key,
            "revision":self.revision}

    @classmethod
    def from_mapping(cls,value:Mapping[str,str])->"SandboxHandle":
        return cls(provider=value.get("provider",""),sandbox_id=value.get("sandbox_id",""),
            allocation_id=value.get("allocation_id",""),profile_key=value.get("profile_key",""),
            revision=value.get("revision",""))


@dataclass(frozen=True)
class SandboxRuntimeRoute:
    gateway_url:str
    proxy_url:str

    def __post_init__(self)->None:
        for value in (self.gateway_url,self.proxy_url):
            parsed=urlsplit(value)
            if (parsed.scheme not in {"http","https"} or not parsed.hostname or not parsed.port
                    or parsed.username or parsed.password or parsed.path not in {"","/"}
                    or parsed.query or parsed.fragment):
                raise ValueError("sandbox_runtime_route_invalid")


class SandboxState(StrEnum):
    INVENTORY="inventory"
    PROVISIONING="provisioning"
    STARTING="starting"
    READY="ready"
    UNHEALTHY="unhealthy"
    STOPPING="stopping"
    STOPPED="stopped"
    MISSING="missing"
    FAILED="failed"
    UNKNOWN="unknown"


@dataclass
class Sandbox:
    id:str
    harness:Harness
    metadata:dict[str,str]
    provider:str="legacy"
    profile:SandboxProfile|None=None
    state:SandboxState=SandboxState.READY
    expires_at:datetime|None=None
    provider_details:dict[str,str]=field(default_factory=dict,repr=False)
    handle:SandboxHandle|None=None
    warm:bool=False
    thread_id:str=""
    turn_id:str=""
    reused:bool=False
    health_error:str=""
    health_state:str="ready"
    # Transitional fields for callers migrated in the next protocol stage.
    image:str=""
    runtime_url:str=""
    runtime_headers:dict[str,str]=field(default_factory=dict,repr=False)
    expires_at_monotonic:float=0
    runtime_endpoint_refresh_at_monotonic:float=0

    def __post_init__(self)->None:
        if self.profile is None:
            reference=self.image or "legacy"
            revision=("sha256:"+reference.rsplit("@sha256:",1)[1]
                if "@sha256:" in reference else reference)
            self.profile=SandboxProfile(profile_id=f"legacy-{self.harness.value}",revision=revision,
                provider=self.provider,provider_ref=reference,
                runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="legacy",
                runtime_code_bundle_digest="legacy")
        elif not self.image:self.image=self.profile.provider_ref
        if self.handle is None:
            allocation=str(self.metadata.get("sandbox-harness.io/idempotency-key")
                or self.turn_id or self.id)
            self.handle=SandboxHandle(provider=self.provider,sandbox_id=self.id,
                allocation_id=allocation,profile_key=self.profile.compatibility_key,
                revision=self.profile.revision)
        elif self.handle.sandbox_id!=self.id or self.handle.provider!=self.provider:
            raise ValueError("sandbox_handle_identity_mismatch")
        if self.state!=SandboxState.READY and self.health_state=="ready":
            self.health_state=self.state.value
        elif self.state==SandboxState.READY and self.health_state!="ready":
            try:self.state=SandboxState(self.health_state)
            except ValueError:self.state=SandboxState.UNKNOWN

    @property
    def compatibility_key(self)->str:return self.profile.compatibility_key

@dataclass(frozen=True)
class SandboxCapabilities:
    native_idempotency:bool
    scoped_inventory:bool
    arbitrary_ports:bool
    ttl_renewal:bool
    authenticated_runtime_http:bool
    delete_convergence:bool
    pause_resume:bool

    def require_primary_runtime(self)->None:
        required=("scoped_inventory","arbitrary_ports","ttl_renewal",
            "authenticated_runtime_http","delete_convergence")
        missing=[name for name in required if not getattr(self,name)]
        if missing:raise ValueError("sandbox_provider_missing_capabilities:"+",".join(missing))


@dataclass(frozen=True)
class SandboxScope:
    namespace:str


class SandboxProviderError(RuntimeError):
    code="sandbox_provider_error"
    retryable=False

    def __init__(self,message:str,*,provider:str="",request_id:str=""):
        super().__init__(message);self.provider=provider;self.request_id=request_id

    @property
    def error_code(self)->str:return self.code


class SandboxNotFound(SandboxProviderError):code="sandbox_not_found"
class SandboxCapacityUnavailable(SandboxProviderError):
    code="infrastructure_capacity_wait";retryable=True
class SandboxRateLimited(SandboxProviderError):code="sandbox_rate_limited";retryable=True
class SandboxAuthenticationFailed(SandboxProviderError):code="sandbox_authentication_failed"
class SandboxProfileUnavailable(SandboxProviderError):code="sandbox_profile_unavailable"
class SandboxEndpointUnavailable(SandboxProviderError):
    code="sandbox_endpoint_unavailable";retryable=True
class SandboxProviderUnavailable(SandboxProviderError):
    code="sandbox_provider_unavailable";retryable=True
class SandboxCreateOutcomeUnknown(SandboxProviderError):code="sandbox_create_outcome_unknown"


class SandboxBackend(Protocol):
    provider:str
    capabilities:SandboxCapabilities
    async def create(self,request:SandboxCreateRequest)->Sandbox:...
    async def get(self,handle:SandboxHandle)->Sandbox:...
    async def reconcile_create(self,scope:SandboxScope,allocation_id:str)->Sandbox|None:...
    async def list_owned(self,scope:SandboxScope)->list[Sandbox]:...
    async def renew(self,sandbox:Sandbox,*,force:bool=False)->Sandbox:...
    async def delete(self,sandbox:Sandbox)->None:...
    async def endpoint(self,sandbox:Sandbox,port:int,*,force_refresh:bool=False)->SandboxEndpoint:...
    def runtime_route(self,sandbox:Sandbox)->SandboxRuntimeRoute:...
    async def aclose(self)->None:...
