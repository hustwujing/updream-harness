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
    compatibility_key:str=field(init=False)

    def __post_init__(self)->None:
        values=(self.profile_id,self.revision,self.provider,self.provider_ref,self.runtime_contract,
            self.runtime_bundle_digest,self.runtime_code_bundle_digest)
        if any(not isinstance(value,str) or not value or len(value)>512 for value in values):
            raise ValueError("sandbox_profile_identity_invalid")
        payload=json.dumps(values,separators=(",",":"),ensure_ascii=True).encode()
        object.__setattr__(self,"compatibility_key",hashlib.sha256(payload).hexdigest())


@dataclass(frozen=True)
class SandboxCreateRequest:
    harness:Harness
    profile:SandboxProfile
    environment:Mapping[str,str]
    metadata:Mapping[str,str]
    ttl_seconds:int
    idempotency_key:str

    def __post_init__(self)->None:
        if not 30<=self.ttl_seconds<=86400:raise ValueError("sandbox_ttl_invalid")
        if not isinstance(self.idempotency_key,str) or not 1<=len(self.idempotency_key)<=64:
            raise ValueError("sandbox_idempotency_key_invalid")
        object.__setattr__(self,"environment",_mapping(self.environment,name="environment",maximum=256))
        object.__setattr__(self,"metadata",_metadata(self.metadata))


@dataclass(frozen=True)
class SandboxUpdate:
    metadata:Mapping[str,str|None]=field(default_factory=dict)
    ttl_seconds:int|None=None

    def __post_init__(self)->None:
        if self.ttl_seconds is not None and not 30<=self.ttl_seconds<=86400:
            raise ValueError("sandbox_ttl_invalid")
        object.__setattr__(self,"metadata",_metadata(self.metadata,allow_delete=True))


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
            self.profile=SandboxProfile(profile_id=f"legacy-{self.harness.value}",revision=reference,
                provider=self.provider,provider_ref=reference,
                runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="legacy",
                runtime_code_bundle_digest="legacy")
        elif not self.image:self.image=self.profile.provider_ref
        if self.state!=SandboxState.READY and self.health_state=="ready":
            self.health_state=self.state.value
        elif self.state==SandboxState.READY and self.health_state!="ready":
            try:self.state=SandboxState(self.health_state)
            except ValueError:self.state=SandboxState.UNKNOWN

    @property
    def compatibility_key(self)->str:return self.profile.compatibility_key

@dataclass(frozen=True)
class SandboxCapabilities:
    metadata_update:bool
    metadata_filter:bool
    arbitrary_ports:bool
    ttl_renewal:bool
    idempotent_create:bool
    authenticated_runtime_http:bool

    def require_primary_runtime(self)->None:
        missing=[name for name,value in vars(self).items() if not value]
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
    async def get(self,sandbox_id:str)->Sandbox:...
    async def find(self,metadata:Mapping[str,str])->list[Sandbox]:...
    async def list(self,scope:SandboxScope)->list[Sandbox]:...
    async def update(self,sandbox:Sandbox,update:SandboxUpdate)->Sandbox:...
    async def renew(self,sandbox:Sandbox,*,force:bool=False)->Sandbox:...
    async def delete(self,sandbox:Sandbox)->None:...
    async def endpoint(self,sandbox:Sandbox,port:int,*,force_refresh:bool=False)->SandboxEndpoint:...
    async def aclose(self)->None:...
