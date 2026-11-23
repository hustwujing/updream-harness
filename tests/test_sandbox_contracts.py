from dataclasses import FrozenInstanceError

import pytest

from sandbox_harness.domain import Harness
from sandbox_harness.sandbox_contracts import (
    SandboxAuthenticationFailed,
    SandboxCapacityUnavailable,
    SandboxCreateOutcomeUnknown,
    SandboxCreateRequest,
    SandboxCapabilities,
    SandboxEndpoint,
    SandboxEndpointUnavailable,
    SandboxProfile,
    SandboxProfileUnavailable,
    SandboxProviderUnavailable,
    SandboxRateLimited,
    SandboxUpdate,
)


def profile(**changes):
    values={
        "profile_id":"primary-full-cpu-codex",
        "revision":"sha256:"+"1"*64,
        "provider":"opensandbox",
        "provider_ref":"registry/runtime@sha256:"+"2"*64,
        "runtime_contract":"sandbox-harness-runtime/v1",
        "runtime_bundle_digest":"3"*64,
        "runtime_code_bundle_digest":"4"*64,
    }
    values.update(changes)
    return SandboxProfile(**values)


def test_profile_compatibility_key_covers_provider_and_runtime_identity():
    original=profile()
    assert original.compatibility_key==profile().compatibility_key
    assert original.compatibility_key!=profile(provider="aliyun").compatibility_key
    assert original.compatibility_key!=profile(provider_ref="template-version-2").compatibility_key
    assert original.compatibility_key!=profile(runtime_code_bundle_digest="5"*64).compatibility_key
    with pytest.raises(FrozenInstanceError):
        original.provider_ref="changed"


def test_create_request_validates_idempotency_and_bounded_metadata():
    request=SandboxCreateRequest(harness=Harness.CODEX,profile=profile(),
        environment={"MODE":"test"},metadata={"sandbox-harness.io/turn":"turn-1"},
        ttl_seconds=300,idempotency_key="turn-1")
    assert request.metadata["sandbox-harness.io/turn"]=="turn-1"
    with pytest.raises(ValueError,match="idempotency"):
        SandboxCreateRequest(harness=Harness.CODEX,profile=profile(),environment={},metadata={},
            ttl_seconds=300,idempotency_key="x"*65)
    with pytest.raises(ValueError,match="metadata"):
        SandboxCreateRequest(harness=Harness.CODEX,profile=profile(),environment={},
            metadata={f"key-{index}":"value" for index in range(65)},ttl_seconds=300,
            idempotency_key="turn-2")


def test_update_none_removes_metadata_and_endpoint_repr_redacts_headers():
    update=SandboxUpdate(metadata={"sandbox-harness.io/pool":None,
        "sandbox-harness.io/turn":"turn-2"},ttl_seconds=600)
    assert update.metadata["sandbox-harness.io/pool"] is None
    endpoint=SandboxEndpoint(url="https://runtime.example",headers={"Authorization":"secret"},
        expires_at_monotonic=123.0)
    assert endpoint.headers["Authorization"]=="secret"
    assert "secret" not in repr(endpoint) and "Authorization" not in repr(endpoint)


@pytest.mark.parametrize(("kind","retryable"),[
    (SandboxCapacityUnavailable,True),(SandboxRateLimited,True),
    (SandboxProviderUnavailable,True),(SandboxEndpointUnavailable,True),
    (SandboxAuthenticationFailed,False),(SandboxProfileUnavailable,False),
    (SandboxCreateOutcomeUnknown,False),
])
def test_provider_errors_publish_uniform_action_metadata(kind,retryable):
    error=kind("failure",provider="opensandbox",request_id="request-1")
    assert error.provider=="opensandbox" and error.request_id=="request-1"
    assert error.retryable is retryable and error.code


def test_primary_runtime_allows_immutable_provider_metadata():
    capabilities=SandboxCapabilities(metadata_update=False,metadata_filter=True,
        arbitrary_ports=True,ttl_renewal=True,idempotent_create=True,
        authenticated_runtime_http=True)
    capabilities.require_primary_runtime()
