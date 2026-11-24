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
    SandboxHandle,
    SandboxProfile,
    SandboxProfileUnavailable,
    SandboxProviderUnavailable,
    SandboxRateLimited,
    SandboxRuntimeRoute,
    Sandbox,
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


def test_create_request_validates_allocation_and_bounded_environment():
    request=SandboxCreateRequest(harness=Harness.CODEX,profile=profile(),
        environment={"MODE":"test"},ttl_seconds=300,allocation_id="turn-1")
    assert request.allocation_id=="turn-1"
    assert request.environment["MODE"]=="test"
    with pytest.raises(ValueError,match="allocation"):
        SandboxCreateRequest(harness=Harness.CODEX,profile=profile(),environment={},
            ttl_seconds=300,allocation_id="x"*161)
    with pytest.raises(ValueError,match="environment"):
        SandboxCreateRequest(harness=Harness.CODEX,profile=profile(),environment={
            f"KEY_{index}":"value" for index in range(257)},ttl_seconds=300,
            allocation_id="turn-2")


def test_endpoint_repr_redacts_headers():
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
    capabilities=SandboxCapabilities(native_idempotency=False,scoped_inventory=True,
        arbitrary_ports=True,ttl_renewal=True,authenticated_runtime_http=True,
        delete_convergence=True,pause_resume=False)
    capabilities.require_primary_runtime()


def test_sandbox_handle_round_trips_provider_identity():
    handle=SandboxHandle(provider="aliyun",sandbox_id="sbx-1",allocation_id="turn-1",
        profile_key="a"*63,revision="template-build-1")
    assert SandboxHandle.from_mapping(handle.to_mapping())==handle
    with pytest.raises(ValueError,match="provider"):
        SandboxHandle(provider="Ali Yun",sandbox_id="sbx",allocation_id="turn",
            profile_key="a"*63,revision="v1")


def test_sandbox_default_handle_does_not_depend_on_provider_metadata():
    sandbox=Sandbox(id="sbx-1",provider="aliyun",harness=Harness.CODEX,profile=profile(
        provider="aliyun",provider_ref="template-1",revision="build-1"),metadata={
            "sandbox-harness.io/idempotency-key":"provider-tag-value"})
    assert sandbox.handle.allocation_id=="sbx-1"


def test_runtime_route_validates_sandbox_visible_origins():
    route=SandboxRuntimeRoute(gateway_url="http://10.194.49.8:29882",
        proxy_url="http://10.194.49.8:29883")
    assert route.gateway_url.endswith(":29882")
    with pytest.raises(ValueError,match="route"):
        SandboxRuntimeRoute(gateway_url="http://user:secret@host:1",proxy_url="http://host:2")


def test_primary_runtime_requires_convergent_delete_not_native_idempotency():
    capabilities=SandboxCapabilities(native_idempotency=False,scoped_inventory=True,
        arbitrary_ports=True,ttl_renewal=True,authenticated_runtime_http=True,
        delete_convergence=False,pause_resume=False)
    with pytest.raises(ValueError,match="delete_convergence"):
        capabilities.require_primary_runtime()
