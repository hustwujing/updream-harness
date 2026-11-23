import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from sandbox_harness.domain import Harness
from sandbox_harness.sandbox_aliyun import AliyunSandboxBackend,_E2BClient
from sandbox_harness.sandbox_contracts import (
    SandboxCreateOutcomeUnknown,
    SandboxCreateRequest,
    SandboxEndpointUnavailable,
    SandboxNotFound,
    SandboxProfile,
    SandboxProfileUnavailable,
    SandboxProviderUnavailable,
    SandboxScope,
)


def creation() -> SandboxCreateRequest:
    profile = SandboxProfile(
        profile_id="primary-full-cpu-codex",
        revision="template-release-1",
        provider="aliyun",
        provider_ref="template-1",
        runtime_contract="sandbox-harness-runtime/v1",
        runtime_bundle_digest="1" * 64,
        runtime_code_bundle_digest="2" * 64,
    )
    return SandboxCreateRequest(
        harness=Harness.CODEX,
        profile=profile,
        environment={"MODE": "test"},
        ttl_seconds=300,
        allocation_id="turn-1",
    )


class FakePaginator:
    def __init__(self, pages):
        self.pages = list(pages)

    @property
    def has_next(self):
        return bool(self.pages)

    async def next_items(self):
        return self.pages.pop(0)


class FakeAliyunClient:
    def __init__(self):
        self.items = {}
        self.create_calls = []
        self.list_calls = []
        self.timeout_calls = []
        self.kill_calls = []
        self.create_error = None
        self.accept_before_error = False
        self.traffic_token="traffic-secret"
        self.hide_inventory=False
        self.kill_removes=True

    @staticmethod
    def _info(sandbox_id, template, metadata, *, state="running"):
        now = datetime.now(timezone.utc)
        return SimpleNamespace(
            sandbox_id=sandbox_id,
            template_id=template,
            metadata=dict(metadata),
            state=state,
            started_at=now,
            end_at=now + timedelta(seconds=300),
        )

    async def create(self, *, template, timeout, metadata, envs):
        self.create_calls.append((template, timeout, dict(metadata), dict(envs)))
        info = self._info("sandbox-1", template, metadata)
        if not self.create_error or self.accept_before_error:
            self.items[info.sandbox_id] = info
        if self.create_error:
            raise self.create_error
        return SimpleNamespace(
            sandbox_id=info.sandbox_id,
            traffic_access_token=self.traffic_token,
            get_host=lambda port: f"{port}-{info.sandbox_id}.sandbox.aliyuncs.com",
        )

    async def get_info(self, sandbox_id):
        if sandbox_id not in self.items:
            raise KeyError(sandbox_id)
        return self.items[sandbox_id]

    async def connect(self, sandbox_id):
        info=await self.get_info(sandbox_id)
        if info.state=="paused":info.state="running"
        return SimpleNamespace(
            sandbox_id=sandbox_id,
            traffic_access_token=self.traffic_token,
            get_host=lambda port: f"{port}-{sandbox_id}.sandbox.aliyuncs.com",
        )

    def list(self, metadata):
        self.list_calls.append(dict(metadata))
        rows = [] if self.hide_inventory else list(self.items.values())
        midpoint = min(1, len(rows))
        return FakePaginator([rows[:midpoint], rows[midpoint:]])

    async def set_timeout(self, sandbox_id, timeout):
        info = await self.get_info(sandbox_id)
        info.end_at = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        self.timeout_calls.append((sandbox_id, timeout))

    async def kill(self, sandbox_id):
        self.kill_calls.append(sandbox_id)
        if not self.kill_removes:return sandbox_id in self.items
        return self.items.pop(sandbox_id, None) is not None


def backend(client, transport=None):
    return AliyunSandboxBackend(
        api_key="test-key",
        api_url="https://api.cn-shanghai.sandbox.aliyuncs.com",
        domain="cn-shanghai.sandbox.aliyuncs.com",
        timeout_seconds=600,
        namespace="ali-test",
        client=client,
        health_transport=transport,
        reconciliation_attempts=1,
        reconciliation_interval=0,
    )


def health_transport(request: httpx.Request) -> httpx.Response:
    assert request.headers["e2b-traffic-access-token"] == "traffic-secret"
    return httpx.Response(200, json={"status": "ok"})


@pytest.mark.asyncio
async def test_e2b_client_forces_authenticated_runtime_ingress(monkeypatch):
    from e2b import AsyncSandbox
    create=AsyncMock(return_value=SimpleNamespace(sandbox_id="sandbox-1"))
    monkeypatch.setattr(AsyncSandbox,"create",create)
    client=_E2BClient(api_key="e2b_"+"1"*40,api_url="https://api.sandbox.test",
        domain="sandbox.test",request_timeout_seconds=10)
    await client.create(template="template",timeout=300,metadata={},envs={})
    assert create.await_args.kwargs["secure"] is True
    assert create.await_args.kwargs["network"]=={"allow_public_traffic":False}


@pytest.mark.asyncio
async def test_aliyun_lifecycle_maps_template_metadata_ttl_endpoint_and_delete():
    client = FakeAliyunClient()
    value = backend(client, httpx.MockTransport(health_transport))
    try:
        sandbox = await value.create(creation())
        sent = client.create_calls[0]
        assert sent[0:2] == ("template-1", 300)
        assert sent[2]["sandbox-harness.io/namespace"] == "ali-test"
        assert sent[2]["sandbox-harness.io/idempotency-key"] == "turn-1"
        assert sent[2]["sandbox-harness.io/profile-key"] == creation().profile.compatibility_key
        assert sent[3] == {"MODE": "test"}
        endpoint = await value.endpoint(sandbox, 8080)
        assert endpoint.url == "https://8080-sandbox-1.sandbox.aliyuncs.com"
        assert endpoint.headers == {"e2b-traffic-access-token": "traffic-secret"}

        sandbox = await value.renew(sandbox, force=True)
        assert client.timeout_calls == [("sandbox-1", 600)]
        assert sandbox.expires_at > datetime.now(timezone.utc)
        reconciled=await value.reconcile_create(SandboxScope("ali-test"),"turn-1")
        assert reconciled and reconciled.id=="sandbox-1"
        assert [row.id for row in await value.list_owned(SandboxScope("ali-test"))] == ["sandbox-1"]

        await value.delete(sandbox)
        await value.delete(sandbox)
        assert client.kill_calls == ["sandbox-1"]
        assert await value.list_owned(SandboxScope("ali-test")) == []
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_exhausts_pages_and_exactly_filters_provider_results():
    client = FakeAliyunClient()
    wanted = creation()
    values = {
        "sandbox-harness.io/namespace": "ali-test",
        "sandbox-harness.io/idempotency-key":"turn-1",
        "sandbox-harness.io/harness": "codex",
        "sandbox-harness.io/profile": wanted.profile.profile_id,
        "sandbox-harness.io/profile-key": wanted.profile.compatibility_key,
    }
    client.items["one"] = client._info("one", "template-1", values)
    client.items["other"] = client._info("other", "template-1", {
        **values,"sandbox-harness.io/idempotency-key":"other"})
    value = backend(client, httpx.MockTransport(health_transport))
    try:
        row = await value.reconcile_create(SandboxScope("ali-test"),"turn-1")
        assert row and row.id == "one"
        assert client.list_calls == [{
            "sandbox-harness.io/namespace": "ali-test",
            "sandbox-harness.io/idempotency-key":"turn-1",
        }]
        assert await value.list_owned(SandboxScope("another-product"))==[]
        assert len(client.list_calls)==1
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_contract_does_not_expose_metadata_mutation():
    client = FakeAliyunClient()
    value = backend(client, httpx.MockTransport(health_transport))
    try:
        sandbox = await value.create(creation())
        assert not hasattr(value,"update")
        assert sandbox.handle and sandbox.handle.allocation_id=="turn-1"
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_reconciles_accepted_create_error_without_replaying():
    client = FakeAliyunClient()
    client.create_error = TimeoutError("response lost")
    client.accept_before_error = True
    value = backend(client, httpx.MockTransport(health_transport))
    try:
        sandbox = await value.create(creation())
        assert sandbox.id == "sandbox-1"
        assert sandbox.handle.revision==creation().profile.revision
        assert sandbox.handle.profile_key==creation().profile.compatibility_key
        value._profiles.clear()
        assert (await value.get(sandbox.handle)).handle==sandbox.handle
        assert len(client.create_calls) == 1
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_unknown_create_without_inventory_match_is_explicit():
    client = FakeAliyunClient()
    client.create_error = TimeoutError("response lost")
    value = backend(client, httpx.MockTransport(health_transport))
    try:
        with pytest.raises(SandboxCreateOutcomeUnknown):
            await value.create(creation())
        assert len(client.create_calls) == 1
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_delete_does_not_trust_transient_inventory_absence(monkeypatch):
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    async def no_sleep(_):return None
    monkeypatch.setattr("sandbox_harness.sandbox_aliyun.asyncio.sleep",no_sleep)
    try:
        sandbox=await value.create(creation())
        client.hide_inventory=True
        client.kill_removes=False
        with pytest.raises(SandboxProviderUnavailable,match="did not converge"):
            await value.delete(sandbox)
        assert client.kill_calls==[sandbox.id]
        assert sandbox.id in client.items
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_endpoint_fails_closed_without_traffic_token():
    client=FakeAliyunClient()
    value=backend(client,httpx.MockTransport(health_transport))
    try:
        sandbox=await value.create(creation())
        client.traffic_token=None
        value._endpoints.clear()
        with pytest.raises(SandboxEndpointUnavailable,match="authenticated traffic token"):
            await value.endpoint(sandbox,8080,force_refresh=True)
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_force_refresh_reconnects_for_a_new_traffic_token():
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    try:
        sandbox=await value.create(creation())
        client.traffic_token="traffic-refreshed"
        endpoint=await value.endpoint(sandbox,8080,force_refresh=True)
        assert endpoint.headers=={"e2b-traffic-access-token":"traffic-refreshed"}
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_get_resumes_paused_sandbox_before_runtime_health():
    client=FakeAliyunClient();request=creation()
    metadata={**AliyunSandboxBackend._profile_metadata(request.profile),
        "sandbox-harness.io/namespace":"ali-test",
        "sandbox-harness.io/idempotency-key":request.allocation_id,
        "sandbox-harness.io/harness":"codex"}
    client.items["paused"]=client._info("paused","template-1",metadata,state="paused")
    value=backend(client,httpx.MockTransport(health_transport))
    try:
        sandbox=await value.get("paused")
        assert sandbox.state.value=="ready" and client.items["paused"].state=="running"
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_repeated_create_adopts_same_allocation_without_replay():
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    try:
        first=await value.create(creation());second=await value.create(creation())
        assert first.id==second.id and len(client.create_calls)==1
        await value.delete(first)
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_get_rejects_mismatched_durable_handle():
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    try:
        sandbox=await value.create(creation())
        value._profiles.clear()
        with pytest.raises(SandboxProfileUnavailable,match="handle fence"):
            await value.get(replace(sandbox.handle,allocation_id="different-allocation"))
        recovered=await value.get(sandbox.handle)
        assert recovered.handle==sandbox.handle
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_idempotency_rejects_different_template_with_same_compatibility_key():
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    first=creation()
    conflicting_profile=SandboxProfile(profile_id=first.profile.profile_id,
        revision=first.profile.revision,provider="aliyun",provider_ref="template-2",
        runtime_contract=first.profile.runtime_contract,
        runtime_bundle_digest=first.profile.runtime_bundle_digest,
        runtime_code_bundle_digest=first.profile.runtime_code_bundle_digest,
        compatibility_key=first.profile.compatibility_key)
    conflicting=SandboxCreateRequest(harness=first.harness,profile=conflicting_profile,
        environment={},ttl_seconds=300,allocation_id=first.allocation_id)
    try:
        sandbox=await value.create(first)
        with pytest.raises(SandboxProfileUnavailable,match="idempotency key conflicts"):
            await value.create(conflicting)
        await value.delete(sandbox)
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_exact_id_cannot_cross_namespace_boundary():
    client=FakeAliyunClient();request=creation()
    metadata={**AliyunSandboxBackend._profile_metadata(request.profile),
        "sandbox-harness.io/namespace":"another-product",
        "sandbox-harness.io/idempotency-key":"foreign",
        "sandbox-harness.io/harness":"codex"}
    info=client._info("foreign","template-1",metadata);client.items[info.sandbox_id]=info
    value=backend(client,httpx.MockTransport(health_transport))
    try:
        with pytest.raises(SandboxNotFound):await value.get(info.sandbox_id)
        await value.delete(value._sandbox(info))
        assert info.sandbox_id in client.items and client.kill_calls==[]
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_does_not_translate_task_cancellation():
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    async def cancelled(_):raise asyncio.CancelledError()
    client.get_info=cancelled
    try:
        with pytest.raises(asyncio.CancelledError):await value.get("sandbox")
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_delete_waits_for_confirmed_absence():
    class DelayedDeleteClient(FakeAliyunClient):
        remaining=0
        async def kill(self,sandbox_id):
            self.kill_calls.append(sandbox_id);self.remaining=2;return True
        async def get_info(self,sandbox_id):
            if self.remaining:
                self.remaining-=1
                if not self.remaining:self.items.clear()
            return await super().get_info(sandbox_id)
    client=DelayedDeleteClient();value=backend(client,httpx.MockTransport(health_transport))
    try:
        sandbox=await value.create(creation());await value.delete(sandbox)
        assert sandbox.id not in client.items and client.kill_calls==[sandbox.id]
    finally:
        await value.aclose()


@pytest.mark.asyncio
async def test_aliyun_renew_skips_provider_write_until_half_ttl_remains():
    client=FakeAliyunClient();value=backend(client,httpx.MockTransport(health_transport))
    try:
        sandbox=await value.create(creation())
        sandbox.expires_at=datetime.now(timezone.utc)+timedelta(seconds=500)
        await value.renew(sandbox)
        assert client.timeout_calls==[]
        sandbox.expires_at=datetime.now(timezone.utc)+timedelta(seconds=100)
        await value.renew(sandbox)
        assert client.timeout_calls==[(sandbox.id,600)]
        await value.delete(sandbox)
    finally:
        await value.aclose()
