from datetime import datetime,timezone

import httpx
import pytest
import re

from sandbox_harness.domain import Harness
from sandbox_harness.sandbox_contracts import (
    Sandbox,SandboxCreateOutcomeUnknown,SandboxCreateRequest,SandboxEndpoint,SandboxProfile,
    SandboxState,SandboxUpdate)
from sandbox_harness.sandbox_opensandbox import OpenSandboxBackend


def request():
    profile=SandboxProfile(profile_id="primary-full-cpu-codex",revision="release-1",
        provider="opensandbox",provider_ref="registry/runtime@sha256:"+"1"*64,
        runtime_contract="sandbox-harness-runtime/v1",runtime_bundle_digest="2"*64,
        runtime_code_bundle_digest="3"*64)
    return SandboxCreateRequest(harness=Harness.CODEX,profile=profile,
        environment={"TEST":"1"},metadata={"sandbox-harness.io/turn":"turn-1"},
        ttl_seconds=300,idempotency_key="turn-1")


def test_opensandbox_profile_metadata_uses_provider_safe_labels():
    values=OpenSandboxBackend._profile_metadata(request().profile)
    assert values["sandbox-harness.io/profile-key"]==request().profile.compatibility_key
    assert all(len(value)<=63 and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?",value)
        for value in values.values())


@pytest.mark.asyncio
async def test_opensandbox_conforms_to_provider_lifecycle_contract():
    deleted=False;metadata={};requests=[]
    async def respond(http_request:httpx.Request):
        nonlocal deleted,metadata
        requests.append((http_request.method,http_request.url.path))
        path=http_request.url.path
        if http_request.method=="POST" and path=="/v1/sandboxes":
            value=__import__("json").loads(http_request.content);metadata=dict(value["metadata"])
            return httpx.Response(200,json={"id":"sandbox-1"})
        if http_request.method=="GET" and path=="/v1/sandboxes/sandbox-1":
            return (httpx.Response(404) if deleted else httpx.Response(200,json={
                "id":"sandbox-1","metadata":metadata,"image":{"uri":request().profile.provider_ref},
                "status":{"state":"running"}}))
        if http_request.method=="GET" and path=="/v1/sandboxes/sandbox-1/endpoints/8080":
            return httpx.Response(200,json={"endpoint":"http://runtime.local"})
        if http_request.method=="GET" and path=="/health":
            return httpx.Response(200,json={"status":"ok"})
        if http_request.method=="PATCH" and path.endswith("/metadata"):
            patch=__import__("json").loads(http_request.content)
            for key,value in patch.items():
                if value is None:metadata.pop(key,None)
                else:metadata[key]=value
            return httpx.Response(200,json={})
        if http_request.method=="POST" and path.endswith("/renew-expiration"):
            return httpx.Response(200,json={})
        if http_request.method=="GET" and path=="/v1/sandboxes":
            return httpx.Response(200,json={"items":[] if deleted else [{
                "id":"sandbox-1","metadata":metadata,"image":{"uri":request().profile.provider_ref}}]})
        if http_request.method=="DELETE" and path=="/v1/sandboxes/sandbox-1":
            if deleted:return httpx.Response(404)
            deleted=True;return httpx.Response(204)
        raise AssertionError((http_request.method,str(http_request.url)))

    backend=OpenSandboxBackend("http://opensandbox","secret",300,"test-namespace",
        transport=httpx.MockTransport(respond))
    sandbox=await backend.create(request())
    assert sandbox.provider=="opensandbox" and sandbox.state==SandboxState.READY
    assert sandbox.profile.compatibility_key==request().profile.compatibility_key
    assert sandbox.metadata["sandbox-harness.io/namespace"]=="test-namespace"
    endpoint=await backend.endpoint(sandbox,8080)
    assert isinstance(endpoint,SandboxEndpoint) and endpoint.url=="http://runtime.local"
    assert endpoint.headers["open-sandbox-api-key"]=="secret"
    sandbox=await backend.update(sandbox,SandboxUpdate(metadata={
        "sandbox-harness.io/pool":None,"sandbox-harness.io/thread":"thread-1"},ttl_seconds=600))
    assert sandbox.metadata["sandbox-harness.io/thread"]=="thread-1"
    assert sandbox.expires_at and sandbox.expires_at>datetime.now(timezone.utc)
    assert [row.id for row in await backend.find({"sandbox-harness.io/turn":"turn-1"})]==["sandbox-1"]
    await backend.delete(sandbox);await backend.delete(sandbox)
    assert await backend.list_namespace()==[]
    await backend.aclose()
    assert ("PATCH","/v1/sandboxes/sandbox-1/metadata") in requests


@pytest.mark.asyncio
async def test_opensandbox_endpoint_cache_refreshes_after_deadline():
    endpoint_requests=0
    async def respond(http_request:httpx.Request):
        nonlocal endpoint_requests
        if http_request.url.path.endswith("/endpoints/8080"):
            endpoint_requests+=1
            return httpx.Response(200,json={"endpoint":f"http://runtime-{endpoint_requests}.local"})
        raise AssertionError(str(http_request.url))
    backend=OpenSandboxBackend("http://opensandbox","",300,"namespace",
        transport=httpx.MockTransport(respond))
    creation=request();sandbox=Sandbox(id="sandbox-1",provider="opensandbox",
        harness=Harness.CODEX,profile=creation.profile,metadata=dict(creation.metadata))
    first=await backend.endpoint(sandbox,8080)
    second=await backend.endpoint(sandbox,8080)
    refreshed=await backend.endpoint(sandbox,8080,force_refresh=True)
    assert first==second and refreshed.url=="http://runtime-2.local" and endpoint_requests==2
    await backend.aclose()


@pytest.mark.asyncio
async def test_opensandbox_unknown_create_waits_for_delayed_inventory_without_replay():
    creation=request();post_calls=0;list_calls=0
    async def respond(http_request:httpx.Request):
        nonlocal post_calls,list_calls
        path=http_request.url.path
        if http_request.method=="POST" and path=="/v1/sandboxes":
            post_calls+=1;raise httpx.ReadError("response lost",request=http_request)
        if http_request.method=="GET" and path=="/v1/sandboxes":
            list_calls+=1
            if list_calls==1:return httpx.Response(200,json={"items":[]})
            metadata={**creation.metadata,"sandbox-harness.io/namespace":"test-namespace",
                "sandbox-harness.io/idempotency-key":creation.idempotency_key,
                "sandbox-harness.io/harness":"codex"}
            return httpx.Response(200,json={"items":[{"id":"delayed","metadata":metadata,
                "image":{"uri":creation.profile.provider_ref}}]})
        if path=="/v1/sandboxes/delayed":return httpx.Response(200,json={"id":"delayed",
            "metadata":{**creation.metadata,"sandbox-harness.io/namespace":"test-namespace",
                "sandbox-harness.io/idempotency-key":creation.idempotency_key,
                "sandbox-harness.io/harness":"codex"},"image":{"uri":creation.profile.provider_ref},
            "status":{"state":"running"}})
        if path.endswith("/endpoints/8080"):return httpx.Response(200,json={"endpoint":"http://runtime"})
        if http_request.url.host=="runtime":return httpx.Response(200,json={"status":"ok"})
        raise AssertionError(str(http_request.url))
    backend=OpenSandboxBackend("http://opensandbox","",300,"test-namespace",
        transport=httpx.MockTransport(respond))
    sandbox=await backend.create(creation)
    assert sandbox.id=="delayed" and post_calls==1 and list_calls==2
    await backend.aclose()


@pytest.mark.asyncio
async def test_opensandbox_post_accept_readiness_failure_is_not_pre_accept_rate_limit():
    async def respond(http_request:httpx.Request):
        if http_request.method=="POST":return httpx.Response(200,json={"id":"accepted"})
        if http_request.url.path=="/v1/sandboxes/accepted":return httpx.Response(429)
        raise AssertionError(str(http_request.url))
    backend=OpenSandboxBackend("http://opensandbox","",300,"test-namespace",
        transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(SandboxCreateOutcomeUnknown,match="accepted"):
            await backend.create(request())
    finally:await backend.aclose()
