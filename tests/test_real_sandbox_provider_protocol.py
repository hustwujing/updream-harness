import asyncio
import importlib.util
from pathlib import Path

import httpx
import pytest


ROOT=Path(__file__).resolve().parents[1]


def module():
    spec=importlib.util.spec_from_file_location("provider_real",
        ROOT/"tests/run_real_sandbox_provider_protocol.py")
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


def test_real_provider_case_catalog_covers_four_abnormal_boundaries():
    value=module()
    assert tuple(value.CASES)==("SP01","SP02","SP03","SP04")
    assert value.CASES["SP01"].__name__=="case_lost_create_response"
    assert value.CASES["SP02"].__name__=="case_external_delete_releases_capacity"
    assert value.CASES["SP03"].__name__=="case_endpoint_cache_expiry"
    assert value.CASES["SP04"].__name__=="case_repeated_delete"


def test_real_provider_requests_have_unique_bounded_ownership_metadata():
    value=module();first=value.create_request(value.settings_fixture(),"SP01","run-one")
    second=value.create_request(value.settings_fixture(),"SP01","run-two")
    assert first.idempotency_key!=second.idempotency_key
    assert first.metadata["sandbox-harness.io/provider-case"]=="SP01"
    assert first.metadata["sandbox-harness.io/provider-run"]=="run-one"
    assert len(first.idempotency_key)<=64


@pytest.mark.asyncio
async def test_response_drop_transport_forwards_acceptance_then_fails_once():
    value=module();calls=[]
    async def respond(request:httpx.Request):
        calls.append(request.url.path);return httpx.Response(200,json={"id":"real-created"})
    transport=value.DropAcceptedCreateTransport(httpx.MockTransport(respond))
    request=httpx.Request("POST","http://provider/v1/sandboxes")
    with pytest.raises(httpx.ReadError,match="accepted response dropped"):
        await transport.handle_async_request(request)
    response=await transport.handle_async_request(request)
    assert response.status_code==200 and calls==["/v1/sandboxes","/v1/sandboxes"]


def test_cleanup_tracker_rejects_foreign_sandbox_ids():
    value=module();tracker=value.OwnedSandboxes()
    tracker.add("owned")
    assert tracker.owns("owned") and not tracker.owns("foreign")
    with pytest.raises(ValueError,match="foreign_sandbox_cleanup"):
        tracker.require_owned("foreign")
