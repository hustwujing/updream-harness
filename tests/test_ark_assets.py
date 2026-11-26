from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

from sandbox_harness.services.media_worker.ark_assets import (ArkAssetError, ArkAssets)


def settings(**values):
    base={"ark_control_endpoint":"https://ark.test","ark_access_key":"access-key",
        "ark_secret_key":"secret-key","ark_project_name":"default",
        "ark_asset_group_id":"group-test","ark_asset_group_type":"AIGC"}
    base.update(values);return SimpleNamespace(**base)


@pytest.mark.asyncio
async def test_ark_asset_create_find_and_get_use_signed_control_plane_requests():
    calls=[]
    def handler(request):
        action=parse_qs(request.url.query.decode())["Action"][0]
        assert request.headers["authorization"].startswith("HMAC-SHA256 Credential=access-key/")
        assert request.headers["x-content-sha256"] and "secret-key" not in request.headers["authorization"]
        calls.append((action,request.content))
        if action=="ListAssets":result={"Items":[{"Id":"asset-existing","Name":"aniforge-"+"1"*40,
            "GroupId":"group-test","Status":"Active"}]}
        elif action=="CreateAsset":result={"Id":"asset-created"}
        else:result={"Id":"asset-created","Status":"Active"}
        return httpx.Response(200,json={"ResponseMetadata":{},"Result":result})
    value=ArkAssets(settings(),transport=httpx.MockTransport(handler))
    found=await value.find("1"*64);assert found["Id"]=="asset-existing"
    created=await value.create({"sha256":"2"*64,"public_url":"https://boss-shjd.biliapi.net/updream/aniforge/image/ref.png","kind":"image"})
    assert created=="asset-created" and (await value.get(created))["Status"]=="Active"
    assert [item[0] for item in calls]==["ListAssets","CreateAsset","GetAsset"]
    assert b"shjd-boss.bilibili.co" not in calls[1][1]
    assert b"boss-shjd.biliapi.net" in calls[1][1]


@pytest.mark.asyncio
async def test_ark_asset_requires_dedicated_access_key_and_group():
    value=ArkAssets(settings(ark_access_key="",ark_secret_key="",ark_asset_group_id=""))
    assert not value.configured
    with pytest.raises(ArkAssetError,match="credentials_unavailable"):
        await value.get("asset-test")


@pytest.mark.asyncio
async def test_ark_asset_error_exposes_only_sanitized_code():
    def handler(_):
        return httpx.Response(401,json={"ResponseMetadata":{"Error":{
            "Code":"InvalidAccessKey","Message":"credential secret should not escape"}}})
    value=ArkAssets(settings(),transport=httpx.MockTransport(handler))
    with pytest.raises(ArkAssetError) as error:
        await value.get("asset-test")
    assert str(error.value)=="ark_asset_InvalidAccessKey"
    assert "secret" not in str(error.value)


@pytest.mark.asyncio
async def test_ark_transport_error_is_sanitized():
    def handler(request):
        raise httpx.ConnectError("internal endpoint details",request=request)
    value=ArkAssets(settings(),transport=httpx.MockTransport(handler))
    with pytest.raises(ArkAssetError,match="ark_asset_transport_error"):
        await value.get("asset-test")
