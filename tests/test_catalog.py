import json
import httpx
import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.model_discovery import ModelDiscovery, compatible_models, parse_model_ids


def test_public_reasoning_level_maps_to_native_harness_effort():
    catalog = HarnessCatalog()
    assert catalog.resolve("codex", "gpt-5.6", "maximum").reasoning_effort == "xhigh"
    assert catalog.resolve("claude", "claude-4.6-sonnet", "deep").reasoning_effort == "high"
    assert catalog.resolve("deepseek", "deepseek-v4.1-flash", "standard").reasoning_effort == "high"
    public = catalog.public()
    assert all("native" not in level for harness in public for model in harness["models"] for level in model["reasoning"])


def test_unsupported_model_or_reasoning_level_is_rejected():
    catalog = HarnessCatalog()
    with pytest.raises(ValueError, match="Model"):
        catalog.resolve("claude", "invented", "deep")
    with pytest.raises(ValueError, match="Reasoning"):
        catalog.resolve("deepseek", "deepseek-v4.1-flash", "maximum")


def test_provider_catalog_is_filtered_by_native_harness_protocol():
    mixed = ["claude-sonnet-5-5", "origin/claude-opus-5-5", "deepseek-v4.1-flash",
        "gpt-6-sol", "gpt-image-2", "text-embedding-v4"]
    assert compatible_models("codex", mixed) == ["gpt-6-sol"]
    assert compatible_models("claude", mixed) == ["claude-sonnet-5-5", "origin/claude-opus-5-5"]
    assert compatible_models("deepseek", mixed) == ["deepseek-v4.1-flash"]
    assert parse_model_ids({"data": [{"id": "one"}, {"name": "two"}, {"invalid": True}]}) == ["one", "two"]


def test_origin_claude_model_has_product_label_and_reasoning_mapping():
    catalog=HarnessCatalog()
    option=next(item for item in catalog.public() if item["id"]=="claude")
    model=next(item for item in option["models"] if item["id"]=="origin/claude-opus-5-5")
    assert model["label"]=="原厂claude-opus-5-5"
    assert catalog.resolve("claude","origin/claude-opus-5-5","deep").reasoning_effort=="high"


async def test_model_discovery_refreshes_catalog_and_persists_snapshot(tmp_path, monkeypatch):
    settings = Settings(
        data_root=tmp_path,
        upstream_codex="https://codex.example",
        upstream_claude="https://claude.example/v1",
        upstream_deepseek="https://deepseek.example/v1",
    )
    for harness in ("CODEX", "CLAUDE", "DEEPSEEK"):
        monkeypatch.setenv("SANDBOX_HARNESS_UPSTREAM_" + harness + "_KEY", harness.lower() + "-secret")

    def handler(request: httpx.Request):
        assert request.headers.get("authorization")
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["model"]
            assert request.url.path in {"/responses", "/v1/messages"}
            return httpx.Response(200, json={"ok": True})
        models = {
            "codex.example": ["gpt-6-sol", "gpt-image-2", "claude-4.6-sonnet"],
            "claude.example": ["claude-4.6-sonnet", "claude-sonnet-5-5", "deepseek-v4.1-flash"],
            "deepseek.example": ["deepseek-v4.1-flash", "deepseek-r1", "claude-opus-5"],
        }[request.url.host]
        return httpx.Response(200, json={"data": [{"id": model} for model in models]})

    catalog = HarnessCatalog()
    discovery = ModelDiscovery(settings, catalog, httpx.MockTransport(handler))
    refreshed = await discovery.refresh()
    assert refreshed[Harness.CODEX] == ["gpt-5.6", "gpt-6-sol"]
    assert [model.id for model in catalog.options[Harness.CLAUDE].models] == [
        "claude-4.6-sonnet", "claude-sonnet-5-5",
    ]
    assert catalog.resolve("deepseek", "deepseek-r1", "deep").model == "deepseek-r1"
    assert discovery.snapshot.is_file()
    assert catalog.public()[0]["models"][0]["status"] == "available"

    restored = HarnessCatalog()
    assert ModelDiscovery(settings, restored).load_snapshot()
    assert restored.resolve("codex", "gpt-6-sol", "maximum").reasoning_effort == "xhigh"


async def test_transient_probe_failure_does_not_disable_or_change_default(tmp_path, monkeypatch):
    settings = Settings(data_root=tmp_path, upstream_codex="https://codex.example",
                        upstream_claude="", upstream_deepseek="")
    monkeypatch.setenv("SANDBOX_HARNESS_UPSTREAM_CODEX_KEY", "secret")

    def handler(request: httpx.Request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "gpt-5.6"}, {"id": "gpt-6-astra"}]})
        body = json.loads(request.content)
        return httpx.Response(200 if body["model"] == "gpt-6-astra" else 502, json={"ok": True})

    catalog = HarnessCatalog()
    discovery = ModelDiscovery(settings, catalog, httpx.MockTransport(handler))
    await discovery.refresh()
    assert discovery.status()["available"] == 1
    assert discovery.status()["discovered"] == 2
    assert discovery.status()["last_refresh_at"]
    models = {row["id"]: row for row in catalog.public()[0]["models"]}
    assert models["gpt-6-astra"]["available"] is True
    assert models["gpt-5.6"]["error_code"] == "http_502"
    assert catalog.options[Harness.CODEX].default_model == "gpt-5.6"
    assert catalog.resolve("codex", "gpt-5.6", "standard").model == "gpt-5.6"

    restored = HarnessCatalog()
    assert ModelDiscovery(settings, restored).load_snapshot()
    assert restored.resolve("codex", "gpt-5.6", "standard").model == "gpt-5.6"


async def test_discovery_probes_explicit_model_on_its_own_route(tmp_path, monkeypatch):
    settings = Settings(data_root=tmp_path, upstream_codex="https://default.example",
        upstream_claude="", upstream_deepseek="", model_routes_json=json.dumps({
            "codex/gpt-6-sol":{"base_url":"https://sol.example/v1","credential_env":"SOL_KEY"}}))
    monkeypatch.setenv("SANDBOX_HARNESS_UPSTREAM_CODEX_KEY", "default-secret")
    monkeypatch.setenv("SOL_KEY", "sol-secret")

    def handler(request: httpx.Request):
        if request.method == "GET":
            return httpx.Response(200, json={"data":[{"id":"gpt-5.6"}]})
        body = json.loads(request.content)
        if body["model"] == "gpt-6-sol":
            assert request.url.host == "sol.example" and request.headers["authorization"] == "Bearer sol-secret"
        else:
            assert request.url.host == "default.example"
        return httpx.Response(200, json={"ok":True})

    catalog = HarnessCatalog(); discovery = ModelDiscovery(settings, catalog, httpx.MockTransport(handler))
    await discovery.refresh()
    assert [model.id for model in catalog.options[Harness.CODEX].models] == ["gpt-5.6", "gpt-6-sol"]


async def test_catalog_outage_keeps_inventory_and_probes_independent_model_route(tmp_path, monkeypatch):
    settings=Settings(data_root=tmp_path,upstream_codex='https://default.example',
        upstream_claude='',upstream_deepseek='',model_routes_json=json.dumps({
            'codex/gpt-6-sol':{'base_url':'https://sol.example/v1','credential_env':'SOL_KEY'}}))
    monkeypatch.setenv('SOL_KEY','test-secret')
    probes=[]
    def handler(request):
        if request.method=='GET':return httpx.Response(503,json={'error':'catalog unavailable'})
        model=json.loads(request.content)['model'];probes.append((request.url.host,model))
        return httpx.Response(200,json={'ok':True})
    catalog=HarnessCatalog()
    catalog.replace_models({Harness.CODEX:['gpt-5.6','gpt-6-astra']})
    discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handler))
    await discovery.refresh()
    assert {m.id for m in catalog.options[Harness.CODEX].models}=={'gpt-5.6','gpt-6-astra','gpt-6-sol'}
    assert ('sol.example','gpt-6-sol') in probes
    assert catalog.availability[(Harness.CODEX,'gpt-6-sol')]['status']=='available'
    restored=HarnessCatalog();assert ModelDiscovery(settings,restored).load_snapshot()
    assert {m.id for m in restored.options[Harness.CODEX].models}=={'gpt-5.6','gpt-6-astra','gpt-6-sol'}
