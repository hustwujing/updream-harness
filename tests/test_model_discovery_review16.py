import copy
import json

import httpx
import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness
from sandbox_harness.model_discovery import ModelDiscovery


def model_ids(catalog,harness):return [model.id for model in catalog.options[Harness(harness)].models]


async def test_inventory_and_probe_count_are_bounded_with_priority(tmp_path):
    probes=[]
    def handle(request):
        if request.method=="GET":
            return httpx.Response(200,json={"data":[{"id":f"gpt-6-{index:03d}"}
                for index in range(45)]+[{"id":"gpt-6-005"}]})
        probes.append(request);return httpx.Response(200,json={"status":"ok"})
    settings=Settings(data_root=tmp_path,upstream_codex="https://codex.test/v1",
        upstream_claude="",upstream_deepseek="",model_catalog_max_models_per_harness=10,
        model_probe_concurrency=3,model_routes_json=json.dumps({"codex/gpt-6-explicit":{
            "base_url":"https://explicit.test/v1","credential_env":"EXPLICIT_KEY"}}))
    catalog=HarnessCatalog();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    await discovery.refresh();identifiers=model_ids(catalog,"codex")
    assert len(identifiers)==10 and {"gpt-5.6","gpt-6-explicit"}<=set(identifiers)
    assert len(probes)==10
    assert catalog.inventory_status[Harness.CODEX]["truncated"] is True


async def test_successful_disjoint_inventories_replace_old_nonprotected_models(tmp_path):
    current=["gpt-6-a"]
    def handle(request):
        if request.method=="GET":return httpx.Response(200,
            json={"data":[{"id":value} for value in current]})
        return httpx.Response(200,json={"ok":True})
    settings=Settings(data_root=tmp_path,upstream_codex="https://codex.test/v1",
        upstream_claude="",upstream_deepseek="",model_catalog_max_models_per_harness=3,
        model_routes_json=json.dumps({"codex/gpt-6-explicit":{"base_url":"https://explicit.test/v1",
            "credential_env":"EXPLICIT_KEY"}}))
    catalog=HarnessCatalog();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    await discovery.refresh();assert {"gpt-5.6","gpt-6-explicit","gpt-6-a"}==set(model_ids(catalog,"codex"))
    current[:]=["gpt-6-b"];await discovery.refresh()
    assert {"gpt-5.6","gpt-6-explicit","gpt-6-b"}==set(model_ids(catalog,"codex"))
    current[:]=["gpt-6-c","gpt-6-d"];await discovery.refresh()
    assert "gpt-6-a" not in model_ids(catalog,"codex") and "gpt-6-b" not in model_ids(catalog,"codex")
    current[:]=["gpt-6-d"];await discovery.refresh()
    assert {"gpt-5.6","gpt-6-explicit","gpt-6-d"}==set(model_ids(catalog,"codex"))


@pytest.mark.parametrize("invalid_harness",["codex","claude","deepseek"])
def test_invalid_snapshot_is_all_or_nothing(tmp_path,invalid_harness):
    catalog=HarnessCatalog();settings=Settings(data_root=tmp_path,model_probe_enabled=False)
    discovery=ModelDiscovery(settings,catalog);before=json.dumps(catalog.public(),sort_keys=True)
    models={"codex":["gpt-6-new"],"claude":["claude-4.6-sonnet"],
        "deepseek":["deepseek-v4.1-flash"]}
    models[invalid_harness]=["x"*161]
    discovery.snapshot.write_text(json.dumps({"version":4,"models":models,
        "availability":{},"metadata":{}}))
    assert discovery.load_snapshot() is False
    assert json.dumps(catalog.public(),sort_keys=True)==before


async def test_probe_and_snapshot_write_failure_leave_catalog_unchanged(tmp_path,monkeypatch):
    def handle(request):
        if request.method=="GET":
            model={"codex.test":"gpt-6-new","claude.test":"claude-new",
                "deepseek.test":"deepseek-new"}[request.url.host]
            return httpx.Response(200,json={"data":[{"id":model}]})
        return httpx.Response(200,json={"ok":True})
    settings=Settings(data_root=tmp_path,upstream_codex="https://codex.test/v1",
        upstream_claude="https://claude.test/v1",upstream_deepseek="https://deepseek.test/v1")
    catalog=HarnessCatalog();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    before=json.dumps(catalog.public(),sort_keys=True)
    async def fail_probe(*args):raise RuntimeError("probe failed")
    monkeypatch.setattr(discovery,"_probe_all",fail_probe)
    with pytest.raises(RuntimeError,match="probe failed"):await discovery.refresh()
    assert json.dumps(catalog.public(),sort_keys=True)==before
    monkeypatch.undo();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    monkeypatch.setattr(discovery,"_save_snapshot",lambda:(_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError,match="disk full"):await discovery.refresh()
    assert json.dumps(catalog.public(),sort_keys=True)==before


async def test_inventory_freshness_and_counts_survive_snapshot_restart(tmp_path):
    failed=set()
    inventory={"codex.test":"gpt-5.6","claude.test":"claude-4.6-sonnet",
        "deepseek.test":"deepseek-v4.1-flash"}
    def handle(request):
        if request.method=="GET":
            if request.url.host in failed:return httpx.Response(503,text="offline")
            return httpx.Response(200,json={"data":[{"id":inventory[request.url.host]}]})
        return httpx.Response(200,json={"ok":True})
    settings=Settings(data_root=tmp_path,upstream_codex="https://codex.test/v1",
        upstream_claude="https://claude.test/v1",upstream_deepseek="https://deepseek.test/v1")
    catalog=HarnessCatalog();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    assert discovery.status()["last_refresh_at"] is None
    await discovery.refresh();fresh=discovery.status();assert fresh["stale"] is False
    last_success=fresh["last_refresh_at"];assert fresh["discovered"]==3
    failed.add("claude.test");await discovery.refresh();stale=discovery.status()
    assert stale["stale"] is True and stale["last_refresh_at"]==last_success
    assert stale["inventory"]["claude"]["status"]=="stale"
    assert stale["inventory"]["codex"]["status"]=="fresh"
    restored_catalog=HarnessCatalog();restored=ModelDiscovery(settings,restored_catalog,
        httpx.MockTransport(handle))
    assert restored.load_snapshot();status=restored.status()
    assert status["discovered"]==stale["discovered"]
    assert status["available"]==stale["available"]
    assert status["last_refresh_at"]==last_success and status["last_attempt_at"]
    assert status["stale"] is True
