import asyncio
from types import SimpleNamespace

from fastapi.testclient import TestClient

import sandbox_harness.gateway as gateway_module
from sandbox_harness.app import create_app
from sandbox_harness.config import Settings
from sandbox_harness.sandbox_inspector import SandboxInspector


def settings(tmp_path,name="health"):
    return Settings(data_root=tmp_path,database_url="sqlite+aiosqlite:///"+str(tmp_path/(name+".db")),
        redis_url="",backend="fake",object_backend="local",model_catalog_discovery=False,
        session_signing_key="review-health-secret",supervisor_retry_initial_seconds=.01,
        supervisor_retry_max_seconds=.02)


def test_api_health_tracks_database_and_required_supervisors(tmp_path):
    app=create_app(settings(tmp_path));store=app.state.store
    with TestClient(app) as client:
        assert client.get("/api/health").status_code==200
        original=store.healthcheck
        async def unavailable():raise ConnectionError("database offline")
        store.healthcheck=unavailable
        response=client.get("/api/health")
        assert response.status_code==503 and response.json()["database"]["available"] is False
        store.healthcheck=original
        class DeadTask:
            def done(self):return True
        app.state.required_tasks["injected_dead"]=DeadTask()
        response=client.get("/api/health")
        assert response.status_code==503 and response.json()["supervisors"]["injected_dead"] is False
        del app.state.required_tasks["injected_dead"]
        assert client.get("/api/health").status_code==200


def test_media_outbox_claim_failure_recovers_without_task_restart(tmp_path):
    app=create_app(settings(tmp_path,"media-loop"));store=app.state.store
    original=store.claim_media_batches;calls=0
    async def flaky(*args,**kwargs):
        nonlocal calls;calls+=1
        if calls<=3:raise ConnectionError("temporary database failure")
        return await original(*args,**kwargs)
    store.claim_media_batches=flaky
    with TestClient(app) as client:
        for _ in range(40):
            if calls>=4:break
            __import__('time').sleep(.05)
        response=client.get("/api/health")
        assert response.status_code==200 and calls>=4
        assert response.json()["media_outbox"]["failures"]==3
        assert response.json()["supervisors"]["media_outbox"] is True


async def test_sandbox_expiry_scan_failure_recovers_and_releases():
    class Store:
        calls=0
        async def expired_sandbox_controls(self):
            self.calls+=1
            if self.calls<=3:raise ConnectionError("temporary")
            return [{"thread_id":"thread","owner_id":"owner","sandbox_id":"sandbox"}]
    values=Store();settings=SimpleNamespace(gateway_signing_key="g"*32,
        session_signing_key="s"*32,supervisor_retry_initial_seconds=.01,
        supervisor_retry_max_seconds=.02);inspector=SandboxInspector(settings,values,None,None,None,None)
    released=asyncio.Event()
    async def release(*args,**kwargs):released.set();inspector.stop_event.set()
    inspector.release=release
    task=asyncio.create_task(inspector._cleanup_loop())
    await asyncio.wait_for(released.wait(),2)
    await task
    assert values.calls>=4 and inspector.cleanup_failures==3
    assert inspector.cleanup_status=="running"


def test_gateway_redis_is_optional_but_database_is_required(tmp_path,monkeypatch):
    class Redis:
        available=False
        async def ping(self):
            if not self.available:raise ConnectionError("redis offline")
            return True
        async def aclose(self):pass
    redis=Redis()
    class Proxy:
        def close(self):pass
        async def wait_closed(self):pass
    async def start_proxy(*args,**kwargs):return Proxy()
    monkeypatch.setattr(gateway_module.Redis,"from_url",lambda *args,**kwargs:redis)
    monkeypatch.setattr(gateway_module,"start_proxy",start_proxy)
    app=gateway_module.create_gateway(settings(tmp_path,"gateway"));store=app.state.store
    with TestClient(app) as client:
        response=client.get("/health")
        assert response.status_code==200 and response.json()["status"]=="degraded"
        assert response.json()["ready"] is True and response.json()["redis"]["available"] is False
        redis.available=True
        assert client.get("/health").json()["status"]=="ok"
        original=store.healthcheck
        async def unavailable():raise ConnectionError("database offline")
        store.healthcheck=unavailable
        response=client.get("/health")
        assert response.status_code==503 and response.json()["ready"] is False
        store.healthcheck=original
        assert client.get("/health").status_code==200
