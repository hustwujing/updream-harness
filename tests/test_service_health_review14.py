import asyncio
from collections import OrderedDict
import json
import logging
from pathlib import Path
import threading
from types import SimpleNamespace

from fastapi.testclient import TestClient

import sandbox_harness.services.sandbox_gateway.gateway as gateway_module
from sandbox_harness.services.api.app import (create_app, web_distribution_digest)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.services.api.sandbox_inspector import (SandboxInspector)
import sandbox_harness.common.observability.observability as obs


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


def test_media_continuation_dispatch_links_durable_origin_trace(tmp_path,monkeypatch):
    telemetry=obs.Telemetry("media-continuation-test",tmp_path/"logs")
    monkeypatch.setattr(obs,"_PROVIDER",telemetry.provider)
    monkeypatch.setattr(obs,"_TELEMETRY",telemetry)
    root=logging.getLogger();old_level=root.level;root.setLevel(logging.INFO);root.addHandler(telemetry.handler)
    with obs.operation("test.media.origin",new_trace=True):
        expected=obs.carrier()["traceparent"]
    app=create_app(settings(tmp_path,"media-linked"));store=app.state.store
    claimed=False;done=threading.Event()
    async def claim(*args,**kwargs):
        nonlocal claimed
        if claimed:return []
        claimed=True
        return [{"turn_id":"a"*32,"thread_id":"b"*32,"owner_id":"owner"}]
    async def group(*args,**kwargs):
        return True,[{"status":"failed","output_role":"candidate","job_id":"c"*32,
            "draft_id":"d"*32,"kind":"image","model":"test","project_path":"",
            "error_code":"provider_failed"}]
    async def origin(*args,**kwargs):
        return SimpleNamespace(network_access="restricted",last_traceparent=expected,
            submit_traceparent="")
    async def submit(*args,**kwargs):return SimpleNamespace(id="e"*32)
    async def finish(*args,**kwargs):done.set()
    store.claim_media_batches=claim;store.media_continuation_group=group
    store.get_turn=origin;store.finish_media_batch=finish;app.state.broker.submit=submit
    try:
        with TestClient(app):assert done.wait(3)
        assert telemetry.flush(5)
        rows=[json.loads(line) for path in telemetry.sink.root.glob("*.telemetry.jsonl")
            for line in path.read_text().splitlines()]
        span=next(row for row in rows
            if row["signal"]=="span" and row["event_name"]=="media.continuation.dispatch")
        assert span["trace_id"]!=expected.split("-")[1]
        assert span["links"]==[{
            "trace_id":expected.split("-")[1],"span_id":expected.split("-")[2]}]
    finally:
        root.removeHandler(telemetry.handler);root.setLevel(old_level);telemetry.close()


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


def test_configured_frontend_digest_is_enforced_at_app_creation(tmp_path):
    current=web_distribution_digest(Path(__file__).resolve().parents[1]/"web"/"dist")
    app=create_app(settings(tmp_path,"web-good").model_copy(update={"web_dist_digest":current}))
    assert app.state.web_dist_digest==current
    try:
        create_app(settings(tmp_path,"web-bad").model_copy(update={"web_dist_digest":"0"*64}))
    except ValueError as exc:
        assert "frontend distribution digest" in str(exc)
    else:
        raise AssertionError("mutated frontend digest was accepted")


def test_media_outbox_finish_success_write_failure_recovers(tmp_path):
    app=create_app(settings(tmp_path,"finish-success"));store=app.state.store
    batch={"turn_id":"turn","thread_id":"thread"};done=False;finishes=0
    async def claim(*args,**kwargs):return [] if done else [batch]
    async def group(*args,**kwargs):return True,[{"status":"cancelled","output_role":"candidate",
        "job_id":"","draft_id":"draft","kind":"image","model":"test","project_path":"",
        "error_code":""}]
    async def finish(*args,**kwargs):
        nonlocal finishes,done;finishes+=1
        if finishes==1:raise ConnectionError("ack failed")
        assert kwargs["delivered"] is True;done=True
    store.claim_media_batches=claim;store.media_continuation_group=group;store.finish_media_batch=finish
    with TestClient(app) as client:
        for _ in range(100):
            if done:break
            __import__('time').sleep(.02)
        assert done and finishes==2
        assert client.get("/api/health").json()["supervisors"]["media_outbox"] is True


def test_media_outbox_finish_failure_write_failure_recovers(tmp_path):
    app=create_app(settings(tmp_path,"finish-failure"));store=app.state.store
    batch={"turn_id":"turn","thread_id":"thread"};done=False;finishes=0
    async def claim(*args,**kwargs):return [] if done else [batch]
    async def group(*args,**kwargs):return False,[]
    async def finish(*args,**kwargs):
        nonlocal finishes,done;finishes+=1
        if finishes==1:raise ConnectionError("nack failed")
        assert kwargs["delivered"] is False;done=True
    store.claim_media_batches=claim;store.media_continuation_group=group;store.finish_media_batch=finish
    with TestClient(app) as client:
        for _ in range(100):
            if done:break
            __import__('time').sleep(.02)
        assert done and finishes==2
        assert client.get("/api/health").json()["supervisors"]["media_outbox"] is True


def test_telemetry_fallback_expires_windows_and_enforces_capacity():
    state=OrderedDict();capacity=64
    for index in range(200):
        assert gateway_module.fallback_rate_count(state,f"nonce-{index}",1,capacity)==1
    assert len(state)==capacity
    assert gateway_module.fallback_rate_count(state,"hot",2,capacity)==1
    assert gateway_module.fallback_rate_count(state,"hot",2,capacity)==2
    assert len(state)==1 and state["hot"]==(2,2)
