#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace
import uuid

import httpx
from dotenv import dotenv_values

from sandbox_harness.config import load_settings
from sandbox_harness.domain import Harness
from sandbox_harness.pool import SandboxPool
from sandbox_harness.sandbox_contracts import SandboxCreateRequest,SandboxProfile,SandboxScope
from sandbox_harness.sandbox_factory import build_profile_resolver,build_sandbox_backend
from sandbox_harness.sandbox_opensandbox import OpenSandboxBackend


ROOT=Path(__file__).resolve().parents[1]


class OwnedSandboxes:
    def __init__(self):self.ids:set[str]=set()
    def add(self,identity:str)->None:self.ids.add(identity)
    def owns(self,identity:str)->bool:return identity in self.ids
    def require_owned(self,identity:str)->None:
        if identity not in self.ids:raise ValueError("foreign_sandbox_cleanup:"+identity)


class DropAcceptedCreateTransport(httpx.AsyncBaseTransport):
    def __init__(self,inner:httpx.AsyncBaseTransport):self.inner=inner;self.dropped=False
    async def handle_async_request(self,request:httpx.Request)->httpx.Response:
        response=await self.inner.handle_async_request(request)
        if (request.method=="POST" and request.url.path=="/v1/sandboxes" and not self.dropped
                and 200<=response.status_code<300):
            self.dropped=True;await response.aread();await response.aclose()
            raise httpx.ReadError("accepted response dropped",request=request)
        return response
    async def aclose(self)->None:await self.inner.aclose()


def settings_fixture():
    return SimpleNamespace(runtime_bundle_digest="1"*64,runtime_code_bundle_digest="2"*64,
        sandbox_timeout_seconds=300,image_for=lambda harness:"registry/runtime@sha256:"+"3"*64)


def create_request(settings,case_id:str,run_id:str)->SandboxCreateRequest:
    image=settings.image_for("codex")
    profile=SandboxProfile(profile_id="provider-protocol-codex",revision=image,
        provider="opensandbox",provider_ref=image,runtime_contract="sandbox-harness-runtime/v1",
        runtime_bundle_digest=settings.runtime_bundle_digest or "legacy",
        runtime_code_bundle_digest=settings.runtime_code_bundle_digest or "legacy")
    identity=(case_id.lower()+"-"+run_id.replace("-",''))[:64]
    return SandboxCreateRequest(harness=Harness.CODEX,profile=profile,environment={},
        ttl_seconds=min(600,settings.sandbox_timeout_seconds),allocation_id=identity)


def opensandbox(settings,*,transport=None)->OpenSandboxBackend:
    return OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,
        settings.sandbox_timeout_seconds,settings.namespace,settings.gateway_url,
        settings.gateway_proxy_port,transport=transport)


async def _delete_owned(backend,tracker:OwnedSandboxes,sandbox)->None:
    tracker.require_owned(sandbox.id);await backend.delete(sandbox)


async def case_lost_create_response(settings,run_id:str)->dict:
    tracker=OwnedSandboxes();transport=DropAcceptedCreateTransport(httpx.AsyncHTTPTransport(retries=0))
    backend=opensandbox(settings,transport=transport);sandbox=None
    try:
        request=create_request(settings,"SP01",run_id);sandbox=await backend.create(request);tracker.add(sandbox.id)
        reconciled=await backend.reconcile_create(SandboxScope(settings.namespace),request.allocation_id)
        if reconciled is None or reconciled.id!=sandbox.id:
            raise AssertionError("create_reconciliation_not_unique")
        return {"sandbox_id":sandbox.id,"response_dropped":transport.dropped,
            "reconciled_count":1,"state":sandbox.state.value}
    finally:
        if sandbox is not None:await _delete_owned(backend,tracker,sandbox)
        await backend.aclose()


async def case_external_delete_releases_capacity(settings,run_id:str)->dict:
    backend=build_sandbox_backend(settings);tracker=OwnedSandboxes()
    resolver=build_profile_resolver(settings,backend)
    pool=SandboxPool(backend,{h:settings.image_for(h.value) for h in Harness},
        {h:0 for h in Harness},max_sandboxes=1,profile_resolver=resolver,
        ttl_seconds=min(600,settings.sandbox_timeout_seconds))
    first=second=None
    try:
        first=await pool.acquire(thread_id="sp02-"+run_id,turn_id="sp02-first-"+run_id,
            harness=Harness.CODEX,environment={});tracker.add(first.id)
        response=await backend.client.delete(f"/v1/sandboxes/{first.id}")
        if response.status_code not in {204,404}:response.raise_for_status()
        for _ in range(40):
            check=await backend.client.get(f"/v1/sandboxes/{first.id}")
            if check.status_code==404:break
            await asyncio.sleep(.25)
        else:raise AssertionError("external_delete_did_not_converge")
        second=await asyncio.wait_for(pool.acquire(thread_id="sp02-"+run_id,
            turn_id="sp02-second-"+run_id,harness=Harness.CODEX,environment={}),30)
        tracker.add(second.id)
        if second.id==first.id:raise AssertionError("deleted_sandbox_was_reused")
        await pool.release(second,keep_attached=False);second=None
        await pool.close()
        if await pool.managed_sandbox_ids() or pool._retiring:raise AssertionError("pool_capacity_leaked")
        return {"deleted_sandbox_id":first.id,"replacement_sandbox_id":next(iter(
            tracker.ids-{first.id})),"capacity_reacquired":True,"managed_after":0}
    finally:
        if second is not None:
            tracker.require_owned(second.id);await pool.release(second,keep_attached=False)
        await pool.close();await backend.aclose()


async def case_endpoint_cache_expiry(settings,run_id:str)->dict:
    backend=opensandbox(settings);tracker=OwnedSandboxes();sandbox=None
    try:
        sandbox=await backend.create(create_request(settings,"SP03",run_id));tracker.add(sandbox.id)
        first=await backend.endpoint(sandbox,8080)
        backend._endpoints[(sandbox.id,8080)]=replace(first,expires_at_monotonic=time.monotonic()-1)
        refreshed=await backend.endpoint(sandbox,8080)
        async with httpx.AsyncClient(base_url=refreshed.url,headers=dict(refreshed.headers),
                timeout=20,trust_env=False) as client:
            health=await client.get("/health");health.raise_for_status()
        return {"sandbox_id":sandbox.id,"same_sandbox":True,"endpoint_refreshed":first is not refreshed,
            "runtime_health":health.json().get("status")}
    finally:
        if sandbox is not None:await _delete_owned(backend,tracker,sandbox)
        await backend.aclose()


async def case_repeated_delete(settings,run_id:str)->dict:
    backend=opensandbox(settings);tracker=OwnedSandboxes();sandbox=None
    try:
        request=create_request(settings,"SP04",run_id);sandbox=await backend.create(request);tracker.add(sandbox.id)
        await _delete_owned(backend,tracker,sandbox);await _delete_owned(backend,tracker,sandbox)
        owned=await backend.list_owned(SandboxScope(settings.namespace))
        if any(value.id==sandbox.id for value in owned):
            raise AssertionError("sandbox_remained_after_repeated_delete")
        return {"sandbox_id":sandbox.id,"delete_calls":2,"remaining":0}
    finally:
        if sandbox is not None:
            try:await _delete_owned(backend,tracker,sandbox)
            except Exception:pass
        await backend.aclose()


CASES={"SP01":case_lost_create_response,"SP02":case_external_delete_releases_capacity,
    "SP03":case_endpoint_cache_expiry,"SP04":case_repeated_delete}


def load_platform_settings(path:Path):
    for key,value in dotenv_values(path).items():
        if value is not None:os.environ[key]=value
    return load_settings()


async def run(args)->dict:
    settings=load_platform_settings(Path(args.env_file));selected=[value for value in args.cases.split(",") if value]
    unknown=set(selected)-set(CASES)
    if unknown:raise ValueError("unknown_provider_cases:"+",".join(sorted(unknown)))
    revision=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    run_id=uuid.uuid4().hex[:20];results={};passed=True
    for case_id in selected:
        started=time.monotonic()
        try:evidence=await CASES[case_id](settings,run_id)
        except Exception as exc:
            passed=False;evidence={"error":type(exc).__name__,"detail":str(exc)}
        results[case_id]={"passed":"error" not in evidence,"elapsed_seconds":round(
            time.monotonic()-started,3),"evidence":evidence}
    return {"schema_version":1,"run_id":run_id,"revision":revision,"provider":"opensandbox",
        "timestamp":datetime.now(timezone.utc).isoformat(),"passed":passed,"cases":results}


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--base-url",default="http://127.0.0.1:24767")
    parser.add_argument("--cases",default=",".join(CASES));parser.add_argument("--env-file",
        default=str(ROOT/".data/config/platform.env"));parser.add_argument("--report",
        default=str(ROOT/".data/evidence/sandbox-provider/latest.json"));args=parser.parse_args()
    report=asyncio.run(run(args));target=Path(args.report);target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n");print(json.dumps(report,ensure_ascii=False))
    if not report["passed"]:raise SystemExit(1)


if __name__=="__main__":main()
