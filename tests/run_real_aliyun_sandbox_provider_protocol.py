#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import httpx
from dotenv import dotenv_values

from sandbox_harness.config import load_settings
from sandbox_harness.domain import Harness
from sandbox_harness.sandbox_contracts import SandboxCreateRequest,SandboxNotFound,SandboxScope
from sandbox_harness.sandbox_factory import (build_profile_resolver,build_sandbox_backend,
    primary_provider_releases)


ROOT=Path(__file__).resolve().parents[1]


def load_platform_settings(path:Path):
    for key,value in dotenv_values(path).items():
        if value is not None:os.environ[key]=value
    settings=load_settings()
    if settings.backend!="aliyun":raise ValueError("aliyun_backend_required")
    return settings


def creation(settings,backend,case_id:str,run_id:str)->SandboxCreateRequest:
    releases=primary_provider_releases(settings,"aliyun")
    resolver=build_profile_resolver(settings,backend,releases)
    release=releases[Harness.CODEX]
    return SandboxCreateRequest(harness=Harness.CODEX,
        profile=resolver.resolve(Harness.CODEX,release.ref,
            profile_id="primary-full-cpu-codex",revision=release.revision),
        environment={},ttl_seconds=min(600,settings.sandbox_timeout_seconds),
        allocation_id=(case_id.lower()+"-"+run_id)[:160])


async def owned_absent(backend,sandbox_id:str)->bool:
    rows=await backend.list_owned(SandboxScope(backend.namespace))
    return all(row.id!=sandbox_id for row in rows)


async def case_repeated_allocation(settings,run_id):
    backend=build_sandbox_backend(settings);sandbox=None
    try:
        request=creation(settings,backend,"AP01",run_id)
        first=await backend.create(request);sandbox=first
        second=await backend.create(request)
        recovered=await backend.get(first.handle)
        if first.id!=second.id or recovered.handle!=first.handle:
            raise AssertionError("allocation_identity_not_stable")
        return {"sandbox_id":first.id,"create_reused":True,"handle":first.handle.to_mapping()}
    finally:
        if sandbox is not None:await backend.delete(sandbox)
        await backend.aclose()


class DropAcceptedCreate:
    def __init__(self,inner):self.inner=inner;self.dropped=False
    def __getattr__(self,name):return getattr(self.inner,name)
    async def create(self,**kwargs):
        value=await self.inner.create(**kwargs)
        if not self.dropped:
            self.dropped=True
            raise TimeoutError("accepted Aliyun create response dropped")
        return value


async def case_lost_create_response(settings,run_id):
    backend=build_sandbox_backend(settings);sandbox=None
    wrapper=DropAcceptedCreate(backend.client);backend.client=wrapper
    try:
        request=creation(settings,backend,"AP02",run_id)
        sandbox=await backend.create(request)
        if not wrapper.dropped or sandbox.handle.allocation_id!=request.allocation_id:
            raise AssertionError("accepted_create_not_reconciled")
        return {"sandbox_id":sandbox.id,"response_dropped":True,
            "revision":sandbox.handle.revision}
    finally:
        if sandbox is not None:await backend.delete(sandbox)
        await backend.aclose()


async def case_external_delete(settings,run_id):
    backend=build_sandbox_backend(settings);sandbox=None
    try:
        sandbox=await backend.create(creation(settings,backend,"AP03",run_id))
        await backend.client.kill(sandbox.id)
        for _ in range(40):
            try:await backend.get(sandbox.handle)
            except SandboxNotFound:break
            await asyncio.sleep(.25)
        else:raise AssertionError("external_delete_did_not_converge")
        await backend.delete(sandbox)
        if not await owned_absent(backend,sandbox.id):raise AssertionError("external_delete_remained")
        value={"sandbox_id":sandbox.id,"external_delete_converged":True};sandbox=None;return value
    finally:
        if sandbox is not None:await backend.delete(sandbox)
        await backend.aclose()


async def case_endpoint_refresh(settings,run_id):
    backend=build_sandbox_backend(settings);sandbox=None
    try:
        sandbox=await backend.create(creation(settings,backend,"AP04",run_id))
        first=await backend.endpoint(sandbox,8080)
        second=await backend.endpoint(sandbox,8080,force_refresh=True)
        async with httpx.AsyncClient(base_url=second.url,headers=dict(second.headers),
                timeout=20,trust_env=False) as client:
            health=await client.get("/health");health.raise_for_status()
        if "e2b-traffic-access-token" not in second.headers:
            raise AssertionError("authenticated_endpoint_header_missing")
        return {"sandbox_id":sandbox.id,"endpoint_refreshed":first is not second,
            "runtime_health":health.json().get("status")}
    finally:
        if sandbox is not None:await backend.delete(sandbox)
        await backend.aclose()


async def case_repeated_delete(settings,run_id):
    backend=build_sandbox_backend(settings);sandbox=None
    try:
        sandbox=await backend.create(creation(settings,backend,"AP05",run_id))
        identity=sandbox.id
        await backend.delete(sandbox);await backend.delete(sandbox);sandbox=None
        if not await owned_absent(backend,identity):raise AssertionError("repeated_delete_remained")
        return {"sandbox_id":identity,"delete_calls":2,"remaining":0}
    finally:
        if sandbox is not None:await backend.delete(sandbox)
        await backend.aclose()


CASES={"AP01":case_repeated_allocation,"AP02":case_lost_create_response,
    "AP03":case_external_delete,"AP04":case_endpoint_refresh,"AP05":case_repeated_delete}


async def run(args)->dict:
    settings=load_platform_settings(Path(args.env_file));selected=[x for x in args.cases.split(",") if x]
    unknown=set(selected)-set(CASES)
    if unknown:raise ValueError("unknown_aliyun_provider_cases:"+",".join(sorted(unknown)))
    revision=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    run_id=uuid.uuid4().hex[:20];results={};passed=True
    for identity in selected:
        started=time.monotonic()
        try:evidence=await CASES[identity](settings,run_id)
        except Exception as exc:
            passed=False;evidence={"error":type(exc).__name__,"detail":str(exc)}
        results[identity]={"passed":"error" not in evidence,
            "elapsed_seconds":round(time.monotonic()-started,3),"evidence":evidence}
    return {"schema_version":1,"run_id":run_id,"revision":revision,"provider":"aliyun",
        "timestamp":datetime.now(timezone.utc).isoformat(),"passed":passed,"cases":results}


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--cases",default=",".join(CASES))
    parser.add_argument("--env-file",default=str(ROOT/".data/config/platform.env"))
    parser.add_argument("--report",default=str(
        ROOT/".data/evidence/sandbox-provider/aliyun-latest.json"));args=parser.parse_args()
    report=asyncio.run(run(args));target=Path(args.report);target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps(report,ensure_ascii=False));raise SystemExit(0 if report["passed"] else 1)


if __name__=="__main__":main()
