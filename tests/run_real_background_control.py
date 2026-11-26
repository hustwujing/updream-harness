"""Real OpenSandbox cases for bounded Sandbox-local background execution."""
from __future__ import annotations

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

from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.sandbox.runtime import (RuntimeClient)
from sandbox_harness.common.sandbox.sandbox import (OpenSandboxBackend)


FIXTURE="""import json,sys,time
json.loads(sys.stdin.readline())
time.sleep(300)
"""


async def wait_rows(client:httpx.AsyncClient,job_id:str,timeout:float|None=None)->list[dict]:
    params={} if timeout is None else {"timeout_seconds":timeout}
    rows=[]
    async with client.stream("GET",f"/v1/background-jobs/{job_id}/wait",params=params) as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if line:rows.append(json.loads(line))
    return rows


async def main()->None:
    root=Path(__file__).resolve().parents[1]
    env_file=os.environ.get("SANDBOX_HARNESS_ENV_FILE",str(root/".data/config/platform.env"))
    for key,value in dotenv_values(env_file).items():
        if value is not None:os.environ[key]=value
    settings=Settings();identity=uuid.uuid4().hex
    namespace="sh-bg-control-"+identity
    backend=OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,300,namespace,
        settings.gateway_url,settings.gateway_proxy_port)
    runtime=RuntimeClient(bundle_dir=settings.runtime_bundle_dir,
        default_bundle_digest=settings.runtime_bundle_digest,
        code_bundle_dir=settings.runtime_code_bundle_dir,
        default_code_bundle_digest=settings.runtime_code_bundle_digest,
        staging_root=settings.data_root/"tmp/runtime-downloads")
    sandbox=None;started=time.monotonic();cases=[]
    async def record(case_id:str,runner):
        case_started=time.monotonic()
        try:detail=await runner();state="passed";error=""
        except Exception as exc:
            detail={};state="failed";error=type(exc).__name__+": "+str(exc)
        cases.append({"id":case_id,"state":state,"seconds":round(time.monotonic()-case_started,3),
            "error":error,**detail})
    try:
        sandbox=await backend.create(Harness.CLAUDE,settings.image_for("claude"),{
            "sandbox-harness.io/idempotency-key":identity},{
            "HARNESS_COMMAND_JSON":json.dumps({"claude":["python","-c",FIXTURE]})})
        sandbox.thread_id="background-control";sandbox.turn_id=identity
        await runtime.inject(sandbox,capability_token="synthetic-fixture-no-provider-access",
            gateway_url=settings.gateway_url,
            proxy_url=settings.gateway_url.rsplit(":",1)[0]+":"+str(settings.gateway_proxy_port),
            harness="claude",model="synthetic",runtime_profile_id="primary-full-cpu")
        async with httpx.AsyncClient(base_url=sandbox.runtime_url,headers=sandbox.runtime_headers,
                timeout=None,trust_env=False) as client:
            held=asyncio.create_task(client.post("/v1/turns",json={"message":"hold",
                "execution_id":"background-control","input_id":"initial"}))
            for _ in range(200):
                if (await client.get("/v1/executions/background-control")).status_code==200:break
                await asyncio.sleep(.05)
            else:raise RuntimeError("fixture_execution_not_ready")

            async def success_wait():
                response=await client.post("/v1/background-jobs",json={"name":"success-wait",
                    "argv":["python","-c","import time;time.sleep(.15);print('ready')"],
                    "timeout_seconds":5,"terminate_grace_seconds":.1})
                response.raise_for_status();job=response.json()
                rows=await wait_rows(client,job["id"])
                terminal=rows[-1]
                assert terminal["type"]=="terminal" and terminal["job"]["status"]=="completed"
                assert "ready" in terminal["job"]["log_tail"]
                return {"job_id":job["id"],"heartbeats":sum(r["type"]=="heartbeat" for r in rows),
                    "terminal":terminal["job"]["status"]}
            await record("B04",success_wait)

            async def observation_timeout():
                response=await client.post("/v1/background-jobs",json={"name":"observation-timeout",
                    "argv":["python","-c","import time;time.sleep(.3);print('later')"],
                    "timeout_seconds":5,"terminate_grace_seconds":.1})
                response.raise_for_status();job=response.json()
                first=await wait_rows(client,job["id"],.05)
                assert first[-1]["type"]=="wait_timeout"
                assert first[-1]["job"]["status"]=="running"
                second=await wait_rows(client,job["id"]);terminal=second[-1]
                assert terminal["type"]=="terminal" and terminal["job"]["status"]=="completed"
                return {"job_id":job["id"],"observation_status":first[-1]["job"]["status"],
                    "terminal":terminal["job"]["status"]}
            await record("B05",observation_timeout)

            async def forced_deadline():
                command=("import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
                    "print('deadline-ready',flush=True);time.sleep(60)")
                response=await client.post("/v1/background-jobs",json={"name":"forced-deadline",
                    "argv":["python","-c",command],"timeout_seconds":.25,
                    "terminate_grace_seconds":.05})
                response.raise_for_status();job=response.json();rows=await wait_rows(client,job["id"])
                terminal=rows[-1]["job"]
                assert terminal["status"]=="timed_out" and terminal["returncode"]==124
                assert terminal["termination_detail"]=={"reason":"timed_out",
                    "sigterm_sent":True,"sigkill_sent":True}
                processes=(await client.get("/v1/inspect/processes")).json()["items"]
                assert all(row["pid"]!=job["pid"] for row in processes)
                return {"job_id":job["id"],"terminal":terminal["status"],
                    "returncode":terminal["returncode"],"process_absent":True}
            await record("B06",forced_deadline)
            (await client.post("/v1/cancel")).raise_for_status();await held
        await backend.delete(sandbox);sandbox=None
        cleanup=not await backend.list_namespace()
        revision=subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True).strip()
        report={"revision":revision,"started_at":datetime.now(timezone.utc).isoformat(),
            "real_opensandbox":True,"model":"synthetic fixture, no model request",
            "runtime_code_bundle_digest":settings.runtime_code_bundle_digest,
            "image":settings.image_for("claude"),"cases":cases,"sandbox_deleted":cleanup,
            "elapsed_seconds":round(time.monotonic()-started,3)}
        target=settings.data_root/"evidence/real-background-control"/(revision[:12]+".json")
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
        print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
        if not cleanup or any(row["state"]!="passed" for row in cases):raise SystemExit(1)
    finally:
        if sandbox is not None:await backend.delete(sandbox)
        await backend.client.aclose()


if __name__=="__main__":asyncio.run(main())
