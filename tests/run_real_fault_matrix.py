from __future__ import annotations
import asyncio,json,subprocess,time,uuid
from pathlib import Path
import httpx
from sandbox_harness.common.configuration.config import (load_settings)
from sandbox_harness.common.sandbox.sandbox import (OpenSandboxBackend)

TERMINAL={"COMPLETED","FAILED","CANCELLED","WAITING_USER"}

async def wait(client,turn_id,timeout=180):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  try:
   response=await client.get(f"api/turns/{turn_id}")
   if response.status_code==200:
    value=response.json()
    if value["state"] in TERMINAL:return value
  except httpx.TransportError:pass
  await asyncio.sleep(.5)
 raise TimeoutError(turn_id)

async def start(client,title,prompt):
 thread=(await client.post("api/threads",json={"harness":"claude","model":"claude-sonnet-5-5","reasoning_level":"standard","title":title})).json()
 response=await client.post(f"api/threads/{thread['id']}/turns",json={"message":prompt,"attachments":[],"idempotency_key":"fault-"+uuid.uuid4().hex,"delivery_mode":"start"});response.raise_for_status()
 return thread,response.json()

async def main():
 report={"cases":[]}
 async with httpx.AsyncClient(base_url="http://127.0.0.1:24766/",timeout=None,trust_env=False) as client:
  thread,turn=await start(client,"LIVE-R03-"+uuid.uuid4().hex[:8],"Create /workspace/outputs/gateway-restart.json as {\"attempted\":true} using Python, verify it, and reply briefly.")
  await asyncio.sleep(.2);subprocess.run(["sudo","-n","systemctl","restart","sandbox-harness-parallel-gateway.service"],check=True)
  final=await wait(client,turn["id"])
  if final["state"] not in {"COMPLETED","FAILED"}:raise AssertionError(json.dumps(final))
  health=(await client.get("api/health")).json()
  report["cases"].append({"id":"R03","status":"passed","turn_id":turn["id"],"terminal":final["state"],"error_code":final.get("error_code",""),"scanner":health["broker"]["recovery_scanner"]})

  thread,turn=await start(client,"LIVE-R04-"+uuid.uuid4().hex[:8],"Run a foreground Python program that prints STARTED, sleeps 30 seconds, then writes /workspace/outputs/should-not-publish.json. Do not run it in background.")
  current=None
  for _ in range(240):
   current=(await client.get(f"api/turns/{turn['id']}")).json()
   if current["state"]=="RUNNING" and current.get("runtime_sequence",0)>=2:break
   if current["state"] in TERMINAL:raise RuntimeError("Turn ended before Sandbox deletion")
   await asyncio.sleep(.25)
  settings=load_settings();backend=OpenSandboxBackend(settings.opensandbox_url,settings.opensandbox_api_key,
   settings.sandbox_timeout_seconds,settings.namespace,settings.gateway_url,settings.gateway_proxy_port)
  try:
   sandbox_id=str(current.get("sandbox_id") or "")
   if not sandbox_id:raise AssertionError("Turn has no durable Sandbox ID")
   sandbox=await backend.get(sandbox_id);await backend.delete(sandbox)
  finally:await backend.client.aclose()
  final=await wait(client,turn["id"])
  if final["state"]!="FAILED" or final["error_code"]=="":raise AssertionError(json.dumps(final))
  if final.get("lease_owner"):raise AssertionError("Lease was not released")
  report["cases"].append({"id":"R04","status":"passed","turn_id":turn["id"],"sandbox_id":sandbox_id,
   "terminal":final["state"],"error_code":final["error_code"],"lease_released":True})
 Path(".data/fault-matrix-report.json").write_text(json.dumps(report,indent=2)+"\n");print(json.dumps(report,indent=2))
if __name__=="__main__":asyncio.run(main())
