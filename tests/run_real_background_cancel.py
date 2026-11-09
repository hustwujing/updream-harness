from __future__ import annotations
import asyncio,json,time,uuid
from pathlib import Path
import httpx

async def main():
 async with httpx.AsyncClient(base_url="http://127.0.0.1:22766/",timeout=None,trust_env=False) as client:
  thread=(await client.post("api/threads",json={"harness":"claude","model":"claude-sonnet-5-5","reasoning_level":"standard","title":"LIVE-B03-RETRY-"+uuid.uuid4().hex[:8]})).json()
  response=await client.post(f"api/threads/{thread['id']}/turns",json={"message":"Run exactly: sandbox-background start --name cancellable -- python -c 'import time;time.sleep(60)'. Then reply briefly. Do not wait with another command.","attachments":[],"idempotency_key":"cancel-"+uuid.uuid4().hex,"delivery_mode":"start"});response.raise_for_status();turn=response.json()
  job=None
  for _ in range(240):
   rows=(await client.get(f"api/turns/{turn['id']}/background-jobs")).json();job=next((row for row in rows if row["status"]=="running"),None)
   if job:break
   current=(await client.get(f"api/turns/{turn['id']}")).json()
   if current["state"] in {"COMPLETED","FAILED","CANCELLED","WAITING_USER"}:raise RuntimeError("Turn ended before job: "+json.dumps(current))
   await asyncio.sleep(.25)
  if not job:raise TimeoutError("background job")
  response=await client.post(f"api/turns/{turn['id']}/background-jobs/{job['id']}/cancel");response.raise_for_status()
  final=None
  for _ in range(240):
   final=(await client.get(f"api/turns/{turn['id']}")).json()
   if final["state"] in {"COMPLETED","FAILED","CANCELLED","WAITING_USER"}:break
   await asyncio.sleep(.25)
  if final["state"]!="FAILED" or final["error_code"]!="background_job_failed":raise AssertionError(json.dumps(final))
  report={"id":"B03","status":"passed","turn_id":turn["id"],"job_id":job["id"],"terminal":final["state"],"error_code":final["error_code"]}
  Path(".data/background-cancel-report.json").write_text(json.dumps(report,indent=2)+"\n");print(json.dumps(report))
if __name__=="__main__":asyncio.run(main())
