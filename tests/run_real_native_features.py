from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import httpx


BASE = "http://127.0.0.1:22766/"
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "WAITING_USER"}
MODELS = {"codex":"gpt-6-sol", "claude":"claude-sonnet-5-5", "deepseek":"deepseek-v4-pro"}


async def create_thread(client, harness, title):
    response = await client.post("api/threads", json={"harness":harness,"model":MODELS[harness],
        "reasoning_level":"standard","title":title}); response.raise_for_status(); return response.json()


async def start_turn(client, thread, message):
    response = await client.post(f"api/threads/{thread['id']}/turns", json={"message":message,"attachments":[],
        "idempotency_key":"native-feature-"+uuid.uuid4().hex,"delivery_mode":"start"})
    response.raise_for_status(); return response.json()


async def wait_turn(client, turn_id, timeout=300):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        response=await client.get(f"api/turns/{turn_id}");response.raise_for_status();value=response.json()
        if value["state"] in TERMINAL:return value
        await asyncio.sleep(.5)
    raise TimeoutError(turn_id)


async def wait_interaction(client, thread_id, timeout=120):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        rows=(await client.get(f"api/threads/{thread_id}/interactions")).json()
        pending=next((row for row in rows if row["status"]=="pending"),None)
        if pending:return pending
        await asyncio.sleep(.25)
    raise TimeoutError("interaction")


async def artifact_json(client, final, path):
    item=next((row for row in final["artifacts"] if row["path"]==path),None)
    if not item:raise AssertionError("missing artifact "+path)
    response=await client.get(f"api/artifacts/{item['id']}");response.raise_for_status();return response.json()


async def main():
    report={"source":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),"cases":[]}
    async with httpx.AsyncClient(base_url=BASE,timeout=None,trust_env=False) as client:
        async def record(case,runner):
            started=time.monotonic()
            try:value=await runner();status="passed";error=""
            except Exception as exc:value={};status="pending_attribution";error=type(exc).__name__+": "+str(exc)
            report["cases"].append({"id":case,"status":status,"seconds":round(time.monotonic()-started,3),"error":error,**value})

        async def interaction_question():
            title="LIVE-I01-"+uuid.uuid4().hex[:8];thread=await create_thread(client,"claude",title)
            turn=await start_turn(client,thread,"Use sandbox-interact question --prompt 'Choose output format' --question format='Which format?' --option mp4 --option webm. Parse its JSON response. Write /workspace/outputs/interaction.json with the selected format, verify it, then reply.")
            pending=await wait_interaction(client,thread["id"])
            token=client.cookies.get("sandbox_harness_session")
            env={**os.environ,"NODE_PATH":"/data/wujing03/node_modules","UI_SESSION_TOKEN":token,"UI_THREAD_TITLE":title}
            subprocess.run(["node","tests/browser_live_interaction.cjs"],cwd=Path(__file__).parents[1],env=env,check=True)
            final=await wait_turn(client,turn["id"])
            if final["state"]!="COMPLETED":raise AssertionError(str(final))
            value=await artifact_json(client,final,"interaction.json")
            if value.get("format")!="mp4":raise AssertionError(str(value))
            return {"turn_id":turn["id"],"interaction_id":pending["id"],"artifact":value}
        await record("I01",interaction_question)

        async def approval_reject():
            thread=await create_thread(client,"codex","LIVE-I02-"+uuid.uuid4().hex[:8])
            turn=await start_turn(client,thread,"Run sandbox-interact approval --prompt 'Create protected marker?'. Parse the JSON. If rejected, do not create /workspace/project/protected.txt and write /workspace/outputs/approval.json as {\"decision\":\"rejected\",\"protected_exists\":false}. Verify it and reply.")
            pending=await wait_interaction(client,thread["id"])
            response=await client.post(f"api/interactions/{pending['id']}/respond",json={"idempotency_key":"reject-"+uuid.uuid4().hex,"action":"reject","answers":{}});response.raise_for_status()
            final=await wait_turn(client,turn["id"])
            if final["state"]!="COMPLETED":raise AssertionError(str(final))
            value=await artifact_json(client,final,"approval.json")
            if value!={"decision":"rejected","protected_exists":False}:raise AssertionError(str(value))
            return {"turn_id":turn["id"],"artifact":value}
        await record("I02",approval_reject)

        async def background_success():
            thread=await create_thread(client,"claude","LIVE-B01-"+uuid.uuid4().hex[:8])
            prompt="Create /workspace/project/bg.py that sleeps 3 seconds, writes /workspace/outputs/background.json as {\"done\":true}, and prints BG_DONE. Run it with sandbox-background start --name delayed -- python /workspace/project/bg.py. After delegation, reply briefly; the Runtime will supervise completion."
            turn=await start_turn(client,thread,prompt);final=await wait_turn(client,turn["id"])
            if final["state"]!="COMPLETED":raise AssertionError(str(final))
            return {"turn_id":turn["id"],"artifact":await artifact_json(client,final,"background.json")}
        await record("B01",background_success)

        async def background_failure():
            thread=await create_thread(client,"deepseek","LIVE-B02-"+uuid.uuid4().hex[:8])
            turn=await start_turn(client,thread,"Run exactly: sandbox-background start --name expected-failure -- python -c 'import time,sys;time.sleep(2);sys.exit(7)'. Then reply briefly without retrying or hiding the failure.")
            final=await wait_turn(client,turn["id"])
            if final["state"]!="FAILED" or final["error_code"]!="background_job_failed":raise AssertionError(str(final))
            return {"turn_id":turn["id"],"observed_failure":final["error_code"]}
        await record("B02",background_failure)

        async def background_cancel():
            thread=await create_thread(client,"codex","LIVE-B03-"+uuid.uuid4().hex[:8])
            turn=await start_turn(client,thread,"Run exactly: sandbox-background start --name cancellable -- python -c 'import time;time.sleep(60)'. Then reply briefly. Do not wait using another command.")
            job=None
            for _ in range(240):
                rows=(await client.get(f"api/turns/{turn['id']}/background-jobs")).json()
                job=next((row for row in rows if row["status"]=="running"),None)
                if job:break
                current=(await client.get(f"api/turns/{turn['id']}")).json()
                if current["state"] in TERMINAL:
                    raise RuntimeError("Turn ended before creating a background job: "+current["state"]+" "+current.get("error_code",''))
                await asyncio.sleep(.25)
            if not job:raise TimeoutError("background job")
            response=await client.post(f"api/turns/{turn['id']}/background-jobs/{job['id']}/cancel");response.raise_for_status()
            final=await wait_turn(client,turn["id"])
            if final["state"]!="FAILED" or final["error_code"]!="background_job_failed":raise AssertionError(str(final))
            return {"turn_id":turn["id"],"job_id":job["id"],"observed_failure":final["error_code"]}
        await record("B03",background_cancel)

        async def workspace_delete():
            thread=await create_thread(client,"claude","LIVE-W03-"+uuid.uuid4().hex[:8])
            response=await client.post(f"api/threads/{thread['id']}/project-files",params={"path":"assets/remove.txt","mime":"text/plain"},content=b"remove-me");response.raise_for_status();file=response.json()
            first=await wait_turn(client,(await start_turn(client,thread,"Verify /workspace/project/assets/remove.txt exists, then reply without modifying it."))["id"])
            response=await client.delete(f"api/project-files/{file['id']}");response.raise_for_status()
            second_turn=await start_turn(client,thread,"Verify /workspace/project/assets/remove.txt does not exist. Write /workspace/outputs/deleted.json as {\"absent\":true}, verify it, and reply.")
            second=await wait_turn(client,second_turn["id"])
            if first["sandbox_id"]!=second["sandbox_id"]:raise AssertionError("attached sandbox was not reused")
            return {"turn_id":second["id"],"same_sandbox":True,"artifact":await artifact_json(client,second,"deleted.json")}
        await record("W03",workspace_delete)

        for harness in MODELS:
            async def casual(h=harness):
                thread=await create_thread(client,h,"LIVE-H04-"+h+"-"+uuid.uuid4().hex[:6]);turn=await start_turn(client,thread,"你好，请只回复一句简短问候，不要创建或修改任何文件。")
                final=await wait_turn(client,turn["id"])
                if final["state"]!="COMPLETED" or final["artifacts"]:raise AssertionError(str(final))
                return {"turn_id":turn["id"],"harness":h}
            await record("H04-"+harness,casual)
    Path(".data/native-features-report.json").write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps(report,ensure_ascii=False,indent=2))
    if any(row["status"]!="passed" for row in report["cases"]):raise SystemExit(1)


if __name__=="__main__":asyncio.run(main())
