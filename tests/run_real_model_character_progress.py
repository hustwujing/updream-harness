from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import time
import uuid

import httpx

from sandbox_harness.common.configuration.config import load_settings
from sandbox_harness.common.events.events import RedisEventBus


TERMINAL={"COMPLETED","WAITING_USER","FAILED","CANCELLED","PAUSED"}


async def remove_thread(client:httpx.AsyncClient,thread_id:str)->None:
    response=None
    for _ in range(120):
        response=await client.delete(f"api/threads/{thread_id}")
        if response.status_code!=409:break
        try:detail=response.json().get("detail")
        except (TypeError,ValueError):detail=""
        if detail!="active_thread_cannot_be_deleted":response.raise_for_status()
        await asyncio.sleep(.5)
    response.raise_for_status()
    if response.status_code==204:return
    value=response.json()
    if value.get("state")!="pending":return
    operation_id=value["operation_id"]
    for _ in range(240):
        current=(await client.get(f"api/lifecycle/{operation_id}"));current.raise_for_status()
        state=current.json().get("state")
        if state=="completed":return
        if state=="refused":raise RuntimeError("thread_cleanup_refused")
        await asyncio.sleep(.5)
    raise TimeoutError("thread_cleanup_timeout")


async def main()->None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--base-url",default="http://127.0.0.1:24767/")
    parser.add_argument("--report",default=".data/evidence/model-character-progress.json")
    args=parser.parse_args();settings=load_settings();thread_id="";turn_id=""
    bus=RedisEventBus(settings.redis_url,settings.redis_prefix)
    observed=[];graph_observed=[];started=time.monotonic()
    async with httpx.AsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
        session=await client.get("api/session");session.raise_for_status()
        await bus.initialize();assert bus.available
        try:
            response=await client.post("api/threads",json={"harness":"codex","model":"gpt-5.6",
                "reasoning_level":"standard","title":"Real model character progress"})
            response.raise_for_status();thread_id=response.json()["id"]
            response=await client.post(f"api/threads/{thread_id}/turns",json={
                "message":"请不要调用任何工具，直接用中文连续写一段不少于八百字的产品设计分析。不要使用表格。",
                "attachments":[],"idempotency_key":"character-progress-"+uuid.uuid4().hex,
                "harness":"codex","model":"gpt-5.6","reasoning_level":"standard",
                "delivery_mode":"start"})
            response.raise_for_status();turn_id=response.json()["id"]
            final=None
            for _ in range(900):
                live=await bus.latest_live("model-turn",{turn_id})
                for value in reversed(live.get(turn_id,[])):
                    count=value.get("output_characters")
                    if (value.get("kind")=="model_character_progress" and isinstance(count,int)
                            and count>0 and (not observed or count>observed[-1])):
                        observed.append(count)
                graph=await client.get(f"api/threads/{thread_id}/execution-graph")
                graph.raise_for_status()
                node=next((item for item in graph.json().get("nodes",[])
                    if item.get("id")==f"turn:{turn_id}:primary"),None)
                if node:
                    values=[int(item.get("output_characters") or 0)
                        for item in node.get("model_rounds",[]) if item.get("status")=="in_progress"]
                    if values and max(values)>0 and (not graph_observed or max(values)>graph_observed[-1]):
                        graph_observed.append(max(values))
                current=await client.get(f"api/turns/{turn_id}");current.raise_for_status()
                final=current.json()
                if final["state"] in TERMINAL:break
                await asyncio.sleep(.2)
            if not final or final["state"]!="COMPLETED":
                raise AssertionError(f"real_turn_not_completed:{final and final.get('state')}")
            if len(observed)<2 or observed!=sorted(set(observed)):
                raise AssertionError(f"live_character_progress_not_incremental:{observed}")
            if not graph_observed:
                raise AssertionError("execution_graph_missing_live_character_progress")
            report={"passed":True,"thread_id":thread_id,"turn_id":turn_id,
                "elapsed_seconds":round(time.monotonic()-started,3),
                "redis_samples":observed,"graph_samples":graph_observed,
                "final_state":final["state"]}
            target=Path(args.report);target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
            print(json.dumps(report,ensure_ascii=False))
        finally:
            if thread_id:await remove_thread(client,thread_id)
            await bus.close()


if __name__=="__main__":asyncio.run(main())
