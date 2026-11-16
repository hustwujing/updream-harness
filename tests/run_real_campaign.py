#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime,timezone
import json
import mimetypes
from pathlib import Path
import statistics
import time
import uuid

import httpx


ROOT=Path(__file__).resolve().parents[1]
TERMINAL={"COMPLETED","FAILED","CANCELLED","WAITING_USER"}
DEFECT_CLASSES={"program_failure","optimizable_prompt"}
INTERVENTIONS={
    "intervention_rename":"把交付文件名改为 revised-output.png，其余要求不变。",
    "intervention_resize_format":"把最终尺寸改为256×128，并同时交付PNG和WebP。",
    "intervention_reorder":"改为rocket在左、portrait在右，其余要求不变。",
    "intervention_add_format":"在原交付之外追加一份WebP，不要重做已正确的中间成果。",
    "intervention_change_brightness":"只把portrait分支亮度改为1.2，保留其他已完成成果。",
    "intervention_replace_source":"只把左侧素材替换为poster.png，保留不受影响的成果。",
    "intervention_cover":"把封面改用rocket.png，不改变视频内容。",
    "intervention_caption":"字幕改为白字黑描边、底部居中。",
    "intervention_portrait":"最终改为360×640竖版、时长8秒。",
    "intervention_thumbnail":"在已完成交付之外追加320×180 JPEG缩略图。",
    "intervention_feedback":"当前候选的边缘有黑边，请只修复边缘后重新审查，不接受当前候选。",
    "intervention_new_goal":"保留已有成果，再追加一张128×128方形联系图。",
    "intervention_pause_replan":"停止旧版输出方向，改为无间隙竖向拼接。",
    "intervention_cancel_race":"取消当前导出；保留可复用中间成果并导出WebP新版。",
    "intervention_refresh":"继续刚才的修订，先说明已恢复到哪个阶段。",
    "intervention_chain":"将当前版本作为V1，再生成亮度1.1的V2和灰度V3。",
    "intervention_scope_successor":"基于已交付和原素材，追加32×24 successor 图片。",
}


def now():return datetime.now(timezone.utc).isoformat()


async def upload(client,path):
    mime=mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    response=await client.post("api/uploads",params={"name":path.name,"mime":mime},content=path.read_bytes())
    response.raise_for_status();return response.json()


def classify(final,artifacts):
    state=final.get("state","");code=final.get("error_code","") or ""
    if state=="COMPLETED" and artifacts:return "success",""
    if state=="COMPLETED":return "optimizable_prompt","completed_without_artifact"
    if state=="WAITING_USER":return "optimizable_prompt",final.get("error_detail") or "unclear_user_intervention"
    if code.startswith(("provider_","model_","upstream_")) or code in {
            "budget_exhausted","content_policy_rejected","no_target_found"}:
        return "non_program_failure",code
    return "program_failure",code or state.lower() or "unknown_failure"


async def wait_turn(client,turn_id,running,identity,timeout):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        response=await client.get(f"api/turns/{turn_id}")
        if response.status_code==200:
            value=response.json();running[identity]["state"]=value["state"]
            if value["state"] in TERMINAL:return value
        await asyncio.sleep(1)
    raise TimeoutError("turn_timeout")


async def primary_case(client,case,args,running):
    attachments=[await upload(client,ROOT/"tests/real_cases/assets"/name) for name in case.get("assets",[])]
    response=await client.post("api/threads",json={"harness":args.harness,"model":args.model,
        "reasoning_level":args.reasoning,"title":case["id"]+" "+case["title"]})
    response.raise_for_status();thread=response.json();running[case["id"]]["thread_id"]=thread["id"]
    response=await client.post(f"api/threads/{thread['id']}/turns",json={"message":case["prompt"],
        "attachments":attachments,"idempotency_key":"campaign-"+case["id"]+"-"+uuid.uuid4().hex,
        "delivery_mode":"start"})
    response.raise_for_status();turn=response.json();running[case["id"]]["turn_id"]=turn["id"]
    intervention=case.get("dynamic_hook")
    async def send_intervention():
        if not intervention:return
        await asyncio.sleep(args.intervention_delay)
        message=INTERVENTIONS.get(intervention)
        if not message:return
        value=await client.post(f"api/threads/{thread['id']}/turns",json={"message":message,
            "attachments":attachments,"idempotency_key":"campaign-intervention-"+case["id"]+"-"+uuid.uuid4().hex,
            "delivery_mode":"queue"})
        value.raise_for_status();identity=value.json()["id"]
        running[case["id"]]["intervention_turn_id"]=identity;return identity
    intervention_task=asyncio.create_task(send_intervention())
    final=await wait_turn(client,turn["id"],running,case["id"],args.timeout)
    intervention_turn=await intervention_task
    if intervention_turn:
        final=await wait_turn(client,intervention_turn,running,case["id"],args.timeout)
    response=await client.get(f"api/threads/{thread['id']}/items");response.raise_for_status()
    items=response.json();artifacts=[row for row in items if row.get("type")=="artifact"]
    classification,reason=classify(final,artifacts)
    return {"classification":classification,"reason":reason,"thread_id":thread["id"],
        "turn_id":turn["id"],"state":final["state"],"error_code":final.get("error_code",""),
        "artifact_count":len(artifacts),"artifact_names":[row.get("summary","") for row in artifacts]}


async def protocol_case(case,args,running):
    report=Path(args.report).parent/(case["id"]+".json")
    process=await asyncio.create_subprocess_exec(str(ROOT/".venv/bin/python"),
        str(ROOT/"tests/run_real_parallel_protocol.py"),"--base-url",args.base_url,
        "--case",case["id"],"--report",str(report))
    running[case["id"]]["state"]="PROTOCOL_RUNNING"
    try:return_code=await asyncio.wait_for(process.wait(),args.timeout)
    except asyncio.TimeoutError:process.kill();await process.wait();raise TimeoutError("protocol_timeout")
    if report.is_file():value=json.loads(report.read_text())
    else:value={"classification":"program_failure","reason":"protocol_report_missing"}
    if return_code and value.get("classification") not in DEFECT_CLASSES:
        value.update(classification="program_failure",reason=value.get("reason") or "protocol_failed")
    return value


async def progress(total,running,results,started_count,stop,interval):
    while not stop.is_set():
        try:await asyncio.wait_for(stop.wait(),interval);continue
        except asyncio.TimeoutError:pass
        durations=[row["elapsed_seconds"] for row in results.values() if "elapsed_seconds" in row]
        counts={kind:sum(row.get("classification")==kind for row in results.values())
            for kind in ("success","program_failure","non_program_failure","optimizable_prompt")}
        print(json.dumps({"progress":True,"total":total,"started":started_count[0],**counts,
            "average_seconds":round(statistics.mean(durations),2) if durations else 0,
            "running":[{"case":identity,"elapsed_seconds":round(time.monotonic()-row["started_monotonic"],1),
                "state":row.get("state","STARTING"),"expected":row.get("expected","")}
                for identity,row in sorted(running.items())]},ensure_ascii=False),flush=True)


async def main():
    parser=argparse.ArgumentParser();parser.add_argument("--base-url",default="http://127.0.0.1:24766/")
    parser.add_argument("--catalog",type=Path,default=ROOT/"tests/real_cases/cases.json")
    parser.add_argument("--report",default=str(ROOT/".data/evidence/real-campaign/latest.json"))
    parser.add_argument("--resume-report",type=Path);parser.add_argument("--case",action="append",default=[])
    parser.add_argument("--affected-case",action="append",default=[])
    parser.add_argument("--concurrency",type=int,default=8);parser.add_argument("--timeout",type=int,default=7200)
    parser.add_argument("--progress-interval",type=int,default=120);parser.add_argument("--intervention-delay",type=int,default=10)
    parser.add_argument("--harness",default="codex");parser.add_argument("--model",default="gpt-5.6-sol")
    parser.add_argument("--reasoning",default="deep");args=parser.parse_args()
    catalog=json.loads(args.catalog.read_text());explicit=set(args.case)|set(args.affected_case)
    selected=[row for row in catalog["cases"] if not explicit or row["id"] in explicit]
    prior={}
    if args.resume_report and args.resume_report.is_file():
        prior=json.loads(args.resume_report.read_text()).get("results",{})
        selected=[row for row in selected if (row["id"] in set(args.affected_case)
            or prior.get(row["id"],{}).get("classification")!="success")]
    running={};results=dict(prior);started_count=[0];stop=asyncio.Event();defect=asyncio.Event()
    queue=asyncio.Queue()
    for case in selected:queue.put_nowait(case)
    async with httpx.AsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
        session=await client.get("api/session");session.raise_for_status()
        async def worker():
            while not queue.empty() and not defect.is_set():
                try:case=queue.get_nowait()
                except asyncio.QueueEmpty:return
                identity=case["id"];started_count[0]+=1;started=time.monotonic()
                running[identity]={"started_monotonic":started,"state":"STARTING",
                    "expected":case.get("expect",{})}
                try:
                    value=(await primary_case(client,case,args,running) if case["executor"]=="primary_harness"
                        else await protocol_case(case,args,running))
                except Exception as exc:
                    value={"classification":"program_failure","reason":type(exc).__name__+":"+str(exc)[:500]}
                value.update(case_id=identity,title=case["title"],elapsed_seconds=round(time.monotonic()-started,3),
                    finished_at=now());results[identity]=value;running.pop(identity,None);queue.task_done()
                print(json.dumps({"case_finished":identity,**value},ensure_ascii=False),flush=True)
                if value["classification"] in DEFECT_CLASSES:defect.set()
        reporter=asyncio.create_task(progress(len(selected),running,results,started_count,stop,args.progress_interval))
        await asyncio.gather(*(worker() for _ in range(max(1,min(8,args.concurrency)))))
        stop.set();await reporter
    report={"revision":__import__('subprocess').check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
        "finished_at":now(),"catalog":str(args.catalog),"selected_count":len(selected),"started_count":started_count[0],
        "stopped_for_defect":defect.is_set(),"results":results}
    target=Path(args.report);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(
        json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({"report":str(target),"total":len(selected),"started":started_count[0],
        "success":sum(row.get("classification")=="success" for row in results.values()),
        "program_failure":sum(row.get("classification")=="program_failure" for row in results.values()),
        "non_program_failure":sum(row.get("classification")=="non_program_failure" for row in results.values()),
        "optimizable_prompt":sum(row.get("classification")=="optimizable_prompt" for row in results.values())},ensure_ascii=False))
    if defect.is_set():raise SystemExit(2)


if __name__=="__main__":asyncio.run(main())
