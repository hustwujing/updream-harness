#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
from fractions import Fraction
import io
from datetime import datetime,timezone
import json
import mimetypes
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time
import uuid

import httpx
from PIL import Image,ImageChops,ImageOps,ImageStat


ROOT=Path(__file__).resolve().parents[1]
TERMINAL={"COMPLETED","FAILED","CANCELLED","AWAITING_USER","WAITING_USER"}
DEFECT_CLASSES={"program_failure","optimizable_prompt"}
MEDIA_SUFFIXES={"video":{'.mp4','.mov','.mkv','.webm'},"audio":{'.wav','.mp3','.m4a','.aac','.flac','.ogg'},
    "image":{'.png','.jpg','.jpeg','.webp','.gif','.bmp','.tiff'}}
INTERVENTIONS={
    "intervention_rename":"把交付文件名改为 revised-output.png，其余要求不变。",
    "intervention_resize_format":"把最终尺寸改为256×128，并同时交付PNG和WebP。",
    "intervention_reorder":"改为rocket在左、portrait在右，其余要求不变。",
    "intervention_add_format":"在原交付之外追加一份WebP，不要重做已正确的中间成果。",
    "intervention_change_brightness":"只把portrait分支亮度改为1.2，保留其他已完成成果。",
    "intervention_replace_source":"只把左侧素材替换为poster.png，保留不受影响的成果。",
    "intervention_cover":"把封面改用rocket.png，不改变视频内容。",
    "intervention_caption":"新增字幕：0–2秒显示“PORTRAIT”，2–4秒显示“ROCKET”；白字黑描边、底部居中。",
    "intervention_portrait":"最终改为360×640竖版、时长8秒。",
    "intervention_thumbnail":"在已完成交付之外追加320×180 JPEG缩略图，保持比例居中裁切。",
    "intervention_feedback":("不接受当前候选。只修复 portrait 半边右下角的黑色头盔和台座区域，"
        "用与相邻背景连续的颜色填充该区域后重新审查；rocket 半边保持不变，不需要再询问。"),
    "intervention_new_goal":"保留已有成果，再追加一张128×128方形联系图。",
    "intervention_pause_replan":"停止旧版输出方向，改为无间隙竖向拼接。",
    "intervention_cancel_race":"取消当前导出；保留可复用中间成果并导出WebP新版。",
    "intervention_refresh":"继续刚才的修订，先说明已恢复到哪个阶段。",
    "intervention_chain":"将当前版本作为V1，再生成亮度1.1的V2和灰度V3。",
    "intervention_scope_successor":"基于已交付和原素材，追加32×24 successor 图片。",
}
INTERVENTION_ASSETS={"intervention_replace_source":["poster.png"]}
AFTER_ARTIFACT_INTERVENTIONS={"intervention_thumbnail","intervention_new_goal",
    "intervention_chain","intervention_scope_successor","intervention_feedback"}
INPUT_ACCEPTING={"QUEUED","RESTORING","STARTING","RUNNING","AWAITING_USER"}


class ResilientAsyncClient(httpx.AsyncClient):
    def __init__(self,*args,reconnect_seconds:float=90,**kwargs):
        super().__init__(*args,**kwargs);self.reconnect_seconds=max(0,float(reconnect_seconds))

    async def request(self,method,url,*args,**kwargs):
        safe=method.upper() in {"GET","HEAD","OPTIONS"}
        deadline=time.monotonic()+self.reconnect_seconds;delay=.25
        while True:
            try:return await super().request(method,url,*args,**kwargs)
            except (httpx.ConnectError,httpx.ReadError,httpx.RemoteProtocolError):
                if not safe or time.monotonic()>=deadline:raise
                await asyncio.sleep(delay);delay=min(2,delay*2)


def now():return datetime.now(timezone.utc).isoformat()


async def upload(client,path):
    mime=mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    response=await client.post("api/uploads",params={"name":path.name,"mime":mime},content=path.read_bytes())
    response.raise_for_status();return response.json()


async def list_all_items(client,thread_id):
    values=[];after=0
    while True:
        response=await client.get(f"api/threads/{thread_id}/items",
            params={"after":after,"limit":500});response.raise_for_status();rows=response.json()
        if not rows:return values
        values.extend(rows);after=max(int(row.get("sequence",0)) for row in rows)
        if len(rows)<500:return values


async def wait_for_first_artifact(client,thread_id,turn_id,deadline,stable_polls=10):
    stable_terminal_polls=0
    while time.monotonic()<deadline:
        if any(row.get('type')=='artifact' for row in await list_all_items(client,thread_id)):
            return True
        current=await client.get(f"api/turns/{turn_id}");current.raise_for_status()
        if current.json()["state"] not in TERMINAL:
            stable_terminal_polls=0
        else:
            turns_response=await client.get(f"api/threads/{thread_id}/turns")
            turns_response.raise_for_status()
            response=await client.get(f"api/threads/{thread_id}/execution-graph",
                params={"include_history":"true"});response.raise_for_status()
            active=any(row.get('state') not in TERMINAL for row in turns_response.json()) or any(
                row.get('type') in {'job','job_group'} and row.get('state') in {
                'CREATED','QUEUED','COMMITTED','RUNNING','WAITING_EXTERNAL','CANCELLING'}
                for row in response.json().get('nodes',[]))
            stable_terminal_polls=0 if active else stable_terminal_polls+1
            if stable_terminal_polls>=stable_polls:return False
        await asyncio.sleep(1)
    raise TimeoutError('intervention_artifact_timeout')


def classify(final,artifacts,media_jobs=None):
    state=final.get("state","");code=final.get("error_code","") or ""
    if state=="COMPLETED" and artifacts:return "success",""
    failed_media=next((job for job in (media_jobs or [])
        if job.get("status") in {"failed","unknown","cancelled"}),None)
    if failed_media:
        code=failed_media.get("error_code","") or failed_media.get("status","")
        if code.startswith(("provider_","model_","upstream_")) or code in {
                "budget_exhausted","content_policy_rejected","no_target_found"}:
            return "non_program_failure",code
        return "program_failure",code or "media_job_failed"
    if state=="COMPLETED":return "optimizable_prompt","completed_without_artifact"
    if state in {"AWAITING_USER","WAITING_USER"}:
        return "optimizable_prompt",final.get("error_detail") or "unclear_user_intervention"
    if code.startswith(("provider_","model_","upstream_")) or code in {
            "budget_exhausted","content_policy_rejected","no_target_found"}:
        return "non_program_failure",code
    return "program_failure",code or state.lower() or "unknown_failure"


def embedded_error_code(detail):
    decoder=json.JSONDecoder()
    for index,value in enumerate(str(detail or '')):
        if value!='{':continue
        try:payload,_=decoder.raw_decode(str(detail)[index:])
        except json.JSONDecodeError:continue
        code=payload.get('detail') if isinstance(payload,dict) else ''
        if isinstance(code,str) and re.fullmatch(r'[a-z][a-z0-9_.-]{0,159}',code):return code
    return ''


def classify_job_failures(rows):
    outcomes=[]
    for row in rows:
        result=row.get('result') or {}
        code=embedded_error_code(result.get('error_detail')) or str(
            result.get('error_code') or row.get('state','')).lower()
        detail=str(result.get('error_detail') or '').lower()
        non_program=(code.startswith(('provider_','model_','upstream_')) or code in {
            'budget_exhausted','content_policy_rejected','no_target_found'} or ' 429 ' in (
                ' '+detail+' '))
        outcomes.append(('non_program_failure' if non_program else 'program_failure',code))
    return (next((value for value in outcomes if value[0]=='program_failure'),None)
        or (outcomes[0] if outcomes else None))


async def failed_job_results(client,thread_id):
    response=await client.get(f"api/threads/{thread_id}/execution-graph",
        params={"include_history":"true"});response.raise_for_status()
    identities=[row.get('job_id') for row in response.json().get('nodes',[])
        if row.get('type')=='job' and row.get('state') in {'FAILED','BLOCKED','UNKNOWN'}
        and row.get('job_id')]
    rows=[]
    for identity in identities:
        detail=await client.get(f'api/jobs/{identity}');detail.raise_for_status();rows.append(detail.json())
    return rows


def waiting_interaction_reason(interactions,turn_id):
    pending=[row for row in interactions
        if row.get('turn_id')==turn_id and row.get('status')=='pending']
    if len(pending)!=1:return 'unexpected_user_decision'
    prompt=str((pending[0].get('request') or {}).get('prompt') or '').strip()
    return 'unexpected_user_decision:'+prompt[:500] if prompt else 'unexpected_user_decision'


def artifact_kind(name,mime):
    prefix=(mime or "").split("/",1)[0]
    if prefix in MEDIA_SUFFIXES:return prefix
    suffix=Path(name).suffix.lower()
    return next((kind for kind,values in MEDIA_SUFFIXES.items() if suffix in values),
        "text" if prefix=="text" or suffix in {'.txt','.srt','.json','.csv','.md'} else "other")


def probe_file(path):
    value=json.loads(subprocess.check_output(["ffprobe","-v","error","-show_streams",
        "-show_format","-of","json",str(path)],text=True))
    streams=value.get("streams",[]);video=next((row for row in streams if row.get("codec_type")=="video"),{})
    audio=next((row for row in streams if row.get("codec_type")=="audio"),{})
    duration=float(value.get("format",{}).get("duration") or video.get("duration")
        or audio.get("duration") or 0)
    rate=video.get("avg_frame_rate") or "0/1"
    try:fps=float(Fraction(rate))
    except (ValueError,ZeroDivisionError):fps=0
    return {"duration":duration,"width":int(video.get("width") or 0),
        "height":int(video.get("height") or 0),"fps":fps,"audio":bool(audio),
        "channels":int(audio.get("channels") or 0),"sample_rate":int(audio.get("sample_rate") or 0)}


def volume_is_silent(report,threshold_db=-80.0):
    match=re.search(r"max_volume:\s+(-inf|[-+]?\d+(?:\.\d+)?)\s+dB",report)
    return bool(match and (match.group(1)=="-inf" or float(match.group(1))<=threshold_db))


def image_source_matches(image,source_names,asset_root):
    target=image.convert('RGB');count=len(source_names)
    if not count or target.width%count:return []
    width=target.width//count;sources=[]
    for name in source_names:
        with Image.open(asset_root/name) as source:
            sources.append(ImageOps.fit(source.convert('RGB'),(width,target.height),
                method=Image.Resampling.LANCZOS,centering=(.5,.5)))
    matches=[]
    for index in range(count):
        segment=target.crop((index*width,0,(index+1)*width,target.height))
        distances=[sum(value*value for value in ImageStat.Stat(
            ImageChops.difference(segment,source)).rms) for source in sources]
        matches.append(source_names[min(range(count),key=distances.__getitem__)])
    return matches


async def validate_artifacts(client,case,artifacts,asset_root=None):
    expected=case.get("expect",{});records=[]
    temp_root=ROOT/".data/evidence/real-campaign/validation";temp_root.mkdir(parents=True,exist_ok=True)
    for item in artifacts:
        payload=item.get("payload") or {};identity=payload.get("artifact_id","")
        name=item.get("summary","");mime=payload.get("mime","")
        if not identity:
            records.append({"name":name,"kind":"other","error":"missing_artifact_id"});continue
        response=await client.get(f"api/artifacts/{identity}");response.raise_for_status();data=response.content
        record={"name":name,"kind":artifact_kind(name,mime),"mime":mime,"size":len(data)}
        try:
            if record["kind"]=="image":
                with Image.open(io.BytesIO(data)) as image:
                    record.update(width=image.width,height=image.height,
                        alpha="A" in image.getbands())
                    source_order=expected.get('source_order',[])
                    if (source_order and asset_root and image.width==expected.get('width')
                            and image.height==expected.get('height')):
                        record['source_matches']=image_source_matches(image,source_order,asset_root)
            elif record["kind"] in {"video","audio"}:
                suffix=Path(name).suffix or (".mp4" if record["kind"]=="video" else ".wav")
                with tempfile.NamedTemporaryFile(dir=temp_root,suffix=suffix) as handle:
                    handle.write(data);handle.flush();record.update(probe_file(Path(handle.name)))
                    if expected.get("audio_silent") and record["kind"]=="video":
                        measured=subprocess.run(["ffmpeg","-hide_banner","-nostats","-i",handle.name,
                            "-map","0:a:0","-af","volumedetect","-f","null","-"],
                            stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True,check=False).stderr
                        record["audio_silent"]=volume_is_silent(measured)
            elif record["kind"]=="text":record["text"]=data.decode("utf-8","replace")
        except Exception as exc:record["error"]=type(exc).__name__+":"+str(exc)[:200]
        records.append(record)
    errors=[]
    if any(row.get("error") for row in records):
        errors.extend(row["name"]+":"+row["error"] for row in records if row.get("error"))
    for row in records:
        if 'source_matches' not in row:continue
        for index,(actual,wanted) in enumerate(zip(row.get('source_matches',[]),
                expected.get('source_order',[]),strict=True)):
            if actual!=wanted:
                errors.append(f"source_order:{row['name']}:{index}:{actual}:{wanted}")
    kind=expected.get("kind");primary_kind="video" if kind=="video_variants" else kind
    primary=[row for row in records if row["kind"]==primary_kind]
    if kind and not primary:errors.append("missing_"+primary_kind)
    if expected.get("min_images",0)>sum(row["kind"]=="image" for row in records):errors.append("min_images")
    if expected.get("min_audio",0)>sum(row["kind"]=="audio" for row in records):errors.append("min_audio")
    extensions={Path(row["name"]).suffix.lower() for row in records}
    for extension in ([expected["extension"]] if expected.get("extension") else expected.get("extensions",[])):
        if extension.lower() not in extensions:errors.append("missing_extension:"+extension)
    if expected.get("extra_extension") and expected["extra_extension"].lower() not in extensions:
        errors.append("missing_extra_extension:"+expected["extra_extension"])
    combined="\n".join(row.get("text","") for row in records).casefold()
    for term in expected.get("terms",[]):
        if str(term).casefold() not in combined:errors.append("missing_term:"+str(term))
    if primary:
        dimensions={(row.get("width"),row.get("height")) for row in primary}
        if expected.get("width") and (expected["width"],expected["height"]) not in dimensions:
            errors.append("dimensions:"+str(sorted(dimensions)))
        tolerance=float(expected.get("duration_tolerance",.5))
        if expected.get("duration") is not None and not any(
                abs(row.get("duration",-999)-expected["duration"])<=tolerance for row in primary):
            errors.append("duration:"+str([round(row.get('duration',0),3) for row in primary]))
        if expected.get("fps") and not any(abs(row.get("fps",0)-expected["fps"])<.1 for row in primary):
            errors.append("fps:"+str([row.get('fps',0) for row in primary]))
        if "audio" in expected and not any(row.get("audio")==expected["audio"] for row in primary):
            errors.append("audio_track")
        if expected.get("audio_silent") and not any(row.get("audio_silent") for row in primary):
            errors.append("audio_not_silent")
        if expected.get("alpha") and not any(row.get("alpha") for row in primary):errors.append("alpha")
        if expected.get("channels") and not any(row.get("channels")==expected["channels"] for row in primary):
            errors.append("channels")
        if expected.get("sample_rate") and not any(row.get("sample_rate")==expected["sample_rate"] for row in primary):
            errors.append("sample_rate")
    if expected.get("sizes"):
        actual={(row.get("width"),row.get("height")) for row in primary}
        for size in expected["sizes"]:
            if tuple(size) not in actual:errors.append("missing_size:"+"x".join(map(str,size)))
    for key,kind_name in (("extra_alpha","image"),("extra_text","text"),
            ("extra_audio","audio"),("extra_image","image")):
        if expected.get(key) and not any(row["kind"]==kind_name and (
                row.get("alpha") if key=="extra_alpha" else True) for row in records):errors.append(key)
    return errors,[{key:value for key,value in row.items() if key!="text"} for row in records]


async def wait_turn(client,turn_id,running,identity,timeout):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        response=await client.get(f"api/turns/{turn_id}")
        if response.status_code==200:
            value=response.json();running[identity]["state"]=value["state"]
            if value["state"] in TERMINAL:return value
        await asyncio.sleep(1)
    raise TimeoutError("turn_timeout")


async def deliver_intervention(client,thread,turn,message,attachments):
    response=await client.get(f"api/turns/{turn['id']}");response.raise_for_status();current=response.json()
    if current["state"] in INPUT_ACCEPTING:
        if current["state"]=="AWAITING_USER":
            response=await client.get(f"api/threads/{thread['id']}/interactions");response.raise_for_status()
            pending=[row for row in response.json()
                if row["turn_id"]==turn["id"] and row["status"]=="pending"]
            if len(pending)==1:
                interaction=pending[0];questions=interaction.get("request",{}).get("questions") or []
                answers={str(row.get("id") or "answer"):message for row in questions} or {"answer":message}
                action="answer" if interaction["kind"]=="question" else "reject"
                response=await client.post(f"api/interactions/{interaction['id']}/respond",json={
                    "idempotency_key":"campaign-interaction-"+uuid.uuid4().hex,
                    "action":action,"answers":answers})
                response.raise_for_status()
                if not attachments:return {"mode":"interaction","turn_id":turn["id"]}
        if current["state"]!="AWAITING_USER":
            response=await client.post(f"api/turns/{turn['id']}/inputs",json={"message":message,
                "attachments":attachments,"idempotency_key":"campaign-intervention-"+uuid.uuid4().hex})
            response.raise_for_status();return {"mode":"input","turn_id":turn["id"],
                "input_id":response.json()["id"]}
    response=await client.post(f"api/threads/{thread['id']}/turns",json={"message":message,
        "attachments":attachments,"idempotency_key":"campaign-intervention-"+uuid.uuid4().hex,
        "delivery_mode":"queue"})
    response.raise_for_status();return {"mode":"turn","turn_id":response.json()["id"]}


async def settle_media_workflow(client,thread_id,initial_final,running,identity,timeout,
                                *,allow_external):
    """Drive confirmation cards and wait for every durable continuation Turn."""
    deadline=time.monotonic()+timeout
    final=initial_final;media_jobs=[];continuation_after=None;stable_signature=None;stable_count=0
    while time.monotonic()<deadline:
        response=await client.get(f"api/threads/{thread_id}/turns");response.raise_for_status()
        turns=response.json()
        for turn in turns:
            if turn["state"] not in TERMINAL:
                final=await wait_turn(client,turn["id"],running,identity,
                    max(1,deadline-time.monotonic()))
            elif turn["id"]==turns[-1]["id"]:
                final=turn

        response=await client.get(f"api/threads/{thread_id}/job-group-drafts");response.raise_for_status()
        job_drafts=response.json();awaiting_jobs=[row for row in job_drafts
            if row["state"]=="AWAITING_CONFIRMATION"]
        if awaiting_jobs:
            if not allow_external:return final,media_jobs
            for draft in awaiting_jobs:
                response=await client.post(f"api/job-group-drafts/{draft['id']}/confirm")
                response.raise_for_status()
            running[identity]["state"]="WAITING_JOB_CONFIRMATION";stable_count=0;continue

        response=await client.get(f"api/threads/{thread_id}/media/drafts");response.raise_for_status()
        drafts=response.json();awaiting=[row for row in drafts if row["status"]=="awaiting_confirmation"]
        if awaiting:
            if not allow_external:
                return final,media_jobs
            continuation_after=len(turns)
            for draft in awaiting:
                response=await client.post(f"api/media/drafts/{draft['id']}/confirm")
                response.raise_for_status()
            running[identity]["state"]="WAITING_MEDIA"
            continue

        response=await client.get(f"api/threads/{thread_id}/media/jobs");response.raise_for_status()
        media_jobs=response.json()
        active=[row for row in media_jobs if row["status"] not in {
            "succeeded","failed","cancelled","unknown"}]
        if active:
            phases=sorted({row.get("phase") or row["status"] for row in active})
            running[identity]["state"]="MEDIA_"+"_".join(phases).upper()
            await asyncio.sleep(1);continue

        if continuation_after is not None:
            response=await client.get(f"api/threads/{thread_id}/turns");response.raise_for_status()
            turns=response.json()
            if len(turns)<=continuation_after:
                running[identity]["state"]="WAITING_CONTINUATION"
                await asyncio.sleep(1);continue
            for turn in turns[continuation_after:]:
                if turn["state"] not in TERMINAL:
                    final=await wait_turn(client,turn["id"],running,identity,
                        max(1,deadline-time.monotonic()))
                else:final=turn
            continuation_after=None
            continue
        response=await client.get(f"api/threads/{thread_id}/execution-graph",
            params={"include_history":"true"});response.raise_for_status()
        graph=response.json();groups=[row for row in graph.get("nodes",[]) if row.get("type")=="job_group"]
        active_groups=[row for row in groups if row.get("state") in {"COMMITTED","RUNNING"}]
        active_jobs=[row for row in graph.get("nodes",[]) if row.get("type")=="job"
            and row.get("state") in {"CREATED","QUEUED","RUNNING","WAITING_EXTERNAL","CANCELLING"}]
        if active_groups or active_jobs:
            running[identity]["state"]="JOB_GROUP_RUNNING";stable_count=0
            await asyncio.sleep(1);continue
        signature=(tuple((turn["id"],turn["state"]) for turn in turns),
            tuple((row["id"],row["state"]) for row in groups),
            tuple((row["id"],row["status"]) for row in drafts),
            tuple((row["id"],row["status"]) for row in media_jobs))
        stable_count=stable_count+1 if signature==stable_signature else 1;stable_signature=signature
        if stable_count>=3:return final,media_jobs
        running[identity]["state"]="WAITING_CONTINUATION_QUIESCENCE"
        await asyncio.sleep(1)
    raise TimeoutError("media_workflow_timeout")


async def primary_case(client,case,args,running):
    attachments=[await upload(client,args.asset_root/name) for name in case.get("assets",[])]
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
        if intervention in AFTER_ARTIFACT_INTERVENTIONS:
            deadline=time.monotonic()+args.timeout
            if not await wait_for_first_artifact(client,thread['id'],turn['id'],deadline):return
        else:await asyncio.sleep(args.intervention_delay)
        message=INTERVENTIONS.get(intervention)
        if not message:return
        intervention_attachments=[await upload(client,args.asset_root/name)
            for name in INTERVENTION_ASSETS.get(intervention,[])]
        delivery=await deliver_intervention(client,thread,turn,message,intervention_attachments)
        running[case["id"]]["intervention_delivery"]=delivery;return delivery
    intervention_task=asyncio.create_task(send_intervention())
    final=await wait_turn(client,turn["id"],running,case["id"],args.timeout)
    intervention_delivery=await intervention_task
    if intervention_delivery and intervention_delivery["mode"]=="turn":
        final=await wait_turn(client,intervention_delivery["turn_id"],running,case["id"],args.timeout)
    elif intervention_delivery and final["state"]=="AWAITING_USER":
        for _ in range(100):
            await asyncio.sleep(.1)
            current=(await client.get(f"api/turns/{turn['id']}")).json()
            if current["state"]!="AWAITING_USER":break
        final=await wait_turn(client,turn["id"],running,case["id"],args.timeout)
    final,media_jobs=await settle_media_workflow(client,thread["id"],final,running,case["id"],
        args.timeout,allow_external=bool(case.get("allowed_external_operations")))
    job_failures=await failed_job_results(client,thread['id'])
    items=await list_all_items(client,thread['id'])
    artifacts=[row for row in items if row.get("type")=="artifact"]
    classification,reason=classify(final,artifacts,media_jobs)
    job_outcome=classify_job_failures(job_failures)
    if classification!='success' and job_outcome:classification,reason=job_outcome
    if final.get('state') in {'AWAITING_USER','WAITING_USER'}:
        response=await client.get(f"api/threads/{thread['id']}/interactions");response.raise_for_status()
        reason=waiting_interaction_reason(response.json(),final['id'])
    validation_errors,validation=await validate_artifacts(
        client,case,artifacts,asset_root=args.asset_root) if artifacts else ([],[])
    if classification=="success" and validation_errors:
        classification,reason=(job_outcome or (
            "program_failure","artifact_contract:"+",".join(validation_errors[:12])))
    return {"classification":classification,"reason":reason,"thread_id":thread["id"],
        "turn_id":turn["id"],"state":final["state"],"error_code":final.get("error_code",""),
        "artifact_count":len(artifacts),"artifact_names":[row.get("summary","") for row in artifacts],
        "artifact_validation":validation,"validation_errors":validation_errors,
        "job_failures":[{'id':row.get('id'),'state':row.get('state'),
            'error_code':(row.get('result') or {}).get('error_code','')} for row in job_failures]}


async def protocol_case(case,args,running):
    report=Path(args.report).parent/(case["id"]+".json")
    process=await asyncio.create_subprocess_exec(str(ROOT/".venv/bin/python"),
        str(ROOT/"tests/run_real_parallel_protocol.py"),"--base-url",args.base_url,
        "--case",case["id"],"--report",str(report),"--harness",args.harness,
        "--model",args.model,"--reasoning",args.reasoning,"--timeout",str(args.timeout),
        "--intervention-delay",str(args.intervention_delay))
    running[case["id"]]["state"]="PROTOCOL_RUNNING"
    try:return_code=await asyncio.wait_for(process.wait(),args.timeout)
    except asyncio.TimeoutError:process.kill();await process.wait();raise TimeoutError("protocol_timeout")
    if report.is_file():value=json.loads(report.read_text())
    else:value={"classification":"program_failure","reason":"protocol_report_missing"}
    if return_code and value.get("classification") not in DEFECT_CLASSES:
        value.update(classification="program_failure",reason=value.get("reason") or "protocol_failed")
    return value


async def inherited_protocol_case(case,args,running):
    report=Path(args.report).parent/(case["id"]+".json")
    process=await asyncio.create_subprocess_exec(str(ROOT/".venv/bin/python"),
        str(ROOT/"tests/run_real_inherited_protocol.py"),"--base-url",args.base_url,
        "--case",case["id"],"--report",str(report),"--harness",args.harness,
        "--model",args.model,"--reasoning",args.reasoning)
    running[case["id"]]["state"]="INHERITED_PROTOCOL_RUNNING"
    try:return_code=await asyncio.wait_for(process.wait(),args.timeout)
    except asyncio.TimeoutError:process.kill();await process.wait();raise TimeoutError("inherited_protocol_timeout")
    if report.is_file():value=json.loads(report.read_text())
    else:value={"classification":"program_failure","reason":"inherited_protocol_report_missing"}
    if return_code and value.get("classification") not in DEFECT_CLASSES:
        value.update(classification="program_failure",reason=value.get("reason") or "inherited_protocol_failed")
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


def write_report_checkpoint(target:Path,*,revision:str,catalog:Path,selected_count:int,
                            started_count:int,stopped_for_defect:bool,results:dict)->None:
    report={"revision":revision,"finished_at":now(),"catalog":str(catalog),
        "selected_count":selected_count,"started_count":started_count,
        "stopped_for_defect":stopped_for_defect,"results":results}
    target.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w",dir=target.parent,delete=False) as stream:
        temporary=Path(stream.name)
        try:
            json.dump(report,stream,ensure_ascii=False,indent=2);stream.write("\n")
            stream.flush();os.fsync(stream.fileno());temporary.replace(target)
        finally:temporary.unlink(missing_ok=True)


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
    configured_root=Path(catalog.get('asset_root') or ROOT/'tests/real_cases/assets')
    args.asset_root=(configured_root if configured_root.is_absolute()
        else (args.catalog.parent/configured_root).resolve())
    if not args.asset_root.is_dir():raise ValueError('campaign_asset_root_missing:'+str(args.asset_root))
    selected=[row for row in catalog["cases"] if not explicit or row["id"] in explicit]
    prior={}
    if args.resume_report and args.resume_report.is_file():
        prior=json.loads(args.resume_report.read_text()).get("results",{})
        selected=[row for row in selected if (row["id"] in set(args.affected_case)
            or prior.get(row["id"],{}).get("classification")!="success")]
    running={};results=dict(prior);started_count=[0];stop=asyncio.Event();defect=asyncio.Event()
    revision=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    target=Path(args.report);report_lock=asyncio.Lock()
    queue=asyncio.Queue()
    for case in selected:queue.put_nowait(case)
    async with ResilientAsyncClient(base_url=args.base_url,timeout=None,trust_env=False) as client:
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
                        else await inherited_protocol_case(case,args,running)
                        if case["executor"]=="inherited_protocol" else await protocol_case(case,args,running))
                except Exception as exc:
                    value={"classification":"program_failure","reason":type(exc).__name__+":"+str(exc)[:500]}
                value.update(case_id=identity,title=case["title"],elapsed_seconds=round(time.monotonic()-started,3),
                    finished_at=now());results[identity]=value;running.pop(identity,None);queue.task_done()
                async with report_lock:
                    await asyncio.to_thread(write_report_checkpoint,target,revision=revision,
                        catalog=args.catalog,selected_count=len(selected),started_count=started_count[0],
                        stopped_for_defect=defect.is_set() or value["classification"] in DEFECT_CLASSES,
                        results=dict(results))
                print(json.dumps({"case_finished":identity,**value},ensure_ascii=False),flush=True)
                if value["classification"] in DEFECT_CLASSES:defect.set()
        reporter=asyncio.create_task(progress(len(selected),running,results,started_count,stop,args.progress_interval))
        await asyncio.gather(*(worker() for _ in range(max(1,min(8,args.concurrency)))))
        stop.set();await reporter
    await asyncio.to_thread(write_report_checkpoint,target,revision=revision,catalog=args.catalog,
        selected_count=len(selected),started_count=started_count[0],
        stopped_for_defect=defect.is_set(),results=dict(results))
    print(json.dumps({"report":str(target),"total":len(selected),"started":started_count[0],
        "success":sum(row.get("classification")=="success" for row in results.values()),
        "program_failure":sum(row.get("classification")=="program_failure" for row in results.values()),
        "non_program_failure":sum(row.get("classification")=="non_program_failure" for row in results.values()),
        "optimizable_prompt":sum(row.get("classification")=="optimizable_prompt" for row in results.values())},ensure_ascii=False))
    if defect.is_set():raise SystemExit(2)


if __name__=="__main__":asyncio.run(main())
