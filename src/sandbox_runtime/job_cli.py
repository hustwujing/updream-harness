from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel,ConfigDict,Field,model_validator

from sandbox_harness.job_contracts import JobGroupCreate
from .state import capability_token_file


def _json_value(raw: str):
    stripped = raw.lstrip()
    if stripped.startswith(("{", "[")):
        source = raw
    else:
        path = Path(raw)
        source = path.read_text() if path.is_file() else raw
    return json.loads(source)


def _value(raw: str) -> dict:
    value = _json_value(raw)
    if not isinstance(value, dict):
        raise ValueError("JobGroup payload must be a JSON object")
    return value


def _job_headers(token:str)->dict[str,str]:
    try:
        raw=token.split('.',1)[0];raw+='='*(-len(raw)%4)
        claims=json.loads(base64.urlsafe_b64decode(raw))
    except (ValueError,TypeError,json.JSONDecodeError):return {}
    if claims.get('scope')!='job':return {}
    mapping={'x-updream-thread-id':'thread_id','x-updream-turn-id':'turn_id',
        'x-updream-group-id':'group_id','x-updream-job-id':'job_id',
        'x-updream-job-generation':'generation','x-updream-source-sandbox':'source_identity',
        'x-updream-descriptor-id':'descriptor_id','x-updream-descriptor-version':'descriptor_version',
        'x-updream-descriptor-digest':'descriptor_digest','x-updream-child-profile-id':'child_profile_id',
        'x-updream-runtime-profile-id':'runtime_profile_id'}
    headers={key:str(claims.get(field,'')) for key,field in mapping.items()}
    headers.update({'x-updream-operation':'check',
        'x-updream-artifact-ids':json.dumps(claims.get('artifact_ids',[]),separators=(',',':')),
        'x-updream-skill-ids':json.dumps(claims.get('skill_ids',[]),separators=(',',':'))})
    return headers


class DelegationTaskSpec(BaseModel):
    model_config=ConfigDict(extra="forbid",frozen=True)
    id:str=Field(min_length=1,max_length=100,pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    objective:str=Field(min_length=1,max_length=20_000)
    inputs:tuple[str,...]=Field(default=(),max_length=100)
    skill_ids:tuple[str,...]=Field(default=(),max_length=32)
    result_mode:Literal["text","artifacts","mixed"]="artifacts"
    output_disposition:Literal["candidate","workspace","delivery"]|None=None
    max_items:int=Field(default=100,ge=1,le=1000)
    mime_patterns:tuple[str,...]=Field(default=(),max_length=100)
    operation_ids:tuple[str,...]=Field(default=(),max_length=32)
    capability_ids:tuple[str,...]=Field(default=(),max_length=32)

    @model_validator(mode="after")
    def validate_output_contract(self):
        if self.result_mode=="text" and self.output_disposition is not None:
            raise ValueError("output_disposition is invalid for text result_mode")
        if self.result_mode!="text" and self.output_disposition is None:
            raise ValueError("output_disposition is required for artifact result_mode")
        return self


class DelegationGroupSpec(BaseModel):
    model_config=ConfigDict(extra="forbid",frozen=True)
    tasks:tuple[DelegationTaskSpec,...]=Field(min_length=1,max_length=32)
    idempotency_key:str=Field(default="",max_length=160)
    suppress_continuation:bool=False


DELEGATION_EXAMPLE={"tasks":[
    {"id":"task-a","objective":"Complete one independent result","inputs":[],
        "skill_ids":[],"result_mode":"text"},
    {"id":"task-b","objective":"Complete the other independent result","inputs":[],
        "skill_ids":[],"result_mode":"artifacts","output_disposition":"delivery"},
]}


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),
        ensure_ascii=True).encode()).hexdigest()


def compile_delegation(value:dict,catalog:dict,environment:dict[str,str]|None=None)->dict:
    environment=environment or os.environ;spec=DelegationGroupSpec.model_validate(value)
    thread_id=environment.get("HARNESS_THREAD_ID","");turn_id=environment.get("HARNESS_TURN_ID","")
    harness=environment.get("HARNESS_KIND","");model=environment.get("HARNESS_MODEL","")
    if not thread_id or not turn_id or not harness or not model:
        raise ValueError("Runtime delegation identity is incomplete")
    profiles={item.get("id"):item for item in catalog.get("child_execution_profiles",[])}
    profile=profiles.get("child-general")
    if not profile or harness not in profile.get("supported_harnesses",[]):
        raise ValueError("child-general does not support the current Harness")
    template=profile.get("job_template")
    if not isinstance(template,dict):raise ValueError("Child Harness Job template is unavailable")
    skills={item.get("id"):item for item in catalog.get("skills",[])}
    operations={item.get('id') for item in catalog.get('operations',[])}
    capabilities={item.get('id') for item in catalog.get('capabilities',[])}
    allowed=set(profile.get("allowed_skill_ids",[]))
    artifacts={}
    for item in catalog.get("input_artifacts",[]):
        artifacts[item.get("artifact_id")]=item
        artifacts[item.get("project_path")]=item
    jobs=[]
    for task in spec.tasks:
        selected=[]
        for skill_id in dict.fromkeys(task.skill_ids):
            skill=skills.get(skill_id)
            if not skill:raise ValueError("Unknown frozen Skill: "+skill_id)
            if skill_id not in allowed:raise ValueError("Skill is outside child-general: "+skill_id)
            if harness not in skill.get("supported_harnesses",[]):
                raise ValueError("Skill does not support current Harness: "+skill_id)
            requirements=list(skill.get('required_capabilities') or [])
            origins=list((skill.get('network') or {}).get('origins') or skill.get('network_origins') or [])
            if not set(requirements)<=set(task.capability_ids) or skill.get('requires_gpu'):
                raise ValueError("Skill requirements are outside child-general: "+skill_id)
            selected.append(skill_id)
        bindings=[]
        unknown_operations=set(task.operation_ids)-operations
        unknown_capabilities=set(task.capability_ids)-capabilities
        if unknown_operations:raise ValueError('Unknown frozen Operation: '+sorted(unknown_operations)[0])
        if unknown_capabilities:raise ValueError('Unknown frozen Capability: '+sorted(unknown_capabilities)[0])
        for index,reference in enumerate(dict.fromkeys(task.inputs),1):
            artifact=artifacts.get(reference)
            if not artifact:raise ValueError("Input is not a frozen Turn Artifact: "+reference)
            bindings.append({"artifact_id":artifact["artifact_id"],"artifact_role":f"input_{index}"})
        outputs=[] if task.result_mode=="text" else [{
            "artifact_role":"child_deliverables","output_disposition":task.output_disposition,
            "min_items":1 if task.result_mode=="artifacts" else 0,
            "max_items":task.max_items,"mime_patterns":list(task.mime_patterns),
            "max_item_bytes":512*1024*1024,"max_total_bytes":2*1024*1024*1024}]
        task_value=task.model_dump(mode="json")
        jobs.append({**template,"local_job_id":task.id,
            "idempotency_key":"delegate-job-"+_digest({"turn":turn_id,"task":task_value})[:48],
            "parameters":{"harness":harness,"model":model,"objective":task.objective},
            "input_artifacts":bindings,"expected_outputs":outputs,"skill_ids":selected})
        if task.operation_ids or task.capability_ids:
            jobs[-1]['resource_requirements']={
                'required_operation_ids':list(task.operation_ids),
                'required_capability_ids':list(task.capability_ids)}
        required_origins=[]
        for skill_id in selected:
            skill=skills[skill_id];required_origins.extend(
                (skill.get('network') or {}).get('origins') or skill.get('network_origins') or [])
        if required_origins:jobs[-1]['network_origins']=list(dict.fromkeys(required_origins))
    normalized=spec.model_dump(mode="json")
    group={"thread_id":thread_id,"origin_turn_id":turn_id,
        "idempotency_key":spec.idempotency_key or "delegate-"+_digest({
            "turn":turn_id,"spec":normalized})[:48],"jobs":jobs,"authorization_snapshot":{},
        "suppress_continuation":spec.suppress_continuation}
    return JobGroupCreate.model_validate(group).model_dump(mode="json")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sandbox-job")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("catalog", help="Show registered Operations, Capabilities, Skills and Child Profiles")
    prepare = sub.add_parser("prepare", help="Validate and submit one complete parallel JobGroup")
    prepare.add_argument("--request", required=True, help="JSON object or path to a JSON file")
    delegate = sub.add_parser("delegate", help="Compile compact independent Child Harness tasks",
        epilog=('Artifact outputs require output_disposition=candidate|workspace|delivery. '
            'Use delivery for final user-facing files.'))
    choice=delegate.add_mutually_exclusive_group(required=True)
    choice.add_argument("--request", help="Compact tasks JSON object or path to a JSON file")
    choice.add_argument("--example",action="store_true",help="Print one valid compact request")
    choice.add_argument("--schema",action="store_true",help="Print the compact request JSON Schema")
    edit = sub.add_parser("edit", help="Replace the editable members of one JobGroupDraft")
    edit.add_argument("--draft-id", required=True)
    edit.add_argument("--expected-digest", required=True)
    edit.add_argument("--jobs", required=True, help="JSON array or path to a JSON file")
    for name in ("draft",):
        command = sub.add_parser(name)
        command.add_argument("--draft-id", required=True)
    group = sub.add_parser("group"); group.add_argument("--group-id", required=True)
    job = sub.add_parser("job"); job.add_argument("--job-id", required=True)
    inspect = sub.add_parser("inspect",help="Run the frozen artifact.inspect inline Operation")
    inspect.add_argument("--artifact-id",required=True)
    cancel_job = sub.add_parser("cancel-job"); cancel_job.add_argument("--job-id", required=True)
    cancel_group = sub.add_parser("cancel-group"); cancel_group.add_argument("--group-id", required=True)
    args = parser.parse_args(argv)
    if args.command=="delegate" and args.example:
        print(json.dumps(DELEGATION_EXAMPLE,ensure_ascii=False,indent=2));return 0
    if args.command=="delegate" and args.schema:
        print(json.dumps(DelegationGroupSpec.model_json_schema(),ensure_ascii=False));return 0
    if args.command=="inspect" and os.environ.get("HARNESS_CHILD_MODE")!="1":
        print(json.dumps({"error":"sandbox-job inspect is Job-only; use sandbox-artifact fetch "
            "to retrieve a reviewed Child Artifact"}))
        return 2
    base = os.environ.get("HARNESS_GATEWAY_URL", "").rstrip("/")
    token_file = capability_token_file()
    if not base or not token_file.is_file():
        print(json.dumps({"error": "Harness Gateway capability is not configured"}))
        return 2
    token=token_file.read_text().strip()
    headers = {"Authorization": "Bearer " + token,**_job_headers(token)}
    try:
        if args.command == "catalog":
            response = httpx.get(base + "/v1/execution/catalog", headers=headers, timeout=20)
        elif args.command in {"prepare","delegate"}:
            if os.environ.get("HARNESS_CHILD_MODE") == "1" and args.command=="delegate":
                print(json.dumps({"error":"A Child Harness cannot delegate another JobGroup"}))
                return 2
            payload=_value(args.request)
            if args.command=="delegate":
                catalog_response=httpx.get(base+"/v1/execution/catalog",headers=headers,timeout=20)
                if not catalog_response.is_success:
                    print(catalog_response.text);return 1
                payload=compile_delegation(payload,json.loads(catalog_response.text))
            else:
                payload.setdefault("thread_id",os.environ.get("HARNESS_THREAD_ID",""))
                payload.setdefault("origin_turn_id",os.environ.get("HARNESS_TURN_ID",""))
            JobGroupCreate.model_validate(payload)
            response = httpx.post(base + "/v1/job-group-drafts", headers=headers, json=payload, timeout=30)
        elif args.command == "edit":
            jobs_value = _json_value(args.jobs)
            jobs = (jobs_value.get("jobs")
                if isinstance(jobs_value, dict) and set(jobs_value) == {"jobs"} else jobs_value)
            if not isinstance(jobs, list):
                raise ValueError("--jobs must contain a JSON array or {\"jobs\": [...]} object")
            response = httpx.patch(base + f"/v1/job-group-drafts/{args.draft_id}", headers=headers,
                json={"expected_digest": args.expected_digest, "jobs": jobs}, timeout=30)
        elif args.command == "inspect":
            headers["x-updream-operation-id"]="artifact.inspect"
            response=httpx.get(base+"/v1/operations/artifact.inspect",headers=headers,
                params={"artifact_id":args.artifact_id},timeout=20)
        elif args.command == "draft":
            response = httpx.get(base + f"/v1/job-group-drafts/{args.draft_id}", headers=headers, timeout=20)
        elif args.command in {"group", "cancel-group"}:
            path = f"/v1/job-groups/{args.group_id}" + ("/cancel" if args.command == "cancel-group" else "")
            response = (httpx.post if args.command == "cancel-group" else httpx.get)(
                base + path, headers=headers, timeout=20)
        else:
            path = f"/v1/jobs/{args.job_id}" + ("/cancel" if args.command == "cancel-job" else "")
            response = (httpx.post if args.command == "cancel-job" else httpx.get)(
                base + path, headers=headers, timeout=20)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    print(response.text)
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main())
