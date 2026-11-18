#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/"tests/real_cases/source-plus-cases.json"
OUTPUT=ROOT/"tests/real_cases/cases.json"


def protocol(identity,title,prompt,expect,*,assets=(),features=(),hook=None):
    return {"id":identity,"title":title,"complexity":"parallel_protocol","assets":list(assets),
        "prompt":prompt,"expect":expect,"content_checks":[],"hook":hook,"status":"not_run",
        "attempts":[],"entry_mode":"deployed_browser_or_api","executor":"parallel_protocol",
        "required_features":list(features),"provenance":"parallel-design"}


PARALLEL_CASES=[
    protocol("P01","混合并行 JobGroup",
        "并行生成两张太空海报、检查上传图片，并委托子 Harness 生成一份对照说明；全部完成后汇总。",
        {"group_members":4,"kinds":["provider","operation","harness"],"one_continuation":True},
        assets=("poster.png",),features=("mixed_group","continuation")),
    protocol("P02","整组授权允许","一次提交七张风格一致的图片，确认前不要启动其中任何一张。",
        {"draft":"confirmed","started_after_confirmation":7},features=("group_confirmation","budget"),
        hook={"action":"confirm_group"}),
    protocol("P03","整组授权拒绝","一次提交七张风格一致的图片；在确认卡拒绝整组。",
        {"draft":"rejected","provider_submissions":0},features=("group_confirmation","budget"),
        hook={"action":"reject_group"}),
    protocol("P04","多文件同角色集合","并行生成四张图片并作为同一个 generated_images 角色的有序集合交付。",
        {"role":"generated_images","count":4,"atomic":True},features=("artifact_collection",)),
    protocol("P05","多组完成合并续接","提交三个独立并行组，让它们接近同时完成。",
        {"coalesced_continuation":True,"max_groups":3},features=("continuation","outbox")),
    protocol("P06","Job在途用户输入排序","后台任务运行时追加：最终还需要一份中文摘要。",
        {"user_turn_before_continuation":True,"intent_preserved":True},features=("input_ordering",),
        hook={"action":"send_while_jobs_running"}),
    protocol("P07","取消并替换子Harness","子 Harness 运行时修改目标，取消原 Job 并创建显式 replacement。",
        {"original":"cancelled","replacement":"succeeded","steer_used":False},features=("replacement",)),
    protocol("P08","单Job停止","并行三项任务时只停止其中一项。",
        {"cancelled":1,"independent_members_continue":True},features=("job_cancel",),hook={"action":"cancel_job"}),
    protocol("P09","整组停止","并行任务运行时停止当前 JobGroup。",
        {"group_terminal":True,"new_group_unaffected":True},features=("group_cancel",),hook={"action":"cancel_group"}),
    protocol("P10","Thread全部停止","存在多个后台组时执行停止全部。",
        {"all_groups_terminal":True,"continuation_suppressed":True},features=("thread_stop",),hook={"action":"stop_all"}),
    protocol("P11","最小Skill Profile","用 PDF Skill 审查两份报告，不开放 Provider 或公网。",
        {"profile":"office","skills":["pdf"],"capabilities":[],"network":[]},features=("profile_resolver","grant")),
    protocol("P12","不兼容Skill组合预检","请求一个没有任何已注册 Runtime Profile 能同时满足的 Skill 组合。",
        {"preflight_error":"No RuntimeDependencyProfile satisfies frozen requirements","jobs_started":0},features=("profile_resolver",)),
    protocol("P13","节点硬杀后接管","子 Harness 接受任务后强杀 Job Worker，再启动替代 Worker。",
        {"same_external_execution":True,"duplicate_submission":False,"terminal":True},features=("node_loss","recovery"),hook={"action":"kill_job_worker"}),
    protocol("P14","可选Reviewer不可用","候选通过确定性校验，但可选 Reviewer 暂时不可用。",
        {"candidate_preserved":True,"availability_warning":True},features=("reviewer",)),
    protocol("P15","必需Reviewer阻止发布","必需 Reviewer 返回 REVISE，候选不能成为 delivery。",
        {"review":"REVISE","delivery_visible":False},features=("reviewer","publication_gate")),
    protocol("P16","预算并发竞争","两个并发整组确认竞争同一笔只够一组的预算。",
        {"confirmed_groups":1,"overspend":False},features=("budget","postgres_race")),
    protocol("P17","Provider接受响应丢失","Provider 已接受但提交响应丢失，随后主动查询对账。",
        {"state_before_reconcile":"UNKNOWN","duplicate_submission":False,"handle_reused":True},features=("provider_reconcile",)),
    protocol("P18","Sandbox检查点接管","保存并释放后重新接管终端，再恢复原 Child Job。",
        {"checkpoint_restored":True,"terminal_prompt":"updream@updream.sandbox.com"},features=("checkpoint","takeover")),
    protocol("P19","Artifact集合原子发布","集合最后一项写入期间重启 Worker。",
        {"partial_delivery_visible":False,"final_collection_complete":True},features=("artifact_collection","recovery")),
    protocol("P20","Provider与子Harness混合扇出","同时提交三张外部生成图和三个独立子 Harness 分析，完成后由 Primary 汇总。",
        {"provider_jobs":3,"harness_jobs":3,"parallel":True,"one_primary_continuation":True},features=("mixed_group","delegation")),
]


def inherited_adaptation(value):
    hook=value.get("dynamic_hook") or "artifact_contract"
    return ("Preserve the original "+value["id"]+" risk and assertions through the Parallel "
        "Primary Harness, Durable JobGroup, JobAttempt, Reviewer, continuation and Gateway "
        "boundaries. Plus hook="+hook+" is exercised by the inherited protocol driver; no "
        "Manager/Orchestrator compatibility layer is introduced.")


def main():
    raw=SOURCE.read_bytes();source=json.loads(raw);business=[];inherited=[];superseded=[]
    for value in source["cases"]:
        if value["id"].startswith(("S","C","R")):
            migrated={**value,"status":"not_run","attempts":[],"executor":"primary_harness",
                "entry_mode":"deployed_browser","provenance":"sandbox-harness-platform-plus:"+value["id"]}
            business.append(migrated)
        elif value["id"].startswith("D"):
            migrated={**value,"status":"not_run","attempts":[],"executor":"inherited_protocol",
                "entry_mode":"deployed_browser_or_api",
                "provenance":"sandbox-harness-platform-plus:"+value["id"],
                "source_project":"sandbox-harness-platform-plus",
                "source_definition_sha256":value.get("original_definition_sha256") or hashlib.sha256(
                    json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()).hexdigest(),
                "parallel_adaptation":inherited_adaptation(value)}
            inherited.append(migrated)
        else:raise ValueError("Unmapped source case: "+value["id"])
    result={"schema_version":1,"campaign":"primary-harness-parallel","default_concurrency":8,
        "source_snapshot":{"path":str(SOURCE.relative_to(ROOT)),"sha256":hashlib.sha256(raw).hexdigest(),
            "source_case_count":len(source["cases"])},
        "rules":{"stop_on_program_or_prompt_defect":True,"rerun_failed_then_affected_only":True,
            "success_requires_user_surface_and_internal_audit":True,"evidence_root":".data/evidence/real-campaign",
            "forbid_case_specific_product_fixes":True},
        "runnable_case_count":len(business)+len(inherited)+len(PARALLEL_CASES),
        "business_case_count":len(business),"inherited_protocol_case_count":len(inherited),
        "parallel_case_count":len(PARALLEL_CASES),"cases":[*business,*inherited,*PARALLEL_CASES],
        "superseded_cases":superseded}
    OUTPUT.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({"output":str(OUTPUT),"runnable":len(result["cases"]),
        "superseded":len(superseded)},ensure_ascii=False))


if __name__=="__main__":main()
