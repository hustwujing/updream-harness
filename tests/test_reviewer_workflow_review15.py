import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import update

from sandbox_harness.artifact_gateway import ArtifactEmission,ArtifactGateway
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness,ItemType,ThreadCreate,TurnCreate
from sandbox_harness.job_adapters import (AdapterStatus,AdapterSubmission,JobExecutionContext,
    PreAcceptFailure)
from sandbox_harness.job_catalog import JobCatalog
from sandbox_harness.job_contracts import (JobExpectedOutput,JobGroupCreate,JobKind,JobRequest,
    JobState,OutputDisposition)
from sandbox_harness.job_coordinator import JobCoordinator
from sandbox_harness.job_worker import JobWorker
from sandbox_harness.objects import ObjectStore
from sandbox_harness.profiles import ProfileResolver
from sandbox_harness.reviewer import ReviewSubmission,ReviewerCoordinator,ReviewerJobAdapter
from sandbox_harness.store import JobArtifactRow,JobOutboxRow,Store


ROOT=Path(__file__).resolve().parents[1]


async def chunks(value):yield value


async def environment(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"review-workflow.db"));await store.initialize()
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level="standard",title="Review workflow"),profile,"owner")
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message="create report",
        idempotency_key="review-workflow-turn"),profile,"owner")
    catalog_raw=(ROOT/"deploy/job-catalog.example.json").read_text()
    catalog=JobCatalog.from_json(catalog_raw,expected_digest=hashlib.sha256(catalog_raw.encode()).hexdigest())
    profile_raw=(ROOT/"deploy/profile-catalog.example.json").read_text()
    resolver=ProfileResolver.from_json(profile_raw,expected_digest=hashlib.sha256(profile_raw.encode()).hexdigest(),
        skill_manifest=json.loads((ROOT/"runtime/skills/manifest.json").read_text()),
        provider="opensandbox")
    reviewer=ReviewerCoordinator(JobCoordinator(store,catalog,profile_resolver=resolver))
    return store,thread,turn,reviewer,ObjectStore(tmp_path/"objects")


async def source_collection(store,thread,turn,objects,suffix,policy="required",finalize=True):
    output=JobExpectedOutput(artifact_role="report",output_disposition=OutputDisposition.DELIVERY,
        min_items=1,max_items=1,mime_patterns=("application/pdf",),validators=("pdf.verify",),
        requires_review=True,review_policy=policy)
    request=JobRequest(local_job_id="source-"+suffix,kind=JobKind.OPERATION,adapter="render",
        descriptor_id="render",descriptor_version="1",descriptor_digest="a"*64,
        idempotency_key="source-job-"+suffix,expected_outputs=(output,))
    _,jobs=await store.create_job_group("owner",JobGroupCreate(thread_id=thread.id,
        origin_turn_id=turn.id,idempotency_key="source-group-"+suffix,jobs=(request,)))
    attempt=await store.claim_job(jobs[0].id,"source-worker","node",60)
    data=("%PDF-1.7\n"+suffix).encode();digest=hashlib.sha256(data).hexdigest()
    gateway=ArtifactGateway(store,objects,objects.root/"stage",maximum_bytes=1024*1024)
    emitted=await gateway.emit(ArtifactEmission(jobs[0].id,attempt.generation,"report","main",0,
        suffix+".pdf",OutputDisposition.DELIVERY,"application/pdf"),chunks(data))
    if finalize:
        waiting=await store.finalize_job_artifact_collection(jobs[0].id,attempt.generation,"report",
            validation_receipts=[{"validator":"pdf.verify","status":"pass",
                "artifact_digests":[digest]}])
        assert waiting["state"]=="awaiting_review"
        await store.transition_job(jobs[0].id,JobState.SUCCEEDED,
            expected_generation=attempt.generation,result={"candidate":True})
    return jobs[0],attempt,emitted,digest


class VerdictDelegate:
    def __init__(self,verdict,digests):self.verdict=verdict;self.digests=digests
    async def collect(self,*args):return {"review":{"verdict":self.verdict,
        "artifact_digests":self.digests,"summary":"checked","findings":[],"missing_evidence":[]}}


async def run_review(store,reviewer,thread,turn,artifact,verdict,digest,*,required=True,key="review-key"):
    prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
        artifact_ids=(artifact.artifact_id,),required=required,harness="codex",model="gpt-5.6",
        rubric="Review the PDF.",idempotency_key=key))
    review_job=prepared.jobs[0];attempt=await store.claim_job(review_job.id,"review-worker","node",60)
    job=await store.get_job(review_job.id);request=await store.get_job_request(review_job.id)
    adapter=ReviewerJobAdapter(VerdictDelegate(verdict,[digest]),store)
    result=await adapter.collect(JobExecutionContext(job=job,request=request,generation=attempt.generation),
        "execution",AdapterStatus(True,JobState.SUCCEEDED,{}))
    return adapter,result,job,request,attempt


async def test_real_reviewer_pass_promotes_and_replay_is_idempotent(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    try:
        source,_,artifact,digest=await source_collection(store,thread,turn,objects,"pass")
        adapter,result,job,request,attempt=await run_review(store,reviewer,thread,turn,
            artifact,"PASS",digest,key="review-pass-key")
        assert (await store.get_review_gate(job.id))["state"]=="published"
        assert result["review_gate"]["state"]=="published"
        files=await store.current_project_files(thread.id,"owner")
        assert [value.path for value in files]==[f"artifacts/jobs/{source.id}/pass.pdf"]
        visible=[item for item in await store.items_after(thread.id) if item.type==ItemType.ARTIFACT]
        assert [item.payload["artifact_id"] for item in visible]==[artifact.artifact_id]
        replay=await adapter.collect(JobExecutionContext(job=job,request=request,generation=attempt.generation),
            "execution",AdapterStatus(True,JobState.SUCCEEDED,{}))
        assert replay["review_gate"]["state"]=="published"
        assert len(await store.current_project_files(thread.id,"owner"))==1
    finally:await store.close()


async def test_review_job_and_gate_are_atomic_and_idempotent_on_prepare_replay(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    try:
        _,_,artifact,_=await source_collection(store,thread,turn,objects,"atomic")
        submission=ReviewSubmission(artifact_ids=(artifact.artifact_id,),required=True,
            harness="codex",model="gpt-5.6",rubric="Review.",idempotency_key="atomic-review")
        first=await reviewer.prepare_submission("owner",thread.id,turn.id,submission)
        gate=await store.get_review_gate(first.jobs[0].id)
        assert gate and gate["state"]=="pending"
        claimed=await store.claim_job(first.jobs[0].id,"worker","node",60)
        assert claimed is not None
        repeated=await reviewer.prepare_submission("owner",thread.id,turn.id,submission)
        assert repeated.jobs[0].id==first.jobs[0].id
        assert (await store.get_review_gate(first.jobs[0].id))["artifact_ids"]==[
            artifact.artifact_id]
    finally:await store.close()


async def test_revise_and_required_failure_retain_candidates_while_optional_failure_publishes(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    try:
        _,_,revise_artifact,revise_digest=await source_collection(store,thread,turn,objects,"revise")
        _,result,_,_,_=await run_review(store,reviewer,thread,turn,revise_artifact,
            "REVISE",revise_digest,key="review-revise-key")
        assert result["review_gate"]["state"]=="review_blocked"
        assert await store.current_project_files(thread.id,"owner")==[]

        _,_,required_artifact,_=await source_collection(store,thread,turn,objects,"required-failure")
        prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
            artifact_ids=(required_artifact.artifact_id,),required=True,harness="codex",model="gpt-5.6",
            rubric="Review.",idempotency_key="required-failure-review"))
        review_job=prepared.jobs[0];attempt=await store.claim_job(review_job.id,"worker","node",60)
        job=await store.get_job(review_job.id);request=await store.get_job_request(review_job.id)
        class Failed:
            async def collect(self,*args):return {"error_code":"reviewer_unavailable"}
        failed=await ReviewerJobAdapter(Failed(),store).collect(
            JobExecutionContext(job=job,request=request,generation=attempt.generation),"execution",
            AdapterStatus(True,JobState.FAILED,{}))
        failed_job=await store.transition_job(job.id,JobState.FAILED,
            expected_generation=attempt.generation,result=failed)
        assert failed_job.result["review_gate"]["state"]=="required_unavailable"

        _,_,optional_artifact,_=await source_collection(store,thread,turn,objects,"optional-failure","optional")
        prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
            artifact_ids=(optional_artifact.artifact_id,),required=False,harness="codex",model="gpt-5.6",
            rubric="Review.",idempotency_key="optional-failure-review"))
        review_job=prepared.jobs[0];attempt=await store.claim_job(review_job.id,"worker","node",60)
        job=await store.get_job(review_job.id);request=await store.get_job_request(review_job.id)
        optional=await ReviewerJobAdapter(Failed(),store).collect(
            JobExecutionContext(job=job,request=request,generation=attempt.generation),"execution",
            AdapterStatus(True,JobState.FAILED,{}))
        optional_job=await store.transition_job(job.id,JobState.FAILED,
            expected_generation=attempt.generation,result=optional)
        assert optional_job.result["review_gate"]["state"]=="optional_warning"
        assert "optional_review_unavailable" in optional_job.result["review_gate"]["warning"]
        assert any(value.path.endswith("optional-failure.pdf")
            for value in await store.current_project_files(thread.id,"owner"))
    finally:await store.close()


async def test_review_rejects_missing_validation_and_changed_digest(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    try:
        _,_,unvalidated,_=await source_collection(store,thread,turn,objects,"unvalidated",finalize=False)
        with pytest.raises(RuntimeError,match="validation_required"):
            await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
                artifact_ids=(unvalidated.artifact_id,),required=True,harness="codex",model="gpt-5.6",
                rubric="Review.",idempotency_key="unvalidated-review"))
        _,_,artifact,digest=await source_collection(store,thread,turn,objects,"changed")
        prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
            artifact_ids=(artifact.artifact_id,),required=True,harness="codex",model="gpt-5.6",
            rubric="Review.",idempotency_key="changed-review"))
        async with store.connections.begin() as db:
            await db.execute(update(JobArtifactRow).where(JobArtifactRow.artifact_id==artifact.artifact_id)
                .values(sha256="0"*64))
        review_job=prepared.jobs[0];attempt=await store.claim_job(review_job.id,"worker","node",60)
        job=await store.get_job(review_job.id);request=await store.get_job_request(review_job.id)
        with pytest.raises(RuntimeError,match="identity_changed"):
            await ReviewerJobAdapter(VerdictDelegate("PASS",[digest]),store).collect(
                JobExecutionContext(job=job,request=request,generation=attempt.generation),"execution",
                AdapterStatus(True,JobState.SUCCEEDED,{}))
    finally:await store.close()


async def test_preaccept_failure_finalizes_required_and_optional_gates(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    class Delegate:
        async def submit(self,*args):raise PreAcceptFailure("review backend unavailable")
    try:
        for policy,expected in (("required","required_unavailable"),("optional","optional_warning")):
            _,_,artifact,_=await source_collection(store,thread,turn,objects,"preaccept-"+policy,policy)
            prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
                artifact_ids=(artifact.artifact_id,),required=policy=="required",harness="codex",
                model="gpt-5.6",rubric="Review.",idempotency_key="preaccept-review-"+policy))
            worker=JobWorker(store,{"reviewer-harness":ReviewerJobAdapter(Delegate(),store)},
                node_id="node",worker_id="review-worker")
            await worker.run_once()
            assert (await store.get_review_gate(prepared.jobs[0].id))["state"]==expected
            terminal=await store.get_job(prepared.jobs[0].id)
            assert terminal.result["review_gate"]["state"]==expected
        assert any(value.path.endswith("preaccept-optional.pdf")
            for value in await store.current_project_files(thread.id,"owner"))
        assert not any(value.path.endswith("preaccept-required.pdf")
            for value in await store.current_project_files(thread.id,"owner"))
    finally:await store.close()


async def test_late_required_gate_rejected_but_mixed_advisory_review_allowed(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    try:
        _,_,first,digest=await source_collection(store,thread,turn,objects,"published")
        await run_review(store,reviewer,thread,turn,first,"PASS",digest,key="published-pass")
        with pytest.raises(RuntimeError,match="already_published"):
            await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
                artifact_ids=(first.artifact_id,),required=True,harness="codex",model="gpt-5.6",
                rubric="Late gate.",idempotency_key="late-required"))

        _,_,second,_=await source_collection(store,thread,turn,objects,"mixed-second")
        data=b"generic";generic=await store.add_artifact(turn.id,path="generic.txt",
            sha256=hashlib.sha256(data).hexdigest(),size=len(data),mime="text/plain",
            object_ref="sha256:"+hashlib.sha256(data).hexdigest())
        prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
            artifact_ids=(first.artifact_id,second.artifact_id,generic.id),required=False,
            harness="codex",model="gpt-5.6",rubric="Advisory grouped review.",
            idempotency_key="mixed-advisory"))
        request=await store.get_job_request(prepared.jobs[0].id)
        assert request.parameters["review_context"]["target"] is None
        assert len(request.input_artifacts)==3
        assert await store.get_review_gate(prepared.jobs[0].id) is None
    finally:await store.close()


async def test_malformed_verdict_terminalizes_job_and_gate_atomically_without_worker_escape(tmp_path):
    store,thread,turn,reviewer,objects=await environment(tmp_path)
    class Malformed:
        async def submit(self,*args):return AdapterSubmission("review-execution",{},completed=True)
        async def collect(self,*args):return {"review":{"verdict":"PASS"}}
    try:
        for policy,expected in (("required","required_unavailable"),("optional","optional_warning")):
            _,_,artifact,_=await source_collection(store,thread,turn,objects,"malformed-"+policy,policy)
            prepared=await reviewer.prepare_submission("owner",thread.id,turn.id,ReviewSubmission(
                artifact_ids=(artifact.artifact_id,),required=policy=="required",harness="codex",
                model="gpt-5.6",rubric="Review.",idempotency_key="malformed-review-"+policy))
            worker=JobWorker(store,{"reviewer-harness":ReviewerJobAdapter(Malformed(),store)},
                node_id="node",worker_id="malformed-worker")
            attempts=await store.claim_available_jobs(worker.worker_id,"node",60,limit=1,
                adapters={"reviewer-harness"})
            await worker._process_isolated(attempts[0])
            terminal=await store.get_job(prepared.jobs[0].id)
            gate=await store.get_review_gate(prepared.jobs[0].id)
            assert terminal.state==JobState.FAILED and gate["state"]==expected
            assert terminal.result["review_gate"]["state"]==expected
            async with store.connections() as db:
                outbox=(await db.execute(__import__('sqlalchemy').select(JobOutboxRow).where(
                    JobOutboxRow.group_id==prepared.group.id))).scalar_one()
            payload=json.loads(outbox.result_json)
            assert payload[0]["review_gate"]["state"]==expected
    finally:await store.close()
