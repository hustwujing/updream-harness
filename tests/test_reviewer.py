import hashlib,json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate
from sandbox_harness.job_catalog import JobCatalog,JobCatalogPayload
from sandbox_harness.job_coordinator import JobCoordinator
from sandbox_harness.profiles import ProfileResolver
from sandbox_harness.reviewer import (ReviewCandidate,ReviewRequest,ReviewerCoordinator,
    ReviewerJobAdapter,ReviewSubmission,ReviewTrigger)
from sandbox_harness.harness_job import JobGrantFactory
from sandbox_harness.job_adapters import JobExecutionContext
from sandbox_harness.config import Settings
from sandbox_harness.store import Store

ROOT=Path(__file__).resolve().parents[1]

async def setup(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'review.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
        reasoning_level='standard',title='Review'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='make reports',
        idempotency_key='review-origin'),profile,'owner')
    artifacts=[]
    for index in range(2):
        data=f'%PDF-{index}'.encode();digest=hashlib.sha256(data).hexdigest()
        artifacts.append(await store.add_artifact(turn.id,path=f'report-{index}.pdf',sha256=digest,
            size=len(data),mime='application/pdf',object_ref='sha256:'+digest))
    job_raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    job_catalog=JobCatalog.from_json(job_raw,expected_digest=hashlib.sha256(job_raw.encode()).hexdigest())
    profile_raw=(ROOT/'deploy/profile-catalog.example.json').read_text()
    resolver=ProfileResolver.from_json(profile_raw,expected_digest=hashlib.sha256(profile_raw.encode()).hexdigest(),
        skill_manifest=json.loads((ROOT/'runtime/skills/manifest.json').read_text()))
    return store,thread,turn,artifacts,ReviewerCoordinator(JobCoordinator(store,job_catalog,
        profile_resolver=resolver))


def review_request(thread,turn,artifacts,trigger=ReviewTrigger.USER,receipts=True):
    return ReviewRequest(thread_id=thread.id,origin_turn_id=turn.id,trigger=trigger,required=True,
        harness='codex',model='gpt-5.6',skill_ids=('pdf',),rubric='Check both PDF reports.',
        idempotency_key='review-group-key',candidates=tuple(ReviewCandidate(artifact_id=value.id,
            artifact_role='reports',sha256=value.sha256,mime=value.mime,
            validation_receipts=({'validator':'pdf.verify','status':'pass'},) if receipts else (
                {'validator':'pdf.verify','status':'fail'},)) for value in artifacts))


async def test_reviewer_is_default_off_and_validators_run_before_model(tmp_path):
    store,thread,turn,artifacts,reviewer=await setup(tmp_path)
    try:
        assert await reviewer.prepare('owner',review_request(thread,turn,artifacts,trigger=None)) is None
        with pytest.raises(RuntimeError,match='deterministic_validation_not_passed'):
            await reviewer.prepare('owner',review_request(thread,turn,artifacts,receipts=False))
        assert await store.list_jobs_for_thread(thread.id)==[]
    finally:await store.close()


async def test_explicit_review_groups_candidates_into_one_read_only_harness_job(tmp_path):
    store,thread,turn,artifacts,reviewer=await setup(tmp_path)
    try:
        prepared=await reviewer.prepare('owner',review_request(thread,turn,artifacts))
        assert prepared.mode=='committed' and len(prepared.jobs)==1
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert [item.artifact_id for item in frozen.input_artifacts]==[value.id for value in artifacts]
        assert frozen.network_origins==() and frozen.expected_outputs==()
        assert frozen.resource_requirements['delegation_depth']==1
        assert 'node_id' not in frozen.resource_requirements and 'image_digest' in frozen.resource_requirements
        job=prepared.jobs[0].model_copy(update={'generation':1})
        context=JobExecutionContext(job=job,request=frozen,generation=1)
        token,claims=await JobGrantFactory(Settings(backend='fake'))(context,
            SimpleNamespace(harness='codex',runtime_profile_id=frozen.runtime_profile_id),
            'reviewer-sandbox')
        assert claims.skill_ids==['pdf'] and claims.artifact_ids==[value.id for value in artifacts]
        assert claims.capability_ids==[] and claims.network_origins==[] and claims.output_scopes==[]
        assert claims.operations==['model'] and claims.child_profile_id=='reviewer-readonly'
        assert '/v1/public-http' not in claims.routes
    finally:await store.close()


async def test_review_submission_resolves_authoritative_artifact_metadata(tmp_path):
    store,thread,turn,artifacts,reviewer=await setup(tmp_path)
    try:
        prepared=await reviewer.prepare_submission('owner',thread.id,turn.id,ReviewSubmission(
            artifact_ids=tuple(value.id for value in artifacts),required=True,harness='codex',model='gpt-5.6',
            skill_ids=('pdf',),rubric='Check both reports.',idempotency_key='review-submission-key'))
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert frozen.parameters['review_context']['artifact_digests']==[value.sha256 for value in artifacts]
        assert [item.artifact_id for item in frozen.input_artifacts]==[value.id for value in artifacts]
    finally:await store.close()


@pytest.mark.parametrize('verdict',['PASS','REVISE','EVIDENCE_REQUIRED','BLOCKED'])
async def test_reviewer_verdict_is_structured_and_digest_bound(verdict):
    digests=('a'*64,'b'*64)
    class Delegate:
        async def collect(self,*args):return {'review':{'verdict':verdict,
            'artifact_digests':list(digests),'summary':'reviewed'}}
    request=SimpleNamespace(parameters={'review_context':{'artifact_digests':list(digests)}})
    result=await ReviewerJobAdapter(Delegate()).collect(SimpleNamespace(request=request),'execution',None)
    assert result['review']['verdict']==verdict
    class Wrong:
        async def collect(self,*args):return {'review':{'verdict':'PASS','artifact_digests':['0'*64]}}
    with pytest.raises(ValueError,match='digest'):
        await ReviewerJobAdapter(Wrong()).collect(SimpleNamespace(request=request),'execution',None)
