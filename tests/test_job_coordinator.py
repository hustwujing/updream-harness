from datetime import timedelta
from types import SimpleNamespace

import pytest

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate, identifier, now)
from sandbox_harness.common.catalog.job_catalog import (JobCatalog, JobCatalogPayload)
from sandbox_harness.common.contracts.job_contracts import (JobExpectedOutput, JobGroupCreate, JobKind, JobRequest, JobState, OutputDisposition)
from sandbox_harness.common.jobs.job_coordinator import (JobCoordinator, JobGroupDraftUpdate)
from sandbox_harness.common.db.store import (Store)


ROOT=__import__('pathlib').Path(__file__).resolve().parents[1]


def catalog() -> JobCatalog:
    payload=JobCatalogPayload.model_validate({"schema_version":1,"operations":[{
        "id":"free.compute","version":"1","digest":"a"*64,"execution_mode":"durable",
        "timeout_seconds":300,"parameter_schema":{"type":"object","properties":{},"additionalProperties":False},
        "input_roles":[],"output_roles":["result"],"required_capabilities":[],"network_origins":[],
        "resources":{"cpu":1,"memory_mb":128,"gpu":0},"side_effects":False,"billable":False,
        "creates_artifacts":True,"retry_policy":{"max_pre_accept_attempts":2},
        "cancellation_policy":{"supported":True},"reconciliation_policy":{"supported":True}
    },{
        "id":"inline.inspect","version":"1","digest":"b"*64,"execution_mode":"inline",
        "timeout_seconds":5,"parameter_schema":{"type":"object","properties":{},"additionalProperties":False},
        "input_roles":[],"output_roles":[],"required_capabilities":[],"network_origins":[],
        "resources":{"cpu":1,"memory_mb":128,"gpu":0},"side_effects":False,"billable":False,
        "creates_artifacts":False,"retry_policy":{"max_pre_accept_attempts":1},
        "cancellation_policy":{"supported":False},"reconciliation_policy":{"supported":False}
    }],"capabilities":[{
        "id":"image.generate","version":"1","digest":"c"*64,"operations":["submit","status"],
        "timeout_seconds":300,
        "parameter_schema":{"type":"object","properties":{"prompt":{"type":"string"}},"additionalProperties":False},
        "input_roles":[],"output_roles":["images"],"network_origins":["image-provider"],
        "resources":{"cpu":1,"memory_mb":128,"gpu":0},"billable":True,"side_effects":True,
        "retry_policy":{"max_pre_accept_attempts":2},"cancellation_policy":{"supported":True},
        "reconciliation_policy":{"supported":True}
    }]})
    return JobCatalog(payload,"d"*64)


def job(local_id: str, descriptor: str = "free.compute") -> JobRequest:
    kind=JobKind.PROVIDER if descriptor=="image.generate" else JobKind.OPERATION
    digest={"free.compute":"a"*64,"inline.inspect":"b"*64,"image.generate":"c"*64}[descriptor]
    outputs=() if descriptor=='inline.inspect' else (JobExpectedOutput(
        artifact_role='images' if kind==JobKind.PROVIDER else 'result'),)
    return JobRequest(local_job_id=local_id,kind=kind,adapter=descriptor,
        descriptor_id=descriptor,descriptor_version="1",descriptor_digest=digest,
        idempotency_key=f"job-{local_id}-key",parameters={"prompt":"one"} if kind==JobKind.PROVIDER else {},
        expected_outputs=outputs)


async def setup(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/'coordinator.db'));await store.initialize()
    return store,JobCoordinator(store,catalog(),draft_ttl_seconds=300)


async def harness_origin(store):
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level=profile.reasoning_level,title='Delegation'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='delegate',
        idempotency_key='delegation-origin-'+identifier()),profile,'owner')
    return thread,turn


async def test_free_durable_group_auto_commits_but_inline_operation_is_rejected(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        value=JobGroupCreate(thread_id="a"*32,origin_turn_id="b"*32,
            idempotency_key="free-group-key",jobs=[job("free")])
        prepared=await coordinator.prepare_group("owner",value)
        assert prepared.mode=="committed" and prepared.group.member_count==1
        assert prepared.jobs[0].state==JobState.CREATED
        with pytest.raises(ValueError,match="inline Operation"):
            await coordinator.prepare_group("owner",JobGroupCreate(
                thread_id="a"*32,origin_turn_id="b"*32,
                idempotency_key="inline-group-key",jobs=[job("inline","inline.inspect")]))
    finally:await store.close()


def test_artifact_producing_descriptor_requires_an_explicit_output_contract():
    coordinator=JobCoordinator(SimpleNamespace(),catalog())
    for request in (job('empty-operation').model_copy(update={'expected_outputs':()}),
            job('empty-provider','image.generate').model_copy(update={'expected_outputs':()})):
        with pytest.raises(ValueError,match='requires expected outputs'):
            coordinator._normalize(JobGroupCreate(thread_id='a'*32,origin_turn_id='b'*32,
                idempotency_key='missing-output-'+request.local_job_id,jobs=(request,)))


def test_child_group_scope_allows_only_pre_authorized_tools():
    claims=SimpleNamespace(job_id='f'*32,generation=2,thread_id='a'*32,turn_id='b'*32,
        operation_ids=['free.compute'],capability_ids=['image.generate'])
    value=JobGroupCreate(thread_id='1'*32,origin_turn_id='2'*32,
        idempotency_key='child-tool-group',jobs=[job('free')])
    scoped=JobCoordinator.scope_child_group(value,claims)
    assert scoped.thread_id==claims.thread_id and scoped.origin_turn_id==claims.turn_id
    assert scoped.authorization_snapshot['parent_job_id']==claims.job_id
    with pytest.raises(ValueError,match='child_harness_recursion_forbidden'):
        JobCoordinator.scope_child_group(value.model_copy(update={'jobs':(JobRequest(
            local_job_id='nested',kind=JobKind.HARNESS,adapter='child-harness',
            descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='nested-harness'),)}),claims)


async def test_one_paid_member_holds_complete_mixed_batch_until_single_confirmation(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        value=JobGroupCreate(thread_id="a"*32,origin_turn_id="b"*32,
            idempotency_key="mixed-group-key",jobs=[job("free"),job("paid","image.generate")])
        prepared=await coordinator.prepare_group("owner",value)
        assert prepared.mode=="draft" and prepared.draft.requires_confirmation
        assert await store.list_jobs_for_thread("a"*32)==[]
        confirmed=await coordinator.confirm_group(prepared.draft.id,"owner")
        assert confirmed.group.member_count==2
        assert {value.local_job_id for value in confirmed.jobs}=={"free","paid"}
    finally:await store.close()


async def test_unavailable_descriptor_is_rejected_before_draft_and_rechecked_on_confirm(tmp_path):
    class Readiness:
        available=True
        async def require_available(self,descriptor_id):
            if descriptor_id=='image.generate' and not self.available:
                raise ValueError('descriptor_unavailable:image.generate:provider_offline')
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/'readiness.db'));await store.initialize()
    readiness=Readiness();coordinator=JobCoordinator(store,catalog(),readiness=readiness)
    value=JobGroupCreate(thread_id='a'*32,origin_turn_id='b'*32,
        idempotency_key='readiness-group',jobs=[job('paid','image.generate')])
    try:
        readiness.available=False
        with pytest.raises(ValueError,match='descriptor_unavailable'):
            await coordinator.prepare_group('owner',value)
        assert await store.list_jobs_for_thread('a'*32)==[]
        readiness.available=True;prepared=await coordinator.prepare_group('owner',value)
        readiness.available=False
        with pytest.raises(ValueError,match='descriptor_unavailable'):
            await coordinator.confirm_group(prepared.draft.id,'owner')
        assert await store.list_jobs_for_thread('a'*32)==[]
    finally:await store.close()


async def test_provider_preflight_rejects_before_confirmation_persistence(tmp_path):
    class Preflight:
        calls=[]
        async def validate(self,request):
            self.calls.append(request.local_job_id)
            raise ValueError('provider_preflight_invalid:reference_mime')
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/'preflight.db'));await store.initialize()
    preflight=Preflight();coordinator=JobCoordinator(store,catalog(),provider_preflight=preflight)
    try:
        with pytest.raises(ValueError,match='provider_preflight_invalid'):
            await coordinator.prepare_group('owner',JobGroupCreate(thread_id='a'*32,
                origin_turn_id='b'*32,idempotency_key='preflight-group',
                jobs=[job('paid','image.generate')]))
        assert preflight.calls==['paid'] and await store.list_jobs_for_thread('a'*32)==[]
    finally:await store.close()


async def test_confirmed_scope_is_inherited_by_contained_terminal_replacement(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        original=await coordinator.prepare_group('owner',JobGroupCreate(
            thread_id='a'*32,origin_turn_id='b'*32,idempotency_key='authorized-original-group',
            jobs=[job('paid','image.generate')]))
        confirmed=await coordinator.confirm_group(original.draft.id,'owner');source=confirmed.jobs[0]
        attempt=await store.claim_job(source.id,'worker','node',30)
        await store.transition_job(source.id,JobState.FAILED,expected_generation=attempt.generation,
            result={'error_code':'provider_processing_failed'})
        replacement=job('paid-replacement','image.generate').model_copy(update={
            'idempotency_key':'paid-replacement-request','replaces_job_id':source.id})
        inherited=await coordinator.prepare_group('owner',JobGroupCreate(
            thread_id='a'*32,origin_turn_id='c'*32,idempotency_key='authorized-replacement-group',
            jobs=[replacement]))
        assert inherited.mode=='committed' and inherited.jobs[0].replaces_job_id==source.id

        expanded=replacement.model_copy(update={'local_job_id':'changed-replacement',
            'idempotency_key':'changed-replacement-request','parameters':{'prompt':'changed'}})
        held=await coordinator.prepare_group('owner',JobGroupCreate(
            thread_id='a'*32,origin_turn_id='d'*32,idempotency_key='expanded-replacement-group',
            jobs=[expanded]))
        assert held.mode=='draft' and held.draft.requires_confirmation
    finally:await store.close()


async def test_descriptor_freezes_adapter_schema_roles_network_resources_and_retry(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        with pytest.raises(ValueError,match='parameter'):
            await coordinator.prepare_group('owner',JobGroupCreate(thread_id='a'*32,
                origin_turn_id='b'*32,idempotency_key='invalid-provider-parameters',jobs=[
                    job('bad','image.generate').model_copy(update={'parameters':{'unknown':True}})]))
        with pytest.raises(ValueError,match='adapter'):
            await coordinator.prepare_group('owner',JobGroupCreate(thread_id='a'*32,
                origin_turn_id='b'*32,idempotency_key='invalid-provider-adapter',jobs=[
                    job('bad-adapter','image.generate').model_copy(update={'adapter':'other'})]))
        output=JobExpectedOutput(artifact_role='images',output_disposition=OutputDisposition.DELIVERY)
        proposed=job('paid','image.generate').model_copy(update={'expected_outputs':(output,),
            'network_origins':('invented-network',),'resource_requirements':{'node_id':'invented'},
            'retry_policy':{'max_pre_accept_attempts':99}})
        prepared=await coordinator.prepare_group('owner',JobGroupCreate(thread_id='a'*32,
            origin_turn_id='b'*32,idempotency_key='frozen-provider-policy',jobs=[proposed]))
        frozen=(await store.get_job_group_draft_request(prepared.draft.id,'owner')).jobs[0]
        assert frozen.adapter=='image.generate' and frozen.network_origins==('image-provider',)
        assert frozen.resource_requirements=={'cpu':1,'memory_mb':128,'gpu':0,
            'required_capability_ids':['image.generate']}
        assert frozen.retry_policy=={'max_pre_accept_attempts':2}
        assert frozen.cancellation_policy=={'supported':True}
        assert frozen.reconciliation_policy=={'supported':True}
    finally:await store.close()


async def test_descriptor_rejects_undeclared_output_validator_before_job_creation(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"validator-contract.db"));await store.initialize()
    value=catalog().payload.model_dump(mode="json")
    value["operations"][0]["output_validators"]={"result":["result.valid"]}
    coordinator=JobCoordinator(store,JobCatalog(JobCatalogPayload.model_validate(value),"e"*64))
    try:
        invalid=job("invalid-validator").model_copy(update={"expected_outputs":(
            JobExpectedOutput(artifact_role="result",validators=("transparent_image",)),)})
        with pytest.raises(ValueError,match="validator.*Descriptor"):
            await coordinator.prepare_group("owner",JobGroupCreate(thread_id="a"*32,
                origin_turn_id="b"*32,idempotency_key="invalid-validator-group",jobs=(invalid,)))
        assert await store.list_jobs_for_thread("a"*32)==[]

        valid=job("valid-validator").model_copy(update={"expected_outputs":(
            JobExpectedOutput(artifact_role="result",validators=("result.valid",)),)})
        prepared=await coordinator.prepare_group("owner",JobGroupCreate(thread_id="a"*32,
            origin_turn_id="b"*32,idempotency_key="valid-validator-group",jobs=(valid,)))
        assert prepared.mode=="committed"
    finally:await store.close()


def test_provider_descriptor_semantics_reject_wrong_kind_and_mode_before_draft():
    raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    frozen=JobCatalog.from_json(raw,expected_digest=__import__('hashlib').sha256(raw.encode()).hexdigest())
    coordinator=JobCoordinator(SimpleNamespace(),frozen)
    digests={row.id:row.digest for row in frozen.payload.capabilities}
    outputs={'image.generate':'generated_images','video.generate':'generated_videos',
        'video.edit':'generated_videos','audio.generate':'generated_audio',
        'model3d.generate':'generated_models','model3d.transform':'generated_models'}
    def request(descriptor,kind,mode):
        return JobRequest(local_job_id='semantic-provider',kind=JobKind.PROVIDER,adapter=descriptor,
            descriptor_id=descriptor,descriptor_version='1',descriptor_digest=digests[descriptor],
            idempotency_key='semantic-provider-request',parameters={'kind':kind,'model':'model',
                'generation_mode':mode,'parameters':{}},expected_outputs=(JobExpectedOutput(
                    artifact_role=outputs[descriptor]),))
    def group(job_request):
        return JobGroupCreate(thread_id='a'*32,origin_turn_id='b'*32,
            idempotency_key='semantic-provider-group',jobs=(job_request,))

    for invalid in (request('image.generate','video','text_to_video'),
            request('video.generate','video','edit_video'),
            request('model3d.transform','model3d','text_to_model')):
        with pytest.raises(ValueError,match='parameter schema'):
            coordinator._normalize(group(invalid))

    normalized=coordinator._normalize(group(
        request('video.generate','video','first_frame'))).jobs[0]
    assert normalized.resource_requirements['required_capability_ids']==['video.generate']
    assert normalized.network_origins==('video-provider',)


async def test_descriptor_review_policy_cannot_be_weakened_by_request(tmp_path):
    store=Store("sqlite+aiosqlite:///"+str(tmp_path/"review-policy.db"));await store.initialize()
    value=catalog().payload.model_dump(mode="json")
    value["operations"][0]["review_policy"]="required"
    value["capabilities"][0]["review_policy"]="optional"
    coordinator=JobCoordinator(store,JobCatalog(JobCatalogPayload.model_validate(value),"e"*64))
    try:
        operation_output=JobExpectedOutput(artifact_role="result",
            output_disposition=OutputDisposition.DELIVERY,requires_review=False)
        operation=job("reviewed-operation").model_copy(update={"expected_outputs":(operation_output,)})
        prepared=await coordinator.prepare_group("owner",JobGroupCreate(thread_id="a"*32,
            origin_turn_id="b"*32,idempotency_key="required-review-policy",jobs=(operation,)))
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert frozen.expected_outputs[0].requires_review is True
        assert frozen.expected_outputs[0].review_policy=="required"

        capability_output=JobExpectedOutput(artifact_role="images",
            output_disposition=OutputDisposition.WORKSPACE,requires_review=False)
        capability=job("reviewed-capability","image.generate").model_copy(
            update={"expected_outputs":(capability_output,)})
        prepared=await coordinator.prepare_group("owner",JobGroupCreate(thread_id="a"*32,
            origin_turn_id="b"*32,idempotency_key="optional-review-policy",jobs=(capability,)))
        draft=await store.get_job_group_draft_request(prepared.draft.id,"owner")
        assert draft.jobs[0].expected_outputs[0].requires_review is True
        assert draft.jobs[0].expected_outputs[0].review_policy=="optional"

        disabled_coordinator=JobCoordinator(store,catalog())
        disabled=job("disabled-review").model_copy(update={"expected_outputs":(operation_output.model_copy(
            update={"requires_review":False}),)})
        prepared=await disabled_coordinator.prepare_group("owner",JobGroupCreate(thread_id="a"*32,
            origin_turn_id="b"*32,idempotency_key="disabled-review-policy",jobs=(disabled,)))
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert frozen.expected_outputs[0].requires_review is False
        assert frozen.expected_outputs[0].review_policy=="disabled"
    finally:await store.close()


async def test_edit_remove_uses_optimistic_digest_and_confirmation_freezes_result(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        prepared=await coordinator.prepare_group("owner",JobGroupCreate(
            thread_id="a"*32,origin_turn_id="b"*32,idempotency_key="editable-group-key",
            jobs=[job("free"),job("paid","image.generate")]))
        edited=await coordinator.edit_group(prepared.draft.id,"owner",JobGroupDraftUpdate(
            expected_digest=prepared.draft.request_digest,jobs=(job("paid","image.generate"),)))
        assert edited.request_digest!=prepared.draft.request_digest
        with pytest.raises(RuntimeError,match="draft_digest_conflict"):
            await coordinator.edit_group(edited.id,"owner",JobGroupDraftUpdate(
                expected_digest=prepared.draft.request_digest,jobs=(job("free"),)))
        confirmed=await coordinator.confirm_group(edited.id,"owner")
        assert [value.local_job_id for value in confirmed.jobs]==["paid"]
        with pytest.raises(RuntimeError,match="draft_not_editable"):
            await coordinator.edit_group(edited.id,"owner",JobGroupDraftUpdate(
                expected_digest=edited.request_digest,jobs=(job("free"),)))
    finally:await store.close()


async def test_reject_expire_and_cancel_scopes_are_truthful(tmp_path):
    store,coordinator=await setup(tmp_path)
    try:
        rejected=await coordinator.prepare_group("owner",JobGroupCreate(
            thread_id="a"*32,origin_turn_id="b"*32,idempotency_key="reject-group-key",
            jobs=[job("paid","image.generate")]))
        assert (await coordinator.reject_group(rejected.draft.id,"owner")).state.value=="REJECTED"
        assert await store.list_jobs_for_thread("a"*32)==[]
        committed=await coordinator.prepare_group("owner",JobGroupCreate(
            thread_id="a"*32,origin_turn_id="b"*32,idempotency_key="cancel-group-key",
            jobs=[job("free-one"),job("free-two")]))
        cancelled=await coordinator.cancel_job(committed.jobs[0].id,"owner")
        assert cancelled.cancel_requested and cancelled.state==JobState.CANCELLING
        group_jobs=await coordinator.cancel_group(committed.group.id,"owner")
        assert all(value.cancel_requested for value in group_jobs)
    finally:await store.close()


async def test_harness_infrastructure_fields_are_resolved_by_host_not_manager(tmp_path):
    import hashlib,json
    from pathlib import Path
    from sandbox_harness.common.catalog.profiles import (ProfileResolver)
    root=Path(__file__).resolve().parents[1]
    raw=(root/'deploy/profile-catalog.example.json').read_text()
    resolver=ProfileResolver.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest(),
        skill_manifest=json.loads((root/'runtime/skills/manifest.json').read_text()),provider="opensandbox")
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'harness-coordinator.db'));await store.initialize()
    coordinator=JobCoordinator(store,catalog(),profile_resolver=resolver)
    try:
        thread,turn=await harness_origin(store)
        child=JobRequest(local_job_id='child',kind=JobKind.HARNESS,adapter='child-harness',
            descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='child-coordinator-key',parameters={'harness':'claude',
                'model':'claude-sonnet-4-5','reasoning_effort':'low','objective':'write plan'},
            child_profile_id='child-general',runtime_profile_id='manager-invented',
            skill_ids=('writing-plans',),resource_requirements={'harness':'claude','cpu':1,
                'node_id':'manager-node','gpu_ids':['gpu-9'],'image_digest':'e'*64})
        prepared=await coordinator.prepare_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='child-coordinator-group',jobs=[child]))
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert frozen.runtime_profile_id=='general-cpu'
        assert frozen.resource_requirements['image_digest']=='1'*64
        assert frozen.resource_requirements['required_capability_ids']==[]
        assert frozen.network_origins==()
        assert 'node_id' not in frozen.resource_requirements and 'gpu_ids' not in frozen.resource_requirements
        assert frozen.resource_requirements['delegation_depth']==1
        assert frozen.parameters['harness']=='codex' and frozen.parameters['model']=='gpt-5.6'
        assert frozen.parameters['reasoning_effort']=='medium'
        assert frozen.retry_policy['max_collection_attempts']==2
    finally:await store.close()


async def test_harness_skill_metadata_freezes_required_capability_and_network(tmp_path):
    import hashlib,json
    from pathlib import Path
    from sandbox_harness.common.catalog.profiles import (ProfileResolver)
    root=Path(__file__).resolve().parents[1]
    raw=(root/'deploy/profile-catalog.example.json').read_text()
    resolver=ProfileResolver.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest(),
        skill_manifest=json.loads((root/'runtime/skills/manifest.json').read_text()),provider="opensandbox")
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'skill-capabilities.db'));await store.initialize()
    coordinator=JobCoordinator(store,catalog(),profile_resolver=resolver)
    try:
        thread,turn=await harness_origin(store)
        child=JobRequest(local_job_id='slides',kind=JobKind.HARNESS,adapter='child-harness',
            descriptor_id='delegate.harness',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='slides-capability-key',parameters={'harness':'codex','objective':'make slides'},
            child_profile_id='child-general',skill_ids=('codex-ppt',))
        prepared=await coordinator.prepare_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='slides-capability-group',jobs=[child]))
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert frozen.runtime_profile_id=='primary-full-cpu'
        assert frozen.resource_requirements['required_capability_ids']==['image.generate']
        assert frozen.network_origins==('image-provider',)
    finally:await store.close()


async def test_harness_missing_adapter_resolves_to_registered_child_adapter(tmp_path):
    import hashlib,json
    from pathlib import Path
    from sandbox_harness.common.catalog.profiles import (ProfileResolver)
    root=Path(__file__).resolve().parents[1]
    raw=(root/'deploy/profile-catalog.example.json').read_text()
    resolver=ProfileResolver.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest(),
        skill_manifest=json.loads((root/'runtime/skills/manifest.json').read_text()),provider="opensandbox")
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'harness-adapter.db'));await store.initialize()
    coordinator=JobCoordinator(store,catalog(),profile_resolver=resolver)
    try:
        thread,turn=await harness_origin(store)
        child=JobRequest.model_validate({'local_job_id':'child','kind':'harness',
            'descriptor_id':'delegate.harness','descriptor_version':'1','descriptor_digest':'d'*64,
            'idempotency_key':'child-adapter-key','parameters':{'harness':'codex','objective':'write plan'},
            'child_profile_id':'child-general'})
        assert child.adapter=='child-harness'
        prepared=await coordinator.prepare_group('owner',JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='child-adapter-group',jobs=[child]))
        frozen=await store.get_job_request(prepared.jobs[0].id)
        assert frozen.adapter=='child-harness'
    finally:await store.close()


async def test_harness_descriptor_identity_is_frozen(tmp_path):
    import hashlib,json
    from pathlib import Path
    from sandbox_harness.common.catalog.profiles import (ProfileResolver)
    root=Path(__file__).resolve().parents[1]
    raw=(root/'deploy/profile-catalog.example.json').read_text()
    resolver=ProfileResolver.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest(),
        skill_manifest=json.loads((root/'runtime/skills/manifest.json').read_text()),provider="opensandbox")
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'harness-descriptor.db'));await store.initialize()
    coordinator=JobCoordinator(store,catalog(),profile_resolver=resolver)
    try:
        thread,turn=await harness_origin(store)
        child=JobRequest(local_job_id='child',kind=JobKind.HARNESS,adapter='child-harness',
            descriptor_id='invented.harness',descriptor_version='1',descriptor_digest='d'*64,
            idempotency_key='child-descriptor-key',parameters={'harness':'codex','objective':'write plan'},
            child_profile_id='child-general')
        with pytest.raises(ValueError,match='Harness Job must use the frozen delegation Descriptor'):
            await coordinator.prepare_group('owner',JobGroupCreate(thread_id=thread.id,
                origin_turn_id=turn.id,idempotency_key='child-descriptor-group',jobs=[child]))
    finally:await store.close()
