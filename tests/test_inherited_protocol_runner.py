import ast
import importlib.util
from pathlib import Path

import pytest


ROOT=Path(__file__).resolve().parents[1]


def load_runner():
    path=ROOT/'tests/run_real_inherited_protocol.py'
    spec=importlib.util.spec_from_file_location('inherited_protocol_runner',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_inherited_runner_has_explicit_handler_for_every_d_case():
    source=(ROOT/'tests/run_real_inherited_protocol.py').read_text();ast.parse(source)
    runner=load_runner()
    assert set(runner.HANDLERS)=={f'D{index:02d}' for index in range(1,52)}
    assert all(callable(value) for value in runner.HANDLERS.values())


def test_inherited_runner_reports_source_and_parallel_boundary():
    runner=load_runner()
    value=runner.decorate('D01','staged_job_continuation',{'classification':'success','job_id':'job'})
    assert value=={'classification':'success','job_id':'job','source_case_id':'D01',
        'source_project':'sandbox-harness-platform-plus',
        'parallel_boundary':'staged_job_continuation'}


def test_structured_job_outcome_failure_preserves_external_classification():
    runner=load_runner();failure=runner.parallel.JobOutcomeFailure([{
        'id':'a'*32,'local_job_id':'image-0','state':'FAILED',
        'result':{'error_code':'provider_endpoint_unavailable','error_detail':'do not expose'}},
        {'id':'b'*32,'local_job_id':'image-1','state':'FAILED',
         'result':{'error_code':'provider_service_unavailable'}}])
    assert failure.outcome=={'classification':'non_program_failure',
        'reason':'provider_endpoint_unavailable,provider_service_unavailable',
        'failed_jobs':['image-0','image-1'],'failures':[
            {'id':'a'*32,'state':'FAILED','error_code':'provider_endpoint_unavailable'},
            {'id':'b'*32,'state':'FAILED','error_code':'provider_service_unavailable'}]}
    assert 'do not expose' not in str(failure)


def test_unknown_or_mixed_job_outcome_failure_remains_program_failure():
    runner=load_runner();failure=runner.parallel.JobOutcomeFailure([{
        'id':'a'*32,'local_job_id':'image-0','state':'FAILED',
        'result':{'error_code':'provider_endpoint_unavailable'}},
        {'id':'b'*32,'local_job_id':'child-0','state':'FAILED',
         'result':{'error_code':'output_contract_violation'}}])
    assert failure.outcome['classification']=='program_failure'
    assert failure.outcome['reason']=='provider_endpoint_unavailable,output_contract_violation'


def test_inherited_non_protocol_cases_require_real_business_artifact_execution():
    source=(ROOT/'tests/run_real_inherited_protocol.py').read_text()
    assert "campaign.primary_case" in source
    assert "business_artifact_verified" in source
    assert "if case.get('expect',{}).get('kind')=='protocol'" in source


def test_destructive_protocols_are_serialized_and_business_assets_are_configured():
    inherited=(ROOT/'tests/run_real_inherited_protocol.py').read_text()
    parallel=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    assert 'parallel.protocol_lock() if serial else parallel.concurrent_case()' in inherited
    assert "SERIAL_CASES={'P13','P19','P21','P27','P28','P29','P31','K08','AS01'}" in parallel
    assert 'boundary=protocol_lock() if args.case in SERIAL_CASES else concurrent_case()' in parallel
    assert 'async with boundary:' in parallel
    assert "args.asset_root=" in inherited and "ResilientAsyncClient" in inherited


async def test_only_shared_service_mutators_take_inherited_protocol_lock(monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    runner=load_runner();entered=[]
    @asynccontextmanager
    async def locked():
        entered.append('locked');yield
    @asynccontextmanager
    async def concurrent():
        entered.append('concurrent');yield
    monkeypatch.setattr(runner.parallel,'protocol_lock',locked)
    monkeypatch.setattr(runner.parallel,'concurrent_case',concurrent)
    async def handler(client,args):return {'classification':'success'}
    for protocol,expected in [('P01','concurrent'),('P13','locked'),('K08','locked'),
                              ('P19','locked'),('P21','locked'),('P27','locked')]:
        monkeypatch.setitem(runner.parallel.HANDLERS,protocol,handler)
        monkeypatch.setitem(runner.SOURCE_CASES,'test',{'expect':{'kind':'protocol'}})
        await runner.adapt('test','boundary',handler)(None,SimpleNamespace())
        assert entered.pop()==expected
        monkeypatch.delitem(runner.parallel.HANDLERS,protocol)


def test_continuation_protocols_wait_for_state_instead_of_fixed_sleep():
    source=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    assert 'async def wait_continuations' in source
    assert source.count('await wait_continuations(')>=5
    assert "await wait_jobs(client,committed['jobs'],1200);await asyncio.sleep(3)" not in source


def test_provider_coverage_reads_media_identity_from_media_database():
    source=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    block=source[source.index('async def verify_provider_coverage'):
        source.index('async def wait_provider_coverage')]
    assert 'media_store=media_repository(settings)' in block
    assert 'await media_store.get_media_job(external_id,owner)' in block
    assert 'await store.get_media_job' not in block


def test_semantic_provider_contract_allows_model_packages_only_for_3d():
    protocol=load_runner().parallel
    model=protocol.semantic_provider_job('model','model3d.generate',kind='model3d',
        model='v3.1',mode='text_to_model',parameters={'prompt':'shape'})
    image=protocol.semantic_provider_job('image','image.generate',kind='image',
        model='seedream-5.0',mode='text_to_image',parameters={'prompt':'shape'})
    assert model['expected_outputs'][0]['min_items']==1
    assert model['expected_outputs'][0]['max_items']==20
    assert image['expected_outputs'][0]['max_items']==1


def test_primary_decision_protocol_stops_after_terminal_model_failure():
    source=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    block=source[source.index('async def case_p23'):source.index('async def case_p24')]
    assert "if final['state']!='COMPLETED'" in block
    assert "'non_program_failure'" in block and "code.startswith" in block
    assert "'optimizable_prompt'" in block
    assert 'primary_did_not_delegate_parallel_work' in block
    assert '相互隔离的执行上下文独立形成证据' in block
    assert 'sandbox-job' not in block


def test_business_artifact_phase_does_not_repeat_protocol_interventions():
    runner=load_runner();case=runner.SOURCE_CASES['D11']
    prompt=runner.business_artifact_prompt(case)
    assert prompt.startswith(case['prompt'])
    assert '本阶段不要再次触发、等待或模拟这些流程' in prompt
    assert '不要创建子 Harness、审查任务或用户确认' in prompt
    assert '上文与这些控制流程有关的指令在本阶段忽略。' in prompt
    assert '最终产物必须满足以下机器可读契约' in prompt


def test_external_business_phase_uses_structured_confirmation_not_chat():
    runner=load_runner();prompt=runner.business_artifact_prompt(runner.SOURCE_CASES['D50'])
    assert '直接创建结构化 JobGroup/媒体授权草案' in prompt
    assert '不要使用普通聊天文本重复询问授权' in prompt


def test_hidden_expected_dimensions_are_explicit_in_business_prompt():
    runner=load_runner();prompt=runner.business_artifact_prompt(runner.SOURCE_CASES['D46'])
    assert '"height":720' in prompt and '"width":1280' in prompt


def test_campaign_routes_inherited_protocol_with_selected_model():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert 'run_real_inherited_protocol.py' in source
    assert 'case["executor"]=="inherited_protocol"' in source
    assert '"--harness",args.harness' in source and '"--model",args.model' in source


def test_usage_rejection_checks_the_supported_durable_draft_endpoint():
    source=(ROOT/'tests/run_real_inherited_protocol.py').read_text()
    assert 'job-group-drafts' in source
    assert "api/threads/{thread['id']}/jobs" not in source
