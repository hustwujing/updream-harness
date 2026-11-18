import ast
import importlib.util
from pathlib import Path


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


def test_inherited_non_protocol_cases_require_real_business_artifact_execution():
    source=(ROOT/'tests/run_real_inherited_protocol.py').read_text()
    assert "campaign.primary_case" in source
    assert "business_artifact_verified" in source
    assert "if case.get('expect',{}).get('kind')=='protocol'" in source


def test_destructive_protocols_are_serialized_and_business_assets_are_configured():
    inherited=(ROOT/'tests/run_real_inherited_protocol.py').read_text()
    parallel=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    assert 'async with parallel.protocol_lock():protocol=await handler' in inherited
    assert 'async with protocol_lock()' in parallel
    assert "args.asset_root=" in inherited and "ResilientAsyncClient" in inherited


def test_continuation_protocols_wait_for_state_instead_of_fixed_sleep():
    source=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    assert 'async def wait_continuations' in source
    assert source.count('await wait_continuations(')>=5
    assert "await wait_jobs(client,committed['jobs'],1200);await asyncio.sleep(3)" not in source


def test_primary_decision_protocol_stops_after_terminal_model_failure():
    source=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    block=source[source.index('async def case_p23'):source.index('async def case_p24')]
    assert "if final['state']!='COMPLETED'" in block
    assert "'non_program_failure'" in block and "code.startswith" in block


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
