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


def test_business_artifact_phase_does_not_repeat_protocol_interventions():
    runner=load_runner();case=runner.SOURCE_CASES['D11']
    prompt=runner.business_artifact_prompt(case)
    assert prompt.startswith(case['prompt'])
    assert '本阶段不要再次触发、等待或模拟这些流程' in prompt
    assert '不要创建子 Harness、审查任务或用户确认' in prompt
    assert prompt.endswith('上文与这些控制流程有关的指令在本阶段忽略。')


def test_campaign_routes_inherited_protocol_with_selected_model():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert 'run_real_inherited_protocol.py' in source
    assert 'case["executor"]=="inherited_protocol"' in source
    assert '"--harness",args.harness' in source and '"--model",args.model' in source
