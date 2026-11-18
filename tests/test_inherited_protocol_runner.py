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


def test_campaign_routes_inherited_protocol_with_selected_model():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert 'run_real_inherited_protocol.py' in source
    assert 'case["executor"]=="inherited_protocol"' in source
    assert '"--harness",args.harness' in source and '"--model",args.model' in source
