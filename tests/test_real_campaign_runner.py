import ast
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def test_campaign_runner_has_bounded_parallel_stop_and_resume_protocol():
    source=(ROOT/"tests/run_real_campaign.py").read_text();ast.parse(source)
    for value in ("--concurrency","default=8","--resume-report","DEFECT_CLASSES",
            "defect.set()","progress-interval","average_seconds","running","--affected-case"):
        assert value in source
    assert "min(8,args.concurrency)" in source
    assert 'default="deep"' in source
    assert "program_failure" in source and "non_program_failure" in source


def test_intervention_cases_use_messages_not_product_keyword_rules():
    source=(ROOT/"tests/run_real_campaign.py").read_text()
    assert "INTERVENTIONS" in source and "delivery_mode\":\"queue" in source
    assert "re.compile" not in source and "regex" not in source.lower()


def test_parallel_protocol_runner_never_fakes_unimplemented_success():
    source=(ROOT/"tests/run_real_parallel_protocol.py").read_text();ast.parse(source)
    assert "protocol_handler_not_implemented" in source
    assert "HANDLERS" in source and "'P02'" in source and "'P12'" in source
    assert all(f"'P{index:02d}'" in source for index in range(1,21))
    assert "classification':'program_failure'" in source
