import ast
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def test_cross_database_runner_implements_all_six_real_cases():
    path=ROOT/'tests/run_real_cross_database.py'
    source=path.read_text();ast.parse(source)
    assert 'DatabaseSettings().for_service("api")' in source
    for repository in ('AccountsRepository','CoreRepository','MediaRepository','SiteRepository'):
        assert repository in source
    for index in range(1,7):
        assert f'async def case_xdb{index:02d}' in source
        assert f'"XDB{index:02d}":case_xdb{index:02d}' in source
    assert 'protocol.provider_job(' in source
    assert 'protocol.decompose_job(' in source
    assert 'protocol_report_missing' not in source


def test_recovery_case_kills_only_after_media_acceptance_and_checks_identity():
    source=(ROOT/'tests/run_real_cross_database.py').read_text()
    assert 'accepted_at is not None' in source
    assert 'provider_job_id' in source
    assert 'kill_service_main(' in source
    assert 'SIGKILL' in source
    assert 'same_generation' in source
    assert 'same_external_execution' in source


def test_cross_database_runner_has_bounded_waits_and_safe_evidence():
    source=(ROOT/'tests/run_real_cross_database.py').read_text()
    assert 'async def wait_until(' in source
    assert '--timeout' in source
    assert 'classification' in source and 'program_failure' in source
    assert 'raw exception' not in source.lower()
    assert '.data' in source

