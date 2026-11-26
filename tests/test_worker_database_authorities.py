from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_core_only_workers_use_explicit_core_authority():
    for relative, service in (
        ("src/sandbox_harness/services/allocation_worker/allocation_service.py", "allocation-worker"),
        ("src/sandbox_harness/services/controlled_worker/controlled_service.py", "controlled-worker"),
    ):
        source = (ROOT / relative).read_text()
        assert f'for_service("{service}")["core"]' in source
        assert "settings.database_url" not in source
        assert "common.db.store" not in source


def test_job_worker_declares_core_and_media_authorities():
    source = (ROOT / "src/sandbox_harness/services/job_worker/job_service.py").read_text()
    assert 'for_service("job-worker")' in source
    assert 'databases["core"]' in source and 'databases["media"]' in source
    assert "MediaAuthorityClient(store,media)" in source
    assert "settings.database_url" not in source
    assert "common.db.store" not in source
