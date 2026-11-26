import json
from pathlib import Path
import shutil
import stat

import pytest

from sandbox_harness.common.observability.diagnostics import DiagnosticCapture
from sandbox_harness.common.sandbox.runtime import (_reap_stale_downloads)
from sandbox_harness.common.storage.directories import (ensure_private_directory)
from sandbox_harness.common.storage.objects import (ObjectStore)
from sandbox_harness.services.media_worker.media_worker import (MediaWorker)


ROOT=Path(__file__).resolve().parents[1]


def test_disposable_directory_recreates_privately_and_rejects_escape_and_symlink(tmp_path):
    boundary=tmp_path/'state';boundary.mkdir()
    target=boundary/'tmp'/'staging'
    assert ensure_private_directory(target,boundary=boundary) is True
    assert stat.S_IMODE(target.stat().st_mode)==0o700
    assert ensure_private_directory(target,boundary=boundary) is False
    shutil.rmtree(target);assert ensure_private_directory(target,boundary=boundary) is True
    target.rmdir();target.symlink_to(tmp_path,target_is_directory=True)
    with pytest.raises(ValueError,match='symlink'):
        ensure_private_directory(target,boundary=boundary)
    with pytest.raises(ValueError,match='outside_boundary'):
        ensure_private_directory(tmp_path/'outside',boundary=boundary)


def test_runtime_media_diagnostic_and_local_object_writers_recover_deleted_roots(tmp_path):
    runtime=tmp_path/'runtime-downloads';_reap_stale_downloads(runtime)
    shutil.rmtree(runtime);_reap_stale_downloads(runtime);assert runtime.is_dir()

    media=object.__new__(MediaWorker);media.staging_root=tmp_path/'provider-outputs'
    media.process_start='test';media._prepare_staging();shutil.rmtree(media.staging_root)
    media._prepare_staging();assert media.staging_root.is_dir()

    capture=DiagnosticCapture(tmp_path/'diagnostics','api','instance',mode='metadata',session='recovery')
    values=dict(direction='ingress',method='GET',url='http://test/health',request_headers={},
        request_body=b'',response_headers={},response_body=b'ok',status_code=200)
    assert capture.record_http(**values);shutil.rmtree(capture.root)
    assert capture.record_http(**values) and capture.path.is_file()

    objects=ObjectStore(tmp_path/'objects');objects.put(b'before');shutil.rmtree(objects.root)
    reference,digest=objects.put(b'after')
    assert objects.get(reference,maximum=100)==b'after' and reference=='sha256:'+digest


def test_every_host_temporary_write_boundary_reasserts_its_directory():
    checks={
        'src/sandbox_harness/common/artifacts/artifact_gateway.py':'ensure_private_directory(self.staging_root',
        'src/sandbox_harness/common/sandbox/runtime.py':'ensure_private_directory(root,boundary=root.parent)',
        'src/sandbox_harness/services/media_worker/media_worker.py':'ensure_private_directory(self.staging_root',
        'src/sandbox_harness/services/media_worker/image_decompose_provider.py':'ensure_private_directory(staging_root',
        'src/sandbox_harness/services/site_worker/site_worker.py':'ensure_private_directory(self.staging_root',
        'src/sandbox_harness/services/api/document_preview.py':'ensure_private_directory(cache_root',
        'src/sandbox_harness/services/api/model_discovery.py':'ensure_private_directory(self.snapshot.parent',
        'src/sandbox_harness/services/sandbox_gateway/gateway.py':'ensure_private_directory(staging_root',
        'src/sandbox_harness/services/controlled_worker/controlled_media_adapter.py':'ensure_private_directory(work',
        'src/sandbox_harness/common/storage/objects.py':'ensure_private_directory(target.parent',
    }
    missing=[path for path,marker in checks.items() if marker not in (ROOT/path).read_text()]
    assert missing==[]
