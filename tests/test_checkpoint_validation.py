import hashlib
import io
import tarfile

import pytest

from sandbox_harness.common.storage.checkpoints import verify_checkpoint_archive
from test_support.checkpoints import checkpoint_bytes


def write(tmp_path,data):
    path=tmp_path/'checkpoint.tar';path.write_bytes(data)
    return path,hashlib.sha256(data).hexdigest()


def test_checkpoint_archive_verifies_digest_paths_and_native_state(tmp_path):
    path,digest=write(tmp_path,checkpoint_bytes())
    result=verify_checkpoint_archive(path,expected_size=path.stat().st_size,
        expected_sha256=digest,harness='codex',native_thread_id='native-1')
    assert result['native_integrity']=='ok'
    assert result['native_thread_id_present'] is True
    assert result['roots']==['project','state']


def test_checkpoint_archive_rejects_digest_and_native_identity_mismatch(tmp_path):
    path,digest=write(tmp_path,checkpoint_bytes())
    with pytest.raises(ValueError,match='checkpoint_archive_invalid'):
        verify_checkpoint_archive(path,expected_size=path.stat().st_size,
            expected_sha256='0'*64,harness='codex',native_thread_id='native-1')
    with pytest.raises(ValueError,match='checkpoint_native_state_invalid'):
        verify_checkpoint_archive(path,expected_size=path.stat().st_size,
            expected_sha256=digest,harness='codex',native_thread_id='missing-native')


def test_checkpoint_archive_rejects_unsafe_member(tmp_path):
    payload=io.BytesIO()
    with tarfile.open(fileobj=payload,mode='w') as archive:
        member=tarfile.TarInfo('../escape');member.size=1
        archive.addfile(member,io.BytesIO(b'x'))
    path,digest=write(tmp_path,payload.getvalue())
    with pytest.raises(ValueError,match='checkpoint_archive_invalid'):
        verify_checkpoint_archive(path,expected_size=path.stat().st_size,
            expected_sha256=digest,harness='claude',native_thread_id='')
