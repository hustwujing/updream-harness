import importlib.util
from pathlib import Path
import shutil

import pytest


ROOT=Path(__file__).resolve().parents[1]


def load_builder():
    path=ROOT/'scripts/build_controlled_runtime_manifest.py'
    spec=importlib.util.spec_from_file_location('controlled_manifest',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def runtime_fixture(tmp_path,module):
    source=tmp_path/'source'
    for name in (*module.CRITICAL_FILES,*module.ENVIRONMENT_MARKERS):
        path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(name)
    for name in module.RUNTIME_TREES:
        path=source/name/'module.py';path.parent.mkdir(parents=True,exist_ok=True);path.write_text(name)
    return source


def test_controlled_runtime_manifest_is_deterministic_and_entry_is_owned_snapshot(tmp_path):
    module=load_builder();source=runtime_fixture(tmp_path,module)
    restricted=source/'restricted';restricted.mkdir(mode=0o700)
    tool=restricted/'tool';tool.write_text('runtime data');tool.chmod(0o600)
    first=module.build_manifest(source);second=module.build_manifest(source)
    assert first==second and len(first['digest'])==64
    entry=tmp_path/'parallel-data/provider-runtime'
    snapshot=module.materialize_entry(source,entry,first)
    assert snapshot==entry.parent/'controlled-runtime'/first['digest']
    assert entry.is_symlink() and entry.resolve()==snapshot.resolve()
    assert restricted.stat().st_mode & 0o777==0o700
    assert (snapshot/'restricted').stat().st_mode & 0o777==0o555
    assert (snapshot/'restricted/tool').stat().st_mode & 0o777==0o444
    assert module.materialize_entry(source,entry,first)==snapshot
    shutil.rmtree(source)
    rebuilt=module.build_manifest(snapshot)
    assert rebuilt['files']==first['files']


def test_controlled_runtime_snapshot_materializes_external_symlinks(tmp_path):
    module=load_builder();source=runtime_fixture(tmp_path,module)
    external=tmp_path/'external';external.write_text('owned bytes')
    (source/'absolute-link').symlink_to(external)
    manifest=module.build_manifest(source);entry=tmp_path/'parallel-data/provider-runtime'
    snapshot=module.materialize_entry(source,entry,manifest)
    copied=snapshot/'absolute-link'
    assert copied.is_file() and not copied.is_symlink()
    assert copied.read_text()=='owned bytes'


def test_controlled_runtime_manifest_covers_import_source_trees(tmp_path):
    module=load_builder();source=runtime_fixture(tmp_path,module)
    first=module.build_manifest(source)
    imported=source/module.RUNTIME_TREES[0]/'module.py';imported.write_text('changed import')
    assert module.build_manifest(source)['digest']!=first['digest']
    shutil.rmtree(source/module.RUNTIME_TREES[1])
    with pytest.raises(FileNotFoundError,match=module.RUNTIME_TREES[1]):
        module.build_manifest(source)


def test_controlled_runtime_snapshot_rejects_corrupt_existing_digest(tmp_path):
    module=load_builder();source=runtime_fixture(tmp_path,module)
    manifest=module.build_manifest(source);entry=tmp_path/'parallel-data/provider-runtime'
    snapshot=module.materialize_entry(source,entry,manifest)
    (snapshot/module.CRITICAL_FILES[0]).chmod(0o644)
    (snapshot/module.CRITICAL_FILES[0]).write_text('corrupt')
    with pytest.raises(ValueError,match='controlled_runtime_snapshot_corrupt'):
        module.materialize_entry(source,entry,manifest)


def test_failed_materialization_keeps_previous_stable_entry(tmp_path,monkeypatch):
    module=load_builder();first_source=runtime_fixture(tmp_path/'first',module)
    first_manifest=module.build_manifest(first_source)
    entry=tmp_path/'parallel-data/provider-runtime'
    first_snapshot=module.materialize_entry(first_source,entry,first_manifest)
    second_source=runtime_fixture(tmp_path/'second',module)
    (second_source/module.CRITICAL_FILES[0]).write_text('new release')
    second_manifest=module.build_manifest(second_source)
    monkeypatch.setattr(module,'_copy_tree',lambda *_: (_ for _ in ()).throw(RuntimeError('copy failed')))
    with pytest.raises(RuntimeError,match='copy failed'):
        module.materialize_entry(second_source,entry,second_manifest)
    assert entry.resolve()==first_snapshot.resolve()
