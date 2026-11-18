import importlib.util
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def load_builder():
    path=ROOT/'scripts/build_controlled_runtime_manifest.py'
    spec=importlib.util.spec_from_file_location('controlled_manifest',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def runtime_fixture(tmp_path,module):
    source=tmp_path/'source'
    for name in (*module.CRITICAL_FILES,*module.ENVIRONMENT_MARKERS,
            'lib-director-vF/src/package/module.py'):
        path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(name)
    return source


def test_controlled_runtime_manifest_is_deterministic_and_entry_is_read_only_alias(tmp_path):
    module=load_builder();source=runtime_fixture(tmp_path,module)
    first=module.build_manifest(source);second=module.build_manifest(source)
    assert first==second and len(first['digest'])==64
    entry=tmp_path/'parallel-data/provider-runtime';module.publish_entry(source,entry)
    module.publish_entry(source,entry)
    assert entry.is_symlink() and entry.resolve()==source.resolve()
    (source/module.CRITICAL_FILES[0]).write_text('changed')
    assert module.build_manifest(source)['digest']!=first['digest']
