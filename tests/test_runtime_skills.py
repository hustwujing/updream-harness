import ast
import io
import importlib.util
import json
from pathlib import Path
import tarfile

from fastapi.testclient import TestClient

import sandbox_runtime.app as runtime_app
from sandbox_runtime.skills import destination_root, materialize_skills


ROOT = Path(__file__).resolve().parents[1]


def test_skill_bundle_manifest_is_frozen_complete_and_licensed():
    spec=importlib.util.spec_from_file_location('runtime_skill_bundle',ROOT/'runtime/skill_bundle.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    expected=json.loads((ROOT/'runtime/skills/manifest.json').read_text())
    assert module.expanded()==expected
    assert len(expected['skills'])==33
    assert {row['id'] for row in expected['skills']}=={
        path.parent.name for path in (ROOT/'runtime/skills').glob('*/SKILL.md')}
    for row in expected['skills']:
        directory=ROOT/'runtime/skills'/row['id']
        assert row['file_count']>0 and len(row['content_sha256'])==64
        assert (directory/row['entry']).is_file()
        if row['license']=='per-skill LICENSE.txt':assert (directory/'LICENSE.txt').is_file()
        elif row['license']=='MIT':assert (directory/'LICENSE').is_file()
        else:assert row['id']=='doc-coauthoring' and row['license']=='not declared upstream'


def test_skill_smoke_matrix_covers_every_bundled_skill():
    tree=ast.parse((ROOT/'runtime/skill_smoke.py').read_text())
    assignment=next(node for node in tree.body
        if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name)
            and target.id=='REQUIRED_FILES' for target in node.targets))
    matrix=ast.literal_eval(assignment.value)
    manifest=json.loads((ROOT/'runtime/skills/manifest.json').read_text())
    assert set(matrix)=={row['id'] for row in manifest['skills']}


def test_skills_materialize_for_each_native_harness_and_upgrade_managed_entries(tmp_path, monkeypatch):
    monkeypatch.setenv('SANDBOX_RUNTIME_SKILL_ROOT',str(ROOT/'runtime/skills'))
    all_ids=[row['id'] for row in json.loads((ROOT/'runtime/skills/manifest.json').read_text())['skills']]
    for harness in ('codex','claude','deepseek'):
        workspace=tmp_path/harness
        root=destination_root(harness,workspace);root.mkdir(parents=True)
        stale=root/'anatomy';stale.mkdir();(stale/'old.txt').write_text('old')
        (root/'custom-skill').mkdir()
        (root/'.image-bundle.json').write_text(json.dumps({'bundle_version':'old','skills':['anatomy','removed-skill']}))
        (root/'removed-skill').mkdir()
        result=materialize_skills(harness,workspace,all_ids)
        assert result['bundle_version']=='2026.11.14.2' and result['skill_count']==33
        assert (root/'anatomy').is_symlink() and (root/'anatomy/SKILL.md').is_file()
        assert not (root/'removed-skill').exists()
        assert (root/'custom-skill').is_dir()
        assert len(list(root.glob('*/SKILL.md')))==33


def test_deferred_skill_install_replaces_checkpoint_copy_after_restore(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    monkeypatch.setattr(runtime_app,'PERSISTENT',('project','state'))
    monkeypatch.setenv('SANDBOX_RUNTIME_SKILL_ROOT',str(ROOT/'runtime/skills'))
    archive=io.BytesIO()
    with tarfile.open(fileobj=archive,mode='w') as stream:
        body=b'old checkpoint copy'
        member=tarfile.TarInfo('state/codex/skills/anatomy/SKILL.md');member.size=len(body)
        stream.addfile(member,io.BytesIO(body))
    with TestClient(runtime_app.create_app()) as client:
        configured=client.post('/v1/configure',json={'capability_token':'x'*32,
            'gateway_url':'http://gateway:28600','harness':'codex','model':'model',
            'proxy_url':'http://172.31.0.1:28601','defer_skill_install':True,
            'skill_ids':['anatomy']})
        assert configured.status_code==200 and configured.json()['skills_deferred'] is True
        assert not (tmp_path/'workspace/state/codex/skills/anatomy').exists()
        restored=client.put('/v1/checkpoint',content=archive.getvalue())
        assert restored.status_code==200
    installed=tmp_path/'workspace/state/codex/skills/anatomy'
    assert installed.is_symlink()
    assert (installed/'SKILL.md').read_text()!=body.decode()


def test_deferred_restore_revokes_current_managed_skills_before_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    monkeypatch.setattr(runtime_app,'PERSISTENT',('project','state'))
    monkeypatch.setenv('SANDBOX_RUNTIME_SKILL_ROOT',str(ROOT/'runtime/skills'))
    app=runtime_app.create_app()
    with TestClient(app) as client:
        assert client.post('/v1/configure',json={'capability_token':'x'*32,
            'gateway_url':'http://gateway:28600','harness':'codex','model':'model',
            'proxy_url':'http://172.31.0.1:28601','skill_ids':['anatomy']}).status_code==200
        installed=tmp_path/'workspace/state/codex/skills/anatomy'
        assert installed.is_symlink()
        deferred=client.post('/v1/configure',json={'capability_token':'x'*32,
            'gateway_url':'http://gateway:28600','harness':'codex','model':'model',
            'proxy_url':'http://172.31.0.1:28601','skill_ids':[],
            'defer_skill_install':True})
        assert deferred.status_code==200 and not installed.exists()


def test_checkpoint_restore_never_writes_through_materialized_skill_link(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    monkeypatch.setattr(runtime_app,'PERSISTENT',('project','state'))
    monkeypatch.setenv('SANDBOX_RUNTIME_SKILL_ROOT',str(ROOT/'runtime/skills'))
    archive=io.BytesIO()
    with tarfile.open(fileobj=archive,mode='w') as stream:
        body=b'overwrite attempt';member=tarfile.TarInfo('state/codex/skills/anatomy/SKILL.md');member.size=len(body)
        stream.addfile(member,io.BytesIO(body))
    original=(ROOT/'runtime/skills/anatomy/SKILL.md').read_bytes()
    with TestClient(runtime_app.create_app()) as client:
        configured=client.post('/v1/configure',json={'capability_token':'x'*32,
            'gateway_url':'http://gateway:28600','harness':'codex','model':'model',
            'proxy_url':'http://172.31.0.1:28601','skill_ids':['anatomy']})
        assert configured.status_code==200
        restored=client.put('/v1/checkpoint',content=archive.getvalue())
        assert restored.status_code==400 and restored.json()['detail']=='checkpoint_archive_invalid'
    assert (ROOT/'runtime/skills/anatomy/SKILL.md').read_bytes()==original


def test_runtime_materializes_only_granted_skills_and_removes_revoked_managed_paths(tmp_path, monkeypatch):
    monkeypatch.setenv('SANDBOX_RUNTIME_SKILL_ROOT',str(ROOT/'runtime/skills'))
    workspace=tmp_path/'workspace'
    destination=destination_root('codex',workspace);destination.mkdir(parents=True)
    (destination/'custom-skill').mkdir()
    first=materialize_skills('codex',workspace,['anatomy','writing-plans'])
    assert first['skill_ids']==['anatomy','writing-plans']
    assert (destination/'anatomy').is_symlink() and (destination/'writing-plans').is_symlink()
    assert not (destination/'docx').exists()
    second=materialize_skills('codex',workspace,['writing-plans'])
    assert second['revoked_skill_ids']==['anatomy']
    assert not (destination/'anatomy').exists()
    assert (destination/'writing-plans').is_symlink()
    assert (destination/'custom-skill').is_dir()


def test_runtime_configure_reports_authoritative_capability_change(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    monkeypatch.setenv('SANDBOX_RUNTIME_SKILL_ROOT',str(ROOT/'runtime/skills'))
    manifest=json.loads((ROOT/'runtime/skills/manifest.json').read_text())
    anatomy=next(row for row in manifest['skills'] if row['id']=='anatomy')
    with TestClient(runtime_app.create_app()) as client:
        first=client.post('/v1/configure',json={'capability_token':'x'*32,
            'gateway_url':'http://gateway:28600','harness':'codex','model':'model',
            'proxy_url':'http://172.31.0.1:28601','runtime_profile_id':'web-browser',
            'skill_ids':['anatomy'],'skill_digests':{'anatomy':anatomy['content_sha256']}})
        assert first.status_code==200
        assert first.json()['capability_change']['added_skill_ids']==['anatomy']
        second=client.post('/v1/configure',json={'capability_token':'x'*32,
            'gateway_url':'http://gateway:28600','harness':'codex','model':'model',
            'proxy_url':'http://172.31.0.1:28601','runtime_profile_id':'general-cpu',
            'skill_ids':[],'skill_digests':{}})
        frame=second.json()['capability_change']
        assert frame['previous_runtime_profile_id']=='web-browser'
        assert frame['runtime_profile_id']=='general-cpu'
        assert frame['revoked_skill_ids']==['anatomy']
        assert not (destination_root('codex',tmp_path/'workspace')/'anatomy').exists()


def test_runtime_image_declares_skill_dependencies_and_domestic_mirrors():
    foundation=(ROOT/'runtime/foundation.Dockerfile').read_text()
    base=(ROOT/'runtime/base.Dockerfile').read_text()
    for package in ('dvisvgm','graphviz','libcairo2','libpango-1.0-0','libpangocairo-1.0-0','libreoffice-core',
            'pandoc','poppler-utils','qpdf','tesseract-ocr','texlive-latex-base','texlive-latex-extra'):
        assert package in foundation
    for version in ('ARG REACT_VERSION=19.3.1','ARG DOCX_VERSION=9.15.3',
            'ARG PPTXGENJS_VERSION=4.0.1','ARG PNPM_VERSION=12.18.0'):
        assert version in foundation
    assert 'registry.npmmirror.com' in foundation
    assert 'runtime/vendor/drawio-amd64-${DRAWIO_VERSION}.deb' in foundation
    assert 'runtime/vendor/npm-prefix/lib/node_modules/' in foundation
    assert 'chmod -R a+rX /usr/local/lib/node_modules /usr/local/bin' in foundation
    assert 'DRAWIO_SHA256=' in foundation and 'sha256sum -c -' in foundation
    assert 'mirrors.cloud.aliyuncs.com' in base
    assert 'runtime/skill_bundle.py verify' not in base
    assert '"runtime" / "skills"' in (ROOT/'runtime/build_runtime_bundle.py').read_text()
    assert 'chmod -R a+rX runtime' in base
    assert 'ENTRYPOINT ["python", "-m", "sandbox_bootstrap.main"]' in base
    assert 'COPY runtime/skills' not in base
    assert (ROOT / "runtime/build_runtime_bundle.py").is_file()
    assert 'runtime/skill_smoke.py /usr/local/bin/sandbox-skill-smoke' in base
    assert 'runtime/drawio_wrapper.sh /usr/local/bin/drawio' in base
