import importlib.util
import json
from pathlib import Path

import pytest


ROOT=Path(__file__).resolve().parents[1]


def release_tool():
    path=ROOT/"tools/service_release.py"
    spec=importlib.util.spec_from_file_location("service_release_test",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def project(tmp_path):
    root=tmp_path/"project"
    files={
        "src/sandbox_harness/__init__.py":"",
        "src/sandbox_harness/services/__init__.py":"",
        "src/sandbox_harness/services/api/__init__.py":"",
        "src/sandbox_harness/services/api/main.py":"def run(): return 7\n",
    }
    for name,value in files.items():
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(value)
    module={"schema_version":1,"id":"api","kind":"service","location":"host",
        "sources":["src/sandbox_harness/services/api/main.py"],"source_roots":[],
        "entry_points":{"api":"sandbox_harness.services.api.main:run"},"common_modules":[],
        "python_dependencies":[],"native_requirements":[],"external_inputs":[],"resources":[],
        "host_credentials_allowed":True,"verification":["clean import"],
        "compatibility":{"wire_protocol":"v1","schema":"v1","baseline":"test"},
        "migration_stage":"isolated"}
    path=root/"modules/host/api/module.json";path.parent.mkdir(parents=True);path.write_text(json.dumps(module))
    environment=tmp_path/"venv";(environment/"bin").mkdir(parents=True)
    (environment/"pyvenv.cfg").write_text("version = 3.13\n")
    python=environment/"bin/python";python.write_bytes(b"test-python\n");python.chmod(0o755)
    package=environment/"lib/python3.13/site-packages/value.py";package.parent.mkdir(parents=True)
    package.write_text("VALUE=7\n")
    (package.parent/"_editable_impl_sandbox_harness_platform_parallel.pth").write_text(
        str(root/"src")+"\n")
    editable=package.parent/"sandbox_harness_platform_parallel-0.1.0.dist-info"
    editable.mkdir();(editable/"direct_url.json").write_text(json.dumps({"url":str(root)}))
    return root,environment


def built(tmp_path):
    tool=release_tool();root,environment=project(tmp_path)
    data=tmp_path/"data";env=tool.freeze_environment(environment,data/"dependency-environments")
    manifest=tool.build_service_release(root,"api",data/"service-releases",env)
    return tool,data,manifest,env


def test_service_release_selects_verified_source_and_environment_atomically(tmp_path):
    tool,data,manifest,env=built(tmp_path)
    descriptor=tool.select_service_release(data,manifest)
    selected=tool.resolve_service(data,"api")
    assert selected["digest"]==json.loads(manifest.read_text())["digest"]
    assert selected["python"]==str(env.parent/"bin/python")
    assert selected["pythonpath"].endswith("/src")
    assert descriptor.stat().st_mode&0o777==0o444
    assert manifest.stat().st_mode&0o777==0o444
    assert env.stat().st_mode&0o777==0o444
    assert all(path.stat().st_mode&0o777==0o555 for path in manifest.parent.rglob("*")
               if path.is_dir())
    assert not list(env.parent.rglob("_editable_impl_sandbox_harness_platform_parallel*.pth"))
    assert not list(env.parent.rglob("sandbox_harness_platform_parallel-*.dist-info"))
    before=descriptor.read_bytes()
    assert tool.select_service_release(data,manifest).read_bytes()!=before
    rollback=data/"service-rollback/api.json"
    assert rollback.is_file() and json.loads(rollback.read_text())["digest"]==selected["digest"]
    assert json.loads(descriptor.read_text())["manifest"]==str(manifest.resolve())


def test_service_release_rejects_source_and_environment_tampering(tmp_path):
    tool,data,manifest,env=built(tmp_path)
    value=json.loads(manifest.read_text())
    source=manifest.parent/value["source_root"]/"src/sandbox_harness/services/api/main.py"
    source.chmod(0o644);source.write_text("raise RuntimeError\n")
    with pytest.raises(ValueError,match="artifact_file_digest_mismatch"):
        tool.verify_service_release(manifest,output_root=data/"service-releases")
    source.write_text("def run(): return 7\n");source.chmod(0o444)
    package=env.parent/"lib/python3.13/site-packages/value.py";package.write_text("VALUE=8\n")
    with pytest.raises(ValueError,match="release_file_digest_mismatch"):
        tool.verify_service_release(manifest,output_root=data/"service-releases")


def test_service_descriptor_rejects_path_escape_and_service_mismatch(tmp_path):
    tool,data,manifest,_=built(tmp_path)
    descriptor=tool.select_service_release(data,manifest)
    value=json.loads(descriptor.read_text());value["manifest"]=str(ROOT/"pyproject.toml")
    descriptor.write_text(json.dumps(value))
    with pytest.raises(ValueError,match="release_path_escape"):
        tool.resolve_service(data,"api")
    value["manifest"]=str(manifest);value["service"]="sandbox-gateway";descriptor.write_text(json.dumps(value))
    with pytest.raises(ValueError,match="service_descriptor_invalid"):
        tool.resolve_service(data,"api")


def test_selecting_api_does_not_change_another_service_descriptor(tmp_path):
    tool,data,manifest,_=built(tmp_path)
    current=data/"service-current";current.mkdir(parents=True)
    other=current/"sandbox-gateway.json";other.write_text('{"retained":true}\n')
    before=other.read_bytes();tool.select_service_release(data,manifest)
    assert other.read_bytes()==before
