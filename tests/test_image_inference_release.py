import importlib.util
from pathlib import Path

import pytest


ROOT=Path(__file__).resolve().parents[1]


def tool():
    path=ROOT/"tools/image_inference_release.py";spec=importlib.util.spec_from_file_location("image_release",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def fixture(tmp_path):
    root=tmp_path/"service";(root/"service").mkdir(parents=True);(root/"service/app.py").write_text("APP=1\n")
    (root/"media_core").mkdir();(root/"media_core/app.py").write_text("CORE=1\n")
    for name in tool().MODEL_PATHS:
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(name.encode())
    python=tmp_path/"env/bin/python";python.parent.mkdir(parents=True);python.write_bytes(b"python");python.chmod(0o755)
    (tmp_path/"env/conda-meta").mkdir();(tmp_path/"env/conda-meta/python.json").write_text("{}")
    return root,python


def test_external_image_release_build_resolve_and_tamper(tmp_path):
    module=tool();root,python=fixture(tmp_path);data=tmp_path/"data"
    manifest=module.build(root,python,data);selected=module.resolve(data,full=True)
    assert selected["digest"]==manifest.parent.name and selected["service_root"]==str(root.resolve())
    (root/"service/app.py").write_text("APP=2\n")
    with pytest.raises(ValueError,match="image_inference_code_changed"):
        module.resolve(data)


def test_external_image_release_tracks_private_dependency_overlay(tmp_path):
    module=tool();root,python=fixture(tmp_path);data=tmp_path/"data"
    dependency=root/"python-packages/itsdangerous/__init__.py"
    dependency.parent.mkdir(parents=True);dependency.write_text("VERSION='2.2.0'\n")
    manifest=module.build(root,python,data)
    value=__import__("json").loads(manifest.read_text())
    assert any(row["path"]=="python-packages/itsdangerous/__init__.py" for row in value["code"])
    dependency.write_text("VERSION='tampered'\n")
    with pytest.raises(ValueError,match="image_inference_code_changed"):
        module.resolve(data)
