from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_IDS = {
    "api", "sandbox-gateway", "job-worker", "allocation-worker", "media-worker",
    "controlled-worker", "site-worker", "sandbox-runtime", "sandbox-bootstrap",
    "sandbox-content", "sandbox-images", "controlled-runtime", "image-inference",
    "web-ui",
}


def loader():
    path = ROOT / "tools/module_manifest.py"
    assert path.is_file(), "module manifests need a validated loader"
    spec = importlib.util.spec_from_file_location("module_manifest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_all_deployment_locations_have_explicit_owners():
    manifests = loader().load_manifests(ROOT)
    assert set(manifests) == MODULE_IDS
    assert sum(value["kind"] == "service" for value in manifests.values()) == 8
    assert manifests["sandbox-gateway"]["location"] == "host"
    assert manifests["sandbox-runtime"]["location"] == "sandbox"
    assert manifests["controlled-runtime"]["location"] == "controlled"
    for value in manifests.values():
        loader().module_sources(ROOT, value)


def test_native_and_untracked_inputs_are_part_of_release_contract():
    manifests = loader().load_manifests(ROOT)
    assert "libreoffice" in {row["name"] for row in manifests["api"]["native_requirements"]}
    controlled = {row["name"] for row in manifests["controlled-worker"]["native_requirements"]}
    assert {"bwrap", "setpriv", "ffprobe", "nvidia-smi"} <= controlled
    external = {row["id"] for row in manifests["sandbox-images"]["external_inputs"]}
    assert {"asr-source-image", "codex-source-image", "claude-source-image", "drawio-vendor"} <= external
    assert manifests["sandbox-runtime"]["host_credentials_allowed"] is False
    assert manifests["controlled-runtime"]["host_credentials_allowed"] is False


@pytest.mark.parametrize("path", ["/etc/passwd", "../outside.py", "src/../../secret", "src\\escape.py"])
def test_manifest_rejects_paths_outside_release_sources(path):
    module = loader()
    manifest = dict(module.load_manifests(ROOT)["api"])
    manifest["sources"] = [path]
    with pytest.raises(ValueError, match="source_path_invalid"):
        module.validate_manifest(manifest)


def test_module_sources_refuses_symlink_escape(tmp_path):
    module = loader()
    manifest = dict(module.load_manifests(ROOT)["api"])
    target = tmp_path / "src"
    target.mkdir()
    (target / "source.py").symlink_to(ROOT / "pyproject.toml")
    manifest["sources"] = ["src/source.py"]
    manifest["source_roots"] = []
    with pytest.raises(ValueError, match="source_symlink_forbidden"):
        module.module_sources(tmp_path, manifest)


def test_duplicate_module_identity_is_rejected(tmp_path):
    module = loader()
    source = module.load_manifests(ROOT)["api"]
    for name in ("one", "two"):
        path = tmp_path / "modules" / name / "module.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(source))
    with pytest.raises(ValueError, match="duplicate_module_identity"):
        module.load_manifests(tmp_path)


def test_unknown_source_module_fails_ownership_gate(tmp_path):
    module = loader()
    path = tmp_path / "src/sandbox_harness/unowned.py"
    path.parent.mkdir(parents=True)
    path.write_text("pass\n")
    with pytest.raises(ValueError, match="unowned_source"):
        module.validate_source_ownership(tmp_path, {"modules": {}})


def test_current_source_ownership_is_complete():
    ownership = json.loads((ROOT / "modules/ownership.json").read_text())
    loader().validate_source_ownership(ROOT, ownership)


def test_every_release_module_has_local_readme_and_agent_rules():
    manifests=loader().load_manifests(ROOT)
    for module in manifests.values():
        directory=Path(module["_manifest_path"]).parent if "_manifest_path" in module else None
        if directory is None:
            candidates=list((ROOT/"modules").rglob("module.json"))
            directory=next(path.parent for path in candidates
                           if json.loads(path.read_text())["id"]==module["id"])
        for name in ("README.md","AGENTS.md"):
            path=directory/name
            assert path.is_file() and path.read_text().startswith("# "),(
                module["id"],name)
