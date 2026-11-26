from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def builder():
    path = ROOT / "tools/build_module.py"
    assert path.is_file(), "independent modules require a closure-aware artifact builder"
    spec = importlib.util.spec_from_file_location("module_build", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def fixture(tmp_path, *, invalid_dependency=False):
    root = tmp_path / "project"
    files = {
        "src/sandbox_harness/__init__.py": "",
        "src/sandbox_harness/services/__init__.py": "",
        "src/sandbox_harness/services/api/__init__.py": "",
        "src/sandbox_harness/services/api/main.py": (
            "from sandbox_harness.services.media_worker.main import run\n" if invalid_dependency
            else "from sandbox_harness.common.contracts.value import VALUE\ndef run(): return VALUE\n"
        ),
        "src/sandbox_harness/services/media_worker/__init__.py": "",
        "src/sandbox_harness/services/media_worker/main.py": "def run(): return 'other-service'\n",
        "src/sandbox_harness/common/__init__.py": "",
        "src/sandbox_harness/common/contracts/__init__.py": "",
        "src/sandbox_harness/common/contracts/value.py": "VALUE = 7\n",
        "assets/catalog.json": "{}\n",
    }
    for name, value in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    manifest = json.loads((ROOT / "modules/host/api/module.json").read_text())
    manifest.update(sources=["src/sandbox_harness/services/api/main.py"], source_roots=[],
                    resources=["assets/catalog.json"], python_dependencies=[], native_requirements=[],
                    external_inputs=[], entry_points={"api": "sandbox_harness.services.api.main:run"})
    path = root / "modules/host/api/module.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(manifest))
    return root


def test_artifact_contains_own_service_and_transitive_common_only(tmp_path):
    root = fixture(tmp_path)
    manifest = builder().build_module(root, "api", tmp_path / "artifacts")
    value = json.loads(manifest.read_text())
    files = {row["path"] for row in value["files"]}
    assert "src/sandbox_harness/services/api/main.py" in files
    assert "src/sandbox_harness/common/contracts/value.py" in files
    assert "src/sandbox_harness/common/contracts/__init__.py" in files
    assert "resources/assets/catalog.json" in files
    assert not any("media_worker" in name for name in files)
    builder().verify_artifact(manifest)


def test_artifact_identity_is_deterministic_and_common_change_invalidates_it(tmp_path):
    root = fixture(tmp_path)
    first = builder().build_module(root, "api", tmp_path / "artifacts")
    assert builder().build_module(root, "api", tmp_path / "artifacts") == first
    (root / "src/sandbox_harness/common/contracts/value.py").write_text("VALUE = 8\n")
    assert builder().build_module(root, "api", tmp_path / "artifacts") != first
    builder().verify_artifact(first)


def test_artifact_rejects_another_service_implementation(tmp_path):
    root = fixture(tmp_path, invalid_dependency=True)
    with pytest.raises(ValueError, match="cross_service_import"):
        builder().build_module(root, "api", tmp_path / "artifacts")


def test_artifact_tampering_is_detected_before_launch(tmp_path):
    root = fixture(tmp_path)
    manifest = builder().build_module(root, "api", tmp_path / "artifacts")
    path = manifest.parent / "src/sandbox_harness/services/api/main.py"
    path.chmod(0o644)
    path.write_text("raise RuntimeError('tampered')\n")
    with pytest.raises(ValueError, match="artifact_file_digest_mismatch"):
        builder().verify_artifact(manifest)


def test_release_resources_cannot_include_an_external_symlink(tmp_path):
    root = fixture(tmp_path)
    (root / "assets/catalog.json").unlink()
    (root / "assets/catalog.json").symlink_to(ROOT / "pyproject.toml")
    with pytest.raises(ValueError, match="source_symlink_forbidden"):
        builder().build_module(root, "api", tmp_path / "artifacts")


def test_artifact_rejects_unlisted_symlink_directories(tmp_path):
    root = fixture(tmp_path)
    manifest = builder().build_module(root, "api", tmp_path / "artifacts")
    manifest.parent.chmod(0o755)
    (manifest.parent / "hidden-code").symlink_to(root / "src", target_is_directory=True)
    with pytest.raises(ValueError, match="source_symlink_forbidden"):
        builder().verify_artifact(manifest)
