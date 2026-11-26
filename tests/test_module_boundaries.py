from __future__ import annotations

import ast
import importlib
import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("module", [
    "sandbox_harness.services.api.main",
    "sandbox_harness.services.sandbox_gateway.gateway",
    "sandbox_harness.services.job_worker.job_service",
    "sandbox_harness.services.allocation_worker.allocation_service",
    "sandbox_harness.services.media_worker.media_service",
    "sandbox_harness.services.controlled_worker.controlled_service",
    "sandbox_harness.services.site_worker.site_service",
])
def test_service_entry_points_import_from_owned_packages(module):
    value = importlib.import_module(module)
    assert Path(value.__file__).is_relative_to(ROOT / "src/sandbox_harness/services")


def test_host_source_migration_is_complete():
    remaining = sorted(path.name for path in (ROOT / "src/sandbox_harness").glob("*.py"))
    assert remaining == ["__init__.py"]
    for row in json.loads((ROOT / "modules/ownership.json").read_text())["modules"].values():
        assert (ROOT / row["target"]).is_file()


def test_controlled_programs_do_not_import_host_service_code():
    paths = [ROOT / "src/controlled_runtime/sam3_job.py", ROOT / "src/controlled_runtime/transcribe_job.py"]
    for path in paths:
        assert path.is_file()
        tree = ast.parse(path.read_text())
        imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        assert not any(value and value.startswith("sandbox_harness") for value in imports)


def boundary_checker():
    path = ROOT / "tools/module_boundaries.py"
    assert path.is_file()
    spec = importlib.util.spec_from_file_location("module_boundaries", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_no_cross_service_or_common_to_service_imports():
    assert boundary_checker().violations(ROOT) == []


def test_boundary_check_detects_function_local_cross_service_import(tmp_path):
    path = tmp_path / "src/sandbox_harness/services/api/example.py"
    path.parent.mkdir(parents=True)
    path.write_text("def later():\n    from sandbox_harness.services.media_worker import worker\n")
    violations = boundary_checker().violations(tmp_path)
    assert len(violations) == 1
    assert violations[0]["rule"] == "cross_service_import"


def test_boundary_check_detects_common_importing_service(tmp_path):
    path = tmp_path / "src/sandbox_harness/common/contracts/example.py"
    path.parent.mkdir(parents=True)
    path.write_text("import sandbox_harness.services.api.app\n")
    assert boundary_checker().violations(tmp_path)[0]["rule"] == "common_imports_service"
