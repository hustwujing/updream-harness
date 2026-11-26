from pathlib import Path

import pytest

from sandbox_harness.common.configuration import resources


def test_explicit_release_resources_ignore_cwd(tmp_path, monkeypatch):
    release = tmp_path / "immutable"
    release.mkdir()
    (release / "catalog.json").write_text("{}")
    elsewhere = tmp_path / "cwd"
    elsewhere.mkdir()
    monkeypatch.setenv("SANDBOX_HARNESS_RESOURCE_ROOT", str(release))
    monkeypatch.chdir(elsewhere)
    assert resources.resource_path("catalog.json").read_text() == "{}"


def test_installed_package_cannot_fall_back_to_parent_checkout(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='unrelated'\n")
    monkeypatch.delenv("SANDBOX_HARNESS_RESOURCE_ROOT", raising=False)
    monkeypatch.setattr(resources, "__file__", str(tmp_path / ".data/release/site-packages/sandbox_harness/common/configuration/resources.py"))
    with pytest.raises(ValueError, match="release_resource_root_required"):
        resources.resource_root()


@pytest.mark.parametrize("relative", ["../outside", "/etc/passwd", "a\\outside"])
def test_resource_paths_cannot_escape_root(tmp_path, monkeypatch, relative):
    monkeypatch.setenv("SANDBOX_HARNESS_RESOURCE_ROOT", str(tmp_path))
    with pytest.raises(ValueError, match="release_resource_path_invalid"):
        resources.resource_path(relative)


def test_resource_symlink_cannot_escape_root(tmp_path, monkeypatch):
    release = tmp_path / "release"
    release.mkdir()
    (release / "escape").symlink_to(tmp_path)
    monkeypatch.setenv("SANDBOX_HARNESS_RESOURCE_ROOT", str(release))
    with pytest.raises(ValueError, match="release_resource_path_invalid"):
        resources.resource_path("escape")


def test_controlled_programs_resolve_from_pinned_program_release(tmp_path, monkeypatch):
    (tmp_path / "sam3_job.py").write_text("pass\n")
    monkeypatch.setenv("SANDBOX_HARNESS_CONTROLLED_PROGRAM_ROOT", str(tmp_path))
    assert resources.controlled_program_path("sam3_job.py") == tmp_path / "sam3_job.py"
    with pytest.raises(ValueError, match="controlled_program_unknown"):
        resources.controlled_program_path("arbitrary.py")
    with pytest.raises(ValueError, match="controlled_program_unavailable"):
        resources.controlled_program_path("transcribe_job.py")
