import pytest

from sandbox_runtime.app import safe_relative


def test_runtime_artifact_paths_remain_relative():
    assert safe_relative("folder/result.mp4").as_posix() == "folder/result.mp4"
    for value in ("/etc/passwd", "../secret", "folder/../secret", ""):
        with pytest.raises(ValueError):
            safe_relative(value)

