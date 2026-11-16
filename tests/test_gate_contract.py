import ast
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def test_gate_covers_required_release_surfaces_and_data2_storage():
    path=ROOT/"tests/gate.py";source=path.read_text();ast.parse(source)
    for value in ("python_tests","frontend_tests","frontend_build","skill_bundle","systemd_verify",
            "PARALLEL_TEST_POSTGRES_URL","browser_*.cjs",".data","evidence/gate"):
        assert value in source
    assert "/tmp/" not in source and "/root/" not in source
    assert "terminate()" not in source
    assert "os.killpg" in source
    assert "--skip-python" in source
    assert "PLAYWRIGHT_BROWSERS_PATH" in source and "/root/.cache" not in source
    assert "--include-live-browser" in source and "deferred_live" in source
    assert "--browser-script" in source
    assert "--browser-only" in source


def test_gate_discovers_every_browser_case_instead_of_using_a_stale_list():
    source=(ROOT/"tests/gate.py").read_text()
    assert 'glob("browser_*.cjs")' in source
    assert "for script in BROWSER_SCRIPTS" in source
