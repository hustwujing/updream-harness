from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "runtime/apply_patch.py"


def run_patch(cwd: Path, patch: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(HELPER)], cwd=cwd, input=patch,
        text=True, capture_output=True)


def test_apply_patch_adds_updates_moves_and_deletes_workspace_files(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    added = work / "generated.py"
    result = run_patch(tmp_path, f"""*** Begin Patch
*** Add File: {added}
+print('first')
+print('second')
*** End Patch
""")
    assert result.returncode == 0 and added.read_text() == "print('first')\nprint('second')\n"

    result = run_patch(tmp_path, """*** Begin Patch
*** Update File: work/generated.py
*** Move to: work/final.py
@@
-print('first')
+print('updated')
 print('second')
*** End Patch
""")
    final = work / "final.py"
    assert result.returncode == 0 and not added.exists()
    assert final.read_text() == "print('updated')\nprint('second')\n"

    result = run_patch(tmp_path, """*** Begin Patch
*** Delete File: work/final.py
*** End Patch
""")
    assert result.returncode == 0 and not final.exists()


def test_apply_patch_rejects_escape_symlink_and_partial_changes(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("unchanged\n")
    outside = tmp_path.parent / "outside-apply-patch.txt"
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    result = run_patch(tmp_path, f"""*** Begin Patch
*** Update File: target.txt
@@
-unchanged
+changed
*** Add File: {outside}
+escape
*** End Patch
""")
    assert result.returncode == 1
    assert target.read_text() == "unchanged\n"
    assert not outside.exists()

    result = run_patch(tmp_path, """*** Begin Patch
*** Update File: link.txt
@@
-unchanged
+changed
*** End Patch
""")
    assert result.returncode == 1
    assert target.read_text() == "unchanged\n"
