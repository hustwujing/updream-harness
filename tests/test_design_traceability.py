import json
from pathlib import Path
import subprocess
import sys


ROOT=Path(__file__).resolve().parents[1]


def test_design_report_has_no_unmapped_section_or_missing_path():
    subprocess.run([sys.executable,str(ROOT/"scripts/render_design_report.py")],check=True)
    trace=json.loads((ROOT/"docs/design-traceability.yaml").read_text())
    report=(ROOT/"docs/reports/initial-implementation-report.md").read_text()
    assert len(trace["requirements"])>=25
    assert "Mapped sections: **" in report and "Unmapped" not in report
    assert "Pending live evidence" in report
