import hashlib
import json
from pathlib import Path
import subprocess
import sys


ROOT=Path(__file__).resolve().parents[1]


def test_real_case_catalog_is_reproducible_complete_and_parallel_specific():
    subprocess.run([sys.executable,str(ROOT/"scripts/build_real_case_catalog.py")],check=True)
    catalog=json.loads((ROOT/"tests/real_cases/cases.json").read_text())
    source=(ROOT/"tests/real_cases/source-plus-cases.json").read_bytes()
    assert catalog["source_snapshot"]["sha256"]==hashlib.sha256(source).hexdigest()
    assert catalog["source_snapshot"]["source_case_count"]==129
    assert catalog["business_case_count"]==78
    assert catalog["parallel_case_count"]>=20 and catalog["runnable_case_count"]>=98
    assert len(catalog["cases"])==catalog["runnable_case_count"]
    assert len({row["id"] for row in catalog["cases"]})==len(catalog["cases"])
    assert {row["id"] for row in catalog["superseded_cases"]}=={
        f"D{value:02d}" for value in range(1,52)}
    assert {row["id"] for row in catalog["cases"] if row["id"].startswith("P")}=={
        f"P{value:02d}" for value in range(1,21)}


def test_every_case_asset_is_manifested_and_available():
    catalog=json.loads((ROOT/"tests/real_cases/cases.json").read_text())
    manifest=json.loads((ROOT/"tests/real_cases/assets/manifest.json").read_text())
    entries=manifest.get("files",manifest.get("assets",manifest))
    names=set(entries if isinstance(entries,dict) else (row["name"] for row in entries))
    referenced={name for case in catalog["cases"] for name in case.get("assets",[])}
    assert referenced<=names
    assert all((ROOT/"tests/real_cases/assets"/name).is_file() for name in referenced)
