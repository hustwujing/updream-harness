import hashlib
import json
from pathlib import Path

from sandbox_harness.job_catalog import JobCatalog
from sandbox_harness.media_catalog import MediaCatalog


ROOT=Path(__file__).resolve().parents[1]


def coverage_cases():
    catalog=json.loads((ROOT/'tests/real_cases/cases.json').read_text())
    return [row for row in catalog['cases'] if row['id'].startswith('K')]


def test_complex_coverage_cases_exactly_cover_every_signed_skill_and_operation():
    cases=coverage_cases()
    assert [row['id'] for row in cases]==[f'K{index:02d}' for index in range(1,12)]
    covered_skills={value for row in cases for value in row['coverage']['skills']}
    covered_operations={value for row in cases for value in row['coverage']['operations']}
    manifest=json.loads((ROOT/'runtime/skills/manifest.json').read_text())
    raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    job_catalog=JobCatalog.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest())
    assert covered_skills=={row['id'] for row in manifest['skills']}
    assert covered_operations=={row.id for row in job_catalog.payload.operations}
    assert all(row['executor']=='parallel_protocol' for row in cases)


def test_complex_coverage_cases_cover_every_provider_adapter_mode_and_capability():
    cases=coverage_cases();declared=[value for row in cases for value in row['coverage']['providers']]
    covered_adapters={row['adapter'] for row in declared}
    covered_modes={(row['adapter'],mode) for row in declared for mode in row['modes']}
    covered_capabilities={row['capability'] for row in declared}
    models=MediaCatalog().public()
    expected_adapters={row['adapter'] for row in models}
    expected_modes={(row['adapter'],mode) for row in models
        for mode in row['parameter_schema']['generation_modes']}
    raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    job_catalog=JobCatalog.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest())
    assert covered_adapters==expected_adapters
    assert covered_modes==expected_modes
    assert covered_capabilities=={row.id for row in job_catalog.payload.capabilities}
    assert {row['coverage']['tier'] for row in cases}=={'required','expensive'}
    assert {row['id'] for row in cases if row['coverage']['tier']=='expensive'}=={'K10','K11'}
