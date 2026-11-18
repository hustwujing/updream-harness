import hashlib
import json
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]
CATALOG=ROOT/'tests/example_library/cases.json'


def test_distributed_example_library_is_exactly_the_frozen_44_source_examples():
    value=json.loads(CATALOG.read_text());source=Path(value['source_catalog'])
    assert value['family']=='distributed-agent-system-example-library-v1'
    assert value['runnable_case_count']==44 and len(value['cases'])==44
    assert [row['id'] for row in value['cases']]==[f'E{index:02d}' for index in range(1,45)]
    assert value['source_catalog_sha256']==hashlib.sha256(source.read_bytes()).hexdigest()
    originals=json.loads(source.read_text())
    assert [row['source_demo_id'] for row in value['cases']]==[row['id'] for row in originals]


def test_distributed_example_assets_and_provenance_are_resolvable():
    value=json.loads(CATALOG.read_text());assets=Path(value['asset_root'])
    assert assets.is_dir()
    for row in value['cases']:
        assert row['family']==value['family'] and row['executor']=='primary_harness'
        assert row['provenance']=='distributed-agent-system:'+row['source_demo_id']
        assert row['prompt'].startswith('已确认本次执行范围') and row['source_prompt']
        assert all((assets/name).is_file() for name in row['assets'])


def test_delivery_contract_comes_from_declared_options_not_internal_demo_files():
    value=json.loads(CATALOG.read_text());by_source={row['source_demo_id']:row for row in value['cases']}
    subtitles=by_source['subtitles']
    assert subtitles['expect']['kind']=='video' and subtitles['expect']['extra_extension']=='.srt'
    assert 'video/mp4' in subtitles['prompt'] and '.srt' in subtitles['prompt']
    assert 'extra_extension' not in by_source['cuts']['expect']
    assert 'extra_extension' not in by_source['grade']['expect']
