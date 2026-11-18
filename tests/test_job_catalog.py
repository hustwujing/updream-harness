import hashlib
import json
from types import SimpleNamespace
from pathlib import Path

import pytest
from pydantic import ValidationError

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.job_catalog import JobCatalog, OperationDescriptor


ROOT = Path(__file__).resolve().parents[1]


def operation(**updates):
    value = {
        "id": "artifact.inspect", "version": "1", "digest": "a" * 64,
        "execution_mode": "inline", "timeout_seconds": 5,
        "parameter_schema": {"type": "object", "properties": {"artifact_id": {"type": "string"}}},
        "input_roles": ["source"], "output_roles": [], "required_capabilities": [],
        "network_origins": [], "resources": {"cpu": 1, "memory_mb": 128, "gpu": 0},
        "side_effects": False, "billable": False, "creates_artifacts": False,
        "retry_policy": {"max_pre_accept_attempts": 1},
        "cancellation_policy": {"supported": False},
        "reconciliation_policy": {"supported": False},
    }
    value.update(updates)
    return value


def test_inline_descriptor_rejects_durable_boundaries_and_reserved_fields():
    for update in (
        {"side_effects": True}, {"billable": True}, {"creates_artifacts": True},
        {"timeout_seconds": 31}, {"resources": {"cpu": 1, "memory_mb": 128, "gpu": 1}},
    ):
        with pytest.raises(ValidationError, match="inline"):
            OperationDescriptor.model_validate(operation(**update))
    with pytest.raises(ValidationError, match="reserved infrastructure field"):
        OperationDescriptor.model_validate(operation(parameter_schema={
            "type": "object", "properties": {"node_id": {"type": "string"}},
        }))


def test_catalog_verifies_digest_and_exact_descriptor_identity():
    payload = {"schema_version": 1, "operations": [operation()], "capabilities": []}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode()).hexdigest()
    catalog = JobCatalog.from_json(raw, expected_digest=digest)
    assert catalog.operation("artifact.inspect", "1", "a" * 64).execution_mode == "inline"
    with pytest.raises(ValueError, match="catalog digest"):
        JobCatalog.from_json(raw, expected_digest="b" * 64)
    with pytest.raises(KeyError, match="Unknown Operation Descriptor"):
        catalog.operation("artifact.inspect", "2", "a" * 64)


def test_durable_descriptor_requires_explicit_recovery_policies():
    with pytest.raises(ValidationError, match="durable"):
        OperationDescriptor.model_validate(operation(
            id="video.render", execution_mode="durable", creates_artifacts=True,
            retry_policy={}, cancellation_policy={}, reconciliation_policy={},
        ))
    descriptor = OperationDescriptor.model_validate(operation(
        id="video.render", execution_mode="durable", timeout_seconds=1800,
        creates_artifacts=True, output_roles=["video"],
        retry_policy={"max_pre_accept_attempts": 2},
        cancellation_policy={"supported": True},
        reconciliation_policy={"supported": True},
    ))
    assert descriptor.execution_mode == "durable"


def test_review_policy_is_structured_and_default_off():
    assert OperationDescriptor.model_validate(operation()).review_policy=='disabled'
    assert OperationDescriptor.model_validate(operation(review_policy='required')).review_policy=='required'
    with pytest.raises(ValidationError):
        OperationDescriptor.model_validate(operation(review_policy='infer_from_prompt'))


def test_example_job_catalog_is_parseable_and_digest_verified():
    raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    value=JobCatalog.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest())
    assert {row.id for row in value.payload.operations}=={
        'artifact.inspect','media.render','audio.process','media.concat_sequence',
        'image.segment','audio.transcribe'}
    assert {row.id for row in value.payload.capabilities}=={'image.generate'}
    audio=value.operation('audio.process','1','d'*64)
    recipe=audio.parameter_schema['properties']['recipe']
    assert audio.runtime_profile_id=='media-cpu' and set(audio.output_roles)=={
        'processed_audio','measurement_report'}
    assert recipe['properties']['target_lufs']['minimum']==-40
    public=HarnessCatalog(
        job_catalog=value,profile_resolver=SimpleNamespace(skills={},children={})).public_execution_metadata()
    exposed=next(row for row in public['operations'] if row['id']=='audio.process')
    assert exposed['parameter_schema']==audio.parameter_schema
    concat=value.operation('media.concat_sequence','1','e'*64)
    assert concat.input_roles==('source','music') and concat.output_roles==(
        'rendered_media','compiler_report')
    assert 'original registered source video' in concat.guidance
    exposed_concat=next(row for row in public['operations'] if row['id']=='media.concat_sequence')
    assert exposed_concat['guidance']==concat.guidance
    render=value.operation('media.render','1','b'*64)
    assert 'contain semantics' in render.guidance and 'transparent' in render.guidance
    segment=value.operation('image.segment','1','1'*64)
    assert segment.input_roles==('source',) and set(segment.output_roles)=={
        'foreground','mask','segmentation_report'}
    assert segment.resources['gpu']==1 and segment.creates_artifacts
    assert 'SAM3' in segment.guidance and 'box' in segment.guidance and 'points' in segment.guidance
    transcribe=value.operation('audio.transcribe','1','2'*64)
    assert transcribe.input_roles==('source',) and set(transcribe.output_roles)=={
        'transcript_json','transcript_text','transcript_srt'}
    assert transcribe.resources['gpu']==0 and 'faster-whisper' in transcribe.guidance


def test_public_child_profiles_expose_frozen_harness_job_template():
    raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    value=JobCatalog.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest())
    child=SimpleNamespace(id='child-general',supported_harnesses=('codex','claude'),
        max_delegation_depth=1)
    public=HarnessCatalog(job_catalog=value,profile_resolver=SimpleNamespace(
        skills={},children={'child-general':child})).public_execution_metadata()
    profile=public['child_execution_profiles'][0];template=profile['job_template']
    assert template=={'kind':'harness','adapter':'child-harness',
        'descriptor_id':'delegate.harness','descriptor_version':'1','descriptor_digest':'d'*64,
        'child_profile_id':'child-general'}
    assert profile['parameter_schema']['properties']['harness']['enum']==['codex','claude']
