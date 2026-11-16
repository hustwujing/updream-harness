import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

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


def test_example_job_catalog_is_parseable_and_digest_verified():
    raw=(ROOT/'deploy/job-catalog.example.json').read_text()
    value=JobCatalog.from_json(raw,expected_digest=hashlib.sha256(raw.encode()).hexdigest())
    assert {row.id for row in value.payload.operations}=={'artifact.inspect','media.render'}
    assert {row.id for row in value.payload.capabilities}=={'image.generate'}
