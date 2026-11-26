import pytest
from sqlalchemy import func, select

from sandbox_harness.common.db.domain_database import DomainDatabase, DomainOutbox


LIFECYCLE = {
    "operation_id": "operation",
    "owner_id": "owner",
    "thread_id": "thread",
    "generation": 3,
    "action": "archive",
    "target_state": "archived",
    "owner_generation": 2,
}


def test_production_command_manifest_is_complete_and_stable():
    from sandbox_harness.common.contracts.domain_commands import command_manifest

    manifest = command_manifest()

    assert set(manifest) == {
        "lifecycle.prepare", "lifecycle.commit", "lifecycle.abort",
        "lifecycle.request", "lifecycle.result",
        "usage.record",
        "media.source.authorize", "media.source.cancel",
        "media.turn.draft", "media.turn.seal", "media.turn.finished",
        "media.promotion.ack", "site.turn.draft",
    }
    assert all(value["version"] == 1 for value in manifest.values())
    assert all(len(value["digest"]) == 64 for value in manifest.values())
    assert all(value["source"] != value["target"] for value in manifest.values())


def test_command_payload_and_receipt_are_exact_typed_contracts():
    from sandbox_harness.common.contracts.domain_commands import (
        validate_command_payload,
        validate_command_result,
    )

    contract = validate_command_payload(
        "lifecycle.prepare", "core", "media", LIFECYCLE)
    assert contract.version == 1 and len(contract.digest) == 64
    validate_command_result("lifecycle.prepare", {"prepared": True, "error_code": ""})

    with pytest.raises(ValueError, match="domain_command_payload_invalid"):
        validate_command_payload(
            "lifecycle.prepare", "core", "media", {**LIFECYCLE, "unexpected": True})
    with pytest.raises(ValueError, match="domain_command_payload_invalid"):
        validate_command_payload(
            "lifecycle.prepare", "core", "media", {**LIFECYCLE, "generation": "3"})
    with pytest.raises(ValueError, match="domain_command_result_invalid"):
        validate_command_result("lifecycle.prepare", {"prepared": "yes", "error_code": ""})


def test_media_source_authorize_accepts_provider_without_generation_mode():
    from sandbox_harness.common.contracts.domain_commands import validate_command_payload

    contract = validate_command_payload("media.source.authorize", "core", "media", {
        "dispatch_id": "d" * 64,
        "media_job_id": "m" * 32,
        "source_job_id": "j" * 32,
        "source_generation": 1,
        "request_digest": "a" * 64,
        "owner_id": "owner",
        "thread_id": "t" * 32,
        "kind": "image",
        "model": "qwen-image-layered",
        "generation_mode": "",
        "provider": "local-qwen-layered",
        "adapter": "image_layered_http",
        "execution_mode": "async",
        "cancellation_mode": "supported",
        "parameters": {},
        "references": [],
        "output_contracts": [{"role": "layer_package"}],
    })
    assert contract.kind == "media.source.authorize"


async def test_invalid_command_fails_before_outbox_insert(tmp_path):
    from sandbox_harness.common.db.handoff import DomainHandoff

    core = DomainDatabase("core", "sqlite+aiosqlite:///" + str(tmp_path / "core.db"))
    await core.initialize()
    sender = DomainHandoff(core, {})
    try:
        with pytest.raises(ValueError, match="domain_command_payload_invalid"):
            async with core.transaction() as session:
                await sender.enqueue(
                    session,
                    command_id="invalid-command",
                    target="media",
                    kind="lifecycle.prepare",
                    aggregate_id="aggregate",
                    owner_id="owner",
                    generation=3,
                    payload={**LIFECYCLE, "generation": "3"},
                )
        async with core.connections() as session:
            assert await session.scalar(select(func.count()).select_from(DomainOutbox)) == 0
    finally:
        await core.close()


async def test_envelope_binds_payload_contract_identity(tmp_path):
    from dataclasses import replace

    from sandbox_harness.common.db.handoff import DomainHandoff

    core = DomainDatabase("core", "sqlite+aiosqlite:///" + str(tmp_path / "core.db"))
    media = DomainDatabase("media", "sqlite+aiosqlite:///" + str(tmp_path / "media.db"))
    await core.initialize()
    await media.initialize()

    async def prepared(session, envelope):
        return {"prepared": True, "error_code": ""}

    sender = DomainHandoff(core, {})
    receiver = DomainHandoff(media, {"lifecycle.prepare": prepared})
    try:
        async with core.transaction() as session:
            envelope = await sender.enqueue(
                session,
                command_id="contract-command",
                target="media",
                kind="lifecycle.prepare",
                aggregate_id="aggregate",
                owner_id="owner",
                generation=3,
                payload=LIFECYCLE,
            )
        assert envelope.contract_version == 1
        assert len(envelope.contract_digest) == 64
        with pytest.raises(ValueError, match="domain_command_contract_mismatch"):
            await receiver.receive(replace(envelope, contract_digest="0" * 64))
        assert (await receiver.receive(envelope))["result"]["prepared"] is True
    finally:
        await core.close()
        await media.close()
