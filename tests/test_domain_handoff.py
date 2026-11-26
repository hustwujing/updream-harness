from __future__ import annotations

from dataclasses import asdict, replace
import json
import logging

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select

from sandbox_harness.common.contracts.domain_commands import DomainCommandContract
from sandbox_harness.common.db.domain_database import DomainDatabase, DomainInbox, DomainOutbox
from sandbox_harness.common.db.models import SiteProjectRow
from sandbox_harness.common.db.models import canonical_digest
from sandbox_harness.common.contracts.domain import now
import sandbox_harness.common.observability.observability as obs


def telemetry_rows(value):
    assert value.flush(5)
    return [json.loads(line) for path in value.sink.root.glob("*.telemetry.jsonl")
        for line in path.read_text().splitlines()]


async def setup_pair(tmp_path):
    from sandbox_harness.common.db.handoff import DomainHandoff
    core = DomainDatabase("core", "sqlite+aiosqlite:///" + str(tmp_path / "core.db"))
    sites = DomainDatabase("sites", "sqlite+aiosqlite:///" + str(tmp_path / "sites.db"))
    await core.initialize()
    await sites.initialize()

    async def create_site(session, envelope):
        session.add(SiteProjectRow(id=envelope.aggregate_id, owner_id=envelope.owner_id,
            thread_id="thread", slug=envelope.aggregate_id, name="site", current_version_id="",
            created_at=now(), updated_at=now()))
        await session.flush()
        return {"project_id": envelope.aggregate_id}

    class SiteCommand(BaseModel):
        model_config = ConfigDict(extra="forbid", strict=True)
        title: str = ""

    class SiteCreated(BaseModel):
        model_config = ConfigDict(extra="forbid", strict=True)
        project_id: str

    class SiteRenamed(BaseModel):
        model_config = ConfigDict(extra="forbid", strict=True)
        renamed: bool

    contracts = {
        "create_site": DomainCommandContract(
            "create_site", "core", ("sites",), SiteCommand, SiteCreated),
        "rename": DomainCommandContract(
            "rename", "core", ("sites",), SiteCommand, SiteRenamed),
    }
    return (core, sites, DomainHandoff(core, {}, contracts=contracts),
            DomainHandoff(sites, {"create_site": create_site}, contracts=contracts))


async def enqueue(core, sender, command="command-1", aggregate="project"):
    async with core.transaction() as session:
        return await sender.enqueue(session, command_id=command, target="sites", kind="create_site",
            aggregate_id=aggregate, owner_id="owner", generation=1, payload={"title": "site"})


async def test_lost_reply_and_process_restart_deduplicate_committed_effect(tmp_path):
    from sandbox_harness.common.db.handoff import DomainHandoff
    core, sites, sender, receiver = await setup_pair(tmp_path)
    try:
        envelope = await enqueue(core, sender)
        receipt = await receiver.receive(envelope)
        # Process died after target commit, before acknowledgement in core.
        receiver = DomainHandoff(sites, receiver.handlers, contracts=receiver.contracts)
        sender = DomainHandoff(core, {}, contracts=sender.contracts)
        assert await receiver.receive(envelope) == receipt
        assert (await sender.relay({"sites": receiver})).delivered == 1
        assert (await sender.relay({"sites": receiver})).delivered == 0
        async with sites.transaction() as session:
            assert await session.scalar(select(func.count()).select_from(SiteProjectRow)) == 1
            assert await session.scalar(select(func.count()).select_from(DomainInbox)) == 1
        async with core.transaction() as session:
            row = await session.get(DomainOutbox, envelope.command_id)
            assert row.state == "acknowledged"
    finally:
        await core.close()
        await sites.close()


async def test_persisted_envelope_links_receive_after_restart(tmp_path,monkeypatch):
    from sandbox_harness.common.db.handoff import DomainHandoff, Envelope
    core, sites, sender, receiver = await setup_pair(tmp_path)
    telemetry=obs.Telemetry("handoff-test",tmp_path/"logs")
    monkeypatch.setattr(obs,"_PROVIDER",telemetry.provider)
    monkeypatch.setattr(obs,"_TELEMETRY",telemetry)
    root=logging.getLogger();old_level=root.level;root.setLevel(logging.INFO);root.addHandler(telemetry.handler)
    try:
        with obs.operation("test.handoff.producer",new_trace=True):
            expected=obs.carrier()["traceparent"]
            envelope=await enqueue(core,sender)
        async with core.connections() as session:
            stored=await session.get(DomainOutbox,envelope.command_id)
            restored=Envelope(**json.loads(stored.envelope_json))
        assert restored.traceparent==expected
        assert restored.digest==stored.digest==envelope.digest
        legacy=asdict(restored);legacy.pop("traceparent")
        assert replace(restored,traceparent="").digest==canonical_digest(legacy)

        # A retry may run in another trace but must retain the first durable context.
        with obs.operation("test.handoff.retry",new_trace=True):
            assert await enqueue(core,sender)==envelope

        receiver=DomainHandoff(sites,receiver.handlers,contracts=receiver.contracts)
        await receiver.receive(restored)
        receive=next(row for row in telemetry_rows(telemetry)
            if row["signal"]=="span" and row["event_name"]=="domain.handoff.receive")
        assert receive["trace_id"]!=expected.split("-")[1]
        assert receive["links"]==[{
            "trace_id":expected.split("-")[1],"span_id":expected.split("-")[2]}]
        with pytest.raises(ValueError,match="handoff_traceparent_invalid"):
            replace(restored,traceparent="invalid").validate()
    finally:
        root.removeHandler(telemetry.handler);root.setLevel(old_level);telemetry.close()
        await core.close();await sites.close()


async def test_source_rollback_does_not_leak_command(tmp_path):
    core, sites, sender, receiver = await setup_pair(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            async with core.transaction() as session:
                await sender.enqueue(session, command_id="rollback", target="sites", kind="create_site",
                    aggregate_id="project", owner_id="owner", generation=1, payload={})
                raise RuntimeError("injected")
        assert (await sender.relay({"sites": receiver})).delivered == 0
    finally:
        await core.close()
        await sites.close()


async def test_target_failure_rolls_back_effect_and_receipt(tmp_path):
    core, sites, sender, receiver = await setup_pair(tmp_path)
    try:
        envelope = await enqueue(core, sender)
        good = receiver.handlers["create_site"]

        async def failed(session, envelope):
            await good(session, envelope)
            raise RuntimeError("injected")

        receiver.handlers["create_site"] = failed
        with pytest.raises(RuntimeError, match="injected"):
            await receiver.receive(envelope)
        async with sites.transaction() as session:
            assert await session.scalar(select(func.count()).select_from(SiteProjectRow)) == 0
            assert await session.scalar(select(func.count()).select_from(DomainInbox)) == 0
        receiver.handlers["create_site"] = good
        assert (await sender.relay({"sites": receiver})).delivered == 1
    finally:
        await core.close()
        await sites.close()


async def test_conflicting_retry_and_wrong_target_are_rejected(tmp_path):
    core, sites, sender, receiver = await setup_pair(tmp_path)
    try:
        envelope = await enqueue(core, sender)
        assert await enqueue(core, sender) == envelope
        await receiver.receive(envelope)
        with pytest.raises(ValueError, match="handoff_identity_conflict"):
            await enqueue(core, sender, aggregate="different")
        with pytest.raises(ValueError, match="handoff_identity_conflict"):
            await receiver.receive(replace(envelope, owner_id="different"))
        with pytest.raises(ValueError, match="handoff_target_mismatch"):
            await sender.receive(envelope)
        with pytest.raises(ValueError, match="handoff_receipt_mismatch"):
            await sender.acknowledge(envelope.command_id, {"digest": "wrong"})
    finally:
        await core.close()
        await sites.close()


async def test_stream_order_survives_reordered_delivery(tmp_path):
    core, sites, sender, receiver = await setup_pair(tmp_path)
    try:
        first = await enqueue(core, sender)
        async with core.transaction() as session:
            second = await sender.enqueue(session, command_id="second", target="sites", kind="rename",
                aggregate_id="project", owner_id="owner", generation=1, payload={"title": "second"})

        async def rename(session, envelope):
            row = await session.get(SiteProjectRow, envelope.aggregate_id)
            row.name = envelope.payload["title"]
            return {"renamed": True}

        receiver.handlers["rename"] = rename
        with pytest.raises(ValueError, match="handoff_sequence_gap"):
            await receiver.receive(second)
        await receiver.receive(first)
        await receiver.receive(second)
        await receiver.receive(first)
        async with sites.transaction() as session:
            assert (await session.get(SiteProjectRow, "project" )).name == "second"
    finally:
        await core.close()
        await sites.close()


async def test_failed_stream_does_not_block_other_work(tmp_path):
    core, sites, sender, receiver = await setup_pair(tmp_path)
    try:
        await enqueue(core, sender, command="bad", aggregate="bad-project")
        await enqueue(core, sender, command="good", aggregate="good-project")
        original = receiver.handlers["create_site"]

        async def handler(session, envelope):
            if envelope.aggregate_id == "bad-project":
                raise ConnectionError("injected")
            return await original(session, envelope)

        receiver.handlers["create_site"] = handler
        result = await sender.relay({"sites": receiver})
        assert result.delivered == 1 and len(result.failures) == 1
        async with sites.transaction() as session:
            assert await session.get(SiteProjectRow, "good-project") is not None
            assert await session.get(SiteProjectRow, "bad-project") is None
        # Backoff on the failed stream also prevents starving newer work.
        await enqueue(core, sender, command="good-next", aggregate="next-project")
        assert (await sender.relay({"sites": receiver}, limit=1)).delivered == 1
    finally:
        await core.close()
        await sites.close()
