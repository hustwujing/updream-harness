"""Durable, ordered domain commands with transaction-local effects.

Handlers mutate only the supplied destination session. They must not call a
Provider or another database: those effects require another persisted intent.
The transport can lose any reply; retry always uses the original envelope.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Awaitable, Callable

from sqlalchemy import func, select, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from sandbox_harness.common.contracts.domain_commands import (
    DOMAIN_COMMAND_CONTRACTS, validate_command_payload, validate_command_result,
)
from sandbox_harness.common.contracts.domain import now
from sandbox_harness.common.observability.observability import (
    add_link, canonical_traceparent, carrier, operation,
)
from .domain_database import DOMAIN_TABLES, DomainDatabase, DomainInbox, DomainOutbox, DomainStream
from .models import canonical_digest, canonical_json


@dataclass(frozen=True)
class Envelope:
    command_id: str
    source: str
    target: str
    kind: str
    aggregate_id: str
    owner_id: str
    generation: int
    sequence: int
    payload_json: str
    traceparent: str = ""
    version: int = 1
    contract_version: int = 1
    contract_digest: str = ""

    @property
    def payload(self) -> dict:
        return json.loads(self.payload_json)

    @property
    def stream_id(self) -> str:
        return canonical_digest([self.source, self.target, self.owner_id, self.aggregate_id])

    @property
    def digest(self) -> str:
        value=asdict(self)
        # Existing version-1 envelopes did not serialize an empty traceparent.
        # Preserve their digest while binding every newly captured context.
        if not value["traceparent"]:value.pop("traceparent")
        return canonical_digest(value)

    def validate(self):
        if (self.source not in DOMAIN_TABLES or self.target not in DOMAIN_TABLES
                or self.source == self.target or self.version != 1
                or self.contract_version != 1
                or type(self.sequence) is not int or self.sequence < 1
                or type(self.generation) is not int or self.generation < 0):
            raise ValueError("handoff_envelope_invalid")
        for value, maximum in ((self.command_id, 64), (self.kind, 80),
                               (self.aggregate_id, 160), (self.owner_id, 64)):
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1," + str(maximum) + "}", value):
                raise ValueError("handoff_envelope_invalid")
        if len(self.payload_json.encode()) > 1024 * 1024 or not isinstance(self.payload, dict):
            raise ValueError("handoff_payload_invalid")
        if canonical_json(self.payload) != self.payload_json:
            raise ValueError("handoff_payload_invalid")
        if self.traceparent and (not isinstance(self.traceparent,str)
                or len(self.traceparent) != 55
                or canonical_traceparent(self.traceparent) != self.traceparent):
            raise ValueError("handoff_traceparent_invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", self.contract_digest):
            raise ValueError("domain_command_contract_mismatch")


Handler = Callable[[AsyncSession, Envelope], Awaitable[dict]]


@dataclass(frozen=True)
class RelayFailure:
    stream_id: str
    target: str
    error_code: str


@dataclass(frozen=True)
class RelayResult:
    delivered: int
    failures: tuple[RelayFailure, ...]


class DomainHandoff:
    def __init__(self, database: DomainDatabase, handlers: dict[str, Handler], *, contracts=None):
        self.database = database
        self.handlers = dict(handlers)
        self.contracts = dict(contracts or DOMAIN_COMMAND_CONTRACTS)

    async def _identity_lock(self, session, source, identity):
        if session.bind.dialect.name == "postgresql":
            key = int(canonical_digest([source, identity])[:16], 16)
            if key >= 2**63:
                key -= 2**64
            await session.execute(select(func.pg_advisory_xact_lock(key)))

    async def _stream(self, session, direction, stream_id):
        key = canonical_digest([direction, stream_id])
        if session.bind.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif session.bind.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise ValueError("handoff_database_unsupported")
        await session.execute(insert(DomainStream).values(id=key, sequence=0).on_conflict_do_nothing())
        return await session.scalar(select(DomainStream).where(DomainStream.id == key).with_for_update())

    async def enqueue(self, session, *, command_id, target, kind, aggregate_id,
                      owner_id, generation, payload) -> Envelope:
        if session.info.get("domain") != self.database.domain or session.bind is not self.database.engine:
            raise ValueError("handoff_source_session_mismatch")
        contract = validate_command_payload(kind, self.database.domain, target, payload,
            contracts=self.contracts)
        proposed = Envelope(command_id=command_id,source=self.database.domain,target=target,
            kind=kind,aggregate_id=aggregate_id,owner_id=owner_id,generation=generation,
            sequence=1,payload_json=canonical_json(payload),
            traceparent=canonical_traceparent(carrier().get("traceparent","")),
            contract_version=contract.version, contract_digest=contract.digest)
        proposed.validate()
        await self._identity_lock(session, self.database.domain, command_id)
        existing = await session.get(DomainOutbox, command_id)
        if existing:
            original = Envelope(**json.loads(existing.envelope_json))
            if {**asdict(proposed), "sequence": original.sequence,
                    "traceparent": original.traceparent} != asdict(original):
                raise ValueError("handoff_identity_conflict")
            return original
        stream = await self._stream(session, "out", proposed.stream_id)
        stream.sequence += 1
        envelope = Envelope(**{**asdict(proposed), "sequence": stream.sequence})
        session.add(DomainOutbox(id=command_id, target=target, stream_id=envelope.stream_id,
            sequence=envelope.sequence, envelope_json=canonical_json(asdict(envelope)),
            digest=envelope.digest, state="pending", receipt_json="", created_at=now()))
        await session.flush()
        return envelope

    async def receive(self, envelope: Envelope) -> dict:
        envelope.validate()
        if envelope.target != self.database.domain:
            raise ValueError("handoff_target_mismatch")
        contract = validate_command_payload(envelope.kind, envelope.source, envelope.target,
            envelope.payload, contracts=self.contracts)
        if (envelope.contract_version != contract.version
                or envelope.contract_digest != contract.digest):
            raise ValueError("domain_command_contract_mismatch")
        with operation("domain.handoff.receive",new_trace=True,
                source_service=envelope.source,peer_service=envelope.target):
            add_link(envelope.traceparent)
            identity = canonical_digest([envelope.source, envelope.command_id])
            async with self.database.transaction() as session:
                await self._identity_lock(session, envelope.source, envelope.command_id)
                previous = await session.get(DomainInbox, identity)
                if previous:
                    if previous.digest != envelope.digest:
                        raise ValueError("handoff_identity_conflict")
                    return json.loads(previous.receipt_json)
                stream = await self._stream(session, "in", envelope.stream_id)
                if envelope.sequence != stream.sequence + 1:
                    raise ValueError("handoff_sequence_gap")
                handler = self.handlers.get(envelope.kind)
                if handler is None:
                    raise ValueError("handoff_kind_unsupported")
                result = await handler(session, envelope)
                if not isinstance(result, dict):
                    raise ValueError("handoff_result_invalid")
                validate_command_result(envelope.kind, result, contracts=self.contracts)
                receipt = {"version": 1, "command_id": envelope.command_id, "digest": envelope.digest,
                           "source": envelope.source, "target": envelope.target,
                           "sequence": envelope.sequence, "result": result}
                session.add(DomainInbox(id=identity, stream_id=envelope.stream_id, sequence=envelope.sequence,
                    digest=envelope.digest, receipt_json=canonical_json(receipt), created_at=now()))
                stream.sequence = envelope.sequence
            return receipt

    async def acknowledge(self, command_id: str, receipt: dict):
        async with self.database.transaction() as session:
            row = await session.scalar(select(DomainOutbox).where(DomainOutbox.id == command_id).with_for_update())
            if row is None:
                raise ValueError("handoff_command_missing")
            if (receipt.get("version") != 1 or receipt.get("command_id") != row.id
                    or receipt.get("digest") != row.digest or receipt.get("source") != self.database.domain
                    or receipt.get("target") != row.target or receipt.get("sequence") != row.sequence
                    or not isinstance(receipt.get("result"), dict)):
                raise ValueError("handoff_receipt_mismatch")
            encoded = canonical_json(receipt)
            if row.receipt_json and row.receipt_json != encoded:
                raise ValueError("handoff_receipt_mismatch")
            row.receipt_json, row.state = encoded, "acknowledged"

    async def _defer(self, identity, code):
        async with self.database.transaction() as session:
            row = await session.scalar(select(DomainOutbox).where(DomainOutbox.id == identity).with_for_update())
            if row is not None and row.state == "pending":
                row.attempts += 1
                row.error_code = code
                row.next_attempt_at = now() + timedelta(seconds=min(60, 2 ** min(row.attempts, 6)))

    async def relay(self, destinations: dict[str, DomainHandoff], limit: int = 100) -> RelayResult:
        if not 1 <= limit <= 1000:
            raise ValueError("handoff_batch_invalid")
        async with self.database.connections() as session:
            earlier = aliased(DomainOutbox)
            preceding = select(earlier.id).where(earlier.stream_id == DomainOutbox.stream_id,
                earlier.state == "pending", earlier.sequence < DomainOutbox.sequence).exists()
            rows = list((await session.scalars(select(DomainOutbox).where(
                DomainOutbox.state == "pending", DomainOutbox.target.in_(destinations), ~preceding,
                or_(DomainOutbox.next_attempt_at.is_(None), DomainOutbox.next_attempt_at <= now()))
                .order_by(DomainOutbox.created_at, DomainOutbox.stream_id, DomainOutbox.sequence)
                .limit(limit))).all())
        delivered = 0
        failures = []
        for row in rows:
            try:
                receipt = await destinations[row.target].receive(Envelope(**json.loads(row.envelope_json)))
                await self.acknowledge(row.id, receipt)
                delivered += 1
            except Exception:
                # Store no raw exception or payload. A source DB outage propagates
                # from _defer; an unavailable destination blocks only its stream.
                code = "handoff_delivery_failed"
                await self._defer(row.id, code)
                failures.append(RelayFailure(row.stream_id, row.target, code))
        return RelayResult(delivered, tuple(failures))
