"""Durable lifecycle decisions; no transaction spans database authorities."""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .domain_database import DOMAIN_TABLES, DomainBarrier, DomainDatabase, DomainLifecycle, DomainOutbox, DomainScope
from .handoff import DomainHandoff, Envelope
from .models import canonical_digest, canonical_json


@dataclass(frozen=True)
class LifecycleProposal:
    operation_id: str
    owner_id: str
    thread_id: str
    generation: int
    action: str
    target_state: str
    owner_generation: int = 0

    @property
    def scope_id(self):
        return canonical_digest([self.owner_id, self.thread_id])

    def validate(self):
        states = {"archive": "archived", "unarchive": "open", "delete": "deleted", "register": "open"}
        if (any(not isinstance(value, str) for value in (self.action, self.target_state,
                self.operation_id, self.owner_id, self.thread_id))
                or self.action not in states or states[self.action] != self.target_state
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", self.operation_id)
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", self.owner_id)
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{0,32}", self.thread_id)
                or len(self.operation_id) > 64 or len(self.owner_id) > 64 or len(self.thread_id) > 32
                or type(self.generation) is not int or self.generation < 0
                or type(self.owner_generation) is not int or self.owner_generation < 0
                or (self.action == "register") != (self.thread_id == "")):
            raise ValueError("lifecycle_proposal_invalid")


@dataclass(frozen=True)
class LifecycleResult:
    operation_id: str
    state: str
    error_code: str = ""


@dataclass(frozen=True)
class ScopeVersion:
    owner_generation: int
    thread_generation: int


Check = Callable[[AsyncSession, LifecycleProposal], Awaitable[str]]
Effect = Callable[[AsyncSession, LifecycleProposal], Awaitable[None]]


class LifecycleParticipant:
    def __init__(self, database: DomainDatabase, check: Check | None = None, effect: Effect | None = None):
        self.database, self.check, self.effect = database, check, effect
        self.handoff = DomainHandoff(database, {
            "lifecycle.prepare": self._receive_prepare,
            "lifecycle.commit": self._receive_commit,
            "lifecycle.abort": self._receive_abort,
        })

    async def _scope(self, session, owner_id, thread_id):
        if session.info.get("domain") != self.database.domain or session.bind is not self.database.engine:
            raise ValueError("lifecycle_session_mismatch")
        if session.bind.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        identity = canonical_digest([owner_id, thread_id])
        await session.execute(insert(DomainScope).values(id=identity, owner_id=owner_id,
            thread_id=thread_id, generation=0, state="open", frozen_by="").on_conflict_do_nothing())
        return await session.scalar(select(DomainScope).where(DomainScope.id == identity).with_for_update())

    async def admit(self, session, owner_id, thread_id, *, generation=None, owner_generation=None):
        owner = await self._scope(session, owner_id, "")
        if owner.state != "open" or owner.frozen_by:
            raise RuntimeError("lifecycle_admission_blocked")
        if owner_generation is not None and owner_generation != owner.generation:
            raise RuntimeError("lifecycle_owner_generation_stale")
        scope = await self._scope(session, owner_id, thread_id) if thread_id else owner
        if scope.state != "open" or scope.frozen_by:
            raise RuntimeError("lifecycle_admission_blocked")
        if generation is not None and generation != scope.generation:
            raise RuntimeError("lifecycle_generation_stale")
        return ScopeVersion(owner.generation, scope.generation)

    async def prepare(self, session, proposal):
        proposal.validate()
        await self.handoff._identity_lock(session, "lifecycle", proposal.operation_id)
        old = await session.get(DomainBarrier, proposal.operation_id)
        encoded = canonical_json(asdict(proposal))
        if old:
            if old.proposal_json != encoded:
                raise ValueError("lifecycle_identity_conflict")
            return {"prepared": old.state in {"prepared", "committed"}, "error_code": old.error_code}
        owner = await self._scope(session, proposal.owner_id, "")
        scope = await self._scope(session, proposal.owner_id, proposal.thread_id) if proposal.thread_id else owner
        error = ""
        if owner.frozen_by or scope.frozen_by or scope.state == "deleted":
            error = "lifecycle_admission_blocked"
        elif proposal.thread_id and owner.generation != proposal.owner_generation:
            error = "lifecycle_owner_generation_stale"
        elif scope.generation != proposal.generation:
            error = "lifecycle_generation_stale"
        elif not proposal.thread_id and await session.scalar(select(DomainScope.id).where(
                DomainScope.owner_id == proposal.owner_id, DomainScope.thread_id != "",
                DomainScope.frozen_by != "").limit(1)):
            error = "lifecycle_admission_blocked"
        elif self.check:
            error = await self.check(session, proposal)
        if error and (not isinstance(error, str) or len(error) > 80
                      or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_" for c in error)):
            raise ValueError("lifecycle_error_code_invalid")
        if not error:
            scope.frozen_by = proposal.operation_id
        session.add(DomainBarrier(id=proposal.operation_id, proposal_json=encoded,
            state="rejected" if error else "prepared", error_code=error))
        await session.flush()
        return {"prepared": not bool(error), "error_code": error}

    async def decide(self, session, proposal, commit, *, retain_barrier=False):
        proposal.validate()
        await self.handoff._identity_lock(session, "lifecycle", proposal.operation_id)
        row = await session.get(DomainBarrier, proposal.operation_id, with_for_update=True)
        if row is None or row.proposal_json != canonical_json(asdict(proposal)):
            raise ValueError("lifecycle_identity_conflict")
        wanted = "committed" if commit else "aborted"
        if row.state == wanted:
            return {"state": wanted}
        if row.state not in ({"prepared"} if commit else {"prepared", "rejected"}):
            raise ValueError("lifecycle_decision_conflict")
        await self._scope(session, proposal.owner_id, "")
        scope = await self._scope(session, proposal.owner_id, proposal.thread_id)
        if row.state == "prepared":
            if scope.frozen_by != proposal.operation_id or scope.generation != proposal.generation:
                raise ValueError("lifecycle_generation_stale")
            if commit:
                if self.effect:
                    await self.effect(session, proposal)
                scope.state, scope.generation = proposal.target_state, proposal.generation + 1
            if not retain_barrier:
                scope.frozen_by = ""
        row.state = wanted
        return {"state": wanted}

    async def release_decision(self, session, proposal):
        """Core admission stays frozen until every decision acknowledgement exists."""
        await self.handoff._identity_lock(session, "lifecycle", proposal.operation_id)
        row = await session.get(DomainBarrier, proposal.operation_id, with_for_update=True)
        if row is None or row.state not in {"committed", "aborted"}:
            raise ValueError("lifecycle_decision_missing")
        await self._scope(session, proposal.owner_id, "")
        scope = await self._scope(session, proposal.owner_id, proposal.thread_id)
        expected = proposal.generation + (1 if row.state == "committed" else 0)
        if scope.frozen_by != proposal.operation_id or scope.generation != expected:
            raise ValueError("lifecycle_generation_stale")
        scope.frozen_by = ""

    def _proposal(self, envelope):
        if envelope.source != "core":
            raise ValueError("lifecycle_authority_invalid")
        proposal = LifecycleProposal(**envelope.payload)
        proposal.validate()
        if (envelope.owner_id != proposal.owner_id or envelope.aggregate_id != proposal.scope_id
                or envelope.generation != proposal.generation):
            raise ValueError("lifecycle_scope_mismatch")
        return proposal

    async def _receive_prepare(self, session, envelope):
        return await self.prepare(session, self._proposal(envelope))

    async def _receive_commit(self, session, envelope):
        return await self.decide(session, self._proposal(envelope), True)

    async def _receive_abort(self, session, envelope):
        return await self.decide(session, self._proposal(envelope), False)


class LifecycleCoordinator:
    def __init__(self, participant: LifecycleParticipant):
        if participant.database.domain != "core":
            raise ValueError("lifecycle_authority_invalid")
        self.local, self.database, self.handoff = participant, participant.database, participant.handoff

    @staticmethod
    def _view(row):
        return LifecycleResult(row.id, row.phase if row.phase in {"completed", "refused"} else "pending",
                               row.error_code)

    @staticmethod
    def _command_id(operation, target, phase):
        return canonical_digest(["lifecycle", operation, target, phase])

    async def _enqueue(self, session, proposal, participants, phase):
        for target in sorted(participants):
            await self.handoff.enqueue(session,
                command_id=self._command_id(proposal.operation_id, target, phase), target=target,
                kind="lifecycle." + phase, aggregate_id=proposal.scope_id, owner_id=proposal.owner_id,
                generation=proposal.generation, payload=asdict(proposal))

    async def start(self, proposal, participants):
        proposal.validate()
        if not participants or not set(participants) <= DOMAIN_TABLES.keys() - {"core"}:
            raise ValueError("lifecycle_participants_invalid")
        encoded, targets = canonical_json(asdict(proposal)), canonical_json(sorted(participants))
        async with self.database.transaction() as session:
            await self.handoff._identity_lock(session, "lifecycle", proposal.operation_id)
            row = await session.get(DomainLifecycle, proposal.operation_id)
            if row:
                if row.proposal_json != encoded or row.participants_json != targets:
                    raise ValueError("lifecycle_identity_conflict")
                return self._view(row)
            result = await self.local.prepare(session, proposal)
            row = DomainLifecycle(id=proposal.operation_id, owner_id=proposal.owner_id, proposal_json=encoded,
                participants_json=targets, phase="preparing" if result["prepared"] else "refused",
                error_code=result["error_code"])
            session.add(row)
            if result["prepared"]:
                await self._enqueue(session, proposal, participants, "prepare")
            return self._view(row)

    async def status(self, operation_id, owner_id=None):
        async with self.database.connections() as session:
            row = await session.get(DomainLifecycle, operation_id)
            if row is None or (owner_id is not None and row.owner_id != owner_id):
                raise KeyError("lifecycle_operation_not_found")
            return self._view(row)

    async def pending_commands(self, operation_id):
        async with self.database.connections() as session:
            row = await session.get(DomainLifecycle, operation_id)
            if row is None:
                raise KeyError("lifecycle_operation_not_found")
            phase = {"preparing": "prepare", "committing": "commit", "aborting": "abort"}.get(row.phase)
            if phase is None:
                return []
            ids = [self._command_id(row.id, target, phase) for target in json.loads(row.participants_json)]
            commands = (await session.scalars(select(DomainOutbox).where(DomainOutbox.id.in_(ids),
                DomainOutbox.state == "pending"))).all()
            return [Envelope(**json.loads(command.envelope_json)) for command in commands]

    async def advance(self, operation_id):
        async with self.database.transaction() as session:
            row = await session.get(DomainLifecycle, operation_id, with_for_update=True)
            if row is None:
                raise KeyError("lifecycle_operation_not_found")
            if row.phase in {"completed", "refused"}:
                return self._view(row)
            proposal = LifecycleProposal(**json.loads(row.proposal_json))
            targets = json.loads(row.participants_json)
            phase = {"preparing": "prepare", "committing": "commit", "aborting": "abort"}[row.phase]
            commands = [await session.get(DomainOutbox, self._command_id(row.id, target, phase)) for target in targets]
            # An unknown participant outcome never becomes an abort by timeout.
            if any(command is None or command.state != "acknowledged" for command in commands):
                return self._view(row)
            results = [json.loads(command.receipt_json)["result"] for command in commands]
            if row.phase == "preparing":
                errors = [result["error_code"] for result in results if not result["prepared"]]
                commit = not errors
                await self.local.decide(session, proposal, commit, retain_barrier=True)
                row.phase = "committing" if commit else "aborting"
                row.error_code = errors[0] if errors else ""
                await self._enqueue(session, proposal, targets, "commit" if commit else "abort")
            else:
                wanted = "committed" if row.phase == "committing" else "aborted"
                if any(result.get("state") != wanted for result in results):
                    raise ValueError("lifecycle_receipt_invalid")
                await self.local.release_decision(session, proposal)
                row.phase = "completed" if row.phase == "committing" else "refused"
            return self._view(row)
