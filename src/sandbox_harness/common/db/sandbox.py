"""Sandbox persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from datetime import (datetime)
from sqlalchemy import (or_, select, update)
from sandbox_harness.common.contracts.domain import (TERMINAL, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (TERMINAL_JOB_STATES)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxHandle)
from .ownership_models import SandboxOwnershipRow
from .models import (JobAttemptRow, JobCleanupRow, JobRow, SandboxAllocationRow, SandboxAuditRow, SandboxControlRow, SandboxPortGrantRow, ThreadRow, TurnRow, canonical_json, utc_datetime)


class SandboxTransactions:
    @staticmethod
    def sandbox_control_view(row: SandboxControlRow) -> dict:
        return {"thread_id": row.thread_id, "sandbox_id": row.sandbox_id, "state": row.state,
            "sandbox_handle":json.loads(row.sandbox_handle_json),
            "generation": row.generation, "pty_session_id": row.pty_session_id,
            "release_phase":row.release_phase,"release_action":row.release_action,
            "expires_at": utc_datetime(row.expires_at), "created_at": row.created_at,
            "updated_at": row.updated_at}


    async def get_sandbox_control(self, thread_id: str, owner_id: str | None = None) -> dict | None:
        async with self.connections() as db:
            query = select(SandboxControlRow).where(SandboxControlRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(SandboxControlRow.owner_id == owner_id)
            row = (await db.execute(query)).scalar_one_or_none()
        return self.sandbox_control_view(row) if row else None


    async def sandbox_control_nonce(self, thread_id: str, owner_id: str) -> str:
        async with self.connections() as db:
            return (await db.execute(select(SandboxControlRow.capability_nonce).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id))).scalar_one_or_none() or ""


    async def begin_sandbox_control(self, thread_id: str, owner_id: str, sandbox_id: str,
                                    sandbox_handle:dict[str,str],capability_nonce: str,
                                    expires_at: datetime) -> dict:
        handle_json=canonical_json(sandbox_handle)
        if sandbox_handle.get("sandbox_id")!=sandbox_id:raise ValueError("sandbox_handle_id_mismatch")
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            active = (await db.execute(select(TurnRow.id).where(TurnRow.thread_id == thread_id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1))).scalar_one_or_none()
            if active:
                raise RuntimeError("active_turn_prevents_sandbox_takeover")
            row = await db.get(SandboxControlRow, thread_id, with_for_update=True)
            timestamp = now()
            if row and row.state == "user_controlled" and utc_datetime(row.expires_at) > timestamp:
                if row.sandbox_id != sandbox_id:
                    raise RuntimeError("sandbox_control_conflict")
                if row.sandbox_handle_json!=handle_json:
                    raise RuntimeError("sandbox_control_handle_conflict")
                row.expires_at = expires_at; row.updated_at = timestamp
            elif row:
                row.owner_id = owner_id; row.sandbox_id = sandbox_id; row.state = "user_controlled"
                row.sandbox_handle_json=handle_json
                row.generation += 1; row.capability_nonce = capability_nonce; row.pty_session_id = ""
                row.release_phase="";row.release_action=""
                row.expires_at = expires_at; row.created_at = timestamp; row.updated_at = timestamp
            else:
                row = SandboxControlRow(thread_id=thread_id, owner_id=owner_id,
                    sandbox_id=sandbox_id,sandbox_handle_json=handle_json,
                    state="user_controlled", generation=1,
                    capability_nonce=capability_nonce, expires_at=expires_at,
                    created_at=timestamp, updated_at=timestamp)
                db.add(row)
        return self.sandbox_control_view(row)


    async def touch_sandbox_control(self, thread_id: str, owner_id: str, expires_at: datetime) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id,
                SandboxControlRow.state == "user_controlled").with_for_update())).scalar_one_or_none()
            if not row or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("sandbox_control_not_active")
            row.expires_at = expires_at; row.updated_at = now()
        return self.sandbox_control_view(row)


    async def set_sandbox_terminal(self, thread_id: str, owner_id: str,
                                   sandbox_id: str, pty_session_id: str) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id,
                SandboxControlRow.sandbox_id == sandbox_id,
                SandboxControlRow.state == "user_controlled").with_for_update())).scalar_one_or_none()
            if not row or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("sandbox_control_not_active")
            row.pty_session_id = pty_session_id; row.updated_at = now()
        return self.sandbox_control_view(row)


    async def mark_sandbox_control_releasing(self, thread_id: str, owner_id: str,
                                              managed_action:str="") -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise RuntimeError("sandbox_control_not_active")
            if row.state!="releasing":
                row.state="releasing";row.release_phase="starting"
                row.release_action=managed_action
            elif managed_action and row.release_action and row.release_action!=managed_action:
                raise RuntimeError("sandbox_release_action_conflict")
            elif managed_action and not row.release_action:row.release_action=managed_action
            row.updated_at = now()
        return self.sandbox_control_view(row)


    async def advance_sandbox_release_phase(self,thread_id:str,owner_id:str,
                                            expected:str,next_phase:str)->dict:
        async with self.connections.begin() as db:
            row=(await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id==thread_id,SandboxControlRow.owner_id==owner_id,
                SandboxControlRow.state=="releasing",SandboxControlRow.release_phase==expected)
                .with_for_update())).scalar_one_or_none()
            if not row:raise RuntimeError("stale_sandbox_release_phase")
            row.release_phase=next_phase;row.updated_at=now()
        return self.sandbox_control_view(row)


    async def finish_sandbox_control(self, thread_id: str, owner_id: str) -> None:
        async with self.connections.begin() as db:
            row = (await db.execute(select(SandboxControlRow).where(
                SandboxControlRow.thread_id == thread_id,
                SandboxControlRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if row:
                await db.delete(row)
            await db.execute(update(SandboxPortGrantRow).where(
                SandboxPortGrantRow.thread_id == thread_id).values(revoked=True))


    async def active_sandbox_control_ids(self) -> set[str]:
        async with self.connections() as db:
            rows = (await db.execute(select(SandboxControlRow.sandbox_id).where(
                SandboxControlRow.state.in_(["user_controlled", "releasing"])))).scalars().all()
        return set(rows)


    async def active_job_sandbox_ids(self) -> set[str]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobAttemptRow.acceptance_receipt_json).join(
                JobRow,JobRow.id==JobAttemptRow.job_id).where(
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                or_(JobAttemptRow.accepted_at.is_not(None),
                    JobAttemptRow.outcome=="SUBMITTING")))).scalars().all()
            rows.extend((await db.execute(select(JobCleanupRow.acceptance_receipt_json).where(
                JobCleanupRow.state!="completed"))).scalars().all())
        result=set()
        for encoded in rows:
            try:identity=json.loads(encoded).get('sandbox_id','')
            except (TypeError,ValueError):continue
            if isinstance(identity,str) and identity:result.add(identity)
        return result


    async def active_sandbox_handles(self)->list[SandboxHandle]:
        terminal=[state.value for state in TERMINAL]
        async with self.connections() as db:
            direct=list((await db.execute(select(TurnRow.sandbox_id,TurnRow.sandbox_handle_json).where(
                TurnRow.state.not_in(terminal),TurnRow.sandbox_id!=""))).all())
            direct.extend((await db.execute(select(SandboxControlRow.sandbox_id,
                SandboxControlRow.sandbox_handle_json).where(
                SandboxControlRow.state.in_(["user_controlled","releasing"])))).all())
            receipts=list((await db.execute(select(JobAttemptRow.acceptance_receipt_json).join(
                JobRow,JobRow.id==JobAttemptRow.job_id).where(
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                JobAttemptRow.acceptance_receipt_json!="{}"))).scalars().all())
            receipts.extend((await db.execute(select(JobCleanupRow.acceptance_receipt_json).where(
                JobCleanupRow.state!="completed"))).scalars().all())
            dispatched=(await db.scalars(select(SandboxAllocationRow.handle_json).where(
                SandboxAllocationRow.state=='READY'))).all()
            owned=(await db.scalars(select(SandboxOwnershipRow.handle_json).where(
                SandboxOwnershipRow.namespace==self.sandbox_namespace,
                SandboxOwnershipRow.state!='RELEASED',
                SandboxOwnershipRow.handle_json!=''))).all()
        result=[]
        for encoded in [*dispatched,*owned]:
            try:result.append(SandboxHandle.from_mapping(json.loads(encoded)))
            except (TypeError,ValueError) as exc:
                raise RuntimeError('active_sandbox_handle_missing_or_invalid') from exc
        for sandbox_id,encoded in direct:
            try:
                handle=SandboxHandle.from_mapping(json.loads(encoded))
            except (TypeError,ValueError) as exc:raise RuntimeError(
                "active_sandbox_handle_missing_or_invalid") from exc
            if handle.sandbox_id!=sandbox_id:raise RuntimeError("active_sandbox_handle_id_mismatch")
            result.append(handle)
        for encoded in receipts:
            try:
                payload=json.loads(encoded);sandbox_id=payload.get("sandbox_id","")
                if not sandbox_id:continue
                handle=SandboxHandle.from_mapping(payload.get("sandbox_handle") or {})
            except (AttributeError,TypeError,ValueError) as exc:raise RuntimeError(
                "active_sandbox_handle_missing_or_invalid") from exc
            if handle.sandbox_id!=sandbox_id:raise RuntimeError("active_sandbox_handle_id_mismatch")
            result.append(handle)
        return result


    async def job_sandbox_capacity(self)->list[dict]:
        """Allocation identities still charged to the JobWorker Pool after restart."""
        from sandbox_harness.common.contracts.sandbox_contracts import (SandboxProfile)
        async with self.connections() as db:
            rows=(await db.scalars(select(SandboxAllocationRow))).all()
            completed=select(JobCleanupRow.id).where(JobCleanupRow.job_id==JobAttemptRow.job_id,
                JobCleanupRow.generation==JobAttemptRow.generation,
                JobCleanupRow.state=='completed').correlate(JobAttemptRow).exists()
            receipts=list((await db.scalars(select(JobAttemptRow.acceptance_receipt_json).join(
                JobRow,JobRow.id==JobAttemptRow.job_id).where(
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                or_(JobAttemptRow.outcome=='SUBMITTING',JobAttemptRow.accepted_at.is_not(None)),
                ~completed))).all())
            receipts.extend((await db.scalars(select(JobCleanupRow.acceptance_receipt_json).where(
                JobCleanupRow.state!='completed'))).all())
        result={};inactive=set()
        for row in rows:
            if row.state not in {'PENDING','SENDING','UNKNOWN','READY'}:
                inactive.add(row.id);continue
            profile=SandboxProfile(**json.loads(row.request_json)['profile'])
            result[(row.provider,row.allocation_id)]=profile.compatibility_key
        for encoded in receipts:
            value=json.loads(encoded);phase=value.get('allocation_phase')
            if phase in {'reserving_resources','awaiting_capacity','absent'}:continue
            if value.get('allocation_dispatch_id') in inactive:continue
            if value.get('allocation_profile'):
                profile=SandboxProfile(**value['allocation_profile'])
                key=(value['allocation_provider'],value['allocation_id']);profile_key=profile.compatibility_key
            elif value.get('sandbox_handle'):
                handle=SandboxHandle.from_mapping(value['sandbox_handle'])
                key=(handle.provider,handle.allocation_id);profile_key=handle.profile_key
            else:continue
            if key in result and result[key]!=profile_key:raise RuntimeError('sandbox_allocation_profile_conflict')
            result[key]=profile_key
        return [{'provider':provider,'allocation_id':identity,'profile_key':profile}
            for (provider,identity),profile in sorted(result.items())]


    async def active_sandbox_allocations(self)->set[tuple[str,str]]:
        async with self.connections() as db:
            receipts=list((await db.execute(select(JobAttemptRow.acceptance_receipt_json).join(
                JobRow,JobRow.id==JobAttemptRow.job_id).where(
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES]),
                JobAttemptRow.acceptance_receipt_json!='{}'))).scalars())
            receipts.extend((await db.execute(select(JobCleanupRow.acceptance_receipt_json).where(
                JobCleanupRow.state!='completed'))).scalars())
            dispatched=(await db.execute(select(SandboxAllocationRow.provider,
                SandboxAllocationRow.allocation_id).where(SandboxAllocationRow.state.in_(
                    ['PENDING','SENDING','READY','UNKNOWN'])))).all()
            owned=(await db.execute(select(SandboxOwnershipRow.provider,
                SandboxOwnershipRow.allocation_id).where(
                    SandboxOwnershipRow.namespace==self.sandbox_namespace,
                    SandboxOwnershipRow.state!='RELEASED'))).all()
        result=set(dispatched)|set(owned)
        for encoded in receipts:
            try:
                value=json.loads(encoded)
                if value.get('allocation_phase') not in {
                        'awaiting_capacity','allocating','preparing','runtime_submission'}:continue
                provider=value['allocation_provider'];allocation=value['allocation_id']
                profile=value['allocation_profile']
                if (not isinstance(provider,str) or not provider or not isinstance(allocation,str)
                        or not allocation or profile.get('provider')!=provider):raise ValueError()
                result.add((provider,allocation))
            except (TypeError,ValueError,KeyError,AttributeError) as exc:
                raise RuntimeError('active_sandbox_allocation_missing_or_invalid') from exc
        return result


    async def active_sandbox_providers(self)->set[str]:
        return ({handle.provider for handle in await self.active_sandbox_handles()}
            | {provider for provider,_ in await self.active_sandbox_allocations()})


    async def active_sandbox_keys(self)->set[tuple[str,str]]:
        return {(handle.provider,handle.sandbox_id)
            for handle in await self.active_sandbox_handles()}


    async def pending_job_cleanup_ids(self)->set[str]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobCleanupRow.job_id).where(
                JobCleanupRow.state!="completed"))).scalars().all()
        return set(rows)


    async def nonterminal_job_ids(self) -> set[str]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobRow.id).where(
                JobRow.state.not_in([state.value for state in TERMINAL_JOB_STATES])))).scalars().all()
        return set(rows)


    async def expired_sandbox_controls(self, limit: int = 20) -> list[dict]:
        async with self.connections() as db:
            rows = (await db.execute(select(SandboxControlRow).where(
                or_(SandboxControlRow.state=="releasing",SandboxControlRow.expires_at <= now()))
                .order_by(SandboxControlRow.updated_at)
                .limit(limit))).scalars().all()
        return [self.sandbox_control_view(row) | {"owner_id": row.owner_id,
            "capability_nonce": row.capability_nonce} for row in rows]


    async def update_thread_checkpoint_idle(self, thread_id: str, owner_id: str,
                                            checkpoint_ref: str) -> None:
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            active = (await db.execute(select(TurnRow.id).where(TurnRow.thread_id == thread_id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(1))).scalar_one_or_none()
            if active:
                raise RuntimeError("active_turn_prevents_checkpoint")
            thread.checkpoint_ref = checkpoint_ref; thread.updated_at = now()


    async def add_sandbox_audit(self, thread_id: str, owner_id: str, sandbox_id: str,
                                action: str, detail: dict | None = None) -> None:
        safe_detail = detail or {}
        async with self.connections.begin() as db:
            db.add(SandboxAuditRow(id=identifier(), thread_id=thread_id, owner_id=owner_id,
                sandbox_id=sandbox_id, action=action[:80],
                detail_json=json.dumps(safe_detail, ensure_ascii=True, sort_keys=True), created_at=now()))


    async def list_sandbox_audit(self, thread_id: str, owner_id: str, limit: int = 100) -> list[dict]:
        async with self.connections() as db:
            rows = (await db.execute(select(SandboxAuditRow).where(
                SandboxAuditRow.thread_id == thread_id, SandboxAuditRow.owner_id == owner_id)
                .order_by(SandboxAuditRow.created_at.desc()).limit(min(max(limit, 1), 500)))).scalars().all()
        return [{"id": row.id, "action": row.action, "detail": json.loads(row.detail_json or "{}"),
            "created_at": row.created_at} for row in rows]


    async def create_sandbox_port_grant(self, thread_id: str, owner_id: str,
                                        sandbox_id: str,sandbox_handle:dict[str,str],port: int,
                                        expires_at: datetime) -> dict:
        if sandbox_handle.get("sandbox_id")!=sandbox_id:raise ValueError("sandbox_handle_id_mismatch")
        row = SandboxPortGrantRow(id=identifier(), thread_id=thread_id, owner_id=owner_id,
            sandbox_id=sandbox_id,sandbox_handle_json=canonical_json(sandbox_handle),
            port=port, expires_at=expires_at,
            revoked=False, created_at=now())
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow.id).where(ThreadRow.id == thread_id,
                ThreadRow.owner_id == owner_id))).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            db.add(row)
        return {"id": row.id, "thread_id": thread_id, "sandbox_id": sandbox_id,
            "sandbox_handle":sandbox_handle,
            "port": port, "expires_at": expires_at}


    async def get_sandbox_port_grant(self, grant_id: str, owner_id: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(SandboxPortGrantRow).where(
                SandboxPortGrantRow.id == grant_id, SandboxPortGrantRow.owner_id == owner_id,
                SandboxPortGrantRow.revoked == False,
                SandboxPortGrantRow.expires_at > now()))).scalar_one_or_none()
        return ({"id": row.id, "thread_id": row.thread_id, "sandbox_id": row.sandbox_id,
            "sandbox_handle":json.loads(row.sandbox_handle_json),
            "port": row.port, "expires_at": row.expires_at} if row else None)


    async def revoke_sandbox_port_grant(self, grant_id: str, owner_id: str) -> bool:
        async with self.connections.begin() as db:
            result = await db.execute(update(SandboxPortGrantRow).where(
                SandboxPortGrantRow.id == grant_id, SandboxPortGrantRow.owner_id == owner_id)
                .values(revoked=True))
        return bool(result.rowcount)

