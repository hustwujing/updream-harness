"""Immutable Thread checkpoint versions and atomic active-pointer projection."""
from __future__ import annotations

import json
import re
from datetime import timedelta

from sqlalchemy import delete, select

from sandbox_harness.common.contracts.domain import now
from .models import CheckpointVersionRow, ThreadRow, TurnRow, canonical_digest


CHECKPOINT_STATES={"STAGING","VERIFIED","ACTIVE","SUPERSEDED","INVALID"}


class CheckpointTransactions:
    @staticmethod
    def checkpoint_version_view(row:CheckpointVersionRow)->dict:
        return {"id":row.id,"owner_id":row.owner_id,"thread_id":row.thread_id,
            "source_turn_id":row.source_turn_id,"parent_id":row.parent_id,
            "object_ref":row.object_ref,"sha256":row.sha256,"size":row.size,
            "native_thread_id":row.native_thread_id,"state":row.state,
            "partial":row.partial,"verification":json.loads(row.verification_json or "{}"),
            "created_at":row.created_at,"updated_at":row.updated_at,
            "activated_at":row.activated_at}

    @staticmethod
    def _checkpoint_identity(thread_id,source_turn_id,object_ref,native_thread_id,partial):
        return canonical_digest(["checkpoint-version",thread_id,source_turn_id,object_ref,
            native_thread_id,"partial" if partial else "complete"])

    @staticmethod
    def _validate_checkpoint_values(object_ref,sha256,size,native_thread_id):
        if (not re.fullmatch(r"sha256:[0-9a-f]{64}",object_ref)
                or sha256!=object_ref.removeprefix("sha256:")
                or type(size) is not int or not 0<=size<=4*1024*1024*1024
                or not isinstance(native_thread_id,str) or len(native_thread_id)>300):
            raise ValueError("checkpoint_version_invalid")

    async def stage_checkpoint_version(self,thread_id:str,checkpoint_ref:str,
            native_thread_id:str,*,size:int,source_turn_id:str="",partial:bool=False,
            lease_owner:str|None=None,recovery_generation:int|None=None,
            require_idle:bool=False,expected_owner_id:str="")->dict:
        sha256=checkpoint_ref.removeprefix("sha256:")
        self._validate_checkpoint_values(checkpoint_ref,sha256,size,native_thread_id)
        identity=self._checkpoint_identity(thread_id,source_turn_id,checkpoint_ref,
            native_thread_id,partial)
        async with self.job_lock("checkpoint:"+thread_id):
            async with self.connections.begin() as db:
                thread=await db.get(ThreadRow,thread_id,with_for_update=True)
                if thread is None:raise KeyError("thread_not_found")
                if expected_owner_id and thread.owner_id!=expected_owner_id:
                    raise KeyError("thread_not_found")
                if lease_owner is not None:
                    turn=await db.get(TurnRow,source_turn_id,with_for_update=True)
                    self.require_lease_row(turn,lease_owner,recovery_generation)
                    if turn.thread_id!=thread_id:raise RuntimeError("stale_turn_lease")
                if require_idle and await db.scalar(select(TurnRow.id).where(
                        TurnRow.thread_id==thread_id,
                        TurnRow.state.not_in(["COMPLETED","WAITING_USER","FAILED",
                            "CANCELLED","PAUSED"])).limit(1)):
                    raise RuntimeError("active_turn_prevents_checkpoint")
                existing=await db.get(CheckpointVersionRow,identity,with_for_update=True)
                if existing:
                    if (existing.thread_id!=thread_id or existing.object_ref!=checkpoint_ref
                            or existing.native_thread_id!=native_thread_id
                            or existing.size!=size or existing.partial!=partial):
                        raise RuntimeError("checkpoint_version_identity_conflict")
                    return self.checkpoint_version_view(existing)
                parent=(await db.scalars(select(CheckpointVersionRow).where(
                    CheckpointVersionRow.thread_id==thread_id,
                    CheckpointVersionRow.state=="ACTIVE").with_for_update())).one_or_none()
                timestamp=now();row=CheckpointVersionRow(id=identity,owner_id=thread.owner_id,
                    thread_id=thread_id,source_turn_id=source_turn_id,parent_id=parent.id if parent else "",
                    object_ref=checkpoint_ref,sha256=sha256,size=size,
                    native_thread_id=native_thread_id,state="STAGING",partial=partial,
                    verification_json="{}",created_at=timestamp,updated_at=timestamp,
                    activated_at=None)
                db.add(row);await db.flush();return self.checkpoint_version_view(row)

    async def verify_checkpoint_version(self,version_id:str,verification:dict)->dict:
        if not isinstance(verification,dict):raise ValueError("checkpoint_verification_invalid")
        encoded=json.dumps(verification,sort_keys=True,separators=(",",":"))
        if len(encoded)>4000:raise ValueError("checkpoint_verification_invalid")
        async with self.connections.begin() as db:
            row=await db.get(CheckpointVersionRow,version_id,with_for_update=True)
            if row is None:raise KeyError("checkpoint_version_not_found")
            if row.state in {"VERIFIED","ACTIVE","SUPERSEDED"}:
                if row.verification_json!=encoded:
                    raise RuntimeError("checkpoint_verification_conflict")
                return self.checkpoint_version_view(row)
            if row.state!="STAGING":raise RuntimeError("checkpoint_version_not_staging")
            row.state="VERIFIED";row.verification_json=encoded;row.updated_at=now()
            return self.checkpoint_version_view(row)

    async def invalidate_checkpoint_version(self,version_id:str,error_code:str)->dict:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}",error_code):
            raise ValueError("checkpoint_error_code_invalid")
        async with self.connections.begin() as db:
            row=await db.get(CheckpointVersionRow,version_id,with_for_update=True)
            if row is None:raise KeyError("checkpoint_version_not_found")
            if row.state in {"ACTIVE","SUPERSEDED"}:
                raise RuntimeError("active_checkpoint_cannot_be_invalidated")
            row.state="INVALID";row.verification_json=json.dumps({"error_code":error_code},
                sort_keys=True,separators=(",",":"));row.updated_at=now()
            return self.checkpoint_version_view(row)

    async def activate_checkpoint_version(self,version_id:str,*,lease_owner:str|None=None,
            recovery_generation:int|None=None,retain:int=3,require_idle:bool=False,
            expected_owner_id:str="")->dict:
        if not 1<=retain<=100:raise ValueError("checkpoint_retention_invalid")
        async with self.connections() as lookup:
            value=await lookup.get(CheckpointVersionRow,version_id)
            if value is None:raise KeyError("checkpoint_version_not_found")
            thread_id=value.thread_id
        async with self.job_lock("checkpoint:"+thread_id):
            async with self.connections.begin() as db:
                row=await db.get(CheckpointVersionRow,version_id,with_for_update=True)
                thread=await db.get(ThreadRow,row.thread_id,with_for_update=True)
                if expected_owner_id and thread.owner_id!=expected_owner_id:
                    raise KeyError("thread_not_found")
                if row.state=="ACTIVE":return self.checkpoint_version_view(row)
                if row.state!="VERIFIED":raise RuntimeError("checkpoint_version_not_verified")
                if lease_owner is not None:
                    turn=await db.get(TurnRow,row.source_turn_id,with_for_update=True)
                    self.require_lease_row(turn,lease_owner,recovery_generation)
                    if turn.thread_id!=row.thread_id:raise RuntimeError("stale_turn_lease")
                if require_idle and await db.scalar(select(TurnRow.id).where(
                        TurnRow.thread_id==row.thread_id,
                        TurnRow.state.not_in(["COMPLETED","WAITING_USER","FAILED",
                            "CANCELLED","PAUSED"])).limit(1)):
                    raise RuntimeError("active_turn_prevents_checkpoint")
                active=list((await db.scalars(select(CheckpointVersionRow).where(
                    CheckpointVersionRow.thread_id==row.thread_id,
                    CheckpointVersionRow.state=="ACTIVE").with_for_update())).all())
                if len(active)>1:raise RuntimeError("checkpoint_active_conflict")
                timestamp=now()
                for previous in active:
                    previous.state="SUPERSEDED";previous.updated_at=timestamp
                row.state="ACTIVE";row.activated_at=timestamp;row.updated_at=timestamp
                thread.checkpoint_ref=row.object_ref;thread.native_thread_id=row.native_thread_id
                thread.updated_at=timestamp
                superseded=list((await db.scalars(select(CheckpointVersionRow).where(
                    CheckpointVersionRow.thread_id==row.thread_id,
                    CheckpointVersionRow.state=="SUPERSEDED").order_by(
                        CheckpointVersionRow.activated_at.desc(),
                        CheckpointVersionRow.created_at.desc()).with_for_update())).all())
                stale=superseded[max(0,retain-1):]
                if stale:await db.execute(delete(CheckpointVersionRow).where(
                    CheckpointVersionRow.id.in_([value.id for value in stale])))
                return self.checkpoint_version_view(row)

    async def list_checkpoint_versions(self,thread_id:str,owner_id:str|None=None)->list[dict]:
        async with self.connections() as db:
            query=select(CheckpointVersionRow).where(
                CheckpointVersionRow.thread_id==thread_id)
            if owner_id is not None:query=query.where(CheckpointVersionRow.owner_id==owner_id)
            rows=list((await db.scalars(query.order_by(
                CheckpointVersionRow.created_at.desc()))).all())
        return [self.checkpoint_version_view(row) for row in rows]

    async def expire_uncommitted_checkpoint_versions(self,max_age_seconds:int)->int:
        if not 60<=max_age_seconds<=365*86400:
            raise ValueError("checkpoint_expiry_invalid")
        cutoff=now()-timedelta(seconds=max_age_seconds);count=0
        async with self.connections.begin() as db:
            rows=list((await db.scalars(select(CheckpointVersionRow).where(
                CheckpointVersionRow.state.in_(["STAGING","VERIFIED"]),
                CheckpointVersionRow.updated_at<cutoff).with_for_update())).all())
            for row in rows:
                row.state="INVALID";row.updated_at=now()
                row.verification_json=json.dumps({"error_code":"checkpoint_staging_expired"},
                    sort_keys=True,separators=(",",":"));count+=1
        return count

    async def rollback_checkpoint_version(self,thread_id:str,failed_ref:str,
            error_code:str,*,source_turn_id:str="",lease_owner:str|None=None,
            recovery_generation:int|None=None)->dict|None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}",error_code):
            raise ValueError("checkpoint_error_code_invalid")
        async with self.job_lock("checkpoint:"+thread_id):
            async with self.connections.begin() as db:
                thread=await db.get(ThreadRow,thread_id,with_for_update=True)
                if thread is None:raise KeyError("thread_not_found")
                if lease_owner is not None:
                    turn=await db.get(TurnRow,source_turn_id,with_for_update=True)
                    self.require_lease_row(turn,lease_owner,recovery_generation)
                    if turn.thread_id!=thread_id:raise RuntimeError("stale_turn_lease")
                active=list((await db.scalars(select(CheckpointVersionRow).where(
                    CheckpointVersionRow.thread_id==thread_id,
                    CheckpointVersionRow.state=="ACTIVE").with_for_update())).all())
                if len(active)!=1 or active[0].object_ref!=failed_ref:
                    return None
                previous=(await db.scalars(select(CheckpointVersionRow).where(
                    CheckpointVersionRow.thread_id==thread_id,
                    CheckpointVersionRow.state=="SUPERSEDED").order_by(
                        CheckpointVersionRow.activated_at.desc(),
                        CheckpointVersionRow.created_at.desc()).limit(1).with_for_update()
                    )).one_or_none()
                if previous is None:return None
                timestamp=now();failed=active[0]
                try:verification=json.loads(failed.verification_json or "{}")
                except (TypeError,ValueError):verification={}
                failed.state="INVALID";failed.updated_at=timestamp
                failed.verification_json=json.dumps({**verification,
                    "invalid_error_code":error_code},sort_keys=True,separators=(",",":"))
                previous.state="ACTIVE";previous.updated_at=timestamp
                previous.activated_at=timestamp
                thread.checkpoint_ref=previous.object_ref
                thread.native_thread_id=previous.native_thread_id
                thread.updated_at=timestamp
                return self.checkpoint_version_view(previous)

    async def commit_checkpoint_version(self,thread_id:str,checkpoint_ref:str,
            native_thread_id:str,*,size:int,source_turn_id:str="",partial:bool=False,
            verification:dict|None=None,lease_owner:str|None=None,
            recovery_generation:int|None=None,require_idle:bool=False,
            expected_owner_id:str="")->dict:
        staged=await self.stage_checkpoint_version(thread_id,checkpoint_ref,native_thread_id,
            size=size,source_turn_id=source_turn_id,partial=partial,
            lease_owner=lease_owner,recovery_generation=recovery_generation,
            require_idle=require_idle,expected_owner_id=expected_owner_id)
        verified=await self.verify_checkpoint_version(staged['id'],verification or {
            'object_digest':'verified','archive':'runtime_checkpoint'})
        return await self.activate_checkpoint_version(verified['id'],
            lease_owner=lease_owner,recovery_generation=recovery_generation,
            require_idle=require_idle,expected_owner_id=expected_owner_id)
