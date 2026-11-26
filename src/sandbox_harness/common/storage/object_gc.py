"""Reference-driven object mark-and-sweep with grace and second checks."""
from __future__ import annotations

import asyncio
import inspect
import json
from datetime import datetime, timezone

from sqlalchemy import select


async def authoritative_object_references(repositories)->set[str]:
    from sandbox_harness.common.db.account_models import UserRow
    from sandbox_harness.common.db.models import (
        ArtifactRow, JobArtifactRow, MediaDraftRow, MediaJobOutputRow, MediaJobRow,
        MediaReferenceAssetRow, MediaVoiceCloneRow, ProjectFileRow, SiteDraftFileRow,
        SiteDraftRow, SiteFileRow, ThreadRow, TurnProjectFileRow, UploadRow,
    )
    result=set()
    async with repositories.accounts.connections() as db:
        result.update(value for value in (await db.scalars(select(
            UserRow.avatar_object_ref).where(UserRow.avatar_object_ref!=""))).all() if value)
    async with repositories.core.connections() as db:
        for model in (ArtifactRow,UploadRow,ProjectFileRow,TurnProjectFileRow,JobArtifactRow):
            result.update(value for value in (await db.scalars(select(model.object_ref).where(
                model.object_ref!=""))).all() if value)
        result.update(value for value in (await db.scalars(select(ThreadRow.checkpoint_ref).where(
            ThreadRow.checkpoint_ref!=""))).all() if value)
    async with repositories.media.connections() as db:
        for model in (MediaReferenceAssetRow,MediaVoiceCloneRow,MediaJobOutputRow):
            result.update(value for value in (await db.scalars(select(model.object_ref).where(
                model.object_ref!=""))).all() if value)
        result.update(value for value in (await db.scalars(select(MediaJobRow.output_object_ref).where(
            MediaJobRow.output_object_ref!=""))).all() if value)
        for encoded in (await db.scalars(select(MediaDraftRow.references_json))).all():
            try:references=json.loads(encoded or "[]")
            except (TypeError,ValueError):references=[]
            result.update(str(item.get("object_ref")) for item in references
                if isinstance(item,dict) and item.get("object_ref"))
    async with repositories.sites.connections() as db:
        for model in (SiteDraftFileRow,SiteFileRow):
            result.update(value for value in (await db.scalars(select(model.object_ref).where(
                model.object_ref!=""))).all() if value)
        result.update(value for value in (await db.scalars(select(SiteDraftRow.archive_object_ref).where(
            SiteDraftRow.archive_object_ref!=""))).all() if value)
    return result


class ObjectGarbageCollector:
    def __init__(self,objects,reference_loader,*,audit=None):
        self.objects=objects;self.reference_loader=reference_loader;self.audit=audit

    async def _references(self)->set[str]:
        value=self.reference_loader()
        if inspect.isawaitable(value):value=await value
        if not isinstance(value,set) or any(not isinstance(item,str) for item in value):
            raise ValueError("object_reference_snapshot_invalid")
        return value

    async def _audit(self,value):
        if not self.audit:return
        result=self.audit(value)
        if inspect.isawaitable(result):await result

    async def run(self,*,grace_seconds:int,dry_run:bool,limit:int=1000)->dict:
        if not 1<=grace_seconds<=365*86400 or not 1<=limit<=10000:
            raise ValueError("object_gc_limits_invalid")
        marked=await self._references();now=datetime.now(timezone.utc)
        objects=await asyncio.to_thread(self.objects.list_objects)
        candidates=[value for value in objects if value["object_ref"] not in marked
            and (now-value["modified_at"]).total_seconds()>=grace_seconds][:limit]
        deleted=0;rescued=0;bytes_deleted=0
        if not dry_run:
            for value in candidates:
                if value["object_ref"] in await self._references():
                    rescued+=1;continue
                await asyncio.to_thread(self.objects.delete,value["object_ref"])
                deleted+=1;bytes_deleted+=value["size"]
                await self._audit({"object_ref":value["object_ref"],"size":value["size"],
                    "reason":"unreferenced_after_grace"})
        return {"marked_count":len(marked),"object_count":len(objects),
            "candidate_count":len(candidates),"deleted_count":deleted,
            "rescued_count":rescued,"bytes_deleted":bytes_deleted,"dry_run":dry_run}
