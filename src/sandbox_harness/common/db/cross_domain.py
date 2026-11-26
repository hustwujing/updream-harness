"""Typed API/Gateway commands whose source authority is core."""
from sandbox_harness.common.contracts.domain import TERMINAL
from .models import ThreadRow, TurnRow, canonical_digest


class CrossDomainCommands:
    def __init__(self, core, media, sites):
        self.core, self.media, self.sites = core, media, sites

    async def relay_pending(self, limit=100):
        return await self.core.relay_remote(
            {"media":self.media.handoff,"sites":self.sites.handoff},limit=limit)

    async def _turn_scope(self, db, owner_id, thread_id, turn_id):
        version = await self.core.lifecycle.admit(db, owner_id, thread_id)
        thread = await db.get(ThreadRow, thread_id, with_for_update=True)
        turn = await db.get(TurnRow, turn_id, with_for_update=True)
        if not thread or thread.owner_id != owner_id: raise KeyError("thread_not_found")
        if not turn or turn.thread_id != thread_id: raise KeyError("turn_not_found")
        if turn.state in {state.value for state in TERMINAL}: raise RuntimeError("turn_terminal")
        return version

    async def create_media_draft(self, owner_id, thread_id, value, *, expires_at,
                                 provider_spec, reference_snapshots=None):
        command_id=canonical_digest(["media.draft",thread_id,value.idempotency_key])
        draft_id=canonical_digest([command_id,"draft"])[:32]
        async with self.core.transaction() as db:
            version=await self._turn_scope(db,owner_id,thread_id,value.turn_id)
            payload={"owner_id":owner_id,"thread_id":thread_id,"turn_id":value.turn_id,
                "owner_generation":version.owner_generation,"thread_generation":version.thread_generation,
                "draft_id":draft_id,"kind":value.kind,"model":value.model,
                "generation_mode":value.generation_mode or "","parameters":value.parameters,
                "references":reference_snapshots or [],"idempotency_key":value.idempotency_key,
                "output_role":value.output_role,"expires_at":expires_at.isoformat()}
            await self.core.enqueue_remote(db,command_id=command_id,target="media",
                kind="media.turn.draft",aggregate_id=canonical_digest([owner_id,thread_id]),
                owner_id=owner_id,thread_id=thread_id,generation=version.thread_generation,payload=payload)
        await self.core.relay_remote({"media":self.media.handoff})
        draft=await self.media.get_media_draft(draft_id,owner_id)
        if not draft:raise RuntimeError("media_draft_delivery_pending")
        return draft,True

    async def seal_media_batch(self, turn_id, thread_id, owner_id):
        command_id=canonical_digest(["media.seal",thread_id,turn_id])
        async with self.core.transaction() as db:
            version=await self._turn_scope(db,owner_id,thread_id,turn_id)
            payload={"owner_id":owner_id,"thread_id":thread_id,"turn_id":turn_id,
                "owner_generation":version.owner_generation,"thread_generation":version.thread_generation}
            await self.core.enqueue_remote(db,command_id=command_id,target="media",
                kind="media.turn.seal",aggregate_id=canonical_digest([owner_id,thread_id]),
                owner_id=owner_id,thread_id=thread_id,generation=version.thread_generation,payload=payload)
        await self.core.relay_remote({"media":self.media.handoff})
        result=await self.media.media_batch_status(turn_id,owner_id)
        if not result:raise RuntimeError("media_batch_delivery_pending")
        return result

    async def create_site_draft(self, owner_id, thread_id, turn_id, **values):
        command_id=canonical_digest(["site.draft",thread_id,values["idempotency_key"]])
        draft_id=canonical_digest([command_id,"draft"])[:32]
        project_id=canonical_digest([command_id,"project"])[:32]
        expires_at=values["expires_at"];requested_slug=values["slug"]
        payload={key:value for key,value in values.items() if key not in {"expires_at","slug"}}
        async with self.core.transaction() as db:
            version=await self._turn_scope(db,owner_id,thread_id,turn_id)
            payload.update({"owner_id":owner_id,"thread_id":thread_id,"turn_id":turn_id,
                "owner_generation":version.owner_generation,"thread_generation":version.thread_generation,
                "draft_id":draft_id,"project_id":project_id,"requested_slug":requested_slug,
                "expires_at":expires_at.isoformat()})
            await self.core.enqueue_remote(db,command_id=command_id,target="sites",
                kind="site.turn.draft",aggregate_id=canonical_digest([owner_id,thread_id]),
                owner_id=owner_id,thread_id=thread_id,generation=version.thread_generation,payload=payload)
        await self.core.relay_remote({"sites":self.sites.handoff})
        draft=await self.sites.get_site_draft(draft_id,owner_id)
        if not draft:raise RuntimeError("site_draft_delivery_pending")
        return draft,True

    async def promote_media_job_for_thread(self, job_id, thread_id, role):
        if role not in {"workspace","delivery"}:raise ValueError("media_output_role_invalid")
        manifest=await self.media.promotion_manifest(job_id,thread_id)
        command_id=canonical_digest(["media.promote",job_id,role])
        async with self.core.transaction() as db:
            thread=await db.get(ThreadRow,thread_id,with_for_update=True)
            if not thread:raise KeyError("thread_not_found")
            version=await self.core.lifecycle.admit(db,thread.owner_id,thread_id)
            promoted=[];primary_path=""
            for item in manifest["outputs"]:
                path=f"artifacts/3d/{job_id}/{item['filename']}"
                project,_=await self.core._register_project_file_locked(db,thread_id,path=path,
                    source_type="artifact",source_turn_id=manifest["turn_id"],sha256=item["sha256"],
                    size=item["size"],mime=item["mime"],object_ref=item["object_ref"])
                promoted.append({**item,"project_file_id":project.id})
                if item["role"]=="primary":primary_path=project.path
            if not primary_path:raise RuntimeError("media_job_output_invalid")
            payload={"job_id":job_id,"thread_id":thread_id,"output_role":role,
                "project_path":primary_path,"outputs":promoted}
            await self.core.enqueue_remote(db,command_id=command_id,target="media",
                kind="media.promotion.ack",aggregate_id=job_id,owner_id=thread.owner_id,
                thread_id=thread_id,generation=version.thread_generation,payload=payload)
        await self.core.relay_remote({"media":self.media.handoff})
        return {"job_id":job_id,"output_role":role,"project_path":primary_path}
