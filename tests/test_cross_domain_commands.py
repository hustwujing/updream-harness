from datetime import timedelta

from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.contracts.domain import (
    Harness, MediaDraftCreate, ThreadCreate, TurnCreate, TurnState, now)


async def setup(tmp_path):
    from sandbox_harness.common.db.core import CoreRepository
    from sandbox_harness.common.db.cross_domain import CrossDomainCommands
    from sandbox_harness.common.db.media_repository import MediaRepository
    from sandbox_harness.common.db.site_repository import SiteRepository
    core=CoreRepository("sqlite+aiosqlite:///"+str(tmp_path/"core.db"),namespace="cross-domain")
    media=MediaRepository("sqlite+aiosqlite:///"+str(tmp_path/"media.db"))
    sites=SiteRepository("sqlite+aiosqlite:///"+str(tmp_path/"sites.db"))
    for repository in (core,media,sites):await repository.initialize()
    profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
    thread=await core.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,title="cross"),
                                    profile,"owner")
    turn,_,_=await core.create_turn(thread.id,TurnCreate(message="build",idempotency_key="cross-turn"),
                                   profile,"owner")
    return core,media,sites,CrossDomainCommands(core,media,sites),thread,turn


async def test_media_draft_and_terminal_seal_cross_databases(tmp_path):
    core,media,sites,commands,thread,turn=await setup(tmp_path)
    value=MediaDraftCreate(kind="image",model="image/model",generation_mode="text_to_image",
        parameters={"prompt":"opaque"},references=[],idempotency_key="media-draft-key",
        turn_id=turn.id,output_role="candidate")
    spec=type("Spec",(),{"provider":"provider","adapter":"image_api",
                         "execution":"async","cancellation":"supported"})()
    try:
        first,_=await commands.create_media_draft("owner",thread.id,value,
            expires_at=now()+timedelta(hours=1),provider_spec=spec)
        second,_=await commands.create_media_draft("owner",thread.id,value,
            expires_at=first.expires_at,provider_spec=spec)
        assert first.id==second.id
        await core.set_turn_state(turn.id,TurnState.COMPLETED)
        await commands.relay_pending()
        assert (await media.media_batch_status(turn.id,"owner"))["status"]=="sealed"
        assert await media.claim_media_batches("dispatcher")==[]
    finally:
        for repository in (core,media,sites):await repository.close()


async def test_resume_turn_wakes_paused_media_batch_through_durable_command(tmp_path):
    core,media,sites,commands,thread,turn=await setup(tmp_path)
    value=MediaDraftCreate(kind="image",model="image/model",generation_mode="text_to_image",
        parameters={"prompt":"opaque"},references=[],idempotency_key="resume-media-draft",
        turn_id=turn.id,output_role="candidate")
    spec=type("Spec",(),{"provider":"provider","adapter":"image_api",
                         "execution":"async","cancellation":"supported"})()
    try:
        draft,_=await commands.create_media_draft("owner",thread.id,value,
            expires_at=now()+timedelta(hours=1),provider_spec=spec)
        job=await media.confirm_media_draft(draft.id,"owner",provider_spec=spec)
        claimed=(await media.claim_media_jobs("worker"))[0]
        await media.update_media_job(job.id,"worker",claimed["generation"],status="failed",
            phase="terminal",error_code="provider_processing_failed")
        await core.set_turn_state(turn.id,TurnState.COMPLETED);await commands.relay_pending()
        batch=(await media.claim_media_batches("dispatcher"))[0]
        await media.finish_media_batch(batch["turn_id"],"dispatcher",delivered=False,
            error="thread_paused",waiting_thread_resume=True)
        assert (await media.media_batch_status(turn.id,"owner"))["status"]=="waiting_thread_resume"
        await core.request_thread_pause(thread.id,"owner")
        await core.resume_thread(thread.id,"owner")
        profile=HarnessCatalog().resolve("codex","gpt-5.6","standard")
        resumed,_,_=await core.create_turn(thread.id,TurnCreate(message="继续",
            idempotency_key="resume-wake-turn"),profile,"owner")
        assert "[pause_resume_context]" in resumed.runtime_message
        await commands.relay_pending()
        assert (await media.media_batch_status(turn.id,"owner"))["status"]=="sealed"
        assert len(await media.claim_media_batches("dispatcher-two"))==1
    finally:
        for repository in (core,media,sites):await repository.close()


async def test_site_draft_cross_database_is_idempotent(tmp_path):
    core,media,sites,commands,thread,turn=await setup(tmp_path)
    values={"name":"Demo","slug":"demo","idempotency_key":"site-draft-key",
        "archive_object_ref":"sha256:"+"a"*64,"bundle_sha256":"a"*64,"archive_size":20,
        "file_count":1,"total_size":12,"expires_at":now()+timedelta(hours=1),
        "public_base_path":"/sites","files":[{"path":"index.html","sha256":"b"*64,
            "size":12,"mime":"text/html","object_ref":"sha256:"+"b"*64}]}
    try:
        first,_=await commands.create_site_draft("owner",thread.id,turn.id,**values)
        second,_=await commands.create_site_draft("owner",thread.id,turn.id,**values)
        assert first.id==second.id and first.project_id==second.project_id
        assert (await sites.get_site_draft_file(first.id,"index.html","owner"))["sha256"]=="b"*64
    finally:
        for repository in (core,media,sites):await repository.close()


async def test_media_promotion_commits_core_file_then_acknowledges_media(tmp_path):
    core,media,sites,commands,thread,turn=await setup(tmp_path)
    value=MediaDraftCreate(kind="image",model="image/model",generation_mode="text_to_image",
        parameters={"prompt":"opaque"},references=[],idempotency_key="promotion-draft",
        turn_id=turn.id,output_role="candidate")
    spec=type("Spec",(),{"provider":"provider","adapter":"image_api",
                         "execution":"async","cancellation":"supported"})()
    try:
        draft,_=await commands.create_media_draft("owner",thread.id,value,
            expires_at=now()+timedelta(hours=1),provider_spec=spec)
        job=await media.confirm_media_draft(draft.id,"owner",provider_spec=spec)
        claimed=(await media.claim_media_jobs("worker"))[0]
        await media.complete_media_job(job.id,"worker",provider_job_id="provider-job",
            outputs=[{"role":"primary","filename":"result.png","object_ref":"sha256:"+"c"*64,
                "mime":"image/png","size":12}],generation=claimed["generation"])
        result=await commands.promote_media_job_for_thread(job.id,thread.id,"delivery")
        assert result["project_path"]==f"artifacts/media/{job.id}.png"
        assert (await core.current_project_files(thread.id,"owner"))[0].path==result["project_path"]
        refreshed=await media.get_media_job(job.id,"owner")
        assert refreshed.output_role=="delivery" and refreshed.outputs[0].project_file_id
    finally:
        for repository in (core,media,sites):await repository.close()


async def test_thread_delete_commits_across_core_media_and_sites(tmp_path):
    from sandbox_harness.common.db.lifecycle import LifecycleProposal
    core,media,sites,commands,thread,turn=await setup(tmp_path)
    value=MediaDraftCreate(kind="image",model="image/model",generation_mode="text_to_image",
        parameters={"prompt":"opaque"},references=[],idempotency_key="delete-media-draft",
        turn_id=turn.id,output_role="candidate")
    spec=type("Spec",(),{"provider":"provider","adapter":"image_api",
                         "execution":"async","cancellation":"supported"})()
    try:
        draft,_=await commands.create_media_draft("owner",thread.id,value,
            expires_at=now()+timedelta(hours=1),provider_spec=spec)
        await media.cancel_media_draft(draft.id,"owner")
        site_values={"name":"Delete","slug":"delete","idempotency_key":"delete-site-draft",
            "archive_object_ref":"sha256:"+"a"*64,"bundle_sha256":"a"*64,"archive_size":20,
            "file_count":1,"total_size":12,"expires_at":now()+timedelta(hours=1),
            "public_base_path":"/sites","files":[{"path":"index.html","sha256":"b"*64,
                "size":12,"mime":"text/html","object_ref":"sha256:"+"b"*64}]}
        site_draft,_=await commands.create_site_draft("owner",thread.id,turn.id,**site_values)
        await sites.cancel_site_draft(site_draft.id,"owner")
        await core.set_turn_state(turn.id,TurnState.COMPLETED)
        await commands.relay_pending()
        claimed=await media.claim_media_batches("continuation")
        assert claimed and claimed[0]["turn_id"]==turn.id
        await media.finish_media_batch(turn.id,"continuation",delivered=True)
        version=await core.lifecycle_version("owner",thread.id)
        proposal=LifecycleProposal("delete-cross-domains","owner",thread.id,
            version.thread_generation,"delete","deleted",version.owner_generation)
        result=await core.coordinator.start(proposal,{"media","sites"})
        for _ in range(3):
            await core.handoff.relay({"media":media.handoff,"sites":sites.handoff})
            result=await core.coordinator.advance(proposal.operation_id)
        assert result.state=="completed"
        assert await core.get_thread(thread.id,"owner") is None
        assert await media.get_media_draft(draft.id,"owner") is None
        assert await sites.get_site_draft(site_draft.id,"owner") is None
    finally:
        for repository in (core,media,sites):await repository.close()
