"""Restricted repository surface for sandbox-gateway."""
from .cross_domain import CrossDomainCommands


CORE_METHODS = frozenset({
    "authorize_capability", "revoke_capability", "reserve_model_tokens", "finalize_model_tokens",
    "get_artifact", "get_reviewed_job_artifact_for_thread", "get_job", "get_job_cleanup",
    "get_job_group", "get_job_group_draft", "list_jobs_for_group", "owner_id_for_thread",
    "turn_input_artifacts", "resolve_project_file_reference", "bind_job_artifact",
    "confirm_job_group_draft", "create_job_group", "create_job_group_draft",
    "get_job_authorization_context", "get_job_group_draft_request", "get_turn_for_owner",
    "reject_job_group_draft", "request_job_cancel", "request_job_group_cancel",
    "update_job_group_draft", "validate_job_input_artifacts",
    "persist_runtime_event", "update_model_activity", "publish_model_activity_progress",
    "resolve_review_candidates",
})
MEDIA_METHODS = frozenset({
    "get_media_job_for_thread", "media_job_output_for_thread", "request_media_cancel_for_thread",
})
SITE_METHODS = frozenset({"get_site_job_for_thread"})


class GatewayRepositories:
    def __init__(self, core, media, sites):
        self.core, self.media, self.sites = core, media, sites
        self.commands = CrossDomainCommands(core, media, sites)

    def __getattr__(self, name):
        if name in CORE_METHODS:return getattr(self.core,name)
        if name in MEDIA_METHODS:return getattr(self.media,name)
        if name in SITE_METHODS:return getattr(self.sites,name)
        raise AttributeError(name)

    async def initialize(self):
        await self.core.initialize();await self.media.initialize();await self.sites.initialize()

    async def close(self):
        await self.core.close();await self.media.close();await self.sites.close()

    async def healthcheck(self):
        return all([await self.core.healthcheck(),await self.media.healthcheck(),await self.sites.healthcheck()])

    async def create_media_draft(self,*args,**kwargs):
        return await self.commands.create_media_draft(*args,**kwargs)

    async def seal_media_batch(self,*args,**kwargs):
        return await self.commands.seal_media_batch(*args,**kwargs)

    async def create_site_draft(self,*args,**kwargs):
        return await self.commands.create_site_draft(*args,**kwargs)

    async def promote_media_job_for_thread(self,*args,**kwargs):
        return await self.commands.promote_media_job_for_thread(*args,**kwargs)

    async def relay_pending(self,limit=100):
        return await self.commands.relay_pending(limit)
