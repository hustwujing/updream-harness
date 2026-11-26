"""Restricted repository surface for the API service."""
from __future__ import annotations

from .accounts import AccountsRepository
from .core import CoreRepository
from .cross_domain import CrossDomainCommands
from .media_repository import MediaRepository
from .site_repository import SiteRepository
from .lifecycle import LifecycleProposal
from .models import canonical_digest


ACCOUNT_METHODS = frozenset({
    "create_auth_session", "delete_auth_session", "get_user", "get_user_avatar",
    "get_user_by_username", "guest_owner_available", "rehash_password",
    "registration_replay_user", "resolve_auth_session", "set_user_avatar",
    "update_account", "update_password", "update_username",
})

MEDIA_METHODS = frozenset({
    "cancel_media_batch", "cancel_media_draft", "claim_media_batches",
    "confirm_media_batch", "confirm_media_draft", "create_media_reference_upload",
    "ensure_media_reference_asset", "finish_media_batch", "finish_media_reference_upload",
    "get_media_draft", "get_media_draft_snapshot", "get_media_job",
    "get_media_reference_upload", "list_media_drafts", "list_media_drafts_for_turn",
    "list_media_jobs", "media_continuation_group", "request_media_cancel",
    "request_media_refresh", "update_media_draft",
})

SITE_METHODS = frozenset({
    "activate_site_version", "cancel_site_draft", "confirm_site_draft",
    "get_site_draft_file", "get_site_job", "list_site_drafts", "list_site_jobs",
    "list_site_projects", "public_site_file", "retry_site_job", "site_draft_by_bundle",
    "unpublish_site",
})


class ApiRepositories:
    def __init__(self, accounts: AccountsRepository, core: CoreRepository,
                 media: MediaRepository, sites: SiteRepository):
        self.accounts, self.core, self.media, self.sites = accounts, core, media, sites
        self.commands = CrossDomainCommands(core, media, sites)

    @property
    def events(self):
        return self.core.events

    def __getattr__(self, name):
        if name in ACCOUNT_METHODS:
            return getattr(self.accounts, name)
        if name in MEDIA_METHODS:
            return getattr(self.media, name)
        if name in SITE_METHODS:
            return getattr(self.sites, name)
        if not name.startswith("_") and hasattr(type(self.core), name):
            return getattr(self.core, name)
        raise AttributeError(name)

    async def initialize(self):
        for repository in (self.accounts, self.core, self.media, self.sites):
            await repository.initialize()

    async def close(self):
        for repository in reversed((self.accounts, self.core, self.media, self.sites)):
            await repository.close()

    async def healthcheck(self):
        return all([
            await self.accounts.healthcheck(), await self.core.healthcheck(),
            await self.media.healthcheck(), await self.sites.healthcheck(),
        ])

    async def create_media_draft(self, *args, **kwargs):
        return await self.commands.create_media_draft(*args, **kwargs)

    async def seal_media_batch(self, *args, **kwargs):
        return await self.commands.seal_media_batch(*args, **kwargs)

    async def create_site_draft(self, *args, **kwargs):
        return await self.commands.create_site_draft(*args, **kwargs)

    async def promote_media_job_for_thread(self, *args, **kwargs):
        return await self.commands.promote_media_job_for_thread(*args, **kwargs)

    async def relay_pending(self, limit=100):
        return await self.commands.relay_pending(limit)

    @property
    def lifecycle_targets(self):
        return {"accounts": self.accounts.handoff, "media": self.media.handoff,
                "sites": self.sites.handoff}

    async def advance_lifecycle_operations(self, limit=100):
        await self.accounts.handoff.relay({"core": self.core.handoff}, limit=limit)
        await self.media.handoff.relay({"core":self.core.handoff},limit=limit)
        await self.core.handoff.relay(self.lifecycle_targets, limit=limit)
        results = []
        for operation_id in await self.core.pending_lifecycle_operations(limit):
            results.append(await self.core.coordinator.advance(operation_id))
        await self.core.handoff.relay(self.lifecycle_targets, limit=limit)
        return results

    async def drive_lifecycle(self, operation_id, rounds=6):
        for _ in range(rounds):
            await self.advance_lifecycle_operations()
            try:
                result = await self.core.coordinator.status(operation_id)
            except KeyError:
                continue
            if result.state in {"completed", "refused"}:
                return result
        return await self.core.coordinator.status(operation_id)

    async def begin_thread_lifecycle(self, thread_id, owner_id, action):
        target = {"archive":"archived", "unarchive":"open", "delete":"deleted"}.get(action)
        if target is None:
            raise ValueError("lifecycle_action_unsupported")
        version = await self.core.lifecycle_version(owner_id, thread_id)
        operation_id = canonical_digest([
            "thread-lifecycle", owner_id, thread_id, action, version.thread_generation])
        proposal = LifecycleProposal(operation_id, owner_id, thread_id,
            version.thread_generation, action, target, owner_generation=version.owner_generation)
        result = await self.core.coordinator.start(proposal, {"media", "sites"})
        return result if result.state != "pending" else await self.drive_lifecycle(operation_id)

    async def lifecycle_status(self, operation_id, owner_id):
        return await self.core.coordinator.status(operation_id, owner_id)

    async def begin_registration(self, operation_id, owner_id, username, password_hash, *,
                                 session_token_hash, session_expires_at):
        version = await self.core.lifecycle_version(owner_id, "")
        await self.accounts.begin_registration(operation_id, owner_id, username, password_hash,
            session_token_hash=session_token_hash, session_expires_at=session_expires_at,
            owner_generation=version.owner_generation)
        await self.drive_lifecycle(operation_id)
        return await self.accounts.registration_result(operation_id, owner_id)
