from types import SimpleNamespace

from sandbox_harness.services.api.broker import (Broker)
from sandbox_harness.common.contracts.sandbox_contracts import (SandboxHandle)


async def test_orphan_reaper_preserves_sandbox_owned_by_nonterminal_job():
    sandbox=SimpleNamespace(id='sandbox-job',handle=SandboxHandle(provider='fake',
        sandbox_id='sandbox-job',allocation_id='job-live',profile_key='a'*63,
        revision='revision-1'),metadata={
        'sandbox-harness.io/turn':'job-live'})

    class Backend:
        def __init__(self):self.deleted=[]
        async def delete(self,value):self.deleted.append(value.id)

    async def nothing():return set()
    async def live_jobs():return {'job-live'}
    async def job_sandboxes():return {sandbox.id}
    async def retry():return None
    async def owned():return [sandbox]
    async def managed_keys():return set()
    async def active_keys():return {('fake',sandbox.id)}

    backend=Backend();pool=SimpleNamespace(backend=backend,retry_deletions=retry,
        managed_sandbox_ids=nothing,managed_sandbox_keys=managed_keys,
        sandbox_key=lambda value:(value.handle.provider,value.handle.sandbox_id),list_owned=owned)
    store=SimpleNamespace(nonterminal_job_ids=live_jobs,nonterminal_turn_sandbox_ids=nothing,
        active_sandbox_control_ids=nothing,
        active_job_sandbox_ids=job_sandboxes,active_sandbox_keys=active_keys)
    broker=SimpleNamespace(pool=pool,store=store,active_sandboxes={})

    await Broker.cleanup_startup_orphans(broker)

    assert backend.deleted==[]
