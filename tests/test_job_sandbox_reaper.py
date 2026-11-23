from types import SimpleNamespace

from sandbox_harness.broker import Broker


async def test_orphan_reaper_preserves_sandbox_owned_by_nonterminal_job():
    sandbox=SimpleNamespace(id='sandbox-job',metadata={
        'sandbox-harness.io/turn':'job-live'})

    class Backend:
        def __init__(self):self.deleted=[]
        async def list_namespace(self):return [sandbox]
        async def delete(self,value):self.deleted.append(value.id)

    async def nothing():return set()
    async def live_jobs():return {'job-live'}
    async def job_sandboxes():return {sandbox.id}
    async def retry():return None

    backend=Backend();pool=SimpleNamespace(backend=backend,retry_deletions=retry,
        managed_sandbox_ids=nothing)
    store=SimpleNamespace(nonterminal_job_ids=live_jobs,nonterminal_turn_sandbox_ids=nothing,
        active_sandbox_control_ids=nothing,
        active_job_sandbox_ids=job_sandboxes)
    broker=SimpleNamespace(pool=pool,store=store,active_sandboxes={})

    await Broker.cleanup_startup_orphans(broker)

    assert backend.deleted==[]
