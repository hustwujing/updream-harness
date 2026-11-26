import asyncio,os
import pytest
from sandbox_harness.common.contracts.domain import (identifier)
from sandbox_harness.common.scheduling.scheduler import (NodeInventory, ResourceRequest, Scheduler)
from sandbox_harness.common.db.store import (Store)

POSTGRES_URL=os.environ.get('PARALLEL_TEST_POSTGRES_URL','')
pytestmark=pytest.mark.skipif(not POSTGRES_URL,reason='PARALLEL_TEST_POSTGRES_URL is not configured')

async def test_two_schedulers_cannot_overbook_same_node_capacity():
    primary=Store(POSTGRES_URL);await primary.initialize();stores=[Store(POSTGRES_URL) for _ in range(2)]
    try:
        node_id='node-'+identifier();scheduler=Scheduler(primary)
        await scheduler.register_node(NodeInventory(node_id=node_id,generation=1,cpu_total=2,
            memory_mb_total=2048,disk_mb_total=4096,profiles=[{'runtime_profile_id':'general-cpu',
                'harness':'codex','image_digest':'a'*64}]))
        request=ResourceRequest(child_profile_id='child-general',runtime_profile_id='general-cpu',
            harness='codex',image_digest='a'*64,cpu=2,memory_mb=1024,disk_mb=1024)
        async def reserve(store):
            try:return await Scheduler(store).reserve(identifier(),1,request)
            except RuntimeError as exc:return exc
        results=await asyncio.gather(*(reserve(store) for store in stores))
        successes=[value for value in results if not isinstance(value,Exception)]
        failures=[value for value in results if isinstance(value,Exception)]
        assert len(successes)==len(failures)==1
        assert str(failures[0])=='no_compatible_node_capacity'
    finally:
        await asyncio.gather(*(store.close() for store in stores));await primary.close()
