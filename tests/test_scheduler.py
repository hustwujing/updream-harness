import pytest
from sqlalchemy import update

from sandbox_harness.common.scheduling.scheduler import (NodeInventory, ResourceRequest, Scheduler)
from sandbox_harness.common.contracts.job_contracts import (JobGroupCreate, JobKind, JobRequest, JobState)
from test_support.legacy_store import (ResourceReservationRow, Store)
from sandbox_harness.common.contracts.domain import (now)
from datetime import timedelta


def node(node_id='node-a',generation=1,load=.2,warm=1,gpus=()):
    return NodeInventory(node_id=node_id,generation=generation,cpu_total=8,memory_mb_total=16000,
        disk_mb_total=100000,load=load,profiles=[{'runtime_profile_id':'media-cpu','harness':'codex',
            'image_digest':'a'*64,'warm_slots':warm}],gpus=gpus,cached_bundle_digests=('b'*64,))


def request(**updates):
    value=dict(child_profile_id='child-general',runtime_profile_id='media-cpu',harness='codex',
        image_digest='a'*64,cpu=2,memory_mb=2000,disk_mb=1000,required_bundle_digests=('b'*64,))
    value.update(updates);return ResourceRequest(**value)


async def setup(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'scheduler.db'));await store.initialize()
    return store,Scheduler(store)


async def test_scheduler_hard_filters_profile_image_and_capacity_then_scores_warm_cache_load(tmp_path):
    store,scheduler=await setup(tmp_path)
    try:
        await scheduler.register_node(node('cold',load=.1,warm=0))
        await scheduler.register_node(node('warm',load=.8,warm=1))
        selected=await scheduler.reserve('1'*32,1,request())
        assert selected.node_id=='warm'
        with pytest.raises(RuntimeError,match='no_compatible'):
            await scheduler.reserve('2'*32,1,request(image_digest='c'*64))
        with pytest.raises(RuntimeError,match='no_compatible'):
            await scheduler.reserve('3'*32,1,request(cpu=20))
    finally:await store.close()


async def test_scheduler_allocates_exact_gpu_count_type_mode_memory_without_double_booking(tmp_path):
    store,scheduler=await setup(tmp_path)
    try:
        gpus=[{'id':'gpu-0','type':'A100','memory_mb':40960,'mode':'exclusive'},
            {'id':'gpu-1','type':'A100','memory_mb':40960,'mode':'exclusive'},
            {'id':'mig-0','type':'A100','memory_mb':10000,'mode':'mig'}]
        await scheduler.register_node(node(gpus=gpus))
        first=await scheduler.reserve('1'*32,1,request(gpu_count=2,gpu_type='A100',
            gpu_mode='exclusive',gpu_memory_mb=20000))
        assert first.gpu_ids==('gpu-0','gpu-1')
        with pytest.raises(RuntimeError,match='no_compatible'):
            await scheduler.reserve('2'*32,1,request(gpu_count=1,gpu_type='A100',
                gpu_mode='exclusive',gpu_memory_mb=20000))
        assert await scheduler.release(first.id,first.node_id,first.node_generation)
        second=await scheduler.reserve('2'*32,1,request(gpu_count=1,gpu_type='A100',
            gpu_mode='exclusive',gpu_memory_mb=20000))
        assert second.gpu_ids==('gpu-0',)
    finally:await store.close()


async def test_scheduler_selects_gpu_by_live_utilization_then_free_memory(tmp_path):
    store,scheduler=await setup(tmp_path)
    try:
        await scheduler.register_node(node(gpus=[
            {'id':'gpu-0','type':'RTX 4090','memory_mb':49140,'memory_free_mb':44000,
                'utilization_percent':35,'mode':'shared'},
            {'id':'gpu-1','type':'RTX 4090','memory_mb':49140,'memory_free_mb':18000,
                'utilization_percent':0,'mode':'shared'},
            {'id':'gpu-2','type':'RTX 4090','memory_mb':49140,'memory_free_mb':48000,
                'utilization_percent':0,'mode':'shared'},
        ]))
        selected=await scheduler.reserve('1'*32,1,request(gpu_count=1,
            gpu_memory_mb=16000))
        assert selected.gpu_ids==('gpu-2',)
        await scheduler.release(selected.id,selected.node_id,selected.node_generation)
        with pytest.raises(RuntimeError,match='no_compatible'):
            await scheduler.reserve('2'*32,1,request(gpu_count=1,gpu_memory_mb=49000))
    finally:await store.close()


async def test_scheduler_fences_stale_node_generation_and_reservation_release(tmp_path):
    store,scheduler=await setup(tmp_path)
    try:
        await scheduler.register_node(node(generation=2))
        with pytest.raises(RuntimeError,match='stale_node_generation'):
            await scheduler.register_node(node(generation=1))
        reservation=await scheduler.reserve('1'*32,1,request())
        with pytest.raises(RuntimeError,match='stale_resource'):
            await scheduler.mark_running(reservation.id,reservation.node_id,1)
        running=await scheduler.mark_running(reservation.id,reservation.node_id,2)
        assert running.state=='running'
        assert await scheduler.release(running.id,running.node_id,2)
        assert not await scheduler.release(running.id,running.node_id,2)
    finally:await store.close()


async def test_scheduler_reacquires_released_reservation_for_same_attempt(tmp_path):
    store,scheduler=await setup(tmp_path)
    try:
        await scheduler.register_node(node(generation=2))
        first=await scheduler.reserve('1'*32,1,request())
        assert await scheduler.release(first.id,first.node_id,first.node_generation)
        assert await scheduler.reservation_released(first.id,first.node_id,first.node_generation,
            job_id='1'*32,job_generation=1)
        for job_id,generation in [('2'*32,1),('1'*32,2)]:
            with pytest.raises(RuntimeError,match='stale_resource_reservation'):
                await scheduler.reservation_released(first.id,first.node_id,first.node_generation,
                    job_id=job_id,job_generation=generation)

        second=await scheduler.reserve('1'*32,1,request())

        assert second.id==first.id
        assert second.state=='reserved'
        assert not await scheduler.reservation_released(second.id,second.node_id,second.node_generation,
            job_id='1'*32,job_generation=1)
        async with scheduler.hold(second.id,second.node_id,second.node_generation):
            assert (await scheduler.mark_running(
                second.id,second.node_id,second.node_generation)).state=='running'
    finally:await store.close()


async def test_scheduler_reaps_only_expired_terminal_stale_generation_without_live_sandbox(tmp_path):
    store,scheduler=await setup(tmp_path)
    try:
        await scheduler.register_node(node(generation=1))
        jobs=[]
        for suffix in ("absent","live"):
            _,created=await store.create_job_group("owner",JobGroupCreate(thread_id="b"*32,
                origin_turn_id="c"*32,idempotency_key=f"stale-{suffix}-group",jobs=[JobRequest(
                    local_job_id=suffix,kind=JobKind.OPERATION,adapter="fake",descriptor_id="fake",
                    descriptor_version="1",descriptor_digest="a"*64,
                    idempotency_key=f"stale-{suffix}-job")]))
            job=created[0];reservation=await scheduler.reserve(job.id,1,request())
            attempt=await store.claim_job(job.id,"worker","node-a",30)
            await store.transition_job(job.id,JobState.FAILED,expected_generation=attempt.generation,
                result={"error_code":"fixture"})
            async with store.connections.begin() as db:
                await db.execute(update(ResourceReservationRow).where(
                    ResourceReservationRow.id==reservation.id).values(expires_at=now()-timedelta(seconds=1)))
            jobs.append((job,reservation))
        await scheduler.register_node(node(generation=2))
        released=await scheduler.reap_stale_reservations("node-a",2,{jobs[1][0].id})
        assert released==[jobs[0][1].id]
        async with store.connections() as db:
            first=await db.get(ResourceReservationRow,jobs[0][1].id)
            second=await db.get(ResourceReservationRow,jobs[1][1].id)
        assert first.state=="released" and second.state=="reserved"
    finally:await store.close()


async def test_stale_reservation_reaper_cannot_erase_unknown_submission_without_cleanup(tmp_path):
    from test_support.legacy_store import (JobRow, JobAttemptRow)
    store,scheduler=await setup(tmp_path)
    try:
        await scheduler.register_node(node(generation=1))
        _,jobs=await store.create_job_group('owner',JobGroupCreate(thread_id='b'*32,
            origin_turn_id='c'*32,idempotency_key='unknown-group',jobs=[JobRequest(
                local_job_id='unknown',kind=JobKind.OPERATION,adapter='fake',descriptor_id='fake',
                descriptor_version='1',descriptor_digest='a'*64,idempotency_key='unknown-job')]))
        job=jobs[0];attempt=await store.claim_job(job.id,'worker','node-a',30)
        await store.mark_job_submission_started(job.id,'worker',attempt.generation)
        reservation=await scheduler.reserve(job.id,attempt.generation,request())
        # Historical terminal rows can lack a cleanup owner. An empty live set
        # does not prove that an interrupted create request was rejected.
        async with store.connections.begin() as db:
            await db.execute(update(JobRow).where(JobRow.id==job.id).values(state='UNKNOWN'))
            await db.execute(update(ResourceReservationRow).where(
                ResourceReservationRow.id==reservation.id).values(expires_at=now()-timedelta(seconds=1)))
        await scheduler.register_node(node(generation=2))
        assert await scheduler.reap_stale_reservations('node-a',2,set())==[]
        assert await scheduler.active_reservation(job.id) is not None
    finally:await store.close()
