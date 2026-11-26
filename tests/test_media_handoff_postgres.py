"""Real PostgreSQL transaction races; each case owns unique Thread/Job rows."""
import asyncio
import os

import pytest
from sqlalchemy import func,select

from sandbox_harness.services.job_worker.image_decompose_adapter import (ImageDecomposeAdapter)
from sandbox_harness.common.db.store import (MediaJobRow, Store)
from test_image_decompose_media import setup

pytestmark=pytest.mark.skipif(not os.environ.get('PARALLEL_TEST_POSTGRES_URL'),
    reason='requires isolated PostgreSQL database')


async def test_postgres_upgrade_keeps_existing_media_rows(tmp_path):
    from datetime import timedelta
    from sqlalchemy import text
    from sandbox_harness.common.contracts.domain import (MediaDraftCreate, now)
    from sandbox_harness.services.job_worker.image_decompose_adapter import (DECOMPOSE_SPEC)
    settings,store,objects,context=await setup(tmp_path,
        database_url=os.environ['PARALLEL_TEST_POSTGRES_URL'])
    try:
        draft,_=await store.create_media_draft('owner',context.job.thread_id,MediaDraftCreate(
            kind='image',model='legacy',parameters={},idempotency_key='old-media'),
            expires_at=now()+timedelta(hours=1),provider_spec=DECOMPOSE_SPEC)
        old=await store.confirm_media_draft(draft.id,'owner',provider_spec=DECOMPOSE_SPEC)
        async with store.engine.begin() as db:
            await db.execute(text('ALTER TABLE media_jobs DROP COLUMN source_job_id'))
        await store.initialize();await store.initialize()
        assert (await store.get_media_job(old.id,'owner')).id==old.id
        assert (await ImageDecomposeAdapter(settings,store,objects).submit(context)).external_execution_id!=old.id
    finally:await store.close()


async def test_concurrent_media_handoff_across_store_instances(tmp_path):
    url=os.environ['PARALLEL_TEST_POSTGRES_URL']
    settings,store,objects,context=await setup(tmp_path,database_url=url)
    other=Store(url)
    try:
        first=ImageDecomposeAdapter(settings,store,objects)
        second=ImageDecomposeAdapter(settings,other,objects)
        results=await asyncio.gather(*(adapter.submit(context)
            for adapter in [first,second,first,second]))
        assert len({value.external_execution_id for value in results})==1
        async with store.connections() as db:
            count=await db.scalar(select(func.count()).select_from(MediaJobRow).where(
                MediaJobRow.source_job_id==context.job.id))
        assert count==1
    finally:await other.close();await store.close()


async def test_source_cancellation_and_media_handoff_serialize(tmp_path):
    from sandbox_harness.common.execution.job_adapters import (PreAcceptCancelled)
    settings,store,objects,context=await setup(tmp_path,
        database_url=os.environ['PARALLEL_TEST_POSTGRES_URL'])
    other=Store(settings.database_url)
    try:
        results=await asyncio.gather(ImageDecomposeAdapter(settings,store,objects).submit(context),
            other.request_job_cancel(context.job.id,'owner'),return_exceptions=True)
        assert not isinstance(results[1],BaseException)
        value=await store.media_execution_for_job(context.job.id,'owner')
        if value:
            jobs=await store.claim_media_jobs('pg-cancel',limit=100)
            assert next(job for job in jobs if job['id']==value.id)['status']=='cancel_requested'
        else:assert isinstance(results[0],PreAcceptCancelled)
    finally:await other.close();await store.close()
