import pytest
from types import SimpleNamespace
from datetime import timedelta
from sqlalchemy import update

from sandbox_harness.common.catalog.catalog import HarnessCatalog
from sandbox_harness.common.contracts.domain import Harness,ThreadCreate,now
from test_support.legacy_store import CheckpointVersionRow,Store
from sandbox_harness.common.storage.object_gc import authoritative_object_references


async def checkpoint_store(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'checkpoints.db'))
    await store.initialize();profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,
        reasoning_level='standard',title='Checkpoint versions'),profile,'a'*64)
    return store,thread


def ref(character):return 'sha256:'+character*64


async def publish(store,thread,character,index):
    staged=await store.stage_checkpoint_version(thread.id,ref(character),f'native-{index}',
        size=100+index,source_turn_id=f'{index:032x}',partial=False)
    await store.verify_checkpoint_version(staged['id'],{'archive':'ok','native':'present'})
    return await store.activate_checkpoint_version(staged['id'])


async def test_checkpoint_version_activation_is_atomic_and_idempotent(tmp_path):
    store,thread=await checkpoint_store(tmp_path)
    try:
        first=await store.stage_checkpoint_version(thread.id,ref('1'),'native-1',
            size=101,source_turn_id='1'*32)
        replay=await store.stage_checkpoint_version(thread.id,ref('1'),'native-1',
            size=101,source_turn_id='1'*32)
        assert replay['id']==first['id'] and replay['state']=='STAGING'
        with pytest.raises(RuntimeError,match='checkpoint_version_not_verified'):
            await store.activate_checkpoint_version(first['id'])
        current=await store.get_thread(thread.id)
        assert current.checkpoint_ref=='' and current.native_thread_id==''

        await store.verify_checkpoint_version(first['id'],{'archive':'ok'})
        active=await store.activate_checkpoint_version(first['id'])
        assert active['state']=='ACTIVE'
        assert (await store.activate_checkpoint_version(first['id']))['state']=='ACTIVE'
        current=await store.get_thread(thread.id)
        assert current.checkpoint_ref==ref('1') and current.native_thread_id=='native-1'

        second=await store.stage_checkpoint_version(thread.id,ref('2'),'native-2',
            size=102,source_turn_id='2'*32)
        await store.invalidate_checkpoint_version(second['id'],'checkpoint_archive_invalid')
        with pytest.raises(RuntimeError,match='checkpoint_version_not_verified'):
            await store.activate_checkpoint_version(second['id'])
        current=await store.get_thread(thread.id)
        assert current.checkpoint_ref==ref('1') and current.native_thread_id=='native-1'
    finally:await store.close()


async def test_checkpoint_version_retention_keeps_active_and_two_previous(tmp_path):
    store,thread=await checkpoint_store(tmp_path)
    try:
        for index,character in enumerate('1234',1):
            await publish(store,thread,character,index)
        rows=await store.list_checkpoint_versions(thread.id,'a'*64)
        assert len(rows)==3
        assert [row['state'] for row in rows].count('ACTIVE')==1
        assert [row['state'] for row in rows].count('SUPERSEDED')==2
        assert next(row for row in rows if row['state']=='ACTIVE')['object_ref']==ref('4')
        assert {row['object_ref'] for row in rows}=={ref('2'),ref('3'),ref('4')}
        references=await authoritative_object_references(SimpleNamespace(
            accounts=store,core=store,media=store,sites=store))
        assert {ref('2'),ref('3'),ref('4')}<=references
        assert ref('1') not in references
    finally:await store.close()


async def test_thread_delete_removes_checkpoint_versions(tmp_path):
    store,thread=await checkpoint_store(tmp_path)
    try:
        await publish(store,thread,'1',1)
        assert len(await store.list_checkpoint_versions(thread.id))==1
        await store.delete_thread(thread.id,'a'*64)
        assert await store.list_checkpoint_versions(thread.id)==[]
    finally:await store.close()


async def test_uncommitted_checkpoint_expires_without_touching_active(tmp_path):
    store,thread=await checkpoint_store(tmp_path)
    try:
        active=await publish(store,thread,'1',1)
        staged=await store.stage_checkpoint_version(thread.id,ref('2'),'native-2',
            size=102,source_turn_id='2'*32)
        async with store.connections.begin() as db:
            await db.execute(update(CheckpointVersionRow).where(
                CheckpointVersionRow.id==staged['id']).values(
                    updated_at=now()-timedelta(seconds=61)))
        assert await store.expire_uncommitted_checkpoint_versions(60)==1
        rows=await store.list_checkpoint_versions(thread.id)
        assert next(row for row in rows if row['id']==active['id'])['state']=='ACTIVE'
        assert next(row for row in rows if row['id']==staged['id'])['state']=='INVALID'
    finally:await store.close()
