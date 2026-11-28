from sqlalchemy import inspect,select

from sandbox_harness.common.db.checkpoint_migration import migrate_checkpoint_versions
from sandbox_harness.common.db.domain_database import (DomainDatabase,DomainIdentity,ProtocolBase)
from sandbox_harness.common.db.account_models import AccountBase
from sandbox_harness.common.db.ownership_models import OwnershipBase
from sandbox_harness.common.db.media_models import MediaAuthorityBase
from sandbox_harness.common.db.models import (Base,CheckpointVersionRow,ThreadRow,TurnRow,
    canonical_digest)
import pytest
from sandbox_harness.common.contracts.domain import now


async def old_core_database(url):
    database=DomainDatabase('core',url)
    old_tables=database.owned_tables-{'checkpoint_versions'}
    old_shape={name:value for name,value in database.shape.items()
        if name!='checkpoint_versions'}
    async with database.engine.begin() as connection:
        await connection.run_sync(lambda value:Base.metadata.create_all(value,
            tables=[table for name,table in Base.metadata.tables.items() if name in old_tables]))
        for metadata in (AccountBase.metadata,OwnershipBase.metadata,MediaAuthorityBase.metadata):
            await connection.run_sync(lambda value,metadata=metadata:metadata.create_all(value,
                tables=[table for name,table in metadata.tables.items() if name in old_tables]))
        await connection.run_sync(ProtocolBase.metadata.create_all)
        await connection.execute(DomainIdentity.__table__.insert().values(id=1,domain='core',
            version=database.schema_version,schema_digest=canonical_digest(old_shape)))
    return database


async def test_checkpoint_version_migration_backfills_and_is_idempotent(tmp_path):
    url='sqlite+aiosqlite:///'+str(tmp_path/'old-core.db');database=await old_core_database(url)
    timestamp=now();reference='sha256:'+'a'*64
    try:
        async with database.engine.begin() as connection:
            await connection.execute(ThreadRow.__table__.insert().values(id='t'*32,
                owner_id='o'*64,harness='codex',model='gpt-5.6',reasoning_level='standard',
                reasoning_effort='medium',title='old',checkpoint_ref=reference,
                native_thread_id='native',active_turn_id='',last_read_turn_id='',
                next_item_sequence=0,archived_at=None,created_at=timestamp,updated_at=timestamp))
    finally:await database.close()

    first=await migrate_checkpoint_versions(url)
    assert first['created'] and first['backfilled']==1
    second=await migrate_checkpoint_versions(url)
    assert not second['created'] and second['backfilled']==0

    current=DomainDatabase('core',url)
    try:
        await current.initialize()
        async with current.connections() as session:
            row=(await session.execute(select(CheckpointVersionRow))).scalar_one()
            assert row.state=='ACTIVE' and row.object_ref==reference
            assert row.verification_json=='{"backfilled":true}'
    finally:await current.close()


async def test_checkpoint_version_migration_refuses_active_turn(tmp_path):
    url='sqlite+aiosqlite:///'+str(tmp_path/'active-core.db');database=await old_core_database(url)
    timestamp=now()
    try:
        async with database.engine.begin() as connection:
            await connection.execute(ThreadRow.__table__.insert().values(id='t'*32,
                owner_id='o'*64,harness='codex',model='gpt-5.6',reasoning_level='standard',
                reasoning_effort='medium',title='active',checkpoint_ref='',native_thread_id='',
                active_turn_id='u'*32,last_read_turn_id='',next_item_sequence=0,
                archived_at=None,created_at=timestamp,updated_at=timestamp))
            await connection.execute(TurnRow.__table__.insert().values(id='u'*32,
                thread_id='t'*32,idempotency_key='active',message='run',public_message='run',
                attachments_json='[]',harness='codex',model='gpt-5.6',
                reasoning_level='standard',reasoning_effort='medium',delivery_mode='start',
                network_access='restricted',internal=False,state='RUNNING',sandbox_id='',
                sandbox_handle_json='',error_code='',error_detail='',lease_owner='',
                lease_expires_at=None,recovery_generation=0,runtime_sequence=0,
                runtime_execution_id='',runtime_image_digest='',runtime_code_bundle_digest='',
                runtime_bundle_digest='',capability_nonce='',submit_traceparent='',
                last_traceparent='',cancel_requested=False,pending_final_state='',
                project_files_frozen=False,created_at=timestamp,updated_at=timestamp))
    finally:await database.close()
    with pytest.raises(RuntimeError,match='checkpoint_migration_active_turns'):
        await migrate_checkpoint_versions(url)
