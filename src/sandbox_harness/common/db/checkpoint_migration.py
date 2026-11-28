"""Explicit additive migration for immutable checkpoint version history."""
from __future__ import annotations

import json
import re

from sqlalchemy import func,inspect,select,text,update

from sandbox_harness.common.contracts.domain import TERMINAL
from .domain_database import DomainDatabase,DomainIdentity
from .models import CheckpointVersionRow,ThreadRow,TurnRow,canonical_digest


async def migrate_checkpoint_versions(database_url:str)->dict:
    database=DomainDatabase('core',database_url)
    old_shape={name:value for name,value in database.shape.items()
        if name!='checkpoint_versions'}
    old_digest=canonical_digest(old_shape);new_digest=database.schema_digest
    created=False;backfilled=0
    try:
        async with database.engine.begin() as connection:
            if connection.dialect.name=='postgresql':
                await connection.execute(text('SELECT pg_advisory_xact_lock(:id)'),
                    {'id':0x5348504152414C4C})
            existing=await connection.run_sync(lambda value:set(
                inspect(value).get_table_names()))
            old_tables=database.owned_tables-{'checkpoint_versions'}
            if existing not in {old_tables,database.owned_tables}:
                raise RuntimeError('checkpoint_migration_schema_unknown')
            identity=(await connection.execute(select(DomainIdentity.__table__))).mappings().all()
            if (len(identity)!=1 or identity[0]['domain']!='core'
                    or identity[0]['version']!=database.schema_version
                    or identity[0]['schema_digest'] not in {old_digest,new_digest}):
                raise RuntimeError('checkpoint_migration_identity_mismatch')
            if existing==old_tables and identity[0]['schema_digest']!=old_digest:
                raise RuntimeError('checkpoint_migration_identity_mismatch')
            active=int((await connection.execute(select(func.count()).select_from(
                TurnRow.__table__).where(TurnRow.state.not_in(
                    [state.value for state in TERMINAL])))).scalar_one())
            if active:raise RuntimeError('checkpoint_migration_active_turns')
            if 'checkpoint_versions' not in existing:
                await connection.run_sync(CheckpointVersionRow.__table__.create)
                created=True
            threads=(await connection.execute(select(ThreadRow.__table__).where(
                ThreadRow.checkpoint_ref!=''))).mappings().all()
            for thread in threads:
                reference=str(thread['checkpoint_ref'])
                if not re.fullmatch(r'sha256:[0-9a-f]{64}',reference):
                    raise RuntimeError('checkpoint_migration_reference_invalid')
                rows=(await connection.execute(select(CheckpointVersionRow.__table__).where(
                    CheckpointVersionRow.thread_id==thread['id'],
                    CheckpointVersionRow.state=='ACTIVE'))).mappings().all()
                if rows:
                    if len(rows)!=1 or rows[0]['object_ref']!=reference:
                        raise RuntimeError('checkpoint_migration_active_conflict')
                    continue
                timestamp=thread['updated_at'];identity_value=canonical_digest([
                    'checkpoint-version','backfill',thread['id'],reference,
                    str(thread['native_thread_id'] or '')])
                await connection.execute(CheckpointVersionRow.__table__.insert().values(
                    id=identity_value,owner_id=thread['owner_id'],thread_id=thread['id'],
                    source_turn_id='',parent_id='',object_ref=reference,
                    sha256=reference.removeprefix('sha256:'),size=0,
                    native_thread_id=str(thread['native_thread_id'] or ''),state='ACTIVE',
                    partial=False,verification_json=json.dumps({'backfilled':True},
                        sort_keys=True,separators=(',',':')),
                    created_at=timestamp,updated_at=timestamp,activated_at=timestamp))
                backfilled+=1
            await connection.execute(update(DomainIdentity.__table__).where(
                DomainIdentity.id==1).values(schema_digest=new_digest))
        await database.initialize()
        return {'created':created,'backfilled':backfilled,
            'schema_digest':new_digest}
    finally:await database.close()
