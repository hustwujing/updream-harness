"""Upgrades persistence; transaction boundaries are preserved."""
from __future__ import annotations
from sqlalchemy import (inspect as sqlalchemy_inspect, text)
from sqlalchemy.exc import (OperationalError)
from sandbox_harness.common.db.models import (Base, SCHEMA_INITIALIZATION_LOCK_ID)
from sandbox_harness.common.db.ownership_models import (OwnershipBase)


class UpgradesTransactions:
    async def initialize(self) -> None:
        if self.events:
            await self.events.initialize()
        async with self.engine.begin() as connection:
            if connection.dialect.name == "postgresql":
                await connection.execute(text("SELECT pg_advisory_xact_lock(:lock_id)"),
                    {"lock_id": SCHEMA_INITIALIZATION_LOCK_ID})
            await connection.run_sync(Base.metadata.create_all)
            await connection.run_sync(OwnershipBase.metadata.create_all)
            allocation_columns=await connection.run_sync(lambda value:{column['name'] for column in
                sqlalchemy_inspect(value).get_columns('sandbox_allocations')})
            timestamp_type='TIMESTAMP WITH TIME ZONE' if connection.dialect.name=='postgresql' else 'TIMESTAMP'
            for column,definition in {
                'reconcile_after':timestamp_type,
                'reconcile_attempts':'INTEGER NOT NULL DEFAULT 0',
                'reconcile_token':"VARCHAR(64) NOT NULL DEFAULT ''",
                'reconcile_expires_at':timestamp_type,
                'issued_at':timestamp_type,
                'recovery_policy_json':"TEXT NOT NULL DEFAULT '{}'",
            }.items():
                if column not in allocation_columns:
                    suffix=' IF NOT EXISTS' if connection.dialect.name=='postgresql' else ''
                    try:await connection.execute(text(
                        f'ALTER TABLE sandbox_allocations ADD COLUMN{suffix} {column} {definition}'))
                    except OperationalError as exc:
                        if (connection.dialect.name!='sqlite' or
                                f'duplicate column name: {column}' not in str(exc).lower()):raise
            await connection.execute(text('CREATE INDEX IF NOT EXISTS ix_sandbox_allocations_due '
                'ON sandbox_allocations (namespace, provider, state, reconcile_after)'))
            job_columns=await connection.run_sync(lambda value:{column['name'] for column in
                sqlalchemy_inspect(value).get_columns('jobs')})
            if 'allocation_replacements' not in job_columns:
                suffix=' IF NOT EXISTS' if connection.dialect.name=='postgresql' else ''
                try:await connection.execute(text(f'ALTER TABLE jobs ADD COLUMN{suffix} '
                    'allocation_replacements INTEGER NOT NULL DEFAULT 0'))
                except OperationalError as exc:
                    if (connection.dialect.name!='sqlite' or
                            'duplicate column name: allocation_replacements' not in str(exc).lower()):raise
            turn_columns=await connection.run_sync(lambda value:{column["name"] for column in
                sqlalchemy_inspect(value).get_columns("turns")})
            if "sandbox_handle_json" not in turn_columns:
                suffix=" IF NOT EXISTS" if connection.dialect.name=="postgresql" else ""
                try:await connection.execute(text(
                    f"ALTER TABLE turns ADD COLUMN{suffix} sandbox_handle_json TEXT NOT NULL DEFAULT ''"))
                except OperationalError as exc:
                    if (connection.dialect.name!="sqlite" or
                            "duplicate column name: sandbox_handle_json" not in str(exc).lower()):raise
            artifact_columns = await connection.run_sync(lambda value: {
                column["name"] for column in sqlalchemy_inspect(value).get_columns("artifacts")})
            if "partial" not in artifact_columns:
                statement = ("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS "
                    "partial BOOLEAN NOT NULL DEFAULT FALSE" if connection.dialect.name == "postgresql"
                    else "ALTER TABLE artifacts ADD COLUMN partial BOOLEAN NOT NULL DEFAULT 0")
                try:
                    await connection.execute(text(statement))
                except OperationalError as exc:
                    if (connection.dialect.name != "sqlite"
                            or "duplicate column name: partial" not in str(exc).lower()):
                        raise
            media_columns = await connection.run_sync(lambda value: {
                column["name"] for column in sqlalchemy_inspect(value).get_columns("media_jobs")})
            if "source_job_id" not in media_columns:
                suffix=" IF NOT EXISTS" if connection.dialect.name=="postgresql" else ""
                await connection.execute(text(
                    f"ALTER TABLE media_jobs ADD COLUMN{suffix} source_job_id VARCHAR(32)"))
            await connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS "
                "ix_media_jobs_source_job_id ON media_jobs (source_job_id)"))
            if "generation" not in media_columns:
                statement = ("ALTER TABLE media_jobs ADD COLUMN IF NOT EXISTS "
                    "generation INTEGER NOT NULL DEFAULT 0" if connection.dialect.name == "postgresql"
                    else "ALTER TABLE media_jobs ADD COLUMN generation INTEGER NOT NULL DEFAULT 0")
                try:await connection.execute(text(statement))
                except OperationalError as exc:
                    if (connection.dialect.name!="sqlite"
                            or "duplicate column name: generation" not in str(exc).lower()):raise
            site_columns = await connection.run_sync(lambda value: {
                column["name"] for column in sqlalchemy_inspect(value).get_columns("site_deploy_jobs")})
            for column,definition in {
                "generation":"INTEGER NOT NULL DEFAULT 0",
                "attempt_count":"INTEGER NOT NULL DEFAULT 0",
                "next_attempt_at":"TIMESTAMP",
            }.items():
                if column not in site_columns:
                    suffix=" IF NOT EXISTS" if connection.dialect.name=="postgresql" else ""
                    try:await connection.execute(text(
                        f"ALTER TABLE site_deploy_jobs ADD COLUMN{suffix} {column} {definition}"))
                    except OperationalError as exc:
                        if (connection.dialect.name!="sqlite"
                                or f"duplicate column name: {column}" not in str(exc).lower()):raise
            draft_columns = await connection.run_sync(lambda value: {
                column["name"] for column in sqlalchemy_inspect(value).get_columns("site_drafts")})
            if "request_digest" not in draft_columns:
                suffix=" IF NOT EXISTS" if connection.dialect.name=="postgresql" else ""
                try:await connection.execute(text(
                    f"ALTER TABLE site_drafts ADD COLUMN{suffix} request_digest VARCHAR(64) NOT NULL DEFAULT ''"))
                except OperationalError as exc:
                    if (connection.dialect.name!="sqlite"
                            or "duplicate column name: request_digest" not in str(exc).lower()):raise
            control_columns=await connection.run_sync(lambda value:{column["name"] for column in
                sqlalchemy_inspect(value).get_columns("sandbox_controls")})
            for column in ("release_phase","release_action"):
                if column not in control_columns:
                    suffix=" IF NOT EXISTS" if connection.dialect.name=="postgresql" else ""
                    try:await connection.execute(text(
                        f"ALTER TABLE sandbox_controls ADD COLUMN{suffix} {column} VARCHAR(32) NOT NULL DEFAULT ''"))
                    except OperationalError as exc:
                        if (connection.dialect.name!="sqlite" or
                                f"duplicate column name: {column}" not in str(exc).lower()):raise
            for table in ("sandbox_controls","sandbox_port_grants"):
                columns=await connection.run_sync(lambda value,table=table:{column["name"] for column in
                    sqlalchemy_inspect(value).get_columns(table)})
                if "sandbox_handle_json" not in columns:
                    suffix=" IF NOT EXISTS" if connection.dialect.name=="postgresql" else ""
                    try:await connection.execute(text(
                        f"ALTER TABLE {table} ADD COLUMN{suffix} sandbox_handle_json TEXT NOT NULL DEFAULT ''"))
                    except OperationalError as exc:
                        if (connection.dialect.name!="sqlite" or
                                "duplicate column name: sandbox_handle_json" not in str(exc).lower()):raise
            await connection.execute(text("""
                INSERT INTO turn_order (turn_id, thread_id, sequence, created_at)
                SELECT t.id, t.thread_id, MIN(i.sequence), t.created_at
                FROM turns t
                JOIN items i ON i.turn_id = t.id AND i.type = 'user_message'
                LEFT JOIN turn_order existing ON existing.turn_id = t.id
                WHERE existing.turn_id IS NULL
                GROUP BY t.id, t.thread_id, t.created_at
            """))

