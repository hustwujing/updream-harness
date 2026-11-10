# Database Migration

## Applies to

Required for SQLAlchemy table, column, index, constraint or persisted payload
changes.

## Current mechanism

There is no Alembic migration directory. `Store.initialize` runs
`Base.metadata.create_all` for fresh databases and then explicit, additive
schema inspection/ALTER logic for existing databases. Therefore changing only a
SQLAlchemy row class updates a fresh install but does not upgrade an existing
deployment.

## Required implementation

1. Update the row model and public/domain contract if exposed.
2. Add an idempotent schema inspection and migration in `Store.initialize`.
3. Supply a server default for new non-null columns so an existing populated
   table upgrades safely.
4. Backfill derived values in the same controlled migration before code relies
   on them.
5. Handle both SQLite tests and PostgreSQL syntax/types explicitly. Do not
   assume timestamp timezone metadata is identical.
6. Add indexes with `IF NOT EXISTS` where supported and verify query predicates.
7. Update data-model documentation to describe the implemented table, not a
   conceptual future table.

Destructive column removal, type narrowing or constraint changes require an
explicit staged migration and rollback/backup plan. Do not hide them inside
startup code without review.

## Tests

- Fresh empty database initialization.
- Upgrade from a database created with the previous schema shape.
- Re-running initialization is idempotent.
- Backfill and defaults are correct for existing rows.
- Relevant Store transaction, owner, idempotency and recovery tests.
- Real isolated PostgreSQL schema when dialect behavior matters.

Before deployment, confirm no migration will block active Turn heartbeats for an
unbounded period.
