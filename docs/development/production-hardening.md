# Production Hardening

## Applies to

Read this before changing multi-replica ownership, durable cross-domain commands,
public errors, business metrics, capability keys, owner admission, Provider
circuits, object garbage collection, Usage Ledger or execution-stage projection.

## Authoritative entry points

- `common/db/handoff.py` and `common/contracts/domain_commands.py`: exact typed
  command payloads, receipts, contract digests and bounded delivery.
- `services/platform_worker/platform_service.py` and `common/db/leaders.py`:
  multi-active work and the database-fenced singleton leader.
- `common/contracts/errors.py` and `public_errors.py`: bounded internal diagnosis
  and stable public code/action/troubleshooting identity.
- `common/observability/observability.py`: fixed low-cardinality metric catalog.
- `common/security/capability.py`: `kid` keyring and current/previous rotation.
- `common/db/job_attempts.py`, `turns.py` and `provider_circuits.py`: fair owner
  admission and create-only circuit behavior.
- `common/storage/object_gc.py` and `common/db/usage.py`: second-check sweep and
  append-only idempotent usage.

## Protected contracts

- API never starts business execution or singleton supervisor loops.
- Durable claims remain generation-fenced and may run on every Worker replica.
- Leader loss cancels singleton coordination without cancelling independently
  leased business work.
- Deterministic Outbox failures stop on the first attempt. Only classified
  dependency failures retry within configured count and age budgets.
- Unknown external side effects reconcile the original identity and never submit
  a replacement implicitly.
- Ordinary owner quotas cannot block accepted execution observation, recovery,
  reconciliation or cleanup.
- Provider circuits block only new create calls. Poll, reconcile, cancel and
  delete remain available.
- Public responses never contain raw exception, Provider body, URL, credential
  or internal detail. Query identities are stable across restart.
- Metric attributes use the fixed catalog and contain no business identifiers.
- Object deletion requires grace, a complete authority snapshot and an immediate
  second reference check. Default deployment mode is dry-run.
- Usage rows are append-only and idempotent; this repository does not bill users.

## Completion

Run focused SQLite tests, disposable PostgreSQL concurrency tests and the
affected EVO real cases. A release requires the full Python/frontend/module
gates, an impact-assessed 198-case composite with current evidence for every
changed contract, immutable release compatibility evidence, service restart
recovery and cleanup of test resources.
