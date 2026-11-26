# Production Hardening Design

## Intent

Prepare the product code for a multi-replica Kubernetes launch without moving
DBA, SRE, legal or company CI responsibilities into this repository. Preserve
the existing provider-neutral state machines, split database ownership and
zero-duplicate external execution guarantees.

## Confirmed Decisions

1. Extract API-owned background execution into one `platform-worker`. API keeps
   browser requests, authentication, SSE and file endpoints. Existing
   Allocation, Job, Controlled, Media, Site, Gateway and image-inference roles
   remain independent.
2. Use a two-level error contract: structured internal diagnosis and stable
   public error code/message/troubleshooting identity.
3. Make Outbox a typed durable transport. Invalid deterministic commands must
   fail before enqueue; deterministic handler failures do not retry; transient
   delivery has bounded retry time/count; unknown external outcomes reconcile
   rather than resubmit. Scans recover lost wakeups/leases only.
4. Emit exact low-cardinality metrics for Turn queue/first output, Sandbox
   create/capacity/reconciliation, Job phases, Outbox lag, Provider phases and
   Artifact persistence/delivery. No SLO, dashboard or alert policy is defined
   before production traffic.
5. Every durable Worker claim starts a new Trace linked to persisted producer
   context.
6. Product services are safe for multi-replica launch. `platform-worker` uses a
   leader lease for singleton coordination; leased work remains multi-active.
7. Capability tokens carry a `kid`; current and previous signing keys support
   rolling rotation.
8. Independent releases declare protocol and contract compatibility, support a
   canary gate and retain rollback artifacts.
9. Admission is fair by owner. Ordinary quotas never block cleanup,
   reconciliation, recovery or observation of accepted work.
10. Object storage uses reference-driven mark-and-sweep with a grace period,
    second reference check, dry run and audit.
11. Public execution stages come from authoritative state and distinguish
    queued, Sandbox wait, Harness, Provider, collection, recovery and blocked.
12. Sandbox-to-Gateway deployment contract requires HTTPS plus the existing
    short-lived capability. Certificate operations belong to SRE.
13. Provider/Profile circuit breakers stop only new creates; existing execution,
    observation and cleanup continue. No hidden provider fallback.
14. Add state-machine/property and multi-process chaos coverage while retaining
    the current real catalog.
15. Add an idempotent Usage Ledger for tokens, Sandbox seconds, Provider usage
    and object bytes. It supports analysis and quotas, not billing.

## External Responsibilities

- DBA owns schema deployment, backup, PITR and disaster recovery.
- SRE owns Kubernetes manifests/Helm, Secrets, replica/resource sizing,
  production load tests, dashboards, SLOs, alerts, TLS certificates and
  environment deployment.
- Company CI/SRE owns image scanning and organization supply-chain policy.
- Data retention/legal policy is outside this project.

The project supplies service commands, ports, probes, graceful shutdown,
permissions, dependencies, protocol compatibility, metrics and test scenarios.

## Architecture

### Request And Execution Planes

`api` is horizontally scalable and contains no singleton background loop.
`platform-worker` owns Primary Turn execution/recovery, durable continuation,
cross-domain protocol advancement, media continuation, Sandbox expiry/orphan
cleanup, warm-pool maintenance, model discovery/probes and draft expiry. Its
singleton loops run only on the database-fenced leader; durable claims remain
safe across all replicas.

### Errors

Internal failures use one bounded `ErrorEnvelope` with `code`, `origin`,
`phase`, `retryable`, `retry_class`, `side_effect_state`, `exception_type`,
`internal_ref` and safe detail. Public APIs map that envelope to a stable public
code/message and troubleshooting identity. Raw Provider/Harness/database
exceptions never become public text.

### Durable Commands

Every Domain command kind maps to one exact typed payload contract and handler.
Canonical serialized payload and contract version/digest are checked before the
source transaction commits. Consumer deployment compatibility is checked before
release. Normal state is `pending -> delivering -> acknowledged`; dependency
outage may enter `waiting_dependency`. Program/contract failures fail fast and
degrade health instead of generating unbounded retry traffic.

### Scheduling

Owner admission uses persisted active counts and fair round-robin/oldest-owner
selection. Thread serialization remains unchanged. Cleanup, recovery,
reconciliation and accepted execution observation use separate reserved
capacity and bypass ordinary owner quotas.

### Storage And Usage

Object GC marks references across Core, Media and Sites authorities, applies a
grace cutoff, then rechecks each candidate before deletion. Upload leases and
in-progress commits protect new objects. Usage events use stable idempotency
keys and append-only ledger rows so crash/replay cannot double count.

## Real Case Expansion

Add `EVO01`-`EVO13` to the catalog, increasing it from 175 to 188:

- `EVO01` API restart while PlatformWorker continues a real Ali Turn.
- `EVO02` PlatformWorker leader failover without message replay.
- `EVO03` multi-replica Worker claims plus random process kills, zero duplicate execution.
- `EVO04` Outbox target outage and lost receipt converge exactly once without unbounded retries.
- `EVO05` OTel Metrics reconcile exactly with authoritative database transitions.
- `EVO06` all durable async claims produce linked Traces through final Artifact.
- `EVO07` capability `kid` rolling rotation preserves active Sandbox work.
- `EVO08` owner fairness prevents a flooding owner from starving another.
- `EVO09` Provider/Profile circuit opens, preserves cleanup, half-opens and recovers.
- `EVO10` real object-store mark/sweep deletes only confirmed unreferenced objects.
- `EVO11` Usage Ledger remains exact across Worker kill/replay.
- `EVO12` internal ErrorEnvelope and public error remain separated on real Harness failure.
- `EVO13` browser stages match authoritative queue/Sandbox/Provider/collection/recovery states.

Invalid pre-enqueue contracts, retry budgets, compatibility digests, metric
cardinality and HTTPS certificate behavior remain deterministic component or
deployment contract tests rather than expensive real cases.

## Completion

- Focused tests and PostgreSQL concurrency tests pass per phase.
- New real cases pass with zero program failures and no clear optimizable
  non-program failures.
- The historical 175 cases are rerun on the final deployed revision.
- One 188-case clean report is produced.
- Full Python/frontend/module/release gates pass.
- README, development rules, module docs and case evidence are current.
- Small commits are pushed to the public GitHub branch.
- Immutable releases and Runtime bundle are deployed; health, page, Sandbox
  execution/recovery and cleanup are verified; unselected generated releases
  and test resources are removed.
