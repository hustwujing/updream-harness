# Persistence source ownership

`models.py`, `account_models.py`, `media_models.py` and `ownership_models.py` are
the canonical ORM registries. `AccountsRepository`, `CoreRepository`,
`MediaRepository` and `SiteRepository` each own one independently credentialed
database. `DomainDatabase` rejects access to tables outside that authority.

Method groups cover connections/upgrades, capabilities/budgets, pause, Job
admission/attempts/allocation/cleanup/publication, accounts, Threads/Turns/events,
files/interactions, media and sites. Cross-domain transaction redesign for fresh
accounts/core/media/sites databases follows the independent-databases spec.

The 339-method extraction is verified by AST comparison and existing behavioral
tests; implementation receipts are below `.data/evidence/module-isolation/`.

`domain_database.py` initializes fresh accounts/core/media/sites schemas, records
their domain/schema identity and rejects foreign-table business sessions. It
does not upgrade existing databases. `handoff.py` persists ordered commands in a
source transaction and commits target effects with their deduplicated receipt.
Each destination handler is restricted to its local database; external Provider
calls require their own durable execution intent. Failed streams back off without
blocking independent streams. Receipts are replayable after reply or process loss.

`lifecycle.py` persists prepare/commit/abort decisions and participant barriers.
Core retains admission freezes until all decision receipts arrive. Owner and
Thread epochs are separate: a remote admission must supply both captured
epochs. Unknown outcomes stay pending; only a known refusal followed by all
abort acknowledgements becomes a final refusal. Business check/effect callbacks
must use only their supplied local transaction, and every new-work path must
participate in admission. The protocol is not a replacement for those checks.

`AccountsRepository` owns account/session operations and durable registration
requests. Registration obtains a core-coordinated owner barrier, preserves the
anonymous owner identity, and atomically commits the User, Session and principal
claim. Old anonymous credentials must consult `guest_owner_available`; even a
disabled account retains its claim. Only the registration replay flow may recover
a session using the original signed proof plus password verification. API identity
middleware fails closed when an anonymous principal has already been claimed.

`CoreRepository` initializes only core tables and records Turn completion and its
media notification atomically. Its lifecycle coordinator preserves core Job,
budget, artifact and cleanup checks. The new sandbox ownership ledger records a
create intent before an external call, fences binding by owner/Thread epochs and
keeps unknown creation capacity owned. A committed lifecycle can defer its local
effect until cleanup records progress through confirmed absence and capacity
release. Cleanup uses the backend's required deletion-convergence contract without
requiring a healthy Runtime, and resumes from the durable phase after process loss.

Pool/Inspector creation, binding and retention use the Core ownership ledger.
API archive/delete and registration return coordinator results; pending operations
resume through the domain-protocol supervisor and browser polling.

The API Pool integration supplies the browser owner identity for Primary and
Inspector acquire/adopt calls. A new external create is preceded by a durable
intent; handle receipt and binding are recorded before the Sandbox is returned.
Warm instances acquire Thread ownership before binding. Deterministic pre-accept
rejection releases the intent and permits retry, while an unknown create keeps
capacity charged and a replacement process reconciles the original allocation
identity without resubmission. All Pool deletion paths update the same ledger.

`SiteRepository` initializes only the sites authority. SiteWorker receives only
the sites credential; API and sandbox-gateway create drafts through durable
Core-to-Sites commands.

`MediaRepository` gives MediaWorker only the media authority. Core issues a
generation-fenced source permit in the same transaction as its remote obligation;
media consumes the permit before Provider submission. Cancellation locks permit
before media Job, and terminal outputs plus publication outbox commit locally.
Real PostgreSQL tests cover duplicate dispatch, one Worker claim and concurrent
permit consumption/cancellation. JobWorker now connects separately to core and
media through `MediaAuthorityClient`; AllocationWorker and ControlledWorker use
only core, while SiteWorker uses only sites.

Turn media/site preparation and media promotion use `CrossDomainCommands`.
Every source transaction records an explicit remote obligation, so lifecycle
operations reject pending delivery. Target effects and receipts are idempotent;
media promotion commits Core ProjectFiles before acknowledging their identities
back to media. `GatewayRepositories` exposes only the sandbox-gateway allowlist.
`ApiRepositories` routes explicit local methods and owns cross-domain commands;
both production entry points require independent database credentials.

These boundaries are verified on SQLite and disposable PostgreSQL databases. The
Ali deployment uses distinct database roles with cross-database CONNECT revoked,
and service releases resolve immutable source and dependency digests before launch.
