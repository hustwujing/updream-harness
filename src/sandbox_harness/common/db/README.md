# Persistence source ownership

`models.py` is the canonical ORM registry. `store.py` composes domain transaction
methods; its connection and locks are shared within one Store instance. Source
separation preserves the existing transaction bodies, owner/generation fences
and model identities. It does not itself provide independent databases.

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

These primitives are verified independently on SQLite and disposable PostgreSQL
databases. The service entry points still use the original Store until their
business transactions have been converted; these primitives alone are not a
completed multi-database deployment.
