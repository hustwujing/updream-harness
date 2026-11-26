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
