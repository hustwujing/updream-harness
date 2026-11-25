# Freeze live-input artifacts before Runtime delivery

R06 on `3006bd2` materialized its new attachment at 04:54:59 UTC. The model
attempted delegation while the input was still delivered, but its Artifact ID
was created only at input completion (04:56:05). The execution catalog therefore
omitted an already authorized attachment when the Harness needed it.

Broker now binds persisted TurnInput attachment records after materialization
and before calling Runtime append. Deferred input execution binds them in the
existing start transaction. Preparation checks the current Turn lease,
generation, execution, cancellation and pending input; it does not claim that
Runtime received or applied the input. IDs are stable per input/name, and retry
or replacement Worker preparation is idempotent. The initial project snapshot
remains immutable.

The catalog retains every immutable input Artifact for old Child grants. Its
path aliases are ordered by input sequence, including earlier random-ID
bindings, so A -> B -> A and re-preparation of an older input select the intended
latest bytes without changing any old grant. Owner filtering also covers
attachment-only results.

Validation covers the original empty-catalog failure inside Runtime append,
deferred consumption, lease/execution/cancel fences, unchanged input status,
Worker replacement, stable IDs, initial snapshot immutability, same-name versions
and an old Child grant. The original regression failed before the fix; the
catalog-retention regression also failed before the review correction. Evidence
is retained in `.data/tmp/live-input-*.log`.

Real scope: rerun R06, plus C09 for the independent cardinality correction.
The runner's only live attachment hook is `intervention_replace_source`, used by
R06. Other completed cases do not exercise changed nonempty-input binding or
failed cardinality paths. Preserve their 68 success receipts; continue the 99
unrun cases. Real retests, release gates and final deployment remain pending.
