# Deterministic collection cardinality failures

C09 on `3006bd2` declared exactly one file in its output collection, while the
accepted external result contained ten files. Store correctly blocked
publication, but JobWorker incorrectly treated the deterministic count failure
as an internal, retryable collection transport failure. Re-reading the same
result could not change its count.

The shared Store boundary now raises a typed error carrying only the role,
frozen count bounds and actual count. JobWorker records a terminal validation
failure with completed side effects and explicit count guidance. Child Harness
collection preserves the typed error and its existing summary. No caller's
contract is widened; candidates remain unpublished and cleanup is unchanged.
No provider, Operation or case condition is introduced. There is no reliable
early count rejection without an authoritative Descriptor output-count bound.

Validation sequence:

1. Reproduce missing/excess files through a real Store and ArtifactGateway;
   observe the old `WAITING_EXTERNAL` state and unnecessary collection retry.
2. Assert one submission, one collection, one failed attempt, durable cleanup,
   preserved candidates, no published ProjectFile, and safe count metadata.
3. Cover Child propagation and existing collection retries/identity fences.
4. Run the Python and isolated PostgreSQL gates before release.
5. Re-run C09; retain passed cases whose collection counts already satisfy
   their frozen contracts. Complete unrun cases. Preserve strict campaign
   classification: a count mismatch alone does not prove a non-program cause.

Evidence is under `.data/tmp/cardinality-red.log` and
`.data/tmp/cardinality-green.log`. The initial regression failed twice as
expected; the focused suite passes 97 tests. Real retest and release remain
pending while the current campaign drains.
