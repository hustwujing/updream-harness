# Cross-Database Real Gate Design

## Intent

The split deployment stores authoritative state in separate Accounts, Core,
Media and Sites PostgreSQL databases. The product gate must prove that the
cross-database protocols preserve identity, ownership, fencing and cleanup on
the deployed Ali environment. XDB01-XDB06 become first-class catalog cases,
increasing the product gate from 169 to 175 cases.

## Constraints

- Every assertion reads the live isolated Ali databases and deployed services.
- Provider work uses the configured real MediaWorker route; it is never faked.
- Checks compare stable identifiers, generation fences, hashes, MIME types,
  sizes, filenames and positions rather than accepting mere row existence.
- Recovery reuses an accepted provider execution and does not create a second
  external execution or advance the source attempt generation while polling.
- Cancellation prevents late Media output from being promoted into Core.
- Lifecycle cleanup covers Accounts, Core, Media and Sites without leaving
  owner or thread rows behind.
- Test data uses unique owner, thread and idempotency identities and is cleaned
  through product lifecycle commands wherever the case exercises deletion.
- No case-, provider- or operation-specific production branch is introduced.

## Cases

| Case | Real-system contract |
| --- | --- |
| XDB01 | Core authorizes a Media source job and collection returns exactly the permitted Media outputs with matching object reference, SHA-256, MIME type, size and source identity. |
| XDB02 | After Media accepts a source job, SIGKILL and restart JobWorker; collection resumes with the same provider execution and source generation and produces one promoted result. |
| XDB03 | A real multi-output Media job preserves role, filename, position, object reference, SHA-256, MIME type and size when Core collects it. |
| XDB04 | A Core source cancellation fences its Media permit; late output cannot be promoted to Core artifacts or project files. |
| XDB05 | A Core site command is acknowledged by Sites with the exact manifest, version and file identities, then becomes observable through the deployed API. |
| XDB06 | Account/thread deletion propagates through Core, Media and Sites and leaves no owner- or thread-scoped rows in any authoritative database. |

## Runner And Evidence

`tests/run_real_cross_database.py` owns the six handlers and emits the same
classification schema as the existing protocol runners. `run_real_campaign.py`
routes catalog rows with executor `cross_database` to that runner. The runner
uses the campaign service controls for deliberate JobWorker termination and
prints stage updates so the two-minute campaign report explains active work.

The final 175-case report reuses the verified 169-case report because these
tests add coverage without changing product code. XDB01-XDB06 must run at the
current revision. Any production fix discovered by an XDB case invalidates the
affected prior evidence and requires the affected catalog cases to rerun.

## Approaches Considered

Embedding checks in existing K cases would hide the split-database contract
inside unrelated provider coverage. A separate supplemental report would not
meet the requested 175-case product gate. First-class XDB catalog cases with a
dedicated executor keep the contracts visible while preserving one report and
one classification model.

## Completion

- Catalog and runner unit tests prove exactly 175 unique runnable cases and six
  implemented XDB handlers.
- XDB01-XDB06 pass against the deployed Ali databases and services.
- The merged report contains exactly the 175 catalog IDs with 175 successes.
- Full Python and frontend checks pass at the final revision.

