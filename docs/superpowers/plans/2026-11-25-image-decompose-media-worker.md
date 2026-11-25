# Image decomposition MediaWorker implementation plan

**Goal:** Make MediaWorker own image decomposition HTTP execution while retaining the existing Operation contract.

**Architecture:** A fenced database-only Job adapter creates/reuses a linked MediaJob. A media provider implementation handles external HTTP. Existing MediaWorker persistence and ArtifactGateway publish the selected outputs.

**Tech Stack:** Python 3.13, SQLAlchemy/PostgreSQL, httpx, existing JobWorker/MediaWorker.

**Spec:** [Confirmed migration direction](../specs/2026-11-25-image-decompose-media-worker.md).

**Execution:** Native implementation in this session, TDD per boundary, then independent review. The user has already selected and authorized this architecture and live verification; do not reopen that choice.

## Constraints and review focus

- Ali worktree/branch only; all generated files under `/data2/wujing03`.
- Preserve public descriptor, output collection, owner scope and confirmation.
- No duplicate provider submission after response loss or process replacement.
- Test cancellation between internal enqueue and Job acceptance, and during a poll/download.
- Test cross-owner references, stale generations and concurrent duplicate enqueue.
- Test excluded MIME formats, many layers, stable output identity and partial download recovery.
- Keep legacy accepted jobs observable without old direct HTTP execution.
- Preserve unaffected successes; real evidence is separate from fake transport tests.

## Task 1: Durable delegated media handoff

Files: `store.py`, `media_job_adapter.py`, `image_decompose_adapter.py`,
`tests/test_image_decompose_media.py`, media Store/PostgreSQL tests.

- [x] Add a regression proving Operation submission only queues one MediaJob and makes no HTTP request; run red.
- [x] Add optional unique `source_job_id` and idempotent schema upgrade; implement fenced enqueue/lookup and owned output access.
- [x] Implement the database bridge, same-row recovery, legacy handle import and source cancellation propagation.
- [x] Prove concurrent enqueue, stale owner/generation and cancellation before external acceptance; run focused and isolated PostgreSQL checks.

## Task 2: MediaWorker provider execution and collection

Files: focused image provider module, `media_worker.py`, `media_service.py`,
`media_job_adapter.py`, `store.py`, provider and media durability tests.

- [x] Write red tests for submit/poll/restart, unknown submission, cancel and multi-output persistence.
- [x] Add the registered external media provider transport and lifecycle hooks; persist exact provider identity before poll.
- [x] Add bounded authenticated downloads and actual-MIME selection before object storage; preserve primary output and unique layer roles.
- [x] Reuse staged outputs and generic collection for dense idempotent Job artifacts; test partial failures and cancellation races.
- [x] Run media, Job, artifact, migration and PostgreSQL suites; commit the coherent migration after review fixes.

## Task 3: Real acceptance and cleanup

Files: real image-decomposition migration driver, affected case/evidence docs,
README/development architecture and release guidance.

- [x] Add real cases for output contracts, killing MediaWorker after acceptance, cancellation and a deliberately lost real response; keep test resource ownership explicit.
- [x] Remove superseded direct-client code and old receipt/cache recovery tests, retaining their relevant assertions in the new suites.
- [x] Run full Python/shared PostgreSQL checks; record source revision and exact affected case list.
- [ ] Deploy affected Ali services and run C09/K07 plus affected media/continuation cases; investigate failures while the batch runs.
- [ ] Verify health, output bytes, execution counts, cleanup and no duplicate continuation; obtain independent branch review.
- [ ] Update docs, scan credentials, push, and deliver results with any remaining limitations.
