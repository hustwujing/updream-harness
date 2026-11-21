# Execution Quality Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate duplicate Child deliverable execution, misleading artifact/tool failures, Primary timeline pollution and cumulative Sandbox diagnostics observed in one real Thread.

**Architecture:** Keep frozen Job contracts immutable. Make artifact disposition explicit, stream reviewed same-Thread Job Artifacts through the authenticated Gateway for later Turn publication, filter Job-scoped catalog access, and separate Job telemetry from Primary Thread Items. Harden bundled PPT tooling and make reused-Sandbox diagnostics incremental.

**Tech Stack:** Python 3.13, FastAPI, SQLAlchemy async, Pydantic v2, httpx, Pillow, pytest, existing Runtime/Gateway capability tokens.

**Spec:** `docs/superpowers/specs/2026-11-21-execution-quality-hardening-design.md`

## Global Constraints

- Modify and deploy only `/data2/wujing03/sandbox-harness-platform-parallel-ali` on branch `sandbox-harness-platform-parallel-ali`; the original Parallel checkout and deployment remain read-only.
- Never mutate a frozen JobArtifact disposition in place and never resubmit a successful Child Job merely to publish identical bytes.
- PostgreSQL ownership and signed capability claims authorize every artifact read; foreign-Thread and non-reviewed artifacts return 404.
- Do not expose object references, credentials, raw URLs, prompts, commands or file contents in telemetry.
- Keep all generated state, dependencies, caches, logs and evidence below the Ali checkout `.data` tree on `/data2`.
- Use TDD for every behavior change and preserve OpenSandbox/Ali provider lifecycle behavior.

## Review Focus

- A malicious Primary Turn guesses another Thread's Artifact ID, or a valid Job token guesses a same-Thread ID: fetch returns 404 and streams no bytes — Task 2 tests `test_gateway_job_artifact_fetch_hides_foreign_thread` and `test_gateway_job_artifact_fetch_rejects_job_scope`.
- A fetch disconnects, reports a wrong content length or returns same-length corrupted bytes: the destination remains unchanged and the temporary file is removed — Task 2 tests `test_artifact_fetch_is_atomic_on_truncated_response` and `test_artifact_fetch_rejects_digest_mismatch`.
- A Child capability requests the catalog: only its signed Skills, Operations, Capabilities and Artifact IDs appear, with no Child profiles — Task 3 test `test_job_catalog_is_filtered_to_frozen_grants`.
- A reused Sandbox starts a new Runtime instance, resets counters or rotates retained records: collection resets the instance-bound cursor/baseline without losing the new instance's first records — Task 5 test `test_runtime_diagnostics_handles_instance_change_counter_reset_and_cursor_rotation`.
- An unexpected proxy programming exception occurs rather than an external connect failure: it remains ERROR and never includes the destination — Task 5 test `test_public_proxy_unexpected_failure_remains_error_without_destination`.

---

### Task 1: Make Delegation Publication Intent Explicit

**Files:**
- Modify: `src/sandbox_runtime/job_cli.py`
- Modify: `src/sandbox_runtime/instructions.py`
- Test: `tests/test_job_cli_delegation.py`

**Interfaces:**
- Consumes: existing `compile_delegation(value, catalog, environment) -> dict`.
- Produces: `DelegationTaskSpec.output_disposition: Literal["candidate","workspace","delivery"] | None` with a model-level artifact/mixed requirement; stable local Primary rejection for `sandbox-job inspect`.

- [ ] **Step 1: Write failing delegation contract tests**

Add `test_artifact_delegation_requires_explicit_output_disposition`,
`test_mixed_delegation_requires_explicit_output_disposition`,
`test_text_delegation_may_omit_output_disposition`,
`test_text_delegation_rejects_output_disposition`, and
`test_delegation_example_shows_final_artifact_delivery`. Assert artifact/mixed
omission raises before compilation and malformed text/disposition combinations
are rejected.

- [ ] **Step 2: Run the tests and verify RED**

Run: `.venv/bin/pytest -q tests/test_job_cli_delegation.py`
Expected: the new omission tests fail because `candidate` is still implicit.

- [ ] **Step 3: Implement the Pydantic validator and examples**

Use `DelegationTaskSpec.validate_output_contract(self) -> DelegationTaskSpec`; require a non-null disposition only for artifact-producing modes. Make `DELEGATION_EXAMPLE` and the CLI epilog include one final artifact task with `output_disposition: "delivery"`.

- [ ] **Step 4: Write and run the Primary inspect preflight test**

Add a CLI test that sets `HARNESS_CHILD_MODE=0`, invokes `sandbox-job inspect`, and asserts exit 2 plus guidance naming `sandbox-artifact fetch`, with zero HTTP calls. Verify RED, implement the preflight, then verify GREEN.

- [ ] **Step 5: Update concise Runtime instructions and run focused tests**

State that artifact-producing delegation must choose disposition explicitly and that `sandbox-job inspect` is Job-only. Run: `.venv/bin/pytest -q tests/test_job_cli_delegation.py tests/test_runtime_code_bundle.py`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sandbox_runtime/job_cli.py src/sandbox_runtime/instructions.py tests/test_job_cli_delegation.py
git commit -m "fix: require explicit delegated artifact disposition"
```

### Task 2: Fetch Reviewed Child Artifacts Without Re-execution

**Files:**
- Modify: `src/sandbox_harness/store.py`
- Modify: `src/sandbox_harness/gateway.py`
- Modify: `src/sandbox_runtime/artifact_cli.py`
- Modify: `src/sandbox_harness/job_continuation.py`
- Test: `tests/test_store.py`
- Test: `tests/test_gateway.py`
- Create: `tests/test_artifact_cli.py`

**Interfaces:**
- Consumes: `JobArtifactRow`, signed Turn claims, `ObjectStore.iter_bytes` and existing `sandbox-artifact` executable.
- Produces: internal `Store.get_reviewed_job_artifact_for_thread(artifact_id: str, thread_id: str) -> JobArtifactOutput | None`; a separate redacted response for `GET /v1/job-artifacts/{artifact_id}/content`; `sandbox-artifact fetch --artifact-id ID --output PATH`.

- [ ] **Step 1: Write failing Store ownership/state tests**

Add tests proving reviewed same-Thread candidate/workspace/delivery rows resolve, while staged/unreviewed and foreign-Thread rows return `None`.

- [ ] **Step 2: Run Store tests and verify RED**

Run: `.venv/bin/pytest -q tests/test_store.py -k reviewed_job_artifact_for_thread`
Expected: FAIL because the method is absent.

- [ ] **Step 3: Implement the narrow Store lookup**

Join `JobArtifactRow -> JobRow`, require exact `thread_id` and `state == "reviewed"`, return `job_artifact_view(row)`, and expose no owner/object-store credential.

- [ ] **Step 4: Write failing Gateway streaming and authorization tests**

Cover same-Thread content/MIME/length/SHA-256/filename, foreign Thread 404,
`scope=job` 404, unreviewed 404 and missing object 503. Assert candidate bytes
are streamed without changing the JobArtifact row and `object_ref` is absent
from every response header/body.

- [ ] **Step 5: Implement the Gateway fetch route**

Add `runtime_fetch_job_artifact(artifact_id: str, request: Request) -> StreamingResponse`; authorize the Turn token, use the Store lookup, stream with the configured maximum and safe `Content-Disposition`.

- [ ] **Step 6: Write failing CLI atomicity tests**

Test successful streaming to a new file, truncated response, same-length digest
mismatch, preservation of an existing destination, HTTP failure cleanup, and
refusal to overwrite a directory.

- [ ] **Step 7: Implement `sandbox-artifact fetch`**

Read `HARNESS_GATEWAY_URL` and the capability token, stream to
`PATH.<random>.partial` in the destination directory, require and validate
frozen `Content-Length` plus `X-Updream-Artifact-Sha256`, `fsync`, and
`os.replace` only on success.

- [ ] **Step 8: Add continuation guidance and run focused suites**

The continuation message must name `sandbox-artifact fetch --artifact-id ID --output PATH` for reviewed candidates and say to verify before copying to `/workspace/outputs`. Run: `.venv/bin/pytest -q tests/test_store.py tests/test_gateway.py tests/test_artifact_cli.py tests/test_job_continuation.py tests/test_runtime_code_bundle.py`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/sandbox_harness/store.py src/sandbox_harness/gateway.py src/sandbox_runtime/artifact_cli.py src/sandbox_harness/job_continuation.py tests/test_store.py tests/test_gateway.py tests/test_artifact_cli.py tests/test_job_continuation.py
git commit -m "feat: fetch reviewed child artifacts for publication"
```

### Task 3: Align Child Catalog Grants And PPT Runtime Tooling

**Files:**
- Modify: `src/sandbox_harness/harness_job.py`
- Modify: `src/sandbox_harness/gateway.py`
- Create: `runtime/skills/pptx/scripts/create_montage.py`
- Modify: `runtime/skills/pptx/SKILL.md`
- Test: `tests/test_gateway.py`
- Test: `tests/test_harness_job.py`
- Test: `tests/test_runtime_skills.py`
- Test: `tests/test_runtime_bundle.py`

**Interfaces:**
- Consumes: signed Job claims (`descriptor_id/version/digest`, `skill_ids`, `operation_ids`, `capability_ids`, `artifact_ids`) and `execution_catalog.public_execution_metadata(...)`.
- Produces: filtered Job-scope execution catalog; `create_montage.py --input_dir DIR --output_file FILE --label_mode filename|none`.

- [ ] **Step 1: Write failing filtered-catalog security tests**

Assert every Child Harness grant freezes the catalog route plus `check`, and a
Job token passes the normal grant verifier to read only its signed descriptor
identity/version/digest, Skills, Operations, Capabilities and Artifacts. It sees
no `child_execution_profiles`, cannot see another Artifact, and remains denied
on an unrelated route.

- [ ] **Step 2: Run Gateway tests and verify RED**

Run: `.venv/bin/pytest -q tests/test_gateway.py -k job_catalog`
Expected: FAIL with route outside grant scope.

- [ ] **Step 3: Implement the catalog-only Job exception and response filter**

Add the catalog route and `check` operation when issuing every Child Harness
grant, then use the unchanged `authorize_runtime_request` and grant-call
verification. Filter the response after authorization. Keep all
submit/provider routes unchanged.

- [ ] **Step 4: Write failing montage helper tests**

Create controlled PNGs and assert deterministic labeled grid dimensions,
alpha-safe RGB/RGBA handling, filename labels, rejection of symlinks/non-images
and decompression bombs, atomic output, and the exact 64-file, 512-MiB,
200-megapixel and 16,384-edge caps.

- [ ] **Step 5: Implement the Pillow montage helper and Skill guidance**

Use natural filename order, fixed padding/cell calculation, default system-safe font, and a temporary sibling output. Document the helper and explicitly state that the ImageMagick `montage` binary is not guaranteed.

- [ ] **Step 6: Verify Runtime bundle inclusion and focused suites**

Run: `.venv/bin/pytest -q tests/test_gateway.py tests/test_harness_job.py tests/test_runtime_skills.py tests/test_runtime_bundle.py`
Expected: PASS and manifest coverage includes the new script.

- [ ] **Step 7: Commit**

```bash
git add src/sandbox_harness/harness_job.py src/sandbox_harness/gateway.py runtime/skills/pptx tests/test_gateway.py tests/test_harness_job.py tests/test_runtime_skills.py tests/test_runtime_bundle.py
git commit -m "fix: align child catalog and ppt runtime tooling"
```

### Task 4: Keep Child Model Activity Out Of Primary Items

**Files:**
- Modify: `src/sandbox_harness/model_activity.py`
- Test: `tests/test_model_activity.py`
- Test: `tests/test_execution_graph.py`

**Interfaces:**
- Consumes: signed claims with optional non-empty `job_id` and existing telemetry spans/events.
- Produces: Primary `ModelActivity` persistence unchanged; Job-scoped activity has `item_id=None` and telemetry/usage unchanged.

- [ ] **Step 1: Write failing Job-scope activity isolation test**

Create a real Store/Thread/Turn, run `ModelActivity` with `job_id`, feed output and finish. Assert zero new Primary `plan` Items, no primary step events, and captured telemetry still contains model usage and `job_id`.

- [ ] **Step 2: Run model tests and verify RED**

Run: `.venv/bin/pytest -q tests/test_model_activity.py`
Expected: FAIL because a Primary Item is created.

- [ ] **Step 3: Skip Item persistence only for Job claims**

In `ModelActivity.start`, do not call `persist_runtime_event` when `claims.job_id` is non-empty. Preserve span creation, output probing, usage and finish telemetry.

- [ ] **Step 4: Verify Primary behavior and execution graph**

Run: `.venv/bin/pytest -q tests/test_model_activity.py tests/test_execution_graph.py tests/test_job_activity_projection.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sandbox_harness/model_activity.py tests/test_model_activity.py tests/test_execution_graph.py
git commit -m "fix: isolate child model activity from thread timeline"
```

### Task 5: Make Reused-Sandbox Diagnostics Incremental And Proxy Errors Actionable

**Files:**
- Modify: `src/sandbox_harness/broker.py`
- Modify: `src/sandbox_harness/config.py`
- Modify: `src/sandbox_harness/gateway.py`
- Modify: `src/sandbox_harness/public_proxy.py`
- Test: `tests/test_broker.py`
- Test: `tests/test_public_proxy.py`
- Test: `tests/test_observability.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: Runtime `diagnostics(sandbox, after)` snapshots and monotonic cumulative counters.
- Produces: bounded per-Sandbox Runtime-instance/cursor/baseline state; `runtime.logs.collected` delta fields; warning-level expected connect failures with safe attributes.

- [ ] **Step 1: Write failing two-window diagnostic tests**

Assert the second collection starts at the previous cursor, imports only new records, reports counter deltas, and does not warn merely because lifetime counters are nonzero.

- [ ] **Step 2: Add failing reset/rotation/bound tests**

Cover Runtime `instance_id` change with sequence restart, counters decreasing,
`retained_from` advancing beyond the cursor, and state eviction after more than
512 distinct Sandbox IDs.

- [ ] **Step 3: Implement bounded diagnostic collection state**

Store Runtime instance ID, cursor and last counters in an
`OrderedDict[str, RuntimeDiagnosticState]`; reset cursor and baseline on
instance change, compute nonnegative deltas, reset baselines on counter reset,
advance through rotated windows, and emit reason `runtime_turn_delta`.

- [ ] **Step 4: Write failing proxy severity/redaction tests**

Patch the new connect helper/open call to raise `ConnectionRefusedError`,
unreachable `OSError`, its 10-second timeout, relay `OSError`, and an unexpected
`RuntimeError`. Assert only helper-raised connect failures are WARNING with
`failure_stage=connect`, exception type and address family; relay/unexpected
failure is ERROR; no host/URL appears.

- [ ] **Step 5: Implement safe proxy classification**

Add `Settings.public_proxy_connect_timeout_seconds` with default `10`, range
`1..60`, and pass it to both proxy listeners. Track only whether resolution
produced IPv4 or IPv6 and the current stage. Emit `public_proxy.unreachable` at
warning only for exceptions from the connect helper; retain
`public_proxy.failed` at error for relay and unexpected exceptions. Keep the
existing 502/403/408 client contract.

- [ ] **Step 6: Run focused suites**

Run: `.venv/bin/pytest -q tests/test_broker.py tests/test_public_proxy.py tests/test_observability.py tests/test_config.py`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sandbox_harness/broker.py src/sandbox_harness/config.py src/sandbox_harness/gateway.py src/sandbox_harness/public_proxy.py tests/test_broker.py tests/test_public_proxy.py tests/test_observability.py tests/test_config.py
git commit -m "fix: bound sandbox diagnostics and proxy warnings"
```

### Task 6: Documentation, Real Regression And Ali Release

**Files:**
- Modify: `docs/development/testing-and-real-cases.md`
- Modify: `docs/development/sandbox-and-harness.md`
- Create: `docs/test-evidence/execution-quality-hardening-report.md`
- Modify: `tests/run_real_campaign.py` or focused real runner only if needed to express the candidate-fetch case generically.

**Interfaces:**
- Consumes: Tasks 1-5 behavior and evidence.
- Produces: current behavioral documentation, revision-bound evidence and deployed Ali release.

- [ ] **Step 1: Document explicit disposition, fetch publication and scoped activity**

Describe current implemented behavior with source/test links; do not document the future Ali adapter as implemented.

- [ ] **Step 2: Run focused regression suites**

Run the combined tests from Tasks 1-5 plus sandbox provider contracts. Expected: PASS.

- [ ] **Step 3: Run the complete Python and frontend suites**

Run: `.venv/bin/pytest -q`
Expected: all tests pass.

Run: `npm --prefix web test && npm --prefix web run build`
Expected: 19 frontend tests and production build pass.

- [ ] **Step 4: Commit documentation and evidence structure**

```bash
git add docs/development docs/test-evidence tests
git commit -m "docs: record execution quality hardening"
```

- [ ] **Step 5: Run revision-bound real cases**

Run SP01-SP04, K01, K02 and K12 against the release candidate. Add a focused candidate-fetch publication case that proves no replacement Child Job is created and the fetched bytes become one delivery Artifact. Expected: all program classifications are success and cleanup is empty.

- [ ] **Step 6: Run the release gate**

Run the full gate using the established separate Python evidence when the live Ali PostgreSQL port conflicts with the gate database. Expected: compile, bundles, shell, frontend, systemd and all non-live browser scripts pass on the exact revision.

- [ ] **Step 7: Deploy only after active-Turn preflight**

Confirm zero nonterminal Ali Turns/Jobs, run service-access checks, record the release, restart only affected Ali services, and verify API/Gateway/Job Worker health, direct/FRPC ingress, bundle digests, warm pool and service restart counts.

- [ ] **Step 8: Push after deployed verification**

Push `sandbox-harness-platform-parallel-ali` only when local HEAD, deployed revision and remote branch are equal.
