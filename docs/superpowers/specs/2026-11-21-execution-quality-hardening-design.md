# Execution Quality Hardening Design

## Intent

Remove the avoidable retries, duplicate Child Harness execution, misleading tool
errors and cumulative Sandbox noise observed in Thread
`2e8637bbafef48449810f0f8fe816096`. The fixes land only on the isolated Ali
branch and deployment. The predecessor Parallel checkout and deployment remain
read-only evidence sources.

## Evidence

The observed Thread completed successfully and used one healthy Sandbox for all
three internal Turns. All four Child Jobs succeeded on their first attempt.
However, the first reviewed PPTX and two PNG outputs were frozen as `candidate`,
so a continuation launched a second two-Job group to recreate them as
`delivery`. The duplicate group consumed 427 seconds, 51,056 ordinary input
tokens, 911,744 cache-read tokens and 28,211 output tokens.

The same run also exposed five Primary tool failures, five recoverable Child
command failures, 50 Child model-activity Items projected into the Primary
Thread, repeated cumulative Runtime diagnostic imports, and expected public
proxy connection failures logged as infrastructure errors.

## Goals

- Make artifact-producing delegation choose `candidate`, `workspace` or
  `delivery` explicitly before submission.
- Let a later Primary Turn fetch a same-Thread reviewed Job Artifact and publish
  the existing bytes without rerunning the Child Job or mutating the frozen Job
  contract.
- Make `sandbox-job inspect` explain its Job-only grant boundary before a doomed
  network request.
- Give Child Harnesses a filtered read-only execution catalog rather than a
  scope error, without exposing unrelated artifacts, Skills, Operations,
  Capabilities or Child profiles.
- Provide the montage helper expected by common PPT workflows and verify every
  bundled Skill script referenced by this change exists in the Runtime bundle.
- Keep Child model timing out of Primary Thread Items while retaining Job
  activities, telemetry and token accounting.
- Import Runtime diagnostic records incrementally per reused Sandbox and report
  counter deltas per collection.
- Classify ordinary public-proxy connect failures as bounded warnings with safe
  stage/type attributes, while preserving errors for unexpected proxy defects.

## Non-Goals

- Change OpenSandbox or Aliyun Sandbox lifecycle semantics.
- Modify the original Parallel deployment or its historical Thread.
- Automatically infer that every artifact is a delivery.
- Reclassify a frozen `candidate` JobArtifact row in place.
- Expose object-store credentials, raw URLs, prompts, commands or file contents.
- Start the Aliyun Sandbox adapter in the same change.

## Approaches

### Explicit contract plus same-byte publication (selected)

Artifact-producing compact delegation requires an explicit disposition. A new
authenticated fetch endpoint streams reviewed same-Thread Job Artifact bytes to
the Primary Sandbox. `sandbox-artifact fetch` writes those bytes to a caller
selected path, verifies both frozen length and SHA-256, then copies a verified
file into `/workspace/outputs` to create a new Turn publication while preserving
the original Job contract and object identity. This prevents duplicate compute
without weakening authorization or auditability.

### Default all artifact delegation to delivery

This avoids the observed omission but silently publishes intermediate or
unreviewed variants. It removes an intentional safety decision and makes later
multi-stage workflows harder to reason about. Rejected.

### Instructions-only correction

The existing instructions already say final outputs must be `delivery`, yet the
observed model omitted it. More prose alone cannot make the invalid state
unrepresentable and does not provide a recovery path for existing candidates.
Rejected.

## Artifact Contract

`DelegationTaskSpec.output_disposition` becomes optional only at parse time.
A model validator requires it whenever `result_mode` is `artifacts` or `mixed`;
`text` tasks have no output collection and may omit it. Validation fails before
creating a draft or Job; text tasks reject a supplied disposition rather than
silently ignoring it. The example and error text show a final artifact task
with `output_disposition: delivery`.

The Store adds an internal same-Thread lookup returning a reviewed
`JobArtifactOutput`. The Gateway exposes
`GET /v1/job-artifacts/{artifact_id}/content` only for a valid `scope=turn`
capability whose `thread_id` owns the Job. A Job-scoped token receives the same
404-style denial as a foreign Thread. Candidate, workspace and
delivery artifacts are readable; non-reviewed, foreign-Thread and missing
artifacts are indistinguishable as 404. The response streams from the object
store with the frozen MIME, size, SHA-256 and safe filename. The endpoint uses a
separate redacted response rather than serializing the internal model, which
contains `object_ref`. No object reference or credential is returned.

`sandbox-artifact fetch --artifact-id ID --output PATH` uses the Gateway and
capability token, streams to a sibling temporary file, verifies the declared
length and SHA-256, then atomically replaces the destination. It never writes
partial output on failure. The continuation message tells the Primary to fetch
a reviewed candidate rather than rebuild it.

## Capability And CLI Boundary

`sandbox-job inspect` remains the inline `artifact.inspect` Operation. In a
Primary Sandbox it exits locally with a stable explanation that it is not a
generic artifact reader and names `sandbox-artifact fetch`. In a Job Sandbox it
continues using the frozen Operation and artifact grants.

Every Child Harness grant includes `/v1/execution/catalog` plus the frozen
`check` operation. Catalog requests therefore traverse the existing signature,
expiry, DB revocation, request quota, current Job generation and exact grant
verification path; there is no authorization bypass. The response is reduced
to the current Job's authorized descriptor identity, Skills, Operations,
Capabilities and input Artifact IDs. It contains no Child execution profiles
and therefore cannot enable nested delegation. Other Gateway routes retain the
existing grant verification.

## Runtime Skill Consistency

Add `runtime/skills/pptx/scripts/create_montage.py` with the observed stable CLI:
`--input_dir`, `--output_file` and `--label_mode filename|none`. It uses Pillow,
accepts at most 64 regular raster files, 512 MiB total encoded input, 200
megapixels decoded input and a 16,384-pixel output edge. It rejects symlinks and
Pillow decompression-bomb inputs, labels deterministically, bounds dimensions
and atomically writes PNG/JPEG output. The PPTX Skill names this helper and
states that the ImageMagick `montage` executable is not part of the contract.

Runtime Skill tests execute the helper on controlled images and verify the
bundle manifest includes it. No package is installed on the root filesystem.

## Activity And Diagnostics

`ModelActivity` recognizes signed Job claims. Job-scoped model requests still
emit tracing, usage and error telemetry, but do not call
`Store.persist_runtime_event` or create Primary Thread `plan` Items. The Child
Runtime event stream remains the authoritative source for `job_activities`.

Broker diagnostic collection keeps a bounded Runtime instance ID, cursor and
counter baseline per Sandbox ID. Reused Sandboxes request only records after
the previous cursor. A Runtime instance change resets the cursor and baseline
and fetches the new instance from zero; counter reset and retained-window
rotation are likewise explicit baseline transitions.
Collected telemetry reports `runtime_turn_delta`; warnings depend on positive
deltas for dropped records, export failures or disk errors, not lifetime totals.
Counter reset is handled as a new baseline. State is bounded and removed when a
Sandbox is no longer managed.

Both public proxy `open_connection` call sites use one helper with a configurable
10-second connect timeout. Refusal, unreachable-network and timeout raised by
that helper are expected external failures and log at warning level with
allowlisted `failure_stage=connect`, exception type and address family only.
Relay, body, write and unexpected exceptions remain errors. Raw destinations
and URLs remain excluded.

## Testing

- Delegation tests prove artifact/mixed requests without an explicit
  disposition fail before submission and text requests remain compatible.
- Store/Gateway/CLI tests prove same-Thread reviewed fetch, foreign and Job-token
  denial, non-reviewed denial, streaming, length plus SHA-256 verification,
  atomic replacement and failed-download cleanup.
- Gateway tests prove Job catalog filtering and continued denial of ungranted
  routes and nested Child profiles.
- Runtime Skill tests execute the montage helper and verify bundle inclusion.
- Model activity tests prove Job calls create no Primary Item while Primary calls
  retain existing behavior and telemetry.
- Broker tests collect two diagnostic windows from one Sandbox and assert cursor
  advancement, delta counters and bounded state.
- Public proxy tests assert expected connect failures are warnings, unexpected
  failures remain errors and no destination is logged.
- Focused suites, the full Python suite, frontend tests/build, browser gate and
  real K01/K02/K12 continuation cases run before release.

## Release

Commit documentation before the final gate. Rebuild the Runtime Code/Content
bundles because CLI, instructions and Skill content change. Do not rebuild base
images because all new runtime dependencies already exist. Confirm no active
Ali Turns before restarting affected API, Gateway, Job Worker and Runtime-facing
services. Verify the deployed revision, direct/FRPC ingress, warm pool, one
candidate-fetch publication flow and the targeted real cases before pushing.
