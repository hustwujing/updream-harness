# sandbox-harness-platform-parallel-ali

A thin, stateful sandbox platform that sends every user message to a Codex,
Claude or DeepSeek Primary Harness. The Primary Harness owns the conversation
and may delegate one dependency-free JobGroup to parallel Child Harnesses,
Operations or Providers. Durable Continuations return bounded results to the
same native session; optional Reviewer Jobs gate publication without becoming a
second planner. The platform restores the workspace, streams public output and
semantic steps, exports files, checkpoints the environment and removes the
sandbox.

The configured `SandboxBackend` supports OpenSandbox and Ali Sandbox. Lifecycle,
submission, recovery and cleanup state belongs to the platform database; a
provider failure never falls back to a host Agent.
To add another sandbox backend, follow
[Adding a Sandbox Provider](docs/development/add-sandbox-provider.md).

## Verified Ali baseline

The public repository is [hustwujing/updream-harness](https://github.com/hustwujing/updream-harness)
on branch `sandbox-harness-platform-parallel-ali`.

The current modular Ali baseline contains 196 cases and is
`composite_verified` at `293cff7`: 196 successes, zero program failures and zero
non-program failures, with each retained result preserving its actual tested
revision. See the
[196-case asynchronous execution report](docs/test-evidence/ali-async-execution-196-case-report.md).
The prior
[193-case runtime-recovery report](docs/test-evidence/ali-runtime-recovery-193-case-report.md)
remains the predecessor baseline.
The earlier [175-case cross-database report](docs/test-evidence/ali-cross-database-175-case-report.md)
remains the split-database baseline.

`EVO01`-`EVO18` cover API/PlatformWorker
restart separation, leader failover, multi-replica claims, bounded Outbox delivery,
metrics, Trace links, key rotation, fair admission, Provider circuits, object GC,
usage replay, public errors, authoritative browser stages, Gateway budget recovery
after SIGKILL, generation-correct Media cancellation after JobWorker takeover,
replayable Runtime restore, exact-once recovery of an undelivered internal completion,
and cross-database Direct Media promotion before Primary continuation.

The candidate deploys as nine independently restartable services:
API, PlatformWorker, sandbox-gateway, JobWorker, AllocationWorker, ControlledWorker,
MediaWorker, SiteWorker and image-inference. Eight Python services resolve immutable descriptors
under `.data/service-current/` and run from per-service source digests under
`.data/service-releases/`; image-inference retains its separately materialized
model/runtime boundary. Accounts, core execution, media and sites use four distinct
PostgreSQL databases and roles. Cross-domain work uses durable ordered receipts and
a Core-coordinated lifecycle protocol. The deployed MediaWorker release is
`5586caff7564d950779d2341cee97e497c5304ec3473b47f915593c0968cfcd3`; its
cancel fence discards terminal Provider results before publication while still
calling a real external cancel adapter when a durable Provider job ID exists.
image-inference resolves the separately hashed external release
`cec471fd7ba8ba8d7ecbbf369cbbe98321891074a003096fc54459a72ea2fd9a`
without copying its 29 GiB model files.

The deployment contains no legacy monolith database or predecessor deployment
scripts. The four domain databases were recreated after sandbox inventory
reached zero; they contain only current schema/service state. Each application
process receives only its declared database credentials.

## Services

```text
Web → Session API → PostgreSQL Turn → PlatformWorker → Warm Sandbox Pool → SandboxBackend
                                             │                         │
                                             └──── Capability Gateway ◄── Harness

Media confirmation → Session API → PostgreSQL Job → Media Worker → Provider
                                      ├── reference → public BOSS → Ark Asset
                                      └── terminal Outbox → Broker continuation

Harness delegation → Job Coordinator → PostgreSQL JobGroup → Job Worker
                                             ├── Child Harness / Operation
                                             ├── optional Reviewer Gate
                                             └── all-terminal Outbox → Primary Harness

Job sandbox allocation → PostgreSQL allocation ledger → Allocation Worker
                                                        └── configured SandboxBackend

Site confirmation  → Session API → PostgreSQL Job → Site Worker → Object storage
                                                        └── immutable version → public route

PostgreSQL accounts: users, sessions and registration claims
PostgreSQL core: Threads, Turns, Jobs, artifacts and sandbox ownership
PostgreSQL media: media drafts, provider executions, outputs and references
PostgreSQL sites: site drafts, deployment Jobs and immutable versions
Redis: live event fan-out and short-lived capability revocation
Object storage: content-addressed checkpoints and artifacts
```

API is a horizontally scalable request plane. It owns browser identity, owner
checks, SSE, file endpoints and public projections; it does not execute Turn or
singleton background loops. PlatformWorker replicas claim durable Turn and
continuation work. One database-fenced leader owns warm-pool convergence,
cross-domain relay, expiry, orphan cleanup, model discovery and object GC.

The Ali checkout and branch are both `sandbox-harness-platform-parallel-ali`.
Its working directory is `/data/wujing02/sandbox-harness-platform-parallel-ali`,
API is `127.0.0.1:24767`, PostgreSQL is `127.0.0.1:28433`, and ingress is
`/sandbox-harness-platform-parallel-ali/`. Use `deploy/ali/` for this deployment.
Database identity, Redis prefix, namespace, object prefix, ports and service
units are isolated. Shared executables may be reused; writable state, caches,
logs and temporary files stay in this checkout's `.data` or another explicit
`/data/wujing02` path.

The Capability Gateway is an infrastructure proxy. It injects model and Provider credentials after validating a Turn-scoped token. It does not understand prompts, plans or media quality.

Model accounting is protocol-aware across Codex, Claude and DeepSeek. The
Gateway records ordinary input, cache-read input, cache-write input and output
separately, then settles the durable Turn budget with configured cache weights.
The execution graph also projects each payload-free model request as an ordered
round for its Primary or Child Harness. Selected Harness details show completed,
active, failed and restart-interrupted rounds with first-output latency, elapsed
time and per-round token usage; prompts and generated content never enter this
projection.
Logical input remains visible for context and throughput analysis. Codex uses
an explicit native local-compaction threshold; Claude Code and DeepSeek Harness
retain their native Session compaction. A local budget exhaustion is reported
as such and is never relabeled as upstream rate limiting.
Native model requests use a separate bounded 64 MiB ingress budget for inline
image history; other capability requests retain the 16 MiB default. Local
oversize failures have a durable model activity and an actionable public reason.

Media confirmation expires after the configured Draft TTL (24 hours by
default). Expiry never creates a Provider Job or billable side effect. A later
natural-language request starts a new Turn and may prepare a replacement Draft,
but it cannot confirm the expired Draft and does not replace the new explicit
confirmation card.

The Media Worker is a separate systemd service. API creates and queries Jobs;
the Worker alone submits external Provider requests, persists external IDs,
resumes polling after crashes and commits the Artifact, terminal state and
Outbox transaction. API and Worker can restart independently.

Every Media Provider has an exact registered contract under
`src/sandbox_harness/common/providers/`. The contract owns model capabilities,
public parameter metadata, reference validation and request normalization;
`MediaCatalog` only resolves generic descriptors. Ark, MiniMax, Tripo,
Happyhorse and Wan are isolated contracts. An unregistered Adapter fails
startup, so adding Kling requires a Kling contract and executor rather than a
branch in the shared Catalog or Worker state machine.

`image.decompose` uses that same MediaWorker execution path. JobWorker performs
only an atomic database handoff from the authorized Operation; MediaWorker owns
the HTTP submit, poll, cancel and multi-file download. The independent
image-layering inference service still owns its model and GPU execution.
The original Job owns Artifact contracts, Review and the sole continuation.
Exact provider IDs and staged output receipts survive Worker replacement;
an uncertain create is never replayed or matched to an unrelated active task.
See [media execution contracts](docs/development/media-generation.md) and the
[real migration runner](tests/run_real_image_decompose_media.py).

The generic Job Worker uses the same accepted-handle rule for Child Harnesses,
Operations and Providers. Submission identity is persisted before polling. A
transient collection failure retries the same completed external execution and
never resubmits business work. Deterministic Reviewer verdict/schema failures
terminalize immediately and atomically update the Review Gate.

All asynchronous Adapters run a side-effect-free `preflight()` before submit.
Program decisions use stable error codes. A bounded external reason is exposed
only when the Adapter creates the trusted sanitized error envelope; API task
details, execution graph and Primary continuation then receive the same
`error_reason` and guidance, while raw Provider bodies and internal diagnostic
detail remain private. The same projection applies to Provider, Operation,
Child Harness, Media and Site work.

Capacity waits restore submission intent within the same fenced attempt before
allocation. Temporary status HTTP failures keep polling the accepted handle.
Child preparation failures are distinguished from uncertain execution-start
responses, which retain their sandbox and reservation for reconciliation.
Child and Reviewer sandbox allocation intent is also durable before create.
An uncertain create reconciles the original allocation without accepting
Runtime work or releasing capacity. Replacement workers persist discovered
handles and confirmed absence before retirement/retry; cancellation remains
effective while allocation is pending.

With the allocation dispatcher enabled, a separate Allocation Worker owns the
provider create RPC and its durable response. Creation and reconciliation have
independent bounded scheduling, with a 30-second allocation lease renewed every
10 seconds and durable fair reconciliation backoff. New allocations freeze a
90-second uncertain-create window and a 300-second provisioning TTL; adoption
renews the normal TTL before business execution. Only an unstarted Job may use
one additional allocation, with separate capacity and retained cleanup of the
old generation. Local, node or provider capacity exhaustion fails that recovery
explicitly; ordinary capacity waits keep their existing behavior. JobWorker
restores retained Pool slots before admission after restart. Empty inventory or
TTL expiry never releases uncertain ownership. Runtime submission/acceptance
continues to prohibit business replay. Explicit replacement allocations
retire an incompatible Thread attachment before acquiring a new sandbox.
JobWorker cleanup continues under full admission load, and bounded recovery
observations reuse already accepted executions without submitting new work.
Broker cancellation settles in-flight database operations and avoids repeatedly
cancelling a task already handling cancellation. Current recovery evidence is
included in the prior 175-case report above; the current candidate adds the EVO gate.

The execution catalog publishes dynamic readiness. Unavailable Operations are
rejected before confirmation or Job creation, and Provider references, modes
and artifact bytes are preflighted before the user authorizes side effects.

`image.segment` and `audio.transcribe` run as controlled host Operations inside
bwrap rather than a general Sandbox Harness. Their complete environments,
models and import trees are frozen in a project-owned manifest-addressed
snapshot, mounted read-only at `/runtime`, and validated on every Worker start.
GPU inventory is probed dynamically and reservations use discovered GPU IDs;
no physical card is fixed in configuration.

The Site Worker is another independent service. A Harness packages a complete
static site with `sandbox-site prepare`; user confirmation creates a durable
deployment Job. The Worker validates and uploads content-addressed files, then
atomically activates an immutable version. Published pages survive Sandbox,
API, Worker and browser restarts and support rollback and downline.

Deployment remains discoverable if a Harness omits the CLI call: after a
successful Turn, Broker detects each exported directory containing
`index.html`, builds the same content-addressed draft and deduplicates it by
bundle digest. This is still only a confirmation draft; no public version is
activated until the user chooses **部署站点**. The HTML Artifact opens as an
interactive owner-scoped preview in a CSP-sandboxed iframe instead of source
text. The Ali deployment publishes versions under
`/sandbox-harness-platform-parallel-ali/sites/<slug>/`, and Workspace
provides the stable link, immutable history, rollback and downline controls.

Seedream supports immutable single/multi-image references through stable public
BOSS URLs. Seedance supports first frame, first/last frame, multimodal reference,
video editing and extension; image/video/audio inputs are registered as Ark
Assets and submitted only after they become `Active`. Project files, BOSS
publication and Ark Asset IDs are cached by content digest and survive Worker
restart without duplicate external submissions.

Tripo v3 supports confirmed text-to-3D, image-to-3D, multiview-to-3D, texture,
decimation and conversion Jobs. The Worker persists the Tripo task ID before
polling, resumes that task after restart and atomically publishes the model and
preview. GLB files open in an interactive Three.js preview; every output remains
available for download and later Harness work.

Direct browser access keeps the existing signed anonymous identity. The FRPC
entry is identified through PROXY Protocol/nginx and requires an account; an
account Cookie resolves to the same stable owner from either entry. Users can
change username/password from the UI, while account creation/reset/disable uses
the `sandbox-harness-user` host CLI.

## Turn Lifecycle

```text
QUEUED → RESTORING → STARTING → RUNNING
                                ├─→ WAITING_USER result
                                ├─→ CHECKPOINTING → COMPLETED result
                                ├─→ FAILED result
                                └─→ CANCELLED result
result → CLEANING → terminal state
```

Only one Turn may run for a Thread. A used sandbox may stay attached to that
same Thread for a short TTL after its workspace checkpoint commits; after
expiry, the sandbox is deleted and its capacity is released.

Turn ownership is a renewable PostgreSQL lease. The Sandbox Runtime consumes Harness stdout in a background task and keeps a sequenced replay journal independently of the Broker HTTP connection. A replacement Broker can reattach the same namespaced Sandbox, replay only missing Runtime events, checkpoint the result and finish cleanup. If the Runtime or Sandbox itself is lost after Harness execution began, the broker reports an interrupted Turn instead of replaying potentially non-idempotent public side effects.

All three native Harnesses publish public assistant output through the same
durable `text_delta` event contract. Codex maps app-server agent-message deltas,
Claude maps stream-json text deltas, and DeepSeek maps committed SDK
`assistant/message` notifications. A terminal full response is retained only as
a fallback snapshot and is suppressed when that input already streamed text;
the deduplication boundary resets for every appended input.

Harnesses can request user decisions with `sandbox-interact`; Codex native user-input and approval requests map to the same durable Interaction protocol. Long commands can be delegated to `sandbox-background`, whose process group, bounded log, cancellation and exit status are supervised by the Runtime and remain part of the Turn result.

Every Turn receives one pinned 32-Skill bundle shared by Codex, Claude and
DeepSeek. It includes diagram, documentation, editable/image-based PowerPoint,
four focused Superpowers workflows, three data-quality/SQL workflows and the
complete pinned `anthropics/skills` collection with declared runtime
dependencies. See [Runtime Skill Bundle](docs/skills.md).

## Workspace

```text
/workspace/project   persistent user workspace
/workspace/inbox     current message and attachments
/workspace/outputs   exported artifacts
/workspace/state     native Harness and Provider state
/workspace/tmp       disposable Turn data
/run/harness         ephemeral token and runtime configuration
```

Runtime UID/GID remain fixed at `10001`; the passwd username is configurable at
image build time with `SANDBOX_HARNESS_SANDBOX_USERNAME` and defaults to
`updream`. Changing the display name does not change checkpoint ownership.

Uploaded Turn attachments are versioned as managed project assets and exported outputs are versioned as managed project artifacts. Their immutable objects remain in object storage and are materialized under `/workspace/project/assets` and `/workspace/project/artifacts` after every checkpoint restore. They therefore survive a new Sandbox or a different Worker without retaining historical `inbox` or `outputs` directories.

Each Turn freezes an immutable ProjectFile snapshot before materialization. A
replacement uploaded while that Turn runs is available to the next Turn but
cannot change the current execution's inputs. Object size and SHA-256 are
verified before every materialization. `/workspace/outputs` carries a hidden
Turn batch identity; the Broker rejects stale output from another Turn.
Materializing a managed project file larger than 4 MiB uses bounded chunks.
Transient failures retry the same chunk; complete size and SHA-256 verification
precedes atomic publication. A newer write or managed-file reset fences an
unfinished upload, and replaying a commit receipt never republishes old bytes.
Smaller uploads retain the single-request protocol and their own temporary file.

The Workspace panel supports direct project uploads, rename, deletion, version
history, text-version comparison and restore-as-new-version. Deletion creates an
auditable tombstone; each Turn resets managed directories before materializing
its frozen snapshot, so stale files cannot survive in an attached Sandbox.

The Sandbox workbench adds owner-scoped live overview, files, processes,
metrics, Execd PTY terminal, private port preview and audit history. Inspection
does not expose sandbox provider credentials. Writable terminal and file editing use
an exclusive takeover state; release checkpoints the workspace and revokes its
short-lived network capability before Agent execution is re-enabled.
If terminal work changed `project/assets` or `project/artifacts`, release
requires an explicit choice: discard and rematerialize, preserve the changed
files under `project/work`, or register them as new managed versions.

The browser keeps the selected Thread in `?thread=<id>`, restores it after refresh and supports history navigation. Users may delete terminal Threads; deletion first removes any retained Session Sandbox, then removes the owner's conversation records. Images, videos, text files and interactive HTML sites open in ownership-checked previews, while every file retains an explicit download action. A Thread with an active published site must first downline that site before deletion.

When the latest Turn fails, the conversation shows one dismissible public
failure tip below the top bar. It uses only the sanitized Error Item, never raw
Turn diagnostics. Dismissal is remembered for that Turn; a later failure is
shown independently.

## Development

Configure an isolated local environment and the `.data` cache/temp exports in
[Getting started](docs/development/getting-started.md) before these commands:

```bash
uv sync --extra test --extra runtime-media
.venv/bin/pytest
npm --prefix web run build
.venv/bin/uvicorn sandbox_harness.services.api.app:create_app --factory --reload --port 22766
```

Set `SANDBOX_HARNESS_BACKEND=fake` only for local UI development and tests.
Production supports configured `opensandbox` or `aliyun` backends and never
falls back to a host Harness. Use the isolated setup and cache environment in
[Getting started](docs/development/getting-started.md).

Before changing code, read [AGENTS.md](AGENTS.md) and use the
[development guide index](docs/development/README.md) to load the rules for the
affected lifecycle, Gateway, Runtime, image, Skill, API or deployment boundary.

See [architecture.md](docs/architecture.md), [client API](docs/api.md), [data model](docs/data-model.md), [product interface](docs/product-interface.md), [gateway.md](docs/gateway.md), [Runtime Skill Bundle](docs/skills.md) and [deployment.md](docs/deployment.md).

Current deployment, cross-database, recovery and case evidence is consolidated
in the [196-case Ali report](docs/test-evidence/ali-async-execution-196-case-report.md).

## Runtime Toolchain

Runtime images have Bootstrap, Common, dependency Profile and native Harness
layers. `primary-full-cpu` contains the complete media, browser, data and Office
toolchain; `general-cpu`, `media-cpu`, `office`, `web-browser` and `data-analysis`
contain their declared subsets. A tool present in the complete profile is not
promised by every smaller profile. The signed catalog and each profile's
Dockerfile and requirements file define its dependencies.

Signed Runtime CLIs required by a profile also need image entrypoints so login
shells can find them after resetting `PATH`. These entrypoints dispatch to the
active signed Code Bundle; they do not duplicate CLI source in the image.
Image releases must verify commands/imports as a non-root user and validate a
representative output. The older foundation `toolchain.lock.json` describes
that foundation, not every current smaller profile. Deployed profile/Harness
images are pinned by registry digest.

Runtime Python and CLI ship as a signed Code Bundle; instructions and Skills
ship as a signed Content Bundle. A minimal image-resident Bootstrap verifies,
activates and supervises the Runtime child. A Turn freezes image and both
Bundle digests, while dependency, Bootstrap and native Harness changes still
use immutable image releases.

## Public Network Policy

Sandboxes have no direct public route. The Runtime injects a short-lived Turn capability into standard `HTTP_PROXY` and `HTTPS_PROXY` variables, so libraries such as Requests use the authenticated Gateway automatically. `sandbox-browser-screenshot URL OUTPUT` uses the same Gateway for the page, redirects and subresources; raw Chromium cannot bypass it.

The Gateway permits only public addresses on ports 80 and 443, rejects private, loopback, link-local, reserved and cloud-metadata addresses, and applies configurable destination rules after DNS resolution:

```dotenv
SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST='["bilibili.com","*.bilibili.com"]'
SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST='[]'
```

Rules may be exact domains, wildcard subdomains, individual IPs or CIDRs. The
allowlist restricts anonymous Turns. Turn submissions from any authenticated
account may reach every otherwise-valid public HTTP/S destination; the
denylist, private-address rejection, 80/443 port limit, request budget and
capability revocation still apply. curl, Requests, Chromium and Playwright use
the same authenticated proxy. See [gateway.md](docs/gateway.md) for the complete
contract.

Runtime boundary handling: execution creation and stdin submission are serialized. An input ID is bound to its original body; a replay returns the existing execution and conflicting content is rejected before attachment metadata changes. Streaming Harnesses must acknowledge their pending inputs: EOF alone is not success. Protocol failures and cancellation quiesce the Harness process group and supervised background jobs before publishing a terminal result; jobs ignoring SIGTERM are killed after a bounded grace period.

Stop requests are persisted on the Turn. The lease owner observes them through its heartbeat, and a replacement Broker honors them without replaying the Harness. Stop and final completion are serialized in the database; a stop accepted before completion prevents a later success from overriding it. The UI displays “正在停止” and disables further input while cleanup finishes. Targeted regression cases are in `tests/test_runtime_boundaries.py` and `tests/test_cancel_boundaries.py`.

Native interaction IDs exposed outside the sandbox are namespaced by execution; late answers cannot reach a later Harness process that reused the same native ID. Ending an execution also releases unanswered helper requests. Model discovery retains the last inventory if the upstream listing endpoint fails and still probes explicitly configured model routes. Redis connection and command stalls are bounded, so event persistence can fall back to database replay instead of waiting indefinitely.

The sidebar green dot means a completed Turn has not been viewed. Once the completed transcript is loaded in the focused, visible conversation, the client acknowledges that exact Turn through `POST /api/threads/{thread_id}/read`. The owner-scoped acknowledgement is stored on the Thread, survives reloads, and cannot pre-read a running Turn or acknowledge a newer completion through a stale request. Running Turns retain their spinner.

Structured lifecycle logs, OpenTelemetry traces and bounded metrics cover API, Broker, workers, Sandbox allocation, Gateway, provider HTTP, storage and Runtime. This repository's K8s application services write structured JSON to stdout and use the company-injected OTLP endpoint; local JSONL remains optional and configurable beneath `.data`. SRE owns the log agent, Collector, disk delivery queue, storage, dashboards and alerts. Durable Domain Envelopes and continuations retain Span Links across restart and delayed delivery. Harness stdout remains the user/business protocol, while separately drained stderr emits one credential-redacted tail record per execution (1 KiB by default, configurable to zero or at most 1 MiB). External Sandbox Runtime signals use the scope-checked Gateway because the SRE-managed cluster agent cannot collect them directly; Gateway validates Runtime Metrics and replaces Sandbox authorization with SRE-provided OTLP credentials. Use `.venv/bin/sandbox-harness-logs --turn TURN_ID` to assemble a timeline. See [logging, IDs, stderr limits, retention, export configuration and troubleshooting](docs/observability.md).

Disposable staging and telemetry directories are revalidated at each write and
may be recreated only below the configured data root. The supplemental `AS01`
real gate deletes Child and Controlled artifact staging roots while Workers are
running and verifies successful collection without duplicate external work.
