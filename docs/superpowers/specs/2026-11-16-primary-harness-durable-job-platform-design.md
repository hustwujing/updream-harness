# Primary Harness + Durable Job Platform Design

Date: 2026-11-16

Status: approved architecture direction. This document defines the target new project. The new
project uses `sandbox-harness-platform` as its implementation baseline and treats Plus only as a
source of selected protocols, UI ideas, tests, and failure evidence.

## Intent

Build a general-purpose controlled Agent platform that preserves the original project's stable
Thread/Turn/Native Harness execution model while adding durable mixed asynchronous jobs, controlled
child-Harness delegation, distributed CPU/GPU scheduling, explicit authorization, and inspectable
execution.

The platform must not require an LLM Manager to compile a complete business DAG before execution.
Planning and ordinary execution remain inside one continuous Primary Native Harness session. The
platform intervenes only at deterministic security, resource, side-effect, persistence, recovery,
validation, and publication boundaries.

## Goals

Deployment invariants:

- All persistent service storage is rooted under `/data2/wujing03/sandbox-harness-platform-parallel/.data`; the root partition must not store project databases, objects, caches, logs, workspaces, bundles, or mutable temporary state.
- Digest-pinned base images, immutable dependency layers and verified local caches should be reused. Downloads prefer configured domestic mirrors and remain lockfile/checksum verified. Mutable state, credentials, namespaces and storage prefixes are never shared with the predecessor environment.

- Preserve a continuous Codex, Claude, or DeepSeek Primary Harness session per Thread.
- Let the Primary Harness plan and execute incrementally in its native context.
- Unify Provider calls, host Operations, site/document work, and child Harness executions under a
  durable Job protocol.
- Support one mixed JobGroup containing concurrently runnable heterogeneous Jobs.
- Resume the same Thread's native Harness session through an idempotent continuation Turn when a
  JobGroup reaches its join condition.
- Accept user input at any time without broadcasting it to child Jobs or guessing intent through
  keywords or regular expressions.
- Preserve authorization, budgets, idempotency, reconciliation, cancellation, cleanup, Artifact
  identity, user-visible progress, and recovery across service restarts.
- Show an actual execution graph derived from durable events rather than a predicted planning DAG.
- Run independently from both predecessor deployments, storage, databases, ports, namespaces, and
  ingress paths.

## Non-Goals

- No mandatory LLM Manager, ManagerProposal, PlanCompiler, or initial/expansion/completion loop.
- No complete up-front business DAG.
- No conversion of ordinary shell commands, local tools, or file edits into platform WorkUnits.
- No default semantic Reviewer for every result.
- No independent Delivery entity, Manifest, state machine, or completion event.
- No unbounded recursive delegation.
- No case-specific behavior or natural-language keyword routing.
- No compatibility with either predecessor's APIs, schema, runtime state, or stored data.
- No database migration, downgrade, compatibility-window, or rolling-schema machinery before the
  first production deployment that must retain user data.

## Original-Project Baseline

The new repository starts from `sandbox-harness-platform`, not from Plus. Unless this design
explicitly replaces a boundary, it retains the original project's implemented behavior and tests:

- account and anonymous ownership, login/session isolation, Thread navigation, unread state, and
  archived/deleted Thread handling;
- Thread/Turn serialization, native Codex/Claude/DeepSeek input delivery, queued input status,
  checkpoint recovery, Runtime cursor replay, warm/attached Sandbox lifecycle, and bounded cleanup;
- upload and paste, immutable ProjectFiles, version history, Artifact preview/download, image,
  audio, video, PDF, Word, spreadsheet, presentation, HTML/site, and 3D preview behavior;
- Sandbox overview, files, terminal, processes, metrics, private port preview, takeover,
  save/release, and structural audit;
- scoped Gateway credentials, model routing, authenticated network proxy, public-network policy,
  revocation, token/request limits, and secret isolation;
- signed Runtime/Skill Bundles, thin foundation images, dynamically injected runtime code, the
  current Skill collection, and user-selected Harness/model configuration;
- existing Media and Site drafts, confirmations, Provider adapters, durable workers, output roles,
  static-site publication, and truthful Provider error handling;
- durable transcript Items/events, SSE reconnect/replay, model-wait and tool timing, structured
  public errors, telemetry, log retention, and browser recovery behavior.

The implementation may refactor these components behind new generic Job interfaces, but cannot
silently remove a working user capability or weaken its isolation, recovery, idempotency, or
observability contract. The new deployment uses independent state and does not copy predecessor
runtime data.

## System Shape

```text
User
  -> Thread / Turn
  -> continuous Primary Native Harness
       |- local commands and file work
       |- synchronous read-only Operation
       |- Durable JobGroup
       |    |- Provider Job
       |    |- Operation Job
       |    `- Harness Delegation Job
       |- Interaction / Decision
       `- Artifact Publish
  -> validation and delivery
```

The Primary Harness is the task brain. The control plane is deterministic and does not reinterpret
the user's business request.

## Modules

| Module | Responsibility |
|---|---|
| API / Web | Threads, chat, files, Interactions, Jobs, execution graph, takeover, and previews |
| Broker | Turn serialization, native input delivery, Runtime lifecycle, checkpoints, and continuation |
| Primary Runtime | Native Codex/Claude/DeepSeek session, Workspace, and normalized activity events |
| Capability Gateway | Scoped tokens, model/provider routes, network, budgets, revocation, and audit |
| Job Coordinator | Immutable parallel JobGroups, atomic scheduling, Outbox, and continuation |
| Job Workers | Provider, CPU/GPU Operation, Harness Delegation, Site, and Document executors |
| Scheduler / Node Agent | Profiles, resources, node inventory, reservations, Sandboxes, and GPUs |
| Object / Artifact Store | Content identity, ProjectFiles, Artifacts, validation, and delivery |

## Primary Harness And Turns

A Thread freezes its selected primary Harness and model configuration. A user message creates a
Turn. While a Runtime execution is live, appended input uses the original native mechanisms:

- Codex: `turn/steer`;
- Claude: streaming JSON input;
- DeepSeek: SDK Session delivery at a safe boundary.

Ordinary task planning, inspection, coding, media processing, validation, and response generation
happen in this one session. The platform does not ask another model to translate the task into a
large nested plan before work starts.

## Durable Job Protocol

All asynchronous execution boundaries share a common envelope and lifecycle:

```text
CREATED -> QUEUED -> RUNNING -> WAITING_EXTERNAL
                    |       `-> CANCELLING
                    |- SUCCEEDED
                    |- FAILED
                    |- CANCELLED
                    |- BLOCKED
                    `- UNKNOWN
```

`CREATED`, `QUEUED`, `RUNNING`, `WAITING_EXTERNAL`, and `CANCELLING` are nonterminal.
`SUCCEEDED`, `FAILED`, `CANCELLED`, `BLOCKED`, and `UNKNOWN` are terminal for immutable Group
completion. `UNKNOWN` means the accepted external side effect cannot currently be resolved; it is
never silently rewritten. A later operator/user reconciliation creates a new Job linked by
`reconciles_job_id`, reuses the known external handle, performs no new submission, and produces a
new factual continuation.

Every Job freezes:

- Job kind and Executor Adapter;
- idempotency key and immutable request digest;
- owner Thread, origin Turn, and immutable JobGroup identity;
- input Artifact identities and semantic roles;
- authorization and budget identity;
- expected output roles where a contract is required;
- resource/Profile requirements;
- retry, reconciliation, cancellation, and cleanup policy.

Every Adapter implements the same control surface:

```text
submit / status / cancel / collect / reconcile / cleanup
```

Each registered Operation Descriptor freezes `execution_mode=inline|durable`, version/digest,
parameter and input/output contracts, authorization, resources, network origins, side effects,
retry, cancellation, and reconciliation policy. The Harness cannot override that mode or select a
Worker, node, GPU, credential, retry count, or network destination.

An inline Operation must be strictly short-lived, read-only, non-billable, side-effect-free,
deterministic or safely idempotent, bounded by a small timeout, require no exclusive GPU, create no
large Artifact, and need no restart recovery. It executes as an immediate Primary Harness tool call
and returns structured data without a Job or continuation. Typical examples are media metadata,
Artifact lookup, and Job status.

Every long CPU/GPU operation, Artifact-producing operation, external API call, billable action,
side effect, cancellable activity, recoverable activity, or action with an uncertain outcome uses a
Durable Job. A Provider remains durable even when its HTTP API is synchronous. An inline timeout is
reported as that invocation's failure and is never dynamically converted into a durable retry,
because the Descriptor's side-effect boundary is authoritative.

## Job Types

| Kind | Executor | Important boundary |
|---|---|---|
| Provider | External service adapter | External handle, billing, polling/webhook, uncertain submission |
| Operation | Controlled CPU/GPU Worker | Deterministic implementation and declared filesystem/network effects |
| Harness | Child Native Harness in OpenSandbox | Model budget, Profile, Sandbox, events, checkpoint, and cleanup |
| Site/Document | Specialized Worker | Durable conversion/publication and immutable output |

The existing original-project Media and Site Jobs are the implementation reference. They are
generalized rather than replaced by the Plus Capability continuation state machine.

## Job Attempts And Acceptance

Every execution claim creates a fenced JobAttempt with worker/node identity, lease generation,
Executor kind, request digest, accepted-at receipt, external/native execution identity, start/end
timestamps, outcome, and error. Pre-accept infrastructure retries and post-accept business
replacement are separate budgets. A stale generation cannot publish a result, Artifact, budget
consumption, or cleanup decision.

Acceptance is Executor-specific:

- Provider: persist the request/idempotency intent before the call; a durable external handle or
  Provider idempotency receipt proves acceptance. A lost response with no queryable identity becomes
  terminal `UNKNOWN` and is never automatically resubmitted.
- Operation: the Worker durably records native-process acceptance before the first declared side
  effect. A Descriptor marked deterministic/idempotent may resume or retry the same request identity;
  a non-idempotent accepted operation with unresolved outcome becomes `UNKNOWN`.
- Harness: Runtime durably records the execution ID, assignment fingerprint, and native input
  acceptance receipt. Recovery reattaches that exact Runtime execution and sequence; it never starts
  another Harness or resends accepted input. Only confirmed pre-accept absence may reconstruct.

Business submission/model/GPU budget is consumed at the durable acceptance boundary, not at claim
time. Recovery generations inherit the original accepted business attempt and cannot consume the
same business allowance twice.

## Mixed JobGroups

The Primary Harness submits one atomic batch for N parallel tasks. A JobGroup may contain any mix
of Provider, Operation, and Harness Jobs.

Every member must have all immutable inputs available when the Group is committed and must be
immediately schedulable independently of every other member. A JobGroup cannot contain dependency
edges, output bindings to sibling Jobs, conditional branches, fan-in, or staged execution. The
contract has one fixed completion condition: every member reaches a terminal state. A failed,
cancelled, or unknown member does not cancel an independent sibling.

“Immediately schedulable” means logically ready without another Job result; it does not promise
instant physical capacity. Scheduler quotas may leave a ready member queued. The configured default
maximum is 32 Jobs per Group with a hard protocol maximum of 100, matching continuation bounds.

Work that requires a result from this wave is submitted by the Primary Harness in a later Group
after the factual continuation arrives. A deterministic sequential pipeline that needs no model
observation is implemented inside one registered Operation Job rather than as several dependent
Jobs.

The platform atomically validates the whole batch before acceptance:

- Artifact ownership and identity;
- registered Executor, Profile, Skill, Capability, and output roles;
- maximum Jobs, concurrency, delegation depth, CPU/GPU resources, and disk;
- billable Provider submissions and model/token budget;
- network and side-effect scope;
- proof that every input Artifact already exists and no inter-Job dependency field is present.

The platform chooses nodes, images, Sandboxes, and exact GPU devices. Harnesses never invent these
infrastructure identities.

An editable JobGroupDraft starts in `DRAFT` or `AWAITING_CONFIRMATION`; its immutable terminal
states are `CONFIRMED`, `REJECTED`, and `EXPIRED`.
Confirmation atomically creates an immutable Group and all member Jobs. The Group projection moves
through `COMMITTED`, `RUNNING`, `TERMINAL`, and `CONTINUED`; `TERMINAL` is derived only when every
member is terminal. Draft replay, edit, confirm, reject, and expiry use immutable request digests and
idempotency keys. A confirmed or rejected Draft cannot be edited.

## Harness Delegation

`delegate.submit_batch` creates Harness Jobs inside a JobGroup. Each child receives a frozen
objective, immutable Artifact inputs, a registered runtime Profile, expected outputs, and a minimal
grant.

Child Harness defaults:

- it cannot interact directly with the user;
- a missing business fact is returned as structured `BLOCKED` evidence to the parent;
- recursive delegation is disabled; the initial maximum delegation depth is one;
- it cannot access the parent's mutable Workspace;
- it publishes only collected immutable Artifacts;
- it exposes native model, command, tool, and file activities;
- it supports cancellation, checkpoint, bounded recovery, and exact resource cleanup.

`BLOCKED` is a terminal Child Job state distinct from `FAILED`. It contains a stable reason code,
summary, missing fields/questions, and whether a replacement can succeed with more input. The Child
terminates and releases its Sandbox instead of waiting for an answer. Other independent Group
members continue; the factual continuation delivers the blocked evidence to the Primary Harness,
which consolidates questions, uses the existing Interaction protocol when necessary, and submits an
explicit replacement Job after receiving new input.

Children have no user Interaction tool and cannot expand authorization or mutate a parent Group.
Missing Profile, Capability, Artifact authorization, budget, GPU, or network scope is a deterministic
preflight rejection, not runtime `BLOCKED`. Reviewer `EVIDENCE_REQUIRED` follows the same parent
return path and never asks the user directly.

Initial Harness delegation does not support Child steer. Changing a running Child requires an
idempotent cancellation request followed by an explicit replacement Job. This keeps the Job request
digest, accepted native input, output contract, and result identity immutable.

Child execution configuration is selected only through a registered `ChildExecutionProfile`. The
default `inherit-primary` profile freezes the parent Thread's Harness and model configuration. Other
profiles may select a deployed Codex, Claude, or DeepSeek model, cost class, resource ceiling,
allowed `RuntimeDependencyProfile` identities, and an optional explicit fallback list. The Primary
Harness sees and selects only authorized public profile IDs. It cannot provide a model name, Base
URL, credential, image, node, Sandbox ID, or GPU device identity.

Every selected Skill declares dependency capabilities such as commands, modules, browser/media/
office classes, and resources rather than naming an arbitrary image. The Resolver unions all Job
and Skill requirements and selects the smallest registered `RuntimeDependencyProfile` allowed by
the ChildExecutionProfile that fully satisfies the union. Profiles are never composed dynamically
at runtime. If no one registered image satisfies the complete set, Group preflight rejects the Job.

The Scheduler resolves both profiles to deployment configuration and resources. An unavailable
profile remains queued or fails with `profile_unavailable`; the platform never silently switches
models. A declared fallback may be used only within its frozen authorization and cost boundary. A
fallback requiring higher cost, GPU, network, or side-effect scope requires new user authorization.

## Continuation Turns

The target retains the original project's successor-Turn model. It does not introduce long-lived
`WAITING_JOBS` or `RESUMING` Turn states.

```text
Turn A submits JobGroup and completes
  -> Jobs execute independently
  -> every immutable group member reaches a terminal state
  -> durable Outbox creates internal Turn B
  -> Turn B resumes Thread.native_thread_id and the checkpointed Workspace
  -> Primary Harness receives the frozen Job result envelope
```

Each Group has one unique Outbox identity. A dispatch transaction locks the Thread and a bounded set
of ready Outbox rows, creates one immutable continuation Turn, binds every claimed Outbox to that
Turn, and marks them delivered in the same transaction. A crash commits all of these writes or none.
The continuation idempotency key is:

```text
job-continuation-{digest(sorted(outbox_ids))}
```

The origin Turn must be terminal before a continuation becomes eligible. If Jobs finish first, the
Outbox waits. Redis/event delivery is advisory; PostgreSQL state and Outbox rows are authoritative.

Committing an authorized JobGroup starts its Jobs without blocking the Primary Harness. The Primary
may continue local work that is independent of those results. It must not poll status, sleep while
waiting, or keep the Turn alive solely to await the Group. When its next useful step requires Job
results, it ends the current Turn normally; Runtime guidance states that an automatic continuation
will deliver the complete factual result envelope. Job completion never interrupts a still-running
origin Turn. The Outbox becomes dispatchable only when both the immutable Group is terminal and the
origin Turn is terminal.

If authorization is required, the Primary creates the editable JobGroupDraft, tells the user what
needs confirmation, and ends the Turn. It never waits inside the same Turn for the confirmation
action. Confirmation later freezes and starts the complete Group.

Each terminal JobGroup writes an independent transactional Outbox record. When the dispatcher is
ready to create a continuation, it atomically claims the currently ready, undelivered Outbox rows
for that Thread and combines their factual result envelopes into one immutable continuation Turn.
It does not add a debounce delay and does not wait for slower Groups. A Group that becomes terminal
after the claim is delivered by a later continuation. Once a continuation Turn exists, its payload
is never amended.

Continuation batching is bounded to 32 Groups, 100 Job results, and a 1 MiB structured payload;
overflow remains ready for the next dispatch. The continuation idempotency key is derived from the
sorted claimed Outbox identities. Each Outbox can be claimed and delivered exactly once. An active
user Turn is never interrupted; the immutable continuation enters normal Thread order behind Turns
that were already accepted.

## User Input While Jobs Run

If the origin Turn remains live, input uses native steer/stream delivery. If the origin Turn has
completed, the message creates a normal user Turn in the same Thread. The Thread active-Turn pointer
serializes that user Turn with any internal continuation Turn.

The Primary Harness alone interprets the input. The platform does not decide whether the message
belongs to an old or new business task, does not broadcast it to children, and does not
automatically cancel Jobs. The Harness may:

- answer chat or status without changing the group;
- add independent Jobs;
- cancel, steer, or replace affected Jobs;
- change final composition while retaining valid intermediates;
- cancel the complete group or task;
- request a structured user Interaction when a material fact is missing.

Every message retains sequence, idempotency, and visible queued/delivered/applied/failed state.

## Cancellation Scope

Cancellation has three explicit user-visible scopes:

- the composer stop button cancels only the current Primary Harness Turn and leaves previously
  submitted background Groups running;
- a Job or JobGroup control cancels exactly that Job or every nonterminal member of that Group;
- a confirmed Thread-level `stop all activity` cancels the active Turn and every nonterminal Job,
  stops child Harnesses, revokes grants and streams, and releases or deletes owned Sandboxes.

All cancellation requests are idempotent. A queued Job becomes `CANCELLED`. An Operation receives a
bounded process interruption. A Harness Job receives native cancellation followed by bounded
process termination and exact Sandbox cleanup. A Provider cancellation is confirmed through the
registered Adapter; unsupported cancellation is recorded as refused, and an uncertain outcome
remains terminal `UNKNOWN`. Any later query uses an explicit reconciliation Job and never mutates
the original Job or resubmits its side effect. The platform never reports cancellation merely
because the user requested it.

Cancelling one Job or Group does not delete completed or partial Artifacts. A Group with other
terminal facts may still create a normal factual continuation. An all-cancelled Group with no result
creates only a durable system Item and does not wake the Primary Harness. Thread-level stop-all
sets `suppress_continuation=true` on every current nonterminal Group and marks its eventual Outbox
`SUPPRESSED` rather than ready. Suppressed results remain part of the next explicit user Turn's
authoritative context but never auto-create a Turn and are never changed to ready merely by time.
Natural-language stop requests are interpreted by the Primary Harness and converted to exact Job
identities; the platform does not route them through keyword matching.

## Immutable JobGroups And Replacement

A committed JobGroup is immutable. It records only factual parallel membership, authorization,
and result identities. The platform does not model the semantic relationship between a later user
message and an earlier group.

When requirements change, the Primary Harness issues explicit operations instead of mutating a
group:

- request cancellation for Jobs that are no longer wanted;
- submit a replacement Job with `replaces_job_id` when an old result must be redone;
- submit a new immutable JobGroup for additional or differently structured work;
- retain and explicitly reference any still-useful immutable Artifact.

Every Job and JobGroup keeps stable identity and complete history. Unsupported or uncertain
cancellation never hides a later terminal result. Replacement lineage expresses an explicit
Harness action, not a platform inference about natural-language intent.

Every terminal JobGroup creates at most one idempotent factual continuation. The continuation says
what ran and what resulted; it never claims those results still satisfy the latest user intent. A
later conversation may use, ignore, cancel, replace, or combine those results. Continuation Turns
and user Turns are serialized by the existing Thread active-Turn pointer, so the same native
session observes both in an authoritative order without `SUPERSEDED` or `SUSPENDED` semantics.

## Interactions And Decisions

The original Interaction protocol remains authoritative. It already binds question/approval to a
Runtime execution, persists requests and idempotent responses, and resumes the native Harness.

The new system expands typed Interaction payloads and effects where needed, but does not import the
Plus Decision state machine. Child Harnesses report blocked evidence to the Primary Harness, which
owns all user-facing questions and consolidates them into one coherent Interaction.

## Authorization And Budgets

The original signed Capability Token remains the security foundation. It binds Thread, Turn,
Sandbox, Harness, model, routes, network access, TTL, request limits, token budget, and revocable
nonce.

Authorization is frozen at four scopes without importing Plus Run/WorkUnit concepts:

```text
Thread Authorization
  -> Turn Execution Grant
  -> JobGroup Authorization Snapshot
  -> Job Execution Grant
```

Thread Authorization is the user's current maximum approved scope. A Turn receives a short-lived
execution Grant. A committed Group freezes the authorization version and aggregate budget that made
the complete batch legal. Every Job receives a smaller Grant bound to its exact executor,
Descriptor version/digest, input Artifact identities, output contracts, Profile, network origins,
budget, Thread, origin Turn, Job, Sandbox/source identity, lease generation, TTL, and revocable
nonce. Later authorization never expands an already-running Job; only a new Group or replacement
may use the newer snapshot.

Authorization distinguishes persistent Thread scope from one-shot Group approval. Billable
submission counts, GPU/model concurrency, and other consumable budgets are never implicitly added to
the persistent Thread baseline. Confirming a Draft atomically locks the relevant budget ledger,
creates a Group-scoped authorization snapshot, reserves the maximum permitted consumption, and
creates the immutable Group/Jobs. Concurrent confirmations cannot over-reserve. Provider allowance
is consumed at the Adapter's durable acceptance receipt; non-consumed reservation is released when
the Group becomes terminal. Replacement consumption follows its frozen Descriptor policy. Every
reserve, consume, release, and reconciliation adjustment is an immutable ledger entry.

The target adds:

- per-Capability and per-Operation submission limits;
- Provider billable-call and replacement accounting;
- Harness/model and GPU concurrency budgets;
- JobGroup aggregate authorization;
- exact input Artifact, semantic `artifact_role`, and output disposition scope;
- reconciliation and cancellation permissions.

The Primary Harness may see public metadata for available registered Skills, Profiles,
Capabilities, and Operations so it can propose a JobGroupDraft, but it receives no execution right
until authorization succeeds. It cannot select a credential, Base URL, image, node, Sandbox, GPU,
or arbitrary model identifier. Provider, model, object-store, and host credentials remain in the
Gateway or Worker and are never injected into a Sandbox.

The Child Grant is the strict intersection of parent authorization, selected
ChildExecutionProfile support, resolved RuntimeDependencyProfile support, and the Job's frozen
declared needs. A Child cannot discover or use another input Artifact, Skill, Capability,
Operation, network origin, artifact role/disposition, model, or budget. It cannot request authority
expansion; preflight rejects an invalid Group before any member starts.

The signed Skill Catalog freezes Skill ID, version/digest, supported Harnesses, dependency
capabilities, commands/modules, required Capabilities, network, GPU/resources, and side effects. Runtime verifies
the signed Bundle and materializes only the Skill IDs in the current execution Grant. Skill content
cannot install an unknown dependency or expand its own permissions. The selected Skill set and
Bundle digest remain frozen for one Runtime execution; newly authorized Skills become available in
a later execution, not through silent live mutation.

On native Session resume, Runtime removes Skill paths absent from the new Grant, materializes the
new frozen set, and injects an authoritative capability-change system frame listing current Skill
IDs/digests and explicit revocations before accepting user or continuation input. Gateway checks
remain authoritative even if model memory mentions an old Skill. If a Harness cannot safely accept
that capability-change frame or its required RuntimeDependencyProfile is incompatible with the
checkpoint, the Broker starts a new native Session with a bounded handoff summary and the same
Thread Workspace instead of pretending the old Session was fully restored.

Capability and Operation Descriptors freeze ID, version/digest, operations, parameter and Artifact
fields, output contracts, authorization/budget policy, resources, side effects, network, retry,
cancellation, and reconciliation. Gateway authorization is continuous rather than submit-only:
every call verifies signature, expiry, nonce/revocation, source Sandbox, Thread/Turn/Job identity,
lease generation, Descriptor identity, operation, parameters, Artifact scope, network origin, and
remaining budget. Cleanup and cancellation revoke the token and active streams immediately.

One structured confirmation covers the complete billable/sensitive batch. Status, poll, collect,
and cancel do not consume submission allowance. Replacement accounting follows the registered
Capability policy.

If any proposed Job requires billable or sensitive authorization, the complete mixed batch remains
an editable `JobGroupDraft`. No Provider, Operation, or Harness Job in that draft may start early,
including Jobs that would be free when submitted alone. The confirmation UI presents every Job,
submission count, cost/resource boundary, and side effect together. The user may remove or edit
draft items; confirmation atomically freezes one immutable JobGroup and makes all accepted members
schedulable. Rejection creates no Job and performs no external side effect. A batch whose complete
scope is already authorized may skip the draft and commit atomically.

## Artifacts, Validation, Review, And User-Visible Outputs

The original content-addressed objects, SHA-256/size/MIME checks, ProjectFile versions, immutable
Turn input manifest, partial-failure Artifacts, previews, and atomic publication are retained.

The target adds optional expected output contracts and deterministic Validators. `artifact_role`
is the Job-local semantic slot such as `generated_images` or `evidence_report`. It is distinct from
the original project's three-valued output disposition, stored as `output_disposition`:

```text
candidate  optional result, not automatically materialized as a persistent ProjectFile
workspace  persistent intermediate for later Harness work
delivery   user-visible result with ProjectFile, file card, preview, and download
```

There is no Delivery table, Manifest, state machine, or `delivery.committed` event. A validated
Artifact with `output_disposition=delivery` is the durable fact that a user-visible file exists. A role
collection is published atomically only after its cardinality and validation contract passes.
ProjectFile versioning and explicit Artifact replacement lineage preserve later revisions.

An output contract may request `delivery`, but any required Validator or Reviewer forces emitted
files to remain candidate Artifacts until every receipt bound to their exact digests passes. The
final transaction atomically promotes the complete semantic collection to `delivery` and registers
its ProjectFiles/file cards. A failed or unavailable required gate never exposes the candidate as
delivery. Contracts without required gates may commit directly to their authorized disposition.

The Primary Harness performs ordinary semantic self-review. An independent Reviewer is a read-only
Harness Job created only when explicitly requested or required by a high-risk policy. It is not a
mandatory phase for every Job.

Independent review is disabled by default. It may be created only by an explicit user request, an
explicit `review.submit` call from the Primary Harness, a registered publication policy with
`review_policy=required`, or a Capability Descriptor whose frozen policy requires it. The platform
never infers review risk from user-language keywords. Deterministic Validators run first and their
receipts are frozen into the review context. Related candidate Artifacts are reviewed together
rather than spawning one model per file.

A Reviewer is a read-only Harness Job. It may inspect frozen candidates and use registered
read-only analysis tools, but cannot mutate candidates, call side-effecting Providers, publish,
interact directly with the user, or delegate. Its structured verdict is `PASS`, `REVISE`,
`EVIDENCE_REQUIRED`, or `BLOCKED`. The Primary Harness handles the verdict and any repair. Reviewer
execution failure is not candidate-quality failure: optional review returns an availability
warning, while required review blocks publication without discarding the candidate. A required
`delivery` disposition needs a PASS receipt bound to the exact candidate digests; modifying a candidate
makes an older receipt inapplicable without deleting its audit history.

Cross-Sandbox inputs are immutable Artifacts, never a shared mutable Workspace. An existing Upload,
ProjectFile, or Job output is referenced by Artifact identity. A new parent Workspace file is
frozen with `artifact.stage`, which rejects links and special files, verifies stable file identity
while streaming it into content-addressed storage, and returns its SHA-256, size, canonical MIME,
and Artifact ID. Child inputs are materialized read-only under role-scoped paths with a frozen
manifest.

`artifact.stage` and `artifact.emit` are authenticated Runtime-to-Gateway streaming protocols. The
Runtime opens and verifies the Sandbox-local path, streams bytes without object-store credentials,
and supplies the pre/post stat identity and digest. The object service writes content-addressed
bytes first; one database transaction then creates or reuses the Object identity and binds the
Artifact/Job slot under the current execution Grant and lease generation. A crash before binding
leaves only deduplicated unreferenced bytes for bounded garbage collection; a crash after commit is
an idempotent replay. The host never interprets an arbitrary Sandbox path as a host filesystem path.

Each Harness Job freezes its allowed `artifact_role` values. A business role is local to that Job, must match
`^[a-z][a-z0-9_-]{0,63}$`, and cannot use reserved `platform.*`, `system.*`, or `internal.*`
namespaces. Provider and Operation roles come from their registered Descriptor; a Parent Harness
may declare local roles only for a Harness Job. The Child cannot emit an undeclared role.

An artifact role declares `accepted_mimes`, `min_items`, `max_items`, `ordered`, and
`collection_mode`. A single file is the special case `min_items=max_items=1`. A multi-item role
requires a stable `item_key`; ordered collections also require a unique position. Identity is
`(job_id, artifact_role, item_key)`: an identical replay is idempotent, conflicting bytes are rejected, and
replacement requires explicit Artifact lineage. Small collections use individual Artifacts. Large
collections use a deterministic archive plus a member Manifest with path, digest, size, MIME, and
order.

Children register output files with `artifact.emit`; merely writing a file does not publish it.
The platform validates regular-file stability, role cardinality, item keys/order, individual and
aggregate byte limits, MIME, and required roles before Job success. Undeclared files are not parent
inputs or user deliveries and may be retained only as bounded diagnostics.

MIME is not globally exhaustive. Contracts accept exact canonical values or controlled family
patterns such as `image/*`; an Artifact always records a concrete detected MIME. The MIME Registry
owns alias normalization, content detection, preview adapters, validators, active-content policy,
and safe download behavior. Detection uses file signatures and format-specific probes rather than
trusting a Harness declaration or filename. Unknown formats may be retained as
`application/octet-stream` for attachment-only download, but receive no inline preview, automatic
execution, or implicit validator.

Artifacts from cancelled or replaced Jobs remain auditable and retrievable but are not
automatically selected for a later conversation or promoted to `output_disposition=delivery`.

## Actual Execution Graph

The graph is a projection of durable facts, not scheduling authority:

```text
Primary Turn
  |- native model/command/tool/file activities
  `- JobGroup
       |- Provider Job
       |- Operation Job
       `- delegated Harness Job
            `- native activities
```

Each node shows public role, execution location, state, elapsed time, resources, retries, errors,
and resulting Artifacts. The UI may reuse the Plus graph layout, but the data comes from original
Thread Items, Runtime events, Job records, reservations, and Artifact records.

Each Primary, Child and Reviewer Harness also shows its compact aggregate model-token usage. Its
details separate ordinary input, cache-read input, cache-write input and output. Durable model
request IDs prevent duplicate aggregation; `(turn_id, job_id)` keeps parent and child accounting
isolated. Bounded projections visibly mark partial usage. The browser animates later updates for
the same selected Harness, but switches Harness identity and reduced-motion mode immediately.

Top-level nodes are limited to real isolation or interaction boundaries: user Turns, Primary
Harness Turns, JobGroupDraft confirmation, immutable JobGroups, Provider/Operation/Harness Jobs,
Continuation Turns, and user Interactions. Model requests, Skill reads, shell commands, tool calls,
file changes, and individual Artifacts are not top-level graph nodes. Clicking a Primary or Child
Harness node opens its live ordered activity list with start time, ticking/completed duration,
status, and error evidence.

Edges express only durable facts such as `submitted_by`, `contains`, `continued_from`, `replaces`,
`requested_interaction`, and `produced_result`; no predicted next step is rendered. Group members
fan out in one local parallel layer and have no dependency edges. Artifact inputs/outputs, preview,
download, digest, disposition, source, and replacement lineage appear in node details rather than
as graph nodes.

The default viewport shows the active Turn, every nonterminal Group, the most recent continuation,
and directly related completed nodes. Older Thread history expands on demand. Layout follows the
approved serpentine path, alternating left-to-right and right-to-left rows at horizontal bounds
rather than growing vertically at one edge. Every node includes its truthful execution location,
resource summary, elapsed time, retry/recovery count, and distinct visible states for running,
queued, waiting confirmation, cancelling, unknown, blocked, failed, and terminal success.

## Sandbox Images, Profiles, And Dynamic Bundles

An OpenSandbox Harness final image has four conceptual OCI layers:

```text
L4 Harness Layer
L3 Runtime Profile Layer
L2 Common Toolchain Layer
L1 Sandbox Bootstrap Layer
```

L1 contains the minimal Linux base, OpenSandbox/Execd startup contract, non-root `updream` user,
directory permissions, minimal Python, health check, process/signal handling, and the signed Bundle
Bootstrap Supervisor. It contains no Skill, Provider credential, or Harness CLI.

L2 contains stable shared tools: shell/coreutils, Git, curl, Python, Node, basic compilation,
archives, file detection, proxy integration, and common libraries. Heavy domain dependencies do not
belong here.

L3 is a `RuntimeDependencyProfile`, initially `primary-full-cpu`, `general-cpu`, `media-cpu`,
`office`, `web-browser`, and `data-analysis`. `primary-full-cpu` preserves the original Primary
Harness's complete supported non-GPU toolchain so ordinary Thread work does not require image
switching. Media adds FFmpeg/ImageMagick/PyAV/OpenCV and approved CPU media libraries;
office adds LibreOffice and document/PDF tooling; web adds Chromium/Playwright and browser build
tools; data adds the approved numerical/analytics stack. Profiles are not Skills. Many Skills reuse
one Profile, and a signed Profile Catalog freezes image digest, supported Harnesses, commands,
modules, dependency capabilities, resources, and security properties.

L4 adds exactly one native Harness CLI and adapter: Codex, Claude, or DeepSeek. The deployable image
matrix is `Profile x Harness`, pinned by immutable digest. OCI storage shares L1-L3, so the heavy
Profile bytes are not duplicated even though final image manifests exist for each supported pair.
Updating one Harness rebuilds only that Harness's thin L4 descendants; updating one Profile rebuilds
only that Profile's final combinations.

GPU execution remains in registered GPU Operation Workers by default and does not contaminate CPU
Harness images. A direct-GPU Harness Profile is added only for work inseparable from a native
Harness and only on a node/runtime that truthfully supports that isolation boundary.

Three signed runtime inputs sit above the image and are not OCI layers:

1. Runtime Code Bundle: Runtime server, Job/Artifact/Interaction clients, event normalization,
   Gateway adapters, and recovery protocol;
2. Skill Bundle subset: only execution-Grant Skill IDs materialized into Harness-specific paths;
3. Execution Grant/config: identities, Artifacts, Descriptors, network, budgets, model route,
   generation, TTL, and nonce.

Runtime code or Skill-content changes rebuild only their signed Bundle when existing image
dependencies suffice. Adding a system/Python/browser dependency rebuilds the affected Profile.
Changing Profile resources/mappings rebuilds only the signed Catalog when its image contents do not
change. Harness CLI changes rebuild that Harness's L4 combinations. Grant, model route, and budget
changes require no build.

The Skill set, Runtime Code Bundle digest, Profile digest, final image digest, Harness/model Profile,
and execution Grant digest are persisted with every Runtime execution and must match on recovery.
The platform never silently substitutes a different image, Skill set, or Bundle.

## Recovery And Failure Semantics

- Broker restart reattaches the exact Runtime execution after its persisted cursor; it never
  blindly replays an accepted Harness input.
- Worker restart reclaims Jobs by lease and resumes an external handle when one exists.
- A potentially accepted external submission without a handle becomes `UNKNOWN`; it is never
  automatically resubmitted.
- Job result, Artifact commit, and Outbox creation use idempotent identities and transactional
  boundaries.
- Cleanup warnings never replace the primary business/provider/model failure.
- Cancellation is truthful: unsupported or uncertain external cancellation remains visible as
  cancel-requested or unknown.
- Failed Turns may retain inspectable partial Artifacts and Workspace checkpoints.
- Reapers are namespace-scoped and delete only confirmed product-owned resources.

## Authoritative Data Model

The original `threads`, `turns`, `turn_inputs`, `items`, `events`, `interactions`, objects,
Artifacts, ProjectFiles, checkpoints, users, and auth sessions remain authoritative for their
existing contracts. New durable entities are:

- `job_group_drafts`: editable/confirmable request, digest, authorization gaps, expiry, and owner;
- `job_groups`: immutable committed batch, origin Turn, authorization snapshot, suppression flag,
  aggregate state, and idempotency identity;
- `jobs`: immutable request/Descriptor/Profile/output contract, state, replacement/reconciliation
  lineage, cancellation intent, and current result projection;
- `job_attempts` and `job_leases`: fenced claims, acceptance receipts, external/native handles,
  generations, timestamps, outcome, and retry kind;
- `job_artifacts`: semantic artifact role, item key/position, disposition, exact object identity,
  validation/review receipts, and state;
- `job_outbox`: one row per terminal Group, ready/suppressed/claimed/delivered state, claim lease,
  result digest, and continuation Turn identity;
- `authorization_snapshots`, `budget_ledger`, and `budget_reservations`: persistent versus one-shot
  scope and immutable reserve/consume/release entries;
- `nodes` and `resource_reservations`: registered Profile/image/resource inventory and fenced
  execution capacity.

Required uniqueness includes Thread-scoped request idempotency, `(group_id, local_job_id)`, one
Outbox per Group, `(job_id, artifact_role, item_key)`, external idempotency identities, and one
continuation Turn per claimed Outbox batch digest. State transitions use conditional updates or
row/advisory locks in one documented global order. Thread active-Turn serialization remains the
only writer ordering for the native Primary Session; there is no Task, Run, WorkUnit, or plan table.

## Database And Release Baseline

The target is a new project with a dedicated empty database, object storage root, cache, ports,
OpenSandbox namespace, service units, and Nginx prefix.

The initial implementation creates the database directly from one authoritative current schema.
It has no predecessor migration, forward compatibility window, downgrade SQL, migration receipt,
or rolling mixed-schema deployment. These are reconsidered only before a production release that
must preserve real user data.

Database migration is separate from release identity. Signed Runtime/Skill Bundles, immutable host
build identity, service readiness, isolated rollback of code/configuration, thin images, dynamic
bundle injection, Warm Pool, and attached Sandbox reuse remain valid engineering controls.

## Distributed Deployment

API/Broker instances use PostgreSQL as authority and advisory event delivery for wakeups. Durable
Workers claim Jobs with leases. Node Agents publish registered Profiles, images, CPU, memory, disk,
GPU identity/mode/memory, health, load, and cached bundles. Scheduler hard-filters compatibility and
security before scoring locality, warm capacity, cache, affinity, and load.

Provider Jobs need no Sandbox node. Operation Jobs select registered CPU/GPU Workers. Harness Jobs
select a compatible OpenSandbox node and image. Resource reservations remain until exact cleanup or
confirmed absence.

## Implementation Sequence

1. Create the independent new project from the original project's code and deployment baseline.
2. Extract common Media/Site Job behavior into the generic Durable Job contract without changing
   their proven adapter semantics.
3. Add mixed immutable parallel JobGroups, all-terminal completion, Outbox, and generic
   continuation Turns.
4. Add Operation adapters and the Harness Delegation executor with depth and grant limits.
5. Add distributed Node Agent, Scheduler, reservations, and GPU inventory.
6. Add optional output contracts, Validators, Reviewer Jobs, generalized output roles, and the actual
   execution graph.
7. Migrate the original real-machine cases and the applicable Plus interaction, recovery,
   capability, delegation, and browser cases.
8. Deploy independently and iterate until a full real-machine round has no program defects or
   clearly optimizable prompt defects.

## Required Test Families

- Turn creation, native input/steer, checkpoint, broker kill, Runtime reattach, and cleanup;
- mixed Provider/Operation/Harness JobGroups whose members are all immediately schedulable;
- attempt acceptance receipts, lost responses, external UNKNOWN, reconciliation Jobs, cancel, and
  replacement without duplicate side effects;
- transactional Outbox/continuation crash points, multi-Outbox batching, stop-all suppression, and
  exactly-once Thread insertion;
- user input before/after Job completion and continuation creation/claim races;
- multiple concurrent immutable groups, explicit cancellation/replacement lineage, and ordered
  user/continuation Turns;
- child Harness failure, timeout, node loss, checkpoint recovery, recursion rejection, and cleanup;
- authorization rejection/approval, budget exhaustion, billable replacement accounting, and token
  revocation, including concurrent Group budget reservation;
- ChildExecutionProfile/RuntimeDependencyProfile resolution, incompatible Skill unions, capability
  changes on native Session resume, and Bundle/image digest mismatch;
- Artifact streaming crash points, identity, partial results, semantic roles/dispositions,
  validators, required-review promotion races, optional review, delivery outputs, and previews;
- browser reconnect, queued/applied input states, execution graph activity, decisions, errors, and
  terminal success/failure signals;
- multi-node CPU/GPU scheduling, service restart, database concurrency, and orphan reaping.
