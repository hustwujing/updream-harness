# Fresh deployment with independently owned databases

The user requested independently created databases at deployment and explicitly
removed backward-compatibility requirements. The implementation default is four
business-domain databases: **accounts, core, media, sites**. This is the
recommended partition stated in the conversation; a strict per-process partition
has not been requested explicitly. This document supersedes the original
single-database and old-version compatibility provisions of the module design.

## Scope and ownership

Use separate database names, schema initialization and credentials. They may run
on the existing isolated Ali PostgreSQL instance; a database is not a separate
application service. No original-project database or shared infrastructure is
changed. Fresh stores receive no business-history migration. Keep the previous
test databases/read-only code snapshot and unresolved resource ownership until
cleanup is authoritatively complete; do not erase their obligations because the
new deployment is fresh.

The inspected 55 current tables divide into accounts 3, core 37, media 9 and sites
6. Exact table/method mapping is retained in
`.data/evidence/module-isolation/database-boundaries.json`. New inbox/outbox,
lifecycle-barrier and publication-receipt tables belong to their local authority.

- Accounts owns users, auth sessions and registration intents.
- Core owns Threads/Turns/inputs/events, Jobs/attempts/groups, allocation and
  cleanup, resource/budget/capability ledgers, artifacts/project files and Review.
  These remain together so execution, publication and continuation retain local
  transactions. API, sandbox-gateway, JobWorker, AllocationWorker and
  ControlledWorker share this core authority rather than replicate it.
- Media owns drafts, batches, provider executions/outputs, media reference and
  voice-clone records. MediaWorker alone issues native provider execution calls.
- Sites owns projects/drafts/deployment jobs/versions/files. Version activation
  and rollback remain local transactions within sites.

Shared code does not imply one shared database connection. Each repository owns
one engine/session factory and only its tables. A service's environment declares
its required domain connections and privileges; initialization creates only the
selected domain schema. Startup rejects wrong schema/domain identity. There is
no SQL join across databases and no multi-engine SQLAlchemy transaction presented
as atomic.

## Transaction-local core versus durable handoffs

Keep all existing core atomic boundaries intact: Job terminal state, budget
release and continuation outbox; lease/generation fences; artifact/ProjectFile/
Item publication; checkpoint-before-success; allocation-before-execution intent;
cleanup-before-capacity-release. Moving database location never changes the
meaning of unknown provider outcomes.

For every cross-domain mutation, persist a source command/outbox in the same
local transaction as the initiating state change. A target inbox deduplicates the
immutable command identity and payload digest and commits its local effect and
receipt together. An acknowledgement is persisted back at the source. Delivery
is at least once; observable effects are idempotent. No correctness depends on
an in-memory callback or Redis notification surviving a crash.

Commands carry a schema version, stable command/aggregate IDs, authoritative
owner/thread/source identity, lifecycle/source generation, per-aggregate sequence
where ordering matters, and frozen data or content-addressed manifest references.
A repeated ID with a different digest is a conflict. Unknown delivery reconciles
the original ID. Failed receipt delivery cannot cause a second Provider submission,
artifact publication or continuation.

Relay/authority interfaces are shared infrastructure libraries with explicit
repositories. A local transport is allowed for co-located components and tests;
any HTTP transport must expose the same authenticated typed command/receipt
contract. Transport choice cannot bypass the durable outbox/inbox, admission or
cancellation fence. No new message-broker dependency is required.

## Required boundary changes

### Admission, lifecycle and cancellation

Core is the authority for owner/thread lifecycle and source Job cancellation.
Remote work receives a durable core admission identity bound to the immutable
request, lifecycle epoch and source scope. Core tracks outstanding remote
obligations, so a delayed remote observation cannot make archive/delete appear
safe. New admission cannot race a committed archive/delete barrier.

Media submission requires an ordered, persisted source authorization/permit;
reading core state once and then independently creating work is insufficient.
Cancellation and permit issuance serialize at core. Media retains its local
lease/generation and consumes an identity-bound permit before native submission.
Cancellation after accepted authorization follows the existing accepted/unknown
execution contract, including observation and cleanup; it is not treated as proof
that no external execution occurred. Reconnect/restart reuses the same dispatch,
permit and provider identity. Saturated admission cannot block cleanup lanes.

Turn-linked standalone drafts and source-Job executions have different existing
scope contracts: an origin Turn may be terminal while an authorized Child/Job
continues. Preserve those contracts; do not invent a blanket active-Turn check
that rejects valid Job continuations or user confirmation after a Turn ends.

Thread archive/delete uses a persisted lifecycle barrier and domain acknowledgements.
Deletion cannot finish while media/site work, published sites, pending publication,
unknown allocations or cleanup remain owned. A late reply is checked against the
barrier generation and cannot recreate a deleted Thread. Retries do not create
fresh identities. Unarchive advances the lifecycle generation and releases only
matching blocked continuations.

### Lifecycle operation result contract

Archive/delete and the registration admission barrier use durable operations.
Preparation freezes the matching aggregate/owner epoch and checks each domain's
active obligations; it does not purge business data. Core persists one commit or
abort decision. Participants then apply that decision idempotently and acknowledge;
no database transaction or row lock is held open across a domain request.

- Healthy operations still return their ordinary successful response only after
  all required effects/acknowledgements complete.
- A known business refusal, such as active work, keeps the original lifecycle
  state. Abort releases all prepared barriers for the same operation before the
  final HTTP 409 is returned. A failed abort acknowledgement remains pending.
- A timeout or lost reply does not choose an abort or infer success. Return HTTP
  202 with the stable operation identity/state; retain the decision/barrier and
  recover it through the command journal. A status endpoint exposes the result.
- After a durable commit decision, failures roll forward by retrying original
  commands; they do not pretend the action failed atomically or recreate data.
  A later lifecycle epoch cannot be overwritten by a stale acknowledgement.

The frontend API layer handles 202 by following the operation status and showing
existing pending/busy feedback. It removes/archives a Thread only on confirmed
completion, and reports a terminal refusal only after barriers have been released.
No page redesign is required. All participating schemas, routes and tests adopt
this new pending contract; no old synchronous-protocol compatibility is promised.

### Core Job to media execution

Core freezes the authorized Job request, artifact identities and output contract
with a stable dispatch intent. Media inbox creates one local execution for the
source Job, independently of later Job attempt generations. Core records the
acceptance receipt under its own fence. Media's provider ID, local generation and
staged outputs remain authoritative in media; core retains business Job outcome,
Review and publication authority.

### Media output and continuation

Media commits provider outcome, immutable output manifest and publication event
locally. Core validates scope/hash/contracts and publishes Artifact/ProjectFile/
Item in one local transaction, returning durable IDs. Media records the receipt;
output is not exposed as delivered before the core publication commit. Candidate
and source-Job outputs keep existing Review/disposition requirements.

Core terminal transitions write a TurnClosed outbox instead of changing media
batch rows inside a core transaction. Media sealing/terminal readiness and core
publication receipts lead to one BatchCompleted command. Core enqueues one
continuation under Thread serialization using a stable deduplication identity.
A lost acknowledgement must not enqueue another Turn. Replace media/core joins
with typed receipt-backed views or bounded domain queries; read projections never
authorize submission, cancellation or deletion.

### Accounts and current anonymous work

No backward compatibility is required, but anonymous-to-account registration is
an existing product behavior that must remain. Prefer a stable principal identity
that survives account registration, so associating credentials does not rewrite
ownership in core/media/sites. Account uniqueness, registration replay, session
revocation and access checks still apply. If a flow actually needs ownership
transfer between different principals, it requires an explicit fenced operation
and domain acknowledgements; do not silently drop the work or bulk-update only
one database. Validate the concrete account/view/ID constraints before changing
identifiers.

Concrete identity decision: keep the existing random account `id` distinct from
`owner_id`; bind the account to the existing anonymous `owner_id` rather than
changing account ID formats. Accounts adds a server-authoritative principal/guest
credential epoch. Registration atomically creates the account/session/registration
receipt, claims the principal and invalidates ordinary use of its old anonymous
credential. A valid anonymous signature alone must no longer grant access to a
principal claimed by an account. If account authority is unavailable, protected
requests fail closed. Logout/disabled-account handling must not fall back to the
old claimed guest identity. Only the registration-replay endpoint may use the
old signed guest proof together with the correct password to recover a lost
registration response and create a new authenticated session.

Preserve `active_work_prevents_registration` with HTTP 409. The new stable owner
mechanism does not silently relax that product rule. An owner admission barrier
serializes the active-work check with new core/remote admissions; no owner data
needs rewriting across databases after successful registration.

### Sites, reference uploads and reads

Site draft creation and media reference uploads consume a core owner/lifecycle
admission identity. Site slug/version activation stays local. Completed media
references register their core ProjectFile with a stable key and persist the
returned receipt before exposure. Core read models, including ExecutionGraph,
aggregate domain views without cross-database SQL or assumptions that propagation
is immediate.

## Implementation and gates

1. Separate existing database source responsibilities as a mechanical preparation:
   compare all 339 method bodies and ORM definitions, then run focused regression.
   This step is not a claim of physical database isolation.
2. Define owned model registries, typed repository/authority interfaces and explicit
   four-connection configuration. Initialize fresh domain databases independently;
   omit historical schema/data upgrade and deprecated entry-point compatibility.
3. Add durable command/inbox/receipt/barrier primitives and negative tests for
   duplicate/conflicting delivery, reordered/stale messages, permission scope,
   lease loss, receipt loss, domain outage and restart. Explicitly cover a permit
   issued but not consumed, consumption followed by process loss before RPC,
   cancellation on each side of issuance, and lost provider acceptance. A permit
   receipt is authorization, not a provider-accepted receipt.
4. Replace each audited cross-domain SQL boundary with the appropriate protocol;
   move Media/Site Worker persistence to their stores and update API/Gateway/read
   aggregation. No production rollout of a half-converted multi-database path.
5. Provision fresh isolated PostgreSQL databases and prove owner/table separation,
   crash recovery, cancellation ordering and publication deduplication with real
   database failures. Independently build and launch all affected service modules.
6. Deploy the fresh isolated Ali environment with explicit service/domain versions,
   verify sandbox execution/recovery, browser, providers and old-environment resource
   accounting, then run the newly required full 184-case real gate. Add dedicated
   cross-database fault evidence; it cannot be substituted by old single-DB passes.
7. Report external endpoint failures separately; restore/verify Seedream without
   silently replacing its configured model. Preserve proof and clean only owned
   retired resources after validation, then publish source and final reports.
