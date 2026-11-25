# Durable child sandbox allocation before Runtime submission

## Observed failure

The 289455c campaign found a C09 Child Job in UNKNOWN after
SandboxCreateOutcomeUnknown during pool acquisition. Runtime submission had not
started. Generic exception handling recorded acceptance without an execution
handle and released the Scheduler reservation. Allocation intent was not yet
persisted. This is separate from the earlier format-selection defect.

## Ownership and state

Persist the planned Job execution ID and an allocation receipt before calling
create. The receipt freezes provider, allocation ID, compatible profile and
Scheduler reservation identity. `accepted_at` stays unset until actual Runtime
submission evidence exists. Allocation, preparation and Runtime submission are
distinct phases.

Callers with a lease-fenced durable intent may opt into pool retention using
`retain_unresolved_allocation`. The default acquisition behavior remains
unchanged for callers with other cleanup protocols. Pending slots are keyed by
(provider, allocation_id). Unknown creation and cancellation after request
initiation retain that slot; a later adoption consumes it without reserving a
second slot. Empty inventory does not authorize another create or release.

A Child waits/reconciles the same allocation under its existing Job deadline.
Cancellation retains the provisional receipt and Scheduler reservation. Worker
replacement reconciles and retires an unstarted allocation before retrying;
it never treats allocation uncertainty as accepted Runtime execution.
Terminal cleanup reconciles allocation-only receipts, deletes a discovered
sandbox without depending on Runtime readiness, and releases capacity last.
Unresolved lookup keeps cleanup pending. Original attempt/reservation generation
survives takeover. The orphan reaper and provider-switch fence include active
and pending-cleanup allocation intents.

## Validation and impact

Regression coverage: delayed inventory with one create/one Runtime execution;
intent-before-create; cancellation/timeout; replacement Worker; earlier-generation
terminal cleanup; single-slot adoption; deletion before release; orphan and
provider-switch fences. Cover common protocols using fake provider identities,
then run the applicable real lifecycle checks and affected case subset.

Per the latest user instruction, keep unaffected successful case receipts.
Before resuming, record affected case IDs from actual Child/Reviewer use and
protocol dependencies, plus all never-run cases. Do not restart all169.
