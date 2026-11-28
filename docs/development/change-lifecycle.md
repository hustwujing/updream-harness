# Changing Turn Lifecycle

## Applies to

Required for a new Turn state, transition, terminal outcome, input-delivery
phase, cancellation behavior or cleanup/recovery change.

## Authoritative surfaces

- `domain.TurnState` and `TERMINAL`.
- State sets and conditional writes in `Store`.
- `Broker.reconcile`, `_reconcile_turn`, `_process`, heartbeat and cleanup.
- API SSE termination and browser active/terminal rendering.
- Public error mapping and observability state events.

State membership is repeated intentionally at transaction boundaries. Search
for every `TurnState.` use before editing; updating the enum alone is unsafe.

## Required questions

- Is the state recoverable after API SIGKILL without shutdown hooks?
- Does it own a lease and must every write be generation-fenced?
- May it accept appended input, interaction responses or cancellation?
- Does the Runtime process already exist? If yes, recovery must reattach rather
  than replay.
- Does the state keep the Thread active pointer?
- What closes in-progress Items and unresolved interactions?
- When is `pending_final_state` written and cleared?
- Can a queued follower start before cleanup and pointer advancement?

## Transition rules

Persist intent before external cleanup. Treat unknown delivery or Provider
outcome as unknown/interrupted, never as permission to retry. Cancellation that
wins before final commit must prevent later success. A stale generation cannot
publish state, checkpoint, artifact or Runtime cursor.

Cross-domain lifecycle operation IDs remain stable for one scope generation. An
immediate local prepare refusal may be re-evaluated under the same identity when
the caller retries after its blocker clears, because no participant command has
left Core. A refusal reached after remote prepare/abort, a requester-mediated
registration, or any committed/aborted barrier is final for that operation and
must never be reopened. Re-evaluation reuses the existing barrier and must not
duplicate participant effects.

## Tests

Add transaction tests for the exact race, Broker recovery tests for each
nonterminal state, cancellation competition, stale-owner rejection, Runtime
replay without duplicate execution, queued follower ordering, SSE termination
and browser rendering. Include a non-graceful deployed case for changes to
recovery or cleanup.
