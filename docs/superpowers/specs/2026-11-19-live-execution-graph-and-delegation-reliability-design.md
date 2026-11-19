# Live Execution Graph And Delegation Reliability Design

## Intent

Users must be able to see the actual current execution of a Thread across
Primary Harness continuations, delegated Harness Jobs and durable media Jobs.
Failures must identify the failed execution boundary and remain actionable.

## Goals

- Follow the newest active Turn by default without taking history navigation
  away from the user.
- Connect a completed JobGroup to the internal continuation it created.
- Project durable media drafts and media Jobs into the same factual graph.
- Show stable public error codes and safe error summaries on failed nodes.
- Use almost the full viewport for the graph on desktop and the full viewport
  on mobile.
- Reject unavailable delegated Harness/model selections before scheduling and
  keep retry decisions within the discovered healthy catalog.
- Keep continuation payloads bounded and structured.
- Keep presentation output semantics explicit and require semantic output
  validation in addition to structural file checks.

## Non-goals

- The browser does not infer orchestration state from transcript prose.
- The control plane does not choose business plans or silently replace a model
  requested by the Harness.
- Provider error bodies, credentials and internal endpoints are not exposed.
- Existing completed Jobs are not rewritten.

## Approaches

1. Frontend-only stitching was rejected because media and continuation facts
   are authoritative in PostgreSQL and cannot be reconstructed safely from
   chat Items.
2. A second graph service was rejected because it would duplicate lifecycle
   state and drift from the API.
3. Selected: extend `ExecutionGraphProjector` as the single factual read model,
   then keep frontend behavior limited to following, history selection and
   layout.

## Design

`ExecutionGraphProjector` reads recent/active Turns, generic JobGroups and Jobs,
their continuation outboxes, media drafts and media Jobs in one snapshot. It
emits explicit `continued_by` edges from a JobGroup to the internal Turn input,
and `contains` edges for media batches. Node errors contain only stable public
codes and sanitized summaries.

The graph UI starts in follow-current mode. Arrival of a newer active internal
Turn changes the selected Turn. Manually selecting an older Turn disables
follow mode and displays a `Return to current execution` action. The modal uses
`calc(100vw - 24px)` by `calc(100dvh - 24px)` on desktop and the full viewport
on mobile.

Ordinary `delegate.harness` drafts inherit the origin Turn's exact Harness,
model and reasoning effort. Harness-authored values for those fields are
ignored while the Job is frozen, so the model cannot select a stale route.
System-owned Reviewer Jobs retain their separately configured execution
policy. Rate limits remain real terminal Job facts unless the descriptor has
an explicit bounded retry policy; retries never invent another model.

Continuation results remain bounded Job facts and object identifiers. Raw
activities and Provider payloads are fetched on demand. Image-based presentation
workflows must report that representation truthfully and complete their text QA
and bounded repair cycle before assembly. Segmentation success requires semantic
evidence when the output contract requests a specific subject or region.

## Verification

- Projector tests cover cross-Turn edges, media progress and sanitized errors.
- Layout tests cover automatic follow versus explicit history selection.
- Browser tests verify active breathing, full-screen sizing and current-execution
  navigation.
- Job preflight tests cover unavailable, limited and healthy child models.
- Focused real case verifies a parallel JobGroup returning into a continuation
  while the graph remains live.
