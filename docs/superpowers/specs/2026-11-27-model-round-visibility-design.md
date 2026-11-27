# Model Round Visibility Design

## Intent

Make long-running Harness work visibly alive by showing each durable model
request as a conversation round in the selected execution-graph Harness node.
Users must be able to distinguish a model that is waiting, streaming, completed
or failed without exposing prompts, generated content or transport details.

## Chosen Approach

Use the existing Core `ItemRow` model-activity records as the product authority.
Persist the same payload-free records for Child Harness requests by retaining
their `job_id`; do not derive product state from logs or OTLP. Project the rows
into each Primary or Child Harness node as `model_rounds` alongside the existing
aggregate `token_usage`.

The model-usage panel shows a compact summary by default, for example
`8 轮已完成 · 第 9 轮生成中 · 32 秒`. An expandable list shows each round's
model, phase, first-output latency, total duration and token count. The graph
node status line remains compact and adds only the round count.

## Data Contract

One round is one unique `model_request_id`. A retry with a new request ID is a
new round because it represents a separate Provider request and cost. Rounds
are ordered by persisted item creation order and receive a one-based display
index.

Each projected row contains only allowlisted fields: `index`, `request_id`,
`model`, `phase`, `status`, `started_at`, `ended_at`, `duration_ms`,
`first_output_ms`, the four token counters and `error_code`. Live duration is
computed from `started_at`; terminal duration is frozen. Missing usage remains
zero. The existing 5,000-row graph bound and truncation marker apply to both
usage and rounds.

A model request interrupted by Gateway or Worker termination is closed as
`interrupted` when its owning Turn or Child Job reaches a terminal state. It is
shown separately from model failures and never remains visually active after
the owning work has completed.

## Constraints

- No database migration or new table.
- No prompt, response text, command, URL, credential or raw exception in the
  API or UI.
- Primary and Child Harness records use the same persistence and projection.
- Replayed request IDs are idempotent and counted once.
- Restart recovery preserves round history and the active round.
- Mobile layout stays compact; details are collapsed by default.

## Verification

Backend tests cover Primary and Child persistence, deduplication, ordering,
live and terminal phases, failed rounds and truncation. Frontend tests cover
the summary and expanded rows. Required checks are focused Python tests,
frontend tests and the production frontend build, followed by an affected real
restart/recovery case.
