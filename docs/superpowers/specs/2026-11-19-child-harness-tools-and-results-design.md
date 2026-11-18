# Child Harness Tools and Results Design

## Intent

Make a Child Harness a capable scoped agent: it may create durable subordinate
Operation and Provider JobGroups, wait without LLM polling, resume the same
native session, and return complete bounded results. Ordinary Children still
cannot create Harness Jobs.

## Boundaries

- JobGroup is a generic task container. Only `kind=harness` creates an Agent.
- A subordinate group carries `parent_job_id`, fixed depth, frozen descriptors,
  inputs, output scopes and budgets inherited from the parent Job grant.
- Operation groups may execute from pre-authorization. Provider groups enter a
  durable approval state and share one open confirmation wave per top-level
  group/thread.
- Terminal subordinate results resume their owning Child; top-level group
  results resume the Primary.
- Skill dependencies are compiled from frozen metadata and intersected with the
  parent authorization. Missing dependencies fail before Sandbox creation.
- Child input prompts include exact materialized paths. Child final text,
  blocker and Artifact manifests are persisted in the Job result envelope.
- Job-scoped model activity is not interrupted when the origin Turn finishes.
- Provisioning Job Sandboxes are live to the orphan reaper before acceptance.

## Product Corrections

- Independent work is classified as Primary, Harness, Operation or Provider
  before choosing compact `delegate` or full `prepare`.
- `result_mode=artifacts` requires at least one output.
- `codex-ppt` uses platform `image.generate`, never direct public Provider SDKs.
- `image.segment` is callable by a Child through a scoped subordinate Operation
  group.
- Execution graph ranks wrap into viewport-width serpentine bands. Node details
  stringify structured payloads and are protected by a local error boundary.

## Completion

A real case produces a PPTX and two transparent PNG deliveries using parallel
Children, real `image.generate` and two real `image.segment` executions. API and
Job Worker restart recovery, confirmation batching, result delivery, reaper
safety, activity status and desktop/mobile graph behavior all have evidence.

## Live-case authorization

Real cases that expect a confirmation-gated Operation must declare that
Operation in `allowed_external_operations`. The campaign driver then exercises
the ordinary confirmation card; it must not bypass confirmation or classify the
pre-confirmation Primary response as a missing-delivery prompt defect. C04
therefore authorizes `image.segment`, while the product authorization boundary
remains unchanged.
