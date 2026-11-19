# Observed Workflow Corrections Design

## Intent

Correct the general contract and user-experience failures observed in Thread
`54652f36d445425082b44de0403e4f52` without adding case-specific behavior.

## Design

- Operation Descriptors declare allowed validator IDs per output role. The Job
  Coordinator rejects an undeclared validator before authorization or GPU work.
- Existing `MediaBatchRow.turn_id` is the batch identity. One API action confirms
  every pending Draft in a sealed batch atomically after validating every model,
  mode, parameter set and reference. Individual endpoints remain for explicit
  per-item edits.
- The media confirmation UI groups Drafts by Turn and offers one confirm/cancel
  action per batch. While a batch is pending, the Thread header says it is
  waiting for media confirmation even though the producing Turn is terminal.
- Job Artifact Items include `local_job_id`. Generic filenames such as
  `foreground.png` are displayed with that semantic identity; intentionally
  named deliverables remain unchanged.
- Platform instructions and Codex-PPT guidance prohibit a separate
  `sandbox-interact` authorization for a Provider that will already use a media
  confirmation card.
- Codex-PPT defaults generated Chinese slides to one exact title plus at most
  five short labels. Detailed prose belongs in speaker notes. `speech.md`
  examples use `## Slide N`, and the assembler accepts heading levels 1-4.

## Non-goals

- Do not automatically approve billable Provider calls.
- Do not replace the image-based PPT workflow with programmatic slide rendering.
- Do not rename immutable Artifact objects or change existing database rows.

## Verification

- Coordinator tests reject `transparent_image` for `image.segment` and accept
  `image.alpha` before group creation.
- Store/API tests prove all-or-nothing media batch confirmation and idempotency.
- Browser tests prove one batch button, immediate lock, and waiting status.
- Artifact tests prove semantic display names without changing explicit names.
- Codex-PPT tests cover `# Slide N` and `## Slide N` notes plus sparse text prompt
  constraints.
- Three deployed real-machine cases exercise business-shaped two-stage DAGs:
  parallel Operations and asynchronous Providers first produce immutable
  Artifacts, then a complex Skill consumes those Artifacts in a Child Harness.
  A recovery variant restarts the Job Worker while mixed members are in flight
  and proves that accepted Provider work is not submitted twice.
- Operation, Provider and Child Harness failures share one public error contract
  with a canonical phase, retry class, external side-effect state, safe message
  and actionable guidance. Adapter diagnostics remain internal.
