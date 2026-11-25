# Adding a Runtime Tool

## Applies to

Required for a new sandbox CLI, browser/media helper, supervised background
operation or image dependency exposed to a Harness.

## Placement

Tool implementation and dependencies belong in Runtime source and images, not
the host API process. The host may expose a narrow Runtime control endpoint when
the tool needs durable interaction, cancellation or status; arbitrary command
execution endpoints are not allowed.

Existing patterns:

- `sandbox-background`: explicit argv, constrained cwd, process-group ownership,
  bounded log and durable Turn hold-open behavior.
- `sandbox-interact`: execution-bound approval/question request and response.
- `sandbox-browser-screenshot`: authenticated public proxy bridge and bounded
  browser arguments.
- browser render/media probes: deterministic image contract validation.
- `sandbox-job delegate`: compact metadata-independent Child Harness tasks
  compiled through the frozen execution catalog into the existing durable
  `JobGroupCreate`; `sandbox-job prepare` remains the exact low-level interface.

## Requirements

- Accept structured arguments; avoid a host shell and ambiguous string parsing.
- For JSON-or-file arguments, recognize inline objects/arrays before filesystem
  inspection; a long rubric or task must not be passed to `Path.is_file` as a
  filename. Preserve file input and the tool's object/array validation. See
  `job_cli._json_value`, `review_cli.main` and `tests/test_job_api.py`.
- Validate paths under approved workspace roots and reject links/special files
  where files cross the host boundary.
- Bound input size, output size, concurrency, duration and retained logs.
- Join the active execution's cancellation and process group.
- Emit stable tool IDs and normalized start/finish/error events.
- Put user-deliverable results in `/workspace/outputs`; working files stay in
  `/workspace/project`.
- Network access uses the capability proxy and destination policy.
- Add dependencies to the correct immutable image layer and update probes/lock
  evidence.

## Completion

Contract/unit tests cover invalid arguments, path escape, timeout, cancellation
and cleanup. Image contract tests prove installation. A real sandbox case runs
the tool, verifies output, cancels or fails it once, checkpoints the workspace
and confirms sandbox cleanup.
