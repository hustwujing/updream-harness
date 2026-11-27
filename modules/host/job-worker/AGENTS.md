# Job Worker Module Rules

- Persist submission intent before any external call.
- Invoke the Adapter's side-effect-free `preflight()` before every new submit.
  Validation failure is pre-accept and may expose only a trusted public reason.
- Reattach accepted executions; never blindly replay an unknown submission.
- Capacity waits reuse the fenced attempt generation and restore `SUBMITTING` first.
- Keep business outcome separate from cleanup completion and capacity release.
- Media Operations use `MediaAuthorityClient`; do not open media tables via Core.
- Complete with affected adapter tests, restart recovery and real-provider evidence.
- New async executors must implement the complete `JobAdapter` lifecycle and
  pass continuation/API/execution-graph error projection tests.
