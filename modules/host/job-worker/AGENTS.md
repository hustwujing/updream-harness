# Job Worker Module Rules

- Persist submission intent before any external call.
- Reattach accepted executions; never blindly replay an unknown submission.
- Capacity waits reuse the fenced attempt generation and restore `SUBMITTING` first.
- Keep business outcome separate from cleanup completion and capacity release.
- Media Operations use `MediaAuthorityClient`; do not open media tables via Core.
- Complete with affected adapter tests, restart recovery and real-provider evidence.
