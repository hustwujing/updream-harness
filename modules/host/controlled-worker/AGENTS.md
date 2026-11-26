# Controlled Worker Module Rules

- Only declared fixed programs may execute; no general host shell fallback.
- Validate runtime manifest, binary identity, mounts, device access and resource limits.
- Use discovered GPU UUIDs and reservations; do not hard-code physical cards.
- Preserve Job lease/generation fencing and artifact contracts.
- Never import JobWorker or MediaWorker service implementations.
- Complete with bwrap, cancellation, artifact and real-operation verification.
