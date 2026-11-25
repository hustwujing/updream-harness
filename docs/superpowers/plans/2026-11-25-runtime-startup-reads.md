# Retry transient Runtime bootstrap status reads

D40 on `5121adc` passed its mixed-group protocol but failed before business
execution: GET /bootstrap/code-bundle raised ConnectError. Bundle uploads and
configuration already had bounded startup retry, while the two prerequisite
status GETs failed immediately.

Use one bounded helper for the idempotent Code/Content status GETs and existing
PUT uploads. Keep the existing eight-attempt limit, backoff and retryable status
set. Permanent rejection still fails immediately. Configuration follows the
completed Bundle stages; execution POST submission remains outside this retry
boundary. The shared RuntimeClient applies to every SandboxBackend.

Regression first reproduced both ConnectError and503 on the status reads and
showed that503 stopped after one request. After the fix, both reads recover,
each upload/configure happens once, permanent403 stays immediate and repeated
503 stops after eight attempts. The focused Runtime/Broker/Child suite passes
144 tests, and independent review found no remaining concrete findings.

Run the full Python gate and deploy affected Ali services without waiting for
case drain, as explicitly authorized by the user. Re-run D40 and continue the
unrun cases; successful initial status reads are behaviorally unchanged, so
preserve unaffected successes. Record any still-running cases across restart
and do not duplicate their external work. Full release and real D40 evidence
remain pending.
