# Worker progress under exhausted admission capacity

The real D45 takeover reached the original execution only after 390 seconds:
all admission tasks were occupied before the previous lease expired. Several
tasks were waiting for resources retained by pending cleanup. `JobWorker.run`
also skipped cleanup whenever its admission capacity was zero. Extending the
test deadline would not repair this dependency cycle.

Keep the configured admission concurrency and all Scheduler resource fences.
Run one bounded cleanup batch independently of admission. Reserve a separately
configured, bounded observation concurrency (default one) for jobs with a
durable accepted external identity. Observations use one status/collection pass,
release through the existing defer protocol, and rotate by `updated_at` so a
pending execution cannot monopolize recovery. This path must never submit work.

Recovery candidates require a currently valid acceptance, excluding acceptance
superseded by a pause/resume boundary. An execution accepted after resume retains
its identity during later recovery. All claims still use the existing Job
generation and lease transaction. No supplier, case or operation branches are
introduced, and no external resource limit is raised.

Validation: regressions first reproduce full-worker cleanup starvation,
accepted-recovery starvation and replay of a newly accepted post-resume job.
Check empty/unaccepted and superseded-acceptance filters, SQLite and PostgreSQL
claims, JobWorker/Store/pause tests, then the broader backend gate. Real impact
includes D45, D08, D06/P05 takeover, active K01/K05 capacity waits and the capacity
gate; preserve unrelated completed cases. Restart the affected isolated Ali
services during active work and verify retained external identities. Final
reports retain previous failures and identify mixed-revision successes.
