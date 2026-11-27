# Job Worker

Executes durable Child Harness, Reviewer, Operation and Provider Jobs. It owns
accepted-handle recovery, polling, collection and cleanup, but not allocation create RPCs.

- Entry: `sandbox_harness.services.job_worker.job_service:run`
- Databases: core and media
- Sandbox access: configured provider through durable allocation ownership
- Verification: JobWorker/adapter/recovery tests and real parallel protocol cases

Every Adapter receives a side-effect-free `preflight()` before submission and
shares one lifecycle/error contract. Terminal Outbox facts expose stable codes,
safe reasons and guidance to the Primary Harness without raw diagnostics.

Child and Reviewer Runtime collectors reuse the shared event buffer. Text and
tool output are aggregated before JobActivity persistence, control events flush
immediately, and Runtime journal ACK occurs only after the Core transaction.
