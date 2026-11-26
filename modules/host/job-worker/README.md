# Job Worker

Executes durable Child Harness, Reviewer, Operation and Provider Jobs. It owns
accepted-handle recovery, polling, collection and cleanup, but not allocation create RPCs.

- Entry: `sandbox_harness.services.job_worker.job_service:run`
- Databases: core and media
- Sandbox access: configured provider through durable allocation ownership
- Verification: JobWorker/adapter/recovery tests and real parallel protocol cases
