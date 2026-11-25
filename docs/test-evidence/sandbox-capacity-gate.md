# Real sandbox capacity gate

`tests/run_real_sandbox_capacity.py` exercises real creation and a Runtime image
resize, without model calls. These five cases are separate from the original
169-case product matrix, for 174 cases in the combined report.

| Case | Boundary | Expected result |
| --- | --- | --- |
| RC01 | Provider rejects creation while a real sandbox occupies its quota | Wait without acceptance; deleting the holder permits one Runtime submission in the same generation |
| RC02 | Cancel while provider capacity is exhausted | CANCELLED, zero Runtime submissions, holder remains healthy |
| RC03 | Deadline expires during capacity wait | FAILED with no execution side effects, zero Runtime submissions |
| RC04 | SIGKILL the isolated Worker during capacity wait | Replacement generation resumes; releasing the holder permits exactly one Runtime submission |
| RC05 | Platform node resources are reserved while a real holder exists | Wait without a provider create call; confirmed deletion and reservation release permit one execution |

Use an exclusively owned provider Team/project with a one-instance quota. The
Ali fixture uses its own Team, API key and template, reusing the validated Runtime
image. A real HTTP 429 denies the second creation while the first sandbox stays
accessible; deleting the first restores creation. This is separate evidence from
an injected SDK error or a local Pool semaphore limit.

The mode-0600 host fixture JSON contains `settings` (serialized `Settings`),
`profile` (frozen `SandboxProfile`), `image_digest`, and `provider_quota`. Set the
effective host control Gateway URL from the deployed service launcher. Credentials
are never printed or copied into sandboxes; Runtime capability tokens remain
short-lived and scoped.

```bash
TMPDIR="$PWD/.data/tmp" PYTHONDONTWRITEBYTECODE=1 \
  .venv/bin/python tests/run_real_sandbox_capacity.py \
  --fixture .data/config/provider-capacity-fixture.json \
  --report .data/evidence/resource-exhaustion/job-capacity-final.json
```

The runner creates a unique PostgreSQL database on the isolated Ali database
instance, namespace, data directory and Worker subprocesses. It kills only its
own PIDs. Workers publish their generation and maintain node heartbeats. Cleanup
retains the database when allocations remain unresolved; empty inventory alone
does not prove absence. Reports require all selected cases, record source hashes
and dirty state, and reject incomplete runs.

The gate reproduced a Worker defect: a definitive pre-accept capacity rejection
left the submission-in-progress flag set during sleep, so a deadline became
UNKNOWN. The Worker now clears the flag after `PreAcceptFailure` and restores it
before another submit. A subsequent submit that starts and times out remains
UNKNOWN. Final evidence is generated after committing both the runner and fix;
earlier receipts with uncommitted source are diagnostic only.
