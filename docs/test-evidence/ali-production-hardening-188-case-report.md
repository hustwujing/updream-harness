# Ali Production-Hardening 188-Case Report

## Outcome

The 188-case catalog has 188 successful latest results, zero program failures
and zero non-program failures. Mean elapsed time is 216.355 seconds. The report
is `composite_verified`: every case has real evidence after 2026-11-26 23:00
Asia/Shanghai, and failed or affected cases were rerun after each shared fix.
Unrelated successful cases retain their actual revision instead of being
repeated solely to manufacture a single-revision report.

The machine-readable report is
`.data/evidence/real-campaign/final-188-composite-c346638.json`. Every result
retains its own `run_revision`, `finished_at`, classification, elapsed time and
case-specific evidence.

| Family | Cases | Success | Program failure | Non-program failure |
| --- | ---: | ---: | ---: | ---: |
| S | 31 | 31 | 0 | 0 |
| C | 30 | 30 | 0 | 0 |
| R | 17 | 17 | 0 | 0 |
| D | 51 | 51 | 0 | 0 |
| P | 26 | 26 | 0 | 0 |
| K | 14 | 14 | 0 | 0 |
| XDB | 6 | 6 | 0 | 0 |
| EVO | 13 | 13 | 0 | 0 |
| **Total** | **188** | **188** | **0** | **0** |

## Issue-To-Evidence Map

| Issue | Resolution | Source | Automated evidence | Real/deployed evidence |
| --- | --- | --- | --- | --- |
| Internal exceptions leaked or lost stable identity | Added bounded `ErrorEnvelope`, stable public projection and troubleshooting IDs. | `common/contracts/errors.py`, `public_errors.py` | `test_error_envelope.py`, `test_public_error_projection.py` | EVO public-error cases |
| Cross-domain payload drift and unbounded Outbox retries | Added typed commands, canonical digests, deterministic terminalization and bounded dependency retry. | `common/contracts/domain_commands.py`, `common/db/handoff.py` | `test_domain_command_contracts.py`, handoff tests | EVO command/Outbox cases |
| API process owned business and singleton loops | Extracted PlatformWorker; database-fenced leader owns singleton coordination while replicas claim business work. | `services/platform_worker/platform_service.py`, `common/db/leaders.py` | platform worker and leader tests | EVO01-EVO03 |
| Missing operational metrics | Added fixed low-cardinality business metric catalog and Runtime metric relay. | `common/observability/observability.py`, sandbox Gateway | `test_observability.py`, Runtime metric tests | EVO metrics case |
| Trace continuity ended at process boundaries | Persisted traceparents and emitted Span Links for claims, continuations and cleanup. | `common/db/handoff.py`, `job_attempts.py`, `turn_broker.py` | observability and trace-link tests | EVO trace case |
| Capability signing keys could not rotate safely | Added `kid` keyring with current/previous bounded acceptance. | `common/security/capability.py` | `test_capability.py` | EVO rotation case |
| Object deletion could race a new reference | Added grace, authority snapshot and immediate second reference check; deployment defaults to dry-run. | `common/storage/object_gc.py` | `test_object_gc.py` | EVO object-GC case |
| Usage replay could double count | Added append-only idempotent Usage Ledger without billing behavior. | `common/db/usage.py` | usage/store tests | EVO usage replay case |
| Provider outage blocked observation and cleanup | Circuits now fence only new creates; poll, reconcile, cancel and delete remain available. | `common/db/provider_circuits.py` | provider circuit tests | EVO circuit case |
| UI stage labels inferred non-authoritative state | Projected execution stage from authoritative Turn, Job, Media and Site state. | `common/db/api_repositories.py` | `test_execution_stage.py` | EVO browser-stage case |
| One owner could consume ordinary admission | Added database owner admission ordering while exempting recovery, observation and cleanup. | `common/db/job_attempts.py`, `turns.py` | SQLite/PostgreSQL concurrency tests | EVO fairness case |
| Mutable service selection and unsafe rollback | Added immutable service descriptors, wire/schema/baseline compatibility and verified rollback descriptors. | `tools/service_release.py`, `deploy/ali/run-service.sh` | release/module/deployment tests | deployed per-service digests |
| Production Gateway could use cleartext routes | Production validation requires HTTPS while local and private provider routes remain explicit. | `common/configuration/config.py` | `test_config.py` | deployed route validation |
| Retained sandboxes lost their expiry after restart | Persisted `RETAINED` expiry and fenced claim/reap transitions. | sandbox ownership/store transactions | retained sandbox tests | `evo03-retained-9fe9931.json` |
| Process-local capacity double charged cross-process work | Made Thread capacity database-authoritative and resumed by owner/thread/profile identity. | pool/ownership transactions, PlatformWorker | pool, ownership and PostgreSQL tests | `p18-db-capacity-0b11082.json`, `d18-d19-db-capacity-0b11082.json` |
| Typed media command errors became unknown side effects | Validate commands before effects and keep empty generation mode valid. | command contracts, Media adapter | command and C09 tests | `c09-domain-contract-0c2c048.json` |
| Deployed Seedream sizes exceeded Provider limits | Published the actual 2K/3K and 3584-pixel contract. | media catalog/configuration | catalog/media tests | `c30-image-size-f603db3.json` |
| Paused Thread cancellation was not reclaimable | Included `CANCELLING` in paused recovery candidates with generation fences. | Turn/job claim transactions | pause/cancellation tests | recovered cancellation deployment evidence |
| Capacity-heavy K08 conflicted with sibling protocol cases | Serialized only K08 against shared protocol capacity; product state machine remained provider-neutral. | real protocol runner | real case catalog tests | `k08-isolated-31f9a7d.json` |
| K11 used inconsistent multiview fixtures | Generated deterministic coherent front/left/back/right views at runtime. | real protocol fixtures | catalog/fixture tests | `k11-four-view-f1ffed2.json` |
| systemd auto-restart raced explicit test restart | Observe changed PID and active unit before fallback start. | cross-database runner | runner tests | `xdb02-auto-restart-30d69a6.json` |
| Site staging root had the wrong owner | Revalidate owner and write/execute access; deployment creates the root as `sandbox-harness:0700`. | `common/storage/directories.py`, SiteWorker, `deploy/ali/start.sh` | writable-directory and Site tests | `xdb05-site-staging-ca9f6e7.json` |
| Media success could be validated before its continuation | Runner now waits for authoritative `media_batch=CONTINUED`, not an unrelated increase in Turn count. | `tests/run_real_campaign.py` | `test_real_campaign_runner.py` | `c30-media-continuation-0bfb9e5.json` |
| Child output manifest rejected harmless private scratch files | Manifest is the publication allowlist; missing/duplicate declared paths and role cardinality still fail. | `services/job_worker/harness_job.py` | `test_harness_job.py` | `c29-manifest-allowlist-ffdc81b.json` |
| Operation returned extra roles outside the frozen publication contract | Filter to authorized output roles; every authorized role must remain unique and present. | `services/job_worker/media_render_adapter.py` | `test_media_render_adapter.py` | `c16-operation-output-roles-332184c.json` |
| Child terminal check raced a newly durable continuation | Re-read the continuation after the open-group check before publishing terminal Child state. | `services/job_worker/harness_job.py` | Child continuation and Harness tests | P24 in `affected-continuation-c346638.json` |
| API showed a stale model catalog while PlatformWorker had refreshed it | API reloads the shared snapshot and trusts freshness only inside the bounded refresh window. | `common/catalog/model_discovery.py`, API catalog/health routes | catalog/model discovery tests | deployed health reports all three inventories fresh |
| Logged-in idle Thread still displayed historical restricted mode | Active work shows its actual authority; idle UI shows the next Turn entitlement from the current session. | `web/src/network-access.js`, `main.jsx` | 31 Vitest tests, `browser_auth.cjs` | deployed frontend digest and browser regression |

## Focused Final Reruns

The latest affected set is 13/13 successful: C16, C29, C30, P24, P27 and
K07-K14. P27 includes a JobWorker SIGKILL, one unchanged Provider execution and
one Primary continuation. C05 and R12 were audited separately; both latest
successful records contain `job_failures=[]` and no hidden
`submission_outcome_unknown`, `ReadError` or `external_status_unavailable`.

## Verification And Deployment

- Focused Python suites passed for every fix, including 118 Child
  continuation/Harness/JobWorker/Store tests and 85 MediaRender/JobWorker/Store
  tests.
- PostgreSQL concurrency gate: 14/14 passed in a disposable database, which was
  removed afterward.
- Frontend: 31/31 Vitest tests and production build passed.
- Browser: authentication/network-mode and model-catalog recovery checks passed
  against the deployed API frontend.
- API health reports the Ali backend and four databases available; model
  inventories are fresh and the selected frontend digest matches served files.
- Full Python release gate: 1569 passed, 23 environment-dependent tests skipped,
  179 existing deprecation warnings; elapsed time 347.32 seconds.
