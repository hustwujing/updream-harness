# Sandbox Protocol v2 - Stages 1-6 Acceptance Report

## Result

Sandbox Protocol v2 stages 1 through 6 are implemented on branch `sandbox-harness-platform-parallel-ali` in the isolated checkout `/data2/wujing03/sandbox-harness-platform-parallel-ali`.

Final implementation revision tested: `50d193b72e38d20f9e35fed85b655065b7b5af8a`

No deployment, service restart, backend switch, cloud template operation, image promotion, or cloud resource mutation was performed.

## Stage Reports

1. `2026-11-24-sandbox-protocol-v2-stage-1.md` - value objects, lifecycle contract, behavioral capabilities.
2. `2026-11-24-sandbox-protocol-v2-stage-2.md` - durable Turn Handles and recovery fences.
3. `2026-11-24-sandbox-protocol-v2-stage-3.md` - Job, cleanup, control, and port-grant Handles.
4. `2026-11-24-sandbox-protocol-v2-stage-4.md` - provider-aware Backend Registry.
5. `2026-11-24-sandbox-protocol-v2-stage-5.md` - Profile Catalog provider releases.
6. `2026-11-24-sandbox-protocol-v2-stage-6.md` - provider-owned Runtime routes.

Each stage report contains design, development, migration/rollback, test results, review findings, and residual risks at that stage boundary.

## Implementation Commits

- `e7e8305` - establish Sandbox Protocol v2 contracts.
- `a60ef5a` - persist durable sandbox Handles for Turns.
- `35e6c71` - persist sandbox Handles for Jobs and controls.
- `a0e57d3` - dispatch Handles through Backend Registry.
- `9863989` - upgrade Profile Catalog to provider releases.
- `943de5c` - resolve Runtime routes through providers.
- `5a5b173` - require durable Handles in recovery fixtures.
- `63d8b52` - close cross-provider protocol invariants.
- `e209d03` - enforce durable provider ownership at startup.
- `50d193b` - publish Aliyun template provider releases correctly.

## Cross-Stage Review Closure

Independent review initially found no Critical and six Important issues. Follow-up review found three remaining Important issues, then one final provider-release issue. All were closed:

- accepted-response-loss now returns the requested immutable Handle and survives process restart;
- startup rejects missing, malformed, unregistered, or switched active-provider Handles;
- Pool, orphan cleanup, Inspector, and shutdown use provider-aware identities;
- cached and attached fast paths compare complete Handles;
- Catalog resolution, Primary Turn allocation, Job scheduling, release recording, and materialization select the active provider release;
- Aliyun template refs are not treated as Docker image refs;
- Aliyun Runtime route configuration is required explicitly and fails closed until a verified listener exists.

Final independent review: no remaining Critical or Important findings.

## Final Verification

Command:

```text
.venv/bin/python -m pytest -q --basetemp=/data2/wujing03/sandbox-harness-platform-parallel-ali/.data/pytest-final-50d193b
```

Result:

```text
1072 passed, 5 skipped, 1 warning in 193.98s
```

The warning is the existing Starlette deprecation notice for the httpx-backed TestClient. `git diff --check` passed. The final test basetemp and all project state remained under `/data2/wujing03`; no project data was written to the root filesystem.

## Release Gate

The code is ready for the next separately authorized release stage on OpenSandbox. Aliyun remains fail-closed until all of these exist:

- a READY Aliyun template release in Catalog schema v2;
- an explicitly configured and reachable sandbox-visible Gateway and proxy route;
- real-provider conformance and abnormal-case evidence;
- K-series campaign evidence with Ali warm targets kept at zero;
- an explicit backend-switch decision after confirming there is no active durable work.
