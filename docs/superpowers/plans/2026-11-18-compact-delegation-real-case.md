# Compact Delegation Real Case Implementation Plan

> **For agentic workers:** Execute the checklist natively in order; this is one small test-catalog change.

**Goal:** Register the already implemented generic compact-delegation protocol as P22 in the authoritative real-machine campaign and prove it through the campaign runner.

**Architecture:** Keep `scripts/build_real_case_catalog.py` authoritative. Reuse `case_p22` from the parallel protocol driver, add only the runner arguments required by the campaign adapter, regenerate `tests/real_cases/cases.json`, and execute exactly P22 against the deployed release.

**Tech Stack:** Python 3.13, pytest, HTTPX, OpenSandbox, Codex Child Harness Jobs

**Spec:** `docs/superpowers/specs/2026-11-18-input-routing-delegation-and-ppt-design.md`

## Constraints

- The case must remain media- and Skill-independent.
- Success requires two persisted Child Harness Jobs and at least one durable continuation.
- The unified campaign runner, not a direct protocol invocation, is the acceptance entry.
- Test evidence and reports remain under `.data` on `/data2`.

### Task 1: Freeze P22 In The Campaign

**Files:**
- Modify: `scripts/build_real_case_catalog.py`
- Modify: `tests/test_real_case_catalog.py`
- Modify: `tests/test_real_campaign_runner.py`
- Modify: `tests/run_real_parallel_protocol.py`
- Regenerate: `tests/real_cases/cases.json`

- [ ] Make catalog/count/runner tests require P22 and verify they fail before registration.
- [ ] Register P22 with compact delegation, metadata-free execution and continuation expectations.
- [ ] Accept the campaign adapter's bounded timeout/intervention arguments in the protocol CLI.
- [ ] Regenerate the catalog and run focused tests.
- [ ] Execute P22 through `tests/run_real_campaign.py --case P22` and inspect persisted Job/Attempt cleanup evidence.
