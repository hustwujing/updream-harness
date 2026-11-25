# Child allocation recovery implementation plan

**Goal:** Preserve allocation ownership through create uncertainty without claiming Runtime acceptance.

**Architecture:** Reuse JobAttempt provisional receipts, Scheduler reservations,
SandboxPool capacity, common reconcile_create, and existing terminal cleanup.
No provider/case/operation branches or provider metadata updates.

**Spec:** `docs/superpowers/specs/2026-11-25-child-allocation-recovery-design.md`.

1. [ ] Reproduce delayed create and cancellation at the Child boundary.
2. [ ] Add opt-in pending allocation capacity/reconciliation to Pool; test one-slot ownership and deletion fences.
3. [ ] Persist allocation intent; retain/recover it across Worker cancellation and replacement.
4. [ ] Preserve provisional cleanup across generations, protect allocation IDs from orphan cleanup and provider switching.
5. [ ] Verify failure/timeout/cancellation paths, run focused/full deterministic tests, obtain independent review, commit.
6. [ ] Evaluate case impact, release Ali candidate and rerun only failed/affected cases plus unfinished cases.

The user has authorized continuous implementation; no further plan approval is required.
