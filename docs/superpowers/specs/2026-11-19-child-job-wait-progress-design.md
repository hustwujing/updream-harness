# Child Job Wait Progress Design

## Intent

When a Primary Harness has ended its current Turn and durable child work is
still running, the user must see that the system is waiting rather than dead.
The wait indicator must show deterministic progress such as `0/3`, `1/3`,
`2/3`, `3/3` and remain correct across refresh, reconnect and service restart.

## Goals

- Show one waiting step per active JobGroup in both the transcript and the
  execution graph.
- Count every terminal child outcome as returned: success, failure, cancelled,
  blocked and unknown.
- Show completed/total plus success, failure, cancellation and unknown detail.
- Animate only while at least one member remains nonterminal.
- Retain the completed `N/N` wait step as historical evidence after the
  continuation resumes the Primary Harness.
- Rebuild entirely from PostgreSQL facts without an additional mutable progress
  record.

## Non-goals

- A failed child does not count as successful.
- The wait step does not reinterpret child output or choose recovery policy.
- Provider media batches keep their existing media progress surface; this
  design covers generic durable JobGroups, including Operation, Provider,
  Child Harness and Reviewer members.
- No prompt text or transcript prose is parsed to infer progress.

## Approaches

1. A frontend-only guess from the active Turn was rejected because the parent
   is terminal while its children run and refresh/recovery would lose state.
2. A separately persisted transcript progress Item was rejected because it
   duplicates Job state and needs an additional event/repair protocol.
3. Selected: project progress from authoritative JobGroup and Job rows, expose
   it through the existing execution graph snapshot and render the same facts
   in the graph and transcript.

## Data Contract

Every `job_group` graph node includes:

```json
{
  "progress": {
    "terminal": 2,
    "total": 3,
    "succeeded": 1,
    "failed": 1,
    "cancelled": 0,
    "blocked": 0,
    "unknown": 0
  }
}
```

The projector also emits a deterministic `wait_barrier:<group_id>` node. It is
`RUNNING` while `terminal < total` and `COMPLETED` once `terminal == total`.
The barrier is connected after the group's member Jobs and before the
continuation input. It runs on the control plane and never claims that the
Primary Harness itself is executing.

## User Interface

The transcript displays an execution step under the originating Turn:

```text
⟳ 正在等待全部子任务返回 · 1/3
  1 已完成 · 其余仍在执行
```

At terminal progress:

```text
✓ 全部子任务已返回 · 3/3
  2 成功 · 1 失败
```

The transcript consumes a compact current execution-graph snapshot and polls
once per second only while a nonterminal barrier exists. Terminal snapshots
are refreshed through the normal Thread refresh path. The execution graph uses
the same node and therefore shows the same count and breathing state.

## Recovery And Ordering

- Counts are derived in one snapshot transaction and cannot exceed `total`.
- A Job terminal replay is idempotent and does not increment progress twice.
- Group continuation may race with the browser refresh; both `TERMINAL` and
  `CONTINUED` groups render `N/N`.
- A replaced or retried Job remains one group member; generation changes do
  not change `total`.
- A user viewing an older Turn sees its frozen group progress but is not pulled
  to a newer Turn.

## Verification

- Projector tests cover mixed success/failure/cancelled groups and replay.
- Layout tests cover barrier placement before continuation.
- Browser tests cover `0/3` through `3/3`, active animation, terminal stop,
  refresh/re-entry and no page errors.
- A deployed real JobGroup confirms progress survives an API/worker restart.
