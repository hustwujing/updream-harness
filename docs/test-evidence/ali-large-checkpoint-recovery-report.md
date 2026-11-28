# Ali Large Checkpoint Recovery Report

## Incident

Thread `42772a1eb3784210a527e67864215789` had a valid 210,749,440-byte checkpoint
after its 13:33 Turn. At 14:17 a new Ali sandbox and Runtime started normally,
but four monolithic restore attempts each ended in `httpx.ReadError` after the
first streamed request chunk. The Turn correctly surfaced
`sandbox_workspace_restore_unavailable`.

The failure path then checkpointed the fresh sandbox even though restore had
never succeeded. It committed a 10,240-byte partial checkpoint before artifact
inspection failed. The next user Turn restored that incomplete state and Codex
`thread/resume` returned RPC `-32600`, surfaced as `native_session_unavailable`.
A later partial save moved the Thread pointer again to a 2,990,080-byte object.

## Recovery

The last good object remained in content-addressed storage:

- object: `sha256:0e4f311420bb82ca13654b67cfbca33aae297833b4c7ca63a8b4db73ce4496ab`
- size: 210,749,440 bytes
- archive members: 271
- Codex SQLite integrity: `ok`
- Codex thread rows: exactly one
- native ID: `01a22066-71e9-79e3-ab4f-e3f52d338476`

With no active Turn, the owner-fenced idle checkpoint API restored the Thread
pointer to this object. It did not create a Turn or replay either failed message.

## Fix

- PlatformWorker tracks whether prior workspace restoration was confirmed.
  Before that boundary, its execution-failure path skips partial checkpoint and
  preserves the existing Thread pointer.
- Checkpoints above 8 MiB use a provider-neutral Runtime upload protocol with
  1 MiB fixed chunks, immutable offsets, byte-identical retries and exact
  total-size/SHA-256 commit before extraction.
- Chunk, begin and commit receipts are idempotent. A lost response cannot create
  a second restore. Runtime restart invalidates unfinished staging.
- Small checkpoints retain the existing bounded whole-request path.

The fix has no Ali-specific branch. OpenSandbox and future providers use the
same Host/Runtime contract.

## Evidence

- Checkpoint/Broker/Runtime focused tests: 106 passed.
- Bundle and image contract tests: 32 passed.
- Final Python gate: 1,647 passed, 23 skipped, zero failed in 354.55 seconds.
- A fault-injected 9 MiB valid archive restored through an ingress rejecting
  every request over 1 MiB. The first successful chunk response and first
  successful commit response were both dropped; restore still completed once,
  and staging was empty afterward.
- Real Ali sandbox restore of the recovered 210,749,440-byte object completed in
  14.878 seconds. The three largest MP4 files were present with exact sizes
  44,003,351, 44,475,665 and 45,237,031 bytes. The isolated sandbox was deleted.

## Deployment

- PlatformWorker release:
  `fa0671e1a857f6f638a6daeb80880fc5de08cc2fb9cfc21ac9ac759e8adc74f7`.
- API release:
  `51f8d7fbd57ba5f2a096ca72681bc7ba42a3e0eedf495187a3c97e81ff46e686`.
- Runtime Code Bundle:
  `5f94eb7bf3b4e2c753732ee9081946816a8719bd44bdba25e46445f610fec3db`.
- API and PlatformWorker use the same configured Code Bundle digest. Health is
  ready with zero active Turns or reconciliations after deployment.
