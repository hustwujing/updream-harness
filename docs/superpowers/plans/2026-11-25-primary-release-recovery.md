# Preserve release revision during Primary recovery

The D08 API restart exposed `sandbox_profile_unavailable` in D06's business Turn
and an R17 continuation. D06's separate Child SIGKILL/reuse protocol passed.
Pool creation resolved the provider release revision, but Broker recovery built
a resolver without that release mapping and defaulted revision to provider_ref.
The resulting compatibility key differed even though the recorded execution
was still available.

Use the durable SandboxHandle revision when reconstructing the expected Primary
profile. Keep the Turn's frozen provider reference and Code/Content Bundle
digests, and retain the Pool compatibility comparison. All call sites already
validate the recorded handle before adoption. There is no provider/case branch
and no provider metadata update or execution resubmission.

The recovery regression now uses a release revision distinct from its provider
reference and a changed current release and Bundle configuration. It first
failed in sandbox.adopt, then passed with reattachment, Runtime journal replay
and cleanup. The focused Broker/Pool/provider/factory/inspector suite passes111.

Real scope: rerun D06 and R17, plus D08 as the API restart trigger while Primary
work is active. Retain other successful cases: the report/DB audit found no
successful Primary adoption in the affected path; R13 recovered before Harness
start. Child-only restarts and Sandbox Inspector APIs use separate paths.
Complete the unrun matrix under the user's scoped-retest policy. Full suite,
real reruns and final deployment remain pending.
