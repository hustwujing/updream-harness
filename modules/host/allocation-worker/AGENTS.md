# Allocation Worker Module Rules

- A create intent precedes the provider RPC.
- Unknown create outcomes retain capacity and reconcile the original allocation ID.
- Do not infer absence from empty inventory, timeout or process death.
- Lease/generation fences every allocation write; reconciliation remains independently schedulable.
- Provider-specific adapters implement the common lifecycle contract without case branches.
- Complete with crash points before/after RPC and provider-capacity exhaustion tests.
