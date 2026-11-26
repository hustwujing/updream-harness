# Allocation Worker

Owns provider sandbox create and reconciliation RPCs for durable Job allocations.

- Entry: `sandbox_harness.services.allocation_worker.allocation_service:main`
- Database: core only
- Contract: immutable allocation request, leased claim, authoritative reconcile/delete receipt
- Verification: allocation dispatch/recovery/PostgreSQL tests and RC/AR real gates
