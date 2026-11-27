# Site Worker

Validates confirmed static-site bundles and atomically publishes immutable versions.

- Entry: `sandbox_harness.services.site_worker.site_service:run`
- Database: sites only
- Storage: content-addressed object store; API serves owner previews and public versions
- Verification: site repository/service/deployment and browser preview tests

Failures use the shared stable error and sanitized public-reason contract, so
the browser and any continuation receive the same actionable explanation.
