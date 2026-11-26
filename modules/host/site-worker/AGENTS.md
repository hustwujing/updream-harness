# Site Worker Module Rules

- Accept only bounded, path-safe bundles containing a valid `index.html`.
- A draft requires confirmation; activation is atomic and versions remain immutable.
- Retry only classified infrastructure failures under the leased generation.
- Do not read Core artifacts or Threads through the Sites database.
- Preserve CSP isolation and public downline/rollback behavior.
- Complete with bundle security, restart, version and public-route tests.
