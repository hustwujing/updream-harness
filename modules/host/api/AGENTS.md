# API Module Rules

- Keep browser identity, owner checks and public error mapping at the API boundary.
- Preserve one-active-Turn serialization and Broker lease/generation fencing.
- Do not query Media or Sites tables through a Core session.
- Pending registration/archive/delete operations return durable operation IDs.
- Sandbox cleanup must confirm provider absence before capacity release.
- SSE may overlay owner-checked Redis live text, but reconnect correctness and
  terminal closure remain anchored to PostgreSQL committed cursors.
- Complete with focused API/Broker tests, frontend tests/build and live health evidence.
