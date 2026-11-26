# Platform Worker Module Rules

- Keep durable Turn and continuation claims multi-active across replicas.
- Run singleton coordination only while holding the current database-fenced leader generation.
- Losing the leader lease cancels singleton loops; it must not cancel independently leased business work.
- Reattach accepted Runtime execution and preserve unknown external outcomes across process loss.
- Keep API request handling out of this module.
- Complete with leader failover, API-independent restart, zero-replay and real Sandbox evidence.
