# OpenSandbox State Ownership Design

## Intent

The shared OpenSandbox service must remain available when predecessor products
are stopped or their runtime data is discarded. Its mutable state must not live
under `distributed-agent-sandbox-backend/.sandbox-cluster`.

## Chosen approach

Move the server config, SQLite store, Docker metadata, volumes, temporary files
and logs to `/data2/wujing03/opensandbox-stack/data/server`. Keep the API key in
the existing protected environment file, but make systemd override the config
and temporary paths with the OpenSandbox-owned locations.

Existing sandbox lifecycle state is disposable because the environment is not
online. Stop clients, remove existing OpenSandbox containers, initialize a fresh
store, restart clients and let their warm pools converge. Preserve the Docker
daemon data and image cache.

## Acceptance

- No OpenSandbox unit or config path references a stopped product runtime tree.
- Health, create, metadata patch, Runtime health and delete all succeed.
- Parallel API reports `backend=opensandbox` and `warm_pool=ready`.
- Stopped Plus and distributed-agent services remain disabled.
