#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "$0")/.." && pwd)
service_user=${1:-sandbox-harness}
cd "$project_root"
# Limit grants to deployable source/assets, never .env or credentials.
setfacl -R -m "u:${service_user}:rX" src web/dist .venv/bin .venv/lib/python*/site-packages
runuser -u "$service_user" -- .venv/bin/python - <<'PY'
from pathlib import Path
import sandbox_harness.app
import sandbox_harness.gateway
import sandbox_harness.media_service
for file in Path('web/dist').rglob('*'):
    if file.is_file():
        with file.open('rb') as stream:
            stream.read(1)
print('Service user can import API/Gateway/Media Worker and read every web asset')
PY
