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
import sandbox_harness.site_service
for file in Path('web/dist').rglob('*'):
    if file.is_file():
        with file.open('rb') as stream:
            stream.read(1)
print('Service user can import API/Gateway/Media Worker/Site Worker and read every web asset')
PY
test -x /usr/bin/libreoffice
test -r /etc/libreoffice/registry/impress.xcd
grep -Fx 'TemporaryFileSystem=/tmp:rw,nosuid,nodev,noexec,mode=1777' \
  "$project_root/deploy/systemd/sandbox-harness-api.service" >/dev/null
install -d -o "$service_user" -g "$service_user" -m 0700 "$project_root/.data/tmp/preview-runtime" "$project_root/.data/tmp/preview-config"
install -d -o "$service_user" -g "$service_user" -m 0700 "$project_root/.data/preview-cache"
chown -R "$service_user:$service_user" "$project_root/.data/preview-cache"
chmod -R u=rwX,go= "$project_root/.data/preview-cache"
runuser -u "$service_user" -- env HOME="$project_root/.data/tmp" \
  XDG_RUNTIME_DIR="$project_root/.data/tmp/preview-runtime" XDG_CONFIG_HOME="$project_root/.data/tmp/preview-config" \
  /usr/bin/libreoffice --headless --version >/dev/null
echo 'Service user can execute LibreOffice Impress and API private /tmp permits IPC pipes'
