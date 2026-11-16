#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
if [[ "$root" != /data2/wujing03/sandbox-harness-platform-parallel ]]; then
  echo "unexpected project root: $root" >&2
  exit 1
fi
env_file="$root/.data/config/platform.env"
if [[ ! -f "$env_file" ]]; then
  echo "create $env_file from .env.example first" >&2
  exit 1
fi
configured_data_root=$(sed -n 's/^SANDBOX_HARNESS_DATA_ROOT=//p' "$env_file" | tail -1)
if [[ "$configured_data_root" != "$root/.data" ]]; then
  echo "SANDBOX_HARNESS_DATA_ROOT must be $root/.data; root-partition storage is forbidden" >&2
  exit 1
fi
if ! getent passwd sandbox-harness >/dev/null; then
  useradd --system --home-dir "$root/.data" --shell /usr/sbin/nologin sandbox-harness
fi
if ! getent passwd sandbox-harness-db >/dev/null; then
  useradd --system --home-dir "$root/.data/postgresql" --shell /usr/sbin/nologin sandbox-harness-db
fi
install -d -o sandbox-harness -g sandbox-harness -m 0700 "$root/.data"
install -d -o sandbox-harness -g sandbox-harness -m 0700 "$root/.data"/{cache,npm-cache,objects,pip-cache,playwright-browsers,preview-cache,runtime-bundles,runtime-code-bundles,service-logs,tmp,uv-cache}
chmod 1777 "$root/.data/tmp"
install -d -o root -g sandbox-harness -m 0750 "$root/.data/config"
install -d -o sandbox-harness-db -g sandbox-harness-db -m 0700 "$root/.data/postgresql"
install -d -o sandbox-harness-db -g sandbox-harness-db -m 0700 "$root/.data/postgresql/run"
setfacl -m u:sandbox-harness:rx /data2/wujing03 "$root"
setfacl -m u:sandbox-harness-db:rx /data2/wujing03 "$root"
setfacl -m u:sandbox-harness-db:x "$root/.data"
setfacl -m u:sandbox-harness:rx "$root/web"
setfacl -m d:u:sandbox-harness:rX "$root/web"
setfacl -R -m u:sandbox-harness:rX "$root/.venv" "$root/src" "$root/web/dist"
setfacl -R -m u:sandbox-harness:rX "$root/runtime"
setfacl -m u:sandbox-harness:rx "$root/deploy"
setfacl -m u:sandbox-harness:r "$root/deploy/job-catalog.example.json"
find "$root/src" -type d -exec setfacl -m d:u:sandbox-harness:rX {} +
setfacl -m d:u:sandbox-harness:rX "$root/web/dist"
chmod 0755 "$root/deploy/network-rule.sh"
install -m 0644 "$root"/deploy/systemd/*.service /etc/systemd/system/
python3 "$root/deploy/record-release.py" --env-file "$env_file"
systemctl daemon-reload
systemctl enable sandbox-harness-parallel-postgresql.service sandbox-harness-parallel-network.service sandbox-harness-parallel-gateway.service sandbox-harness-parallel-job-worker.service sandbox-harness-parallel-api.service sandbox-harness-parallel-media-worker.service sandbox-harness-parallel-site-worker.service
echo "units installed but not started"
