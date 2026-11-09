#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
if [[ "$root" != /data2/wujing03/sandbox-harness-platform ]]; then
  echo "unexpected project root: $root" >&2
  exit 1
fi
if [[ ! -f /etc/sandbox-harness/platform.env ]]; then
  echo "create /etc/sandbox-harness/platform.env from .env.example first" >&2
  exit 1
fi
if ! getent passwd sandbox-harness >/dev/null; then
  useradd --system --home-dir "$root/.data" --shell /usr/sbin/nologin sandbox-harness
fi
install -d -o sandbox-harness -g sandbox-harness -m 0700 "$root/.data"
setfacl -m u:sandbox-harness:rx /data2/wujing03 "$root"
chmod 0755 "$root/deploy/network-rule.sh"
install -m 0644 "$root"/deploy/systemd/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable sandbox-harness-network.service sandbox-harness-gateway.service sandbox-harness-api.service
echo "units installed but not started"
