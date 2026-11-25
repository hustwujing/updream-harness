#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
[[ "$root" == /data2/wujing03/sandbox-harness-platform-parallel-ali ]] || {
  echo "unexpected_project_root:$root" >&2; exit 1; }
stop_units=(
  sandbox-harness-parallel-ali-api
  sandbox-harness-parallel-ali-site-worker
  sandbox-harness-parallel-ali-media-worker
  sandbox-harness-parallel-ali-controlled-worker
  sandbox-harness-parallel-ali-job-worker
  sandbox-harness-parallel-ali-allocation-worker
  sandbox-harness-parallel-ali-image-decompose
  sandbox-harness-parallel-ali-gateway-aliyun
  sandbox-harness-parallel-ali-gateway-opensandbox
  sandbox-harness-parallel-ali-gateway
  sandbox-harness-parallel-ali-network
  sandbox-harness-parallel-ali-postgresql
)
for unit in "${stop_units[@]}"; do
  systemctl stop "$unit.service" 2>/dev/null || true
done
"$root/deploy/ali/network-rule.sh" remove
