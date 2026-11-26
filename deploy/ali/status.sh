#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
[[ "$root" == /data2/wujing03/sandbox-harness-platform-parallel-ali ]] || {
  echo "unexpected_project_root:$root" >&2; exit 1; }
source "$root/.data/config/platform.env"
printf 'selected sandbox backend: %s\n' "${SANDBOX_HARNESS_BACKEND:-unset}"
units=(
  sandbox-harness-parallel-ali-postgresql
  sandbox-harness-parallel-ali-network
  sandbox-harness-parallel-ali-sandbox-gateway-opensandbox
  sandbox-harness-parallel-ali-sandbox-gateway-aliyun
  sandbox-harness-parallel-ali-image-inference
  sandbox-harness-parallel-ali-job-worker
  sandbox-harness-parallel-ali-allocation-worker
  sandbox-harness-parallel-ali-controlled-worker
  sandbox-harness-parallel-ali-media-worker
  sandbox-harness-parallel-ali-site-worker
  sandbox-harness-parallel-ali-platform-worker
  sandbox-harness-parallel-ali-api
)
for unit in "${units[@]}"; do
  name="$unit.service"
  printf '%-58s %s\n' "$name" "$(systemctl is-active "$name" 2>/dev/null || true)"
done

port_state() {
  local label=$1 host=$2 port=$3 state=closed
  if ss -H -lnt "sport = :$port" | awk -v address="$host:$port" '$4 == address {found=1} END {exit !found}'; then
    state=listening
  fi
  printf '%-28s %-21s %s\n' "$label" "$host:$port" "$state"
}
for value in \
  'sandbox-gateway-opensandbox 172.31.0.1 29882' \
  'proxy-opensandbox 172.31.0.1 29883' \
  'sandbox-gateway-aliyun 10.194.49.8 29882' \
  'proxy-aliyun 10.194.49.8 29883' \
  'image-inference 127.0.0.1 8912' \
  'api 127.0.0.1 24767' \
  'postgresql 127.0.0.1 28433'; do
  read -r label host port <<<"$value"
  port_state "$label" "$host" "$port"
done
curl -sS --max-time 5 http://127.0.0.1:24767/api/health 2>/dev/null || true
printf '\n'
