#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
[[ "$root" == /data2/wujing03/sandbox-harness-platform-parallel-ali ]] || {
  echo "unexpected_project_root:$root" >&2; exit 1; }
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
env_file="$root/.data/config/platform.env"
[[ -f "$env_file" ]] || { echo "Ali environment is not configured" >&2; exit 1; }
set -a
source "$env_file"
set +a

wait_port() {
  local host=$1 port=$2
  for _ in $(seq 1 60); do
    if timeout 1 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "port did not become ready: $host:$port" >&2; return 1
}

start_transient() {
  local unit=$1 user=$2
  shift 2
  if systemctl is-active --quiet "$unit.service"; then return 0; fi
  systemd-run --unit="$unit" --collect --uid="$user" \
    --property=WorkingDirectory="$root" --property=Restart=on-failure \
    --property=RestartSec=3 --property=KillMode=control-group "$@"
}

start_transient sandbox-harness-parallel-ali-postgresql sandbox-harness-db \
  "$root/deploy/ali/postgresql-start.sh"
wait_port 127.0.0.1 28433

if ! systemctl is-active --quiet sandbox-harness-parallel-ali-network.service; then
  systemd-run --unit=sandbox-harness-parallel-ali-network --collect \
    --property=Type=oneshot --property=RemainAfterExit=yes \
    "$root/deploy/ali/network-rule.sh" install
fi

systemctl stop sandbox-harness-parallel-ali-gateway.service 2>/dev/null || true
case "${SANDBOX_HARNESS_BACKEND:-}" in
  opensandbox)
    if systemctl is-active --quiet sandbox-harness-parallel-ali-gateway-aliyun.service; then
      echo "provider switch requires deploy/ali/stop.sh first" >&2; exit 1
    fi
    start_transient sandbox-harness-parallel-ali-gateway-opensandbox sandbox-harness \
      "$root/deploy/ali/run-service.sh" gateway-opensandbox
    wait_port 172.31.0.1 29882
    wait_port 172.31.0.1 29883
    ;;
  aliyun)
    [[ "${SANDBOX_HARNESS_ALIYUN_SANDBOX_GATEWAY_URL:-}" == "http://10.194.49.8:29882" && \
       "${SANDBOX_HARNESS_ALIYUN_SANDBOX_PROXY_URL:-}" == "http://10.194.49.8:29883" ]] || {
      echo "Aliyun sandbox routes are not explicitly configured" >&2; exit 1; }
    if systemctl is-active --quiet sandbox-harness-parallel-ali-gateway-opensandbox.service; then
      echo "provider switch requires deploy/ali/stop.sh first" >&2; exit 1
    fi
    start_transient sandbox-harness-parallel-ali-gateway-aliyun sandbox-harness \
      "$root/deploy/ali/run-service.sh" gateway-aliyun
    wait_port 10.194.49.8 29882
    wait_port 10.194.49.8 29883
    ;;
  *) echo "unsupported sandbox backend: ${SANDBOX_HARNESS_BACKEND:-unset}" >&2; exit 1 ;;
esac
if [[ ! -f "$root/.data/services/image-layered/service/app.py" ]]; then
  "$root/deploy/ali/materialize-image-decompose.sh"
fi
start_transient sandbox-harness-parallel-ali-image-decompose root \
  "$root/deploy/ali/run-service.sh" image-decompose
wait_port 127.0.0.1 8912
start_transient sandbox-harness-parallel-ali-job-worker sandbox-harness \
  "$root/deploy/ali/run-service.sh" job-worker
start_transient sandbox-harness-parallel-ali-controlled-worker root \
  "$root/deploy/ali/run-service.sh" controlled-worker
start_transient sandbox-harness-parallel-ali-media-worker sandbox-harness \
  "$root/deploy/ali/run-service.sh" media-worker
start_transient sandbox-harness-parallel-ali-site-worker sandbox-harness \
  "$root/deploy/ali/run-service.sh" site-worker
start_transient sandbox-harness-parallel-ali-api sandbox-harness \
  "$root/deploy/ali/run-service.sh" api
wait_port 127.0.0.1 24767
"$root/deploy/ali/status.sh"
