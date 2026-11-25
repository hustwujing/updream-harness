#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
if [[ "$root" != /data2/wujing03/sandbox-harness-platform-parallel-ali ]]; then
  echo "unexpected_project_root:$root" >&2
  exit 1
fi
component=${1:-}
env_file="$root/.data/config/platform.env"
log_root="$root/.data/service-logs"
[[ -f "$env_file" ]] || { echo "missing $env_file" >&2; exit 1; }
mkdir -p "$log_root" "$root/.data/tmp"
set -a
source "$env_file"
set +a
case "${SANDBOX_HARNESS_BACKEND:-}" in
  opensandbox) provider_gateway_host="172.31.0.1" ;;
  aliyun) provider_gateway_host="10.194.49.8" ;;
  *) echo "unsupported sandbox backend: ${SANDBOX_HARNESS_BACKEND:-unset}" >&2; exit 1 ;;
esac
export SANDBOX_HARNESS_GATEWAY_URL="http://${provider_gateway_host}:${SANDBOX_HARNESS_GATEWAY_BIND_PORT}"
export TMPDIR="$root/.data/tmp"
export XDG_CACHE_HOME="$root/.data/cache"
export PIP_CACHE_DIR="$root/.data/pip-cache"
export UV_CACHE_DIR="$root/.data/uv-cache"
export npm_config_cache="$root/.data/npm-cache"
export PLAYWRIGHT_BROWSERS_PATH="$root/.data/playwright-browsers"
export PYTHONDONTWRITEBYTECODE=1
export CUDA_CACHE_PATH="$root/.data/cache/cuda"
export HF_HOME="$root/.data/cache/huggingface"
export TORCH_HOME="$root/.data/cache/torch"
export TORCHINDUCTOR_CACHE_DIR="$root/.data/cache/torchinductor"
export TRITON_CACHE_DIR="$root/.data/cache/triton"

case "$component" in
  api) command=("$root/.venv/bin/sandbox-harness-parallel-api") ;;
  gateway-opensandbox)
    export SANDBOX_HARNESS_GATEWAY_BIND_HOST="172.31.0.1"
    export SANDBOX_HARNESS_GATEWAY_URL="http://172.31.0.1:${SANDBOX_HARNESS_GATEWAY_BIND_PORT}"
    command=("$root/.venv/bin/sandbox-harness-parallel-gateway")
    ;;
  gateway-aliyun)
    export SANDBOX_HARNESS_GATEWAY_BIND_HOST="10.194.49.8"
    export SANDBOX_HARNESS_GATEWAY_URL="http://10.194.49.8:${SANDBOX_HARNESS_GATEWAY_BIND_PORT}"
    command=("$root/.venv/bin/sandbox-harness-parallel-gateway")
    ;;
  image-decompose)
    service_root="$root/.data/services/image-layered"
    [[ -f "$service_root/service/app.py" ]] || { echo "image decompose service is not materialized" >&2; exit 1; }
    "$root/.venv/bin/python" "$root/deploy/ali/isolate-image-service.py"
    export PYTHONPATH="$service_root:${PYTHONPATH:-}"
    command=(/data/wujing03/miniforge3/envs/media-foundation/bin/python -m uvicorn
      service.app:app --host 127.0.0.1 --port 8912 --no-access-log)
    ;;
  job-worker) command=("$root/.venv/bin/sandbox-harness-parallel-job-worker") ;;
  allocation-worker) command=("$root/.venv/bin/python" -m sandbox_harness.allocation_service) ;;
  controlled-worker) command=("$root/.venv/bin/sandbox-harness-parallel-controlled-worker") ;;
  media-worker) command=("$root/.venv/bin/sandbox-harness-parallel-media-worker") ;;
  site-worker) command=("$root/.venv/bin/sandbox-harness-parallel-site-worker") ;;
  *) echo "unknown Ali service: $component" >&2; exit 2 ;;
esac
exec "${command[@]}" >>"$log_root/$component.log" 2>&1
