#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
if [[ "$root" != /data/wujing02/sandbox-harness-platform-parallel-ali ]]; then
  echo "unexpected_project_root:$root" >&2
  exit 1
fi
component=${1:-}
trap 'status=$?; echo "service launcher failed component=$component line=$LINENO status=$status" >&2' ERR
env_file="$root/.data/config/platform.env"
[[ -f "$env_file" ]] || { echo "missing $env_file" >&2; exit 1; }
set -a
source "$env_file"
set +a
log_root=$(readlink -m "${SANDBOX_HARNESS_SERVICE_LOG_DIR:-$root/.data/service-logs}")
case "$log_root" in "$root/.data"|"$root/.data/"*) ;; *) echo "service_log_directory_outside_data_root" >&2; exit 1 ;; esac
log_template=${SANDBOX_HARNESS_SERVICE_LOG_FILE_TEMPLATE:-\{component\}.log}
[[ "$log_template" =~ ^[A-Za-z0-9_.{}-]+$ && "$log_template" != *".."* && "$log_template" != */* ]] || {
  echo "invalid_service_log_filename_template" >&2; exit 1; }
log_file=${log_template//\{component\}/$component}
[[ "$log_file" =~ ^[A-Za-z0-9_.-]+\.log$ ]] || { echo "invalid_service_log_filename" >&2; exit 1; }
mkdir -p "$log_root" "$root/.data/tmp"

restrict_database_credentials() {
  unset SANDBOX_HARNESS_ADMIN_DATABASE_URL SANDBOX_HARNESS_DATABASE_URL
  case "$component" in
    api|platform-worker) ;;
    sandbox-gateway-opensandbox|sandbox-gateway-aliyun)
      unset SANDBOX_HARNESS_ACCOUNTS_DATABASE_URL
      ;;
    job-worker)
      unset SANDBOX_HARNESS_ACCOUNTS_DATABASE_URL
      unset SANDBOX_HARNESS_SITES_DATABASE_URL
      ;;
    allocation-worker|controlled-worker)
      unset SANDBOX_HARNESS_ACCOUNTS_DATABASE_URL
      unset SANDBOX_HARNESS_MEDIA_DATABASE_URL
      unset SANDBOX_HARNESS_SITES_DATABASE_URL
      ;;
    media-worker)
      unset SANDBOX_HARNESS_ACCOUNTS_DATABASE_URL
      unset SANDBOX_HARNESS_CORE_DATABASE_URL
      unset SANDBOX_HARNESS_SITES_DATABASE_URL
      ;;
    site-worker)
      unset SANDBOX_HARNESS_ACCOUNTS_DATABASE_URL
      unset SANDBOX_HARNESS_CORE_DATABASE_URL
      unset SANDBOX_HARNESS_MEDIA_DATABASE_URL
      ;;
    image-inference)
      unset SANDBOX_HARNESS_ACCOUNTS_DATABASE_URL
      unset SANDBOX_HARNESS_CORE_DATABASE_URL
      unset SANDBOX_HARNESS_MEDIA_DATABASE_URL
      unset SANDBOX_HARNESS_SITES_DATABASE_URL
      ;;
  esac
}
restrict_database_credentials

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
  sandbox-gateway-opensandbox|sandbox-gateway-aliyun) service=sandbox-gateway ;;
  api|platform-worker|job-worker|allocation-worker|controlled-worker|media-worker|site-worker) service="$component" ;;
  image-inference) service="" ;;
  *) echo "unknown Ali service: $component" >&2; exit 2 ;;
esac
if [[ -n "$service" ]]; then
  mapfile -t release < <(/data/wujing03/miniforge3/bin/python \
    "$root/.data/release-tools/service_release.py" resolve --data-root "$root/.data" --service "$service")
  [[ ${#release[@]} -eq 4 && -x "${release[0]}" && -d "${release[1]}" && -d "${release[2]}" ]] || {
    echo "invalid selected service release: $service" >&2; exit 1; }
  release_python=${release[0]}
  export PYTHONPATH=${release[1]}
  export SANDBOX_HARNESS_RESOURCE_ROOT=${release[2]}
  export SANDBOX_HARNESS_RELEASE_DIGEST=${release[3]}
  export SANDBOX_HARNESS_RELEASE=${release[3]}
fi

case "$component" in
  api) command=("$release_python" -m sandbox_harness.services.api.main) ;;
  platform-worker) command=("$release_python" -m sandbox_harness.services.platform_worker.platform_service) ;;
  sandbox-gateway-opensandbox)
    export SANDBOX_HARNESS_GATEWAY_BIND_HOST="172.31.0.1"
    export SANDBOX_HARNESS_GATEWAY_URL="http://172.31.0.1:${SANDBOX_HARNESS_GATEWAY_BIND_PORT}"
    command=("$release_python" -m sandbox_harness.services.sandbox_gateway.gateway)
    ;;
  sandbox-gateway-aliyun)
    export SANDBOX_HARNESS_GATEWAY_BIND_HOST="10.194.49.8"
    export SANDBOX_HARNESS_GATEWAY_URL="http://10.194.49.8:${SANDBOX_HARNESS_GATEWAY_BIND_PORT}"
    command=("$release_python" -m sandbox_harness.services.sandbox_gateway.gateway)
    ;;
  image-inference)
    mapfile -t image_release < <(/data/wujing03/miniforge3/bin/python \
      "$root/.data/release-tools/image_inference_release.py" resolve --data-root "$root/.data")
    [[ ${#image_release[@]} -eq 3 && -x "${image_release[0]}" && -d "${image_release[1]}" ]] || {
      echo "invalid selected image-inference release" >&2; exit 1; }
    image_python=${image_release[0]};service_root=${image_release[1]}
    export SANDBOX_HARNESS_RELEASE=${image_release[2]}
    [[ -f "$service_root/service/app.py" ]] || { echo "image decompose service is not materialized" >&2; exit 1; }
    "$root/.venv/bin/python" "$root/deploy/ali/isolate-image-service.py"
    export PYTHONPATH="$service_root/python-packages:$service_root"
    command=("$image_python" -m uvicorn
      service.app:app --host 127.0.0.1 --port 8912 --no-access-log)
    ;;
  job-worker) command=("$release_python" -m sandbox_harness.services.job_worker.job_service) ;;
  allocation-worker) command=("$release_python" -m sandbox_harness.services.allocation_worker.allocation_service) ;;
  controlled-worker) command=("$release_python" -m sandbox_harness.services.controlled_worker.controlled_service) ;;
  media-worker) command=("$release_python" -m sandbox_harness.services.media_worker.media_service) ;;
  site-worker) command=("$release_python" -m sandbox_harness.services.site_worker.site_service) ;;
esac
exec "${command[@]}" >>"$log_root/$log_file" 2>&1
