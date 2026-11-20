#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
[[ "$root" == /data2/wujing03/sandbox-harness-platform-parallel ]]
docker=/data2/wujing03/opensandbox-stack/tools/docker-29.9.3/docker
python=${SANDBOX_HARNESS_BUILD_PYTHON:-$root/.venv/bin/python}
test -x "$python"
export DOCKER_HOST=unix:///run/docker.sock
git -C "$root" diff --quiet
git -C "$root" diff --cached --quiet
"$python" "$root/runtime/skill_bundle.py" verify

registry=${SANDBOX_HARNESS_IMAGE_REGISTRY:-127.0.0.1:25001/sandbox-harness-parallel}
tag=$(git -C "$root" rev-parse --short=12 HEAD)
sandbox_username=${SANDBOX_HARNESS_SANDBOX_USERNAME:-updream}
profiles=${BUILD_PROFILES:-"primary-full-cpu general-cpu media-cpu office web-browser data-analysis"}
harnesses=${BUILD_HARNESSES:-"codex claude deepseek"}
all_profiles="primary-full-cpu general-cpu media-cpu office web-browser data-analysis"
all_harnesses="codex claude deepseek"
drawio_vendor=${SANDBOX_HARNESS_DRAWIO_VENDOR:-/data2/wujing03/sandbox-harness-platform/runtime/vendor}
asr_source_image=${SANDBOX_HARNESS_ASR_SOURCE_IMAGE:-sandbox-harness-runtime-base:0027ec835f16}
profile_catalog_path=${SANDBOX_HARNESS_PROFILE_CATALOG_PATH:-}
if [[ -z "$profile_catalog_path" && -f "$root/.data/config/platform.env" ]]; then
  profile_catalog_path=$(sed -n 's/^SANDBOX_HARNESS_PROFILE_CATALOG_PATH=//p' \
    "$root/.data/config/platform.env" | tail -1)
fi
codex_source_image=${SANDBOX_HARNESS_CODEX_SOURCE_IMAGE:-}
claude_source_image=${SANDBOX_HARNESS_CLAUDE_SOURCE_IMAGE:-}
if [[ -f "$profile_catalog_path" ]]; then
  [[ -n "$codex_source_image" ]] || codex_source_image=$(jq -r \
    '.runtime_dependency_profiles[]|select(.id=="primary-full-cpu")|.image_refs.codex' \
    "$profile_catalog_path")
  [[ -n "$claude_source_image" ]] || claude_source_image=$(jq -r \
    '.runtime_dependency_profiles[]|select(.id=="primary-full-cpu")|.image_refs.claude' \
    "$profile_catalog_path")
fi
output_root="$root/.data/image-build/$tag"
mkdir -p "$output_root"
[[ "$output_root" == /data2/wujing03/sandbox-harness-platform-parallel/.data/* ]]

content_hash() {
  local parent=$1
  shift
  { printf '%s\n' "$parent"; sha256sum "$@"; } | sha256sum | cut -c1-12
}
mapfile -t bootstrap_sources < <(find "$root/src/sandbox_bootstrap" -type f | sort)
bootstrap_hash=$(content_hash bootstrap-v1 "$root/runtime/bootstrap.Dockerfile" \
  "$root/runtime/requirements-runtime.txt" "$root/runtime/bundle_public_key.pem" \
  "$root/runtime/bootstrap_entrypoint.sh" "$root/runtime/runtime_cli_dispatch.sh" \
  "${bootstrap_sources[@]}")
bootstrap="sandbox-harness-parallel-bootstrap:$bootstrap_hash"
if ! "$docker" image inspect "$bootstrap" >/dev/null 2>&1; then
  "$docker" build --network host -f "$root/runtime/bootstrap.Dockerfile" \
    --build-arg "SANDBOX_USERNAME=$sandbox_username" -t "$bootstrap" "$root"
fi
common_hash=$(content_hash "$bootstrap_hash" "$root/runtime/common.Dockerfile" \
  "$root/runtime/requirements-common.txt" "$root/runtime/proxy_bridge.cjs" \
  "$root/runtime/playwright_proxy_patch.cjs" "$root/runtime/apply_patch.py" \
  "$root/runtime/original_compatibility_probe.py")
common="sandbox-harness-parallel-common:$common_hash"
if ! "$docker" image inspect "$common" >/dev/null 2>&1; then
  "$docker" build --network host -f "$root/runtime/common.Dockerfile" \
    --build-arg "BOOTSTRAP_IMAGE=$bootstrap" -t "$common" "$root"
fi
asr_source_id=$($docker image inspect "$asr_source_image" --format '{{.Id}}')

: >"$output_root/images.tsv"
for profile in $profiles; do
  dockerfile="$root/runtime/profiles/$profile.Dockerfile"
  test -f "$dockerfile"
  case "$profile" in
    general-cpu) requirement="$root/runtime/requirements-common.txt" ;;
    media-cpu) requirement="$root/runtime/requirements-media.txt" ;;
    office) requirement="$root/runtime/requirements-office.txt" ;;
    web-browser) requirement="$root/runtime/requirements-web.txt" ;;
    data-analysis) requirement="$root/runtime/requirements-data.txt" ;;
    primary-full-cpu) requirement="$root/runtime/requirements-runtime.txt" ;;
  esac
  profile_inputs=("$dockerfile" "$requirement")
  if [[ "$profile" == web-browser || "$profile" == primary-full-cpu ]]; then
    profile_inputs+=("$root/runtime/browser_screenshot.mjs" "$root/runtime/browser_render.mjs" \
      "$root/runtime/chromium_proxy.cjs")
  fi
  if [[ "$profile" == primary-full-cpu ]]; then
    test -f "$drawio_vendor/drawio-amd64-32.6.10.deb"
    mkdir -p "$root/runtime/vendor"
    if [[ ! -f "$root/runtime/vendor/drawio-amd64-32.6.10.deb" ]]; then
      ln "$drawio_vendor/drawio-amd64-32.6.10.deb" \
        "$root/runtime/vendor/drawio-amd64-32.6.10.deb"
    fi
    printf '%s  %s\n' fc96faafe68cb4e06c0b7116779891592b20fce2cde7907adec504f23c5af83c \
      "$root/runtime/vendor/drawio-amd64-32.6.10.deb" | sha256sum -c -
    profile_inputs+=("$root/runtime/vendor/drawio-amd64-32.6.10.deb")
    profile_inputs+=("$root/runtime/drawio_wrapper.sh")
  fi
  profile_hash=$(content_hash "$common_hash" "${profile_inputs[@]}")
  profile_image="sandbox-harness-parallel-$profile:$profile_hash"
  if ! "$docker" image inspect "$profile_image" >/dev/null 2>&1; then
    "$docker" build --network host -f "$dockerfile" \
      --build-arg "COMMON_IMAGE=$common" -t "$profile_image" "$root"
  fi
  if [[ "$profile" == primary-full-cpu || "$profile" == media-cpu ]]; then
    asr_hash=$(content_hash "$profile_hash:$asr_source_id" \
      "$root/runtime/asr-overlay.Dockerfile" "$root/runtime/requirements-asr.txt" \
      "$root/runtime/asr_cli.py" "$root/runtime/asr_probe.py")
    asr_image="sandbox-harness-parallel-$profile-asr:$asr_hash"
    if ! "$docker" image inspect "$asr_image" >/dev/null 2>&1; then
      "$docker" build --network host -f "$root/runtime/asr-overlay.Dockerfile" \
        --build-arg "PROFILE_IMAGE=$profile_image" --build-arg "ASR_SOURCE_IMAGE=$asr_source_image" \
        --build-arg "PROFILE_ID=$profile" \
        -t "$asr_image" "$root"
    fi
    profile_image=$asr_image
  fi
  for harness in $harnesses; do
    image="$registry/$profile-$harness:$tag"
    harness_args=(--build-arg "PROFILE_IMAGE=$profile_image")
    if [[ "$harness" == codex && -n "$codex_source_image" ]]; then
      harness_args+=(--build-arg "CODEX_SOURCE_IMAGE=$codex_source_image")
    elif [[ "$harness" == claude && -n "$claude_source_image" ]]; then
      harness_args+=(--build-arg "CLAUDE_SOURCE_IMAGE=$claude_source_image")
    fi
    "$docker" build --network host -f "$root/runtime/$harness.Dockerfile" \
      "${harness_args[@]}" -t "$image" "$root"
    "$docker" push "$image"
    digest=$($docker image inspect "$image" --format '{{index .RepoDigests 0}}')
    test -n "$digest"
    printf '%s\t%s\t%s\n' "$profile" "$harness" "$digest" >>"$output_root/images.tsv"
  done
done
if [[ " $profiles " == " $all_profiles " && " $harnesses " == " $all_harnesses " ]]; then
  "$python" "$root/runtime/build_profile_catalog.py" --matrix "$output_root/images.tsv" \
    --template "$root/deploy/profile-catalog.example.json" \
    --output "$output_root/profile-catalog.json" \
    --signing-key "$root/.data/config/runtime-bundle-signing.pem"
else
  echo "partial development matrix: signed deployment catalog was not generated"
fi
echo "image matrix recorded in $output_root"
