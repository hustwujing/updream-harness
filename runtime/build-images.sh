#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
[[ "$root" == /data2/wujing03/sandbox-harness-platform-parallel ]]
docker=/data2/wujing03/opensandbox-stack/tools/docker-29.9.3/docker
export DOCKER_HOST=unix:///run/docker.sock
git -C "$root" diff --quiet
git -C "$root" diff --cached --quiet
python3 "$root/runtime/skill_bundle.py" verify

registry=${SANDBOX_HARNESS_IMAGE_REGISTRY:-127.0.0.1:25001/sandbox-harness-parallel}
tag=$(git -C "$root" rev-parse --short=12 HEAD)
sandbox_username=${SANDBOX_HARNESS_SANDBOX_USERNAME:-updream}
profiles=${BUILD_PROFILES:-"primary-full-cpu general-cpu media-cpu office web-browser data-analysis"}
harnesses=${BUILD_HARNESSES:-"codex claude deepseek"}
all_profiles="primary-full-cpu general-cpu media-cpu office web-browser data-analysis"
all_harnesses="codex claude deepseek"
drawio_vendor=${SANDBOX_HARNESS_DRAWIO_VENDOR:-/data2/wujing03/sandbox-harness-platform/runtime/vendor}
output_root="$root/.data/image-build/$tag"
mkdir -p "$output_root"
[[ "$output_root" == /data2/wujing03/sandbox-harness-platform-parallel/.data/* ]]

bootstrap_hash=$(sha256sum "$root/runtime/bootstrap.Dockerfile" | cut -c1-12)
bootstrap="sandbox-harness-parallel-bootstrap:$bootstrap_hash"
if ! "$docker" image inspect "$bootstrap" >/dev/null 2>&1; then
  "$docker" build --network host -f "$root/runtime/bootstrap.Dockerfile" \
    --build-arg "SANDBOX_USERNAME=$sandbox_username" -t "$bootstrap" "$root"
fi
common_hash=$(sha256sum "$root/runtime/common.Dockerfile" "$root/runtime/requirements-common.txt" | sha256sum | cut -c1-12)
common="sandbox-harness-parallel-common:$common_hash"
if ! "$docker" image inspect "$common" >/dev/null 2>&1; then
  "$docker" build --network host -f "$root/runtime/common.Dockerfile" \
    --build-arg "BOOTSTRAP_IMAGE=$bootstrap" -t "$common" "$root"
fi

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
  profile_hash=$(sha256sum "$dockerfile" "$requirement" | sha256sum | cut -c1-12)
  profile_image="sandbox-harness-parallel-$profile:$profile_hash"
  if ! "$docker" image inspect "$profile_image" >/dev/null 2>&1; then
    if [[ "$profile" == primary-full-cpu ]]; then
      test -f "$drawio_vendor/drawio-amd64-32.6.10.deb"
      mkdir -p "$root/runtime/vendor"
      if [[ ! -f "$root/runtime/vendor/drawio-amd64-32.6.10.deb" ]]; then
        ln "$drawio_vendor/drawio-amd64-32.6.10.deb" \
          "$root/runtime/vendor/drawio-amd64-32.6.10.deb"
      fi
      printf '%s  %s\n' fc96faafe68cb4e06c0b7116779891592b20fce2cde7907adec504f23c5af83c \
        "$root/runtime/vendor/drawio-amd64-32.6.10.deb" | sha256sum -c -
    fi
    "$docker" build --network host -f "$dockerfile" \
      --build-arg "COMMON_IMAGE=$common" -t "$profile_image" "$root"
  fi
  for harness in $harnesses; do
    image="$registry/$profile-$harness:$tag"
    "$docker" build --network host -f "$root/runtime/$harness.Dockerfile" \
      --build-arg "PROFILE_IMAGE=$profile_image" -t "$image" "$root"
    "$docker" push "$image"
    digest=$($docker image inspect "$image" --format '{{index .RepoDigests 0}}')
    test -n "$digest"
    printf '%s\t%s\t%s\n' "$profile" "$harness" "$digest" >>"$output_root/images.tsv"
  done
done
if [[ " $profiles " == " $all_profiles " && " $harnesses " == " $all_harnesses " ]]; then
  python3 "$root/runtime/build_profile_catalog.py" --matrix "$output_root/images.tsv" \
    --template "$root/deploy/profile-catalog.example.json" \
    --output "$output_root/profile-catalog.json" \
    --signing-key "$root/.data/config/runtime-bundle-signing.pem"
else
  echo "partial development matrix: signed deployment catalog was not generated"
fi
echo "image matrix recorded in $output_root"
