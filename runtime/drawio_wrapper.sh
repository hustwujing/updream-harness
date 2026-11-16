#!/usr/bin/env bash
set -euo pipefail

export XDG_CONFIG_HOME=${XDG_CONFIG_HOME:-${HOME:-/workspace/state/home}/.config}
export XDG_CACHE_HOME=${XDG_CACHE_HOME:-/workspace/tmp/cache}
mkdir -p "$XDG_CONFIG_HOME" "$XDG_CACHE_HOME"
display_number=$((100 + ($$ % 50000)))
display=:$display_number
log=${TMPDIR:-/workspace/tmp}/drawio-xvfb-$$.log
Xvfb "$display" -screen 0 1280x1024x24 -nolisten tcp -ac >"$log" 2>&1 &
xvfb_pid=$!
cleanup() {
  kill "$xvfb_pid" 2>/dev/null || true
  wait "$xvfb_pid" 2>/dev/null || true
  rm -f "$log"
}
trap cleanup EXIT INT TERM
for _ in $(seq 1 100); do
  [[ -S "/tmp/.X11-unix/X$display_number" ]] && break
  kill -0 "$xvfb_pid" 2>/dev/null || { cat "$log" >&2; exit 1; }
  sleep 0.02
done
[[ -S "/tmp/.X11-unix/X$display_number" ]] || { cat "$log" >&2; exit 1; }
DISPLAY="$display" /usr/bin/drawio --no-sandbox "$@"
