#!/usr/bin/env bash
set -euo pipefail

action=${1:-}
chain=INPUT
iptables=/usr/sbin/iptables-nft
docker=/data2/wujing03/opensandbox-stack/tools/docker-29.9.3/docker
read -r network_id source_cidr gateway internal < <(
  DOCKER_HOST=unix:///run/docker.sock "$docker" network inspect sandbox-agent-net \
    --format '{{.Id}} {{(index .IPAM.Config 0).Subnet}} {{(index .IPAM.Config 0).Gateway}} {{.Internal}}'
)
if [[ ! "$network_id" =~ ^[0-9a-f]{64}$ || "$source_cidr" != 172.31.0.0/24 || \
      "$gateway" != 172.31.0.1 || "$internal" != true ]]; then
  echo "unexpected shared OpenSandbox network identity" >&2
  exit 1
fi
bridge="br-${network_id:0:12}"
port=29882:29883
comment=sandbox-harness-parallel-ali-gateway
rule=(-i "$bridge" -s "$source_cidr" -d "$gateway" -p tcp -m multiport \
  --dports "$port" -m comment --comment "$comment" -j ACCEPT)

case "$action" in
  validate) echo "$bridge $source_cidr $gateway:$port" ;;
  install)
    if ! "$iptables" -C "$chain" "${rule[@]}" 2>/dev/null; then
      drop_line=$("$iptables" -L "$chain" --line-numbers -n | \
        awk '$0 ~ /sandbox-agent-native/ && $2 == "DROP" {print $1; exit}')
      [[ -n "$drop_line" ]] || { echo "sandbox bridge deny rule is missing" >&2; exit 1; }
      "$iptables" -I "$chain" "$drop_line" "${rule[@]}"
    fi
    ;;
  remove) "$iptables" -D "$chain" "${rule[@]}" 2>/dev/null || true ;;
  *) echo "usage: $0 validate|install|remove" >&2; exit 2 ;;
esac

