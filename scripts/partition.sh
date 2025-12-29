#!/usr/bin/env bash
set -euo pipefail

# Сетевые разделения (partition) и blackhole через iptables внутри контейнеров.
#
# Примеры:
#   ./scripts/partition.sh split node1 node2         # split: {node1,node2} | {node3}
#
# Снятие правил:
#   ./scripts/heal.sh

DC=${DOCKER_COMPOSE:-"docker compose"}

mode=${1:-}
if [[ -z "$mode" ]]; then
  echo "[ERROR] usage: $0 <minority|isolate|split> ..." >&2
  exit 2
fi

echo "[partition] mode: $mode"


_ip() {
  local node=$1
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$node"
}

_drop_pair() {
  local a=$1
  local b=$2
  local ipb

  ipb=$(_ip "$b")

  echo "  [DROP] $a  <->  $b  (ip: $ipb)"

  $DC exec -T "$a" sh -lc \
    "iptables -I OUTPUT -d $ipb -j DROP || true;
     iptables -I INPUT  -s $ipb -j DROP || true"
}

print_ips() {
  echo "[INFO] container IPs:"
  for n in $($DC ps --services | grep '^node'); do
    echo "  - $n : $(_ip "$n")"
  done
}

ALL_NODES=($($DC ps --services | grep '^node'))

print_ips

if [[ "$mode" == "split" ]]; then
  a=${2:-}
  b=${3:-}
  [[ -z "$a" || -z "$b" ]] && { echo "[ERROR] usage: $0 split <nodeA> <nodeB>" >&2; exit 2; }

  GROUP_A=("$a" "$b")
  GROUP_B=()

  for n in "${ALL_NODES[@]}"; do
    if [[ "$n" != "$a" && "$n" != "$b" ]]; then
      GROUP_B+=("$n")
    fi
  done

  echo "[ACTION] split cluster into two partitions"
  echo "  group A: {${GROUP_A[*]}}"
  echo "  group B: {${GROUP_B[*]}}"

  for left in "${GROUP_A[@]}"; do
    for right in "${GROUP_B[@]}"; do
      _drop_pair "$left" "$right"
      _drop_pair "$right" "$left"
    done
  done

  echo "[DONE] network partition applied between group A and group B"
  exit 0
fi

echo "[ERROR] unknown mode: $mode" >&2
exit 2
