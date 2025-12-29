#!/usr/bin/env bash
set -euo pipefail

# Сетевые разделения (partition) и blackhole через iptables внутри контейнеров.
#
# Примеры:
#   ./scripts/partition.sh minority node1            # node1 изолирован, node2+node3 общаются
#   ./scripts/partition.sh isolate node2             # node2 не видит никого и никто не видит node2
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

if [[ "$mode" == "isolate" ]]; then
  node=${2:-}
  [[ -z "$node" ]] && { echo "[ERROR] usage: $0 isolate <nodeX>" >&2; exit 2; }

  echo "[ACTION] isolate single node"
  echo "  isolated node : {$node}"
  echo "  rest of cluster: {${ALL_NODES[*]}}"

  for other in "${ALL_NODES[@]}"; do
    [[ "$other" == "$node" ]] && continue
    _drop_pair "$node" "$other"
    _drop_pair "$other" "$node"
  done

  echo "[DONE] node '$node' is fully isolated from the cluster"
  exit 0
fi

if [[ "$mode" == "minority" ]]; then
  leader=${2:-}
  [[ -z "$leader" ]] && { echo "[ERROR] usage: $0 minority <nodeX>" >&2; exit 2; }

  GROUP_A=("$leader")
  GROUP_B=()

  for n in "${ALL_NODES[@]}"; do
    [[ "$n" != "$leader" ]] && GROUP_B+=("$n")
  done

  echo "[ACTION] force minority"
  echo "  minority group : {${GROUP_A[*]}}"
  echo "  majority group : {${GROUP_B[*]}}"

  for other in "${GROUP_B[@]}"; do
    _drop_pair "$leader" "$other"
    _drop_pair "$other" "$leader"
  done

  echo "[DONE] node '$leader' is now isolated in minority"
  exit 0
fi

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
