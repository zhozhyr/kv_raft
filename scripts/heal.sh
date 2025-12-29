#!/usr/bin/env bash
set -euo pipefail

# Снять сетевые деградации и разделения, созданные netem.sh/partition.sh.
# В контейнерах проекта безопасно очищать INPUT/OUTPUT, потому что дополнительных
# правил там нет.

DC=${DOCKER_COMPOSE:-"docker compose"}

echo "[heal] removing network impairments (tc/iptables)"

ALL_NODES=($($DC ps --services | grep '^node' || true))

if [[ ${#ALL_NODES[@]} -eq 0 ]]; then
  echo "[WARN] no node services found via '$DC ps --services'"
  exit 0
fi

echo "[INFO] nodes from compose: {${ALL_NODES[*]}}"

alive=()
skipped=()

for n in "${ALL_NODES[@]}"; do
  # проверяем, что контейнер реально запущен
  if ! docker ps --format '{{.Names}}' | grep -qx "$n"; then
    echo "[SKIP] $n is not running"
    skipped+=("$n")
    continue
  fi

  alive+=("$n")
  echo "[NODE] $n"

  # tc netem
  echo "  [TC]   delete qdisc root on eth0"
  $DC exec -T "$n" sh -lc 'tc qdisc del dev eth0 root 2>/dev/null || true'

  # iptables (очищаем цепочки)
  echo "  [IPT]  flush INPUT/OUTPUT"
  $DC exec -T "$n" sh -lc 'iptables -F INPUT  2>/dev/null || true; iptables -F OUTPUT 2>/dev/null || true'

  echo "  [OK]   cleaned"
done

echo "[DONE] heal finished"
echo "  cleaned : {${alive[*]:-}}"
echo "  skipped : {${skipped[*]:-}}"
