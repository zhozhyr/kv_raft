#!/usr/bin/env bash
set -euo pipefail

# Применить/снять деградацию сети внутри контейнера через tc netem.
#
# Примеры:
#   ./scripts/netem.sh apply node2 --loss 30 --delay 200 --jitter 50
#   ./scripts/netem.sh clear node2

MODE=${1:-}
NODE=${2:-}
shift 2 || true

if [[ -z "${MODE}" || -z "${NODE}" ]]; then
  echo "usage: $0 <apply|clear> <nodeX> [--loss PCT] [--delay MS] [--jitter MS] [--dup PCT] [--reorder PCT]" >&2
  exit 2
fi

LOSS=0
DELAY=0
JITTER=0
DUP=0
REORDER=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --loss) LOSS="$2"; shift 2;;
    --delay) DELAY="$2"; shift 2;;
    --jitter) JITTER="$2"; shift 2;;
    --dup) DUP="$2"; shift 2;;
    --reorder) REORDER="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

DC=${DOCKER_COMPOSE:-"docker compose"}

if [[ "$MODE" == "clear" ]]; then
  $DC exec -T "$NODE" sh -lc 'tc qdisc del dev eth0 root 2>/dev/null || true'
  exit 0
fi

if [[ "$MODE" != "apply" ]]; then
  echo "unknown mode: $MODE" >&2
  exit 2
fi

ARGS=("netem")
if [[ "$DELAY" != "0" ]]; then
  if [[ "$JITTER" != "0" ]]; then
    ARGS+=("delay" "${DELAY}ms" "${JITTER}ms")
  else
    ARGS+=("delay" "${DELAY}ms")
  fi
fi
if [[ "$LOSS" != "0" ]]; then
  ARGS+=("loss" "${LOSS}%")
fi
if [[ "$DUP" != "0" ]]; then
  ARGS+=("duplicate" "${DUP}%")
fi
if [[ "$REORDER" != "0" ]]; then
  ARGS+=("reorder" "${REORDER}%")
fi

CMD="tc qdisc replace dev eth0 root ${ARGS[*]}"
$DC exec -T "$NODE" sh -lc "$CMD"
