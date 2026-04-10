#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# status.sh — Show what is running on every node and tail recent log lines.
#
# Usage:  ./scripts/p3/status.sh [--tail N]
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${SCRIPT_DIR}/config.sh"
source "$CFG"

TAIL_N=5
[[ "${1:-}" == "--tail" ]] && TAIL_N="${2:-5}"

ssh_q() {
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$1" "${@:2}" 2>/dev/null || echo "(ssh failed)"
}

check_node() {
  local label="$1"
  local host="$2"
  local log_glob="$3"

  echo "────────────────────────────────────────"
  echo "  ${label}  (${host})"

  procs=$(ssh_q "$host" "pgrep -la kvmanager kvserver kvclient 2>/dev/null || echo '(none running)'")
  echo "  Processes: ${procs}"

  if [[ -n "$log_glob" ]]; then
    recent=$(ssh_q "$host" "ls -t ${log_glob} 2>/dev/null | head -1")
    if [[ -n "$recent" && "$recent" != "(ssh failed)" ]]; then
      echo "  Log (${recent}, last ${TAIL_N} lines):"
      ssh_q "$host" "tail -n ${TAIL_N} ${recent} 2>/dev/null" | sed 's/^/    /'
    fi
  fi
}

echo "════════════════════════════════════════"
echo "  P3 Cluster Status  (NPARTS=${NPARTS}  RF=${RF})"
echo "════════════════════════════════════════"

check_node "m.0  (manager)" "${NODE_HOSTS[1]}" "/tmp/madkv-p3/logs/manager-*.log"

for ((p=0; p < NPARTS; p++)); do
  for ((r=0; r < RF; r++)); do
    idx=$((p * RF + r))
    check_node "s${p}.${r} (part=${p} rep=${r})" \
        "${NODE_HOSTS[$((idx+2))]}" \
        "/tmp/madkv-p3/logs/server-s${p}.${r}.log"
  done
done

echo "════════════════════════════════════════"
