#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_manager_replica.sh — Run LOCALLY on a manager node.
#
# Usage:
#   bash start_manager_replica.sh REP_ID [config.sh path]
#
# REP_ID:  0, 1, or 2  (manager replica index)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REP_ID="${1:?Usage: $0 REP_ID [config.sh path]}"
CFG="${2:-${SCRIPT_DIR}/config.sh}"
# shellcheck source=config.sh
source "$CFG"

# Allow caller to override RF / SERVERS via environment variables
[[ -n "${OVERRIDE_RF:-}"      ]] && RF="$OVERRIDE_RF"
[[ -n "${OVERRIDE_SERVERS:-}" ]] && SERVERS="$OVERRIDE_SERVERS"

MAN_LISTEN_PORT=$((MAN_PORT + REP_ID))
P2P_LISTEN_PORT=$((MGR_P2P_PORT + REP_ID))

BACKER="$(get_backer "m.${REP_ID}")"
LOG="${LOG_DIR}/manager-${REP_ID}.log"

mkdir -p "$LOG_DIR" "$BACKER"

# Build peer P2P addresses (all other manager replicas, sorted by id, excl self)
peers=""
for ((i=0; i<MGR_RF; i++)); do
  [[ $i -eq REP_ID ]] && continue
  [[ -n "$peers" ]] && peers+=","
  peers+="${MGR_ADDRS[$i]}:$((MGR_P2P_PORT + i))"
done
[[ -z "$peers" ]] && peers="none"

echo "[$(date '+%H:%M:%S')] Starting manager replica m.${REP_ID}"
echo "  man_port   = ${MAN_LISTEN_PORT}"
echo "  p2p_port   = ${P2P_LISTEN_PORT}"
echo "  peer_addrs = ${peers}"
echo "  server_rf  = ${RF}"
echo "  servers    = ${SERVERS}"
echo "  backer     = ${BACKER}"
echo "  log        = ${LOG}"

nohup "${MADKV}/kvstore/bin/kvmanager" \
    --replica_id   "${REP_ID}" \
    --man_port     "${MAN_LISTEN_PORT}" \
    --p2p_port     "${P2P_LISTEN_PORT}" \
    --peer_addrs   "${peers}" \
    --server_rf    "${RF}" \
    --server_addrs "${SERVERS}" \
    --backer_path  "${BACKER}" \
    > "${LOG}" 2>&1 &

echo "[$(date '+%H:%M:%S')] Manager m.${REP_ID} started (pid=$!), log: ${LOG}"
