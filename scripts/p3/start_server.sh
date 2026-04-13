#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_server.sh — Run this script LOCALLY on the server node.
#
# Usage:
#   ./scripts/p3/start_server.sh <PART_ID> <REP_ID> [config.sh path]
#
# Example (partition 0, replica 1):
#   ./scripts/p3/start_server.sh 0 1
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <PART_ID> <REP_ID> [config.sh path]" >&2
  exit 1
fi

PART_ID="$1"
REP_ID="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${3:-${SCRIPT_DIR}/config.sh}"
# shellcheck source=config.sh
source "$CFG"

# Allow caller to override RF via environment variable so that port/peer
# calculations match what start_all.sh computed with a non-default RF.
if [[ -n "${OVERRIDE_RF:-}" ]]; then
  RF="$OVERRIDE_RF"
  # Recompute SERVER_ADDRS with the new RF
  SERVER_ADDRS=()
  for ((i=0; i < NPARTS*RF; i++)); do
    SERVER_ADDRS[$i]="${NODE_ADDRS[$((i+2))]}"
  done
fi

TAG="s${PART_ID}.${REP_ID}"
API_PORT="$(get_server_api_port "$PART_ID" "$REP_ID")"
P2P_PORT="$(get_server_p2p_port "$PART_ID" "$REP_ID")"
PEERS="$(get_server_peers "$PART_ID" "$REP_ID")"
BACKER="$(get_backer "${TAG}")"
LOG="${LOG_DIR}/server-${TAG}.log"

mkdir -p "$LOG_DIR" "$BACKER"

echo "[$(date '+%H:%M:%S')] Starting server ${TAG}"
echo "  partition  = ${PART_ID}"
echo "  replica    = ${REP_ID}"
echo "  managers   = ${MANAGERS}"
echo "  api_port   = ${API_PORT}"
echo "  p2p_port   = ${P2P_PORT}"
echo "  peer_addrs = ${PEERS}"
echo "  backer     = ${BACKER}"
echo "  log        = ${LOG}"

nohup "${MADKV}/kvstore/bin/kvserver" \
    --partition_id "${PART_ID}" \
    --replica_id   "${REP_ID}" \
    --manager_addrs "${MANAGERS}" \
    --api_port     "${API_PORT}" \
    --p2p_port     "${P2P_PORT}" \
    --peer_addrs   "${PEERS}" \
    --backer_path  "${BACKER}" \
    > "${LOG}" 2>&1 &

echo "[$(date '+%H:%M:%S')] Server ${TAG} started (pid=$!), log: ${LOG}"
