#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_manager.sh — Run this script LOCALLY on the manager node.
#
# Usage:
#   ./scripts/p3/start_manager.sh [config.sh path]
#
# Defaults to scripts/p3/config.sh relative to the madkv repo root.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${1:-${SCRIPT_DIR}/config.sh}"
# shellcheck source=config.sh
source "$CFG"

# Allow caller to override RF / SERVERS via environment variables
# (used by start_all.sh when --rf or --nparts flags are passed)
[[ -n "${OVERRIDE_RF:-}"      ]] && RF="$OVERRIDE_RF"
[[ -n "${OVERRIDE_SERVERS:-}" ]] && SERVERS="$OVERRIDE_SERVERS"

REPLICA_ID=0   # single manager for baseline; increment if adding manager replicas

BACKER="$(get_backer "m.${REPLICA_ID}")"
LOG="${LOG_DIR}/manager-${REPLICA_ID}.log"

mkdir -p "$LOG_DIR" "$BACKER"

echo "[$(date '+%H:%M:%S')] Starting manager m.${REPLICA_ID}"
echo "  man_port   = ${MAN_PORT}"
echo "  p2p_port   = ${MGR_P2P_PORT}"
echo "  peer_addrs = none  (single manager)"
echo "  server_rf  = ${RF}"
echo "  servers    = ${SERVERS}"
echo "  backer     = ${BACKER}"
echo "  log        = ${LOG}"

nohup "${MADKV}/kvstore/bin/kvmanager" \
    --replica_id "${REPLICA_ID}" \
    --man_port   "${MAN_PORT}" \
    --p2p_port   "${MGR_P2P_PORT}" \
    --peer_addrs none \
    --server_rf  "${RF}" \
    --server_addrs "${SERVERS}" \
    --backer_path "${BACKER}" \
    > "${LOG}" 2>&1 &

echo "[$(date '+%H:%M:%S')] Manager started (pid=$!), log: ${LOG}"
