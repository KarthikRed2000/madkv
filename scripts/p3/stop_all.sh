#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# stop_all.sh — Kill all P3 processes on every node in the cluster.
#
# Run from node0 (or any node with SSH access to the others).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFG="${SCRIPT_DIR}/config.sh"
source "$CFG"

log() { echo "[$(date '+%H:%M:%S')] $1"; }

kill_remote() {
  local host="$1"
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$host" \
      "pkill -9 kvmanager 2>/dev/null; pkill -9 kvserver 2>/dev/null; pkill -9 kvclient 2>/dev/null; true" \
    2>/dev/null || true
}

log "Killing processes on all nodes..."

# Manager node
kill_remote "${NODE_HOSTS[1]}" &

# Server nodes
for ((i=0; i < NPARTS*RF; i++)); do
  kill_remote "${NODE_HOSTS[$((i+2))]}" &
done

# Also kill locally (in case this machine runs something)
pkill -9 kvmanager 2>/dev/null || true
pkill -9 kvserver  2>/dev/null || true
pkill -9 kvclient  2>/dev/null || true

wait
log "All P3 processes stopped."
