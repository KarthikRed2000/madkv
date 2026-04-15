#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup_nodes.sh — One-time setup: install deps + clone repo on every node.
#
# Run this ONCE from node0 (pc297) after your CloudLab experiment is ready.
#
# What it does on each node:
#   1. Installs system dependencies (gRPC, protobuf, rocksdb, cmake, rust).
#   2. Clones the madkv repo (or pulls latest if already present).
#   3. Builds the project.
#
# Usage:
#   bash scripts/p3/setup_nodes.sh [--repo <git-url>] [--branch <branch>]
#
# Defaults:
#   --repo   : inferred from local git remote "origin"
#   --branch : current branch
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# ── Parse args ────────────────────────────────────────────────────────────────
REPO=""
BRANCH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)   REPO="$2";   shift 2 ;;
    --branch) BRANCH="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# Try to infer repo and branch from local git
if [[ -z "$REPO" ]]; then
  REPO=$(git -C "${SCRIPT_DIR}" remote get-url origin 2>/dev/null || echo "")
fi
if [[ -z "$BRANCH" ]]; then
  BRANCH=$(git -C "${SCRIPT_DIR}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "main")
fi

if [[ -z "$REPO" ]]; then
  echo "ERROR: could not determine git repo URL. Pass --repo <url>." >&2
  exit 1
fi

echo "Repo  : ${REPO}"
echo "Branch: ${BRANCH}"
echo "Target: ${#NODE_HOSTS[@]} nodes"
echo ""

# ── Remote setup script (heredoc, runs as bash on each node) ─────────────────
REMOTE_SETUP=$(cat <<'REMOTE'
set -euo pipefail
REPO="$1"
BRANCH="$2"
MADKV="$HOME/madkv"

# Remove existing repo if it exists
if [[ -d "${MADKV}" ]]; then
  echo "[$(hostname)] Removing existing repo at ${MADKV}..."
  rm -rf "${MADKV}"
fi

# Clone the repo
echo "[$(hostname)] Cloning ${REPO} (${BRANCH})..."
git clone --quiet --branch "${BRANCH}" "${REPO}" "${MADKV}"

# Build the project
echo "[$(hostname)] Building madkv..."
source "$HOME/.cargo/env"
cd "${MADKV}"
just p3::build 2>&1 | tail -10

echo "[$(hostname)] Setup DONE."
REMOTE
)

ssh_cmd() {
  local host="$1"; shift
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=30 "$host" \
      "bash -s -- '${REPO}' '${BRANCH}'" <<< "$REMOTE_SETUP"
}

# ── Run setup in parallel on all nodes ───────────────────────────────────────
echo "Starting parallel setup on all ${#NODE_HOSTS[@]} nodes..."
pids=()
for host in "${NODE_HOSTS[@]}"; do
  logfile="/tmp/setup-${host##*@}.log"
  (
    ssh_cmd "$host" > "$logfile" 2>&1
    echo "[OK]  ${host}  →  ${logfile}"
  ) &
  pids+=($!)
done

failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || { echo "[FAILED] pid=${pid}"; failed=1; }
done

echo ""
if [[ $failed -eq 0 ]]; then
  echo "All nodes set up successfully."
else
  echo "Some nodes failed. Check /tmp/setup-*.log files."
  exit 1
fi
