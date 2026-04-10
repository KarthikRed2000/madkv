#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# discover_ips.sh — Print the private experiment-network IP of every node.
#
# Run this ONCE from your local machine (or node0) after the experiment starts.
# Copy the printed NODE_ADDRS block into config.sh if you want to use private
# IPs instead of public hostnames (better throughput on Emulab).
#
# Usage:
#   bash scripts/p3/discover_ips.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# Emulab experiment interface is usually eth1 or enp1s0d1.
# We look for the first non-loopback, non-control address in 10.x.x.x range.
GET_IP='ip -4 addr show | grep -oP "(?<=inet )10\.[0-9.]+(?=/)" | head -1'

echo "Discovering private IPs for ${#NODE_HOSTS[@]} nodes (runs in parallel)..."
echo ""

declare -a FOUND_IPS
pids=()
tmpdir=$(mktemp -d)

for ((i=0; i<${#NODE_HOSTS[@]}; i++)); do
  host="${NODE_HOSTS[$i]}"
  (
    ip=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
             -o BatchMode=yes "$host" "$GET_IP" 2>/dev/null || echo "UNKNOWN")
    echo "$i $ip" > "${tmpdir}/node${i}.txt"
  ) &
  pids+=($!)
done

for pid in "${pids[@]}"; do wait "$pid" || true; done

echo "NODE_ADDRS=("
for ((i=0; i<${#NODE_HOSTS[@]}; i++)); do
  ip="UNKNOWN"
  [[ -f "${tmpdir}/node${i}.txt" ]] && ip=$(awk '{print $2}' "${tmpdir}/node${i}.txt")
  label="${NODE_HOSTS[$i]##*@}"   # strip user@ prefix
  printf '  "%-20s"  # node%-2d  %s\n' "$ip" "$i" "$label"
done
echo ")"

rm -rf "$tmpdir"
echo ""
echo "Paste the NODE_ADDRS block above into config.sh to use private IPs."
