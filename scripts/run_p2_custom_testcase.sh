#!/usr/bin/env bash
set -euo pipefail

# Deterministic Project 2 custom testcase runner.
#
# It validates the required flow:
# 1) launch 3 partitions
# 2) run PUT/SWAP
# 3) run successful GET/SCAN
# 4) kill one server
# 5) run GET/SCAN on unaffected partition
# 6) run GET on failed partition and confirm timeout
# 7) restart failed server
# 8) GET from recovered partition and confirm latest value
#
# Usage:
#   ./scripts/run_p2_custom_testcase.sh [manager_ip]
#
# Example:
#   ./scripts/run_p2_custom_testcase.sh 10.10.1.2

INPUT_MANAGER_IP="${1:-127.0.0.1}"
CLUSTER_IP="127.0.0.1"
MANAGER_PORT=3666
BASE_PORT=3777
MANAGER_ADDR="${CLUSTER_IP}:${MANAGER_PORT}"
RUN_DIR="/tmp/madkv-p2/custom-testcase"

cd "$(dirname "$0")/.."

if [[ "${INPUT_MANAGER_IP}" != "127.0.0.1" && "${INPUT_MANAGER_IP}" != "localhost" ]]; then
  echo "==> Note: custom testcase runs a local single-host cluster."
  echo "==> Ignoring manager_ip=${INPUT_MANAGER_IP}; using ${CLUSTER_IP}."
fi

mkdir -p "${RUN_DIR}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_cmd python3

TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_BIN="gtimeout"
else
  echo "Missing timeout utility (install coreutils for gtimeout)."
  exit 1
fi

get_key_for_partition() {
  local part="$1"
  python3 - <<'PY' "$part"
import sys
part = int(sys.argv[1])
def fnv1a64(s: str) -> int:
    h = 1469598103934665603
    for b in s.encode("ascii"):
        h ^= b
        h = (h * 1099511628211) & ((1 << 64) - 1)
    return h
for i in range(200000):
    k = f"k{i}"
    if fnv1a64(k) % 3 == part:
        print(k)
        break
PY
}

cleanup() {
  just p2::kill >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> Building binaries"
just p2::build >/tmp/madkv-p2-custom-build.log 2>&1

echo "==> Starting cluster manager + 3 servers"
cleanup
rm -rf "${RUN_DIR}/backer.s0" "${RUN_DIR}/backer.s1" "${RUN_DIR}/backer.s2"
mkdir -p "${RUN_DIR}/backer.s0" "${RUN_DIR}/backer.s1" "${RUN_DIR}/backer.s2"

./kvstore/bin/kvmanager "0.0.0.0:${MANAGER_PORT}" \
  "${CLUSTER_IP}:3777,${CLUSTER_IP}:3778,${CLUSTER_IP}:3779" \
  >"${RUN_DIR}/manager.log" 2>&1 &
M_PID=$!

./kvstore/bin/kvserver 0 "${MANAGER_ADDR}" 3777 "${RUN_DIR}/backer.s0" \
  >"${RUN_DIR}/s0.log" 2>&1 &
S0_PID=$!
./kvstore/bin/kvserver 1 "${MANAGER_ADDR}" 3778 "${RUN_DIR}/backer.s1" \
  >"${RUN_DIR}/s1.log" 2>&1 &
S1_PID=$!
./kvstore/bin/kvserver 2 "${MANAGER_ADDR}" 3779 "${RUN_DIR}/backer.s2" \
  >"${RUN_DIR}/s2.log" 2>&1 &
S2_PID=$!

sleep 2

K0="$(get_key_for_partition 0)"
K1="$(get_key_for_partition 1)"
K2="$(get_key_for_partition 2)"

echo "==> Keys selected by partition"
echo "partition0 key: ${K0}"
echo "partition1 key: ${K1}"
echo "partition2 key: ${K2}"

echo "==> Step 2/3: PUT/SWAP and successful GET/SCAN"
cat <<EOF | ./kvstore/bin/kvclient --manager "${MANAGER_ADDR}" | tee "${RUN_DIR}/step2_3.log"
PUT ${K0} v0
PUT ${K1} v1
PUT ${K2} v2
SWAP ${K1} v1_new
GET ${K1}
SCAN ${K0} ${K2}
STOP
EOF

echo "==> Step 4: kill server for partition 1 (s1)"
# Use SIGKILL for deterministic failure behavior in automated runs.
kill -9 "${S1_PID}" >/dev/null 2>&1 || true
wait "${S1_PID}" 2>/dev/null || true
if kill -0 "${S1_PID}" >/dev/null 2>&1; then
  echo "Failed to stop server process for partition 1 (pid=${S1_PID})."
  exit 1
fi

echo "==> Step 5: GET/SCAN on unaffected partition (s0) should succeed"
cat <<EOF | ./kvstore/bin/kvclient "${CLUSTER_IP}:3777" | tee "${RUN_DIR}/step5_unaffected.log"
GET ${K0}
SCAN ${K0} ${K0}
STOP
EOF

echo "==> Step 6: GET on failed partition should timeout"
set +e
cat <<EOF | "${TIMEOUT_BIN}" 6s ./kvstore/bin/kvclient "${CLUSTER_IP}:3778" \
  >"${RUN_DIR}/step6_failed_partition.log" 2>&1
GET ${K1}
STOP
EOF
STEP6_RC=$?
set -e

if [[ "${STEP6_RC}" -ne 124 ]]; then
  echo "Expected timeout (124) for failed partition GET, got ${STEP6_RC}"
  echo "See ${RUN_DIR}/step6_failed_partition.log"
  echo "Recent server logs:"
  tail -n 20 "${RUN_DIR}/s1.log" || true
  exit 1
fi
echo "Observed expected timeout while partition server was down."

echo "==> Step 7: restart failed partition server"
./kvstore/bin/kvserver 1 "${MANAGER_ADDR}" 3778 "${RUN_DIR}/backer.s1" \
  >"${RUN_DIR}/s1-restart.log" 2>&1 &
S1_PID=$!
sleep 2

echo "==> Step 8: GET after recovery should return latest value"
cat <<EOF | ./kvstore/bin/kvclient "${CLUSTER_IP}:3778" | tee "${RUN_DIR}/step8_recovered.log"
GET ${K1}
STOP
EOF

if ! grep -q "GET ${K1} v1_new" "${RUN_DIR}/step8_recovered.log"; then
  echo "Recovery check failed: expected 'GET ${K1} v1_new'"
  exit 1
fi

echo
echo "Custom Project 2 testcase PASSED."
echo "Logs saved to: ${RUN_DIR}"
