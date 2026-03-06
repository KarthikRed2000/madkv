#!/usr/bin/env bash
set -euo pipefail

# Run full required Project 2 matrix on one host:
# - Fuzz: (3,no), (3,yes), (5,yes)
# - Bench: 10 clients x workloads a-f x partitions 1/3/5
# - Bench scaling: workload a, clients 1/20/30 with partitions 1/5
#
# Usage:
#   ./scripts/run_p2_required.sh [manager_ip]
#
# Example:
#   ./scripts/run_p2_required.sh 10.10.1.2

MANAGER_IP="${1:-127.0.0.1}"
MANAGER_PORT=3666
BASE_PORT=3777
MANAGER_ADDR="${MANAGER_IP}:${MANAGER_PORT}"
RESULT_DIR="/tmp/madkv-p2"
RUN_DIR="/tmp/madkv-p2-runs"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_cmd just
require_cmd cargo

cd "$(dirname "$0")/.."

echo "==> Preparing environment"
just p2::kill >/dev/null 2>&1 || true
rm -rf "${RESULT_DIR}" "${RUN_DIR}"
mkdir -p "${RESULT_DIR}/fuzz" "${RESULT_DIR}/bench" "${RUN_DIR}"

echo "==> Building binaries"
just p2::build >/tmp/madkv-p2-build.log 2>&1
just utils::build >/tmp/madkv-p2-utils-build.log 2>&1
just utils::ycsb >/tmp/madkv-p2-ycsb.log 2>&1

gen_servers_csv() {
  local n="$1"
  local out=""
  local i
  for ((i = 0; i < n; i++)); do
    local addr="${MANAGER_IP}:$((BASE_PORT + i))"
    if [[ -z "${out}" ]]; then
      out="${addr}"
    else
      out="${out},${addr}"
    fi
  done
  printf "%s" "${out}"
}

MPID=""
SPIDS=()

start_cluster() {
  local n="$1"
  local tag="$2"
  local csv
  csv="$(gen_servers_csv "${n}")"
  mkdir -p "${RUN_DIR}/${tag}"

  ./kvstore/bin/kvmanager "0.0.0.0:${MANAGER_PORT}" "${csv}" >"${RUN_DIR}/${tag}/manager.log" 2>&1 &
  MPID=$!
  SPIDS=()

  local i
  for ((i = 0; i < n; i++)); do
    local backer="${RUN_DIR}/${tag}/backer.s${i}"
    mkdir -p "${backer}"
    ./kvstore/bin/kvserver "${i}" "${MANAGER_ADDR}" "$((BASE_PORT + i))" "${backer}" >"${RUN_DIR}/${tag}/s${i}.log" 2>&1 &
    SPIDS+=("$!")
  done

  sleep 2
}

stop_cluster() {
  just p2::kill >/dev/null 2>&1 || true
  if [[ -n "${MPID:-}" ]]; then
    kill "${MPID}" >/dev/null 2>&1 || true
  fi
  local p
  for p in "${SPIDS[@]:-}"; do
    kill "${p}" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  MPID=""
  SPIDS=()
}

run_fuzz_no_crash() {
  local n="$1"
  local tag="fuzz-${n}-no"
  echo "[RUN] ${tag}"
  start_cluster "${n}" "${tag}"
  cargo run -p runner -r --bin fuzzer -- \
    --num-clis 5 \
    --conflict \
    --client-just-args p2::client "${MANAGER_ADDR}" \
    | tee "${RESULT_DIR}/fuzz/${tag}.log" >"${RUN_DIR}/${tag}.log" 2>&1
  grep -q "Fuzz testing result:" "${RESULT_DIR}/fuzz/${tag}.log"
  stop_cluster
}

run_fuzz_with_crash() {
  local n="$1"
  local tag="fuzz-${n}-yes"
  echo "[RUN] ${tag}"
  start_cluster "${n}" "${tag}"

  cargo run -p runner -r --bin fuzzer -- \
    --num-clis 5 \
    --conflict \
    --client-just-args p2::client "${MANAGER_ADDR}" \
    | tee "${RESULT_DIR}/fuzz/${tag}.log" >"${RUN_DIR}/${tag}.log" 2>&1 &
  local fpid=$!

  # Short outage so fuzzer blocks then resumes after recovery.
  sleep 4
  kill "${SPIDS[1]}" >/dev/null 2>&1 || true
  if [[ "${n}" -ge 5 ]]; then
    kill "${SPIDS[2]}" >/dev/null 2>&1 || true
  fi
  sleep 3

  ./kvstore/bin/kvserver 1 "${MANAGER_ADDR}" "$((BASE_PORT + 1))" "${RUN_DIR}/${tag}/backer.s1" >"${RUN_DIR}/${tag}/s1-restart.log" 2>&1 &
  SPIDS[1]=$!
  if [[ "${n}" -ge 5 ]]; then
    ./kvstore/bin/kvserver 2 "${MANAGER_ADDR}" "$((BASE_PORT + 2))" "${RUN_DIR}/${tag}/backer.s2" >"${RUN_DIR}/${tag}/s2-restart.log" 2>&1 &
    SPIDS[2]=$!
  fi

  wait "${fpid}"
  grep -q "Fuzz testing result:" "${RESULT_DIR}/fuzz/${tag}.log"
  stop_cluster
}

run_bench_case() {
  local nclis="$1"
  local wload="$2"
  local nservers="$3"
  local tag="bench-${nclis}-${wload}-${nservers}"
  echo "[RUN] ${tag}"
  start_cluster "${nservers}" "${tag}"
  cargo run -p runner -r --bin bencher -- \
    --num-clis "${nclis}" \
    --workload "${wload}" \
    --client-just-args p2::client "${MANAGER_ADDR}" \
    | tee "${RESULT_DIR}/bench/${tag}.log" >"${RUN_DIR}/${tag}.log" 2>&1
  grep -q "Benchmarking results:" "${RESULT_DIR}/bench/${tag}.log"
  stop_cluster
}

echo "==> Running required fuzz tests"
run_fuzz_no_crash 3
run_fuzz_with_crash 3
run_fuzz_with_crash 5

echo "==> Running required 10-client benchmark matrix"
for w in a b c d e f; do
  for n in 1 3 5; do
    run_bench_case 10 "${w}" "${n}"
  done
done

echo "==> Running required workload-a scaling benchmarks"
for n in 1 5; do
  for c in 1 20 30; do
    run_bench_case "${c}" a "${n}"
  done
done

echo "==> Validating expected output files"
python3 - <<'PY'
import os
root='/tmp/madkv-p2'
missing=[]
for n,c in [(3,'no'),(3,'yes'),(5,'yes')]:
    p=f'{root}/fuzz/fuzz-{n}-{c}.log'
    if not os.path.isfile(p): missing.append(p)
for w in 'abcdef':
    for n in [1,3,5]:
        p=f'{root}/bench/bench-10-{w}-{n}.log'
        if not os.path.isfile(p): missing.append(p)
for n in [1,5]:
    for c in [1,20,30]:
        p=f'{root}/bench/bench-{c}-a-{n}.log'
        if not os.path.isfile(p): missing.append(p)
if missing:
    print('Missing files:')
    for m in missing:
        print(m)
    raise SystemExit(1)
print('All required logs are present in /tmp/madkv-p2')
PY

echo "==> Done"
echo "Logs: ${RESULT_DIR}"
echo "Next: ./scripts/generate_p2_report.sh"
