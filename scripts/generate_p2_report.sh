#!/usr/bin/env bash
set -euo pipefail

# Generate report/proj2.md and plots from /tmp/madkv-p2 logs.
#
# Usage:
#   ./scripts/generate_p2_report.sh [result_dir] [report_dir]
#
# Defaults:
#   result_dir=/tmp/madkv-p2
#   report_dir=report

RESULT_DIR="${1:-/tmp/madkv-p2}"
REPORT_DIR="${2:-report}"

cd "$(dirname "$0")/.."

if [[ ! -d "${RESULT_DIR}/fuzz" || ! -d "${RESULT_DIR}/bench" ]]; then
  echo "Missing expected results directories under ${RESULT_DIR}"
  echo "Run: ./scripts/run_p2_required.sh"
  exit 1
fi

mkdir -p "${REPORT_DIR}/plots-p2"

# Avoid matplotlib permission/cache warnings on shared environments.
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/matplotlib-${USER}}"
mkdir -p "${MPLCONFIGDIR}"

echo "Generating Project 2 report from ${RESULT_DIR} -> ${REPORT_DIR}"
printf '\n\n' | python3 sumgen/proj2.py -i "${RESULT_DIR}" -o "${REPORT_DIR}"

echo "Generated:"
echo "  ${REPORT_DIR}/proj2.md"
echo "  ${REPORT_DIR}/plots-p2/ycsb-ten-clients.png"
echo "  ${REPORT_DIR}/plots-p2/ycsb-tput-trend.png"
