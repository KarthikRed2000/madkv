#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define your manager address (adjust if your configuration differs)
MANAGER_ADDR="10.10.1.2:3666"

echo "============================================"
echo " Starting Project 2 Automated Test Suite "
echo "============================================"

# Ensure the project is built before running tests
just p2::clean
just p2::build

echo ""
echo "--------------------------------------------"
echo " 1. Running Fuzz Tests"
echo "--------------------------------------------"
# Requirement: 3 partitions, no crashing [cite: 180]
echo "[Fuzz] Scenario 1: 3 partitions, no crashing..."
just p2::fuzz 3 no $MANAGER_ADDR

# Requirement: 3 partitions, with crash/recovery of server 1 [cite: 181]
echo "[Fuzz] Scenario 2: 3 partitions, crashing server 1..."
just p2::fuzz 3 yes $MANAGER_ADDR

# Requirement: 5 partitions, with crash/recovery of servers 1 and 2 [cite: 182]
# Note: Ensure your justfile's fuzz recipe is equipped to handle multiple crashes
echo "[Fuzz] Scenario 3: 5 partitions, crashing servers 1 & 2..."
just p2::fuzz 5 yes $MANAGER_ADDR

echo ""
echo "--------------------------------------------"
echo " 2. Running YCSB Benchmarks"
echo "--------------------------------------------"

# Requirement: 10 clients on all workloads A to F, varying partition servers across 1, 3, and 5 [cite: 152, 184]
WORKLOADS=("a" "b" "c" "d" "e" "f")
PARTITIONS=(1 3 5)

for p in "${PARTITIONS[@]}"; do
    for w in "${WORKLOADS[@]}"; do
        echo "[Bench] 10 clients | Workload $w | $p Partitions"
        just p2::bench 10 $w $p $MANAGER_ADDR
    done
done

# Requirement: Workload A with 1 or 5 partitions, scaling the number of clients from 1 to 30 [cite: 153, 185]
# (Testing increments of 1, 10, 20, 30 clients)
CLIENT_COUNTS=(1 10 20 30)
SCALING_PARTITIONS=(1 5)

for p in "${SCALING_PARTITIONS[@]}"; do
    for c in "${CLIENT_COUNTS[@]}"; do
        # Skip 10 clients since it was already run in the previous loop
        if [ "$c" -eq 10 ]; then
            continue
        fi
        echo "[Bench] Scaling: $c clients | Workload a | $p Partitions"
        just p2::bench $c "a" $p $MANAGER_ADDR
    done
done

echo ""
echo "--------------------------------------------"
echo " 3. Generating Report"
echo "--------------------------------------------"
# Requirement: Generate a template Markdown report from your code and plot images [cite: 189, 191]
echo "Running report generation script..."
just p2::report

echo "============================================"
echo " All tasks completed successfully! "
echo " Please check your generated Markdown report and plot images."
echo "============================================"