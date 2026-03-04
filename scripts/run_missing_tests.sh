#!/bin/bash
# Run only the missing tests for report generation
# Usage: ./run_missing_tests.sh <server_ip:port>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <server_ip:port>"
    echo "Example: $0 10.10.1.2:3777"
    exit 1
fi

SERVER=$1
echo "======================================"
echo "Running MISSING tests only"
echo "Server: $SERVER"
echo "======================================"
echo

# Check what's missing
echo "Checking existing results..."
echo

# Run missing fuzz tests (you ran 5 clients, need 3 clients)
if [ ! -f /tmp/madkv-p1/fuzz/fuzz-3-no.log ]; then
    echo "--- Running fuzz test: 3 clients, no conflicts ---"
    just p1::fuzz 3 no $SERVER
    echo
fi

if [ ! -f /tmp/madkv-p1/fuzz/fuzz-3-yes.log ]; then
    echo "--- Running fuzz test: 3 clients, with conflicts ---"
    just p1::fuzz 3 yes $SERVER
    echo
fi

# Run missing multi-client benchmarks (need 10, 25, 40, 55, 70, 85)
echo "======================================"
echo "Running missing multi-client benchmarks..."
echo "======================================"

for ncli in 10 25 40 55 70 85; do
    # Workload A
    if [ ! -f /tmp/madkv-p1/bench/bench-$ncli-a.log ]; then
        echo "Running: $ncli clients, workload A..."
        just p1::bench $ncli a $SERVER
    fi
    
    # Workload C
    if [ ! -f /tmp/madkv-p1/bench/bench-$ncli-c.log ]; then
        echo "Running: $ncli clients, workload C..."
        just p1::bench $ncli c $SERVER
    fi
    
    # Workload E
    if [ ! -f /tmp/madkv-p1/bench/bench-$ncli-e.log ]; then
        echo "Running: $ncli clients, workload E..."
        just p1::bench $ncli e $SERVER
    fi
    
    sleep 2
done

echo
echo "======================================"
echo "Missing tests complete!"
echo "Now try: just p1::report"
echo "======================================"
