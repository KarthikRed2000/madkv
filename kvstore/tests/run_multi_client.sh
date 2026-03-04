#!/bin/bash
# Helper script to run multiple clients concurrently
# Usage: ./run_multi_client.sh <server_addr> <test_num>

if [ $# -lt 2 ]; then
    echo "Usage: $0 <server_addr> <test_num>"
    echo "Example: $0 127.0.0.1:3777 3"
    exit 1
fi

SERVER_ADDR=$1
TEST_NUM=$2
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_BIN="$SCRIPT_DIR/../bin/kvclient"

echo "=== Running Test $TEST_NUM with Multiple Clients ==="
echo "Server: $SERVER_ADDR"
echo ""

# Run clients in parallel, capturing output
echo "--- Client 1 Output ---"
cat "$SCRIPT_DIR/test${TEST_NUM}_client1.txt" | "$CLIENT_BIN" "$SERVER_ADDR" &
PID1=$!

echo "--- Client 2 Output ---"
cat "$SCRIPT_DIR/test${TEST_NUM}_client2.txt" | "$CLIENT_BIN" "$SERVER_ADDR" &
PID2=$!

# Wait for both to complete
wait $PID1
wait $PID2

echo ""
echo "=== Test $TEST_NUM Complete ==="
