#!/bin/bash

# Count operations at each stage in coordinator
echo "=== COORDINATOR STATS ==="
echo "Operations assigned to frames:"
grep "OPERATION_FLOW: Assigning operation" logs/websocket-server.log | wc -l

echo "Manifests prepared with operations:"
grep "OPERATION_FLOW: Preparing manifest.*with [1-9]" logs/websocket-server.log | wc -l

echo "Total operations in all manifests:"
grep "OPERATION_FLOW: Preparing manifest.*with [1-9]" logs/websocket-server.log | awk -F'with ' '{print $2}' | awk '{sum += $1} END {print sum}'

echo "Late operations (delayed, not dropped):"
grep "Late operation detected" logs/websocket-server.log | wc -l

# Count operations at each stage in worker
echo "=== WORKER STATS ==="
echo "Frames processed by worker:"
grep "OPERATION_FLOW: Worker.*processing frame.*with [1-9]" logs2/websocket-server.log | wc -l

echo "Operations successfully processed:"
grep "OPERATION_FLOW: Worker.*successfully processed" logs2/websocket-server.log | wc -l

echo "Operations failed:"
grep "OPERATION_FLOW:.*failed" logs2/websocket-server.log | wc -l
