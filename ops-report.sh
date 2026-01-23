# Count operations across all stages
echo "=== OPERATION FLOW TRACKING ==="
grep -E "OPERATION_FLOW:" *-runtime.log | wc -l

# Break down by stage
grep -E "OPERATION_FLOW:.*Assigning operation" COORDINATOR-app-coordinator-runtime.log | wc -l
grep -E "OPERATION_FLOW:.*Preparing manifest" COORDINATOR-app-coordinator-runtime.log | wc -l
grep -E "OPERATION_FLOW:.*processing frame" WORKER-*-runtime.log | wc -l
grep -E "OPERATION_FLOW:.*successfully processed" WORKER-*-runtime.log | wc -l

# Frame processing patterns
echo "=== FRAME PROCESSING ==="
grep -E "Processing logical frame [1-9]+ with [1-9]+ operations" WORKER-*-runtime.log

# Delta storage operations
echo "=== DELTA STORAGE ==="
grep -E "(Captured|Stored|Failed to store) delta" WORKER-*-runtime.log

# Mutation processing
echo "=== MUTATIONS ==="
grep -E "Process(ing|ed) (mutate command|MUTATE)" WORKER-*-runtime.log | wc -l

# Errors (excluding false positives)
echo "=== ERRORS ==="
grep -E "ERROR|Error processing operation|Failed" *-runtime.log | grep -v "errorMessage"

# Per-worker breakdown
echo "=== PER WORKER COUNTS ==="
for log in WORKER-*-runtime.log; do
    count=$(grep -E "Processed MUTATE" $log | wc -l)
    echo "$log: $count mutations"
done

# Check if frames are completing
# grep -E "Reported logical frame [0-9]+ completion" WORKER-*-runtime.log | tail -10