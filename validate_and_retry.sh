#!/bin/bash
# Validate and Retry Failed Trajectories
# Automatically detects ground-level data and retries those trajectories

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
INPUT_DIR="${1:-/home/user/uav-data/drone/uav-flow-sim/train_data/extracted_json_files}"
OUTPUT_DIR="${2:-/home/user/uav-data/trajectory_recordings_new}"
Z_THRESHOLD=0.5  # Below this altitude = ground-level failure
NUM_UAVS=8

echo "========================================"
echo "Validate and Retry Failed Trajectories"
echo "========================================"
echo "Output Dir: $OUTPUT_DIR"
echo "Z Threshold: $Z_THRESHOLD m"
echo "========================================"

# Find ground-level failures
find_failures() {
    echo "Scanning for ground-level failures..."
    FAILURES=()
    
    for data_csv in $(find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null); do
        avg_z=$(tail -n +2 "$data_csv" | awk -F, '{ sum += $15; n++ } END { if(n>0) print sum/n; else print 999 }' 2>/dev/null)
        if [ $(echo "$avg_z < $Z_THRESHOLD" | bc -l 2>/dev/null || echo 0) -eq 1 ]; then
            # Extract trajectory name from path
            TRAJ_DIR=$(dirname "$(dirname "$data_csv")")
            TRAJ_NAME=$(basename "$TRAJ_DIR")
            JSON_FILE="$INPUT_DIR/${TRAJ_NAME}.json"
            if [ -f "$JSON_FILE" ]; then
                FAILURES+=("$JSON_FILE")
                echo "  FAIL: $TRAJ_NAME (avg_z=$avg_z)"
            fi
        fi
    done
    
    echo "Found ${#FAILURES[@]} failures"
}

# Remove failed directories
remove_failures() {
    echo "Removing failed data..."
    for data_csv in $(find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null); do
        avg_z=$(tail -n +2 "$data_csv" | awk -F, '{ sum += $15; n++ } END { if(n>0) print sum/n; else print 999 }' 2>/dev/null)
        if [ $(echo "$avg_z < $Z_THRESHOLD" | bc -l 2>/dev/null || echo 0) -eq 1 ]; then
            UAV_DIR=$(dirname "$data_csv")
            echo "  Removing: $UAV_DIR"
            rm -rf "$UAV_DIR"
        fi
    done
}

# Retry failed trajectories
retry_failures() {
    if [ ${#FAILURES[@]} -eq 0 ]; then
        echo "No failures to retry!"
        return 0
    fi
    
    echo "Retrying ${#FAILURES[@]} failed trajectories..."
    
    # Check if simulator is running
    if ! pgrep -f "mavlink_sim_vehicle.py" > /dev/null; then
        echo "ERROR: Simulator not running! Please start it first."
        return 1
    fi
    
    # Process failures in batches
    BATCH_SIZE=$NUM_UAVS
    TOTAL_FAILURES=${#FAILURES[@]}
    PROCESSED=0
    
    while [ $PROCESSED -lt $TOTAL_FAILURES ]; do
        BATCH_END=$((PROCESSED + BATCH_SIZE))
        if [ $BATCH_END -gt $TOTAL_FAILURES ]; then
            BATCH_END=$TOTAL_FAILURES
        fi
        
        echo "[$(date '+%H:%M:%S')] Retry batch: $((PROCESSED + 1))-$BATCH_END of $TOTAL_FAILURES"
        
        # Start collectors for this batch
        PIDS=()
        for UAV_ID in $(seq 0 $((BATCH_SIZE - 1))); do
            FAIL_IDX=$((PROCESSED + UAV_ID))
            if [ $FAIL_IDX -ge $TOTAL_FAILURES ]; then
                break
            fi
            
            JSON_FILE="${FAILURES[$FAIL_IDX]}"
            CTRL_PORT=$((5009 + UAV_ID))
            
            python3 -u examples/simple_trajectory_collector.py \
                --uav-id $UAV_ID \
                --json-file "$JSON_FILE" \
                --out-dir "$OUTPUT_DIR" \
                --control-base http://127.0.0.1:$CTRL_PORT \
                --scale 0.01 \
                --time-scale 2.5 \
                > logs/retry_$(basename "$JSON_FILE" .json)_uav${UAV_ID}.log 2>&1 &
            PIDS+=($!)
        done
        
        # Wait for batch
        echo "  Waiting for batch completion..."
        for PID in "${PIDS[@]}"; do
            wait $PID 2>/dev/null || true
        done
        
        PROCESSED=$BATCH_END
        
        # Wait between batches for UAVs to reset
        sleep 10
    done
}

# Main
find_failures
INITIAL_COUNT=${#FAILURES[@]}

if [ $INITIAL_COUNT -eq 0 ]; then
    echo "No failures found. All data looks valid!"
    exit 0
fi

echo ""
echo "Options:"
echo "  1) Remove failures and retry"
echo "  2) Just remove failures (no retry)"
echo "  3) Exit without changes"
read -p "Choice [1/2/3]: " CHOICE

case $CHOICE in
    1)
        remove_failures
        find_failures  # Refresh the list after removal
        retry_failures
        ;;
    2)
        remove_failures
        ;;
    3)
        echo "Exiting without changes."
        exit 0
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

# Final validation
echo ""
echo "========================================"
echo "Final Validation"
echo "========================================"
find_failures
FINAL_COUNT=${#FAILURES[@]}
echo "Before: $INITIAL_COUNT failures"
echo "After: $FINAL_COUNT failures"
echo "Fixed: $((INITIAL_COUNT - FINAL_COUNT))"
echo "========================================"
