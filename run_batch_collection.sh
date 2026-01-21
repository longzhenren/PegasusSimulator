#!/bin/bash
# Robust Batch 8-UAV Data Collection Script
# Features:
# - Progress monitoring every 30 seconds
# - 3-minute timeout detection with auto-restart
# - Resume from last completed file
# - Detailed logging

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
INPUT_DIR="${1:-/home/user/uav-data/drone/uav-flow-sim/train_data/extracted_json_files}"
OUTPUT_DIR="${2:-/home/user/uav-data/trajectory_recordings_new}"
NUM_UAVS=8
TIMEOUT_SECONDS=300  # 2 minutes
PROGRESS_INTERVAL=30 # Log progress every 30 seconds

# State files
PROGRESS_FILE="$OUTPUT_DIR/.batch_progress"

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p logs

echo "========================================"
echo "Robust Batch Data Collection"
echo "========================================"
echo "Input Dir: $INPUT_DIR"
echo "Output Dir: $OUTPUT_DIR"
echo "Timeout: ${TIMEOUT_SECONDS}s (3 min)"
echo "Started: $(date)"
echo "========================================"

# Get all JSON files
mapfile -t JSON_FILES < <(ls "$INPUT_DIR"/*.json 2>/dev/null | sort)
TOTAL_FILES=${#JSON_FILES[@]}
echo "Total Files: $TOTAL_FILES"

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo "ERROR: No JSON files found!"
    exit 1
fi

# Check for existing progress
START_IDX=0
if [ -f "$PROGRESS_FILE" ]; then
    COMPLETED=$(cat "$PROGRESS_FILE" 2>/dev/null || echo "0")
    if [ "$COMPLETED" -gt 0 ] && [ "$COMPLETED" -lt "$TOTAL_FILES" ]; then
        echo "Resuming from file $COMPLETED..."
        START_IDX=$COMPLETED
    fi
fi

# Function: Cleanup
cleanup() {
    pkill -f "px4" 2>/dev/null || true
    pkill -f "mavlink_sim_vehicle" 2>/dev/null || true
    pkill -f "trajectory_collector" 2>/dev/null || true
    rm -rf /tmp/pegasus_px4_sitl /tmp/px4_lock* 2>/dev/null || true
}

# Function: Start simulator
start_simulator() {
    echo "[$(date '+%H:%M:%S')] Starting Simulator with $NUM_UAVS UAVs..."
    cleanup
    sleep 3
    
    /home/user/isaacsim-5.1.0/python.sh examples/mavlink_sim_vehicle.py \
        --config examples/eight_uav_config.json \
        --no-images \
        --headless \
        > logs/mavlink_sim_batch.log 2>&1 &
    SIM_PID=$!
    echo "  Simulator PID: $SIM_PID"
    
    echo "  Waiting for simulator startup (90s)..."
    sleep 90
    
    if ! kill -0 $SIM_PID 2>/dev/null; then
        echo "  ERROR: Simulator failed to start!"
        tail -30 logs/mavlink_sim_batch.log
        return 1
    fi
    
    echo "  Simulator running OK"
    return 0
}

# Function: Run one batch (up to 8 files)
run_batch() {
    local BATCH_START=$1
    local BATCH_END=$((BATCH_START + NUM_UAVS))
    
    if [ $BATCH_END -gt $TOTAL_FILES ]; then
        BATCH_END=$TOTAL_FILES
    fi
    
    local BATCH_SIZE=$((BATCH_END - BATCH_START))
    if [ $BATCH_SIZE -le 0 ]; then
        return 0
    fi
    
    local BATCH_NUM=$(( (BATCH_START / NUM_UAVS) + 1 ))
    local TOTAL_BATCHES=$(( (TOTAL_FILES + NUM_UAVS - 1) / NUM_UAVS ))
    
    echo "[$(date '+%H:%M:%S')] Batch $BATCH_NUM/$TOTAL_BATCHES: Files $((BATCH_START + 1))-$BATCH_END of $TOTAL_FILES"
    
    # Start collectors
    local PIDS=()
    for UAV_ID in $(seq 0 $((BATCH_SIZE - 1))); do
        local FILE_IDX=$((BATCH_START + UAV_ID))
        local JSON_FILE="${JSON_FILES[$FILE_IDX]}"
        local CTRL_PORT=$((5009 + UAV_ID))
        
        python3 -u examples/simple_trajectory_collector.py \
            --uav-id $UAV_ID \
            --json-file "$JSON_FILE" \
            --out-dir "$OUTPUT_DIR" \
            --control-base http://127.0.0.1:$CTRL_PORT \
            --scale 0.01 \
            --time-scale 1.0 \
            > logs/collector_f${FILE_IDX}_uav${UAV_ID}.log 2>&1 &
        PIDS+=($!)
    done
    
    # Wait with timeout check
    local BATCH_START_TIME=$(date +%s)
    local LAST_PROGRESS_TIME=$(date +%s)
    local COMPLETED=0
    local TOTAL_IN_BATCH=${#PIDS[@]}
    
    while true; do
        sleep 5
        
        # Check how many completed
        local NEW_COMPLETED=0
        for PID in "${PIDS[@]}"; do
            if ! kill -0 $PID 2>/dev/null; then
                ((NEW_COMPLETED++)) || true
            fi
        done
        
        # Check for progress
        local NOW=$(date +%s)
        if [ $NEW_COMPLETED -gt $COMPLETED ]; then
            COMPLETED=$NEW_COMPLETED
            LAST_PROGRESS_TIME=$NOW
        fi
        
        # All done?
        if [ $COMPLETED -eq $TOTAL_IN_BATCH ]; then
            echo "  ✓ Batch complete ($COMPLETED/$TOTAL_IN_BATCH)"
            echo "$BATCH_END" > "$PROGRESS_FILE"
            return 0
        fi
        
        # Timeout check
        local ELAPSED=$((NOW - LAST_PROGRESS_TIME))
        if [ $ELAPSED -gt $TIMEOUT_SECONDS ]; then
            echo "  ⚠ TIMEOUT: No progress for ${ELAPSED}s, restarting..."
            # Kill remaining
            for PID in "${PIDS[@]}"; do
                kill $PID 2>/dev/null || true
            done
            return 1
        fi
        
        # Progress report every 30 seconds
        local TOTAL_ELAPSED=$((NOW - BATCH_START_TIME))
        if [ $((TOTAL_ELAPSED % PROGRESS_INTERVAL)) -lt 5 ]; then
            local PROGRESS_PCT=$((BATCH_END * 100 / TOTAL_FILES))
            echo "  [$(date '+%H:%M:%S')] $COMPLETED/$TOTAL_IN_BATCH done, Overall: $BATCH_END/$TOTAL_FILES ($PROGRESS_PCT%)"
        fi
    done
}

# Main loop with restart
RESTART_COUNT=0
MAX_RESTARTS=50
CURRENT_IDX=$START_IDX

while [ $CURRENT_IDX -lt $TOTAL_FILES ]; do
    # Start/restart simulator if needed
    if ! pgrep -f "mavlink_sim_vehicle.py" > /dev/null; then
        if ! start_simulator; then
            ((RESTART_COUNT++)) || true
            if [ $RESTART_COUNT -gt $MAX_RESTARTS ]; then
                echo "ERROR: Too many restarts, giving up"
                exit 1
            fi
            echo "Restarting... (attempt $RESTART_COUNT)"
            sleep 10
            continue
        fi
    fi
    
    # Run batch
    if run_batch $CURRENT_IDX; then
        CURRENT_IDX=$((CURRENT_IDX + NUM_UAVS))
        RESTART_COUNT=0  # Reset restart counter on success
        
        # Per-batch validation: check and remove ground-level failures
        echo "  Validating batch data..."
        for data_csv in $(find "$OUTPUT_DIR" -name "data.csv" -newer "$PROGRESS_FILE" 2>/dev/null || find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null | tail -$NUM_UAVS); do
            avg_z=$(tail -n +2 "$data_csv" 2>/dev/null | awk -F, '{ sum += $15; n++ } END { if(n>0) printf "%.2f", sum/n; else print 999 }' 2>/dev/null || echo "999")
            if [ $(echo "$avg_z < 0.5" | bc -l 2>/dev/null || echo 0) -eq 1 ]; then
                UAV_DIR=$(dirname "$data_csv")
                TRAJ_NAME=$(basename "$(dirname "$UAV_DIR")")
                echo "  ✗ FAIL: $TRAJ_NAME (z=$avg_z), removing..."
                rm -rf "$UAV_DIR"
            fi
        done
        
        # Longer pause between batches for UAVs to fully reset
        sleep 10
    else
        # Timeout - need to restart simulator
        ((RESTART_COUNT++)) || true
        if [ $RESTART_COUNT -gt $MAX_RESTARTS ]; then
            echo "ERROR: Too many restarts, giving up"
            exit 1
        fi
        echo "[$(date '+%H:%M:%S')] Need to restart simulator (attempt $RESTART_COUNT)"
        cleanup
        sleep 5
    fi
done

echo ""
echo "========================================"
echo "Main Collection Complete! Running validation..."
echo "========================================"
echo "Finished: $(date)"

# Count results
DATA_CSV_COUNT=$(find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null | wc -l)
echo "Data CSV files generated: $DATA_CSV_COUNT"

# Auto-validation: Find ground-level failures
echo ""
echo "Validating data quality (checking for z < 0.5m)..."
FAIL_COUNT=0
FAIL_FILES=()

for data_csv in $(find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null); do
    avg_z=$(tail -n +2 "$data_csv" | awk -F, '{ sum += $15; n++ } END { if(n>0) print sum/n; else print 999 }' 2>/dev/null || echo "999")
    if [ $(echo "$avg_z < 0.5" | bc -l 2>/dev/null || echo 0) -eq 1 ]; then
        TRAJ_DIR=$(dirname "$(dirname "$data_csv")")
        TRAJ_NAME=$(basename "$TRAJ_DIR")
        JSON_FILE="$INPUT_DIR/${TRAJ_NAME}.json"
        if [ -f "$JSON_FILE" ]; then
            ((FAIL_COUNT++)) || true
            FAIL_FILES+=("$JSON_FILE")
            echo "  FAIL: $TRAJ_NAME (avg_z=$avg_z)"
            # Remove failed directory
            rm -rf "$(dirname "$data_csv")"
        fi
    fi
done

echo "Ground-level failures: $FAIL_COUNT"

# Auto-retry failures if any
if [ $FAIL_COUNT -gt 0 ] && [ $FAIL_COUNT -le 100 ]; then
    echo ""
    echo "Auto-retrying $FAIL_COUNT failures..."
    
    RETRY_PROCESSED=0
    while [ $RETRY_PROCESSED -lt $FAIL_COUNT ]; do
        BATCH_END=$((RETRY_PROCESSED + NUM_UAVS))
        if [ $BATCH_END -gt $FAIL_COUNT ]; then
            BATCH_END=$FAIL_COUNT
        fi
        
        echo "[$(date '+%H:%M:%S')] Retry batch: $((RETRY_PROCESSED + 1))-$BATCH_END of $FAIL_COUNT"
        
        PIDS=()
        for UAV_ID in $(seq 0 $((NUM_UAVS - 1))); do
            FAIL_IDX=$((RETRY_PROCESSED + UAV_ID))
            if [ $FAIL_IDX -ge $FAIL_COUNT ]; then
                break
            fi
            
            JSON_FILE="${FAIL_FILES[$FAIL_IDX]}"
            CTRL_PORT=$((5009 + UAV_ID))
            
            python3 -u examples/simple_trajectory_collector.py \
                --uav-id $UAV_ID \
                --json-file "$JSON_FILE" \
                --out-dir "$OUTPUT_DIR" \
                --control-base http://127.0.0.1:$CTRL_PORT \
                --scale 0.01 \
                --time-scale 1.0 \
                > logs/retry_$(basename "$JSON_FILE" .json)_uav${UAV_ID}.log 2>&1 &
            PIDS+=($!)
        done
        
        for PID in "${PIDS[@]}"; do
            wait $PID 2>/dev/null || true
        done
        
        RETRY_PROCESSED=$BATCH_END
        sleep 10
    done
    
    # Re-count
    FINAL_CSV_COUNT=$(find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null | wc -l)
    echo ""
    echo "After retry: $FINAL_CSV_COUNT data.csv files"
elif [ $FAIL_COUNT -gt 100 ]; then
    echo "Too many failures ($FAIL_COUNT), run validate_and_retry.sh manually"
fi

# Cleanup
cleanup
rm -f "$PROGRESS_FILE"

echo ""
echo "========================================"
echo "All Done!"
echo "========================================"
echo "Final data.csv count: $(find "$OUTPUT_DIR" -name "data.csv" 2>/dev/null | wc -l)"
