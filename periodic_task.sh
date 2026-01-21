#!/bin/bash
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)

# Function to handle cleanup on script exit or end of cycle
cleanup() {
    echo "Stopping all background processes..."
    if [ -n "$PID1" ]; then 
        kill -INT "$PID1" 2>/dev/null
        echo "Sent SIGINT to PID $PID1"
    fi
    if [ -n "$PID2" ]; then 
        kill -INT "$PID2" 2>/dev/null
        echo "Sent SIGINT to PID $PID2"
    fi
}

# Trap Ctrl+C (SIGINT) for the script itself to exit gracefully
trap "cleanup; exit" SIGINT

echo "Starting periodic task loop..."

while true; do
    echo "=================================================="
    echo "[$(date)] Starting new 20-minute cycle"

    # --- Command 1 ---
    echo "[$(date)] Starting Command 1: launch_multi_rospy.py"
    cd /home/user/PegasusSimulator-5.1/examples || exit
    python3 launch_multi_rospy.py &
    PID1=$!
    echo "Command 1 started with PID: $PID1"

    # Wait 90 seconds
    echo "[$(date)] Waiting 90 seconds..."
    sleep 90

    # --- Command 2 ---
    echo "[$(date)] Starting Command 2: trajectory_data_collector.py"
    cd /home/user/PegasusSimulator-5.1/ || exit
    ~/isaacsim-5.1.0/python.sh examples/trajectory_data_collector.py \
        --input-dir ~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files \
        --pattern "*.json" \
        --skip-existing \
        --out-dir /home/user/uav-data/trajectory_recordings \
        --control-base http://127.0.0.1:5009 \
        --image-base http://127.0.0.1:8081 &
    PID2=$!
    echo "Command 2 started with PID: $PID2"

    # Wait for the remaining time (20 mins total = 1200s. 1200 - 120 = 1080s)
    echo "[$(date)] Waiting remaining 18 minutes..."
    sleep 1080

    # --- Cleanup at end of cycle ---
    echo "[$(date)] Cycle finished. Stopping processes..."
    
    cleanup
    
    # Wait for them to exit gracefully
    wait $PID1 2>/dev/null
    wait $PID2 2>/dev/null
    
    echo "[$(date)] Processes stopped."
    
    # Reset PIDs
    PID1=""
    PID2=""
    
    echo "[$(date)] Preparing for next cycle..."
    echo ""
done
