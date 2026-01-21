#!/bin/bash
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# Full trajectory collection with auto-restart monitor
#
# This script runs the complete data collection with automatic restart
# when PhysX errors or UAV stuck conditions are detected.
#
# Usage: ./start_full_collection.sh [output_suffix]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
ISAAC_SIM_PYTHON="/home/user/isaacsim-5.1.0/python.sh"
SIM_SCRIPT="$SCRIPT_DIR/mavlink_sim_vehicle.py"
COLLECTOR_SCRIPT="$SCRIPT_DIR/mavlink_trajectory_collector.py"
SIM_CONFIG="multi_uav_config_1.json"
INPUT_DIR="/home/user/uav-data/drone/uav-flow-sim/train_data/extracted_json_files"

# Output directory with timestamp
OUTPUT_SUFFIX="${1:-$(date +%Y%m%d_%H%M%S)}"
OUTPUT_DIR="/home/user/uav-data/drone/uav-flow-sim/train_data/collected_$OUTPUT_SUFFIX"
LOG_DIR="$OUTPUT_DIR/logs"

# Collection parameters
BATCH_SIZE=50       # Restart simulation every 50 trajectories
STUCK_THRESHOLD=120  # Restart if no progress for 2 minutes
SCALE=0.01          # Coordinate scale factor

echo "=============================================="
echo "Full Trajectory Collection with Auto-Restart"
echo "=============================================="
echo ""
echo "Input:  $INPUT_DIR"
echo "Output: $OUTPUT_DIR"
echo "Logs:   $LOG_DIR"
echo ""
echo "Batch size: $BATCH_SIZE trajectories"
echo "Stuck threshold: $STUCK_THRESHOLD seconds"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"
mkdir -p "$LOG_DIR"

# Count total trajectories
TOTAL=$(ls -1 "$INPUT_DIR"/*.json 2>/dev/null | wc -l)
echo "Total trajectories to collect: $TOTAL"
echo ""

# Kill any existing processes
echo "Cleaning up existing processes..."
pkill -9 px4 2>/dev/null || true
pkill -9 -f "mavlink_sim_vehicle" 2>/dev/null || true
pkill -9 -f "mavlink_trajectory_collector" 2>/dev/null || true
pkill -9 -f "isaacsim" 2>/dev/null || true
sleep 5

echo "Starting collection monitor..."
echo ""

# Run the monitor
python3 "$SCRIPT_DIR/collection_monitor.py" \
    --isaac-sim-python "$ISAAC_SIM_PYTHON" \
    --sim-script "$SIM_SCRIPT" \
    --collector-script "$COLLECTOR_SCRIPT" \
    --sim-config "$SIM_CONFIG" \
    --input-dir "$INPUT_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --log-dir "$LOG_DIR" \
    --batch-size "$BATCH_SIZE" \
    --stuck-threshold "$STUCK_THRESHOLD" \
    --scale "$SCALE"

# Final summary
echo ""
echo "=============================================="
echo "Collection Complete"
echo "=============================================="
COMPLETED=$(ls -1 "$OUTPUT_DIR" 2>/dev/null | grep -v logs | wc -l)
echo "Trajectories collected: $COMPLETED / $TOTAL"
echo "Output: $OUTPUT_DIR"
echo "Logs: $LOG_DIR"
