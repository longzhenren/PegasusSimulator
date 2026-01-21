#!/bin/bash
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
#
# Multi-UAV PX4 SITL + Gazebo Launch Script
# ==========================================
#
# This script launches multiple PX4 SITL instances with Gazebo simulation
# for parallel trajectory data collection.
#
# Prerequisites:
#   - PX4-Autopilot (v1.14 or later recommended)
#   - Gazebo (Classic or Garden/Harmonic)
#   - ROS2 Humble/Iron
#   - MAVROS
#
# Usage:
#   ./launch_multi_uav_gazebo.sh [NUM_UAVS] [START_ID]
#
# Examples:
#   ./launch_multi_uav_gazebo.sh 4      # Launch 4 UAVs (ID: 0-3)
#   ./launch_multi_uav_gazebo.sh 8 0    # Launch 8 UAVs starting from ID 0
#

set -e

# Configuration
NUM_UAVS=${1:-4}
START_ID=${2:-0}
PX4_DIR="${PX4_DIR:-$HOME/PX4-Autopilot}"
GAZEBO_WORLD="${GAZEBO_WORLD:-empty}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check PX4
    if [ ! -d "$PX4_DIR" ]; then
        log_error "PX4-Autopilot not found at $PX4_DIR"
        log_error "Set PX4_DIR environment variable or install PX4"
        exit 1
    fi

    # Check Gazebo
    if ! command -v gz &> /dev/null && ! command -v gazebo &> /dev/null; then
        log_warn "Gazebo not found in PATH. Make sure it's installed."
    fi

    # Check ROS2
    if [ -z "$ROS_DISTRO" ]; then
        log_error "ROS2 not sourced. Run: source /opt/ros/<distro>/setup.bash"
        exit 1
    fi

    log_info "Prerequisites check passed"
}

# Calculate spawn positions (grid layout)
get_spawn_position() {
    local id=$1
    local cols=4
    local spacing=3.0  # meters between UAVs

    local row=$((id / cols))
    local col=$((id % cols))

    local x=$(echo "$col * $spacing" | bc)
    local y=$(echo "$row * $spacing" | bc)

    echo "$x $y"
}

# Launch Gazebo world
launch_gazebo() {
    log_info "Launching Gazebo world: $GAZEBO_WORLD"

    # For Gazebo Garden/Harmonic
    if command -v gz &> /dev/null; then
        gz sim -r "${GAZEBO_WORLD}.sdf" &
    # For Gazebo Classic
    elif command -v gazebo &> /dev/null; then
        gazebo --verbose "${PX4_DIR}/Tools/simulation/gazebo-classic/worlds/${GAZEBO_WORLD}.world" &
    else
        log_error "No Gazebo installation found"
        exit 1
    fi

    GAZEBO_PID=$!
    sleep 5
    log_info "Gazebo launched (PID: $GAZEBO_PID)"
}

# Launch single PX4 SITL instance
launch_px4_instance() {
    local id=$1
    local x=$2
    local y=$3

    log_info "Launching PX4 SITL instance $id at position ($x, $y)"

    # Calculate ports
    local mavlink_tcp_port=$((4560 + id))
    local mavlink_udp_port=$((14540 + id))
    local simulator_port=$((14560 + id))

    # Set instance-specific environment
    export PX4_SYS_AUTOSTART=4001  # Generic Quadcopter
    export PX4_GZ_MODEL=x500
    export PX4_GZ_MODEL_POSE="${x},${y},0,0,0,0"
    export PX4_SIM_MODEL=gz_x500
    export MAVLINK_TCP_PORT=$mavlink_tcp_port

    # Create instance directory
    local instance_dir="/tmp/px4_${id}_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$instance_dir"

    # Launch PX4 SITL
    cd "$PX4_DIR"

    # Use make px4_sitl for simulation
    PX4_INSTANCE=$id \
    PX4_SIM_HOST_ADDR=localhost \
    ./build/px4_sitl_default/bin/px4 \
        -i $id \
        -d "$instance_dir" \
        > "${instance_dir}/px4_${id}.log" 2>&1 &

    local px4_pid=$!
    echo $px4_pid >> /tmp/px4_pids.txt

    log_info "PX4 instance $id launched (PID: $px4_pid)"

    sleep 2
}

# Launch MAVROS node for single UAV
launch_mavros() {
    local id=$1
    local fcu_url="udp://:$((14540 + id))@127.0.0.1:$((14580 + id))"
    local namespace="uav${id}"

    log_info "Launching MAVROS for UAV $id (namespace: /$namespace)"

    ros2 run mavros mavros_node \
        --ros-args \
        -r __ns:=/${namespace} \
        -p fcu_url:="$fcu_url" \
        -p gcs_url:="" \
        -p target_system_id:=$((id + 1)) \
        -p target_component_id:=1 \
        > "/tmp/mavros_${id}.log" 2>&1 &

    local mavros_pid=$!
    echo $mavros_pid >> /tmp/mavros_pids.txt

    log_info "MAVROS for UAV $id launched (PID: $mavros_pid)"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."

    # Kill MAVROS nodes
    if [ -f /tmp/mavros_pids.txt ]; then
        while read pid; do
            kill $pid 2>/dev/null || true
        done < /tmp/mavros_pids.txt
        rm -f /tmp/mavros_pids.txt
    fi

    # Kill PX4 instances
    if [ -f /tmp/px4_pids.txt ]; then
        while read pid; do
            kill $pid 2>/dev/null || true
        done < /tmp/px4_pids.txt
        rm -f /tmp/px4_pids.txt
    fi

    # Kill Gazebo
    if [ -n "$GAZEBO_PID" ]; then
        kill $GAZEBO_PID 2>/dev/null || true
    fi

    # Kill any remaining processes
    pkill -f "px4" 2>/dev/null || true
    pkill -f "gazebo" 2>/dev/null || true
    pkill -f "gz sim" 2>/dev/null || true
    pkill -f "mavros" 2>/dev/null || true

    log_info "Cleanup complete"
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Main
main() {
    log_info "========================================"
    log_info "Multi-UAV PX4 SITL + Gazebo Launcher"
    log_info "========================================"
    log_info "Number of UAVs: $NUM_UAVS"
    log_info "Starting ID: $START_ID"
    log_info "PX4 Directory: $PX4_DIR"
    log_info "========================================"

    # Clear old PID files
    rm -f /tmp/px4_pids.txt /tmp/mavros_pids.txt

    # Check prerequisites
    check_prerequisites

    # Launch Gazebo
    launch_gazebo

    # Launch PX4 instances
    for ((i=0; i<NUM_UAVS; i++)); do
        id=$((START_ID + i))
        pos=$(get_spawn_position $i)
        x=$(echo $pos | cut -d' ' -f1)
        y=$(echo $pos | cut -d' ' -f2)

        launch_px4_instance $id $x $y
    done

    # Wait for PX4 to initialize
    log_info "Waiting for PX4 instances to initialize..."
    sleep 10

    # Launch MAVROS nodes
    for ((i=0; i<NUM_UAVS; i++)); do
        id=$((START_ID + i))
        launch_mavros $id
    done

    # Wait for MAVROS to connect
    log_info "Waiting for MAVROS connections..."
    sleep 5

    log_info "========================================"
    log_info "All systems launched successfully!"
    log_info "========================================"
    log_info ""
    log_info "UAV Topics:"
    for ((i=0; i<NUM_UAVS; i++)); do
        id=$((START_ID + i))
        echo "  /uav${id}/mavros/state"
        echo "  /uav${id}/mavros/local_position/pose"
    done
    log_info ""
    log_info "Press Ctrl+C to shutdown all systems"
    log_info "========================================"

    # Keep running
    wait
}

main "$@"
