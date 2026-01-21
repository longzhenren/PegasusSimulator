#!/bin/bash
# =============================================================================
# 4环境 × 2机 并行数据采集启动脚本
# 推荐配置：8架飞机并行，内存占用~52%，稳定可靠
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PEGASUS_DIR="$(dirname "$SCRIPT_DIR")"
ISAAC_PYTHON="$HOME/isaacsim-5.1.0/python.sh"
LOG_DIR="/tmp/multi_env_collection_$(date +%Y%m%d_%H%M%S)"

# 轨迹数据目录
TRAJECTORY_DIR="${1:-$HOME/uav-data/drone/uav-flow-sim/train_data/extracted_json_files}"

echo "============================================================"
echo "4环境 × 2机 并行数据采集"
echo "============================================================"
echo "轨迹目录: $TRAJECTORY_DIR"
echo "日志目录: $LOG_DIR"
echo ""

# 创建日志目录
mkdir -p "$LOG_DIR"

# =============================================================================
# 步骤1: 清理残留进程
# =============================================================================
echo "[步骤1] 清理残留进程..."
pkill -9 -f "mavlink_sim_vehicle" 2>/dev/null || true
pkill -9 -f "isaac-sim" 2>/dev/null || true
pkill -9 -f "isaacsim" 2>/dev/null || true
pkill -9 -f "px4_sitl" 2>/dev/null || true
pkill -9 -f "px4-" 2>/dev/null || true
pkill -9 -f "rcS_minmal" 2>/dev/null || true
rm -rf /tmp/px4_instance_* /tmp/px4-* 2>/dev/null || true
sleep 3
echo "  清理完成"

# =============================================================================
# 步骤2: 生成环境配置
# =============================================================================
echo ""
echo "[步骤2] 生成环境配置..."
mkdir -p "$SCRIPT_DIR/multi_env_configs"

for env_id in 0 1 2 3; do
    uav0=$((env_id * 2))
    uav1=$((env_id * 2 + 1))
    config_file="$SCRIPT_DIR/multi_env_configs/env${env_id}_2uav.json"

    cat > "$config_file" << EOF
{
  "description": "Environment ${env_id}: UAV ${uav0}-${uav1}",
  "vehicles": [
    {
      "vehicle_id": ${uav0},
      "ros2_namespace": "uav${uav0}",
      "initial_position": [0.0, 0.0, 0.5],
      "initial_orientation_euler_deg": [0.0, 0.0, 0.0],
      "px4_autolaunch": true,
      "px4_dir": "/home/user/PX4-Autopilot",
      "sim_speed_factor": 1.0
    },
    {
      "vehicle_id": ${uav1},
      "ros2_namespace": "uav${uav1}",
      "initial_position": [3.0, 0.0, 0.5],
      "initial_orientation_euler_deg": [0.0, 0.0, 0.0],
      "px4_autolaunch": true,
      "px4_dir": "/home/user/PX4-Autopilot",
      "sim_speed_factor": 1.0
    }
  ]
}
EOF
    echo "  生成: $config_file"
done

# =============================================================================
# 步骤3: 启动4个仿真环境
# =============================================================================
echo ""
echo "[步骤3] 启动4个仿真环境..."

declare -a SIM_PIDS

for env_id in 0 1 2 3; do
    http_port=$((8081 + env_id))
    ctrl_base=$((5009 + env_id * 100))
    config_file="$SCRIPT_DIR/multi_env_configs/env${env_id}_2uav.json"
    log_file="$LOG_DIR/env${env_id}.log"

    echo "  启动环境 $env_id: HTTP=$http_port, 控制器基础端口=$ctrl_base"

    cd "$SCRIPT_DIR"
    $ISAAC_PYTHON mavlink_sim_vehicle.py \
        --headless \
        --no-images \
        --config "$config_file" \
        --http-port $http_port \
        --controller-base-port $ctrl_base \
        > "$log_file" 2>&1 &

    SIM_PIDS[$env_id]=$!
    echo "    PID: ${SIM_PIDS[$env_id]}"

    # 每个环境启动间隔20秒，避免资源争用
    if [ $env_id -lt 3 ]; then
        echo "    等待20秒..."
        sleep 20
    fi
done

# =============================================================================
# 步骤4: 等待环境就绪
# =============================================================================
echo ""
echo "[步骤4] 等待环境就绪 (超时=180秒)..."

TIMEOUT=180
ELAPSED=0
ALL_READY=false

while [ $ELAPSED -lt $TIMEOUT ]; do
    ready_count=0

    for env_id in 0 1 2 3; do
        http_port=$((8081 + env_id))
        if curl -s "http://localhost:$http_port/status" > /dev/null 2>&1; then
            ready_count=$((ready_count + 1))
        fi
    done

    echo "  [${ELAPSED}s] HTTP就绪: $ready_count/4"

    if [ $ready_count -eq 4 ]; then
        ALL_READY=true
        break
    fi

    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ "$ALL_READY" = false ]; then
    echo ""
    echo "[错误] 环境启动超时！"
    echo "查看日志: $LOG_DIR/env*.log"
    exit 1
fi

echo ""
echo "============================================================"
echo "所有环境就绪！"
echo "============================================================"
echo ""
echo "端口配置:"
echo "  环境0: HTTP=8081, 控制器=5009-5010"
echo "  环境1: HTTP=8082, 控制器=5109-5110"
echo "  环境2: HTTP=8083, 控制器=5209-5210"
echo "  环境3: HTTP=8084, 控制器=5309-5310"
echo ""
echo "日志目录: $LOG_DIR"
echo ""
echo "现在可以启动数据采集:"
echo "  python3 collect_4env_8uav.py --trajectory-dir $TRAJECTORY_DIR"
echo ""
echo "停止所有环境:"
echo "  pkill -9 -f mavlink_sim_vehicle"
echo ""

# 保持脚本运行，显示资源使用
echo "按 Ctrl+C 停止监控（仿真将继续运行）"
while true; do
    cpu=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}')
    mem=$(free | grep Mem | awk '{printf "%.1f", $3/$2 * 100}')
    echo "[$(date +%H:%M:%S)] CPU: ${cpu}%, 内存: ${mem}%"
    sleep 10
done
