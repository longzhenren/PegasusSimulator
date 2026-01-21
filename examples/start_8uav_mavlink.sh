#!/bin/bash
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# 8-UAV MAVLink仿真启动脚本
# 使用方法: ./start_8uav_mavlink.sh [--headless]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ISAACSIM_PYTHON="/home/user/isaacsim-5.1.0/python.sh"
CONFIG="$SCRIPT_DIR/multi_uav_config_8.json"

# 解析参数
HEADLESS=""
if [[ "$1" == "--headless" ]]; then
    HEADLESS="--headless"
fi

echo "=================================================="
echo "8-UAV MAVLink仿真启动脚本"
echo "=================================================="

# 步骤1: 清理现有进程
echo ""
echo "[步骤1] 清理现有仿真进程..."

pkill -9 -f "8_camera_vehicle" 2>/dev/null || true
pkill -9 -f "mavlink_sim_vehicle" 2>/dev/null || true
pkill -9 -f "rospy_isaacsim" 2>/dev/null || true
pkill -9 -f "launch_multi_rospy" 2>/dev/null || true
pkill -9 -f "px4" 2>/dev/null || true

echo "等待进程终止..."
sleep 5

# 步骤2: 检查端口
echo ""
echo "[步骤2] 检查端口状态..."

check_port() {
    local port=$1
    if ss -tlnp 2>/dev/null | grep -q ":${port}"; then
        echo "  端口 $port: 占用中"
        return 1
    else
        echo "  端口 $port: 空闲"
        return 0
    fi
}

all_ports_free=true
for port in 8081 5009 5010 5011 5012 5013 5014 5015 5016; do
    if ! check_port $port; then
        all_ports_free=false
    fi
done

if [ "$all_ports_free" = false ]; then
    echo ""
    echo "[警告] 部分端口仍被占用，等待释放..."
    sleep 10
fi

# 步骤3: 检查配置文件
echo ""
echo "[步骤3] 检查配置文件..."

if [ ! -f "$CONFIG" ]; then
    echo "[错误] 配置文件不存在: $CONFIG"
    exit 1
fi

echo "  配置文件: $CONFIG"
NUM_VEHICLES=$(python3 -c "import json; print(len(json.load(open('$CONFIG'))['vehicles']))")
echo "  UAV数量: $NUM_VEHICLES"

# 步骤4: 启动仿真
echo ""
echo "[步骤4] 启动MAVLink仿真..."
echo "  命令: $ISAACSIM_PYTHON $SCRIPT_DIR/mavlink_sim_vehicle.py --config $CONFIG $HEADLESS"
echo ""
echo "=================================================="
echo "正在启动Isaac Sim..."
echo "请等待仿真初始化完成（约2-3分钟）"
echo "启动完成后可在另一终端运行数据采集脚本"
echo "=================================================="
echo ""

# 执行仿真
$ISAACSIM_PYTHON "$SCRIPT_DIR/mavlink_sim_vehicle.py" --config "$CONFIG" $HEADLESS
