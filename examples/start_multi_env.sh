#!/bin/bash
# 多环境并行启动脚本
# 4个环境 x 2架飞机 = 8架飞机并行
#
# 端口规划（避免冲突）：
# ============================================
# 环境 | HTTP端口 | 控制器基础端口 | UAV IDs | MAVLink端口范围
# ----+----------+----------------+---------+----------------
# 0   | 8081     | 5009           | 0,1     | 4560-4561, 14580-14581
# 1   | 8082     | 5109           | 2,3     | 4562-4563, 14582-14583
# 2   | 8083     | 5209           | 4,5     | 4564-4565, 14584-14585
# 3   | 8084     | 5309           | 6,7     | 4566-4567, 14586-14587
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/multi_env_configs"
LOG_DIR="/tmp/multi_env_sim_$(date +%Y%m%d_%H%M%S)"
ISAACSIM_PYTHON="${HOME}/isaacsim-5.1.0/python.sh"

# 环境数量（可配置：1-4）
NUM_ENVS=${1:-4}

echo "=========================================="
echo "多环境并行仿真启动器"
echo "=========================================="
echo "环境数量: ${NUM_ENVS}"
echo "日志目录: ${LOG_DIR}"
echo ""

# 创建日志目录
mkdir -p "${LOG_DIR}"

# 清理残留进程和端口
echo "[1/4] 清理残留进程..."
pkill -9 -f "mavlink_sim_vehicle" 2>/dev/null || true
pkill -9 -f "px4_sitl" 2>/dev/null || true
sleep 2

# 清理 PX4 锁文件
rm -rf /tmp/px4_instance_* /tmp/px4-* /tmp/px4_*.pid 2>/dev/null || true
rm -rf /tmp/pegasus_px4_sitl/*.pid 2>/dev/null || true

echo "[2/4] 启动仿真环境..."

# 启动各环境
declare -a ENV_PIDS
declare -a SIM_PORTS
declare -a CTRL_BASE_PORTS

for i in $(seq 0 $((NUM_ENVS - 1))); do
    CONFIG_FILE="${CONFIG_DIR}/env${i}_2uav.json"

    # 端口计算（每个环境间隔100）
    SIM_PORT=$((8081 + i))
    CTRL_BASE=$((5009 + i * 100))

    SIM_PORTS[$i]=$SIM_PORT
    CTRL_BASE_PORTS[$i]=$CTRL_BASE

    echo "  启动环境 ${i}: 配置=${CONFIG_FILE}, HTTP端口=${SIM_PORT}, 控制器端口=${CTRL_BASE}+id"

    # 检查配置文件存在
    if [ ! -f "${CONFIG_FILE}" ]; then
        echo "    错误: 配置文件不存在: ${CONFIG_FILE}"
        exit 1
    fi

    # 启动仿真（headless + no-images 模式减少资源消耗）
    nohup ${ISAACSIM_PYTHON} "${SCRIPT_DIR}/mavlink_sim_vehicle.py" \
        --config "${CONFIG_FILE}" \
        --headless \
        --no-images \
        --sim-port ${SIM_PORT} \
        --ctrl-base-port ${CTRL_BASE} \
        > "${LOG_DIR}/env${i}.log" 2>&1 &

    ENV_PIDS[$i]=$!
    echo "    PID: ${ENV_PIDS[$i]}"

    # 环境间启动间隔（避免GPU资源争用）
    if [ $i -lt $((NUM_ENVS - 1)) ]; then
        echo "    等待15秒后启动下一个环境..."
        sleep 15
    fi
done

echo ""
echo "[3/4] 等待所有环境就绪..."

# 等待所有环境的健康检查通过
MAX_WAIT=180  # 最长等待3分钟
READY_COUNT=0

for wait_time in $(seq 1 ${MAX_WAIT}); do
    READY_COUNT=0

    for i in $(seq 0 $((NUM_ENVS - 1))); do
        SIM_PORT=${SIM_PORTS[$i]}

        # 检查进程是否还在运行
        if ! kill -0 ${ENV_PIDS[$i]} 2>/dev/null; then
            echo "  警告: 环境 ${i} 进程已退出!"
            continue
        fi

        # 检查HTTP健康
        if curl -s --connect-timeout 2 "http://127.0.0.1:${SIM_PORT}/health" > /dev/null 2>&1; then
            ((READY_COUNT++))
        fi
    done

    echo "  [${wait_time}s] 就绪环境: ${READY_COUNT}/${NUM_ENVS}"

    if [ ${READY_COUNT} -eq ${NUM_ENVS} ]; then
        echo "  所有环境HTTP服务就绪!"
        break
    fi

    sleep 1
done

if [ ${READY_COUNT} -lt ${NUM_ENVS} ]; then
    echo "警告: 仅 ${READY_COUNT}/${NUM_ENVS} 环境就绪"
fi

echo ""
echo "[4/4] 等待PX4就绪..."

# 检查每个UAV的PX4状态
MAX_PX4_WAIT=120
ALL_READY=false

for wait_time in $(seq 1 ${MAX_PX4_WAIT}); do
    PX4_READY_COUNT=0
    TOTAL_UAVS=$((NUM_ENVS * 2))

    for i in $(seq 0 $((NUM_ENVS - 1))); do
        SIM_PORT=${SIM_PORTS[$i]}

        # 每个环境有2架飞机
        for uav_offset in 0 1; do
            UAV_ID=$((i * 2 + uav_offset))

            RESULT=$(curl -s --connect-timeout 2 "http://127.0.0.1:${SIM_PORT}/uav/${UAV_ID}/px4/ready" 2>/dev/null || echo '{"ready":false}')
            if echo "${RESULT}" | grep -q '"ready": true\|"ready":true'; then
                ((PX4_READY_COUNT++))
            fi
        done
    done

    echo "  [${wait_time}s] PX4就绪: ${PX4_READY_COUNT}/${TOTAL_UAVS}"

    if [ ${PX4_READY_COUNT} -eq ${TOTAL_UAVS} ]; then
        ALL_READY=true
        echo "  所有PX4实例就绪!"
        break
    fi

    sleep 1
done

echo ""
echo "=========================================="
echo "仿真环境状态摘要"
echo "=========================================="
echo "日志目录: ${LOG_DIR}"
echo ""

for i in $(seq 0 $((NUM_ENVS - 1))); do
    echo "环境 ${i}:"
    echo "  PID: ${ENV_PIDS[$i]}"
    echo "  HTTP端口: ${SIM_PORTS[$i]}"
    echo "  控制器端口: ${CTRL_BASE_PORTS[$i]}+UAV_ID"
    echo "  UAV IDs: $((i*2)), $((i*2+1))"
    echo "  日志: ${LOG_DIR}/env${i}.log"
    echo ""
done

if [ "$ALL_READY" = true ]; then
    echo "状态: 所有环境和PX4就绪 ✓"
    echo ""
    echo "可以开始数据采集了！"
else
    echo "状态: 部分环境未就绪 ⚠"
    echo "请检查日志文件排查问题"
fi

echo ""
echo "停止所有环境: pkill -9 -f mavlink_sim_vehicle"
