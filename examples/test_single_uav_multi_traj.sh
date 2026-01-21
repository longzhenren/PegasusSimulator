#!/bin/bash
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# 单机多轨迹连续测试 - 验证 kill→teleport→start 流程

TRAJ_DIR="/home/user/PegasusSimulator-5.1/examples/real_trajectory_data/train_data/extracted_json_files"
OUT_DIR="/tmp/single_uav_multi_traj_test_$(date +%Y%m%d_%H%M%S)"
LOG_FILE="/tmp/single_uav_multi_traj_test.log"

mkdir -p "$OUT_DIR"

# 获取前5条轨迹 (使用find避免参数列表过长)
TRAJ_FILES=($(find "$TRAJ_DIR" -name "*.json" -type f 2>/dev/null | sort | head -5))
TOTAL=${#TRAJ_FILES[@]}

echo "========================================" | tee "$LOG_FILE"
echo "单机多轨迹连续测试" | tee -a "$LOG_FILE"
echo "轨迹数量: $TOTAL" | tee -a "$LOG_FILE"
echo "输出目录: $OUT_DIR" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# 等待仿真器和控制器完全就绪
echo "" | tee -a "$LOG_FILE"
echo "等待 120 秒让仿真器和控制器完全就绪..." | tee -a "$LOG_FILE"
for i in {120..1}; do
    printf "\r倒计时: %3d 秒" $i
    sleep 1
done
echo ""
echo "等待完成，开始测试" | tee -a "$LOG_FILE"

SUCCESS=0
FAILED=0

for i in "${!TRAJ_FILES[@]}"; do
    TRAJ="${TRAJ_FILES[$i]}"
    TRAJ_NAME=$(basename "$TRAJ" .json)

    echo "" | tee -a "$LOG_FILE"
    echo "[$((i+1))/$TOTAL] 处理: $TRAJ_NAME" | tee -a "$LOG_FILE"
    echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_FILE"

    # 运行采集器
    cd /home/user/PegasusSimulator-5.1/examples
    timeout 180 python3 simple_trajectory_collector.py \
        --uav-id 0 \
        --json-file "$TRAJ" \
        --out-dir "$OUT_DIR" \
        --scale 0.01 \
        --time-scale 1.0 \
        --control-base "http://127.0.0.1:5009" \
        --image-base "http://127.0.0.1:8081" \
        2>&1 | tee -a "$LOG_FILE"

    EXIT_CODE=${PIPESTATUS[0]}

    if [ $EXIT_CODE -eq 0 ]; then
        # 检查输出目录是否有数据
        if [ -f "$OUT_DIR/$TRAJ_NAME/data.csv" ]; then
            ROWS=$(wc -l < "$OUT_DIR/$TRAJ_NAME/data.csv")
            echo "✓ 成功: $TRAJ_NAME (rows=$ROWS)" | tee -a "$LOG_FILE"
            ((SUCCESS++))
        else
            echo "✗ 失败: $TRAJ_NAME (无data.csv)" | tee -a "$LOG_FILE"
            ((FAILED++))
        fi
    else
        echo "✗ 失败: $TRAJ_NAME (exit=$EXIT_CODE)" | tee -a "$LOG_FILE"
        ((FAILED++))
    fi

    echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_FILE"

    # 轨迹间隔 - 等待系统稳定
    if [ $i -lt $((TOTAL-1)) ]; then
        echo "等待3秒后继续下一条轨迹..." | tee -a "$LOG_FILE"
        sleep 3
    fi
done

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "测试完成!" | tee -a "$LOG_FILE"
echo "成功: $SUCCESS / $TOTAL" | tee -a "$LOG_FILE"
echo "失败: $FAILED / $TOTAL" | tee -a "$LOG_FILE"
echo "输出目录: $OUT_DIR" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# 输出结果文件列表
echo "" | tee -a "$LOG_FILE"
echo "输出文件:" | tee -a "$LOG_FILE"
ls -la "$OUT_DIR"/*/ 2>/dev/null | head -30 | tee -a "$LOG_FILE"
