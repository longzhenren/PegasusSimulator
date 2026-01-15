# PegasusSimulator 轨迹采集指南

## 概述

本文档记录了轨迹采集系统的所有修改、问题修复和启动命令。

---

## 问题修复记录

### 1. PX4 Recovery 失败问题

**问题描述**: UAV在多次轨迹采集后，PX4恢复失败，无人机卡在地面（0.06m高度）。

**根因**: HTTP端点 `/uav/<id>/px4/recover` 使用了简化的手动恢复序列，缺少关键状态重置。

**修复文件**: `examples/mavlink_sim_vehicle.py`

**修复位置**: 行 1595-1643

**修改内容**:

```python
# 修改前（有问题）:
@app.route('/uav/<int:uav_id>/px4/recover', methods=['POST'])
def px4_recover(uav_id: int):
    # 手动执行恢复步骤（缺少状态重置）
    backend._kill_px4()
    backend.re_initialize_interface()
    backend.launch_px4()
    # 缺少: _received_first_hearbeat = False
    # 缺少: _current_utime = 0
    # 缺少: _skip_large_dt_count = 10

# 修改后（正确）:
@app.route('/uav/<int:uav_id>/px4/recover', methods=['POST'])
def px4_recover(uav_id: int):
    """重启PX4进程（硬重置）

    使用后端的 recover_px4() 方法进行完整的恢复序列，包括：
    - 正确的状态重置（heartbeat标志、时间戳等）
    - MAVLink TCP服务器必须在PX4启动之前创建
    - 处理物理循环阻塞期间的大dt值
    """
    vehicle = self._get_vehicle(uav_id)
    if vehicle is None:
        return jsonify({"status": "error", "message": "UAV not found"}), 404

    backend = self.manager.px4_backends.get(uav_id)
    if backend is None:
        return jsonify({"status": "error", "message": "Backend not found"}), 404

    try:
        ts_log(f"[UAV{uav_id}]", "PX4 recover: calling backend.recover_px4()...")

        # 使用后端的完整恢复方法
        backend.recover_px4()

        # 等待PX4就绪
        ts_log(f"[UAV{uav_id}]", "PX4 recover: waiting for PX4 ready...")
        ready_timeout = 30.0
        ready_start = time.time()
        while time.time() - ready_start < ready_timeout:
            if backend.px4_ready_to_takeoff:
                ts_log(f"[UAV{uav_id}]", "PX4 recover: PX4 is ready!")
                break
            time.sleep(0.5)

        return jsonify({
            "status": "success",
            "uav_id": uav_id,
            "ready": backend.px4_ready_to_takeoff,
            "heartbeat_received": backend._received_first_hearbeat
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
```

---

### 2. 测试数据Z轴单位问题

**问题描述**: 测试数据中无人机几乎不移动，z轴控制输入高度只有0.03m（3厘米）。

**根因**: 测试数据 (`test_collection_input/*.json`) 使用了错误的z坐标单位。

**数据分析**:

| 数据类型 | raw_logs[0].z | scale=0.01后 | 状态 |
|---------|--------------|-------------|------|
| 测试数据 | 3.0 | 0.03m (3cm) | 错误：地面高度 |
| 真实数据 | 246.646 | 2.47m | 正确：飞行高度 |

**解决方案**: 使用真实轨迹数据进行采集。

**真实数据位置**:
- 压缩包: `json.tar`
- 解压后: `examples/real_trajectory_data/train_data/extracted_json_files/`
- 文件数量: 16,771 个轨迹

---

## 数据格式说明

### 轨迹JSON格式

```json
{
  "id": "2025-05-02_11-54-18",
  "raw_logs": [
    [x, y, z, roll, yaw, pitch],  // 绝对坐标（厘米）
    ...
  ],
  "preprocessed_logs": [
    [x, y, z, roll, yaw, pitch],  // 相对坐标（从原点开始）
    ...
  ]
}
```

### 坐标转换

```
最终z高度 = (raw_logs[0].z + preprocessed_logs[i].z) * scale
         = (初始高度 + 相对变化) * 0.01
         = 米
```

---

## 启动命令

### 步骤1: 解压真实轨迹数据

```bash
cd /home/user/PegasusSimulator-5.1
mkdir -p examples/real_trajectory_data
tar -xf json.tar -C examples/real_trajectory_data --strip-components=5
```

验证:
```bash
find examples/real_trajectory_data -name "*.json" | wc -l
# 应该显示约 16771
```

### 步骤2: 启动仿真环境

```bash
# 终端1: 启动Isaac Sim仿真
cd /home/user/PegasusSimulator-5.1/examples
python3 mavlink_sim_vehicle.py --config multi_uav_config_8.json
```

等待输出显示:
```
[INFO] HTTP server started on 0.0.0.0:5009
[INFO] All UAVs initialized
```

### 步骤3: 运行轨迹采集

```bash
# 终端2: 运行采集器
cd /home/user/PegasusSimulator-5.1/examples

# 测试采集（10条轨迹）
python3 -c "
import sys
sys.argv = [
    'mavlink_trajectory_collector.py',
    '--input-dir', 'real_trajectory_data/train_data/extracted_json_files',
    '--out-dir', '/tmp/trajectory_output',
    '--config', 'multi_uav_config_8.json',
    '--scale', '0.01',
    '--max-trajs', '10'
]
from mavlink_trajectory_collector import main
main()
"

# 完整采集（全部轨迹）
python3 -c "
import sys
sys.argv = [
    'mavlink_trajectory_collector.py',
    '--input-dir', 'real_trajectory_data/train_data/extracted_json_files',
    '--out-dir', '/data/trajectory_collection',
    '--config', 'multi_uav_config_8.json',
    '--scale', '0.01'
]
from mavlink_trajectory_collector import main
main()
"
```

### 步骤4: 监控采集进度

```bash
# 实时监控日志
tail -f /tmp/trajectory_output/mission_status.csv

# 检查输出目录
ls -la /tmp/trajectory_output/
```

---

## 配置文件说明

### multi_uav_config_8.json (8架UAV并行)

```json
{
  "vehicles": [
    {"vehicle_id": 0},
    {"vehicle_id": 1},
    ...
    {"vehicle_id": 7}
  ]
}
```

### multi_uav_config_1.json (单架UAV测试)

```json
{
  "vehicles": [
    {"vehicle_id": 0}
  ]
}
```

---

## 常用命令

### 清理僵尸进程

```bash
# 杀死所有PX4进程
pkill -9 -f px4
pkill -9 -f "PX4"

# 杀死仿真进程
pkill -9 -f mavlink_sim_vehicle
pkill -9 -f isaac
```

### 检查端口占用

```bash
# MAVLink控制端口
lsof -i :5009

# HTTP服务端口
lsof -i :8081

# PX4 MAVLink TCP端口
lsof -i :4560  # UAV0
lsof -i :4561  # UAV1
# ...
```

### 验证仿真状态

```bash
# 检查UAV状态
curl http://localhost:5009/uav/0/info

# 检查PX4状态
curl http://localhost:5009/uav/0/px4/status
```

---

## 输出数据格式

### mission_status.csv

```csv
traj_name,uav_id,timestamp,status
2025-05-02_11-54-18_log,0,1768266189.777,success
2025-03-27_15-44-50_log,1,1768266213.868,success
...
```

### data.csv (每条轨迹)

包含字段:
- `traj_json`: 源轨迹文件路径
- `cmd_in_x/y/z`: 输入命令坐标
- `obs_pos_x/y/z`: 观测位置
- `obs_aligned_x/y/z`: 对齐后位置
- `origin_offset_x/y/z`: 原点偏移
- `obs_att_w/x/y/z`: 姿态四元数
- `obs_linvel_x/y/z`: 线速度
- `obs_angvel_x/y/z`: 角速度
- `obs_linacc_x/y/z`: 线加速度

---

## 修改文件汇总

| 文件 | 行号 | 修改类型 | 说明 |
|-----|------|---------|------|
| `examples/mavlink_sim_vehicle.py` | 1595-1643 | 代码修改 | PX4 recover使用backend.recover_px4() |

---

## 测试验证结果

### 单架UAV测试 (2026-01-13)

使用真实轨迹数据 `2025-05-02_11-54-18_log.json` 测试：

| 指标 | 值 |
|-----|-----|
| 目标高度 | 2.47m |
| 实际爬升 | 0.06m → 2.47m |
| 采样点数 | 30 |
| 图像数量 | 30张 |
| 平均跟踪误差 | 2.08m |
| 最终位置误差 | 0.128m |

结论：**z轴问题已修复**，真实数据的飞行高度正确。

---

## 注意事项

1. **Scale参数**: 真实数据使用 `--scale 0.01`（厘米转米）
2. **并行数量**: 建议使用8架UAV并行，平衡性能和稳定性
3. **重置超时**: 默认60秒，复杂场景可增加 `--reset-timeout 90`
4. **磁盘空间**: 完整采集约需 50GB+ 空间（包含图像）
5. **真实数据位置**: `examples/real_trajectory_data/train_data/extracted_json_files/` (16,771条轨迹)
