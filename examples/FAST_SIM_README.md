# 纯Python仿真环境使用说明

## 概述

本目录包含一套完整的纯Python仿真环境，使用 `FastController` 控制器替代 PX4-Autopilot，实现更快速、更轻量的无人机仿真和数据采集。

## 文件组成

| 文件 | 功能 | 对应原系统文件 |
|------|------|----------------|
| `fast_sim_vehicle.py` | 仿真主程序 | `8_camera_vehicle.py` |
| `fast_trajectory_collector.py` | 数据采集脚本 | `trajectory_data_collector.py` |
| `utils/fast_controller.py` | 姿态控制器 | PX4 飞控 |

## 与原系统的区别

| 特性 | 原系统 (PX4) | 纯Python系统 |
|------|-------------|--------------|
| 控制后端 | PX4 SITL + MAVLink | FastController |
| 依赖项 | PX4-Autopilot, MAVROS, ROS2 | 无外部依赖 |
| 启动时间 | ~30秒 (含PX4启动) | ~5秒 |
| 资源消耗 | 高 (多进程) | 低 (单进程) |
| HTTP仿真端口 | 8081 | 8081 (兼容) |
| HTTP控制端口 | 5009+id (需rospy_isaacsim.py) | 5009+id (内置) |
| 输入格式 | JSON轨迹文件 | JSON轨迹文件 (相同) |
| 输出格式 | CSV + PNG + ULG | CSV + PNG (相同，无ULG) |

## 使用方法

### 1. 启动仿真环境

```bash
# 使用默认配置启动
ISAACSIM_PYTHON examples/fast_sim_vehicle.py

# 使用自定义配置
ISAACSIM_PYTHON examples/fast_sim_vehicle.py --config examples/multi_uav_config.json

# 无头模式运行
ISAACSIM_PYTHON examples/fast_sim_vehicle.py --headless
```

### 2. 执行数据采集

在仿真环境启动后，打开另一个终端执行数据采集：

```bash
# 基础采集（单机）
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --uav-ids 0

# 多机并行采集
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --config examples/multi_uav_config.json

# 指定坐标缩放和轴向
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --scale 0.01 \
  --z-down \
  --max-points 100
```

## 命令行参数

### fast_sim_vehicle.py

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--config` | `multi_uav_config.json` | UAV配置文件路径 |
| `--trajectory` | 无 | 轨迹文件路径（可选，用于自动执行） |
| `--headless` | False | 无头模式运行 |
| `--sim-port` | 8081 | 仿真HTTP端口 |
| `--ctrl-base-port` | 5009 | 控制器HTTP端口基础 |
| `--scale` | 0.01 | 坐标缩放因子 |
| `--z-down` | True | Z轴向下 |

### fast_trajectory_collector.py

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--input-dir` | 必需 | 轨迹JSON文件目录 |
| `--pattern` | `*.json` | JSON文件匹配模式 |
| `--out-dir` | `./recordings` | 输出目录 |
| `--config` | `multi_uav_config.json` | UAV配置文件路径 |
| `--uav-ids` | 从配置读取 | UAV ID列表（逗号分隔） |
| `--control-base` | `http://127.0.0.1:5009` | 控制器基础URL |
| `--image-base` | `http://127.0.0.1:8081` | 图像服务基础URL |
| `--scale` | 0.01 | 坐标缩放因子 |
| `--max-points` | 0 (不限制) | 最大轨迹点数 |
| `--z-down` / `--z-up` | `--z-down` | Z轴方向 |
| `--reset-timeout` | 120.0 | 重置超时（秒） |
| `--cmd-timeout` | 60.0 | 命令超时（秒） |
| `--skip-existing` | True | 跳过已存在的轨迹 |
| `--dry-run` | False | 仅扫描文件，不执行采集 |

## HTTP API 接口

### 仿真端 (端口 8081)

与 `8_camera_vehicle.py` 完全兼容：

```bash
# 获取位姿
curl http://127.0.0.1:8081/uav/0/pose

# 获取图像（JSON + Base64）
curl http://127.0.0.1:8081/uav/0/image

# 获取图像（PNG二进制）
curl -o image.png http://127.0.0.1:8081/uav/0/image.png

# 获取图像+位姿同步快照
curl http://127.0.0.1:8081/uav/0/all

# 重置UAV位置
curl -X POST http://127.0.0.1:8081/uav/0/reset \
  -H "Content-Type: application/json" \
  -d '{"position":[0,0,2],"yaw_deg":0}'

# 查询就绪状态
curl http://127.0.0.1:8081/uav/0/px4/ready
```

### 控制端 (端口 5009+id)

与 `rospy_isaacsim.py` 完全兼容：

```bash
# 健康检查
curl http://127.0.0.1:5009/health

# 重置UAV
curl -X POST http://127.0.0.1:5009/reset \
  -H "Content-Type: application/json" \
  -d '{"position":[0,0,2],"hard":true,"force":true}'

# 移动到指定位置
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"move_to","x":1.0,"y":2.0,"z":3.0,"force":true}'

# 降落
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"land","force":true}'

# 获取状态
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"get_status"}'
```

## 输入输出格式

### 输入: JSON轨迹文件

与 `trajectory_data_collector.py` 完全相同：

```json
{
  "raw_logs": [
    [x, y, z, roll, yaw, pitch]  // 初始位置（ENU坐标系）
  ],
  "preprocessed_logs": [
    [x, y, z, roll, yaw, pitch],  // 轨迹点1
    [x, y, z, roll, yaw, pitch],  // 轨迹点2
    ...
  ]
}
```

### 输出: 目录结构

与 `trajectory_data_collector.py` 完全相同：

```
<out_dir>/
  ├── mission_status.csv           # 全局任务状态日志
  └── <traj_name>/
      └── uav<id>/
          ├── data.csv             # 主数据文件
          ├── all_pose_data.csv    # 简化位姿数据
          └── images/
              ├── img_000000_<ts_ms>.png
              ├── img_000001_<ts_ms>.png
              └── ...
```

### 输出: CSV字段

#### data.csv

| 字段 | 说明 |
|------|------|
| `traj_json` | 源JSON文件路径 |
| `traj_name` | 轨迹名称 |
| `uav_id` | UAV ID |
| `step_idx` | 步骤索引 |
| `cmd_in_x/y/z` | 输入坐标（变换前） |
| `cmd_in_roll/yaw/pitch_deg` | 输入姿态角 |
| `cmd_x/y/z` | 命令坐标（变换后） |
| `cmd_roll/yaw/pitch_deg` | 命令姿态角 |
| `image_timestamp_s` | 图像时间戳 |
| `image_path` | 图像文件路径 |
| `obs_pos_x/y/z` | 观测位置 |
| `obs_aligned_x/y/z` | 对齐后的观测位置 |
| `origin_offset_x/y/z` | 坐标系偏移量 |
| `obs_att_w/x/y/z` | 观测四元数姿态 |
| `obs_linvel_x/y/z` | 观测线速度 |
| `obs_angvel_x/y/z` | 观测角速度 |
| `obs_linacc_x/y/z` | 观测线加速度 |

#### all_pose_data.csv

| 字段 | 说明 |
|------|------|
| `traj_json`, `traj_name`, `uav_id`, `step_idx` | 基本信息 |
| `image_timestamp_s` | 图像时间戳 |
| `pos_x/y/z` | 原始位置 |
| `aligned_pos_x/y/z` | 对齐后位置 |
| `cmd_roll/yaw/pitch_deg` | 命令姿态 |
| `att_w/x/y/z` | 姿态四元数 |
| `linvel_x/y/z` | 线速度 |
| `angvel_x/y/z` | 角速度 |
| `linacc_x/y/z` | 线加速度 |

## FastController 控制参数

控制器使用级联PID架构，默认参数针对10kg无人机优化：

### 位置环（PID）
- Kp = [10.0, 10.0, 10.0]
- Kd = [8.5, 8.5, 8.5]
- Ki = [1.50, 1.50, 1.50]

### 姿态环（几何+PD）
- KAng = [15.0, 15.0, 12.0]
- Kdang = [0.0, 0.0, 0.0]

### 角速度环（PID+积分抗饱和）
- KAng_vel = [0.2, 0.15, 0.32]
- KiAng_vel = [0.2, 0.2, 0.1]
- KiAng_vel_max = [1.0, 0.7, 0.5]

### 安全约束
- max_roll_deg = 45.0°
- max_pitch_deg = 45.0°
- max_yaw_rate_deg = 90.0°/s

## 完整使用示例

### 示例1: 单机轨迹采集

```bash
# 终端1: 启动仿真
ISAACSIM_PYTHON examples/fast_sim_vehicle.py

# 终端2: 执行采集
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --uav-ids 0 \
  --scale 0.01 \
  --z-down
```

### 示例2: 多机并行采集

```bash
# 终端1: 启动仿真（多机配置）
ISAACSIM_PYTHON examples/fast_sim_vehicle.py \
  --config examples/multi_uav_config.json

# 终端2: 执行采集
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --config examples/multi_uav_config.json \
  --out-dir ~/recordings
```

### 示例3: 与原系统混合使用

由于HTTP接口完全兼容，可以：

1. 使用 `fast_sim_vehicle.py` 仿真 + `trajectory_data_collector.py` 采集
2. 使用 `8_camera_vehicle.py` 仿真 + `fast_trajectory_collector.py` 采集（需要 rospy_isaacsim.py）

## 常见问题

### Q: 为什么没有ULG日志？

A: 纯Python系统不使用PX4，因此没有PX4日志。如需飞行日志，可使用原系统或通过CSV数据进行分析。

### Q: 如何调整控制器参数？

A: 修改 `utils/fast_controller.py` 中的参数，或在创建控制器时传入自定义参数。

### Q: 与原系统的数据兼容吗？

A: 是的，输入的JSON轨迹格式和输出的CSV格式与原系统完全相同，可以直接互换使用。

### Q: 如何切换回原系统？

A: 使用原来的启动命令即可：
```bash
python launch_multi_rospy.py --config multi_uav_config.json
python trajectory_data_collector.py --input-dir ~/trajectories
```

## 参考文档

- [FastController 使用说明](utils/FAST_CONTROLLER_README.md)
- [trajectory_data_collector.py 文档](trajectory_data_collector.py) (文件头注释)
- [8_camera_vehicle.py 文档](8_camera_vehicle.py) (文件头注释)
