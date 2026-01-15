# MAVLink直接控制仿真环境使用说明

## 概述

本目录包含一套基于MAVLink直接控制的仿真环境，使用PX4飞控但**不使用MAVROS**，直接通过MAVLink协议进行控制。

## 文件组成

| 文件 | 功能 | 对应原系统文件 |
|------|------|----------------|
| `mavlink_sim_vehicle.py` | 仿真主程序 | `8_camera_vehicle.py` |
| `mavlink_trajectory_collector.py` | 数据采集脚本 | `trajectory_data_collector.py` |

## 系统架构对比

### 原系统 (PX4 + MAVROS)
```
trajectory_data_collector.py
         │
         ├── MAVROS (ROS2) ──► rospy_isaacsim.py (HTTP :5009+id)
         │                              │
         │                              ▼
         └── HTTP ──────────► 8_camera_vehicle.py (HTTP :8081)
                                        │
                                        ▼
                              PX4 SITL (MAVLink lockstep)
```

### 新系统 (MAVLink直接控制)
```
mavlink_trajectory_collector.py
         │
         └── HTTP ──────────► mavlink_sim_vehicle.py (HTTP :8081)
                                   │
                                   ├── MAVLinkController (HTTP :5009+id)
                                   │        │
                                   │        └── MAVLink ──► PX4 SITL
                                   │
                                   └── PX4MavlinkBackend (lockstep)
```

## 与原系统的区别

| 特性 | 原系统 (PX4+MAVROS) | MAVLink直接控制系统 |
|------|---------------------|---------------------|
| 控制后端 | PX4 SITL + MAVROS + ROS2 | PX4 SITL + MAVLink直接 |
| 依赖项 | PX4-Autopilot, MAVROS, ROS2 | PX4-Autopilot, pymavlink |
| 启动时间 | ~30秒 (含ROS2+MAVROS) | ~15秒 |
| 资源消耗 | 高 (多进程ROS2节点) | 中 |
| HTTP仿真端口 | 8081 | 8081 (兼容) |
| HTTP控制端口 | 5009+id (rospy_isaacsim.py) | 5009+id (内置) |
| 控制方式 | MAVROS服务调用 | MAVLink PVA控制 |
| 输入格式 | JSON轨迹文件 | JSON轨迹文件 (相同) |
| 输出格式 | CSV + PNG + ULG | CSV + PNG + ULG (相同) |
| 控制精度 | 高（真实飞控） | 高+（PVA前馈） |

## 新增特性 ⭐

### PVA前馈控制 (Position + Velocity + Acceleration)

相比原系统的位置+速度控制，MAVLink系统新增**加速度前馈**：
- **P**: 位置目标
- **V**: 速度前馈，提高轨迹跟踪响应速度
- **A**: 加速度前馈，进一步减少跟踪误差

### 高动态参数自动配置

MAVLink控制器在建立连接时自动配置PX4高动态飞行参数：

| 参数名 | 值 | 说明 |
|--------|-----|------|
| `MPC_TILTMAX_AIR` | 60° | 空中最大倾角（默认35°→60°） |
| `MPC_XY_P` | 1.5 | 水平位置P增益 |
| `MPC_XY_VEL_P_ACC` | 3.0 | 速度到加速度增益 |
| `MPC_Z_P` | 1.5 | 垂直位置P增益 |
| `MPC_Z_VEL_P_ACC` | 6.0 | Z轴速度到加速度增益 |
| `MPC_THR_HOVER` | 0.55 | 悬停油门（补偿大倾角） |
| `MPC_JERK_AUTO` | 20.0 | 最大jerk (m/s³) |
| `MPC_XY_VEL_MAX` | 12.0 | 最大水平速度 (m/s) |

## 优势

1. **无需MAVROS/ROS2**: 减少系统依赖和配置复杂度
2. **更快启动**: 省去ROS2节点启动时间
3. **更低资源占用**: 单进程架构
4. **完全兼容**: HTTP接口与原系统完全相同
5. **真实飞控**: 保留PX4的完整飞行控制能力
6. **ULG日志**: 保留PX4飞行日志用于分析

## 使用方法

### 1. 启动仿真环境

```bash
# 使用默认配置启动
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py

# 使用自定义配置
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py --config examples/multi_uav_config.json

# 无头模式运行
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py --headless
```

### 2. 执行数据采集

在仿真环境启动后，打开另一个终端执行数据采集：

```bash
# 基础采集（单机）
python examples/mavlink_trajectory_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --uav-ids 0

# 多机并行采集
python examples/mavlink_trajectory_collector.py \
  --input-dir ~/trajectories \
  --config examples/multi_uav_config.json

# 使用Mission模式（一次性上传所有航点）
python examples/mavlink_trajectory_collector.py \
  --input-dir ~/trajectories \
  --use-mission
```

## 命令行参数

### mavlink_sim_vehicle.py

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--config` | `multi_uav_config.json` | UAV配置文件路径 |
| `--headless` | False | 无头模式运行 |
| `--sim-port` | 8081 | 仿真HTTP端口 |
| `--ctrl-base-port` | 5009 | 控制器HTTP端口基础 |

### mavlink_trajectory_collector.py

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
| `--use-mission` | False | 使用Mission模式 |
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

# 查询PX4就绪状态
curl http://127.0.0.1:8081/uav/0/px4/ready

# 查询PX4详细状态
curl http://127.0.0.1:8081/uav/0/px4/status
```

### 控制端 (端口 5009+id)

与 `rospy_isaacsim.py` 兼容：

```bash
# 健康检查
curl http://127.0.0.1:5009/health

# 重置UAV
curl -X POST http://127.0.0.1:5009/reset \
  -H "Content-Type: application/json" \
  -d '{"position":[0,0,2],"hard":true,"force":true}'

# 移动到指定位置（OFFBOARD模式）
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"move_to","x":1.0,"y":2.0,"z":3.0,"force":true}'

# 执行任务（Mission模式）
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"execute_mission","waypoints":[{"x":1,"y":0,"z":2},{"x":2,"y":1,"z":2}]}'

# 降落
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"land","force":true}'

# 解锁
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"arm"}'

# 设置飞行模式
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"set_mode","mode":"AUTO.MISSION"}'

# 获取状态
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"get_status"}'

# ⭐ PVA setpoint (位置+速度+加速度前馈)
curl -X POST http://127.0.0.1:5009/command \
  -H "Content-Type: application/json" \
  -d '{
    "cmd":"setpoint",
    "x":1.0, "y":2.0, "z":5.0,
    "vx":0.5, "vy":0.0, "vz":0.0,
    "afx":0.1, "afy":0.0, "afz":0.0,
    "yaw":0.0, "yaw_rate":0.0
  }'
```

## 控制模式

### OFFBOARD PVA轨迹跟踪模式（默认，推荐）

基于三次样条插值的50Hz高频PVA控制 + 5Hz数据采样：

```
[5Hz人工轨迹数据] → [Cubic Spline插值] → [50Hz PVA前馈控制] → [5Hz同步采样]
                                              ↓
                                    位置+速度+加速度前馈
```

#### 控制架构详解

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        mavlink_trajectory_collector.py                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────┐    ┌──────────────────────────────────────────┐  │
│  │ JSON轨迹文件         │    │ TrajectorySmoother (三次样条插值器)       │  │
│  │ preprocessed_logs    │───►│                                          │  │
│  │ (5Hz, 0.2s间隔)      │    │  • CubicSpline 对 x,y,z,roll,pitch,yaw   │  │
│  │ 单位: cm             │    │  • get_full_state(t) → 位置+速度+加速度  │  │
│  └──────────────────────┘    │  • 一阶导数(速度)+二阶导数(加速度)       │  │
│                              │  • 角度自动unwrap处理                    │  │
│                              └──────────────────────────────────────────┘  │
│                                           │                                  │
│                                           ▼                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                      50Hz 控制循环 (control_dt = 0.02s)               │  │
│  │  ┌────────────────────────────────────────────────────────────────┐  │  │
│  │  │  while elapsed < total_duration:                               │  │  │
│  │  │      t = elapsed                                               │  │  │
│  │  │      state = smoother.get_full_state(t, for_control=True)      │  │  │
│  │  │                                                                │  │  │
│  │  │      # 发送 SET_POSITION_TARGET_LOCAL_NED (PVA)               │  │  │
│  │  │      cmd = {                                                   │  │  │
│  │  │          "cmd": "setpoint",                                    │  │  │
│  │  │          "x": state.x, "y": state.y, "z": state.z,            │  │  │
│  │  │          "vx": state.vx, "vy": state.vy, "vz": state.vz,      │  │  │
│  │  │          "afx": state.ax, "afy": state.ay, "afz": state.az,   │  │  │
│  │  │          "yaw": state.yaw, "yaw_rate": state.yaw_rate         │  │  │
│  │  │      }                                                         │  │  │
│  │  │      HTTP POST → MAVLinkController → PX4                       │  │  │
│  │  │                                                                │  │  │
│  │  │      # 5Hz采样 (每10个控制循环采样一次)                         │  │  │
│  │  │      if elapsed >= next_sample_time:                           │  │  │
│  │  │          sample_state = smoother.get_full_state(sample_t,      │  │  │
│  │  │                                                for_control=False)│  │  │
│  │  │          collect_image_and_pose()                              │  │  │
│  │  │          save_to_csv()                                         │  │  │
│  │  │                                                                │  │  │
│  │  │      sleep(control_dt - loop_elapsed)  # 维持50Hz节拍          │  │  │
│  │  └────────────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                           │                                  │
│                                           ▼                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                      MAVLinkController (HTTP :5009+id)                │  │
│  │  • 接收setpoint命令 (PVA: 位置+速度+加速度)                          │  │
│  │  • 转换为MAVLink SET_POSITION_TARGET_LOCAL_NED消息                   │  │
│  │  • type_mask = 0x0000 (使用全部PVA字段)                              │  │
│  │  • 自动配置高动态PX4参数                                             │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                           │                                  │
│                                           ▼                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                           PX4 SITL (lockstep)                         │  │
│  │  • 接收OFFBOARD PVA setpoints                                        │  │
│  │  • 内部位置+速度+加速度控制器处理                                    │  │
│  │  • 输出电机指令                                                      │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| CONTROL_HZ | 50 Hz | 控制循环频率，发送setpoint到PX4 |
| SAMPLE_HZ | 5 Hz | 数据采样频率，与原始轨迹频率一致 |
| control_dt | 0.02 s | 控制周期 |
| sample_dt | 0.2 s | 采样周期 |
| 插值方法 | CubicSpline | 三次样条，bc_type='natural' |
| type_mask | 0x0000 | 使用全部PVA+yaw字段 |

#### PVA前馈设计

```python
# 控制输入 (for_control=True): 使用插值速度和加速度进行前馈
state = smoother.get_full_state(t, for_control=True)
# → vx, vy, vz 来自样条一阶导数，提供速度前馈
# → ax, ay, az 来自样条二阶导数，提供加速度前馈

# 数据记录 (for_control=False): 最终航点速度/加速度为0
state = smoother.get_full_state(t, for_control=False)
# → 中间航点: vx, vy, vz, ax, ay, az 来自样条导数
# → 最终航点: vx=vy=vz=ax=ay=az=0 (表示悬停目标)
```

#### 坐标系统

1. **输入坐标**: JSON文件中的 `preprocessed_logs` 是相对坐标（起点为0,0），单位为厘米
2. **坐标变换**: `--scale 0.01` 将厘米转换为米
3. **高度处理**: Z轴使用 `raw_logs[0]` 的绝对高度 + 相对变化量
4. **重置位置**: UAV重置到原点 `[0, 0, init_z * scale]`，与相对坐标系对齐

### Mission模式（--use-mission）

一次性上传所有航点到PX4，使用PX4的Mission模式执行。

优点：
- 更流畅的飞行轨迹
- PX4自动处理航点间过渡
- 减少通信开销

缺点：
- 无法精确控制采样时刻
- 航点间插值由PX4内部处理

## 输入输出格式

### 输入: JSON轨迹文件

与原系统完全相同：

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

与原系统完全相同：

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

## 完整使用示例

### 示例1: 单机轨迹采集

```bash
# 终端1: 启动仿真
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py

# 终端2: 执行采集
python examples/mavlink_trajectory_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --uav-ids 0 \
  --scale 0.01 \
  --z-down
```

### 示例2: 多机并行采集

```bash
# 终端1: 启动仿真（多机配置）
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py \
  --config examples/multi_uav_config.json

# 终端2: 执行采集
python examples/mavlink_trajectory_collector.py \
  --input-dir ~/trajectories \
  --config examples/multi_uav_config.json \
  --out-dir ~/recordings
```

### 示例3: 使用Mission模式

```bash
# 终端1: 启动仿真
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py

# 终端2: 使用Mission模式采集
python examples/mavlink_trajectory_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --use-mission
```

### 示例4: 与原系统混合使用

由于HTTP接口完全兼容，可以：

1. 使用 `mavlink_sim_vehicle.py` 仿真 + `trajectory_data_collector.py` 采集（需要rospy_isaacsim.py）
2. 使用 `8_camera_vehicle.py` 仿真 + `mavlink_trajectory_collector.py` 采集

## MAVLink控制原理

### 连接方式

MAVLinkController通过UDP连接到PX4的MAVLink端口（14550+vehicle_id），发送控制命令：

```python
# 连接建立
mavlink_conn = mavutil.mavlink_connection(f"udpout:127.0.0.1:{14550+uav_id}")

# 等待心跳
mavlink_conn.wait_heartbeat()

# 发送命令
mavlink_conn.mav.command_long_send(...)
```

### Mission模式协议

1. 清除现有任务: `MISSION_CLEAR_ALL`
2. 发送任务数量: `MISSION_COUNT`
3. 等待请求并发送航点: `MISSION_ITEM`
4. 确认上传: `MISSION_ACK`
5. 开始任务: `MAV_CMD_MISSION_START`

### 支持的PX4模式

| 模式 | 说明 |
|------|------|
| `MANUAL` | 手动模式 |
| `STABILIZED` | 稳定模式 |
| `OFFBOARD` | 外部控制模式 |
| `AUTO.MISSION` | 任务模式 |
| `AUTO.LOITER` | 悬停模式 |
| `AUTO.RTL` | 返航模式 |
| `AUTO.LAND` | 降落模式 |
| `AUTO.TAKEOFF` | 起飞模式 |

## 常见问题

### Q: 与纯Python系统(fast_sim_vehicle.py)有什么区别？

A:
- `fast_sim_vehicle.py`: 使用纯Python的FastController，**不使用PX4**，快速但控制精度较低
- `mavlink_sim_vehicle.py`: 使用**真实PX4飞控**，通过MAVLink直接控制，控制精度高但需要启动PX4

### Q: 为什么选择MAVLink直接控制而不是MAVROS？

A:
1. 减少依赖：无需安装ROS2和MAVROS
2. 更快启动：省去ROS2节点初始化
3. 更低资源占用：避免ROS2多进程通信开销
4. 简化调试：直接看MAVLink消息

### Q: Mission模式和OFFBOARD模式哪个更好？

A:
- **Mission模式**: 适合预定义轨迹，PX4自动处理航点过渡，飞行更流畅
- **OFFBOARD模式**: 适合实时控制，可以动态调整目标位置

### Q: 如何切换回原系统？

A: 使用原来的启动命令即可：
```bash
python launch_multi_rospy.py --config multi_uav_config.json
python trajectory_data_collector.py --input-dir ~/trajectories
```

## 参考文档

- [PX4 MAVLink消息参考](https://mavlink.io/en/messages/common.html)
- [pymavlink文档](https://mavlink.io/en/mavgen_python/)
- [PX4 Mission协议](https://mavlink.io/en/services/mission.html)
- [8_camera_vehicle.py 文档](8_camera_vehicle.py) (文件头注释)
- [trajectory_data_collector.py 文档](trajectory_data_collector.py) (文件头注释)

---

*更新时间: 2026-01-13*
*更新内容: 添加PVA前馈控制、高动态参数自动配置、加速度前馈说明*
