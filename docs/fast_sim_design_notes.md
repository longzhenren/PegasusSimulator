# 纯Python仿真环境设计文档

## 文档信息

- 创建日期: 2026-01-10
- 作者: AI Assistant
- 版本: 1.0

## 一、项目背景与需求

### 1.1 原始需求

用户要求设计一套新的仿真环境：
1. 使用已有的无人机动力学控制器 `fast_controller.py`
2. 提供与现有环境完全兼容的控制和获取状态等接口
3. 设计新的完整的动力学数据收集代码
4. 输入输出形式与现有数据采集代码完全一致
5. 行为表现相同（状态机可以不同设计）
6. 不使用PX4和Autopilot，使用纯Python配置
7. 不影响现有仿真流程和代码

### 1.2 项目分析

原有系统架构：
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

关键组件：
- `8_camera_vehicle.py`: Isaac Sim仿真主程序 + PX4 SITL
- `rospy_isaacsim.py`: ROS2 + MAVROS控制层
- `trajectory_data_collector.py`: 数据采集脚本
- `fast_controller.py`: 级联PID姿态控制器（纯Python）

## 二、设计方案

### 2.1 系统架构

新系统架构：
```
fast_trajectory_collector.py
         │
         └── HTTP ──────────► fast_sim_vehicle.py (HTTP :8081)
                                   │
                                   ├── VirtualController (HTTP :5009+id)
                                   │
                                   └── FastController (纯Python控制)
```

### 2.2 组件设计

#### 2.2.1 fast_sim_vehicle.py

主要功能：
- Isaac Sim仿真主程序（替代8_camera_vehicle.py）
- 使用FastController替代PX4作为控制后端
- 内置VirtualController，提供与rospy_isaacsim.py兼容的HTTP接口

核心类：
```python
class FastMultiUAVManager:
    """多机管理器，使用FastController替代PX4"""
    - spawn(): 生成UAV，配置FastController
    - _configure_collision_filtering(): 碰撞过滤
    - _configure_uav_transparency(): UAV透明度

class VirtualController:
    """虚拟控制器，模拟rospy_isaacsim.py"""
    HTTP接口：
    - POST /reset: 重置UAV
    - POST /command: 发送控制命令
    - GET /health: 健康检查
    - GET /uav/<id>/all: 获取图像和位姿

class FastSimApp:
    """仿真主应用"""
    HTTP接口（端口8081）：
    - GET /uav/<id>/pose: 获取位姿
    - GET /uav/<id>/image: 获取图像
    - GET /uav/<id>/all: 获取同步快照
    - POST /uav/<id>/reset: 重置位置
    - GET /uav/<id>/px4/ready: 就绪状态（兼容接口）
```

#### 2.2.2 fast_trajectory_collector.py

主要功能：
- 数据采集脚本（替代trajectory_data_collector.py）
- 纯HTTP控制，不依赖ROS2/MAVROS
- 输入输出格式完全兼容

核心类：
```python
class Worker:
    """轨迹采集工作线程"""
    - _reset_and_wait_ready(): 重置并等待就绪
    - _process_one(): 处理单个轨迹
    - _calculate_origin_offset(): 计算坐标偏移
    - _apply_alignment(): 应用坐标对齐
```

### 2.3 接口兼容性设计

#### HTTP仿真端口 (8081)

| 接口 | 原系统 | 新系统 |
|------|--------|--------|
| GET /uav/<id>/pose | ✓ | ✓ (相同) |
| GET /uav/<id>/image | ✓ | ✓ (相同) |
| GET /uav/<id>/image.png | ✓ | ✓ (相同) |
| GET /uav/<id>/all | ✓ | ✓ (相同) |
| POST /uav/<id>/reset | ✓ | ✓ (相同) |
| GET /uav/<id>/px4/ready | ✓ | ✓ (始终返回ready=True) |
| GET /uav/<id>/px4/status | ✓ | ✓ (模拟返回) |

#### HTTP控制端口 (5009+id)

| 接口 | 原系统 | 新系统 |
|------|--------|--------|
| POST /reset | ✓ | ✓ (相同) |
| POST /command | ✓ | ✓ (相同) |
| GET /health | ✓ | ✓ (相同) |
| GET /uav/<id>/all | ✓ | ✓ (相同) |

#### 数据格式兼容性

输入JSON格式：
```json
{
  "raw_logs": [[x, y, z, roll, yaw, pitch]],
  "preprocessed_logs": [[x, y, z, roll, yaw, pitch], ...]
}
```

输出CSV字段：
- data.csv: traj_json, traj_name, uav_id, step_idx, cmd_*, obs_*, image_path, ...
- all_pose_data.csv: traj_json, traj_name, uav_id, step_idx, pos_*, att_*, linvel_*, ...

### 2.4 关键设计决策

1. **控制器选择**: 使用项目已有的FastController，无需新增控制算法
2. **状态机简化**: 使用简单的OFFBOARD模式，无需复杂的PX4状态机
3. **内置控制服务**: VirtualController内置于仿真程序，无需单独启动
4. **就绪状态**: 纯Python系统始终就绪，无需等待PX4初始化
5. **坐标对齐**: 保留原系统的坐标对齐机制，确保数据一致性

## 三、关键代码实现

### 3.1 FastController集成

```python
# 创建FastController
fast_ctrl = FastController(
    trajectory_file=None,
    results_file=None,
    scale=ARGS.scale,
    z_down=ARGS.z_down,
    uav_id=vid,
)

# 配置多旋翼
config_multirotor = MultirotorConfig()
config_multirotor.graphical_sensors = [camera]
config_multirotor.backends = [fast_ctrl]  # 使用FastController作为后端
```

### 3.2 VirtualController HTTP路由

```python
@app.route('/command', methods=['POST'])
def command():
    cmd = data.get("cmd", "")
    if cmd == "move_to":
        return self._handle_move_to(x, y, z)
    elif cmd == "land":
        return self._handle_land()
    elif cmd == "get_status":
        return jsonify({"status": {"mode": "OFFBOARD", "armed": True}})
```

### 3.3 坐标对齐机制

```python
def _calculate_origin_offset(self, cmd_pos, obs_pos):
    """计算命令坐标系和观测坐标系之间的原点偏移量"""
    return (
        obs_pos[0] - cmd_pos[0],
        obs_pos[1] - cmd_pos[1],
        obs_pos[2] - cmd_pos[2],
    )

def _apply_alignment(self, obs_pos):
    """将观测坐标对齐到命令坐标系"""
    if self._origin_offset is None:
        return obs_pos
    return (
        obs_pos[0] - self._origin_offset[0],
        obs_pos[1] - self._origin_offset[1],
        obs_pos[2] - self._origin_offset[2],
    )
```

## 四、文件清单

### 4.1 新建文件

| 文件路径 | 功能 |
|----------|------|
| `examples/fast_sim_vehicle.py` | 纯Python仿真主程序 |
| `examples/fast_trajectory_collector.py` | 纯Python数据采集脚本 |
| `examples/FAST_SIM_README.md` | 使用说明文档 |
| `docs/fast_sim_design_notes.md` | 设计文档（本文件） |

### 4.2 依赖的已有文件

| 文件路径 | 功能 |
|----------|------|
| `examples/utils/fast_controller.py` | 级联PID姿态控制器 |
| `examples/utils/FAST_CONTROLLER_README.md` | 控制器说明文档 |
| `examples/multi_uav_config.json` | UAV配置文件 |

### 4.3 未修改的原有文件

| 文件路径 | 功能 |
|----------|------|
| `examples/8_camera_vehicle.py` | 原PX4仿真主程序 |
| `examples/rospy_isaacsim.py` | 原MAVROS控制层 |
| `examples/trajectory_data_collector.py` | 原数据采集脚本 |
| `examples/launch_multi_rospy.py` | 原多机启动脚本 |

## 五、使用方法

### 5.1 启动仿真

```bash
# 使用默认配置
ISAACSIM_PYTHON examples/fast_sim_vehicle.py

# 使用自定义配置
ISAACSIM_PYTHON examples/fast_sim_vehicle.py --config examples/multi_uav_config.json

# 无头模式
ISAACSIM_PYTHON examples/fast_sim_vehicle.py --headless
```

### 5.2 执行数据采集

```bash
# 单机采集
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --uav-ids 0

# 多机并行采集
python examples/fast_trajectory_collector.py \
  --input-dir ~/trajectories \
  --config examples/multi_uav_config.json
```

## 六、性能对比

| 指标 | 原系统 (PX4) | 新系统 (FastController) |
|------|-------------|------------------------|
| 启动时间 | ~30秒 | ~5秒 |
| 内存占用 | 高（多进程） | 低（单进程） |
| CPU占用 | 高 | 低 |
| 依赖复杂度 | 高 | 低 |
| 控制精度 | 高（真实飞控） | 中（简化模型） |
| 适用场景 | 真实飞行模拟 | 快速数据采集 |

## 七、已知限制

1. **无ULG日志**: 纯Python系统不使用PX4，无法生成PX4飞行日志
2. **简化动力学**: FastController使用简化的控制模型，不包含完整的飞行器动力学
3. **无传感器噪声**: 状态读取直接来自仿真器，无传感器噪声模拟
4. **无故障模拟**: 不支持PX4的故障注入和安全功能

## 八、后续扩展建议

1. **添加传感器噪声**: 可在状态读取时添加高斯噪声
2. **支持更多控制模式**: 添加速度控制、姿态控制等模式
3. **日志功能**: 添加自定义日志格式，替代ULG
4. **可视化工具**: 添加实时轨迹可视化

---

# MAVLink直接控制仿真环境设计文档

## 文档信息

- 创建日期: 2026-01-10
- 作者: AI Assistant
- 版本: 1.0

## 九、MAVLink直接控制系统需求

### 9.1 原始需求

用户要求在纯Python系统基础上，再设计一套并列的新系统：
1. 直接用MAVLink对飞机进行控制
2. 只启动PX4，不使用MAVROS
3. 使用Mission模式控制飞机
4. 其余与PX4系统保持一致
5. 不修改现有代码
6. 功能相同部分直接复用代码

### 9.2 设计分析

MAVLink直接控制系统需要：
- 保留PX4飞控的完整能力
- 绕过MAVROS/ROS2依赖
- 使用pymavlink库直接与PX4通信
- 支持Mission模式航点导航

## 十、MAVLink系统架构

### 10.1 系统架构

```
mavlink_trajectory_collector.py
         │
         └── HTTP ──────────► mavlink_sim_vehicle.py (HTTP :8081)
                                   │
                                   ├── MAVLinkController (HTTP :5009+id)
                                   │        │
                                   │        └── MAVLink UDP ──► PX4 SITL
                                   │
                                   └── PX4MavlinkBackend (lockstep)
```

### 10.2 与其他系统的对比

| 系统 | 控制后端 | 依赖 | 控制精度 | 启动时间 |
|------|---------|------|---------|---------|
| 原系统 (PX4+MAVROS) | PX4 + MAVROS + ROS2 | 复杂 | 高 | ~30s |
| 纯Python系统 | FastController | 无 | 中 | ~5s |
| MAVLink直接控制 | PX4 + pymavlink | 简单 | 高 | ~15s |

## 十一、MAVLink系统组件设计

### 11.1 mavlink_sim_vehicle.py

主要功能：
- Isaac Sim仿真主程序
- 使用PX4MavlinkBackend作为控制后端（与8_camera_vehicle.py相同）
- 内置MAVLinkController，提供与rospy_isaacsim.py兼容的HTTP接口
- 通过MAVLink直接与PX4通信

核心类：
```python
class MAVLinkController:
    """MAVLink直接控制器"""
    - _setup_mavlink_connection(): 建立MAVLink连接
    - _handle_move_to(): 处理移动命令
    - _handle_execute_mission(): 执行Mission模式任务
    - _upload_mission(): 上传MAVLink任务
    - _arm() / _disarm(): 解锁/锁定
    - _set_mode(): 设置飞行模式

class MAVLinkMultiUAVManager:
    """多机管理器"""
    - spawn(): 生成UAV，配置PX4后端
    - 为每个UAV创建MAVLinkController

class MAVLinkSimApp:
    """仿真主应用"""
    - HTTP接口与8_camera_vehicle.py完全兼容
```

### 11.2 mavlink_trajectory_collector.py

主要功能：
- 数据采集脚本
- 支持OFFBOARD模式（逐个航点）
- 支持Mission模式（一次性上传）
- 输入输出格式与trajectory_data_collector.py完全兼容

核心功能：
```python
class Worker:
    """轨迹采集工作线程"""
    - _process_offboard_mode(): OFFBOARD模式导航
    - _process_mission_mode(): Mission模式导航
    - _collect_data_point(): 采集单个数据点
```

## 十二、MAVLink通信协议

### 12.1 连接建立

```python
# UDP连接到PX4
mavlink_conn = mavutil.mavlink_connection(f"udpout:127.0.0.1:{14550+uav_id}")

# 等待心跳
mavlink_conn.wait_heartbeat()
```

### 12.2 Mission模式协议

1. 清除任务: `MISSION_CLEAR_ALL`
2. 发送数量: `MISSION_COUNT`
3. 响应请求: `MISSION_REQUEST` → `MISSION_ITEM`
4. 确认上传: `MISSION_ACK`
5. 开始任务: `MAV_CMD_MISSION_START`

### 12.3 支持的命令

| 命令 | MAVLink消息 | 说明 |
|------|------------|------|
| arm | MAV_CMD_COMPONENT_ARM_DISARM | 解锁 |
| disarm | MAV_CMD_COMPONENT_ARM_DISARM | 锁定 |
| set_mode | SET_MODE | 设置飞行模式 |
| mission_start | MAV_CMD_MISSION_START | 开始任务 |
| land | SET_MODE (AUTO.LAND) | 降落 |

## 十三、接口兼容性

### 13.1 HTTP仿真端口 (8081)

与8_camera_vehicle.py完全相同：
- GET /uav/<id>/pose
- GET /uav/<id>/image
- GET /uav/<id>/all
- POST /uav/<id>/reset
- GET /uav/<id>/px4/ready
- GET /uav/<id>/px4/status

### 13.2 HTTP控制端口 (5009+id)

与rospy_isaacsim.py兼容：
- POST /reset
- POST /command (move_to, move_to_many, execute_mission, land, ...)
- GET /health

## 十四、文件清单

### 14.1 新建文件

| 文件路径 | 功能 |
|----------|------|
| `examples/mavlink_sim_vehicle.py` | MAVLink直接控制仿真主程序 |
| `examples/mavlink_trajectory_collector.py` | MAVLink数据采集脚本 |
| `examples/MAVLINK_SIM_README.md` | 使用说明文档 |

### 14.2 依赖的已有文件

| 文件路径 | 功能 |
|----------|------|
| `px4_mavlink_backend.py` | PX4 MAVLink后端（复用） |
| `multi_uav_config.json` | UAV配置文件（复用） |

## 十五、三套系统总结

现在项目中有三套并列的仿真系统：

### 15.1 原系统 (PX4 + MAVROS)
- 文件: `8_camera_vehicle.py` + `rospy_isaacsim.py` + `trajectory_data_collector.py`
- 特点: 完整的PX4飞控 + MAVROS控制
- 适用: 需要ROS2集成的场景

### 15.2 纯Python系统 (FastController)
- 文件: `fast_sim_vehicle.py` + `fast_trajectory_collector.py`
- 特点: 轻量级，无外部依赖
- 适用: 快速原型开发，简单数据采集

### 15.3 MAVLink直接控制系统
- 文件: `mavlink_sim_vehicle.py` + `mavlink_trajectory_collector.py`
- 特点: 真实PX4飞控，无需MAVROS
- 适用: 需要PX4精度但不想安装ROS2的场景

---

*文档结束*
