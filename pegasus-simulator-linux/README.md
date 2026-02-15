# Pegasus Simulator - Linux Side

Linux端的Pegasus Simulator，运行PX4/ArduPilot/ROS2控制后端。

## 概述

这是Pegasus Simulator的Linux端，负责：
- 运行PX4/ArduPilot SITL或ROS2控制后端
- 从Windows端接收传感器数据和状态
- 处理飞行控制逻辑
- 发送控制命令到Windows端

## 架构

```
Windows (远程)                    Linux (本端)
┌─────────────────┐              ┌─────────────────┐
│  Isaac Sim      │              │  PX4/ROS2       │
│  仿真环境        │   TCP/IP     │  控制后端        │
│  NetworkBackend │ <─────────> │  Backend Runner │
└─────────────────┘              └─────────────────┘
```

## 安装

### 前置要求

1. **Python**: Python 3.7+
2. **pegasus-common**: 共享包
3. **PX4/ArduPilot** (可选): 如果使用MAVLink后端
4. **ROS2** (可选): 如果使用ROS2后端

### 安装步骤

1. 安装共享包：
```bash
cd ../pegasus-common
pip install -e .
```

2. 安装Linux端：
```bash
cd pegasus-simulator-linux
pip install -e .
```

3. 安装可选依赖：

**PX4 SITL**:
```bash
# 克隆PX4仓库
git clone https://github.com/PX4/PX4-Autopilot.git
cd PX4-Autopilot
make px4_sitl_default
```

**ArduPilot SITL**:
```bash
# 克隆ArduPilot仓库
git clone https://github.com/ArduPilot/ardupilot.git
cd ardupilot
./Tools/environment_install/install-prereqs-ubuntu.sh -y
. ~/.profile
```

**ROS2**:
```bash
# 安装ROS2 (以Humble为例)
sudo apt install ros-humble-desktop
pip install rclpy
```

## 使用方法

### 环境配置

**首次使用前，建议运行环境检测：**
```bash
cd scripts
python check_environment.py --windows-host <Windows机器IP>
```

**配置ROS2远程访问（如果使用ROS2）：**
```bash
cd scripts
./setup_ros2_network.sh 0 0  # domain_id=0, localhost_only=0
sudo ./configure_firewall.sh
source ~/.ros2_network_config/setup_env.sh
```

### 基本使用

1. **启动Windows端仿真**（在Windows机器上）：
```bash
cd pegasus-simulator-windows/examples
python 1_px4_single_vehicle_network.py
```

2. **启动Linux端后端**（在Linux机器上）：

**PX4后端**:
```bash
cd examples
python run_px4_backend.py --server-host <Windows机器IP>
```

**ArduPilot后端**:
```bash
cd examples
python run_ardupilot_backend.py --server-host <Windows机器IP>
```

**ROS2后端**:
```bash
cd examples
python run_ros2_backend.py --server-host <Windows机器IP>
```

### 命令行参数

所有示例脚本支持以下参数：

```bash
--server-host <IP>      # Windows主机IP地址（必需）
--server-port <PORT>    # Windows主机端口（默认：5555）
--vehicle-id <ID>       # 车辆ID（默认：0）
```

**PX4/ArduPilot特定参数**:
```bash
--mavlink-port <PORT>   # MAVLink基础端口（默认：4560）
```

**ROS2特定参数**:
```bash
--namespace <NS>        # ROS2命名空间（默认：drone）
```

### 使用PX4 SITL

1. 启动PX4 SITL：
```bash
cd ~/PX4-Autopilot
make px4_sitl_default
```

2. 在另一个终端启动后端：
```bash
python run_px4_backend.py --server-host 192.168.1.100
```

3. 使用QGroundControl连接：
- 连接到 `tcp://localhost:4560`

### 使用ArduPilot SITL

1. 启动ArduPilot SITL：
```bash
cd ~/ardupilot/ArduCopter
sim_vehicle.py -v ArduCopter -f gazebo-iris --console
```

2. 在另一个终端启动后端：
```bash
python run_ardupilot_backend.py --server-host 192.168.1.100
```

### 使用ROS2后端

1. 启动ROS2后端：
```bash
python run_ros2_backend.py --server-host 192.168.1.100 --namespace drone
```

2. 查看ROS2话题：
```bash
ros2 topic list
ros2 topic echo /drone/state/pose
```

3. 使用RViz2可视化：
```bash
rviz2
```

## 目录结构

```
pegasus-simulator-linux/
├── setup.py                    # 安装脚本
├── README.md                   # 本文件
├── pegasus/
│   └── simulator/
│       ├── backends/           # 后端实现
│       │   ├── backend.py
│       │   ├── px4_mavlink_backend.py
│       │   ├── ardupilot_mavlink_backend.py
│       │   ├── ros2_backend.py
│       │   └── tools/
│       │       ├── px4_launch_tool.py
│       │       ├── ardupilot_launch_tool.py
│       │       └── ArduPilotPlugin.py
│       └── network/            # 网络通信层
│           ├── network_client.py
│           ├── simulator_proxy.py
│           ├── message_handler.py
│           └── backend_runner.py
├── scripts/                    # 配置和检测脚本
│   ├── check_environment.py    # 环境检测
│   ├── setup_ros2_network.sh   # ROS2网络配置
│   ├── configure_firewall.sh   # 防火墙配置
│   └── ros2_domain_config.py   # ROS2 domain管理
└── examples/
    ├── run_px4_backend.py
    ├── run_ardupilot_backend.py
    └── run_ros2_backend.py
```

## 网络协议

### 接收的消息类型

- **STATE_UPDATE**: 车辆状态（位置、姿态、速度等）
- **SENSOR_UPDATE**: 传感器数据（IMU、GPS、气压计、磁力计）
- **GRAPHICAL_SENSOR_UPDATE**: 图形传感器数据（相机、激光雷达）
- **HEARTBEAT**: 心跳消息

### 发送的消息类型

- **CONTROL_COMMAND**: 控制命令（旋翼角速度）
- **HEARTBEAT**: 心跳消息

### 数据流

```
Windows → Linux:
- STATE_UPDATE (每个物理步)
- SENSOR_UPDATE (传感器更新时)
- GRAPHICAL_SENSOR_UPDATE (图形传感器更新时)
- HEARTBEAT (每秒)

Linux → Windows:
- CONTROL_COMMAND (控制更新时)
- HEARTBEAT (每秒)
```

## 后端说明

### PX4MavlinkBackend

通过MAVLink协议与PX4 SITL通信：
- 接收传感器数据并打包为MAVLink消息
- 发送HIL_SENSOR、HIL_GPS等消息到PX4
- 接收ACTUATOR_CONTROLS消息获取控制命令
- 支持lockstep同步

### ArduPilotMavlinkBackend

通过MAVLink协议与ArduPilot SITL通信：
- 类似PX4后端，但使用ArduPilot特定的消息格式
- 支持ArduPilot的SITL协议

### ROS2Backend

发布传感器数据到ROS2话题：
- `/drone/state/pose`: 车辆位姿
- `/drone/state/twist`: 车辆速度
- `/drone/sensors/imu`: IMU数据
- `/drone/sensors/gps`: GPS数据
- 等等

## 故障排除

### 连接问题

**问题**: 无法连接到Windows端

**解决方案**:
1. 确认Windows端正在运行并监听端口5555
2. 检查网络连接：`ping <Windows_IP>`
3. 检查防火墙设置
4. 尝试使用telnet测试连接：`telnet <Windows_IP> 5555`

### MAVLink连接问题

**问题**: PX4/ArduPilot无法连接

**解决方案**:
1. 确认SITL正在运行
2. 检查MAVLink端口（默认4560）未被占用
3. 查看后端日志输出

### ROS2问题

**问题**: ROS2话题未发布

**解决方案**:
1. 确认ROS2环境已source：`source /opt/ros/humble/setup.bash`
2. 检查ROS2_DOMAIN_ID设置
3. 使用`ros2 topic list`验证话题

## 高级配置

### 自定义后端配置

可以通过修改示例脚本来自定义后端配置：

```python
# 自定义PX4配置
px4_config = PX4MavlinkBackendConfig({
    "vehicle_id": 0,
    "connection_type": "tcpin",
    "connection_ip": "localhost",
    "connection_baseport": 4560,
    "enable_lockstep": True,
    "num_rotors": 4,
    "update_rate": 250  # Hz
})
```

### 多车辆支持

运行多个后端实例，每个使用不同的vehicle_id和端口：

```bash
# 车辆1
python run_px4_backend.py --server-host 192.168.1.100 --vehicle-id 0 --mavlink-port 4560

# 车辆2
python run_px4_backend.py --server-host 192.168.1.100 --vehicle-id 1 --mavlink-port 4570
```

## 依赖项

- **pegasus-simulator-common**: 共享协议和工具
- **numpy**: 数值计算
- **scipy**: 科学计算
- **pymavlink**: MAVLink协议（PX4/ArduPilot后端）
- **msgpack**: 消息序列化
- **rclpy**: ROS2 Python客户端（ROS2后端，可选）

## 许可证

BSD 3-Clause License

## 相关链接

- [Pegasus Simulator主仓库](https://github.com/PegasusSimulator/PegasusSimulator)
- [PX4文档](https://docs.px4.io/)
- [ArduPilot文档](https://ardupilot.org/)
- [ROS2文档](https://docs.ros.org/)
- [pegasus-simulator-windows](../pegasus-simulator-windows/README.md)
- [pegasus-common](../pegasus-common/README.md)
