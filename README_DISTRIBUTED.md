# Pegasus Simulator - 分布式架构版本

PegasusSimulator 的分布式架构版本，支持将 Isaac Sim 仿真（Windows）和控制后端（Linux）分离到不同主机上运行。

## 🎯 架构概述

本版本将 PegasusSimulator 拆分为三个独立的包：

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pegasus Simulator 架构                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Windows 主机                          Linux 主机                │
│  ┌─────────────────────┐              ┌─────────────────────┐  │
│  │  Isaac Sim 仿真      │              │  PX4/ROS2 控制       │  │
│  │  - 物理引擎          │   TCP/IP     │  - 飞控软件          │  │
│  │  - 图形渲染          │  <────────>  │  - MAVLink          │  │
│  │  - 传感器仿真        │              │  - ROS2 话题         │  │
│  │  NetworkBackend     │              │  Backend Runner     │  │
│  └─────────────────────┘              └─────────────────────┘  │
│           ↑                                      ↑              │
│           └──────────────────┬───────────────────┘              │
│                              │                                  │
│                    ┌─────────────────────┐                      │
│                    │  pegasus-common     │                      │
│                    │  - 协议定义          │                      │
│                    │  - 状态表示          │                      │
│                    │  - 消息序列化        │                      │
│                    └─────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

### 三个包的职责

1. **pegasus-common**: 共享代码包
   - 车辆状态表示（State类）
   - 网络协议定义（消息类型、序列化）
   - 坐标系转换工具

2. **pegasus-simulator-windows**: Windows端（Isaac Sim）
   - Isaac Sim 物理仿真和图形渲染
   - 车辆动力学和传感器仿真
   - NetworkBackend（发送传感器数据，接收控制命令）

3. **pegasus-simulator-linux**: Linux端（控制后端）
   - PX4/ArduPilot MAVLink 后端
   - ROS2 后端
   - SimulatorProxy（接收传感器数据，发送控制命令）

## 🚀 快速开始

### 1. 安装共享包

```bash
cd pegasus-common
pip install -e .
```

### 2. 安装 Windows 端（在 Windows 机器上）

```bash
cd pegasus-simulator-windows
pip install -e .

# 链接扩展到 Isaac Sim
link_app.bat
```

### 3. 安装 Linux 端（在 Linux 机器上）

```bash
cd pegasus-simulator-linux
pip install -e .
```

### 4. 运行仿真

**在 Windows 机器上启动仿真**:
```bash
cd pegasus-simulator-windows/examples
python 1_px4_single_vehicle_network.py
```

**在 Linux 机器上启动 PX4 后端**:
```bash
cd pegasus-simulator-linux/examples
python run_px4_backend.py --server-host <Windows机器IP>
```

## 📦 包结构

```
PegasusSimulator/
├── pegasus-common/                  # 共享包
│   ├── pegasus/simulator/
│   │   ├── common/                  # 状态和工具
│   │   └── protocol/                # 网络协议
│   └── README.md
│
├── pegasus-simulator-windows/       # Windows 端
│   ├── extensions/pegasus.simulator/
│   │   └── pegasus/simulator/
│   │       ├── logic/
│   │       │   ├── vehicles/        # 车辆实现
│   │       │   ├── sensors/         # 传感器
│   │       │   ├── network/         # 网络通信层
│   │       │   └── ...
│   │       └── ui/
│   ├── examples/
│   └── README.md
│
└── pegasus-simulator-linux/         # Linux 端
    ├── pegasus/simulator/
    │   ├── backends/                # PX4/ArduPilot/ROS2
    │   └── network/                 # 网络通信层
    ├── examples/
    │   ├── run_px4_backend.py
    │   ├── run_ardupilot_backend.py
    │   └── run_ros2_backend.py
    └── README.md
```

## 🔌 网络协议

### 消息类型

- **STATE_UPDATE**: 车辆状态（位置、姿态、速度、加速度）
- **SENSOR_UPDATE**: 传感器数据（IMU、GPS、气压计、磁力计）
- **GRAPHICAL_SENSOR_UPDATE**: 图形传感器（相机、激光雷达）
- **CONTROL_COMMAND**: 控制命令（旋翼角速度）
- **HEARTBEAT**: 心跳消息

### 通信特性

- **传输协议**: TCP socket（可靠传输）
- **序列化**: MessagePack（高效二进制格式）
- **压缩**: 大数据自动压缩（>10KB）
- **心跳**: 1秒间隔，5秒超时
- **重连**: 自动重连机制

## 📖 详细文档

- [pegasus-common 文档](pegasus-common/README.md)
- [pegasus-simulator-windows 文档](pegasus-simulator-windows/README.md)
- [pegasus-simulator-linux 文档](pegasus-simulator-linux/README.md)

## 🎮 使用示例

### PX4 SITL 示例

**Windows 端**:
```python
from pegasus.simulator.logic.network import NetworkBackend, NetworkBackendConfig

network_config = NetworkBackendConfig({
    "vehicle_id": 0,
    "server_host": "0.0.0.0",
    "server_port": 5555,
    "num_rotors": 4
})

network_backend = NetworkBackend(network_config)
config_multirotor.backends = [network_backend]
```

**Linux 端**:
```bash
# 启动 PX4 SITL
cd ~/PX4-Autopilot
make px4_sitl_default

# 启动后端
python run_px4_backend.py --server-host 192.168.1.100
```

### ROS2 示例

**Linux 端**:
```bash
python run_ros2_backend.py --server-host 192.168.1.100 --namespace drone

# 查看话题
ros2 topic list
ros2 topic echo /drone/state/pose
```

## 🔧 配置

### 网络配置

**Windows 端** (`NetworkBackendConfig`):
```python
{
    "vehicle_id": 0,
    "server_host": "0.0.0.0",      # 监听所有接口
    "server_port": 5555,            # 监听端口
    "num_rotors": 4,
    "enable_graphical_sensors": False,  # 是否发送相机/激光雷达
    "heartbeat_interval": 1.0,      # 心跳间隔（秒）
    "connection_timeout": 5.0       # 连接超时（秒）
}
```

**Linux 端** (命令行参数):
```bash
--server-host <IP>      # Windows 主机 IP（必需）
--server-port <PORT>    # Windows 主机端口（默认：5555）
--vehicle-id <ID>       # 车辆 ID（默认：0）
```

### 防火墙设置

**Windows**:
```powershell
# 允许端口 5555 入站连接
New-NetFirewallRule -DisplayName "Pegasus Simulator" -Direction Inbound -LocalPort 5555 -Protocol TCP -Action Allow
```

**Linux**:
```bash
# 允许端口 5555 出站连接
sudo ufw allow out 5555/tcp
```

## 🐛 故障排除

### 连接问题

**问题**: Linux 端无法连接到 Windows 端

**解决方案**:
1. 检查 Windows 防火墙设置
2. 确认端口 5555 未被占用：`netstat -an | findstr 5555`
3. 测试网络连接：从 Linux 机器 ping Windows 机器
4. 使用 telnet 测试端口：`telnet <Windows_IP> 5555`

### 性能问题

**问题**: 网络延迟高或仿真卡顿

**解决方案**:
1. 禁用图形传感器传输（`enable_graphical_sensors: False`）
2. 使用有线网络而非 WiFi
3. 检查网络带宽和延迟
4. 减少传感器更新频率

### 超时断开

**问题**: 连接频繁超时

**解决方案**:
1. 增加 `connection_timeout` 值
2. 检查网络稳定性
3. 确认两端程序正常运行

## 🔄 从原版本迁移

原版本的本地模式仍然保留在 `extensions/` 目录中。如果需要使用原版本：

```bash
cd examples
python 1_px4_single_vehicle.py  # 原版本本地模式
```

新版本网络模式：

```bash
cd pegasus-simulator-windows/examples
python 1_px4_single_vehicle_network.py  # 新版本网络模式
```

## 📊 性能指标

- **网络延迟**: < 10ms（局域网）
- **数据吞吐**: ~1-5 MB/s（不含图形传感器）
- **数据吞吐**: ~10-50 MB/s（含图形传感器）
- **CPU 开销**: < 5%（网络通信层）

## 🤝 贡献

欢迎贡献代码！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

BSD 3-Clause License - 详见 [LICENSE](LICENSE) 文件

## 🔗 相关链接

- [原版 PegasusSimulator](https://github.com/PegasusSimulator/PegasusSimulator)
- [Isaac Sim 文档](https://docs.omniverse.nvidia.com/isaacsim/latest/index.html)
- [PX4 文档](https://docs.px4.io/)
- [ArduPilot 文档](https://ardupilot.org/)
- [ROS2 文档](https://docs.ros.org/)

## 📮 联系方式

如有问题或建议，请提交 Issue 或 Pull Request。

---

**注意**: 本分支（win-linux）是分布式架构版本。如需使用原版本，请切换到 main 分支。
