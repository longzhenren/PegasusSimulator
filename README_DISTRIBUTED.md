# Pegasus Simulator - 分布式架构版本

PegasusSimulator 的分布式架构版本，支持将 Isaac Sim 仿真和控制后端分离到不同主机上运行，**支持多种 Linux 环境部署**。

## 🎯 架构概述

本版本将 PegasusSimulator 拆分为三个独立的包，支持灵活的跨平台部署：

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pegasus Simulator 分布式架构                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  仿真端 (Windows/Linux)            控制端 (Linux)                │
│  ┌─────────────────────┐          ┌─────────────────────┐      │
│  │  Isaac Sim 仿真      │          │  PX4/ROS2 控制       │      │
│  │  - 物理引擎          │  TCP/IP  │  - 飞控软件          │      │
│  │  - 图形渲染          │ <──────> │  - MAVLink          │      │
│  │  - 传感器仿真        │          │  - ROS2 话题         │      │
│  │  NetworkBackend     │          │  Backend Runner     │      │
│  └─────────────────────┘          └─────────────────────┘      │
│           ↑                                  ↑                  │
│           └──────────────────┬───────────────┘                  │
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
   - 日志和连接管理

2. **pegasus-simulator-windows**: 仿真端（**支持 Windows 和 Linux**）
   - Isaac Sim 物理仿真和图形渲染
   - 车辆动力学和传感器仿真
   - NetworkBackend（发送传感器数据，接收控制命令）
   - **跨平台支持**：可在 Windows 或 Linux 上运行

3. **pegasus-simulator-linux**: 控制端（Linux）
   - PX4/ArduPilot MAVLink 后端
   - ROS2 后端
   - SimulatorProxy（接收传感器数据，发送控制命令）

## 🚀 部署场景

### 场景 1: Windows 仿真 + Linux 控制（原始场景）
```
Windows 机器 (仿真端)  ←→  Linux 机器 (控制端)
    Isaac Sim              PX4/ROS2
```

### 场景 2: Linux 仿真 + Linux 控制（新增支持）
```
Linux 机器 A (仿真端)  ←→  Linux 机器 B (控制端)
    Isaac Sim              PX4/ROS2
```

### 场景 3: Linux 仿真 + 多个 Linux 控制端
```
Linux 机器 A (仿真端)  ←→  Linux 机器 B (PX4 控制)
    Isaac Sim          ←→  Linux 机器 C (ROS2 控制)
                       ←→  Linux 机器 D (ArduPilot 控制)
```

### 场景 4: 单机 Linux 部署（开发测试）
```
Linux 机器 (仿真端 + 控制端)
    Isaac Sim + PX4/ROS2
```

## 📦 快速开始

### 1. 安装共享包

```bash
cd pegasus-common
pip install -e .
```

### 2. 安装仿真端（Windows 或 Linux）

```bash
cd pegasus-simulator-windows
pip install -e .

# Windows: 链接扩展到 Isaac Sim
link_app.bat

# Linux: 链接扩展到 Isaac Sim
./link_app.sh
```

### 3. 安装控制端（Linux）

```bash
cd pegasus-simulator-linux
pip install -e .
```

### 4. 环境检测

**仿真端（Windows 或 Linux）：**
```bash
cd pegasus-simulator-windows/scripts
python check_environment.py
```

**控制端（Linux）：**
```bash
cd pegasus-simulator-linux/scripts
python check_environment.py --windows-host <仿真端IP>
```

### 5. 运行仿真

**仿真端（Windows 或 Linux）：**
```bash
cd pegasus-simulator-windows/examples
python 1_px4_single_vehicle_network.py
```

**控制端（Linux）：**
```bash
cd pegasus-simulator-linux/examples
python run_px4_backend.py --server-host <仿真端IP>
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

## 🐧 Linux 上运行 Isaac Sim

### 系统要求

- Ubuntu 20.04 或 22.04
- NVIDIA GPU（RTX 系列推荐）
- NVIDIA 驱动 525+
- 16GB+ RAM
- 50GB+ 磁盘空间

### 安装 Isaac Sim（Linux）

1. **安装 NVIDIA 驱动**：
```bash
sudo apt update
sudo apt install nvidia-driver-525
sudo reboot
```

2. **安装 Isaac Sim**：
   - 下载 Omniverse Launcher: https://www.nvidia.com/en-us/omniverse/download/
   - 或使用命令行安装：
```bash
# 下载 Isaac Sim
wget https://install.launcher.omniverse.nvidia.com/installers/omniverse-launcher-linux.AppImage
chmod +x omniverse-launcher-linux.AppImage
./omniverse-launcher-linux.AppImage

# 通过 Launcher 安装 Isaac Sim
```

3. **配置环境变量**：
```bash
# 添加到 ~/.bashrc
export ISAAC_SIM_PATH="$HOME/.local/share/ov/pkg/isaac-sim-*"
export PATH="$ISAAC_SIM_PATH:$PATH"

# 创建 isaac_run 函数
isaac_run() {
    $ISAAC_SIM_PATH/python.sh "$@"
}
export -f isaac_run
```

4. **验证安装**：
```bash
cd pegasus-simulator-windows/scripts
python check_environment.py
```

## 🔧 配置

### 网络配置

**仿真端** (`NetworkBackendConfig`):
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

**控制端** (命令行参数):
```bash
--server-host <IP>      # 仿真端 IP（必需）
--server-port <PORT>    # 仿真端端口（默认：5555）
--vehicle-id <ID>       # 车辆 ID（默认：0）
```

### 防火墙设置

**Linux 仿真端**:
```bash
sudo ufw allow 5555/tcp
```

**Windows 仿真端**:
```powershell
New-NetFirewallRule -DisplayName "Pegasus Simulator" -Direction Inbound -LocalPort 5555 -Protocol TCP -Action Allow
```

### ROS2 远程访问配置（Linux 控制端）

```bash
cd pegasus-simulator-linux/scripts
./setup_ros2_network.sh 0 0
sudo ./configure_firewall.sh
source ~/.ros2_network_config/setup_env.sh
```

## 🐛 故障排除

### 连接问题

**问题**: 控制端无法连接到仿真端

**解决方案**:
1. 检查防火墙设置
2. 确认端口 5555 未被占用：
   - Linux: `netstat -tuln | grep 5555`
   - Windows: `netstat -an | findstr 5555`
3. 测试网络连接：
   ```bash
   python pegasus-common/scripts/test_network.py --host <仿真端IP>
   ```
4. 检查仿真端是否正在运行

### Linux 上 Isaac Sim 问题

**问题**: Isaac Sim 无法启动或渲染问题

**解决方案**:
1. 检查 NVIDIA 驱动：`nvidia-smi`
2. 检查 GPU 支持：确保是 RTX 系列或 Quadro
3. 检查 Vulkan 支持：`vulkaninfo`
4. 查看日志：`~/.nvidia-omniverse/logs/`

### 性能问题

**问题**: 网络延迟高或仿真卡顿

**解决方案**:
1. 禁用图形传感器传输（`enable_graphical_sensors: False`）
2. 使用有线网络而非 WiFi
3. 检查网络带宽和延迟
4. 减少传感器更新频率
5. 在仿真端使用更强大的 GPU

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

**注意**:
- 本分支（win-linux）是分布式架构版本
- 仿真端（pegasus-simulator-windows）**支持 Windows 和 Linux**
- 控制端（pegasus-simulator-linux）仅支持 Linux
- 如需使用原版本，请切换到 main 分支
