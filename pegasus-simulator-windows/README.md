# Pegasus Simulator - Simulation Side

仿真端的 Pegasus Simulator，运行 Isaac Sim 物理仿真和图形渲染。**支持 Windows 和 Linux 环境**。

## 概述

这是 Pegasus Simulator 的仿真端，负责：
- 运行 Isaac Sim 物理引擎和图形渲染
- 车辆动力学和传感器仿真
- 通过网络发送传感器数据和状态到控制端
- 从控制端接收控制命令

**注意**：虽然目录名为 `pegasus-simulator-windows`，但本包**同时支持 Windows 和 Linux 环境**。

## 架构

```
仿真端 (Windows/Linux)            控制端 (Linux)
┌─────────────────┐              ┌─────────────────┐
│  Isaac Sim      │              │  PX4/ROS2       │
│  仿真环境        │   TCP/IP     │  控制后端        │
│  NetworkBackend │ <─────────> │  Backend Runner │
└─────────────────┘              └─────────────────┘
```

## 安装

### 前置要求

**Windows 环境：**
1. **Windows 10/11**: 64位系统
2. **NVIDIA GPU**: RTX 系列或 Quadro 系列
3. **NVIDIA 驱动**: 最新版本
4. **Isaac Sim**: 通过 Omniverse Launcher 安装

**Linux 环境：**
1. **Ubuntu 20.04 或 22.04**: 推荐 22.04
2. **NVIDIA GPU**: RTX 系列推荐
3. **NVIDIA 驱动**: 525+
4. **Isaac Sim**: 通过 Omniverse Launcher 安装
5. **内存**: 16GB+ RAM
6. **磁盘空间**: 50GB+

### 安装步骤

1. 安装共享包：
```bash
cd ../pegasus-common
pip install -e .
```

2. 安装仿真端：
```bash
cd pegasus-simulator-windows
pip install -e .
```

3. 链接扩展到 Isaac Sim：

**Windows**:
```bash
link_app.bat
```

**Linux**:
```bash
./link_app.sh
```

## 使用方法

### 环境配置

**首次使用前，建议运行环境检测：**
```bash
cd scripts
python check_environment.py
```

### 基本使用

1. **启动仿真端**：

**Windows**:
```bash
cd examples
python 1_px4_single_vehicle_network.py
```

**Linux**:
```bash
cd examples
isaac_run 1_px4_single_vehicle_network.py
```

2. **启动控制端**（在 Linux 机器上）：
```bash
cd ../pegasus-simulator-linux/examples
python run_px4_backend.py --server-host <仿真端IP>
```

### 配置NetworkBackend

```python
from pegasus.simulator.logic.network import NetworkBackend, NetworkBackendConfig

# 创建配置
network_config = NetworkBackendConfig({
    "vehicle_id": 0,
    "server_host": "0.0.0.0",  # 监听所有网络接口
    "server_port": 5555,       # 监听端口
    "num_rotors": 4,           # 旋翼数量
    "enable_graphical_sensors": False,  # 是否发送相机/激光雷达数据
    "heartbeat_interval": 1.0,  # 心跳间隔（秒）
    "connection_timeout": 5.0   # 连接超时（秒）
})

# 创建网络后端
network_backend = NetworkBackend(network_config)

# 添加到车辆配置
config_multirotor.backends = [network_backend]
```

### 网络配置

**防火墙设置**：
- 确保Windows防火墙允许端口5555的入站连接
- 或者使用自定义端口并相应配置防火墙

**网络连接**：
- Windows和Linux机器需要在同一网络中
- 或者通过VPN/端口转发连接

## 示例

### 示例1：PX4单机网络模式
```bash
python examples/1_px4_single_vehicle_network.py
```

运行Isaac Sim仿真，等待Linux端的PX4后端连接。

### 示例2：启用图形传感器
```python
network_config = NetworkBackendConfig({
    "vehicle_id": 0,
    "server_host": "0.0.0.0",
    "server_port": 5555,
    "num_rotors": 4,
    "enable_graphical_sensors": True,  # 启用相机/激光雷达传输
})
```

注意：启用图形传感器会显著增加网络带宽使用。

## 目录结构

```
pegasus-simulator-windows/
├── setup.py                    # 安装脚本
├── README.md                   # 本文件
├── extensions/
│   └── pegasus.simulator/      # Isaac Sim扩展
│       └── pegasus/
│           └── simulator/
│               ├── logic/
│               │   ├── vehicles/        # 车辆实现
│               │   ├── sensors/         # 传感器实现
│               │   ├── graphical_sensors/  # 相机/激光雷达
│               │   ├── dynamics/        # 动力学模型
│               │   ├── thrusters/       # 推进器模型
│               │   ├── interface/       # PegasusInterface
│               │   └── network/         # 网络通信层
│               │       ├── network_backend.py
│               │       └── message_handler.py
│               ├── ui/                  # UI组件
│               └── parser/              # 配置解析器
├── scripts/                    # 配置和检测脚本
│   └── check_environment.py    # 环境检测
└── examples/
    └── 1_px4_single_vehicle_network.py  # 网络模式示例
```

## 网络协议

### 消息类型

- **STATE_UPDATE**: 车辆状态（位置、姿态、速度等）
- **SENSOR_UPDATE**: 传感器数据（IMU、GPS、气压计、磁力计）
- **GRAPHICAL_SENSOR_UPDATE**: 图形传感器数据（相机、激光雷达）
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

## 故障排除

### 连接问题

**问题**: Linux端无法连接到Windows端

**解决方案**:
1. 检查Windows防火墙设置
2. 确认端口5555未被占用：`netstat -an | findstr 5555`
3. 确认网络连接：从Linux机器ping Windows机器
4. 检查NetworkBackend日志输出

### 性能问题

**问题**: 仿真运行缓慢或网络延迟高

**解决方案**:
1. 禁用图形传感器传输（如果不需要）
2. 减少传感器更新频率
3. 使用有线网络连接而非WiFi
4. 检查网络带宽和延迟

### 超时断开

**问题**: 连接频繁超时断开

**解决方案**:
1. 增加 `connection_timeout` 值
2. 检查网络稳定性
3. 确认Linux端后端正常运行

## 依赖项

- **pegasus-simulator-common**: 共享协议和工具
- **numpy**: 数值计算
- **scipy**: 科学计算
- **msgpack**: 消息序列化
- **Isaac Sim**: NVIDIA Isaac Sim仿真环境

## 许可证

BSD 3-Clause License

## 相关链接

- [Pegasus Simulator主仓库](https://github.com/PegasusSimulator/PegasusSimulator)
- [Isaac Sim文档](https://docs.omniverse.nvidia.com/isaacsim/latest/index.html)
- [pegasus-simulator-linux](../pegasus-simulator-linux/README.md)
- [pegasus-common](../pegasus-common/README.md)
