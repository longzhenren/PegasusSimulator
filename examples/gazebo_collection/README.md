# Gazebo PX4 多机动力学数据采集指南

本指南详细说明如何在纯 PX4 SITL + Gazebo 环境中进行多机并行动力学数据采集。

## 目录

1. [系统要求](#系统要求)
2. [环境安装](#环境安装)
3. [文件结构](#文件结构)
4. [使用方法](#使用方法)
5. [数据格式](#数据格式)
6. [故障排除](#故障排除)

---

## 系统要求

### 硬件要求
- **CPU**: 8核以上（每个UAV实例约占用1核）
- **内存**: 16GB以上（每个UAV约占用1-2GB）
- **推荐配置**: 16核 32GB 可稳定运行 8 架 UAV

### 软件要求
- Ubuntu 22.04 LTS（推荐）或 Ubuntu 20.04 LTS
- ROS2 Humble（Ubuntu 22.04）或 ROS2 Foxy（Ubuntu 20.04）
- PX4 Autopilot v1.14 或更高版本
- Gazebo Classic 11 或 Gazebo Sim（Harmonic/Garden）
- Python 3.8+

---

## 环境安装

### 1. 安装 ROS2

```bash
# Ubuntu 22.04 - ROS2 Humble
sudo apt update && sudo apt install locales
sudo locale-gen en_US en_US.UTF-8
sudo update-locale LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
export LANG=en_US.UTF-8

sudo apt install software-properties-common
sudo add-apt-repository universe
sudo apt update && sudo apt install curl -y
sudo curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key -o /usr/share/keyrings/ros-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros2/ubuntu $(. /etc/os-release && echo $UBUNTU_CODENAME) main" | sudo tee /etc/apt/sources.list.d/ros2.list > /dev/null

sudo apt update
sudo apt install ros-humble-desktop ros-dev-tools -y

# 添加到 .bashrc
echo "source /opt/ros/humble/setup.bash" >> ~/.bashrc
source ~/.bashrc
```

### 2. 安装 MAVROS

```bash
# 安装 MAVROS 及其依赖
sudo apt install ros-humble-mavros ros-humble-mavros-extras ros-humble-mavros-msgs -y

# 安装 GeographicLib 数据集（MAVROS 必需）
sudo /opt/ros/humble/lib/mavros/install_geographiclib_datasets.sh
```

### 3. 安装 PX4 Autopilot

```bash
# 克隆 PX4 仓库
cd ~
git clone https://github.com/PX4/PX4-Autopilot.git --recursive
cd PX4-Autopilot

# 运行安装脚本（自动安装依赖）
bash ./Tools/setup/ubuntu.sh

# 编译 SITL
make px4_sitl_default

# 可选：编译 Gazebo Classic 插件
make px4_sitl_default gazebo-classic

# 设置环境变量
echo "export PX4_DIR=~/PX4-Autopilot" >> ~/.bashrc
source ~/.bashrc
```

### 4. 安装 Gazebo

```bash
# Gazebo Classic（推荐用于 PX4 SITL）
sudo apt install gazebo libgazebo-dev -y

# 或 Gazebo Sim（Harmonic）
# sudo apt install gz-harmonic -y
```

### 5. 安装 Python 依赖

```bash
# 创建虚拟环境（可选但推荐）
python3 -m venv ~/gazebo_collection_env
source ~/gazebo_collection_env/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 6. 安装 pyulog（可选，用于 ULG 日志分析）

```bash
pip install pyulog
```

---

## 文件结构

```
gazebo_collection/
├── gazebo_dynamics_collector.py  # 主数据采集脚本
├── launch_multi_uav.py           # Python 多机启动脚本
├── launch_multi_uav_gazebo.sh    # Bash 多机启动脚本
├── requirements.txt              # Python 依赖
└── README.md                     # 本文档
```

---

## 使用方法

### 步骤 1: 启动多机仿真环境

**方法 A: 使用 Python 启动脚本（推荐）**

```bash
# 启动 4 架 UAV
python3 launch_multi_uav.py --num-uavs 4

# 启动 8 架 UAV，ID 从 0 开始
python3 launch_multi_uav.py --num-uavs 8 --start-id 0

# 自定义间距（默认 3 米）
python3 launch_multi_uav.py --num-uavs 4 --spacing 5.0

# 使用 Gazebo Sim 而非 Gazebo Classic
python3 launch_multi_uav.py --num-uavs 4 --gazebo-sim
```

**方法 B: 使用 Bash 启动脚本**

```bash
# 启动 4 架 UAV
./launch_multi_uav_gazebo.sh 4

# 启动 8 架 UAV，ID 从 0 开始
./launch_multi_uav_gazebo.sh 8 0
```

**方法 C: 手动启动（调试用）**

```bash
# 终端 1: 启动 Gazebo
gazebo --verbose ~/PX4-Autopilot/Tools/simulation/gazebo-classic/worlds/empty.world

# 终端 2: 启动 PX4 实例 0
cd ~/PX4-Autopilot
PX4_SYS_AUTOSTART=4001 ./build/px4_sitl_default/bin/px4 -i 0

# 终端 3: 启动 MAVROS
ros2 run mavros mavros_node --ros-args -r __ns:=/uav0 -p fcu_url:=udp://:14540@127.0.0.1:14580
```

### 步骤 2: 验证系统状态

```bash
# 检查 MAVROS 连接状态
ros2 topic echo /uav0/mavros/state

# 检查位置数据
ros2 topic echo /uav0/mavros/local_position/pose

# 列出所有 UAV 话题
ros2 topic list | grep uav
```

### 步骤 3: 运行数据采集

```bash
# 单机采集
python3 gazebo_dynamics_collector.py \
    --input-dir ~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files \
    --out-dir ~/gazebo_recordings \
    --uav-ids 0

# 多机并行采集（4架 UAV）
python3 gazebo_dynamics_collector.py \
    --input-dir ~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files \
    --out-dir ~/gazebo_recordings \
    --uav-ids 0,1,2,3

# 8机并行采集
python3 gazebo_dynamics_collector.py \
    --input-dir ~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files \
    --out-dir ~/gazebo_recordings \
    --uav-ids 0,1,2,3,4,5,6,7 \
    --waypoint-timeout 180

# 先预览轨迹文件（dry-run）
python3 gazebo_dynamics_collector.py \
    --input-dir ~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files \
    --dry-run
```

### 命令行参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--input-dir` | 必需 | 轨迹 JSON 文件目录 |
| `--out-dir` | `./gazebo_recordings` | 输出目录 |
| `--pattern` | `*.json` | JSON 文件匹配模式 |
| `--uav-ids` | `0` | UAV ID 列表，逗号分隔 |
| `--namespace-prefix` | `/uav` | ROS2 命名空间前缀 |
| `--scale` | `0.01` | 坐标缩放因子 |
| `--max-points` | `0` | 最大航点数（0=不限制）|
| `--z-down` / `--z-up` | `--z-down` | Z轴方向 |
| `--waypoint-timeout` | `120` | 航点超时时间（秒）|
| `--skip-existing` | 是 | 跳过已存在的轨迹 |
| `--dry-run` | 否 | 仅扫描文件不执行 |

---

## 数据格式

### 输入 JSON 格式

与原有格式完全兼容：

```json
{
  "raw_logs": [
    [x, y, z, roll, yaw, pitch]  // 初始位置
  ],
  "preprocessed_logs": [
    [x, y, z, roll, yaw, pitch],  // 轨迹点序列
    [x, y, z, roll, yaw, pitch],
    ...
  ]
}
```

**坐标系说明**：
- 输入坐标系: ENU（东-北-天）
- 内部转换为 NEU（北-东-天）后发送给 PX4

### 输出目录结构

```
<out_dir>/
├── collection_status.csv          # 全局采集状态日志
└── <traj_name>/
    └── uav<id>/
        ├── dynamics_data.csv      # 动力学数据（主文件）
        └── px4_uav<id>_<ts>.ulg   # PX4 飞行日志
```

### dynamics_data.csv 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `traj_json` | string | 源 JSON 文件路径 |
| `traj_name` | string | 轨迹名称 |
| `uav_id` | int | UAV ID |
| `step_idx` | int | 步骤索引 |
| `timestamp` | float | 采集时间戳（Unix时间） |
| `cmd_in_x/y/z` | float | 输入坐标（变换前）|
| `cmd_in_roll/yaw/pitch_deg` | float | 输入姿态角 |
| `cmd_x/y/z` | float | 命令坐标（变换后）|
| `obs_pos_x/y/z` | float | 观测位置（原始）|
| `obs_aligned_x/y/z` | float | 观测位置（对齐后）|
| `origin_offset_x/y/z` | float | 坐标系偏移量 |
| `obs_att_w/x/y/z` | float | 观测四元数姿态 |
| `obs_linvel_x/y/z` | float | 观测线速度 |
| `obs_angvel_x/y/z` | float | 观测角速度 |
| `ulg_path` | string | PX4 日志文件路径 |

---

## 多机并行性能

### 推荐配置

| UAV 数量 | CPU 核心 | 内存 | 预计采集速度 |
|----------|----------|------|--------------|
| 1 | 2 | 4GB | 1x |
| 4 | 8 | 16GB | ~3.5x |
| 8 | 16 | 32GB | ~6x |
| 16 | 32 | 64GB | ~10x |

### 性能优化建议

1. **降低仿真实时因子**：如果系统负载过高，可以降低仿真速度
   ```bash
   export PX4_SIM_SPEED_FACTOR=0.5
   ```

2. **禁用 GUI**：在纯数据采集场景下禁用 Gazebo GUI
   ```bash
   gazebo --verbose -s libgazebo_ros_factory.so empty.world
   ```

3. **使用 headless 模式**：
   ```bash
   HEADLESS=1 python3 launch_multi_uav.py --num-uavs 8
   ```

---

## 故障排除

### 问题 1: MAVROS 无法连接

**症状**: `MAVROS connection timeout`

**解决方案**:
```bash
# 检查 PX4 是否在运行
ps aux | grep px4

# 检查端口是否正确
netstat -tulnp | grep 14540

# 重启 MAVROS
ros2 run mavros mavros_node --ros-args -r __ns:=/uav0 -p fcu_url:=udp://:14540@127.0.0.1:14580
```

### 问题 2: 解锁失败

**症状**: `arming timeout`

**解决方案**:
```bash
# 检查 PX4 状态
ros2 topic echo /uav0/mavros/state

# 确保 PX4 处于 STANDBY 状态
# system_status 应为 3，landed_state 应为 1
```

### 问题 3: Mission 上传失败

**症状**: `failed to upload mission`

**解决方案**:
```bash
# 检查 MAVROS mission 服务是否可用
ros2 service list | grep mission

# 确保 PX4 已完成初始化（等待更长时间）
```

### 问题 4: Gazebo 崩溃

**症状**: 多机启动后 Gazebo 崩溃

**解决方案**:
```bash
# 减少 UAV 数量
# 增加启动间隔
# 检查 GPU 驱动
nvidia-smi  # 如果使用 NVIDIA GPU
```

### 问题 5: 无法找到 ULG 文件

**症状**: `ulg_path` 为空

**解决方案**:
```bash
# 检查 PX4 日志目录
ls -la /tmp/px4_*

# 确保 PX4 正确记录日志
# 检查 ~/.ros/log 目录
```

---

## 与 Isaac Sim 版本的对比

| 特性 | Isaac Sim 版本 | Gazebo 版本 |
|------|----------------|-------------|
| 图像采集 | 支持 | 不支持（仅动力学）|
| 仿真精度 | 高 | 中 |
| 系统要求 | 高（需要 GPU）| 中 |
| 多机支持 | 8+ | 16+ |
| 安装复杂度 | 高 | 低 |
| 开源程度 | 部分 | 完全 |

---

## 联系与支持

如有问题，请检查：
1. ROS2 和 MAVROS 是否正确安装
2. PX4 SITL 是否正常编译和运行
3. 日志文件中的错误信息

日志位置：
- Gazebo: `/tmp/gazebo.log`
- PX4: `/tmp/px4_<id>_*/px4_<id>.log`
- MAVROS: `/tmp/mavros_<id>.log`
- 采集脚本: 标准输出
