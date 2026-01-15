# Fast Controller 使用说明

## 概述

`fast_controller.py` 是根据 `controller.md` 文档设计的快速姿态控制器,专门用于四旋翼无人机控制。该控制器采用级联 PID 控制架构,提供高性能的轨迹跟踪能力。

**重要更新：** 现在完全兼容 `trajectory_data_collector.py` 的输入输出格式！

## 控制架构

该控制器实现了三层级联控制结构:

1. **外环: 位置控制**
   - 输入: 期望位置、速度、加速度
   - 输出: 期望加速度(世界坐标系)
   - 控制器类型: PID

2. **中环: 姿态控制**
   - 输入: 期望加速度、期望偏航角
   - 输出: 期望角速度(机体坐标系)
   - 控制器类型: 几何控制器 + PD

3. **内环: 角速度控制**
   - 输入: 期望角速度
   - 输出: 控制力矩
   - 控制器类型: PID

## 控制参数(来自 controller.md)

### 机器人参数
- **质量**: 10.0 kg
- **转动惯量**: diag([7.0, 5.0, 9.0]) × 10⁻³ kg·m²
- **重力加速度**: 9.81 m/s²

### 姿态控制增益
- **KAng** (姿态比例增益): [15.0, 15.0, 12.0]
- **Kdang** (姿态微分增益): [0.0, 0.0, 0.0] (未启用)

### 角速度控制增益
- **KAng_vel** (角速度比例增益): [0.2, 0.15, 0.32]
- **KiAng_vel** (角速度积分增益): [0.2, 0.2, 0.1]
- **KiAng_vel_max** (积分限幅): [1.0, 0.7, 0.5]

### 位置控制增益(保留自原始控制器)
- **Kp** (位置比例增益): [10.0, 10.0, 10.0]
- **Kd** (位置微分增益): [8.5, 8.5, 8.5]
- **Ki** (位置积分增益): [1.50, 1.50, 1.50]

### 姿态角度限制(安全约束)
- **max_roll_deg** (最大滚转角): 45.0° (默认)
- **max_pitch_deg** (最大俯仰角): 45.0° (默认)
- **max_yaw_rate_deg** (最大偏航角速度): 90.0°/s (默认)

## 轨迹文件格式

### JSON 格式 (推荐 - 兼容 trajectory_data_collector.py)

```json
{
  "raw_logs": [
    [x, y, z, roll, yaw, pitch]  // 初始位置 (ENU坐标系)
  ],
  "preprocessed_logs": [
    [x, y, z, roll, yaw, pitch],  // 轨迹点1 (ENU坐标系)
    [x, y, z, roll, yaw, pitch],  // 轨迹点2
    ...
  ]
}
```

**坐标系说明：**
- 输入坐标系: ENU (东-北-天)
- 控制器会自动转换为 NEU (北-东-天): x↔y 交换
- 支持 `scale` 参数缩放坐标
- 支持 `z_down` 参数控制Z轴方向

### CSV 格式 (传统格式)

CSV 文件应包含以下列(按顺序):
1. **时间** (s)
2-4. **位置** px, py, pz (m)
5-7. **速度** vx, vy, vz (m/s)
8-10. **加速度** ax, ay, az (m/s²)
11-13. **加加速度** jx, jy, jz (m/s³)
14. **偏航角** yaw (rad)
15. **偏航角速度** yaw_rate (rad/s)

## 使用示例

### 使用 JSON 轨迹 (兼容 trajectory_data_collector.py)

```python
from utils.fast_controller import FastController

# 使用与数据采集脚本相同的JSON轨迹文件
controller = FastController(
    trajectory_file="/path/to/trajectory.json",
    results_file="/path/to/results.npz",
    scale=0.01,          # 与数据采集脚本相同的缩放因子
    z_down=True,         # 与数据采集脚本相同的Z轴方向
    uav_id=0             # UAV ID
)

vehicle.set_backend(controller)
```

### 使用 CSV 轨迹 (传统格式)

```python
from utils.fast_controller import FastController

controller = FastController(
    trajectory_file="/path/to/trajectory.csv",
    results_file="/path/to/results.npz"
)

vehicle.set_backend(controller)
```

### 基本使用(内置轨迹)

```python
from utils.fast_controller import FastController

# 创建控制器实例(使用默认参数)
controller = FastController()

# 将控制器设置到无人机
vehicle.set_backend(controller)
```

### 自定义控制增益和角度限制

```python
from utils.fast_controller import FastController

# 自定义控制参数
controller = FastController(
    # 位置控制增益
    Kp=[12.0, 12.0, 12.0],
    Kd=[9.0, 9.0, 9.0],
    Ki=[2.0, 2.0, 2.0],

    # 姿态控制增益
    KAng=[18.0, 18.0, 15.0],
    Kdang=[0.5, 0.5, 0.5],

    # 角速度控制增益
    KAng_vel=[0.25, 0.18, 0.35],
    KiAng_vel=[0.25, 0.25, 0.15],
    KiAng_vel_max=[1.2, 0.8, 0.6],

    # 姿态角度限制 (安全约束)
    max_roll_deg=30.0,      # 限制最大滚转角为30度
    max_pitch_deg=30.0,     # 限制最大俯仰角为30度
    max_yaw_rate_deg=60.0   # 限制最大偏航角速度为60度/秒
)

vehicle.set_backend(controller)
```

## 输出格式

控制器会保存统计数据到以下格式:

### 1. NPZ 格式 (用于绘图)

保存到 `results_file` 指定的路径，包含：
- `time`: 时间序列
- `p`: 位置
- `desired_p`: 期望位置
- `ep`: 位置误差
- `ev`: 速度误差
- `er`: 姿态误差
- `ew`: 角速度误差
- `attitude`: 姿态四元数 (w, x, y, z)
- `angular_velocity`: 角速度
- `linear_acceleration`: 线性加速度
- `thrust_magnitude`: 推力大小
- `torque`: 控制力矩

### 2. CSV 格式 (兼容 trajectory_data_collector.py)

保存到 `results_file` 的父目录，生成两个文件:

#### `data.csv` - 主数据文件

包含字段:
- `traj_json`: 轨迹文件路径
- `traj_name`: 轨迹名称
- `uav_id`: UAV ID
- `step_idx`: 步骤索引
- `time_s`: 时间戳
- `cmd_x/y/z`: 命令位置
- `cmd_roll/yaw/pitch_deg`: 命令姿态角
- `obs_pos_x/y/z`: 观测位置
- `obs_att_w/x/y/z`: 观测姿态四元数
- `obs_linvel_x/y/z`: 观测线速度
- `obs_angvel_x/y/z`: 观测角速度
- `obs_linacc_x/y/z`: 观测线性加速度
- `pos_error_x/y/z`: 位置误差
- `att_error_x/y/z`: 姿态误差

#### `all_pose_data.csv` - 简化位姿数据

包含字段:
- `traj_json`, `traj_name`, `uav_id`, `step_idx`, `time_s`
- `pos_x/y/z`: 位置
- `cmd_roll/yaw/pitch_deg`: 命令姿态
- `att_w/x/y/z`: 姿态四元数
- `linvel_x/y/z`: 线速度
- `angvel_x/y/z`: 角速度
- `linacc_x/y/z`: 线性加速度

## 与 trajectory_data_collector.py 的集成

`fast_controller.py` 现在完全兼容数据采集脚本的格式:

```python
# 使用数据采集脚本生成的轨迹文件
controller = FastController(
    trajectory_file="./trajectories/my_trajectory.json",
    results_file="./results/my_results.npz",
    scale=0.01,      # 与数据采集脚本相同
    z_down=True,     # 与数据采集脚本相同
    uav_id=0
)

# 运行仿真...
vehicle.set_backend(controller)

# 停止时会自动保存兼容格式的CSV文件
# 输出:
#   ./results/data.csv
#   ./results/all_pose_data.csv
#   ./results/my_results.npz
```

## 与原始 NonlinearController 的主要区别

| 特性 | NonlinearController | FastController |
|------|---------------------|----------------|
| 姿态控制方法 | 几何控制器 | 级联PID控制 |
| 角速度控制 | PD控制 | PID控制(带积分抗饱和) |
| 机器人质量 | 1.50 kg | 10.0 kg |
| 转动惯量 | 未显式定义 | 显式定义(基于controller.md) |
| 积分控制 | 仅位置环 | 位置环 + 角速度环 |
| 抗饱和保护 | 无 | 有(角速度积分限幅) |
| 轨迹格式 | 仅CSV | CSV + JSON |
| 输出格式 | 仅NPZ | NPZ + CSV (兼容数据采集) |
| 坐标转换 | 无 | 支持ENU→NEU转换 |

## 性能特点

1. **鲁棒性**: 通过积分控制和抗饱和机制提高了抗干扰能力
2. **快速响应**: 优化的增益参数提供更快的姿态调整
3. **轨迹跟踪**: 三层级联控制实现精确的轨迹跟踪
4. **稳定性**: 基于controller.md的经过验证的控制参数
5. **兼容性**: 完全兼容trajectory_data_collector.py的输入输出格式

## 姿态角度限制说明

控制器提供了三个安全约束参数,用于限制无人机的姿态动作:

### 1. 滚转角限制 (max_roll_deg)
- **作用**: 限制无人机左右倾斜的最大角度
- **默认值**: 45°
- **原理**: 通过限制期望推力方向向量的倾斜角度实现
- **场景**: 防止激进的侧向机动导致失稳

### 2. 俯仰角限制 (max_pitch_deg)
- **作用**: 限制无人机前后倾斜的最大角度
- **默认值**: 45°
- **原理**: 通过限制期望推力方向向量的倾斜角度实现
- **场景**: 防止激进的前后加速导致失稳

### 3. 偏航角速度限制 (max_yaw_rate_deg)
- **作用**: 限制无人机绕Z轴旋转的最大角速度
- **默认值**: 90°/s
- **原理**: 直接限制期望偏航角速度
- **场景**: 防止过快的旋转导致控制不稳定

### 使用建议

**保守设置** (适合新手或测试):
```python
controller = FastController(
    max_roll_deg=20.0,      # 温和的侧向机动
    max_pitch_deg=20.0,     # 温和的前后加速
    max_yaw_rate_deg=45.0   # 缓慢旋转
)
```

**标准设置** (默认值,适合正常飞行):
```python
controller = FastController(
    max_roll_deg=45.0,      # 标准机动能力
    max_pitch_deg=45.0,     # 标准加速能力
    max_yaw_rate_deg=90.0   # 标准旋转速度
)
```

**激进设置** (适合竞速或特技):
```python
controller = FastController(
    max_roll_deg=60.0,      # 激进的侧向机动
    max_pitch_deg=60.0,     # 快速加速
    max_yaw_rate_deg=180.0  # 快速旋转
)
```

### 限制触发行为

当姿态角超过限制时:
1. **滚转/俯仰限制**: 控制器会自动将期望推力方向限制在允许的倾斜角内,并输出警告日志
2. **偏航角速度限制**: 控制器会将期望偏航角速度限制到最大值

警告日志示例:
```
[WARN] Attitude angle limit applied: tilt=52.3° > max=45.0°
```

## 注意事项

1. 该控制器专为**10kg级无人机**设计,如果使用不同质量的无人机,需要调整控制增益
2. 控制增益已针对特定惯性参数优化,更改机器人参数时应重新调整增益
3. 积分限幅参数 `KiAng_vel_max` 对于防止积分饱和很重要,应根据实际情况调整
4. 该控制器**不包含**机械臂/夹爪控制功能
5. 使用JSON轨迹时,确保 `scale` 和 `z_down` 参数与数据采集脚本一致
6. **姿态角度限制**是安全约束,设置过小会影响轨迹跟踪性能,设置过大可能导致不稳定

## 调试和性能分析

### 保存和分析统计数据

```python
controller = FastController(
    trajectory_file="trajectory.json",
    results_file="results.npz"  # 保存统计数据
)

# 运行仿真...

# 分析NPZ结果
import numpy as np
data = np.load("results.npz")
print("时间:", data["time"])
print("位置误差:", data["ep"])
print("速度误差:", data["ev"])
print("姿态误差:", data["er"])
print("角速度误差:", data["ew"])

# 分析CSV结果 (兼容数据采集格式)
import pandas as pd
df_data = pd.read_csv("results/data.csv")
df_pose = pd.read_csv("results/all_pose_data.csv")
```

### 绘制轨迹跟踪性能

```python
import matplotlib.pyplot as plt
import numpy as np

# 加载数据
data = np.load("results.npz")

# 绘制位置跟踪
fig, axes = plt.subplots(3, 1, figsize=(10, 8))
labels = ['X', 'Y', 'Z']
for i, ax in enumerate(axes):
    ax.plot(data['time'], data['p'][:, i], label='Actual')
    ax.plot(data['time'], data['desired_p'][:, i], label='Desired', linestyle='--')
    ax.set_ylabel(f'Position {labels[i]} (m)')
    ax.legend()
    ax.grid()
axes[-1].set_xlabel('Time (s)')
plt.tight_layout()
plt.savefig('position_tracking.png')

# 绘制位置误差
plt.figure(figsize=(10, 6))
for i, label in enumerate(labels):
    plt.plot(data['time'], data['ep'][:, i], label=f'{label} error')
plt.xlabel('Time (s)')
plt.ylabel('Position Error (m)')
plt.legend()
plt.grid()
plt.savefig('position_error.png')
```

## 参考文献

控制器基于以下文献和文档:
- `controller.md` - 项目内部控制器设计文档
- `trajectory_data_collector.py` - 轨迹数据采集脚本格式规范
- [1] J. Pinto et al., "Planning Parcel Relay Manoeuvres for Quadrotors," ICUAS 2021
- [2] D. Mellinger and V. Kumar, "Minimum snap trajectory generation and control for quadrotors," ICRA 2011

## 常见问题

### Q: 如何从数据采集脚本的输出切换到控制器的输入？

A: 数据采集脚本生成的JSON文件可以直接用作控制器的输入:

```python
# 使用数据采集脚本生成的轨迹
controller = FastController(
    trajectory_file="./trajectories/recorded_trajectory.json",
    scale=0.01,  # 与数据采集脚本相同的缩放
    z_down=True  # 与数据采集脚本相同的Z轴设置
)
```

### Q: 为什么我的CSV输出与数据采集脚本的不完全一样？

A: 控制器的CSV输出包含相同的字段,但不包含图像相关字段 (`image_timestamp_s`, `image_path`)。其他位姿数据字段完全兼容。

### Q: 如何调整控制器以适应不同质量的无人机？

A: 修改质量参数并按比例调整增益:

```python
controller = FastController(
    # 质量参数
    Kp=[10.0 * (new_mass/10.0), ...],  # 按质量比例调整
    # ... 其他参数
)
# 或直接在代码中修改 self.m
```

### Q: JSON轨迹的坐标转换逻辑是什么？

A:
1. 从JSON读取ENU坐标 (x, y, z)
2. 交换x和y得到NEU坐标: (y, x, z)
3. 应用缩放和基准偏移:
   - `x_final = base_x * scale + y * scale`
   - `y_final = base_y * scale + x * scale`
   - `z_final = base_z * scale ± z * scale` (取决于z_down)

### Q: 如何选择合适的姿态角度限制？

A: 根据应用场景选择:
- **室内飞行/狭窄空间**: 使用保守设置 (20-30°)
- **正常轨迹跟踪**: 使用标准设置 (45°,默认)
- **快速机动/竞速**: 使用激进设置 (60°+)

如果发现轨迹跟踪误差过大且日志中频繁出现角度限制警告,说明限制过于严格,应适当增大角度限制。

### Q: 姿态角度限制会影响轨迹跟踪性能吗？

A: 会的。过小的角度限制会导致:
- 无法产生足够的水平加速度
- 轨迹跟踪误差增大
- 响应速度变慢

建议根据实际轨迹的激进程度调整限制参数。对于温和的轨迹,默认的45°限制通常足够。
