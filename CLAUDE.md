# PegasusSimulator 项目指南

## 常用启动命令

### 1. 启动仿真器 (Headless模式)
```bash
cd /home/user/PegasusSimulator-5.1/examples
./start_headless_sim.sh
```
或直接运行：
```bash
cd /home/user/PegasusSimulator-5.1/examples
~/.local/share/ov/pkg/isaac-sim-4.2.0/python.sh mavlink_sim_vehicle.py --headless
```

### 2. 运行单机多轨迹测试
```bash
cd /home/user/PegasusSimulator-5.1/examples
./test_single_uav_multi_traj.sh
```
- 测试5条轨迹连续采集
- 输出目录: `/tmp/single_uav_multi_traj_test_YYYYMMDD_HHMMSS/`
- 需要等待120秒让仿真器完全就绪

### 3. 分析采集的轨迹
```bash
cd /home/user/PegasusSimulator-5.1/examples
python analyze_collected_trajectory.py /tmp/single_uav_multi_traj_test_XXXXXX/
```

### 4. 清理残留进程
```bash
# 清理仿真器
pkill -9 -f "mavlink_sim_vehicle"
pkill -9 -f "isaac-sim"

# 清理PX4
pkill -9 -f "px4_sitl"
rm -rf /tmp/px4_instance_* /tmp/px4-*
```

## 关键文件

| 文件 | 用途 |
|------|------|
| `examples/mavlink_sim_vehicle.py` | 仿真主入口 |
| `examples/simple_trajectory_collector.py` | 轨迹采集器 |
| `examples/analyze_collected_trajectory.py` | 轨迹分析脚本 |
| `examples/test_single_uav_multi_traj.sh` | 多轨迹测试脚本 |
| `pegasus/.../px4_launch_tool.py` | PX4进程管理 |

## 已知问题与状态

### 已解决
- [x] 控制空白期导致UAV漂移4.31m -> 通过hold setpoints解决
- [x] PX4 "server already running"超时 -> 通过cleanup_px4_residuals()解决

### 待解决
- [ ] 多轨迹连续采集时PX4 EKF坐标系未重置，导致误差累积
  - 第1条轨迹RMSE=0.35m（正常）
  - 后续轨迹误差累积到14-29m
  - 需要在teleport后正确重置EKF状态

## 端口配置
- HTTP Controller: 8081
- MAVLink UDP: 14580
- Isaac Sim Web: 8211
