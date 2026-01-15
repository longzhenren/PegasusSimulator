# 轨迹跟踪误差修复会话记录

**日期**: 2026-01-12
**问题**: 轨迹跟随误差约10m偏差
**状态**: 代码修复已完成，需要重启仿真验证

---

## 1. 核心问题分析

### 1.1 原始问题
- 轨迹跟随误差高达10m
- 仿真时间不同步（使用 `time.time()` 而非仿真时间戳）
- Yaw坐标系转换错误（ENU到NED映射不正确）
- HTTP请求阻塞导致控制循环jitter
- 终点判定逻辑不完善

### 1.2 涉及文件
- `examples/mavlink_sim_vehicle.py` - MAVLink控制器
- `examples/mavlink_trajectory_collector.py` - 轨迹采集器

---

## 2. 已完成的代码修复

### 2.1 Yaw坐标系修复 (mavlink_sim_vehicle.py:554-624)

**修改位置**: `_send_velocity_setpoint` 函数

**修改内容**:
```python
# Yaw坐标系转换: ENU -> NED
# ENU: East=0 (X轴), CCW正 (逆时针)
# NED: North=0 (X轴), CW正 (顺时针)
# 转换公式: NED_yaw = π/2 - ENU_yaw
ned_yaw = 0.5 * math.pi - yaw

# Yaw_rate方向取反: ENU CCW正 -> NED CW正
ned_yaw_rate = -yaw_rate
```

**原因**:
- 输入是 ENU (East=0, CCW正)
- PX4 需要 NED (North=0, CW正)
- 之前代码直接透传 yaw 值，导致机头指向偏差

### 2.2 仿真时间同步 (mavlink_trajectory_collector.py:970-1260)

**修改位置**: `_process_offboard_mode` 函数

**关键修改**:
```python
# 获取初始仿真时间戳
info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=5)
sim_start_ts = float(info.get("pose", {}).get("timestamp", time.time()))

# 在控制循环中使用仿真时间
current_sim_ts = float(pose.get("timestamp", 0))
elapsed = current_sim_ts - sim_start_ts

# 本地时间预测（当HTTP超时时）
wall_elapsed = time.time() - last_wall_time
current_sim_ts = last_sim_ts + wall_elapsed * sim_speed_factor  # sim_speed_factor=2.0
```

**原因**:
- 原代码使用 `time.time() - start_time`
- 在 `sim_speed_factor=2.0` 环境下，指令频率与仿真物理频率脱节

### 2.3 HTTP非阻塞优化 (mavlink_trajectory_collector.py:1215-1260)

**修改内容**:
```python
# HTTP请求节流（每N次控制循环获取一次观测，减少阻塞）
obs_fetch_interval = 5  # 每5次循环获取一次（10Hz观测，50Hz控制）
loop_counter = 0

# 缓存当前观测状态（初始化为aligned_start，避免[0,0,0]）
current_obs_pos = list(aligned_start)
current_obs_vel = [0, 0, 0]

# 在循环中
if loop_counter % obs_fetch_interval == 0:
    try:
        info = _fetch_all_info(self.image_base, self.uav_id, timeout=0.1, retries=1)
        # 更新观测值...
    except Exception:
        pass  # 保持上次有效的观测值
```

**原因**:
- 50Hz循环内同步HTTP请求导致严重jitter
- 15ms超时太短，导致大量请求失败
- 失败时观测值变成[0,0,0]

### 2.4 终点判定逻辑 (mavlink_trajectory_collector.py:1254-1300)

**修改内容**:
```python
# 终点判定参数
FINAL_DIST_THRESHOLD = 0.3  # 位置阈值（米）
FINAL_VEL_THRESHOLD = 0.1   # 速度阈值（m/s）

# 当elapsed > total_duration时，进入"FINAL mode"
if elapsed > total_duration:
    if not in_final_mode:
        in_final_mode = True
        final_mode_start = time.time()

    # 发送终点保持setpoint（零速度）
    cmd = {
        "cmd": "setpoint",
        "x": final_pos[0], "y": final_pos[1], "z": final_pos[2],
        "vx": 0.0, "vy": 0.0, "vz": 0.0,
        "yaw": end_state['yaw'],
        "yaw_rate": 0.0
    }

    # 检查是否满足终点条件
    if dist_to_final < FINAL_DIST_THRESHOLD and current_speed < FINAL_VEL_THRESHOLD:
        break  # 完成

    # 超时保护（最多10秒收尾）
    if time.time() - final_mode_start > 10.0:
        break
```

**原因**:
- 原逻辑时间一到就退出，忽略了物理惯性导致的滞后
- 需要等待UAV实际到达终点

### 2.5 每帧误差日志 (mavlink_trajectory_collector.py:1319-1330)

**修改内容**:
```python
# 计算并记录跟踪误差
tracking_error = math.sqrt(
    (current_obs_pos[0] - aligned_pos[0])**2 +
    (current_obs_pos[1] - aligned_pos[1])**2 +
    (current_obs_pos[2] - aligned_pos[2])**2
)
error_sum += tracking_error
error_count += 1

# 每0.5秒输出一次误差日志
if sample_idx % 25 == 0:
    self._log(f"[UAV{self.uav_id}] t={t:.2f}s Error={tracking_error:.3f}m obs=(...) cmd=(...)")
```

---

## 3. 测试结果

### 3.1 16UAV仿真测试（代码修复前的旧仿真进程）

```
[2026-01-12 15:26:45] avg_error=0.348m (从10m降低!)
[2026-01-12 15:26:45] final position error: 2.938m
```

**结论**: 平均误差从~10m降到~0.35m，效果显著

### 3.2 重启后1UAV仿真测试

```
[2026-01-12 16:07:21] t=0.00s Error=0.000m obs=(-0.00,0.00,3.00) cmd=(-0.00,0.00,3.00)
[2026-01-12 16:07:22] t=0.79s Error=3.337m obs=(-0.00,0.00,0.06) cmd=(1.58,0.00,3.00)
```

**问题**: UAV停在地面不动（z=0.06m），不响应setpoint

**根本原因**: PX4没有发送 `HIL_ACTUATOR_CONTROLS` 消息给Isaac Sim
- ARM命令通过HTTP成功
- OFFBOARD模式设置成功
- 但PX4内部没有正确触发HIL执行器输出

---

## 4. 架构说明

### 4.1 MAVLink连接架构

```
┌─────────────────┐                    ┌─────────────────┐
│   Isaac Sim     │                    │      PX4        │
│                 │                    │                 │
│ px4_mavlink_    │ ←─ tcpin:4560 ───→ │  HIL Sensor/    │
│ backend.py      │    (HIL lockstep)  │  Actuator       │
│                 │                    │                 │
└─────────────────┘                    └─────────────────┘

┌─────────────────┐                    ┌─────────────────┐
│ mavlink_sim_    │                    │      PX4        │
│ vehicle.py      │ ─→ udpout:14580 ─→ │  MAVLink        │
│ (HTTP API)      │    (commands)      │  Onboard        │
└─────────────────┘                    └─────────────────┘
```

### 4.2 数据流

1. Isaac Sim 通过 `tcpin:localhost:4560` 发送HIL传感器数据
2. 外部客户端通过 `udpout:127.0.0.1:14580` 发送控制命令
3. PX4处理命令，生成电机控制
4. PX4通过 `HIL_ACTUATOR_CONTROLS` 返回执行器控制
5. Isaac Sim接收并应用到物理引擎

---

## 5. 当前状态

### 5.1 已修复（代码层面）
- [x] Yaw坐标系转换 (ENU → NED)
- [x] 仿真时间同步
- [x] HTTP非阻塞优化
- [x] 终点判定逻辑
- [x] 每帧误差日志
- [x] 速度命令记录到CSV

### 5.2 待验证
- [ ] 重启完整仿真后测试误差是否降低到0.5m以内
- [ ] 检查TrajectorySmoother中的np.unwrap是否正确处理Roll/Pitch/Yaw

### 5.3 已知问题
- 仿真重启后PX4-Isaac Sim HIL通信可能需要时间建立
- 需要等待PX4完全初始化后再发送命令

---

## 6. 重启后操作步骤

### 6.1 启动仿真

```bash
cd /home/user/PegasusSimulator-5.1

# 启动16UAV仿真（推荐，已验证工作）
nohup /home/user/isaacsim-5.1.0/python.sh examples/mavlink_sim_vehicle.py \
  --config examples/multi_uav_config_16.json --headless > /tmp/sim_16uav.log 2>&1 &

# 等待2-3分钟让仿真完全初始化
sleep 180
```

### 6.2 检查仿真状态

```bash
# 检查HTTP端点
curl -s http://127.0.0.1:8081/health
curl -s http://127.0.0.1:5009/health

# 检查UAV位置
curl -s http://127.0.0.1:8081/uav/0/pose | python3 -m json.tool
```

### 6.3 运行轨迹测试

```bash
# 创建测试轨迹（如果不存在）
# 文件已创建: examples/test_collection_input/long_test_trajectory.json

# 运行测试
python3 examples/mavlink_trajectory_collector.py \
  --input-dir examples/test_collection_input \
  --pattern "long_test_trajectory.json" \
  --out-dir /tmp/trajectory_test_output \
  --uav-ids 0 \
  --scale 1.0 \
  --no-skip-existing \
  --image-timeout 5.0 \
  --cmd-timeout 60.0
```

### 6.4 检查结果

```bash
# 查看输出CSV
head -20 /tmp/trajectory_test_output/long_test_trajectory/uav0/data.csv

# 检查误差日志（在运行输出中查找）
# 期望: avg_error < 0.5m
```

---

## 7. 测试轨迹文件

已创建测试轨迹: `examples/test_collection_input/long_test_trajectory.json`

```json
{
  "description": "Long test trajectory for tracking error analysis (10 seconds, 50 points)",
  "raw_logs": [
    [0.0, 0.0, 3.0, 0.0, 0.0, 0.0]
  ],
  "preprocessed_logs": [
    [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    [0.2, 0.0, 0.0, 0.0, 0.0, 0.0],
    ... // 41个点，形成方形轨迹
    [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
  ]
}
```

轨迹路径: (0,0) → (2,0) → (2,2) → (0,2) → (0,0) 方形，高度3m

---

## 8. 关键配置

### 8.1 PX4端口配置 (px4-rc_minmal.mavlink)
- 本地端口: `14580 + instance`
- 远端端口: `14740 + instance`

### 8.2 仿真速度
- `sim_speed_factor = 2.0` (仿真比实时快2倍)

### 8.3 控制频率
- 控制循环: 50Hz
- 观测获取: 10Hz (每5次循环)
- 数据采样: 50Hz

---

## 9. 故障排除

### 9.1 UAV不响应setpoint
1. 检查OFFBOARD模式是否设置: `curl -s http://127.0.0.1:5009/command -X POST -d '{"cmd":"get_status"}'`
2. 检查是否armed
3. 检查仿真日志: `tail -50 /tmp/sim_16uav.log`
4. 等待更长时间让PX4完全初始化

### 9.2 误差仍然很大
1. 检查yaw转换是否正确
2. 检查仿真时间戳是否有效
3. 验证setpoint坐标系

### 9.3 HTTP请求超时
1. 增加timeout参数
2. 检查仿真进程是否正常运行
3. 检查端口是否正确

---

## 10. 相关文件列表

修改的文件:
- `examples/mavlink_sim_vehicle.py` - Yaw坐标转换修复
- `examples/mavlink_trajectory_collector.py` - 时间同步、HTTP优化、终点判定

新建的文件:
- `examples/test_collection_input/long_test_trajectory.json` - 测试轨迹
- `docs/trajectory_tracking_fix_session.md` - 本文档

---

*文档创建时间: 2026-01-12 16:30*
