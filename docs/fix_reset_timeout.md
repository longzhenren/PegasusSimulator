# POST /reset 接口超时问题修复记录

## 问题描述

POST `/reset` 接口在连续调用时超时失败：
- 第一次 reset 成功（~36秒）
- 第二次及后续 reset 超时（60+秒），最终报错 `OFFBOARD not enabled within 60s`

## 根本原因分析

### 问题 1: `recv_match(blocking=True)` 无限阻塞

**位置**: `px4_mavlink_backend.py` 的 `poll_mavlink_messages()` 方法

**原因**:
- 在 lockstep 模式下，物理线程调用 `recv_match(blocking=True)` 等待 PX4 的 actuator 控制命令
- 当 Flask 线程调用 `recover_px4()` 关闭 MAVLink 连接时，物理线程的 `recv_match` 仍在阻塞
- 导致整个物理回调系统（包括所有 UAV）停止运行

**症状**:
- Isaac 日志显示 "PX4 recovery completed" 后完全没有传感器消息
- 只有 "Vehicle Manager is defined already" 每秒打印一次
- UAV0 和 UAV1 的 `update()` 回调都停止被调用

### 问题 2: 无人机姿态翻转未重置

**位置**: `rospy_isaacsim.py` 的 `reboot_px4_hard()` 方法

**原因**:
- `reset_uav()` 函数在 `yaw_deg=None` 时保留当前四元数姿态
- 如果第一次起飞失败/崩溃，无人机可能已翻转
- 第二次 reset 时保留了翻转姿态，导致 IMU Z 轴方向错误（+9.8 而非 -9.8）
- PX4 检测到 "Attitude failure (roll)" 无法通过预飞检查

### 问题 3: 恢复后时间跳跃

**位置**: `px4_mavlink_backend.py` 的 `recover_px4()` 和 `_update_impl()` 方法

**原因**:
- 即使设置了 `_current_utime=0`，恢复后第一个物理步的 `dt` 可能很大（30+秒）
- 导致 `_current_utime += int(dt * 1000000)` 立即跳到很大的值
- PX4 检测到 "Time jump"，EKF 重置，姿态估计失败

## 修复方案

### 修复 1: 为 recv_match 添加超时

**文件**: `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`

**修改位置**: 第 1295-1308 行

```python
# 修改前
msg = self._connection.recv_match(blocking=needs_to_wait_for_actuator)

# 修改后
# 关键修复：使用超时防止 blocking recv 无限阻塞
# 如果需要等待 actuator，使用 0.1 秒超时而不是无限阻塞
# 这样可以在恢复期间及时检测到 _connection=None 或 _is_running=False
recv_timeout = 0.1 if needs_to_wait_for_actuator else None
msg = self._connection.recv_match(blocking=needs_to_wait_for_actuator, timeout=recv_timeout)
```

### 修复 2: 强制重置姿态为水平

**文件**: `examples/rospy_isaacsim.py`

**修改位置**: 第 1252-1259 行

```python
# 修改前
if position is not None and isinstance(position, (list, tuple)) and len(position) >= 3:
    ts_log(log_prefix, f"Step 3: Moving UAV to ground position [{position[0]}, {position[1]}, 0.07] yaw={yaw_deg}")
    self._sim_move_uav([position[0], position[1], 0.07], yaw_deg)

# 修改后
if position is not None and isinstance(position, (list, tuple)) and len(position) >= 3:
    # 关键修复：硬重启时必须重置姿态为水平，否则可能保留崩溃时的翻转姿态
    # 如果 yaw_deg 为 None，使用默认值 0 确保无人机水平
    reset_yaw = yaw_deg if yaw_deg is not None else 0.0
    ts_log(log_prefix, f"Step 3: Moving UAV to ground position [{position[0]}, {position[1]}, 0.07] yaw={reset_yaw}")
    self._sim_move_uav([position[0], position[1], 0.07], reset_yaw)
```

### 修复 3: 跳过恢复后的大 dt

**文件**: `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`

**修改位置 1**: `recover_px4()` 方法（第 1004-1011 行）

```python
# 关键修复：重置仿真时间戳，新 PX4 进程期望从 0 开始的时间戳
self._current_utime = 0
# 关键修复：标记需要跳过下一个大的 dt 值
# 因为恢复过程中物理循环暂停，恢复后第一个 dt 可能很大（30+ 秒）
self._skip_large_dt_count = 10  # 跳过前 10 个帧的大 dt
ts_log(self._log_prefix, "Reset simulation time (_current_utime = 0, skip_large_dt_count = 10)")
```

**修改位置 2**: `_update_impl()` 方法（第 1197-1206 行）

```python
# Update the current u_time for px4
# 关键修复：恢复后的前几帧使用固定小 dt，防止时间跳跃
if hasattr(self, '_skip_large_dt_count') and self._skip_large_dt_count > 0:
    self._skip_large_dt_count -= 1
    # 使用固定的小 dt（基于配置的更新频率）
    dt = self._time_step
    if self._skip_large_dt_count == 9:  # 第一次跳过时打印日志
        ts_log(self._log_prefix, f"Skipping large dt, using fixed dt={dt:.6f}s")

self._current_utime += int(dt * 1000000)
```

### 修复 4: update() 异常处理

**文件**: `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`

**修改位置**: 第 1123-1138 行

```python
def update(self, dt):
    """
    Method that is called at every physics step to send data to px4 and receive the control inputs via mavlink
    """
    # 关键修复：包装整个 update() 方法在 try-except 中，防止异常传播到 Isaac Sim
    # 导致整个物理回调系统崩溃
    try:
        self._update_impl(dt)
    except Exception as e:
        ts_log(self._log_prefix, f"update() exception: {e}", "ERROR")
        ts_log(self._log_prefix, traceback.format_exc(), "ERROR")

def _update_impl(self, dt):
    # 原 update() 的实现代码移到这里
    ...
```

### 修复 5: 移除等待 heartbeat 时的 early return

**文件**: `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`

**修改位置**: 第 1155-1160 行

```python
# 修改前
if not self._received_first_hearbeat:
    self.wait_for_first_hearbeat()
    return  # 这里的 return 导致死锁

# 修改后
# 关键修复：不要在等待 heartbeat 时提前返回，因为 PX4 在 lockstep 模式下
# 需要先收到传感器数据才会发送 heartbeat
if not self._received_first_hearbeat:
    self.wait_for_first_hearbeat()
    # 不要 return，继续发送传感器数据以打破死锁
```

## 测试结果

修复后连续执行三次 reset 测试：

| 测试 | 起始位置 | 目标位置 | 耗时 | 结果 |
|------|----------|----------|------|------|
| 第一次 | 初始位置 | [0, 0, 2] | ~39秒 | 成功 |
| 第二次 | [0, 0, 2] | [5, 5, 3] | ~38秒 | 成功 |
| 第三次 | [5, 5, 3] | [0, 0, 2] | ~38秒 | 成功 |

## 相关文件

- `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`
- `examples/rospy_isaacsim.py`

## Git 提交

```
commit 1cb285b
fix(px4_mavlink_backend): 修复recv_match阻塞导致物理循环停止的问题
```

## 调试技巧

### 查看物理回调是否正常运行

检查 Isaac 日志中的 `update() entry` 日志：
```bash
grep "update() entry" logs/*/isaac/8_camera_vehicle.log | tail -20
```

正常情况下应该每秒看到约 120 条（按 120Hz 物理频率）。

### 查看 PX4 是否收到传感器数据

检查 PX4 日志中是否有 "poll timeout"：
```bash
grep "poll timeout" logs/*/px4/px4_0.log | wc -l
```

大量 "poll timeout" 表示 PX4 未收到传感器数据。

### 查看 IMU 数据方向

检查 IMU Z 轴加速度：
```bash
grep "imu_data=" logs/*/isaac/8_camera_vehicle.log | tail -5
```

正常应为约 -9.8（向下），如果是 +9.8 表示无人机翻转。
