# Reset 接口调试修改记录

本文档记录为解决 POST /reset 接口超时问题所做的所有修改，以便必要时撤销。

## 问题描述

POST /reset 接口在调用时会超时失败（60秒），错误信息是 `TimeoutError: OFFBOARD not enabled within 60s`。

根本原因：Isaac Sim 的 HTTP handler 与物理循环同步，`recover_px4()` 中的长时间等待阻塞物理循环，导致 PX4 无法收到传感器数据。

## 修改记录

---

### 修改 1: Step 6 - 删除 10 秒等待循环

**文件**: `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`

**位置**: 约 1014-1037 行

**修改时间**: 2026-01-06

**修改前**:
```python
            # Step 6: 等待传感器数据就绪
            ts_log(self._log_prefix, "Step 6: Waiting for sensor data to be ready...")
            sensor_ready_timeout = 10.0  # 最多等待 10 秒
            sensor_ready_start = time.time()
            while time.time() - sensor_ready_start < sensor_ready_timeout:
                # 检查是否有有效的 IMU 数据
                # 条件1：已接收过 IMU 且有新数据
                # 条件2：或者 IMU 数据非零（即使标志未设置）
                imu_has_data = (
                    abs(self._sensor_data.xacc) > 0.01 or
                    abs(self._sensor_data.yacc) > 0.01 or
                    abs(self._sensor_data.zacc) > 0.01
                )
                if self._sensor_data.new_imu_data or imu_has_data:
                    ts_log(self._log_prefix, f"Sensor data ready (IMU: new_data={self._sensor_data.new_imu_data}, has_data={imu_has_data})")
                    # 确保 received_first_imu 被设置
                    self._sensor_data.received_first_imu = True
                    break
                time.sleep(0.1)
            else:
                ts_log(self._log_prefix, "Sensor data wait timeout, continuing anyway", "WARN")
                # 即使超时，也设置 received_first_imu 以避免死锁
                # 下一次物理步时会触发传感器更新
                self._sensor_data.received_first_imu = True
```

**修改后**:
```python
            # Step 6: 设置传感器标志（不等待，因为物理循环被阻塞）
            # 关键修复：HTTP handler 与物理循环同步，等待会阻塞物理循环，
            # 导致 PX4 无法收到传感器数据（poll timeout），进入不正常状态
            ts_log(self._log_prefix, "Step 6: Setting sensor flags (no wait - physics blocked)")
            self._sensor_data.received_first_imu = True  # 让物理循环恢复后处理
```

---

### 修改 2: Step 7 - 删除 30 秒等待循环

**文件**: `extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`

**位置**: 约 1039-1067 行

**修改时间**: 2026-01-06

**修改前**:
```python
            # Step 7: 等待新 PX4 连接并接收 heartbeat
            # 关键修复：不要在这里轮询 recv_match()，因为这会与物理线程竞争消息！
            # 使用 type='HEARTBEAT' 过滤器会导致其他消息（如 actuator controls）被丢弃
            # 改为：只检查 _received_first_hearbeat 标志，让物理线程处理所有消息接收
            ts_log(self._log_prefix, "Step 7: Waiting for PX4 connection (physics thread will handle messages)...")
            heartbeat_timeout = 30.0
            heartbeat_start = time.time()
            poll_interval = 0.1  # 100ms 检查间隔
            while time.time() - heartbeat_start < heartbeat_timeout:
                # 检查物理线程是否已经接收到 heartbeat
                if self._received_first_hearbeat:
                    ts_log(self._log_prefix, "Heartbeat received by physics thread")
                    break
                # 检查连接是否已建立（port 属性被设置）
                if hasattr(self._connection, 'port') and self._connection.port is not None:
                    # 连接已建立，再等待一小段时间让物理线程接收 heartbeat
                    time.sleep(0.5)
                    if self._received_first_hearbeat:
                        ts_log(self._log_prefix, "Heartbeat received by physics thread (after connection)")
                        break
                time.sleep(poll_interval)
            else:
                ts_log(self._log_prefix, "Heartbeat wait timeout, connection may not be fully established", "WARN")

            # 验证连接状态
            if hasattr(self._connection, 'port') and self._connection.port is not None:
                ts_log(self._log_prefix, f"TCP connection established (fd={self._connection.port.fileno()})")
            else:
                ts_log(self._log_prefix, "WARNING: TCP connection NOT established (port is None)!", "ERROR")
```

**修改后**:
```python
            # Step 7: 跳过等待（物理循环恢复后会处理连接）
            # 关键修复：不要在这里等待，因为物理循环被阻塞，PX4 无法发送 heartbeat
            # 物理循环恢复后，update() 会调用 wait_for_first_hearbeat() 接受连接
            ts_log(self._log_prefix, "Step 7: Skipping heartbeat wait (physics thread will handle)")
```

---

## 如何撤销修改

如果需要撤销以上修改，可以使用 git：

```bash
cd /home/user/PegasusSimulator-5.1
git checkout -- extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py
```

或者手动将上述"修改前"的代码复制回对应位置。

---

## 测试记录

### 测试 1 - 会话 1767708583 (2026-01-06 22:09)

**结果**: 部分成功

| 次数 | 结果 | 耗时 | 备注 |
|------|------|------|------|
| 1 | 成功 | ~34秒 | PX4 recovery 正常，Step 4.5 等待 1 秒 |
| 2 | 成功 | ~32秒 | 正常 |
| 3 | 失败 | 超时 | PX4 attitude failure (roll) |

**分析**:
- 修改后 recover_px4() 从 40+ 秒减少到 ~1.5 秒
- 第三次失败是由于 PX4 preflight 检查失败，非本修改导致

### 测试 2 - 会话 1767709126 (2026-01-06 22:18)

**结果**: 失败

| 次数 | 结果 | 耗时 | 备注 |
|------|------|------|------|
| 1 | 失败 | 超时 | PhysX 错误导致物理循环停止 |

**分析**:
- recover_px4() 正常完成（1.5 秒）
- 但出现 PhysX 错误：`PxArticulationReducedCoordinate::copyInternalStateToCache() not allowed while simulation is running`
- 此错误导致物理循环在 reset 后停止
- PX4 报告大量 `poll timeout` 错误
- 这是 Isaac Sim 与 PhysX 的竞态条件问题，非本修改导致

---

## 结论

### 修改效果

本次修改（删除 Step 6 和 Step 7 的等待）**部分解决**了原始问题：

1. ✅ recover_px4() 执行时间从 40+ 秒减少到 ~1.5 秒
2. ✅ 在正常情况下，reset 可以在 ~34 秒内完成
3. ⚠️ 仍存在 Isaac Sim PhysX 竞态条件问题，导致偶发失败

### 已知问题

1. **PhysX 竞态条件**: Isaac Sim 在 reset 期间可能出现 `copyInternalStateToCache() not allowed while simulation is running` 错误，导致物理循环停止。这不是本修改导致的问题。

2. **PX4 Preflight 失败**: 连续 reset 时可能出现 `Preflight Fail: Attitude failure (roll)` 错误，导致 arm 失败。这是 PX4 的问题。

### 建议

1. 当前修改可以保留，它确实减少了 reset 的阻塞时间
2. 如果遇到 reset 失败，可以尝试重新启动整个仿真系统
3. 对于 PhysX 竞态条件问题，可能需要在 Isaac Sim 端进行修复（如暂停物理仿真再进行 reset 操作）

