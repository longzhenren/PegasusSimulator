# 仿真时间与PX4时间同步分析报告

**日期**: 2025-01-21
**作者**: Claude Code Analysis
**项目**: PegasusSimulator 多机仿真数据采集

---

## 1. 概述

本文档分析了 Isaac Sim 仿真时间与 PX4 SITL 内部时间不同步时，对轨迹数据采集质量的影响。

## 2. 时间同步架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           时间同步架构                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Isaac Sim 物理时间 (world.current_time)                                     │
│       │                                                                      │
│       ├──────► TimeBroadcaster ──UDP:14555──► (外部控制器可监听)              │
│       │                                                                      │
│       ├──────► StateRecorder.record(sim_time, ...) ──► raw_observations      │
│       │           └─ 记录: position, velocity, cmd_in, cmd                   │
│       │                                                                      │
│       ├──────► HTTP /sim_time ──► SimTimeListener ──► Collector              │
│       │                               │                                      │
│       │                               └─► wait_for_advance() 同步控制        │
│       │                                                                      │
│       └──────► HIL_SENSOR (含time_usec) ──► PX4 SITL                        │
│                                                │                             │
│                                                ▼                             │
│                                    PX4 EKF 时间 (boot_time_us)               │
│                                                │                             │
│                                    ┌───────────┴───────────┐                 │
│                                    ▼                       ▼                 │
│                              LOCAL_POSITION_NED      HIL_ACTUATOR_CONTROLS   │
│                              (带 time_boot_ms)        (带 time_usec)         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. 关键代码分析

### 3.1 仿真端时间获取 (mavlink_sim_vehicle.py)

```python
def get_sim_time() -> float:
    """获取Isaac Sim模拟器时间（秒）"""
    global _GLOBAL_WORLD
    if _GLOBAL_WORLD is not None:
        return _GLOBAL_WORLD.current_time  # ← 物理仿真时间
    timeline = omni.timeline.get_timeline_interface()
    return timeline.get_current_time()
```

### 3.2 状态记录 (StateRecorder)

```python
def record(self, sim_time: float, uav_id: int, state, px4_state, cmd_in, cmd):
    record = {
        "sim_time": sim_time,              # ← Isaac Sim 时间戳
        "position": state.position.tolist(),
        "linear_velocity": state.linear_velocity.tolist(),
        "cmd_in": cmd_in,
        "cmd": cmd,
    }
    if px4_state:
        record["px4_position"] = px4_state.get("position", ...).tolist()
```

### 3.3 控制器端时间同步 (simple_trajectory_collector.py)

```python
class SimTimeListener(threading.Thread):
    def wait_for_advance(self, last_time: float, timeout: float = 1.0):
        """等待仿真时间前进后再发送下一条命令"""
        # 轮询 /sim_time 端点，等待时间推进

# 轨迹控制循环
current_sim_ts = time_listener.wait_for_advance(last_cmd_sim_ts)
elapsed = (current_sim_ts - sim_start_ts) * time_scale
state = smoother.get_full_state(elapsed + LOOKAHEAD_TIME * time_scale)
```

## 4. 时间不同步的后果

### 4.1 Setpoint 命令时机错配

**正常情况（Lockstep同步）：**
```
仿真时间: t=1.000s → 发送setpoint位置(10, 5, 2) → PX4收到时其内部时间也≈1.000s
                   → PX4 EKF认为"我在1.000s时应该到达(10, 5, 2)"
```

**时间漂移情况：**
```
仿真时间: t=1.000s → 发送setpoint位置(10, 5, 2)
PX4内部时间: t=0.800s  ← 落后200ms!
→ PX4 EKF认为"我在0.800s时应该到达(10, 5, 2)"
→ 但轨迹是按照1.000s计算的 → 命令领先实际位置
→ 造成过激控制或抖动
```

### 4.2 LOOKAHEAD 前馈计算失效

```python
LOOKAHEAD_TIME = 0.2  # 200ms 前馈
state = smoother.get_full_state(elapsed + LOOKAHEAD_TIME * time_scale)
```

**问题：**
- 前馈假设：发送命令后约200ms，UAV应该到达该位置
- 如果PX4时间落后：UAV控制过激，可能超调

### 4.3 观测数据时间戳不匹配

```python
record = {
    "sim_time": sim_time,              # Isaac Sim 物理时间
    "position": state.position,         # Isaac Sim 刚体位置 (真值)
    "px4_position": px4_state["position"],  # PX4 EKF估计位置
}
```

**问题：** `sim_time` 和 `px4_position` 使用不同的时间基准，直接比较会产生系统性偏差。

### 4.4 CMD-OBS 数据对齐错误

```python
cmd_times = np.array([r['sim_time'] for r in cmd_records])
obs_times = np.array([r.get('sim_time', 0) for r in raw_observations])
cmd_row[k] = float(np.interp(obs_time, cmd_times, cmd_arrays[k]))
```

当时间不同步时，插值对齐会产生错位。

## 5. 量化影响估算

| 时间偏差 | 速度2m/s时位置误差 | 加速度5m/s²时控制误差 |
|---------|-------------------|---------------------|
| 10ms    | 2cm               | 0.25mm              |
| 50ms    | 10cm              | 6.25mm              |
| 100ms   | 20cm              | 25mm                |
| 200ms   | 40cm              | 100mm               |

**关键发现：高速飞行时，时间偏差的影响显著放大！**

## 6. Lockstep 机制分析

### 6.1 Lockstep 工作原理 (px4_mavlink_backend.py)

```python
def poll_mavlink_messages(self):
    needs_to_wait_for_actuator = self._received_first_actuator and self._enable_lockstep
    while True:
        msg = self._connection.recv_match(blocking=needs_to_wait_for_actuator, timeout=0.1)
        # 阻塞等待 PX4 返回 HIL_ACTUATOR_CONTROLS
```

### 6.2 Lockstep 导致的问题

1. **单UAV阻塞全局物理**：一个慢的PX4进程会阻塞所有UAV的物理更新
2. **初始化死锁**：PX4等待传感器数据，仿真器等待心跳，形成死锁
3. **多机串行化**：8个UAV变成串行执行，效率极低

### 6.3 禁用 Lockstep 的风险

| 风险项 | Lockstep启用 | Lockstep禁用 |
|-------|-------------|-------------|
| 时间同步 | 强制同步 | 可能漂移 |
| 仿真加速 | 支持 (`sim_speed_factor=2.0`) | 不支持 |
| EKF估计质量 | 稳定 | 可能发散 |
| 数据采集对齐 | 精确 | 可能错位 |
| 多机并行 | 互相阻塞 | 独立运行 |

## 7. 解决方案对比

### 方案A：保留Lockstep，异步化Backend更新
- 优点：保持精确时间同步
- 缺点：实现复杂，需要大幅重构

### 方案B：禁用Lockstep + 额外时间补偿
- 优点：简单，多机独立运行
- 缺点：需要后处理对齐，可能有残余误差

### 方案C：多环境并行（推荐）
- 每个环境运行2-3架飞机
- 多个独立仿真进程并行
- 优点：保持Lockstep精度，避免互相阻塞
- 缺点：资源消耗较大

## 8. 结论与建议

对于高精度轨迹跟踪数据采集：
1. **时间同步是关键**：误差>50ms会显著影响控制质量
2. **单环境多机存在Lockstep阻塞问题**：8机场景下初始化超时
3. **推荐方案**：多环境并行（如4×2或3×3配置）

---

## 附录：相关代码文件

- `/home/user/PegasusSimulator-5.1/examples/mavlink_sim_vehicle.py`
- `/home/user/PegasusSimulator-5.1/examples/simple_trajectory_collector.py`
- `/home/user/PegasusSimulator-5.1/extensions/pegasus.simulator/pegasus/simulator/logic/backends/px4_mavlink_backend.py`
- `/home/user/PegasusSimulator-5.1/extensions/pegasus.simulator/pegasus/simulator/logic/backends/tools/px4_launch_tool.py`
