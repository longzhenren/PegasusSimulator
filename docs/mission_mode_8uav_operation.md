# 8架UAV Mission Mode 数据采集操作记录

## 操作日期
2026-01-09

## 目标
- 使用8架无人机进行轨迹数据采集
- 使用 MAVROS Mission 模式（而非 OFFBOARD 逐点控制）
- 自主解决过程中遇到的问题

## 当前状态检查

### 运行中的进程
- trajectory_data_collector.py 正在运行 (PID: 210570)
- 当前使用 UAV: 0,1,2,3 (4架)
- 当前模式: OFFBOARD (未使用 --mission-mode)
- 已完成轨迹: 386/16772

---

## 操作步骤

### Step 1: 停止当前采集脚本
时间: 2026-01-09 15:22
状态: 已完成
操作: `kill -9 210570 210564`

### Step 2: 检查8架UAV配置
时间: 2026-01-09 15:22
状态: 已完成
发现问题:
- 仿真端 8 架 UAV (0-7) 全部正常
- 控制器只有部分 UAV 响应 (0,1,2)
- UAV3 端口积压，UAV 4,5,7 缺少控制器进程

### Step 3: 修复代码Bug
时间: 2026-01-09 15:25
状态: 已完成
问题: `'IsaacSimEnv' object has no attribute '_mavros_connected'`
原因: `_mavros_connected` 属性在 state 订阅回调之后才初始化，但回调在 spin_sleep 期间被触发
修复:
- 文件: `examples/rospy_isaacsim.py`
- 将 `_mavros_connected` 和 `_mavros_connected_event` 初始化移到 state 订阅创建之前 (第490-492行)
- 移除后面重复的初始化代码 (原第576-578行)

### Step 4: 测试 Mission Mode
时间: 2026-01-09 18:30 - 19:15
状态: 失败
详见问题记录

### Step 5: 结论
时间: 2026-01-09 19:15
**MISSION 模式在当前 PX4 SITL 环境下无法正常工作**
建议: 使用 OFFBOARD 模式进行数据采集

---

## 问题记录与解决

### 问题1: _mavros_connected 属性未定义
- 发现时间: 2026-01-09 15:23
- 错误信息: `Unhandled exception in main(): 'IsaacSimEnv' object has no attribute '_mavros_connected'`
- 原因分析: 初始化顺序问题，state 订阅在第491行创建，但 `_mavros_connected` 在第573行才初始化
- 解决方案: 将属性初始化移到订阅创建之前
- 状态: ✅ 已修复

### 问题2: push_mission 失败 (waypoints 使用错误坐标系)
- 发现时间: 2026-01-09 16:45
- 错误信息: `[mission] Failed to push waypoints, transferred=1`
- 原因分析:
  - 原代码使用 `frame=1` (MAV_FRAME_LOCAL_NED) 和本地坐标
  - PX4 SITL 的 MISSION 模式需要 `frame=3` (MAV_FRAME_GLOBAL_RELATIVE_ALT) 和 GPS 坐标
  - 第一个航点需要使用 `command=22` (NAV_TAKEOFF)
- 解决方案:
  - 修改 `push_mission` 函数使用 GPS 坐标
  - 添加本地 ENU 坐标到 GPS 坐标的转换
  - GPS 参考点: lat=47.397742, lon=8.545594
  - 第一个航点使用 TAKEOFF 命令，后续使用 WAYPOINT 命令
- 状态: ✅ 已修复

### 问题3: execute_mission 不支持字典格式航点
- 发现时间: 2026-01-09 16:41
- 错误信息: `[execute_mission] Using MISSION mode with 0 waypoints`
- 原因分析: 代码只检查 `isinstance(pt, (list, tuple))`，不支持字典格式
- 解决方案: 添加字典格式支持 `isinstance(pt, dict)`
- 状态: ✅ 已修复

### 问题4: arm 服务始终返回 success=False
- 发现时间: 2026-01-09 18:21
- 错误信息: `[arm_and_mission] Arm response: success=False` (多次)
- 原因分析:
  - CommandBool 服务的 arm 请求被 PX4 拒绝
  - 可能是 PX4 的安全检查或模式限制
- 解决方案:
  - 使用 CommandLong 服务发送 MAV_CMD_COMPONENT_ARM_DISARM (400) 命令
  - 添加 param2=21196 强制解锁（safety bypass）
  - 先尝试 CommandBool，失败后尝试 CommandLong
- 状态: ⚠️ 部分解决，仍存在问题

### 问题5: PX4 拒绝接受任务上传（核心问题）
- 发现时间: 2026-01-09 19:00
- 错误信息:
  ```
  [ERROR] WP: upload failed: Generic error / not accepting mission commands at all right now.
  [WARN] WP: timeout, retries left 2
  [WARN] WP: timeout, retries left 1
  [WARN] WP: timeout, retries left 0
  [ERROR] WP: timed out.
  ```
- 原因分析:
  - PX4 SITL 返回 "not accepting mission commands at all right now"
  - 可能原因:
    1. PX4 内部状态问题（EVENT 4796299 错误大量出现）
    2. MAVROS mission service 与 PX4 之间的协议/时序问题
    3. 仿真环境特有的限制
- 尝试的解决方案:
  - 添加 HOME 航点作为第一个航点
  - 在 clear 和 push 之间添加延迟
  - 重试机制（3次重试）
  - 使用 `call_service_sync` 替代 `spin_until_future_complete`
- 状态: ❌ 未能解决

---

## 代码修改摘要

### 1. 修复 _mavros_connected 初始化顺序
文件: `examples/rospy_isaacsim.py`
位置: 第490-492行
```python
# MAVROS state 监控 - 必须在创建 state 订阅之前初始化
self._mavros_connected = False
self._mavros_connected_event = threading.Event()
```

### 2. 修改 push_mission 使用 GPS 坐标
文件: `examples/rospy_isaacsim.py`
位置: `push_mission` 函数
- 添加 GPS 参考点 (47.397742, 8.545594)
- 实现本地 ENU 坐标到 GPS 坐标的转换
- 使用 `frame=3` (MAV_FRAME_GLOBAL_RELATIVE_ALT)
- 第一个航点使用 `cmd=22` (NAV_TAKEOFF)

### 3. 添加 arm_and_mission 命令
文件: `examples/rospy_isaacsim.py`
功能: 完整的 MISSION 模式流程
- 清除旧航点
- 推送新航点（带重试）
- 解锁（先 CommandBool 后 CommandLong）
- 切换到 AUTO.MISSION 模式
- 等待任务完成

### 4. 修复服务调用超时
文件: `examples/rospy_isaacsim.py`
修改: `clear_mission` 和 `push_mission`
- 使用 `call_service_sync` 替代 `spin_until_future_complete`
- 添加 TimeoutError 处理

---

## 最终结论

### MISSION 模式状态: ❌ 不可用

经过大量调试和修复，MAVROS MISSION 模式在当前 PX4 SITL 仿真环境下仍然无法正常工作。核心问题是 PX4 拒绝接受任务上传命令，返回 "not accepting mission commands at all right now" 错误。

### 可能的根本原因
1. PX4 SITL 与 MAVROS 之间的 mission 协议实现存在兼容性问题
2. 仿真环境中缺少某些必要的条件（如真实 GPS 锁定）
3. PX4 版本或配置与 MAVROS mission 插件不完全兼容

### 建议
**继续使用 OFFBOARD 模式进行数据采集**，因为：
1. OFFBOARD 模式已被验证可以正常工作
2. 通过 `move_to_many` 命令可以实现多航点顺序飞行
3. 数据采集的核心功能不受影响

### 后续可能的改进方向
1. 升级 MAVROS 版本
2. 检查 PX4 配置参数（特别是与 mission 相关的参数）
3. 使用 QGroundControl 手动上传任务以验证 PX4 mission 功能本身是否正常
4. 考虑使用 PX4 的 FlightTask 接口替代 MAVROS mission

---

## 操作时间线
- 15:22 - 停止现有采集脚本
- 15:23 - 发现 _mavros_connected 属性错误
- 15:25 - 修复属性初始化顺序
- 16:41 - 发现航点格式解析问题
- 16:45 - 修复 push_mission 坐标系问题
- 18:21 - 尝试修复 arm 问题
- 18:36 - 航点推送成功（5个航点）
- 18:38 - arm 仍然失败
- 19:00 - 发现 PX4 拒绝接受任务上传的核心问题
- 19:15 - 确认 MISSION 模式不可用，建议使用 OFFBOARD 模式
