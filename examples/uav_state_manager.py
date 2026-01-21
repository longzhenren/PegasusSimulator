# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
UAV State Manager - PX4 + MAVROS 联合状态机

==========================
概述
==========================
本模块实现 UAV 系统的显式状态机管理，负责：
- 聚合 PX4、MAVROS、飞行状态为统一视图
- 提供线程安全的状态查询和更新接口
- 管理状态转换和恢复操作
- 提供状态变化通知机制

==========================
状态分层
==========================
Layer 1: PX4 Backend State (底层)
  STOPPED → STARTING → CONNECTED → HEARTBEAT → READY

Layer 2: MAVROS State (中层)
  STOPPED → STARTING → CONNECTED → FCU_CONNECTED

Layer 3: Flight State (顶层)
  UNKNOWN → DISARMED → ARMED → OFFBOARD → FLYING → LANDED

System State (联合视图)
  UNINITIALIZED → INITIALIZING → READY → FLYING → RECOVERING → ERROR

==========================
使用示例
==========================
from uav_state_manager import UAVStateManager, UAVRecoveryExecutor

# 创建状态管理器
state_manager = UAVStateManager(vehicle_id=0)

# 获取状态快照
snapshot = state_manager.get_snapshot()
print(f"System State: {snapshot.system_state.name}")
print(f"Is Ready: {snapshot.is_ready_for_flight()}")

# 创建恢复执行器
executor = UAVRecoveryExecutor(state_manager, px4_backend, mavros_manager, env)

# 执行恢复
success = executor.recover_system(position=[0, 0, 1], yaw_deg=0)

==========================
作者信息
==========================
Author: Claude Code Assistant
License: BSD-3-Clause
"""

__all__ = [
    "PX4BackendState",
    "MavrosState",
    "FlightState",
    "SystemState",
    "UAVStateSnapshot",
    "UAVStateManager",
    "UAVRecoveryExecutor",
    "ts_log",
]

import threading
import time
import traceback
from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional, Callable, List, Any
from datetime import datetime


# ============================================================================
#                           时间戳日志工具
# ============================================================================

def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """
    生成带时间戳的日志消息

    Args:
        prefix: 日志前缀（如 [UAVStateManager uav0]）
        message: 日志内容
        level: 日志级别 (INFO, WARN, ERROR, DEBUG)

    Returns:
        格式化的日志字符串
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg)
    return log_msg


# ============================================================================
#                           状态枚举定义
# ============================================================================

class PX4BackendState(Enum):
    """PX4 MAVLink 后端状态"""
    STOPPED = auto()      # 后端未启动，无连接
    STARTING = auto()     # PX4 进程已启动，等待 MAVLink 连接
    CONNECTED = auto()    # MAVLink 连接已建立，等待心跳
    HEARTBEAT = auto()    # 已收到 PX4 心跳，正在交换数据
    READY = auto()        # PX4 报告 "Ready for takeoff"
    ERROR = auto()        # 错误状态（进程崩溃、连接断开等）


class MavrosState(Enum):
    """MAVROS 连接状态"""
    STOPPED = auto()      # MAVROS 进程未启动
    STARTING = auto()     # MAVROS 进程已启动，等待连接
    CONNECTED = auto()    # MAVROS 已连接（state topic 可用）
    FCU_CONNECTED = auto() # FCU 连接已建立
    ERROR = auto()        # 错误状态


class FlightState(Enum):
    """飞行状态"""
    UNKNOWN = auto()      # 未知状态
    DISARMED = auto()     # 未解锁
    ARMED = auto()        # 已解锁
    OFFBOARD = auto()     # OFFBOARD 模式
    FLYING = auto()       # 飞行中（离地）
    LANDING = auto()      # 着陆中
    LANDED = auto()       # 已着陆


class SystemState(Enum):
    """系统联合状态（顶层视图）"""
    UNINITIALIZED = auto()  # 系统未初始化
    INITIALIZING = auto()   # 正在初始化
    READY = auto()          # 就绪，可以起飞
    FLYING = auto()         # 飞行中
    RECOVERING = auto()     # 恢复中（PX4/MAVROS 重启）
    ERROR = auto()          # 错误状态


# ============================================================================
#                           状态快照
# ============================================================================

@dataclass
class UAVStateSnapshot:
    """UAV 状态快照（线程安全的只读视图）"""
    vehicle_id: int
    timestamp: float
    timestamp_str: str

    # 分层状态
    px4_state: PX4BackendState
    mavros_state: MavrosState
    flight_state: FlightState
    system_state: SystemState

    # 详细状态
    px4_process_alive: bool = False
    px4_ready_to_takeoff: bool = False
    mavlink_connected: bool = False
    heartbeat_received: bool = False
    actuator_received: bool = False
    mavros_process_alive: bool = False
    fcu_connected: bool = False
    armed: bool = False
    mode: str = ""

    # 错误信息
    last_error: Optional[str] = None
    error_count: int = 0
    recovery_count: int = 0

    def is_ready_for_flight(self) -> bool:
        """检查是否可以起飞"""
        return (
            self.px4_state == PX4BackendState.READY and
            self.mavros_state == MavrosState.FCU_CONNECTED and
            self.flight_state in (FlightState.DISARMED, FlightState.ARMED)
        )

    def is_healthy(self) -> bool:
        """检查系统是否健康"""
        return self.system_state not in (SystemState.ERROR, SystemState.RECOVERING)

    def to_dict(self) -> dict:
        """转换为字典（用于 HTTP API）"""
        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": self.timestamp,
            "timestamp_str": self.timestamp_str,
            "px4_state": self.px4_state.name,
            "mavros_state": self.mavros_state.name,
            "flight_state": self.flight_state.name,
            "system_state": self.system_state.name,
            "px4_process_alive": self.px4_process_alive,
            "px4_ready_to_takeoff": self.px4_ready_to_takeoff,
            "mavlink_connected": self.mavlink_connected,
            "heartbeat_received": self.heartbeat_received,
            "actuator_received": self.actuator_received,
            "mavros_process_alive": self.mavros_process_alive,
            "fcu_connected": self.fcu_connected,
            "armed": self.armed,
            "mode": self.mode,
            "last_error": self.last_error,
            "error_count": self.error_count,
            "recovery_count": self.recovery_count,
            "is_ready_for_flight": self.is_ready_for_flight(),
            "is_healthy": self.is_healthy(),
        }


# ============================================================================
#                           状态管理器
# ============================================================================

class UAVStateManager:
    """
    UAV 状态管理器

    职责：
    1. 聚合 PX4、MAVROS、飞行状态为统一视图
    2. 提供线程安全的状态查询
    3. 管理状态转换
    4. 提供状态变化通知
    """

    def __init__(self, vehicle_id: int):
        self._vehicle_id = vehicle_id
        self._lock = threading.RLock()

        # 分层状态
        self._px4_state = PX4BackendState.STOPPED
        self._mavros_state = MavrosState.STOPPED
        self._flight_state = FlightState.UNKNOWN

        # 详细状态
        self._px4_process_alive = False
        self._px4_ready_to_takeoff = False
        self._mavlink_connected = False
        self._heartbeat_received = False
        self._actuator_received = False
        self._mavros_process_alive = False
        self._fcu_connected = False
        self._armed = False
        self._mode = ""

        # 错误追踪
        self._last_error: Optional[str] = None
        self._error_count = 0
        self._recovery_count = 0

        # 是否处于恢复过程中
        self._is_recovering = False

        # 状态变化回调
        self._state_callbacks: List[Callable[[UAVStateSnapshot], None]] = []

        # 状态历史（用于调试）
        self._state_history: List[dict] = []
        self._max_history = 100

        # 日志前缀
        self._log_prefix = f"[UAVStateManager uav{vehicle_id}]"

        ts_log(self._log_prefix, f"Initialized for vehicle {vehicle_id}")

    # =========== 状态查询接口 ===========

    def get_snapshot(self) -> UAVStateSnapshot:
        """获取当前状态快照（线程安全）"""
        with self._lock:
            now = time.time()
            return UAVStateSnapshot(
                vehicle_id=self._vehicle_id,
                timestamp=now,
                timestamp_str=datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                px4_state=self._px4_state,
                mavros_state=self._mavros_state,
                flight_state=self._flight_state,
                system_state=self._compute_system_state(),
                px4_process_alive=self._px4_process_alive,
                px4_ready_to_takeoff=self._px4_ready_to_takeoff,
                mavlink_connected=self._mavlink_connected,
                heartbeat_received=self._heartbeat_received,
                actuator_received=self._actuator_received,
                mavros_process_alive=self._mavros_process_alive,
                fcu_connected=self._fcu_connected,
                armed=self._armed,
                mode=self._mode,
                last_error=self._last_error,
                error_count=self._error_count,
                recovery_count=self._recovery_count,
            )

    def get_state_history(self) -> List[dict]:
        """获取状态历史（用于调试）"""
        with self._lock:
            return list(self._state_history)

    def _compute_system_state(self) -> SystemState:
        """计算系统联合状态"""
        # 恢复中
        if self._is_recovering:
            return SystemState.RECOVERING

        # 错误状态
        if self._px4_state == PX4BackendState.ERROR or self._mavros_state == MavrosState.ERROR:
            return SystemState.ERROR

        # 未初始化
        if self._px4_state == PX4BackendState.STOPPED:
            return SystemState.UNINITIALIZED

        # 初始化中
        if self._px4_state in (PX4BackendState.STARTING, PX4BackendState.CONNECTED):
            return SystemState.INITIALIZING

        if self._mavros_state in (MavrosState.STOPPED, MavrosState.STARTING, MavrosState.CONNECTED):
            return SystemState.INITIALIZING

        # 飞行中
        if self._flight_state == FlightState.FLYING:
            return SystemState.FLYING

        # 就绪
        if self._px4_state == PX4BackendState.READY and self._mavros_state == MavrosState.FCU_CONNECTED:
            return SystemState.READY

        return SystemState.INITIALIZING

    # =========== 状态更新接口（供各组件调用）===========

    def update_px4_process(self, alive: bool):
        """更新 PX4 进程状态"""
        with self._lock:
            old_alive = self._px4_process_alive
            self._px4_process_alive = alive

            if alive and not old_alive:
                self._transition_px4(PX4BackendState.STARTING, "process started")
            elif not alive and old_alive:
                if not self._is_recovering:
                    self._set_error("PX4 process died unexpectedly")
                    self._transition_px4(PX4BackendState.ERROR, "process died")

    def update_mavlink_connected(self, connected: bool):
        """更新 MAVLink 连接状态"""
        with self._lock:
            old = self._mavlink_connected
            self._mavlink_connected = connected

            if connected and not old:
                if self._px4_state == PX4BackendState.STARTING:
                    self._transition_px4(PX4BackendState.CONNECTED, "mavlink connected")
            elif not connected and old:
                if self._px4_state not in (PX4BackendState.STOPPED, PX4BackendState.ERROR):
                    if not self._is_recovering:
                        self._set_error("MAVLink connection lost")
                        self._transition_px4(PX4BackendState.ERROR, "connection lost")

    def update_heartbeat_received(self, received: bool):
        """更新心跳接收状态"""
        with self._lock:
            old = self._heartbeat_received
            self._heartbeat_received = received

            if received and not old:
                if self._px4_state == PX4BackendState.CONNECTED:
                    self._transition_px4(PX4BackendState.HEARTBEAT, "heartbeat received")
                elif self._px4_state == PX4BackendState.STARTING:
                    # 可能跳过了 CONNECTED 状态
                    self._mavlink_connected = True
                    self._transition_px4(PX4BackendState.HEARTBEAT, "heartbeat received (fast path)")

    def update_actuator_received(self, received: bool):
        """更新执行器控制接收状态"""
        with self._lock:
            self._actuator_received = received

    def update_px4_ready(self, ready: bool):
        """更新 PX4 就绪状态"""
        with self._lock:
            old = self._px4_ready_to_takeoff
            self._px4_ready_to_takeoff = ready

            if ready and not old:
                if self._px4_state == PX4BackendState.HEARTBEAT:
                    self._transition_px4(PX4BackendState.READY, "ready for takeoff")
                elif self._px4_state in (PX4BackendState.CONNECTED, PX4BackendState.STARTING):
                    # 可能跳过了中间状态
                    self._heartbeat_received = True
                    self._transition_px4(PX4BackendState.READY, "ready for takeoff (fast path)")

    def update_mavros_process(self, alive: bool):
        """更新 MAVROS 进程状态"""
        with self._lock:
            old = self._mavros_process_alive
            self._mavros_process_alive = alive

            if alive and not old:
                self._transition_mavros(MavrosState.STARTING, "process started")
            elif not alive and old:
                self._transition_mavros(MavrosState.STOPPED, "process stopped")

    def update_mavros_connected(self, connected: bool):
        """更新 MAVROS 连接状态"""
        with self._lock:
            if connected:
                if self._mavros_state == MavrosState.STARTING:
                    self._transition_mavros(MavrosState.CONNECTED, "state topic received")
            else:
                if self._mavros_state in (MavrosState.CONNECTED, MavrosState.FCU_CONNECTED):
                    if not self._is_recovering:
                        self._set_error("MAVROS disconnected")
                        self._transition_mavros(MavrosState.ERROR, "disconnected")

    def update_fcu_connected(self, connected: bool):
        """更新 FCU 连接状态"""
        with self._lock:
            old = self._fcu_connected
            self._fcu_connected = connected

            if connected and not old:
                if self._mavros_state == MavrosState.CONNECTED:
                    self._transition_mavros(MavrosState.FCU_CONNECTED, "fcu connected")
                elif self._mavros_state == MavrosState.STARTING:
                    self._transition_mavros(MavrosState.FCU_CONNECTED, "fcu connected (fast path)")
            elif not connected and old:
                if self._mavros_state == MavrosState.FCU_CONNECTED:
                    if not self._is_recovering:
                        self._set_error("FCU disconnected")
                        self._transition_mavros(MavrosState.ERROR, "fcu disconnected")

    def update_flight_state(self, armed: bool, mode: str, landed_state: int = 0):
        """
        更新飞行状态

        Args:
            armed: 是否解锁
            mode: 飞行模式（如 "OFFBOARD"）
            landed_state: 着陆状态（0=未知，1=在地，2=在空，3=起飞中，4=着陆中）
        """
        with self._lock:
            self._armed = armed
            self._mode = mode

            old_state = self._flight_state

            if not armed:
                self._flight_state = FlightState.DISARMED
            elif landed_state == 1:  # ON_GROUND
                self._flight_state = FlightState.LANDED
            elif landed_state == 4:  # LANDING
                self._flight_state = FlightState.LANDING
            elif landed_state == 2 or landed_state == 3:  # IN_AIR or TAKING_OFF
                if mode == "OFFBOARD":
                    self._flight_state = FlightState.FLYING
                else:
                    self._flight_state = FlightState.ARMED
            elif mode == "OFFBOARD":
                self._flight_state = FlightState.OFFBOARD
            else:
                self._flight_state = FlightState.ARMED

            if old_state != self._flight_state:
                ts_log(self._log_prefix,
                       f"Flight state: {old_state.name} -> {self._flight_state.name} "
                       f"(armed={armed}, mode={mode}, landed={landed_state})")
                self._notify_state_change()

    # =========== 状态转换辅助方法 ===========

    def _transition_px4(self, new_state: PX4BackendState, reason: str):
        """PX4 状态转换（内部方法，需持有锁）"""
        old = self._px4_state
        if old != new_state:
            self._px4_state = new_state
            self._record_history("px4", old.name, new_state.name, reason)
            ts_log(self._log_prefix, f"PX4 state: {old.name} -> {new_state.name} ({reason})")
            self._notify_state_change()

    def _transition_mavros(self, new_state: MavrosState, reason: str):
        """MAVROS 状态转换（内部方法，需持有锁）"""
        old = self._mavros_state
        if old != new_state:
            self._mavros_state = new_state
            self._record_history("mavros", old.name, new_state.name, reason)
            ts_log(self._log_prefix, f"MAVROS state: {old.name} -> {new_state.name} ({reason})")
            self._notify_state_change()

    def _set_error(self, error: str):
        """记录错误（内部方法，需持有锁）"""
        self._last_error = error
        self._error_count += 1
        ts_log(self._log_prefix, f"ERROR: {error} (total errors: {self._error_count})", "ERROR")

    def _record_history(self, component: str, old_state: str, new_state: str, reason: str):
        """记录状态历史"""
        now = time.time()
        entry = {
            "timestamp": now,
            "timestamp_str": datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "component": component,
            "old_state": old_state,
            "new_state": new_state,
            "reason": reason,
        }
        self._state_history.append(entry)

        # 限制历史记录数量
        if len(self._state_history) > self._max_history:
            self._state_history = self._state_history[-self._max_history:]

    def _notify_state_change(self):
        """通知状态变化"""
        snapshot = self.get_snapshot()
        for callback in self._state_callbacks:
            try:
                callback(snapshot)
            except Exception as e:
                ts_log(self._log_prefix, f"Callback error: {e}\n{traceback.format_exc()}", "ERROR")

    # =========== 恢复相关方法 ===========

    def begin_recovery(self):
        """开始恢复过程"""
        with self._lock:
            self._is_recovering = True
            self._recovery_count += 1
            ts_log(self._log_prefix, f"Recovery started (count: {self._recovery_count})")

    def end_recovery(self, success: bool):
        """结束恢复过程"""
        with self._lock:
            self._is_recovering = False
            if success:
                self._last_error = None
                ts_log(self._log_prefix, "Recovery completed successfully")
            else:
                ts_log(self._log_prefix, "Recovery failed", "ERROR")

    def reset_for_recovery(self):
        """为恢复操作重置状态"""
        with self._lock:
            self._px4_state = PX4BackendState.STOPPED
            self._mavros_state = MavrosState.STOPPED
            self._flight_state = FlightState.UNKNOWN

            self._px4_process_alive = False
            self._px4_ready_to_takeoff = False
            self._mavlink_connected = False
            self._heartbeat_received = False
            self._actuator_received = False
            self._mavros_process_alive = False
            self._fcu_connected = False
            self._armed = False
            self._mode = ""

            self._record_history("system", "RECOVERING", "RESET", "recovery reset")
            ts_log(self._log_prefix, "State reset for recovery")

    # =========== 回调注册 ===========

    def register_callback(self, callback: Callable[[UAVStateSnapshot], None]):
        """注册状态变化回调"""
        self._state_callbacks.append(callback)

    def unregister_callback(self, callback: Callable[[UAVStateSnapshot], None]):
        """取消注册回调"""
        if callback in self._state_callbacks:
            self._state_callbacks.remove(callback)


# ============================================================================
#                           恢复执行器
# ============================================================================

class UAVRecoveryExecutor:
    """
    UAV 恢复执行器

    职责：
    1. 执行系统恢复操作
    2. 确保状态转换的原子性
    3. 提供超时和重试机制
    """

    def __init__(
        self,
        state_manager: UAVStateManager,
        px4_backend: Any,  # PX4MavlinkBackend 实例
        mavros_manager: Any,  # MavrosManager 类
        env: Any,  # IsaacSimEnv 实例
    ):
        self._state_manager = state_manager
        self._px4_backend = px4_backend
        self._mavros_manager = mavros_manager
        self._env = env
        self._vehicle_id = state_manager._vehicle_id
        self._log_prefix = f"[RecoveryExecutor uav{self._vehicle_id}]"
        self._lock = threading.Lock()

    def recover_system(
        self,
        position: Optional[List[float]] = None,
        yaw_deg: Optional[float] = None,
        timeout: float = 120.0,
    ) -> bool:
        """
        执行完整的系统恢复

        步骤：
        1. 停止 MAVROS
        2. 停止 PX4 后端
        3. 等待资源释放
        4. 移动 UAV（如果指定位置）
        5. 启动 PX4 后端
        6. 等待 PX4 就绪
        7. 启动 MAVROS
        8. 等待 MAVROS 连接
        9. 等待 FCU 连接

        Returns:
            True 如果恢复成功
        """
        with self._lock:
            start_time = time.time()

            try:
                ts_log(self._log_prefix, "=" * 50)
                ts_log(self._log_prefix, "Starting system recovery")
                ts_log(self._log_prefix, "=" * 50)

                # 标记开始恢复
                self._state_manager.begin_recovery()

                # 1. 重置状态管理器
                self._state_manager.reset_for_recovery()

                # 2. 停止 MAVROS
                ts_log(self._log_prefix, "Step 1/9: Stopping MAVROS...")
                self._stop_mavros()

                # 3. 停止 PX4 后端
                ts_log(self._log_prefix, "Step 2/9: Stopping PX4 backend...")
                self._stop_px4_backend()

                # 4. 等待资源释放
                ts_log(self._log_prefix, "Step 3/9: Waiting for resources to settle...")
                time.sleep(2.0)

                # 5. 移动 UAV（如果指定位置）
                if position is not None:
                    ts_log(self._log_prefix, f"Step 4/9: Moving UAV to {position}, yaw={yaw_deg}...")
                    self._move_uav(position, yaw_deg)
                else:
                    ts_log(self._log_prefix, "Step 4/9: Skipping UAV move (no position specified)")

                # 6. 启动 PX4 后端
                ts_log(self._log_prefix, "Step 5/9: Starting PX4 backend...")
                self._start_px4_backend()

                # 7. 等待 PX4 就绪
                ts_log(self._log_prefix, "Step 6/9: Waiting for PX4 ready...")
                remaining = timeout - (time.time() - start_time)
                if not self._wait_for_px4_ready(timeout=min(remaining, 60.0)):
                    raise TimeoutError("PX4 ready timeout")

                # 8. 启动 MAVROS
                ts_log(self._log_prefix, "Step 7/9: Starting MAVROS...")
                self._start_mavros()

                # 9. 等待 MAVROS 连接
                ts_log(self._log_prefix, "Step 8/9: Waiting for MAVROS connection...")
                remaining = timeout - (time.time() - start_time)
                if not self._wait_for_mavros_connected(timeout=min(remaining, 60.0)):
                    raise TimeoutError("MAVROS connection timeout")

                # 10. 等待 FCU 连接
                ts_log(self._log_prefix, "Step 9/9: Waiting for FCU connection...")
                remaining = timeout - (time.time() - start_time)
                if not self._wait_for_fcu_connection(timeout=min(remaining, 30.0)):
                    raise TimeoutError("FCU connection timeout")

                # 标记恢复成功
                self._state_manager.end_recovery(success=True)

                elapsed = time.time() - start_time
                ts_log(self._log_prefix, "=" * 50)
                ts_log(self._log_prefix, f"Recovery completed successfully in {elapsed:.1f}s")
                ts_log(self._log_prefix, "=" * 50)
                return True

            except Exception as e:
                ts_log(self._log_prefix, f"Recovery failed: {e}", "ERROR")
                ts_log(self._log_prefix, traceback.format_exc(), "ERROR")
                self._state_manager.end_recovery(success=False)
                return False

    def _stop_mavros(self):
        """停止 MAVROS"""
        try:
            self._mavros_manager.stop_mavros(self._vehicle_id)
            self._state_manager.update_mavros_process(False)
            self._state_manager.update_mavros_connected(False)
            self._state_manager.update_fcu_connected(False)
            ts_log(self._log_prefix, "MAVROS stopped")
        except Exception as e:
            ts_log(self._log_prefix, f"Stop MAVROS failed: {e}", "WARN")
            ts_log(self._log_prefix, traceback.format_exc(), "WARN")

    def _stop_px4_backend(self):
        """停止 PX4 后端"""
        # 1. 杀死 PX4 进程
        if self._px4_backend.px4_tool is not None:
            try:
                self._px4_backend.px4_tool.kill_px4_save()
                ts_log(self._log_prefix, "PX4 process killed")
            except Exception as e:
                ts_log(self._log_prefix, f"Kill PX4 failed: {e}", "WARN")
                ts_log(self._log_prefix, traceback.format_exc(), "WARN")

        # 2. 关闭 MAVLink 连接
        if self._px4_backend._connection is not None:
            try:
                self._px4_backend._connection.close()
                ts_log(self._log_prefix, "MAVLink connection closed")
            except Exception as e:
                ts_log(self._log_prefix, f"Close MAVLink failed: {e}", "WARN")
                ts_log(self._log_prefix, traceback.format_exc(), "WARN")
            self._px4_backend._connection = None

        # 3. 更新状态
        self._px4_backend._is_running = False
        self._state_manager.update_px4_process(False)
        self._state_manager.update_mavlink_connected(False)
        self._state_manager.update_heartbeat_received(False)

        # 4. 等待端口释放
        from pegasus.simulator.logic.backends.px4_mavlink_backend import _wait_for_port_release
        port = self._px4_backend.config.connection_baseport + self._vehicle_id
        ts_log(self._log_prefix, f"Waiting for port {port} to be released...")
        if _wait_for_port_release(port, timeout=30.0):
            ts_log(self._log_prefix, f"Port {port} released")
        else:
            ts_log(self._log_prefix, f"Port {port} release timeout", "WARN")

    def _move_uav(self, position: List[float], yaw_deg: Optional[float]):
        """移动 UAV 到指定位置"""
        try:
            # 将 z 设为接近地面的高度
            ground_pos = [position[0], position[1], 0.07]
            self._env._sim_move_uav(ground_pos, yaw_deg)
            time.sleep(0.5)
            ts_log(self._log_prefix, f"UAV moved to {ground_pos}")
        except Exception as e:
            ts_log(self._log_prefix, f"Move UAV failed: {e}", "WARN")
            ts_log(self._log_prefix, traceback.format_exc(), "WARN")

    def _start_px4_backend(self):
        """启动 PX4 后端"""
        from pymavlink import mavutil
        from pegasus.simulator.logic.backends.px4_mavlink_backend import SensorMsg
        from pegasus.simulator.logic.backends.tools.px4_launch_tool import PX4LaunchTool

        # 1. 创建 MAVLink 连接（先于 PX4 启动，避免连接竞争）
        ts_log(self._log_prefix, f"Creating MAVLink connection: {self._px4_backend._connection_port}")
        self._px4_backend._connection = mavutil.mavlink_connection(
            self._px4_backend._connection_port
        )
        self._state_manager.update_mavlink_connected(True)

        # 2. 重置必要的状态
        # 关键：不重置 _received_first_hearbeat，避免死锁
        # 但如果这是首次启动，需要设置为 False
        # 这里我们保持当前值，让 update() 循环正常发送数据
        self._px4_backend._received_first_actuator = False
        self._px4_backend._received_actuator = False
        self._px4_backend._sensor_data = SensorMsg()
        self._px4_backend._last_heartbeat_sent_time = 0

        # 3. 标记为运行状态
        self._px4_backend._is_running = True

        # 4. 启动 PX4 进程
        ts_log(self._log_prefix, "Launching PX4 process...")
        self._px4_backend.px4_tool = PX4LaunchTool(
            self._px4_backend.px4_dir,
            self._vehicle_id,
            self._px4_backend.px4_vehicle_model,
            self._px4_backend.config.sim_speed_factor,
            log_dir=self._px4_backend._px4_log_dir
        )
        self._px4_backend.px4_tool.launch_px4()
        self._state_manager.update_px4_process(True)

        # 5. 重置启动保护期
        self._px4_backend._px4_start_time = time.time()

        ts_log(self._log_prefix, "PX4 backend started")

    def _start_mavros(self):
        """启动 MAVROS"""
        try:
            success = self._mavros_manager.start_mavros(
                self._vehicle_id,
                self._env._mavros_launch_file,
                self._env._mavros_log_dir
            )
            if success:
                self._state_manager.update_mavros_process(True)
                ts_log(self._log_prefix, "MAVROS started")
            else:
                raise RuntimeError("MAVROS start returned False")
        except Exception as e:
            ts_log(self._log_prefix, f"Start MAVROS failed: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")
            raise

    def _wait_for_px4_ready(self, timeout: float = 60.0) -> bool:
        """等待 PX4 就绪"""
        start = time.time()
        last_log_time = 0
        log_interval = 5.0

        while time.time() - start < timeout:
            # 检查心跳
            if self._px4_backend._received_first_hearbeat:
                self._state_manager.update_heartbeat_received(True)

            # 检查就绪
            if self._px4_backend.px4_ready_to_takeoff:
                self._state_manager.update_px4_ready(True)
                ts_log(self._log_prefix, "PX4 is ready for takeoff")
                return True

            # 定期日志
            now = time.time()
            if now - last_log_time > log_interval:
                elapsed = now - start
                snapshot = self._state_manager.get_snapshot()
                ts_log(self._log_prefix,
                       f"Waiting for PX4 ready... ({elapsed:.1f}s) "
                       f"state={snapshot.px4_state.name} "
                       f"heartbeat={snapshot.heartbeat_received} "
                       f"ready={snapshot.px4_ready_to_takeoff}")
                last_log_time = now

            time.sleep(0.5)

        ts_log(self._log_prefix, f"PX4 ready timeout after {timeout}s", "ERROR")
        return False

    def _wait_for_mavros_connected(self, timeout: float = 60.0) -> bool:
        """等待 MAVROS 连接"""
        try:
            result = self._env.wait_for_mavros_connected(timeout=timeout)
            if result:
                self._state_manager.update_mavros_connected(True)
                ts_log(self._log_prefix, "MAVROS connected")
            else:
                ts_log(self._log_prefix, f"MAVROS connection timeout after {timeout}s", "ERROR")
            return result
        except Exception as e:
            ts_log(self._log_prefix, f"Wait for MAVROS failed: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")
            return False

    def _wait_for_fcu_connection(self, timeout: float = 30.0) -> bool:
        """等待 FCU 连接"""
        start = time.time()
        last_log_time = 0
        log_interval = 5.0

        while time.time() - start < timeout:
            if hasattr(self._env, 'current_state') and getattr(self._env.current_state, 'connected', False):
                self._state_manager.update_fcu_connected(True)
                ts_log(self._log_prefix, "FCU connected")
                return True

            # 定期日志
            now = time.time()
            if now - last_log_time > log_interval:
                elapsed = now - start
                ts_log(self._log_prefix, f"Waiting for FCU connection... ({elapsed:.1f}s)")
                last_log_time = now

            time.sleep(0.2)

        ts_log(self._log_prefix, f"FCU connection timeout after {timeout}s", "ERROR")
        return False
