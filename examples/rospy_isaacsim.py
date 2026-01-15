#!/usr/bin/env python3
# Copyright (c) 2024-2026 amurzzb@gmail.com
# Licensed under the MIT License
"""
Pegasus ROS2 控制器（rospy_isaacsim.py）

概述
- 提供 ROS2 + MAVROS 控制节点以及 HTTP 控制接口，用于在仿真（Isaac Sim）中以 OFFBOARD 控制多架载具。
- 具备任务并发保护（互斥锁）：当一个任务未完成时拒绝新的请求，除非请求包含 `force:true`。
- 在重置流程中，优先执行降落并确认着陆后再 `disarm`，随后进行（可选）PX4 重启与重新起飞。

架构
- 控制核心：`IsaacSimEnv`（ROS2 Node），保持所有外部接口与行为完全一致。
- 公共功能抽象：
  - `HttpApi` 封装所有 HTTP 路由的业务处理，统一并发控制与错误返回；
  - `ImageService` 统一图像采集逻辑（ROS2/HTTP 两种来源），提供稳定的 `capture_b64()`；
  - 其余控制与时序逻辑保持原先的实现与调用频率不变。

启动流程
- `main()`：解析 `--mavros-ns`，创建节点，`wait_for_fcu_connection()` 阻塞等待 FCU 连接；随后执行 `reset()` 完整起飞至 `INIT_HEIGHT`，开启 HTTP 服务并进入悬停循环。
- `wait_for_fcu_connection()`：轮询 `State.connected`，连接成功后继续。

控制流程（核心方法）
- `reset()`：
  1) 预热 setpoints（50 次），避免进入 RTL/FailSafe。
  2) 切换 `OFFBOARD`（`SetMode`）。
  3) `arm`（`CommandBool`）。
  4) 起飞到 `start_height + INIT_HEIGHT`，使用 `tqdm` 进度；到达后设定 `hover_target`。
- `move_to(x,y,z)` / `move_to_many(points)`：按位置目标发布 `PositionTarget` 并以 `REACH_THRESHOLD` 检查到达。
- `land()`：
  - 每步将高度向 `start_height` 下降，调用 `wait_until_landed()` 做窗口稳定检测；着陆后调用 `disarm` 并 `wait_until_disarmed()`。
- `reboot_px4(position,yaw_deg)`：
  - 若已 `armed`：先 `land()`，必要时 `disarm` 并等待解除。
  - 可选移动仿真载具位置：向仿真端 `http://127.0.0.1:8081/uav/<vid>/reset` 提交位置与 yaw。
  - 检查服务 `/<ns>/mavros/cmd/command` 是否存在；存在则下发 `MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN (246)`、`param1=1.0` 重启 PX4。
  - 若发起重启：等待 `/<ns>/mavros/state.connected` 恢复；随后重复预热、`OFFBOARD`、`arm`、起飞至 `INIT_HEIGHT` 并设定 `hover_target`。

并发控制
- `self._task_lock` 在所有 HTTP 路由上生效：当锁被占用时返回 `409 busy`，除非请求体包含 `force:true` 允许阻塞式获取锁。
- 路由均在异常与结束时确保释放锁，防止死锁。

HTTP 接口与示例
- `POST /reset`
  - 请求体：`{"vid":0, "position":[x,y,z], "yaw_deg":0.0, "force":false}`
  - 响应：`{"status":"success","message":"reset ok","vid":0,"position":[x,y,z]}` 或 `409 busy`
- `POST /command`
  - `{"cmd":"move_to","x":X,"y":Y,"z":Z}` → 移动并返回当前位姿
  - `{"cmd":"move_to_many","points":[[x,y,z],...]}` → 批量移动（OFFBOARD逐点）
  - `{"cmd":"execute_mission","points":[[x,y,z],...]}` → 使用MAVROS MISSION模式执行航点任务
    - `use_mission_mode`: true=AUTO.MISSION模式，false=OFFBOARD逐点模式（默认true）
    - `timeout`: 任务超时秒数（默认300）
  - `{"cmd":"land"}` → 执行降落并解除
  - `{"cmd":"get_position"}` → 返回当前位姿 `{x,y,z}`
  - `{"cmd":"get_status"}` → 返回连接、解锁、模式
- `POST /step` / `POST /step_http`
  - 请求体：`{"actions": [[[x,y,z],...]], "per_step": false, "force": false}`
  - 响应体：`{"status":"success","images":[[b64...]],"dones":[false...],"message":"Step executed successfully","batch_size":N}`
- `GET /health`：`{"status":"healthy","env_id":"<ns>:<port>"}`

图像来源
- ROS2：`ImageService` 自动选择彩色图像主题并编码。
- 仿真 HTTP：读取 `http://127.0.0.1:8081/uav/<vid>/image` 的 JSON 并解析 `data` 字段。

端口与网关
- 控制器端口：`PEGASUS_HTTP_PORT`（默认 `5009 + vid`）。
- 网关端口：`5008`；统一将 `/uav/<vid>/...` 转发到对应控制器端口。
- 仿真端口：`8081`（`8_camera_vehicle.py`）。

时间同步与稳定性
- 配置文件 `px4_config.yaml` 设置 `timesync_rate=0.0`、`system_time_rate=10.0`；本文件还提供 `configure_mavros_time_plugin()` 运行时调整（优先在 `/<ns>/mavros/time`，回退到 `/<ns>/mavros`）。

错误处理与返回码
- 连接/服务不可用时返回 `504`（超时）或 `500`（执行错误）；并发占用返回 `409 busy`。
- PX4 reboot 服务缺失时会记录告警并跳过重启，但继续执行起飞与悬停流程。

日志与度量
- 控制器会记录起飞进度、模式切换、解锁与降落事件；配套脚本 `tools/log_status_report.py` 可提取统计指标。

端到端关系
- 仿真端在 `8081` 暴露 `/uav/<id>/all|image|pose|reset`；控制器在 `5009+id` 暴露控制接口；网关在 `5008` 统一转发。

接口用法示例（Python）
- 以 `uav0` 为例控制端口默认 `5009`（可被 `PEGASUS_HTTP_PORT` 覆写）：

```
import requests

base = "http://127.0.0.1:5009"

# Reset（可选硬重启 PX4 并移动仿真载具位置）
r = requests.post(base + "/reset", json={
    "vid": 0,
    "position": [0.0, 0.0, 1.0],
    "yaw_deg": 0.0,
    "hard": True,
    "force": True,
}, timeout=60)
print("reset:", r.status_code, r.json())

# move_to / move_to_many
requests.post(base + "/command", json={"cmd": "move_to", "x":1, "y":-0.5, "z":2, "force": True})
requests.post(base + "/command", json={"cmd": "move_to_many", "points": [[0,0,1.5],[0.5,0,1.5]], "force": True})

# get_position / get_status / land
requests.post(base + "/command", json={"cmd": "get_position"})
requests.post(base + "/command", json={"cmd": "get_status"})
requests.post(base + "/command", json={"cmd": "land", "force": True}, timeout=60)

# step（ROS 图像源）/ step_http（仿真 HTTP 图像源）
requests.post(base + "/step", json={"actions": [[[0,0,2],[0.5,0.5,2]]], "per_step": False, "force": True}, timeout=60)
requests.post(base + "/step_http", json={"actions": [[[0,0,2]]], "per_step": True, "force": True}, timeout=60)

# health
requests.get(base + "/health", timeout=5)
```

接口用法示例（curl）
```
curl -X POST "http://127.0.0.1:5009/reset" -H "Content-Type: application/json" -d '{"vid":0,"position":[0,0,1],"yaw_deg":0,"hard":true,"force":true}'
curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"move_to","x":1,"y":-0.5,"z":2,"force":true}'
curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"move_to_many","points":[[0,0,1.5],[0.5,0,1.5]],"force":true}'
curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"get_position"}'
curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"get_status"}'
curl -X POST "http://127.0.0.1:5009/command" -H "Content-Type: application/json" -d '{"cmd":"land","force":true}'
curl -X POST "http://127.0.0.1:5009/step" -H "Content-Type: application/json" -d '{"actions":[[[0,0,2],[0.5,0.5,2]]],"per_step":false,"force":true}'
curl -X POST "http://127.0.0.1:5009/step_http" -H "Content-Type: application/json" -d '{"actions":[[[0,0,2]]],"per_step":true,"force":true}'
```

PX4 无限制飞行配置
- 提供方法 `configure_unlimited_flight()`：在保持时间同步设置的同时，尝试通过 MAVROS 参数接口关闭 EKF2 验证、允许无 GPS 解锁、关闭或放宽地理围栏，从而实现在 PX4 环境下的“无限制飞行”。若目标服务不可用将记录告警并继续。
"""
# --- ROS 2 & System Imports ---
import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
import json
import time
import math
import os
import sys
import signal
import subprocess as sp
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from tqdm import tqdm
import base64
from io import BytesIO
import numpy as np
import traceback
from datetime import datetime


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """
    生成带时间戳的日志消息并输出

    Args:
        prefix: 日志前缀（如 [MavrosManager]）
        message: 日志内容
        level: 日志级别 (INFO, WARN, ERROR, DEBUG)

    Returns:
        格式化的日志字符串
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg)
    return log_msg


try:
    from PIL import Image as PILImage
except Exception:
    PILImage = None

# HTTP server (Flask) for exposing command endpoints
try:
    from flask import Flask, request, jsonify, g
    _FLASK_AVAILABLE = True
except Exception:
    _FLASK_AVAILABLE = False
if not _FLASK_AVAILABLE:
    print("[ERROR] Flask not installed. Please 'pip install flask' and retry.")
    sys.exit(2)
from threading import Thread
import threading
from rcl_interfaces.srv import SetParameters
from rcl_interfaces.msg import Parameter, ParameterValue, ParameterType
from geometry_msgs.msg import PoseStamped
from mavros_msgs.msg import State, RCIn, PositionTarget, ExtendedState
from sensor_msgs.msg import Image
from mavros_msgs.srv import CommandBool, SetMode, CommandLong
# Mission waypoint services
try:
    from mavros_msgs.srv import WaypointPush, WaypointClear
    from mavros_msgs.msg import Waypoint
    _MISSION_AVAILABLE = True
except Exception:
    _MISSION_AVAILABLE = False
try:
    from mavros_msgs.srv import ParamSet
    from mavros_msgs.msg import ParamValue as MavrosParamValue
    _PARAM_AVAILABLE = True
except Exception:
    _PARAM_AVAILABLE = False

# ---------------- Helper: spin rate ----------------
def spin_sleep(node: Node, hz: float):
    """Simple sleep maintaining callbacks via spin_once."""
    period = 1.0 / max(hz, 1.0)
    rclpy.spin_once(node, timeout_sec=0.0)
    time.sleep(period)

DEFAULT_HTTP_PORT = int(os.environ.get("PEGASUS_HTTP_PORT", "5008"))
# 图像来源开关：True=从ROS2 topic读取；False=从8_camera_vehicle的HTTP接口读取
IMAGE_FROM_ROS = os.environ.get("PEGASUS_IMAGE_FROM_ROS", "1") not in ("0", "false", "False")
IMAGE_HTTP_URL = os.environ.get("PEGASUS_IMAGE_HTTP_URL", "http://127.0.0.1:8081")
STEP_RETURN_EACH = os.environ.get("PEGASUS_STEP_RETURN_EACH", "0") in ("1", "true", "True")
INIT_HEIGHT = float(os.environ.get("PEGASUS_INIT_HEIGHT", "2.0"))
TAKEOFF_REACH_THRESHOLD = float(os.environ.get("PEGASUS_TAKEOFF_REACH_THRESHOLD", "0.1"))
MOVE_REACH_THRESHOLD = float(os.environ.get("PEGASUS_MOVE_REACH_THRESHOLD", "0.2"))  # 减小阈值以提高精度
MOVE_STABLE_WINDOW_S = float(os.environ.get("PEGASUS_MOVE_STABLE_WINDOW_S", "0.3"))  # 到达后等待稳定的时间窗口
MOVE_STABLE_VEL_EPS = float(os.environ.get("PEGASUS_MOVE_STABLE_VEL_EPS", "0.15"))   # 速度阈值(m/s)，低于此值认为稳定
CONTROL_LOOP_HZ = float(os.environ.get("PEGASUS_HOVER_HZ", "30.0"))
LAND_TIMEOUT_S = float(os.environ.get("PEGASUS_LAND_TIMEOUT_S", "45.0"))
DISARM_TIMEOUT_S = float(os.environ.get("PEGASUS_DISARM_TIMEOUT_S", "15.0"))
LAND_STABLE_WINDOW_S = float(os.environ.get("PEGASUS_LAND_STABLE_WINDOW_S", "2.5"))
LAND_STABLE_EPS = float(os.environ.get("PEGASUS_LAND_STABLE_EPS", "0.03"))
POSITION_READY_TIMEOUT_S = float(os.environ.get("PEGASUS_POSITION_READY_TIMEOUT_S", "30.0"))
POSITION_READY_WINDOW_S = float(os.environ.get("PEGASUS_POSITION_READY_WINDOW_S", "1.5"))
POSITION_READY_EPS = float(os.environ.get("PEGASUS_POSITION_READY_EPS", "0.05"))

SETMODE_TIMEOUT_S = float(os.environ.get("PEGASUS_SETMODE_TIMEOUT_S", "10.0"))
ARMING_TIMEOUT_S = float(os.environ.get("PEGASUS_ARMING_TIMEOUT_S", "10.0"))

MAX_PITCH_DEG = 20.0
MAX_ROLL_DEG = 20.0

# MAVROS ROS2 launch command path (using Isaac Sim's Python environment)
ISAACSIM_PYTHON = os.environ.get("ISAACSIM_PYTHON", "/home/user/isaacsim-5.1.0/python.sh")
ISAACSIM_ROS2_CMD = os.environ.get("ISAACSIM_ROS2_CMD", "/home/user/IsaacSim-ros_workspaces/build_ws/humble/humble_ws/install/bin/ros2")


class MavrosManager:
    """
    MAVROS 进程生命周期管理器。
    负责启动、停止和重启 MAVROS 进程。
    """

    _instance = None
    _lock = threading.Lock()
    _processes: Dict[int, sp.Popen] = {}  # vid -> Popen
    _log_files: Dict[int, Any] = {}  # vid -> file handle
    _log_prefix = "[MavrosManager]"

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = object.__new__(cls)
            return cls._instance

    @classmethod
    def get_instance(cls) -> 'MavrosManager':
        return cls()

    @classmethod
    def start_mavros(cls, vid: int, launch_file: Path, log_dir: Optional[Path] = None) -> bool:
        """
        启动单个 MAVROS 实例。

        Args:
            vid: Vehicle ID
            launch_file: Path to the MAVROS launch file
            log_dir: Optional directory for log files

        Returns:
            True if started successfully
        """
        with cls._lock:
            if vid in cls._processes:
                proc = cls._processes[vid]
                if proc.poll() is None:
                    ts_log(cls._log_prefix, f"MAVROS for vid={vid} already running (pid={proc.pid})")
                    return True

            if not launch_file.exists():
                ts_log(cls._log_prefix, f"Launch file not found: {launch_file}", "ERROR")
                return False

            try:
                cmd = [ISAACSIM_PYTHON, ISAACSIM_ROS2_CMD, "launch", str(launch_file)]
                ts_log(cls._log_prefix, f"Starting MAVROS vid={vid}: {' '.join(cmd)}")

                log_f = None
                if log_dir:
                    log_dir.mkdir(parents=True, exist_ok=True)
                    log_path = log_dir / f"mavros_uav{vid}.log"
                    log_f = log_path.open("w")
                    cls._log_files[vid] = log_f
                    proc = sp.Popen(cmd, stdout=log_f, stderr=sp.STDOUT, preexec_fn=os.setsid)
                else:
                    proc = sp.Popen(cmd, stdout=sp.DEVNULL, stderr=sp.DEVNULL, preexec_fn=os.setsid)

                cls._processes[vid] = proc
                ts_log(cls._log_prefix, f"MAVROS vid={vid} started (pid={proc.pid})")
                return True
            except Exception as e:
                ts_log(cls._log_prefix, f"Failed to start MAVROS vid={vid}: {e}", "ERROR")
                ts_log(cls._log_prefix, traceback.format_exc(), "ERROR")
                return False

    @classmethod
    def stop_mavros(cls, vid: int, timeout: float = 5.0) -> bool:
        """
        停止单个 MAVROS 实例。

        Args:
            vid: Vehicle ID
            timeout: Timeout for graceful termination

        Returns:
            True if stopped successfully
        """
        with cls._lock:
            if vid not in cls._processes:
                ts_log(cls._log_prefix, f"No MAVROS process for vid={vid}")
                return True

            proc = cls._processes[vid]
            if proc.poll() is not None:
                ts_log(cls._log_prefix, f"MAVROS vid={vid} already terminated")
                cls._cleanup_vid(vid)
                return True

            try:
                # Try to kill the process group
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
                ts_log(cls._log_prefix, f"Sent SIGTERM to MAVROS vid={vid} (pgid={pgid})")

                # Wait for termination
                start = time.time()
                while time.time() - start < timeout:
                    if proc.poll() is not None:
                        ts_log(cls._log_prefix, f"MAVROS vid={vid} terminated gracefully")
                        cls._cleanup_vid(vid)
                        return True
                    time.sleep(0.2)

                # Force kill if still running
                os.killpg(pgid, signal.SIGKILL)
                ts_log(cls._log_prefix, f"Sent SIGKILL to MAVROS vid={vid}", "WARN")
                proc.wait(timeout=2.0)
                cls._cleanup_vid(vid)
                return True
            except Exception as e:
                ts_log(cls._log_prefix, f"Error stopping MAVROS vid={vid}: {e}", "ERROR")
                ts_log(cls._log_prefix, traceback.format_exc(), "ERROR")
                cls._cleanup_vid(vid)
                return False

    @classmethod
    def restart_mavros(cls, vid: int, launch_file: Path, log_dir: Optional[Path] = None) -> bool:
        """
        重启单个 MAVROS 实例。

        Args:
            vid: Vehicle ID
            launch_file: Path to the MAVROS launch file
            log_dir: Optional directory for log files

        Returns:
            True if restarted successfully
        """
        ts_log(cls._log_prefix, f"Restarting MAVROS vid={vid}")
        cls.stop_mavros(vid)
        time.sleep(1.0)  # Brief pause before restart
        return cls.start_mavros(vid, launch_file, log_dir)

    @classmethod
    def stop_all(cls, timeout: float = 5.0):
        """停止所有 MAVROS 实例。"""
        vids = list(cls._processes.keys())
        for vid in vids:
            cls.stop_mavros(vid, timeout)
        ts_log(cls._log_prefix, "All MAVROS processes stopped")

    @classmethod
    def is_running(cls, vid: int) -> bool:
        """检查指定 vid 的 MAVROS 是否正在运行。"""
        with cls._lock:
            if vid not in cls._processes:
                return False
            return cls._processes[vid].poll() is None

    @classmethod
    def _cleanup_vid(cls, vid: int):
        """清理指定 vid 的资源。"""
        cls._processes.pop(vid, None)
        log_f = cls._log_files.pop(vid, None)
        if log_f:
            try:
                log_f.close()
            except Exception as e:
                ts_log(cls._log_prefix, f"Failed to close log file for vid={vid}: {e}", "WARN")

# ---------------- Helper: wait for a single message ----------------
def wait_for_message(node: Node, topic: str, msg_type, timeout: float = None):
    """
    Minimal wait_for_message for rclpy.
    Spins the node until a single message on 'topic' arrives or timeout.
    """
    got_msg = {"msg": None}

    def _cb(msg):
        got_msg["msg"] = msg

    best_effort_qos = QoSProfile(
        reliability=ReliabilityPolicy.BEST_EFFORT,
        history=HistoryPolicy.KEEP_LAST,
        depth=10
    )
    
    sub = node.create_subscription(msg_type, topic, _cb, best_effort_qos)
    start = time.time()
    try:
        while rclpy.ok():
            if got_msg["msg"] is not None:
                return got_msg["msg"]
            rclpy.spin_once(node, timeout_sec=0.1)
            if timeout is not None and (time.time() - start) > timeout:
                raise TimeoutError(f"Timeout waiting for {topic}")
    finally:
        node.destroy_subscription(sub)

# ---------------- Helper: sync service call with timeout ----------------
def call_service_sync(node: Node, client, request, timeout_sec: float = 5.0):
    if not client.service_is_ready():
        if not client.wait_for_service(timeout_sec=timeout_sec):
            raise TimeoutError(f"Service {client.srv_name} not available")
    future = client.call_async(request)
    start = time.time()
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.1)
        if future.done():
            return future.result()
        if (time.time() - start) > timeout_sec:
            raise TimeoutError(f"Service call {client.srv_name} timed out")

# =======================================================================
#                           IsaacSimEnv (ROS2)
# =======================================================================
class IsaacSimEnv(Node):
    """
    ROS2 + PX4 (MAVROS) compatible env.
    Mimics original RosEnv's reset()/step() ordering & semantics.
    """
    def __init__(self, mavros_ns: Optional[str] = None):
        try:
            ns = (mavros_ns or "").strip()
            if ns.startswith("uav"):
                self._vid = int(ns[3:])
            else:
                self._vid = 0
        except Exception:
            self._vid = 0
        
        super().__init__(f"isaac_sim_nav_node_{self._vid}")

        self.current_pose = PoseStamped()
        self.current_state = State()
        self._reset_target_position = None

        self._shutdown_requested = False
        self._external_spin = False
        self._control_paused = False
        self._pose_updates = 0
        self._state_updates = 0

        # MAVROS namespace support: build topic/service prefix
        self._mavros_ns = mavros_ns if mavros_ns is not None else os.environ.get("MAVROS_NS", None)
        self._mavros_prefix = f"/{self._mavros_ns}" if self._mavros_ns else ""
        
        # # --- subscribers (adjust topics to your bridge) ---
        best_effort_qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10
        )

        # MAVROS state 监控 - 必须在创建 state 订阅之前初始化，否则 state_cb 回调可能在属性创建前被触发
        self._mavros_connected = False
        self._mavros_connected_event = threading.Event()

        # --- subscribers ---
        self.create_subscription(State, self._mavros_prefix + "/mavros/state", self.state_cb, best_effort_qos)
        self.create_subscription(PoseStamped, self._mavros_prefix + "/mavros/local_position/pose", self.pose_cb, best_effort_qos)
        self.create_subscription(RCIn, self._mavros_prefix + "/mavros/rc/in", self.rc_in_callback, best_effort_qos)
        self.extended_state = ExtendedState()
        self.create_subscription(ExtendedState, self._mavros_prefix + "/mavros/extended_state", self.extended_state_cb, best_effort_qos)

        # --- publishers ---
        # 直接控制：向 MAVROS /mavros/setpoint_raw/local 发布 PositionTarget
        self.raw_setpoint_pub = self.create_publisher(PositionTarget, self._mavros_prefix + "/mavros/setpoint_raw/local", 10)

        # --- services ---
        self.arming_client    = self.create_client(CommandBool, self._mavros_prefix + "/mavros/cmd/arming")
        self.set_mode_client  = self.create_client(SetMode,    self._mavros_prefix + "/mavros/set_mode")
        self.cmdlong_client   = self.create_client(CommandLong,self._mavros_prefix + "/mavros/cmd/command")

        # --- mission services ---
        if _MISSION_AVAILABLE:
            self.mission_push_client = self.create_client(WaypointPush, self._mavros_prefix + "/mavros/mission/push")
            self.mission_clear_client = self.create_client(WaypointClear, self._mavros_prefix + "/mavros/mission/clear")
        else:
            self.mission_push_client = None
            self.mission_clear_client = None

        # FCU 连接等待改为外部方法，在启动 MAVROS 之后调用
        self.get_logger().info(f"UAV{self._vid} Waiting for FCU connection...")
        # --- 先发送一段时间的零速度（原始逻辑 for _ in range(100)）---
        for _ in range(60):
            self.pub_velocity(0.0, 0.0, 0.0, 0.0)
            spin_sleep(self, 20.0)

        # --- “请求对象”在 ROS2 中直接用 .Request() ---
        self.offb_set_mode = SetMode.Request()
        self.arm_cmd       = CommandBool.Request()
        self.last_req_time = self.get_clock().now()

        # RC
        self.current_rc_in = None
        self.offboard_channel_value = 2000  # channel 7 (index 6)

        # 轨迹（可选：若你要跑轨迹）
        self.waypoints = []
        self.init_height = INIT_HEIGHT  # 起飞高度

        # Hover target maintained while waiting for HTTP commands
        self.hover_target = None  # (x, y, z)
        
        # 默认端口按车辆ID映射: gateway=5008, controllers从5009开始
        default_port = 5009 + int(self._vid) if self._vid is not None else DEFAULT_HTTP_PORT
        self._http_port = int(os.environ.get("PEGASUS_HTTP_PORT", str(default_port)))
        self.get_logger().info(f"HTTP server port: {self._http_port}")
        # env_id 用于健康检查区分不同实例（命名空间+端口）
        self._env_id = f"{self._mavros_ns or 'default'}:{self._http_port}"
        self._http_app = None
        self._http_thread = None
        self._task_lock = threading.Lock()
        # Minimal batch state placeholders to match env_server's responses
        self._batch_states: List[Dict[str, Any]] = [{"done": False}]
        self._batch_size: int = 0
        self._json_name_list: List[Optional[str]] = []
        self._instructions_list: List[Optional[str]] = []
        self._color_topic: Optional[str] = None
        self._color_sub = None
        self._last_color_msg: Optional[Image] = None
        self._image_from_ros = bool(IMAGE_FROM_ROS)
        self._image_http_base = str(IMAGE_HTTP_URL)
        self._step_return_each = bool(STEP_RETURN_EACH)
        self._setup_http_server()
        self._http_api = HttpApi(self)
        self._image_service = ImageService(self._image_from_ros, self._image_http_base, self._vid)
        self._reset_cancel_event = threading.Event()

        # MAVROS launch file path (set externally or derived from config)
        self._mavros_launch_file: Optional[Path] = None
        self._mavros_log_dir: Optional[Path] = None

        # Hover loop 暂停控制
        self._hover_paused = False
        self._hover_pause_lock = threading.Lock()
        self._hover_pause_event = threading.Event()
        self._hover_pause_event.set()  # 初始非暂停状态

        # --- helper: configure MAVROS time sync after launch (as a method) ---
        # Intention: reduce/disable TIMESYNC to avoid PX4 "RTT too high" warnings
        # Tries setting params on '/<ns>/mavros/time', then falls back to '/<ns>/mavros'
        # Safe to call multiple times; ignores missing services gracefully.
        def _make_param(name: str, value):
            p = Parameter()
            p.name = name
            pv = ParameterValue()
            if isinstance(value, bool):
                pv.type = ParameterType.PARAMETER_BOOL
                pv.bool_value = value
            elif isinstance(value, (float, int)):
                pv.type = ParameterType.PARAMETER_DOUBLE
                pv.double_value = float(value)
            else:
                pv.type = ParameterType.PARAMETER_STRING
                pv.string_value = str(value)
            p.value = pv
            return p

        def _set_params(node_path: str, params: dict, timeout_sec: float = 3.0) -> bool:
            try:
                client = self.create_client(SetParameters, f"{node_path}/set_parameters")
            except Exception as e:
                self.get_logger().warn(f"Create SetParameters client for {node_path} failed: {e}")
                return False
            if not client.wait_for_service(timeout_sec=timeout_sec):
                self.get_logger().warn(f"SetParameters service for {node_path} not ready")
                return False
            req = SetParameters.Request()
            req.parameters = [_make_param(k, v) for k, v in params.items()]
            try:
                future = client.call_async(req)
                rclpy.spin_until_future_complete(self, future, timeout_sec=timeout_sec)
                result = future.result()
                ok = True
                if result is None:
                    ok = False
                else:
                    for r in result.results:
                        if not r.successful:
                            ok = False
                            break
                return ok
            except Exception as e:
                self.get_logger().warn(f"Call SetParameters for {node_path} failed: {e}")
                return False

        def _set_params_candidates(params: dict, timeout_sec: float = 3.0) -> bool:
            ns_prefix = self._mavros_prefix or ""
            candidates = []
            if ns_prefix:
                candidates.extend([f"{ns_prefix}/mavros", f"{ns_prefix}/mavros/mavros"])
            else:
                candidates.extend(["/mavros", "/mavros/mavros"])
            ok_any = False
            for node in candidates:
                if _set_params(node, params, timeout_sec=timeout_sec):
                    ok_any = True
                    break
            return ok_any

        # def _set_px4_params(params: dict, timeout_sec: float = 3.0) -> bool:
        #     if not _PARAM_AVAILABLE:
        #         self.get_logger().warn("MAVROS ParamSet not available; skip PX4 param configuration")
        #         return False
        #     srv = f"{self._mavros_prefix}/mavros/param/set" if self._mavros_prefix else "/mavros/param/set"
        #     try:
        #         client = self.create_client(ParamSet, srv)
        #     except Exception as e:
        #         self.get_logger().warn(f"Create ParamSet client failed: {e}")
        #         return False
        #     if not client.wait_for_service(timeout_sec=timeout_sec):
        #         self.get_logger().warn("ParamSet service not ready; skip PX4 param configuration")
        #         return False
        #     ok_any = False
        #     for name, value in params.items():
        #         req = ParamSet.Request()
        #         req.param_id = str(name)
        #         pv = MavrosParamValue()
        #         if isinstance(value, bool):
        #             pv.integer = int(value)
        #             pv.real = float(int(value))
        #         elif isinstance(value, int):
        #             pv.integer = int(value)
        #             pv.real = float(value)
        #         else:
        #             pv.integer = 0
        #             pv.real = float(value)
        #         req.value = pv
        #         try:
        #             call_service_sync(self, client, req, timeout_sec=timeout_sec)
        #             ok_any = True
        #         except Exception as e:
        #             self.get_logger().warn(f"Param set failed: {name} -> {value}; {e}")
        #     return ok_any

        def _set_px4_params_impl(params: dict, timeout_sec: float = 3.0) -> bool:
            if not _PARAM_AVAILABLE:
                self.get_logger().warn("MAVROS ParamSet not available; skip PX4 param configuration")
                return False
            srv = f"{self._mavros_prefix}/mavros/param/set" if self._mavros_prefix else "/mavros/param/set"
            try:
                client = self.create_client(ParamSet, srv)
            except Exception as e:
                self.get_logger().warn(f"Create ParamSet client failed: {e}")
                return False
            if not client.wait_for_service(timeout_sec=timeout_sec):
                self.get_logger().warn("ParamSet service not ready; skip PX4 param configuration")
                return False
            ok_any = False
            for name, value in params.items():
                req = ParamSet.Request()
                req.param_id = str(name)
                pv = MavrosParamValue()
                if isinstance(value, bool):
                    pv.integer = int(value)
                    pv.real = float(int(value))
                elif isinstance(value, int):
                    pv.integer = int(value)
                    pv.real = float(value)
                else:
                    pv.integer = 0
                    pv.real = float(value)
                req.value = pv
                try:
                    call_service_sync(self, client, req, timeout_sec=timeout_sec)
                    ok_any = True
                    self.get_logger().info(f"Set PX4 param {name} to {value} success.")
                except Exception as e:
                    self.get_logger().warn(f"Param set failed: {name} -> {value}; {e}")
            return ok_any
        
        # Expose
        self.set_px4_params = _set_px4_params_impl

        def configure_unlimited_flight_method():
            fallback_params = {
                "time.timesync_mode": "MAVLINK",
                "time.timesync_rate": 0.0,
                "time.system_time_rate": 10.0,
                "use_sim_time": False,
            }
            ok_any = _set_params_candidates(fallback_params, timeout_sec=3.0)
            if ok_any:
                self.get_logger().info("Configured MAVROS time sync parameters (via candidates)")
            else:
                self.get_logger().warn("Failed to configure MAVROS time sync parameters; proceeding anyway")

            # PX4 params for unlimited flight (disable EKF checks, allow arm without GPS, remove geofence limits)
            # px4_params = {
            #     "COM_ARM_EKF": 0.0,
            #     "COM_ARM_IMU": 0.0,
            #     "COM_ARM_WO_GPS": 1.0,
            #     "EKF2_GPS_CHECK": 0.0,
            #     "EKF2_AID_REQ": 0.0,
            #     "EKF2_REQ_GPS_H": 0.0,
            #     "EKF2_REQ_GPS_V": 0.0,
            #     "EKF2_REQ_IMU": 0.0,
            #     "EKF2_REQ_MAG": 0.0,
            #     "EKF2_REQ_BARO": 0.0,
            #     "GF_ACTION": 0.0,
            #     "GF_MAX_HOR_DIST": 3.4e38,
            #     "GF_MAX_VER_DIST": 3.4e38,
            #     "GF_SOURCE": 0.0,
            #     "GF_ENABLE": 0.0,
            # }
            # _set_px4_params(px4_params, timeout_sec=5.0)
            return True
        # expose as instance method（直接替换原方法名）
        self.configure_mavros_time_plugin = configure_unlimited_flight_method
            
    # ---------- HTTP helpers ----------
    def _setup_http_server(self):
        app = Flask(f"MAVROS_Based_UAV_Controller_{self._vid}")
        @app.before_request
        def _http_log_pre():
            try:
                g._http_start = time.time()
                if request.path == '/health':
                    g._skip_http_log = True
                    return
                g._skip_http_log = False
                payload = request.get_json(silent=True)
                if isinstance(payload, (dict, list)):
                    body = json.dumps(payload, ensure_ascii=False)
                else:
                    body = str(payload)
                self.get_logger().info(f"HTTP IN {request.method} {request.path} body={body}")
            except Exception as e:
                import traceback
                self.get_logger().error(f"HTTP IN log error: {e}\n{traceback.format_exc()}")
        @app.after_request
        def _http_log_post(resp):
            try:
                start = getattr(g, "_http_start", None)
                end = time.time()
                dur_ms = int(((end - start) if start else 0.0) * 1000)
                if getattr(g, "_skip_http_log", False) and request.path == '/health':
                    return resp
                try:
                    text = resp.get_data(as_text=True)
                except Exception:
                    text = "<non-text>"
                if text is None:
                    text = ""
                if len(text) > 2000:
                    text_out = text[:2000] + f"...(len={len(text)})"
                else:
                    text_out = text
                self.get_logger().info(f"HTTP OUT {request.method} {request.path} status={resp.status_code} dur_ms={dur_ms} resp={text_out}")
            except Exception as e:
                import traceback
                self.get_logger().error(f"HTTP OUT log error: {e}\n{traceback.format_exc()}")
            return resp
        @app.route('/reset', methods=['POST'])
        def http_reset():
            data = request.json or {}
            resp, code = self._http_api.reset(data)
            return jsonify(resp), code
        @app.route('/command', methods=['POST'])
        def http_command():
            data = request.json or {}
            resp, code = self._http_api.command(data)
            return jsonify(resp), code
        @app.route('/step', methods=['POST'])
        def http_step():
            data = request.json or {}
            resp, code = self._http_api.step(data, use_ros_image=True)
            return jsonify(resp), code
        @app.route('/step_http', methods=['POST'])
        def http_step_http():
            data = request.json or {}
            resp, code = self._http_api.step(data, use_ros_image=False)
            return jsonify(resp), code
        @app.route('/health', methods=['GET'])
        def http_health():
            return jsonify({"status": "healthy", "env_id": self._env_id})
        self._http_app = app

    def _process_step_api(self, data: Dict[str, Any], use_ros_image: bool) -> Tuple[Dict[str, Any], int]:
        actions_batch = data.get("actions", None)
        if actions_batch is None or not isinstance(actions_batch, list) or len(actions_batch) == 0:
            return {"status": "error", "message": "actions is required and must be a non-empty list"}, 400
        if not self._batch_states:
            return {"status": "error", "message": "Environment not reset. Please call /reset first."}, 400
        first_actions = actions_batch[0] if actions_batch else []
        per_step = bool(data.get("per_step", self._step_return_each))
        old = self._image_from_ros
        self._image_from_ros = bool(use_ros_image)
        pts: List[List[float]] = []
        if isinstance(first_actions, list) and len(first_actions) > 0:
            for pt in first_actions:
                if isinstance(pt, (list, tuple)) and len(pt) >= 3:
                    pts.append([float(pt[0]), float(pt[1]), float(pt[2])])
        images_batch: List[List[str]] = []
        done_batch: List[bool] = []
        self.get_logger().info(f"[_process_step_api] pts: {pts}, per_step: {per_step}, use_ros_image: {use_ros_image}, batch_size: {len(self._batch_states)}, actions_batch: {actions_batch}, done_batch: {done_batch}, images_batch: {images_batch}")
        if pts:
            if per_step and len(pts) > 1:
                frames: List[str] = []
                for x, y, z in pts:
                    self.get_logger().info(f"[_process_step_api] move_to_and_wait: [{x}, {y}, {z}]")
                    self.move_to_and_wait(float(x), float(y), float(z))
                    b = self._capture_image_b64()
                    if b:
                        frames.append(b)
                images_batch.append(frames)
            else:
                self.get_logger().info(f"[_process_step_api] move_to_and_wait sequence: {pts}")
                for x, y, z in pts:
                    self.move_to_and_wait(float(x), float(y), float(z))
                b = self._capture_image_b64()
                images_batch.append([b] if b else [])
        for _ in range(len(self._batch_states)):
            done_batch.append(False)
        resp = {"status": "success", "images": images_batch, "dones": done_batch, "message": "Step executed successfully", "batch_size": len(images_batch)}
        self._image_from_ros = old
        return resp, 200

    def start_http_server(self):
        if self._http_app is not None:
            def _run():
                try:
                    self._external_spin = True
                    self._http_app.run(host='0.0.0.0', port=self._http_port, threaded=True, debug=False, use_reloader=False)
                except Exception as e:
                    self.get_logger().warn(f"[ERROR] HTTP server run failed: {e}")
            t = Thread(target=_run, daemon=True)
            t.start()
            self._http_thread = t

    def _http_hover_loop(self):
        self.get_logger().info("Entering hover + HTTP command loop...")
        hz = CONTROL_LOOP_HZ
        while rclpy.ok() and not self._shutdown_requested:
            try:
                # 检查是否暂停
                if self._hover_paused:
                    # 等待恢复信号，最多等待 0.1 秒后继续检查
                    self._hover_pause_event.wait(timeout=0.1)
                    continue

                li = bool(getattr(self, "_landing_in_progress", False))
                if not li and self.hover_target is not None:
                    x, y, z = self.hover_target
                    try:
                        self.pub_position(x, y, z)
                    except Exception as e:
                        self.get_logger().error(f"Hover loop publish failed: {e}")
                if not li and not self._external_spin:
                    try:
                        rclpy.spin_once(self, timeout_sec=0.05)
                    except Exception as e:
                        import traceback as _tb
                        tb = _tb.format_exc()
                        mode = getattr(self.current_state, "mode", "")
                        armed = bool(getattr(self.current_state, "armed", False))
                        z = float(getattr(getattr(self, "current_pose", PoseStamped()), "pose", PoseStamped().pose).position.z or 0.0)
                        ls = int(getattr(self.extended_state, "landed_state", 0))
                        self.get_logger().error(f"Hover loop exception: {e}")
                        self.get_logger().error(f"Hover loop traceback: {tb}")
                        self.get_logger().error(f"Hover loop context: mode={mode} armed={armed} z={z:.3f} landed_state={ls} landing_in_progress={li}")
            except Exception as e:
                import traceback as _tb
                tb = _tb.format_exc()
                self.get_logger().error(f"Hover loop outer exception: {e}")
                self.get_logger().error(f"Hover loop outer traceback: {tb}")
            time.sleep(1.0 / hz)

    def pause_hover_loop(self):
        """暂停 hover loop 并等待当前发送完成。"""
        with self._hover_pause_lock:
            if self._hover_paused:
                return
            self._hover_paused = True
            self._hover_pause_event.clear()
            self.get_logger().info(f"[hover] UAV{self._vid} hover loop paused")
            # 等待一个周期确保当前发送完成
            time.sleep(1.0 / CONTROL_LOOP_HZ + 0.05)

    def resume_hover_loop(self, new_target: Optional[Tuple[float, float, float]] = None):
        """恢复 hover loop，可选更新目标位置。"""
        with self._hover_pause_lock:
            if new_target is not None:
                self.hover_target = new_target
                self.get_logger().info(f"[hover] UAV{self._vid} hover target updated to {new_target}")
            self._hover_paused = False
            self._hover_pause_event.set()
            self.get_logger().info(f"[hover] UAV{self._vid} hover loop resumed")

    def wait_for_mavros_connected(self, timeout: float = 60.0) -> bool:
        """等待 MAVROS 连接成功。"""
        self.get_logger().info(f"[mavros] UAV{self._vid} waiting for MAVROS connection (timeout={timeout}s)...")
        start = time.time()
        while time.time() - start < timeout:
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            if self._mavros_connected:
                self.get_logger().info(f"[mavros] UAV{self._vid} MAVROS connected!")
                return True
            # spin 一下以接收消息
            try:
                rclpy.spin_once(self, timeout_sec=0.1)
            except Exception:
                pass
            time.sleep(0.2)
        self.get_logger().warn(f"[mavros] UAV{self._vid} MAVROS connection timeout!")
        return False

    # ---------- Callbacks ----------
    def state_cb(self, msg: State):
        self.current_state = msg
        self._state_updates += 1
        # 更新 MAVROS 连接状态
        was_connected = self._mavros_connected
        self._mavros_connected = bool(getattr(msg, "connected", False))
        if self._mavros_connected and not was_connected:
            self._mavros_connected_event.set()
            self.get_logger().info(f"[state_cb] UAV{self._vid} MAVROS connected")
        elif not self._mavros_connected and was_connected:
            self._mavros_connected_event.clear()
            self.get_logger().warn(f"[state_cb] UAV{self._vid} MAVROS disconnected")

    def pose_cb(self, msg: PoseStamped):
        self.current_pose = msg
        self._pose_updates += 1
    
    def extended_state_cb(self, msg: ExtendedState):
        self.extended_state = msg
    
    def rc_in_callback(self, msg: RCIn):
        self.current_rc_in = msg
        try:
            if msg.channels and len(msg.channels) > 6:
                self.offboard_channel_value = msg.channels[6]
        except Exception as e:
            self.get_logger().warn(f"[WARN] rc_in_callback failed to update offboard channel: {e}")

    def _perform_takeoff(self, target_x: float, target_y: float, target_z: float, log_tag: str):
        self.start_height = float(self.current_pose.pose.position.z)
        try:
            self.get_logger().info(f"{log_tag} Current height: {self.start_height}")
        except Exception as e:
            self.get_logger().warn(f"{log_tag} Current height log failed: {e}")
            self.get_logger().warn(traceback.format_exc())
        required_height = max(0.0, float(target_z) - float(self.start_height))
        reach_eps = max(0.2, TAKEOFF_REACH_THRESHOLD)
        progress_counter = 0
        progress_bar = tqdm(total=required_height, desc=f"{log_tag} 起飞进度", unit="m")
        has_reached_initial_point = False
        stable_start = None
        last_progress = None
        stagnant_since = None
        while not has_reached_initial_point:
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            self.pub_position(target_x, target_y, target_z)
            cur_z = float(self.current_pose.pose.position.z)
            climb = max(0.0, cur_z - float(self.start_height))
            shown = min(required_height, climb)
            progress_counter += 1
            if progress_counter >= 20:
                progress_counter = 0
                progress_bar.n = shown
                progress_bar.last_print_n = shown
                progress_bar.update(0)

            if last_progress is None or abs(shown - last_progress) > 1e-3:
                last_progress = shown
                stagnant_since = None

            z_err = abs(float(self.current_pose.pose.position.z) - float(target_z))

            if required_height > 0.0 and shown >= (required_height - reach_eps * 0.5):
                has_reached_initial_point = True
                progress_bar.n = required_height
                progress_bar.last_print_n = required_height
                progress_bar.update(0)
                try:
                    self.get_logger().info(f"{log_tag} ***** Reached initial point (progress close) *****")
                except Exception as e:
                    self.get_logger().warn(f"{log_tag} Reached point log failed: {e}")
                    self.get_logger().warn(traceback.format_exc())
                continue

            if z_err < reach_eps:
                if stable_start is None:
                    stable_start = time.time()
                if (time.time() - stable_start) > 1.0:
                    has_reached_initial_point = True
                    progress_bar.n = required_height
                    progress_bar.last_print_n = required_height
                    progress_bar.update(0)
                    try:
                        self.get_logger().info(f"{log_tag} ***** Reached initial point (stable) *****")
                    except Exception as e:
                        self.get_logger().warn(f"{log_tag} Reached point log failed: {e}")
                        self.get_logger().warn(traceback.format_exc())
            else:
                stable_start = None

            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.05)

        progress_bar.n = required_height
        progress_bar.last_print_n = required_height
        progress_bar.update(0)
        progress_bar.close()
        try:
            self.get_logger().info(f"{log_tag} Takeoff complete.")
        except Exception as e:
            self.get_logger().warn(f"{log_tag} Takeoff complete log failed: {e}")
            self.get_logger().warn(traceback.format_exc())
        self.hover_target = (
            self.current_pose.pose.position.x,
            self.current_pose.pose.position.y,
            self.current_pose.pose.position.z,
        )

    def wait_until_attitude_stable(self, timeout_s: float = 30.0, window_s: float = 1.5, eps: float = 0.05) -> bool:
        start = time.time()
        stable_since = None
        last_q = None
        while rclpy.ok() and (time.time() - start) < float(timeout_s):
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            q = self.current_pose.pose.orientation
            curr = (float(q.x), float(q.y), float(q.z), float(q.w))
            if last_q is not None:
                dot = abs(curr[0] * last_q[0] + curr[1] * last_q[1] + curr[2] * last_q[2] + curr[3] * last_q[3])
                dot = max(min(dot, 1.0), -1.0)
                ang = 2.0 * math.acos(dot)
                if ang < float(eps):
                    if stable_since is None:
                        stable_since = time.time()
                    if (time.time() - stable_since) >= float(window_s):
                        return True
                else:
                    stable_since = None
            last_q = curr
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.05)
        return False

    def move_to_and_wait(self, x: float, y: float, z: float, threshold: float = MOVE_REACH_THRESHOLD, timeout_s: float = 60.0) -> bool:
        """
        移动到指定位置并等待到达且稳定。

        Args:
            x, y, z: 目标位置
            threshold: 位置误差阈值（米）
            timeout_s: 超时时间（秒）

        Returns:
            True 如果成功到达并稳定，False 如果超时
        """
        start = time.time()
        stable_since = None

        while rclpy.ok() and (time.time() - start) < float(timeout_s):
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")

            self.pub_position(x, y, z)
            self.hover_target = (x, y, z)

            # 计算位置误差
            dx = self.current_pose.pose.position.x - x
            dy = self.current_pose.pose.position.y - y
            dz = self.current_pose.pose.position.z - z
            dist = math.sqrt(dx * dx + dy * dy + dz * dz)

            if dist < float(threshold):
                # 位置接近目标，开始检查稳定性
                if stable_since is None:
                    stable_since = time.time()

                # 等待稳定时间窗口
                if (time.time() - stable_since) >= MOVE_STABLE_WINDOW_S:
                    return True
            else:
                # 位置不够接近，重置稳定计时器
                stable_since = None

            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.05)
        return False

    # ========== MAVROS Mission Mode Methods ==========

    def clear_mission(self, timeout_s: float = 10.0) -> bool:
        """清除所有 mission 航点"""
        if not _MISSION_AVAILABLE or self.mission_clear_client is None:
            self.get_logger().warn("[mission] WaypointClear service not available")
            return False

        if not self.mission_clear_client.wait_for_service(timeout_sec=5.0):
            self.get_logger().warn("[mission] WaypointClear service not ready")
            return False

        req = WaypointClear.Request()
        try:
            result = call_service_sync(self, self.mission_clear_client, req, timeout_sec=timeout_s)
            if result is not None and getattr(result, 'success', False):
                self.get_logger().info("[mission] Waypoints cleared successfully")
                return True
            else:
                self.get_logger().warn("[mission] Failed to clear waypoints")
                return False
        except TimeoutError:
            self.get_logger().warn("[mission] Clear waypoints timed out")
            return False
        except Exception as e:
            self.get_logger().error(f"[mission] Clear waypoints exception: {e}")
            return False

    def push_mission(self, waypoints: list, start_index: int = 0, timeout_s: float = 30.0) -> bool:
        """
        推送 mission 航点到 FCU

        Args:
            waypoints: 航点列表，每个航点为 (x, y, z) 元组（本地坐标，单位：米）
                       x=North, y=East, z=Altitude (正值向上)
                       注意：z 是高度而非 NED 的 Down
            start_index: 起始航点索引
            timeout_s: 超时时间

        Returns:
            是否成功

        注意：PX4 SITL 的 mission 模式需要 GPS 坐标 (MAV_FRAME_GLOBAL_RELATIVE_ALT=3)。
        本函数会将本地坐标转换为 GPS 坐标。
        """
        if not _MISSION_AVAILABLE or self.mission_push_client is None:
            self.get_logger().warn("[mission] WaypointPush service not available")
            return False

        if not self.mission_push_client.wait_for_service(timeout_sec=5.0):
            self.get_logger().warn("[mission] WaypointPush service not ready")
            return False

        if not waypoints:
            self.get_logger().warn("[mission] No waypoints to push")
            return False

        # GPS 参考点（与 Pegasus configs.yaml 保持一致）
        from pyproj import Transformer, CRS

        REF_LAT = 47.397742  # 参考纬度 (PX4 SITL 默认)
        REF_LON = 8.545594   # 参考经度 (PX4 SITL 默认)
        REF_ALT = 488.0      # 参考高度 (PX4 SITL 默认)

        # 创建本地 ENU 坐标系到 WGS84 的转换器
        # 使用 EPSG:4326 (WGS84) 和以参考点为中心的本地切平面坐标系
        local_crs = CRS.from_proj4(
            f"+proj=tmerc +lat_0={REF_LAT} +lon_0={REF_LON} +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
        )
        wgs84_crs = CRS.from_epsg(4326)
        transformer = Transformer.from_crs(local_crs, wgs84_crs, always_xy=True)

        def local_to_gps(north, east, altitude):
            """将本地坐标转换为 GPS 坐标（使用 pyproj 精确计算）

            Args:
                north: North方向 (米) - 对应 Transverse Mercator 的 Y
                east: East方向 (米) - 对应 Transverse Mercator 的 X
                altitude: 高度 (米，正值向上，相对高度)

            Returns:
                (latitude, longitude, altitude) - GPS坐标和相对高度
            """
            # pyproj transformer expects (x, y) = (east, north) for tmerc
            lon, lat = transformer.transform(east, north)
            return lat, lon, altitude

        # 构建航点列表
        wp_list = []

        # 注意：不显式添加 HOME 航点，让 PX4 使用当前位置作为 HOME
        # PX4 mission 模式会自动将第一个 TAKEOFF 航点作为起点

        for i, (x, y, z) in enumerate(waypoints):
            wp = Waypoint()
            wp.frame = 3  # MAV_FRAME_GLOBAL_RELATIVE_ALT

            if i == 0:
                # 第一个航点使用 TAKEOFF 命令
                wp.command = 22  # MAV_CMD_NAV_TAKEOFF
            else:
                # 后续航点使用 WAYPOINT 命令
                wp.command = 16  # MAV_CMD_NAV_WAYPOINT

            wp.is_current = (i == start_index)
            wp.autocontinue = True
            wp.param1 = 0.0  # Hold time (s) / Pitch for takeoff
            wp.param2 = MOVE_REACH_THRESHOLD  # Acceptance radius (m)
            wp.param3 = 0.0  # Pass through (0=no)
            wp.param4 = float('nan')  # Desired yaw (NaN=unchanged)

            # 转换为 GPS 坐标 (输入: x=North, y=East, z=Altitude 正值向上)
            lat, lon, alt = local_to_gps(x, y, z)
            wp.x_lat = lat
            wp.y_long = lon
            wp.z_alt = alt

            wp_list.append(wp)
            self.get_logger().info(f"[mission] WP{i}: NED=({x:.2f}, {y:.2f}, {z:.2f}) -> GPS=({lat:.6f}, {lon:.6f}, {alt:.1f}m) cmd={wp.command}")

        self.get_logger().info(f"[mission] Pushing {len(wp_list)} waypoints to FCU")

        req = WaypointPush.Request()
        req.start_index = start_index
        req.waypoints = wp_list

        try:
            result = call_service_sync(self, self.mission_push_client, req, timeout_sec=timeout_s)
            if result is not None:
                if getattr(result, 'success', False):
                    self.get_logger().info(f"[mission] Pushed {getattr(result, 'wp_transfered', 0)} waypoints successfully")
                    return True
                else:
                    self.get_logger().warn(f"[mission] Failed to push waypoints, transferred={getattr(result, 'wp_transfered', 0)}")
                    return False
            else:
                self.get_logger().warn("[mission] Push waypoints timed out (result is None)")
                return False
        except TimeoutError:
            self.get_logger().warn("[mission] Push waypoints timed out")
            return False
        except Exception as e:
            self.get_logger().error(f"[mission] Push waypoints exception: {e}")
            return False

    def start_mission(self, timeout_s: float = 10.0) -> bool:
        """切换到 AUTO.MISSION 模式开始执行任务"""
        mode_req = SetMode.Request()
        mode_req.custom_mode = "AUTO.MISSION"

        try:
            resp = call_service_sync(self, self.set_mode_client, mode_req, timeout_sec=timeout_s)
            if resp and getattr(resp, "mode_sent", False):
                self.get_logger().info("[mission] Switched to AUTO.MISSION mode")
                return True
            else:
                self.get_logger().warn("[mission] Failed to switch to AUTO.MISSION mode")
                return False
        except Exception as e:
            self.get_logger().error(f"[mission] Set mode exception: {e}")
            return False

    def execute_mission(
        self,
        waypoints: list,
        timeout_s: float = 300.0,
        completion_threshold: float = MOVE_REACH_THRESHOLD,
    ) -> bool:
        """
        执行完整的 mission 飞行任务

        Args:
            waypoints: 航点列表，每个为 (x, y, z) 元组
            timeout_s: 整体超时时间
            completion_threshold: 到达最后一个航点的判定阈值

        Returns:
            是否成功完成任务
        """
        if not waypoints:
            self.get_logger().warn("[mission] No waypoints provided")
            return False

        self.get_logger().info(f"[mission] Starting mission with {len(waypoints)} waypoints")

        # Step 1: 清除旧航点
        if not self.clear_mission():
            self.get_logger().warn("[mission] Failed to clear old waypoints, continuing anyway")

        # Step 2: 推送新航点
        if not self.push_mission(waypoints):
            self.get_logger().error("[mission] Failed to push waypoints")
            return False

        # Step 3: 确保已解锁
        if not self.current_state.armed:
            self.get_logger().info("[mission] Arming vehicle...")
            self.arm_cmd.value = True
            resp = call_service_sync(self, self.arming_client, self.arm_cmd, timeout_sec=ARMING_TIMEOUT_S)
            if not (resp and getattr(resp, "success", False)):
                self.get_logger().error("[mission] Failed to arm")
                return False

        # Step 4: 切换到 AUTO.MISSION 模式
        if not self.start_mission():
            self.get_logger().error("[mission] Failed to start mission mode")
            return False

        # Step 5: 等待任务完成（监控到达最后一个航点）
        last_wp = waypoints[-1]
        start_time = time.time()

        self.get_logger().info(f"[mission] Waiting for mission completion, last wp=({last_wp[0]:.2f}, {last_wp[1]:.2f}, {last_wp[2]:.2f})")

        while rclpy.ok() and (time.time() - start_time) < timeout_s:
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")

            # 检查是否到达最后一个航点
            dx = self.current_pose.pose.position.x - last_wp[0]
            dy = self.current_pose.pose.position.y - last_wp[1]
            dz = self.current_pose.pose.position.z - last_wp[2]
            dist = math.sqrt(dx * dx + dy * dy + dz * dz)

            if dist < completion_threshold:
                self.get_logger().info(f"[mission] Reached last waypoint (dist={dist:.3f}m)")
                return True

            # 检查是否还在 MISSION 模式
            if self.current_state.mode != "AUTO.MISSION":
                self.get_logger().warn(f"[mission] Mode changed to {self.current_state.mode}, mission may have ended")
                # 可能是任务完成后自动切换了模式
                if dist < completion_threshold * 2:
                    return True

            rclpy.spin_once(self, timeout_sec=0.1)
            time.sleep(0.1)

        self.get_logger().warn(f"[mission] Mission timeout after {timeout_s}s")
        return False

    def switch_to_offboard(self, timeout_s: float = 10.0) -> bool:
        """切换回 OFFBOARD 模式"""
        self.offb_set_mode.custom_mode = "OFFBOARD"
        try:
            resp = call_service_sync(self, self.set_mode_client, self.offb_set_mode, timeout_sec=timeout_s)
            if resp and getattr(resp, "mode_sent", False):
                self.get_logger().info("[mission] Switched back to OFFBOARD mode")
                return True
            else:
                self.get_logger().warn("[mission] Failed to switch to OFFBOARD mode")
                return False
        except Exception as e:
            self.get_logger().error(f"[mission] Switch to OFFBOARD exception: {e}")
            return False

    # ---------- Core API ----------
    def reset(self, target_z: float = None):
        if target_z is None:
            target_z = self.init_height

        if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
            raise InterruptedError("reset cancelled")

        # PX4 需要先收到 setpoint 流才能允许 arm（安全机制）
        # 在 arm 之前先发送一些 setpoint
        self.get_logger().info("[reset] Publishing setpoints before arm (PX4 requirement)...")
        for _ in range(30):
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            self.pub_position(0.0, 0.0, target_z)
            spin_sleep(self, 20.0)

        self.arm_cmd.value = True
        while not self.current_state.armed:
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            self.get_logger().info("[reset] Sending arm command...")
            resp = call_service_sync(self, self.arming_client, self.arm_cmd, timeout_sec=ARMING_TIMEOUT_S)
            if resp and getattr(resp, "success", False):
                self.get_logger().info("[reset] ***** Vehicle armed *****")
                break
            spin_sleep(self, 2.0)
        self.get_logger().info("[reset] Vehicle armed.")

        self.get_logger().info("[reset] Publishing initial dummy setpoints to prevent RTL...")
        for _ in range(60): 
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            self.pub_position(0.0, 0.0, target_z)
            spin_sleep(self, 20.0)
        
        self.get_logger().info(f"[reset] UAV{self._vid} Initial setpoints published.")
        
        self.offb_set_mode.custom_mode = "OFFBOARD"
        while self.current_state.mode != "OFFBOARD":
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            resp = call_service_sync(self, self.set_mode_client, self.offb_set_mode, timeout_sec=SETMODE_TIMEOUT_S)
            if resp and getattr(resp, "mode_sent", False):
                self.get_logger().info("[reset] ***** OFFBOARD enabled *****")
            spin_sleep(self, 2.0)
        self.get_logger().info("[reset] Vehicle in OFFBOARD mode.")

        target_x, target_y = 0.0, 0.0
        self._perform_takeoff(target_x, target_y, target_z, "[reset]")

    def _sim_move_uav(self, position: Optional[List[float]] = None, yaw_deg: Optional[float] = None) -> bool:
        """通过仿真端 HTTP 接口重置 UAV 位置。"""
        vid = int(self._vid)
        if position is None or not isinstance(position, (list, tuple)) or len(position) < 3:
            return False
        try:
            import urllib.request, json as _json
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            url = f"{self._image_http_base}/uav/{vid}/reset"
            payload = _json.dumps({"position": [float(position[0]), float(position[1]), float(position[2])], "yaw_deg": yaw_deg}).encode("utf-8")
            req = urllib.request.Request(url, data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=60) as resp:
                _ = resp.read()
                self.get_logger().info(f"[sim_move] UAV{vid} reset response from PGSIM: {_}")
                return True
        except Exception as e:
            self.get_logger().warn(f"[sim_move] UAV{vid} reboot move Request failed: {e}")
            return False

    def _sim_px4_hard_reset(self) -> bool:
        """通过仿真端 HTTP 接口恢复 PX4（使用新的 /px4/recover 接口）。"""
        vid = int(self._vid)
        try:
            import urllib.request
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            url = f"{self._image_http_base}/uav/{vid}/px4/recover"
            req = urllib.request.Request(url, data=b"{}", method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
                self.get_logger().info(f"[px4_recover] UAV{vid} response: {raw}")
                return True
        except Exception as e:
            self.get_logger().warn(f"[px4_recover] UAV{vid} request failed: {e}")
            self.get_logger().warn(traceback.format_exc())
            return False

    def _sim_px4_relaunch(self) -> bool:
        """通过仿真端 HTTP 接口恢复 PX4（使用新的 /px4/recover 接口）。"""
        vid = int(self._vid)
        try:
            import urllib.request
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            url = f"{self._image_http_base}/uav/{vid}/px4/recover"
            req = urllib.request.Request(url, data=b"{}", method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
                self.get_logger().info(f"[px4_recover] UAV{vid} response: {raw}")
                return True
        except Exception as e:
            self.get_logger().warn(f"[px4_recover] UAV{vid} request failed: {e}")
            self.get_logger().warn(traceback.format_exc())
            return False

    def _sim_px4_status(self) -> Optional[Dict[str, Any]]:
        """通过仿真端 HTTP 接口查询 PX4 状态。"""
        vid = int(self._vid)
        try:
            import urllib.request, json as _json
            url = f"{self._image_http_base}/uav/{vid}/px4/status"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                raw = resp.read()
                obj = _json.loads(raw.decode("utf-8"))
                return obj.get("px4_backend", {})
        except Exception as e:
            self.get_logger().warn(f"[_sim_px4_status] UAV{vid} request failed: {e}")
            self.get_logger().warn(traceback.format_exc())
            return None

    def set_mavros_launch_file(self, launch_file: Path, log_dir: Optional[Path] = None):
        """设置 MAVROS launch 文件路径，供后续启动/重启使用。"""
        self._mavros_launch_file = Path(launch_file) if launch_file else None
        self._mavros_log_dir = Path(log_dir) if log_dir else None

    def start_mavros(self) -> bool:
        """启动当前 UAV 的 MAVROS 实例。"""
        if self._mavros_launch_file is None:
            self.get_logger().warn(f"[start_mavros] UAV{self._vid} no launch file configured")
            return False
        return MavrosManager.start_mavros(self._vid, self._mavros_launch_file, self._mavros_log_dir)

    def stop_mavros(self) -> bool:
        """停止当前 UAV 的 MAVROS 实例。"""
        return MavrosManager.stop_mavros(self._vid)

    def restart_mavros(self) -> bool:
        """重启当前 UAV 的 MAVROS 实例。"""
        if self._mavros_launch_file is None:
            self.get_logger().warn(f"[restart_mavros] UAV{self._vid} no launch file configured")
            return False
        return MavrosManager.restart_mavros(self._vid, self._mavros_launch_file, self._mavros_log_dir)

    def reboot_px4_hard(self, position: Optional[List[float]] = None, yaw_deg: Optional[float] = None):
        """
        硬重启流程（统一入口）：
        1. 暂停 hover loop
        2. 停止 MAVROS
        3. 移动 UAV 到目标位置（在 PX4 恢复之前）
        4. 通过仿真端恢复 PX4（使用新的 /px4/recover 原子操作）
        5. 重启 MAVROS
        6. 等待 MAVROS 连接（通过 state topic 监控）
        7. 等待 FCU 连接和姿态稳定
        8. ARM 和 OFFBOARD
        9. 起飞
        10. 恢复 hover loop（更新目标位置）
        """
        vid = int(self._vid)
        log_prefix = f"[reboot_hard uav{vid}]"
        if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
            raise InterruptedError("reset cancelled")

        ts_log(log_prefix, "Starting hard reboot...")
        ts_log(log_prefix, "=" * 50)

        # 1. 暂停 hover loop
        ts_log(log_prefix, "Step 1: Pausing hover loop...")
        self.pause_hover_loop()

        try:
            # 2. 停止 MAVROS
            ts_log(log_prefix, "Step 2: Stopping MAVROS...")
            MavrosManager.stop_mavros(vid)
            self._mavros_connected = False
            self._mavros_connected_event.clear()

            # 3. 移动 UAV 到目标位置（在 PX4 恢复之前）
            if position is not None and isinstance(position, (list, tuple)) and len(position) >= 3:
                # 关键修复：硬重启时必须重置姿态为水平，否则可能保留崩溃时的翻转姿态
                # 如果 yaw_deg 为 None，使用默认值 0 确保无人机水平
                reset_yaw = yaw_deg if yaw_deg is not None else 0.0
                ts_log(log_prefix, f"Step 3: Moving UAV to ground position [{position[0]}, {position[1]}, 0.07] yaw={reset_yaw}")
                self._sim_move_uav([position[0], position[1], 0.07], reset_yaw)
                time.sleep(0.5)
            else:
                ts_log(log_prefix, "Step 3: Skipped (no position specified)")

            # 4. 通过仿真端恢复 PX4（原子操作）
            ts_log(log_prefix, "Step 4: Recovering PX4 via simulation endpoint...")
            ok_recover = self._sim_px4_hard_reset()
            if not ok_recover:
                ts_log(log_prefix, "PX4 recovery failed", "WARN")

            # 4.5 等待 PX4 后端状态就绪（确保仿真端和 PX4 之间的通信正常）
            ts_log(log_prefix, "Step 4.5: Waiting for PX4 backend to be ready...")
            px4_ready_timeout = 30.0
            px4_ready_start = time.time()
            px4_backend_ok = False
            while time.time() - px4_ready_start < px4_ready_timeout:
                if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                    raise InterruptedError("reset cancelled")
                try:
                    import urllib.request, json as _json
                    url = f"{self._image_http_base}/uav/{self._vid}/px4/status"
                    req = urllib.request.Request(url, method="GET")
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        data = _json.loads(resp.read().decode("utf-8"))
                        backend = data.get("px4_backend", {})
                        # 检查关键状态：received_first_actuator 表示仿真端收到了 PX4 的数据
                        if backend.get("received_first_actuator") and backend.get("received_first_heartbeat"):
                            ts_log(log_prefix, f"PX4 backend ready: actuator={backend.get('received_first_actuator')}, heartbeat={backend.get('received_first_heartbeat')}")
                            px4_backend_ok = True
                            break
                        else:
                            ts_log(log_prefix, f"Waiting for PX4 backend... actuator={backend.get('received_first_actuator')}, heartbeat={backend.get('received_first_heartbeat')}")
                except Exception as e:
                    ts_log(log_prefix, f"PX4 status check error: {e}", "WARN")
                time.sleep(1.0)
            if not px4_backend_ok:
                ts_log(log_prefix, "PX4 backend not ready after timeout, continuing anyway...", "WARN")

            # 4.6 等待 PX4 完成预检（ready_to_takeoff）
            # 这是确保 PX4 EKF 收敛并通过所有预检的关键步骤
            ts_log(log_prefix, "Step 4.6: Waiting for PX4 ready_to_takeoff...")
            px4_takeoff_timeout = 45.0  # EKF 收敛可能需要更长时间
            px4_takeoff_start = time.time()
            px4_takeoff_ok = False
            while time.time() - px4_takeoff_start < px4_takeoff_timeout:
                if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                    raise InterruptedError("reset cancelled")
                try:
                    import urllib.request, json as _json
                    url = f"{self._image_http_base}/uav/{self._vid}/px4/status"
                    req = urllib.request.Request(url, method="GET")
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        data = _json.loads(resp.read().decode("utf-8"))
                        backend = data.get("px4_backend", {})
                        ready = backend.get("px4_ready_to_takeoff", False)
                        if ready:
                            ts_log(log_prefix, f"PX4 ready_to_takeoff: True")
                            px4_takeoff_ok = True
                            break
                        else:
                            elapsed = time.time() - px4_takeoff_start
                            ts_log(log_prefix, f"Waiting for px4_ready_to_takeoff... ({elapsed:.1f}s)")
                except Exception as e:
                    ts_log(log_prefix, f"PX4 status check error: {e}", "WARN")
                time.sleep(1.0)
            if not px4_takeoff_ok:
                ts_log(log_prefix, "PX4 ready_to_takeoff not achieved after timeout, continuing anyway...", "WARN")

            time.sleep(2.0)

            # 5. 重启 MAVROS
            ts_log(log_prefix, "Step 5: Restarting MAVROS...")
            if self._mavros_launch_file:
                ok = MavrosManager.start_mavros(vid, self._mavros_launch_file, self._mavros_log_dir)
                if not ok:
                    ts_log(log_prefix, "MAVROS restart failed", "WARN")
            else:
                ts_log(log_prefix, "No MAVROS launch file configured, skipping MAVROS restart", "WARN")

            # 6. 等待 MAVROS 连接（通过 state topic 监控）
            ts_log(log_prefix, "Step 6: Waiting for MAVROS connection...")
            mavros_ok = self.wait_for_mavros_connected(timeout=60.0)
            if not mavros_ok:
                raise TimeoutError(f"MAVROS connection timeout for UAV{vid}")

            # 7. 等待 FCU 连接和姿态稳定
            ts_log(log_prefix, "Step 7: Waiting for FCU connection...")
            self.wait_for_fcu_connection()

            ok_att = self.wait_until_attitude_stable(
                timeout_s=POSITION_READY_TIMEOUT_S,
                window_s=POSITION_READY_WINDOW_S,
                eps=POSITION_READY_EPS,
            )
            ts_log(log_prefix, f"Attitude stable: {ok_att}")

            # 8. ARM 和 OFFBOARD
            ts_log(log_prefix, "Step 8: Arming and enabling OFFBOARD mode...")
            self.arming_client.wait_for_service(timeout_sec=20.0)
            self.arm_cmd.value = True
            arm_start = time.time()
            while not self.current_state.armed and (time.time() - arm_start) < 60.0:
                try:
                    if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                        raise InterruptedError("reset cancelled")
                    resp = call_service_sync(self, self.arming_client, self.arm_cmd, timeout_sec=ARMING_TIMEOUT_S)
                    if resp and getattr(resp, "success", False):
                        ts_log(log_prefix, "***** Vehicle armed *****")
                        break
                except Exception as e:
                    ts_log(log_prefix, f"Arming failed: {e}", "WARN")
                spin_sleep(self, 2.0)

            # Send warmup position commands
            target_x = float(position[0]) if position else 0.0
            target_y = float(position[1]) if position else 0.0
            target_z = float(position[2]) if position else self.init_height
            warmup_count = 60
            for _ in range(warmup_count):
                self.pub_position(target_x, target_y, 0.07)
                spin_sleep(self, 20.0)

            # Send OFFBOARD mode command
            self.set_mode_client.wait_for_service(timeout_sec=SETMODE_TIMEOUT_S)
            self.offb_set_mode.custom_mode = "OFFBOARD"
            off_start = time.time()
            while self.current_state.mode != "OFFBOARD" and (time.time() - off_start) < 60.0:
                try:
                    if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                        raise InterruptedError("reset cancelled")
                    resp = call_service_sync(self, self.set_mode_client, self.offb_set_mode, timeout_sec=SETMODE_TIMEOUT_S)
                    if resp and getattr(resp, "mode_sent", False):
                        ts_log(log_prefix, "***** OFFBOARD enabled *****")
                except Exception as e:
                    ts_log(log_prefix, f"OFFBOARD enable failed: {e}", "WARN")
                spin_sleep(self, 2.0)
            if self.current_state.mode != "OFFBOARD":
                raise TimeoutError("OFFBOARD not enabled within 60s")

            # 9. 起飞
            ts_log(log_prefix, f"Step 9: Taking off to [{target_x}, {target_y}, {target_z}]...")
            self._perform_takeoff(target_x, target_y, target_z, log_prefix)
            ts_log(log_prefix, f"Moved to target={position}")

            ts_log(log_prefix, "=" * 50)
            ts_log(log_prefix, "Hard reboot completed successfully")

        finally:
            # 10. 恢复 hover loop（更新目标位置）
            final_target = None
            if position is not None and isinstance(position, (list, tuple)) and len(position) >= 3:
                final_target = (float(position[0]), float(position[1]), float(position[2]))
            ts_log(log_prefix, f"Step 10: Resuming hover loop with target={final_target}")
            self.resume_hover_loop(new_target=final_target)

    def reboot_px4(self, position: Optional[List[float]] = None, yaw_deg: Optional[float] = None):
        """
        软重启流程（不重启 PX4 和 MAVROS，仅移动位置并重新起飞）：
        1. 暂停 hover loop
        2. 通过仿真端移动 UAV 到目标位置
        3. 等待姿态稳定
        4. 重新 ARM 和 OFFBOARD（如果需要）
        5. 起飞
        6. 恢复 hover loop（更新目标位置）
        """
        vid = int(self._vid)
        log_prefix = f"[reboot_soft uav{vid}]"
        if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
            raise InterruptedError("reset cancelled")

        ts_log(log_prefix, "Starting soft reboot...")
        ts_log(log_prefix, "=" * 50)

        # 1. 暂停 hover loop
        ts_log(log_prefix, "Step 1: Pausing hover loop...")
        self.pause_hover_loop()

        try:
            # 2. 通过仿真端移动 UAV 到目标位置
            if position is not None and isinstance(position, (list, tuple)) and len(position) >= 3:
                ts_log(log_prefix, f"Step 2: Moving UAV to position [{position[0]}, {position[1]}, 0.07] yaw={yaw_deg}")
                self._sim_move_uav([position[0], position[1], 0.07], yaw_deg)
            else:
                ts_log(log_prefix, "Step 2: Skipped (no position specified)")

            time.sleep(1.0)

            # 3. 等待姿态稳定
            ts_log(log_prefix, "Step 3: Waiting for attitude to stabilize...")
            ok_att = self.wait_until_attitude_stable(
                timeout_s=POSITION_READY_TIMEOUT_S,
                window_s=POSITION_READY_WINDOW_S,
                eps=POSITION_READY_EPS,
            )
            ts_log(log_prefix, f"Attitude stable: {ok_att}")

            # 4. 如果未 armed，重新 ARM
            if not self.current_state.armed:
                ts_log(log_prefix, "Step 4: Re-arming vehicle...")
                self.arming_client.wait_for_service(timeout_sec=10.0)
                self.arm_cmd.value = True
                arm_start = time.time()
                while not self.current_state.armed and (time.time() - arm_start) < 30.0:
                    try:
                        if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                            raise InterruptedError("reset cancelled")
                        resp = call_service_sync(self, self.arming_client, self.arm_cmd, timeout_sec=ARMING_TIMEOUT_S)
                        if resp and getattr(resp, "success", False):
                            ts_log(log_prefix, "***** Vehicle armed *****")
                            break
                    except Exception as e:
                        ts_log(log_prefix, f"Arming failed: {e}", "WARN")
                    spin_sleep(self, 1.0)
            else:
                ts_log(log_prefix, "Step 4: Vehicle already armed, skipping")

            # Send warmup position commands
            target_x = float(position[0]) if position else 0.0
            target_y = float(position[1]) if position else 0.0
            target_z = float(position[2]) if position else self.init_height

            # 如果不在 OFFBOARD 模式，切换到 OFFBOARD
            if self.current_state.mode != "OFFBOARD":
                ts_log(log_prefix, "Step 4b: Enabling OFFBOARD mode...")
                warmup_count = 60
                for _ in range(warmup_count):
                    self.pub_position(target_x, target_y, 0.07)
                    spin_sleep(self, 20.0)

                self.set_mode_client.wait_for_service(timeout_sec=SETMODE_TIMEOUT_S)
                self.offb_set_mode.custom_mode = "OFFBOARD"
                off_start = time.time()
                while self.current_state.mode != "OFFBOARD" and (time.time() - off_start) < 30.0:
                    try:
                        if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                            raise InterruptedError("reset cancelled")
                        resp = call_service_sync(self, self.set_mode_client, self.offb_set_mode, timeout_sec=SETMODE_TIMEOUT_S)
                        if resp and getattr(resp, "mode_sent", False):
                            ts_log(log_prefix, "***** OFFBOARD enabled *****")
                    except Exception as e:
                        ts_log(log_prefix, f"OFFBOARD enable failed: {e}", "WARN")
                    spin_sleep(self, 1.0)
            else:
                ts_log(log_prefix, "Step 4b: Already in OFFBOARD mode, skipping")

            # 5. 起飞
            ts_log(log_prefix, f"Step 5: Taking off to [{target_x}, {target_y}, {target_z}]...")
            self._perform_takeoff(target_x, target_y, target_z, log_prefix)
            ts_log(log_prefix, f"Moved to target={position}")

            ts_log(log_prefix, "=" * 50)
            ts_log(log_prefix, "Soft reboot completed successfully")

        finally:
            # 6. 恢复 hover loop（更新目标位置）
            final_target = None
            if position is not None and isinstance(position, (list, tuple)) and len(position) >= 3:
                final_target = (float(position[0]), float(position[1]), float(position[2]))
            ts_log(log_prefix, f"Step 6: Resuming hover loop with target={final_target}")
            self.resume_hover_loop(new_target=final_target)

    def wait_for_fcu_connection(self):
        """在 MAVROS 启动后阻塞等待 FCU 连接成功。"""
        try:
            self.get_logger().info("Waiting for FCU connection...")
        except Exception as e:
            self.get_logger().warn(f"[INFO] HTTP /command resp log failed: {e}")
        while rclpy.ok() and not getattr(self.current_state, "connected", False):
            spin_sleep(self, 20.0)
        try:
            self.get_logger().info(f"UAV{self._vid} FCU connected.")
        except Exception as e:
            self.get_logger().warn(f"[INFO] FCU connected log failed: {e}")

    def pub_velocity(self, velocity_x, velocity_y, velocity_z, yaw_rate):
        # 与原始一致：速度控制 + FRAME_BODY_NED，通过桥接主题发送
        target_velocity = PositionTarget()
        target_velocity.header.stamp = self.get_clock().now().to_msg()
        target_velocity.coordinate_frame = PositionTarget.FRAME_BODY_NED
        target_velocity.type_mask = (
            PositionTarget.IGNORE_PX | PositionTarget.IGNORE_PY | PositionTarget.IGNORE_PZ |
            PositionTarget.IGNORE_AFX | PositionTarget.IGNORE_AFY | PositionTarget.IGNORE_AFZ |
            PositionTarget.IGNORE_YAW
        )
        target_velocity.velocity.x = float(velocity_x)
        target_velocity.velocity.y = float(velocity_y)
        target_velocity.velocity.z = float(velocity_z)
        target_velocity.yaw_rate   = float(yaw_rate)
        self.raw_setpoint_pub.publish(target_velocity)
        
    def pub_position(self, target_x, target_y, target_z):
        tx = float(target_x)
        ty = float(target_y)
        tz = float(target_z)

        # -----------------------------
        # 正常 MAVROS 发布 setpoint
        # -----------------------------
        target_position = PositionTarget()
        target_position.header.stamp = self.get_clock().now().to_msg()
        target_position.coordinate_frame = PositionTarget.FRAME_LOCAL_NED
        
        # # 限制 Pitch 和 Roll
        # # 通过全局变量动态设置 MPC_TILTMAX_AIR 参数来限制最大倾角
        # try:
        #     # 取两者中较小的一个作为整体倾角限制（PX4 MPC_TILTMAX_AIR 是总倾角限制）
        #     limit_deg = min(float(MAX_PITCH_DEG), float(MAX_ROLL_DEG))
        #     limit_deg = max(0.0, min(limit_deg, 89.0)) # Clamp 0-89
            
        #     # 只有当参数发生变化时才调用服务（简单缓存机制）
        #     if not hasattr(self, "_last_tilt_max_deg") or abs(self._last_tilt_max_deg - limit_deg) > 1.0:
        #          # 使用异步调用避免阻塞控制循环太久
        #         self.set_px4_params({"MPC_TILTMAX_AIR": float(limit_deg)}, timeout_sec=1.0)
        #         self._last_tilt_max_deg = limit_deg
        # except Exception as e:
        #     self.get_logger().warn(f"Failed to update tilt limit: {e}")
        
        target_position.type_mask = (
            PositionTarget.IGNORE_VX | PositionTarget.IGNORE_VY | PositionTarget.IGNORE_VZ |
            PositionTarget.IGNORE_AFX | PositionTarget.IGNORE_AFY | PositionTarget.IGNORE_AFZ |
            PositionTarget.IGNORE_YAW | PositionTarget.IGNORE_YAW_RATE
        )
        target_position.position.x = tx
        target_position.position.y = ty
        target_position.position.z = tz

        self.raw_setpoint_pub.publish(target_position)

    def pub_position_velocity(self, x: float, y: float, z: float,
                               vx: float, vy: float, vz: float,
                               yaw: float, yaw_rate: float):
        """发送带速度前馈的设定点 (OFFBOARD模式)

        参数坐标系: ENU (与Isaac Sim一致)
        - 位置: (x, y, z) - East, North, Up
        - 速度: (vx, vy, vz) - m/s
        - yaw: 航向角 (弧度), ENU坐标系 (East=0, CCW正)
        - yaw_rate: 航向角速率 (rad/s)

        MAVROS会自动将FRAME_LOCAL_NED转换为PX4的NED坐标系，
        但这里输入为ENU坐标，需要手动转换为NED。
        转换: NED.x = ENU.y, NED.y = ENU.x, NED.z = -ENU.z
        Yaw转换: NED_yaw = π/2 - ENU_yaw
        """
        import math

        # 坐标系转换: ENU -> NED
        ned_x = float(y)       # ENU.y -> NED.x (North)
        ned_y = float(x)       # ENU.x -> NED.y (East)
        ned_z = -float(z)      # ENU.z -> NED.z (Down)

        ned_vx = float(vy)     # ENU.vy -> NED.vx
        ned_vy = float(vx)     # ENU.vx -> NED.vy
        ned_vz = -float(vz)    # ENU.vz -> NED.vz

        # Yaw坐标系转换: ENU -> NED
        # ENU: East=0 (X轴), CCW正 (逆时针)
        # NED: North=0 (X轴), CW正 (顺时针)
        # 转换公式: NED_yaw = π/2 - ENU_yaw
        ned_yaw = 0.5 * math.pi - float(yaw)

        # Yaw_rate方向取反: ENU CCW正 -> NED CW正
        ned_yaw_rate = -float(yaw_rate)

        # 构建 PositionTarget 消息
        target = PositionTarget()
        target.header.stamp = self.get_clock().now().to_msg()
        target.coordinate_frame = PositionTarget.FRAME_LOCAL_NED

        # Type Mask: 使用位置、速度、yaw、yaw_rate，忽略加速度
        # 位0-2: 忽略位置 -> 0 (使用位置)
        # 位3-5: 忽略速度 -> 0 (使用速度)
        # 位6-8: 忽略加速度 -> 111 (忽略)
        # 位9: 忽略力 -> 1 (忽略)
        # 位10: 忽略yaw -> 0 (使用yaw)
        # 位11: 忽略yaw_rate -> 0 (使用yaw_rate)
        # 0b0000_0011_1111_1000 = 0x03F8 = 1016
        # 但 PositionTarget 使用 IGNORE 标志，设置为1表示忽略
        target.type_mask = (
            PositionTarget.IGNORE_AFX |
            PositionTarget.IGNORE_AFY |
            PositionTarget.IGNORE_AFZ
        )  # 只忽略加速度，使用位置、速度、yaw、yaw_rate

        target.position.x = ned_x
        target.position.y = ned_y
        target.position.z = ned_z

        target.velocity.x = ned_vx
        target.velocity.y = ned_vy
        target.velocity.z = ned_vz

        target.yaw = ned_yaw
        target.yaw_rate = ned_yaw_rate

        self.raw_setpoint_pub.publish(target)

    # ---------- 轨迹（可选：如果你要跑外部 JSON 轨迹） ----------
    def load_trajectory(self, file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            self.waypoints = self.convert_coordinates(data['raw_logs'], data['preprocessed_logs'])
            self.get_logger().info(f"Loaded {len(self.waypoints)} waypoints.")
        except Exception as e:
            self.get_logger().error(f"Failed to load trajectory file: {e}")

    def convert_coordinates(self, raw_logs, preprocessed_logs):
        waypoints = []
        # log all waypoints only in preprocessed
        for i, preprocessed in enumerate(preprocessed_logs):
            self.get_logger().info(f"Waypoint {i}: {preprocessed}")
        # self.get_logger().info(f"Converting {len(raw_logs)} waypoints from ENU to NED...")
        
        for raw, preprocessed in zip(raw_logs, preprocessed_logs):
            x_local, y_local, z_local, roll_local, yaw_local, pitch_local = preprocessed
            # ENU->NED + cm->m（保持你给的转换）
            x_ned =  y_local / 100.0
            y_ned =  x_local / 100.0
            z_ned =  -z_local / 100.0
            waypoints.append([x_ned, y_ned, z_ned])
        # add takeoff height to z
        waypoints = [[x, y, z + self.init_height] for x, y, z in waypoints]
        return waypoints

    def run_trajectory(self):
        for x, y, z in self.waypoints:
            z = z + self.start_height
            self.pub_position(x, y, z)
            self.get_logger().info(f"Go to waypoint: ({x:.2f}, {y:.2f}, {z:.2f})")
            time.sleep(1.0)  # 同原始风格的简单等待

    # Interfaces
    def get_position(self) -> Dict[str, float]:
        p = self.current_pose.pose.position
        return {"x": p.x, "y": p.y, "z": p.z}

    def get_status(self) -> Dict[str, Any]:
        st = self.current_state
        return {"connected": getattr(st, "connected", False), "armed": getattr(st, "armed", False), "mode": getattr(st, "mode", "")}

    def move_to_many_wait(self, points: List[List[float]], threshold: float = MOVE_REACH_THRESHOLD, timeout: float = 60.0):
        for pt in points:
            if not isinstance(pt, (list, tuple)) or len(pt) < 3:
                continue
            x, y, z = float(pt[0]), float(pt[1]), float(pt[2])
            self.move_to_and_wait(x, y, z, threshold=threshold, timeout_s=timeout)
            self.hover_target = (x, y, z)

    def _select_color_topic(self) -> Optional[str]:
        try:
            topics = self.get_topic_names_and_types()
        except Exception:
            topics = []
        ns_prefix = self._mavros_prefix or ""
        candidates = []
        for name, _types in topics:
            if name.endswith("/color/image_raw"):
                candidates.append(name)
        for name in candidates:
            if ns_prefix and name.startswith(ns_prefix + "/"):
                return name
        return candidates[0] if candidates else None

    def _color_image_cb(self, msg: Image):
        self._last_color_msg = msg

    def ensure_camera_subscription(self) -> bool:
        if self._color_sub is not None and self._color_topic:
            return True
        topic = self._select_color_topic()
        if not topic:
            return False
        qos = QoSProfile(reliability=ReliabilityPolicy.BEST_EFFORT, history=HistoryPolicy.KEEP_LAST, depth=10)
        try:
            self._color_sub = self.create_subscription(Image, topic, self._color_image_cb, qos)
            self._color_topic = topic
            return True
        except Exception as e:
            self.get_logger().warn(f"[WARN] ensure_camera_subscription failed: {e}")
            return False

    def _encode_image_msg_to_b64(self, msg: Image) -> Optional[str]:
        if msg is None:
            return None
        if PILImage is None:
            return None
        w = int(getattr(msg, "width", 0) or 0)
        h = int(getattr(msg, "height", 0) or 0)
        enc = str(getattr(msg, "encoding", "")).lower()
        data = bytes(getattr(msg, "data", b""))
        if w <= 0 or h <= 0 or not data:
            return None
        try:
            arr = np.frombuffer(data, dtype=np.uint8)
            if enc in ("rgb8", "bgr8", "rgba8", "bgra8"):
                c = 3 if enc.endswith("rgb8") or enc.endswith("bgr8") else 4
                arr = arr.reshape((h, w, c))
                if enc.startswith("bgr") or enc.startswith("bgra"):
                    arr = arr[:, :, :3][:, :, ::-1]
                else:
                    arr = arr[:, :, :3]
            elif enc in ("mono8", "8UC1"):
                arr = arr.reshape((h, w))
            else:
                step = int(getattr(msg, "step", w * 3))
                c = step // max(w, 1)
                try:
                    arr = arr.reshape((h, w, c))
                    arr = arr[:, :, :3]
                except Exception:
                    return None
            pil_img = PILImage.fromarray(arr)
            buf = BytesIO()
            pil_img.save(buf, format='PNG')
            return base64.b64encode(buf.getvalue()).decode('ascii')
        except Exception:
            return None

    def _capture_image_b64(self) -> Optional[str]:
        return self._image_service.capture_b64(self)

    def land(self):
        vid = int(self._vid)
        target_z = getattr(self, "start_height", 0.0)
        self.get_logger().info(f"UAV{vid} initiating landing via MAVROS CommandLong MAV_CMD_NAV_LAND (21). target_z={target_z}")
        self.get_logger().info(f"UAV{vid} land() context before pause: mode={getattr(self.current_state,'mode','')} armed={bool(getattr(self.current_state,'armed',False))} z={float(getattr(getattr(self,'current_pose',PoseStamped()),'pose',PoseStamped().pose).position.z or 0.0):.3f} ls={int(getattr(self.extended_state,'landed_state',0))}")
        old_hover = self.hover_target
        self._landing_in_progress = True
        try:
            self.hover_target = None

            sent_cmd = False
            try:
                if self.cmdlong_client.wait_for_service(timeout_sec=5.0):
                    req = CommandLong.Request()
                    req.command = 21
                    req.confirmation = 0
                    req.param1 = float('nan')
                    req.param2 = float('nan')
                    req.param3 = float('nan')
                    req.param4 = float('nan')
                    req.param5 = float('nan')
                    req.param6 = float('nan')
                    req.param7 = float('nan')
                    self.get_logger().info(f"UAV{vid} calling cmd/command MAV_CMD_NAV_LAND")
                    resp = call_service_sync(self, self.cmdlong_client, req, timeout_sec=5.0)
                    self.get_logger().info(f"UAV{vid} NAV_LAND resp: success={getattr(resp,'success',None)} result={getattr(resp,'result',None)}")
                    sent_cmd = True
                else:
                    self.get_logger().warn(f"UAV{vid} cmd/command service not ready; skip CommandLong")
            except Exception as e:
                self.get_logger().warn(f"UAV{vid} CommandLong NAV_LAND failed: {e}")

            if not sent_cmd:
                try:
                    land_mode = SetMode.Request()
                    land_mode.custom_mode = "AUTO.LAND"
                    self.get_logger().info(f"UAV{vid} fallback: set_mode AUTO.LAND")
                    resp2 = call_service_sync(self, self.set_mode_client, land_mode, timeout_sec=SETMODE_TIMEOUT_S)
                    self.get_logger().info(f"UAV{vid} fallback set_mode AUTO.LAND resp: {resp2}")
                except Exception as e:
                    self.get_logger().warn(f"UAV{vid} fallback set_mode AUTO.LAND failed: {e}")

            start = time.time()
            window_s = LAND_STABLE_WINDOW_S
            eps = LAND_STABLE_EPS
            stable_for = 0.0
            last = None
            step = 0.2
            timeout_s = LAND_TIMEOUT_S

            initial_z = float(self.current_pose.pose.position.z)
            total_desc = max(0.0, initial_z - float(target_z))
            progress_bar = tqdm(total=total_desc if total_desc > 0 else 1.0, desc="降落进度", unit="m")
            progress_counter = 0

            landed = False
            while time.time() - start < timeout_s and rclpy.ok():
                if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                    raise InterruptedError("reset cancelled")
                z = float(self.current_pose.pose.position.z)
                try:
                    on_ground = int(getattr(self.extended_state, "landed_state", 0)) == int(ExtendedState.LANDED_STATE_ON_GROUND)
                except Exception:
                    on_ground = False
                near_target = abs(z - target_z) <= 0.05
                if near_target or on_ground:
                    if last is not None and abs(z - last) <= eps:
                        stable_for += step
                    else:
                        stable_for = 0.0
                else:
                    stable_for = 0.0
                last = z
                mode = getattr(self.current_state, "mode", "")
                armed = bool(getattr(self.current_state, "armed", False))
                ach = max(0.0, initial_z - z)
                if ach > progress_bar.total:
                    ach = progress_bar.total
                progress_counter += 1
                if progress_counter >= 20:
                    progress_counter = 0
                    progress_bar.n = ach
                    progress_bar.last_print_n = ach
                    progress_bar.set_postfix({
                        "z": f"{z:.2f}",
                        "ground": on_ground,
                        "near": near_target,
                        "stable": f"{stable_for:.1f}s",
                        "mode": mode,
                    })
                    progress_bar.update(0)
                if stable_for >= window_s:
                    landed = True
                    break
                rclpy.spin_once(self, timeout_sec=0.05)
                time.sleep(step)

            if landed:
                progress_bar.n = progress_bar.total
                progress_bar.last_print_n = progress_bar.total
                progress_bar.update(0)
                progress_bar.close()
                self.get_logger().info(f"UAV{vid} landed. disarming...")
                try:
                    disarm_req = CommandBool.Request()
                    disarm_req.value = False
                    resp3 = call_service_sync(self, self.arming_client, disarm_req, timeout_sec=ARMING_TIMEOUT_S)
                    self.get_logger().info(f"UAV{vid} disarm resp: {resp3}")
                except Exception as e:
                    mode = getattr(self.current_state, "mode", "")
                    armed = bool(getattr(self.current_state, "armed", False))
                    z = float(self.current_pose.pose.position.z)
                    ls = int(getattr(self.extended_state, "landed_state", 0))
                    srv = getattr(self.arming_client, "srv_name", "<unknown>")
                    ready = bool(self.arming_client.service_is_ready())
                    self.get_logger().error(f"UAV{vid} disarm failed: {e}; srv={srv} ready={ready} mode={mode} armed={armed} z={z:.3f} landed_state={ls}")
                self.wait_until_disarmed(timeout_s=DISARM_TIMEOUT_S)
            else:
                progress_bar.close()
                z = float(self.current_pose.pose.position.z)
                dist = abs(z - target_z)
                ls = int(getattr(self.extended_state, "landed_state", 0))
                mode = getattr(self.current_state, "mode", "")
                armed = bool(getattr(self.current_state, "armed", False))
                self.get_logger().warn(f"UAV{vid} landing timeout: z={z:.3f} target_z={target_z:.3f} dist={dist:.3f} stable_for={stable_for:.1f}s landed_state={ls} mode={mode} armed={armed}")
                self.get_logger().warn(f"UAV{vid} landing timeout context: _landing_in_progress={getattr(self,'_landing_in_progress',None)} hover_target={self.hover_target}")
        finally:
            self.hover_target = old_hover
            self._landing_in_progress = False
        self.get_logger().info(f"UAV{vid} land() exit: landed={landed} mode={getattr(self.current_state,'mode','')} armed={bool(getattr(self.current_state,'armed',False))} z={float(self.current_pose.pose.position.z):.3f} ls={int(getattr(self.extended_state,'landed_state',0))}")
        return landed
    
    def wait_until_landed(self, target_z: Optional[float] = None, timeout_s: float = 15.0, window_s: float = 2.0, eps: float = 0.03) -> bool:
        if target_z is None:
            target_z = getattr(self, "start_height", 0.0)
        start = time.time()
        last = None
        stable_for = 0.0
        step = 0.1
        while time.time() - start < timeout_s:
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            z = float(self.current_pose.pose.position.z)
            on_ground = False
            try:
                on_ground = int(getattr(self.extended_state, "landed_state", 0)) == int(ExtendedState.LANDED_STATE_ON_GROUND)
            except Exception:
                on_ground = False
            near_target = abs(z - target_z) <= 0.05
            if near_target or on_ground:
                if last is not None and abs(z - last) <= eps:
                    stable_for += step
                else:
                    stable_for = 0.0
            else:
                stable_for = 0.0
            last = z
            if stable_for >= window_s:
                return True
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(step)
        return False

    def wait_until_disarmed(self, timeout_s: float = 5.0) -> bool:
        start = time.time()
        while time.time() - start < timeout_s:
            if getattr(self, "_reset_cancel_event", None) and self._reset_cancel_event.is_set():
                raise InterruptedError("reset cancelled")
            if not bool(getattr(self.current_state, "armed", False)):
                return True
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.1)
        return False

    def process_command(self, cmd: Dict[str, Any]):
        ctype = cmd.get("cmd") or cmd.get("type")
        resp: Dict[str, Any] = {"ok": True}
        try:
            if ctype == "move_to":
                x = float(cmd.get("x"))
                y = float(cmd.get("y"))
                z = float(cmd.get("z"))
                self.move_to_and_wait(x, y, z)
                resp.update({"position": self.get_position()})
            elif ctype == "move_to_many":
                pts = cmd.get("points") or cmd.get("waypoints") or []
                for pt in pts:
                    if isinstance(pt, (list, tuple)) and len(pt) >= 3:
                        x, y, z = float(pt[0]), float(pt[1]), float(pt[2])
                        self.move_to_and_wait(x, y, z)
                resp.update({"position": self.get_position()})
            elif ctype == "arm_and_mission":
                # 完整的 MISSION 模式流程：arm -> push waypoints -> start mission
                # 用于跳过 OFFBOARD 模式的场景
                pts = cmd.get("points") or cmd.get("waypoints") or []
                timeout = float(cmd.get("timeout", 300.0))

                if not pts:
                    resp = {"ok": False, "error": "No waypoints provided"}
                else:
                    waypoints = []
                    for pt in pts:
                        if isinstance(pt, dict):
                            x = float(pt.get("x", 0.0))
                            y = float(pt.get("y", 0.0))
                            z = float(pt.get("z", 0.0))
                            waypoints.append((x, y, z))
                        elif isinstance(pt, (list, tuple)) and len(pt) >= 3:
                            waypoints.append((float(pt[0]), float(pt[1]), float(pt[2])))

                    if not waypoints:
                        resp = {"ok": False, "error": "No valid waypoints"}
                    else:
                        self.get_logger().info(f"[arm_and_mission] Starting with {len(waypoints)} waypoints")
                        success = True
                        error_msg = None

                        # Step 1: 先上传航点（按 ROS1 示例的顺序，先 push 再 arm）
                        self.get_logger().info("[arm_and_mission] Pushing waypoints first...")

                        # 清除旧航点
                        if not self.clear_mission():
                            self.get_logger().warn("[arm_and_mission] Clear mission failed, continuing...")

                        # 等待一段时间让mission service稳定
                        self.get_logger().info("[arm_and_mission] Waiting for mission service to stabilize...")
                        time.sleep(2.0)
                        rclpy.spin_once(self, timeout_sec=0.1)

                        # 尝试推送航点，最多重试3次
                        push_success = False
                        for push_attempt in range(3):
                            self.get_logger().info(f"[arm_and_mission] Push attempt {push_attempt + 1}/3...")
                            if self.push_mission(waypoints):
                                push_success = True
                                break
                            else:
                                self.get_logger().warn(f"[arm_and_mission] Push attempt {push_attempt + 1} failed, retrying...")
                                time.sleep(1.0)
                                rclpy.spin_once(self, timeout_sec=0.1)

                        if not push_success:
                            success = False
                            error_msg = "Failed to push waypoints after 3 attempts"

                        # Step 2: 发送 setpoint 流让 PX4 准备好
                        if success:
                            self.get_logger().info("[arm_and_mission] Publishing setpoints for PX4...")
                            first_wp = waypoints[0]
                            for _ in range(60):
                                self.pub_position(first_wp[0], first_wp[1], first_wp[2])
                                spin_sleep(self, 20.0)

                        # Step 3: arm（先尝试标准 CommandBool，失败后尝试 CommandLong 强制解锁）
                        if success and not self.current_state.armed:
                            self.get_logger().info("[arm_and_mission] Arming vehicle...")
                            arm_attempts = 0
                            arm_success = False
                            while not self.current_state.armed and arm_attempts < 30:
                                # 持续发送 setpoint
                                self.pub_position(first_wp[0], first_wp[1], first_wp[2])
                                rclpy.spin_once(self, timeout_sec=0.05)

                                # 先尝试标准 CommandBool arm
                                self.arm_cmd.value = True
                                try:
                                    arm_resp = call_service_sync(self, self.arming_client, self.arm_cmd, timeout_sec=2.0)
                                    arm_svc_success = getattr(arm_resp, 'success', False) if arm_resp else False
                                    self.get_logger().info(f"[arm_and_mission] CommandBool arm response: success={arm_svc_success}")
                                    if arm_svc_success:
                                        # 等待 state 更新
                                        for _ in range(20):
                                            rclpy.spin_once(self, timeout_sec=0.1)
                                            if self.current_state.armed:
                                                self.get_logger().info("[arm_and_mission] Vehicle armed via CommandBool!")
                                                arm_success = True
                                                break
                                        if arm_success:
                                            break
                                except Exception as e:
                                    self.get_logger().warn(f"[arm_and_mission] CommandBool arm failed: {e}")

                                # 如果 CommandBool 失败，尝试 CommandLong 强制解锁
                                if not self.current_state.armed:
                                    cmdlong_req = CommandLong.Request()
                                    cmdlong_req.broadcast = False
                                    cmdlong_req.command = 400  # MAV_CMD_COMPONENT_ARM_DISARM
                                    cmdlong_req.confirmation = 0
                                    cmdlong_req.param1 = 1.0  # 1=arm, 0=disarm
                                    cmdlong_req.param2 = 21196.0  # Force arm (safety bypass)
                                    cmdlong_req.param3 = 0.0
                                    cmdlong_req.param4 = 0.0
                                    cmdlong_req.param5 = 0.0
                                    cmdlong_req.param6 = 0.0
                                    cmdlong_req.param7 = 0.0

                                    try:
                                        cmd_resp = call_service_sync(self, self.cmdlong_client, cmdlong_req, timeout_sec=2.0)
                                        if cmd_resp:
                                            result = getattr(cmd_resp, 'result', -1)
                                            cmd_success = getattr(cmd_resp, 'success', False)
                                            self.get_logger().info(f"[arm_and_mission] CommandLong arm response: success={cmd_success}, result={result}")
                                            if cmd_success:
                                                for _ in range(20):
                                                    rclpy.spin_once(self, timeout_sec=0.1)
                                                    if self.current_state.armed:
                                                        self.get_logger().info("[arm_and_mission] Vehicle armed via CommandLong!")
                                                        arm_success = True
                                                        break
                                                if arm_success:
                                                    break
                                    except Exception as e:
                                        self.get_logger().warn(f"[arm_and_mission] CommandLong arm failed: {e}")

                                arm_attempts += 1
                                time.sleep(0.5)
                            if not self.current_state.armed:
                                success = False
                                error_msg = f"Failed to arm vehicle after {arm_attempts} attempts"

                        # Step 4: 切换到 AUTO.MISSION 模式
                        if success:
                            self.get_logger().info("[arm_and_mission] Starting mission...")
                            if not self.start_mission():
                                success = False
                                error_msg = "Failed to start mission mode"

                        # Step 5: 等待任务完成
                        if success:
                            last_wp = waypoints[-1]
                            start_time = time.time()
                            completion_threshold = MOVE_REACH_THRESHOLD
                            while time.time() - start_time < timeout:
                                cur = self.current_pose.pose.position
                                dist = math.sqrt((cur.x - last_wp[0])**2 + (cur.y - last_wp[1])**2 + (cur.z - last_wp[2])**2)
                                if dist < completion_threshold:
                                    self.get_logger().info(f"[arm_and_mission] Reached last waypoint (dist={dist:.2f}m)")
                                    break
                                rclpy.spin_once(self, timeout_sec=0.1)
                                time.sleep(0.5)
                            else:
                                self.get_logger().warn(f"[arm_and_mission] Mission timeout after {timeout}s")
                                success = False
                                error_msg = "Mission timeout"

                        resp.update({
                            "success": success,
                            "error": error_msg,
                            "mode": "MISSION",
                            "waypoints_count": len(waypoints),
                            "position": self.get_position()
                        })

            elif ctype == "execute_mission":
                # 使用 MAVROS mission 模式执行航点任务
                pts = cmd.get("points") or cmd.get("waypoints") or []
                timeout = float(cmd.get("timeout", 300.0))
                use_mission_mode = bool(cmd.get("use_mission_mode", True))

                if not pts:
                    resp = {"ok": False, "error": "No waypoints provided"}
                else:
                    waypoints = []
                    for pt in pts:
                        if isinstance(pt, dict):
                            # 字典格式: {"x": ..., "y": ..., "z": ...}
                            x = float(pt.get("x", 0.0))
                            y = float(pt.get("y", 0.0))
                            z = float(pt.get("z", 0.0))
                            waypoints.append((x, y, z))
                        elif isinstance(pt, (list, tuple)) and len(pt) >= 3:
                            # 列表/元组格式: [x, y, z] 或 (x, y, z)
                            waypoints.append((float(pt[0]), float(pt[1]), float(pt[2])))

                    if use_mission_mode and _MISSION_AVAILABLE:
                        # 使用 AUTO.MISSION 模式
                        self.get_logger().info(f"[execute_mission] Using MISSION mode with {len(waypoints)} waypoints")
                        success = self.execute_mission(waypoints, timeout_s=timeout)
                        # 任务完成后切换回 OFFBOARD 模式以便后续控制
                        self.switch_to_offboard()
                    else:
                        # 回退到 OFFBOARD 逐点移动模式
                        self.get_logger().info(f"[execute_mission] Using OFFBOARD mode with {len(waypoints)} waypoints")
                        success = True
                        for wp in waypoints:
                            if not self.move_to_and_wait(wp[0], wp[1], wp[2], timeout_s=timeout / len(waypoints)):
                                success = False
                                break

                    resp.update({
                        "success": success,
                        "mode": "MISSION" if (use_mission_mode and _MISSION_AVAILABLE) else "OFFBOARD",
                        "waypoints_count": len(waypoints),
                        "position": self.get_position()
                    })
            elif ctype == "land":
                self.land()
                resp.update({"status": self.get_status()})
            elif ctype == "get_position":
                resp.update({"position": self.get_position()})
            elif ctype == "get_status":
                resp.update({"status": self.get_status()})
            elif ctype == "set_px4_ready":
                # 接收来自 trajectory_data_collector 的 PX4 ready 状态通知
                ready = bool(cmd.get("ready", False))
                self._px4_ready_from_collector = ready
                self.get_logger().info(f"[set_px4_ready] UAV{self._vid} received px4_ready={ready}")
                resp.update({"message": f"px4_ready set to {ready}"})
            elif ctype == "get_px4_ready":
                # 查询 PX4 ready 状态（从仿真端获取）
                ready = self._get_sim_px4_ready()
                resp.update({"px4_ready": ready})
            elif ctype == "get_mission_status":
                # 查询当前任务状态
                mode = self.current_state.mode if self.current_state else "UNKNOWN"
                armed = self.current_state.armed if self.current_state else False
                position = self.get_position()
                # 判断任务状态
                if mode == "AUTO.MISSION":
                    mission_state = "executing"
                elif mode == "AUTO.LAND" or mode == "AUTO.RTL":
                    mission_state = "landing"
                elif mode == "OFFBOARD":
                    mission_state = "offboard"
                else:
                    mission_state = "idle"
                resp.update({
                    "mode": mode,
                    "armed": armed,
                    "position": position,
                    "mission_state": mission_state,
                    "current_waypoint": getattr(self, "_current_mission_waypoint", -1),
                })
            elif ctype == "setpoint":
                # 速度前馈setpoint命令：位置+速度+yaw+yaw_rate
                # 参数坐标系为ENU（与mavlink_sim_vehicle.py一致）
                x = float(cmd.get("x", 0))
                y = float(cmd.get("y", 0))
                z = float(cmd.get("z", 0))
                vx = float(cmd.get("vx", 0))
                vy = float(cmd.get("vy", 0))
                vz = float(cmd.get("vz", 0))
                yaw = float(cmd.get("yaw", 0))
                yaw_rate = float(cmd.get("yaw_rate", 0))
                self.pub_position_velocity(x, y, z, vx, vy, vz, yaw, yaw_rate)
                resp.update({"status": "success"})
            elif ctype == "shutdown":
                self._shutdown_requested = True
                resp.update({"message": "Shutdown requested"})
            else:
                resp = {"ok": False, "error": f"Unknown cmd: {ctype}"}
        except Exception as e:
            resp = {"ok": False, "error": str(e)}
            try:
                self.get_logger().error(f"HTTP command '{ctype}' failed: {e}")
            except Exception as le:
                self.get_logger().warn(f"[ERROR] HTTP command error log failed: {le}")
        return resp

    def _get_sim_px4_ready(self) -> bool:
        """从仿真端查询 PX4 ready to takeoff 状态"""
        vid = int(self._vid)
        try:
            import urllib.request, json as _json
            url = f"{self._image_http_base}/uav/{vid}/px4/ready"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                raw = resp.read()
                obj = _json.loads(raw.decode("utf-8"))
                return bool(obj.get("ready", False))
        except Exception as e:
            self.get_logger().warn(f"[_get_sim_px4_ready] UAV{vid} request failed: {e}")
            return False

class TaskGuard:
    def __init__(self, lock: threading.Lock, force: bool, cancel_event: Optional[threading.Event] = None, timeout_s: float = 10.0):
        self._lock = lock
        self.acquired = False
        if force:
            if self._lock.acquire(blocking=False):
                self.acquired = True
                if cancel_event:
                    cancel_event.clear()
            else:
                if cancel_event:
                    cancel_event.set()
                start = time.time()
                while True:
                    if self._lock.acquire(blocking=False):
                        self.acquired = True
                        break
                    if (time.time() - start) > float(timeout_s):
                        break
                    time.sleep(0.05)
                if self.acquired and cancel_event:
                    cancel_event.clear()
        else:
            self.acquired = self._lock.acquire(blocking=False)
    def release(self):
        if self.acquired:
            try:
                self._lock.release()
            except Exception:
                import traceback as _tb
                print(_tb.format_exc())

class ImageService:
    def __init__(self, image_from_ros: bool, image_http_base: str, vid: int):
        self.image_from_ros = bool(image_from_ros)
        self.image_http_base = str(image_http_base)
        self.vid = int(vid)
    def select_color_topic(self, node: Node, ns_prefix: str) -> Optional[str]:
        try:
            topics = node.get_topic_names_and_types()
        except Exception:
            topics = []
        candidates = []
        for name, _types in topics:
            if name.endswith("/color/image_raw"):
                candidates.append(name)
        if ns_prefix:
            for name in candidates:
                if name.startswith(ns_prefix + "/"):
                    return name
        return candidates[0] if candidates else None
    def ensure_camera_subscription(self, env: 'IsaacSimEnv') -> bool:
        if env._color_sub is not None and env._color_topic:
            return True
        topic = self.select_color_topic(env, env._mavros_prefix or "")
        if not topic:
            return False
        qos = QoSProfile(reliability=ReliabilityPolicy.BEST_EFFORT, history=HistoryPolicy.KEEP_LAST, depth=10)
        try:
            env._color_sub = env.create_subscription(Image, topic, env._color_image_cb, qos)
            env._color_topic = topic
            return True
        except Exception as e:
            import traceback as _tb
            env.get_logger().warn(f"ensure_camera_subscription failed: {e}")
            env.get_logger().warn(_tb.format_exc())
            return False
    def encode_image_msg_to_b64(self, msg: Image) -> Optional[str]:
        if msg is None:
            return None
        if PILImage is None:
            return None
        w = int(getattr(msg, "width", 0) or 0)
        h = int(getattr(msg, "height", 0) or 0)
        enc = str(getattr(msg, "encoding", "")).lower()
        data = bytes(getattr(msg, "data", b""))
        if w <= 0 or h <= 0 or not data:
            return None
        try:
            arr = np.frombuffer(data, dtype=np.uint8)
            if enc in ("rgb8", "bgr8", "rgba8", "bgra8"):
                c = 3 if enc.endswith("rgb8") or enc.endswith("bgr8") else 4
                arr = arr.reshape((h, w, c))
                if enc.startswith("bgr") or enc.startswith("bgra"):
                    arr = arr[:, :, :3][:, :, ::-1]
                else:
                    arr = arr[:, :, :3]
            elif enc in ("mono8", "8UC1"):
                arr = arr.reshape((h, w))
            else:
                step = int(getattr(msg, "step", w * 3))
                c = step // max(w, 1)
                try:
                    arr = arr.reshape((h, w, c))
                    arr = arr[:, :, :3]
                except Exception:
                    return None
            pil_img = PILImage.fromarray(arr)
            buf = BytesIO()
            pil_img.save(buf, format='PNG')
            return base64.b64encode(buf.getvalue()).decode('ascii')
        except Exception:
            import traceback as _tb
            print(_tb.format_exc())
            return None
    def capture_b64(self, env: 'IsaacSimEnv') -> Optional[str]:
        if env._image_from_ros:
            ok = self.ensure_camera_subscription(env)
            if not ok:
                return None
            if env._last_color_msg is None and env._color_topic:
                try:
                    msg = wait_for_message(env, env._color_topic, Image, timeout=2.0)
                    env._last_color_msg = msg
                except Exception as e:
                    import traceback as _tb
                    env.get_logger().warn(f"wait_for_message failed: {e}")
                    env.get_logger().warn(_tb.format_exc())
            rclpy.spin_once(env, timeout_sec=0.05)
            time.sleep(0.05)
            return self.encode_image_msg_to_b64(env._last_color_msg)
        else:
            try:
                import urllib.request, json as _json
                url = f"{self.image_http_base}/uav/{self.vid}/image"
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    raw = resp.read()
                    obj = _json.loads(raw.decode("utf-8"))
                    b64 = obj.get("data") or obj.get("image", {}).get("data")
                    if isinstance(b64, str) and b64:
                        return b64
                    return None
            except Exception as e:
                import traceback as _tb
                env.get_logger().warn(f"fetch HTTP image failed: {e}")
                env.get_logger().warn(_tb.format_exc())
                return None

class HttpApi:
    def __init__(self, env: 'IsaacSimEnv'):
        self.env = env
        
    def reset(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        try:
            force = bool(data.get("force", True))
            hard = bool(data.get("hard", True))
            guard = TaskGuard(self.env._task_lock, force, getattr(self.env, "_reset_cancel_event", None), timeout_s=30.0)
            if not guard.acquired:
                return {"status": "error", "message": "busy"}, 409
            vid = data.get("vid")
            pos = data.get("position") or data.get("pos")
            yaw_deg = data.get("yaw_deg")
            try:
                vid = int(vid) if vid is not None else None
            except Exception:
                guard.release()
                return {"status": "error", "message": "vid must be int"}, 400
            if vid is not None and vid != self.env._vid:
                guard.release()
                return {"status": "error", "message": f"vid mismatch: expected {self.env._vid}, got {vid}"}, 400
            if pos is not None and (not isinstance(pos, (list, tuple)) or len(pos) < 3):
                guard.release()
                return {"status": "error", "message": "position must be [x,y,z]"}, 400
            self.env._batch_size = 1
            self.env._batch_states = [{"done": False}]
            self.env._json_name_list = [None]
            self.env._instructions_list = [None]
            try:
                if getattr(self.env, "_reset_cancel_event", None):
                    self.env._reset_cancel_event.clear()
                if hard:
                    self.env.reboot_px4_hard(pos, yaw_deg)
                else:
                    self.env.reboot_px4(pos, yaw_deg)
                return {"status": "success", "message": "reset ok", "vid": vid, "position": pos}, 200
            finally:
                guard.release()
        except InterruptedError as e:
            self.env.get_logger().warn(f"HTTP /reset interrupted: {e}\n{traceback.format_exc()}")
            return {"status": "error", "message": "reset interrupted"}, 409
        except TimeoutError as e:
            self.env.get_logger().error(f"HTTP /reset exception: {e}\n{traceback.format_exc()}")
            return {"status": "error", "message": str(e)}, 504
        except Exception as e:
            self.env.get_logger().error(f"HTTP /reset exception: {e}\n{traceback.format_exc()}")
            return {"status": "error", "message": str(e)}, 500
        
    def command(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        try:
            force = bool(data.get("force", False))
            guard = TaskGuard(self.env._task_lock, force)
            if not guard.acquired:
                return {"ok": False, "error": "busy"}, 409
            if not isinstance(data, dict):
                guard.release()
                return {"ok": False, "error": "JSON body must be an object"}, 400
            try:
                resp = self.env.process_command(data)
            finally:
                guard.release()
            return resp, 200
        except Exception as e:
            self.env.get_logger().error(f"HTTP /command exception: {e}\n{traceback.format_exc()}")
            return {"ok": False, "error": str(e)}, 500
        
    def step(self, data: Dict[str, Any], use_ros_image: bool) -> Tuple[Dict[str, Any], int]:
        try:
            force = bool(data.get("force", False))
            guard = TaskGuard(self.env._task_lock, force)
            if not guard.acquired:
                return {"status": "error", "message": "busy"}, 409
            try:
                resp, code = self.env._process_step_api(data, use_ros_image=use_ros_image)
            finally:
                guard.release()
            return resp, code
        except Exception as e:
            self.env.get_logger().error(f"HTTP /step exception: {e}\n{traceback.format_exc()}")
            return {"status": "error", "message": str(e)}, 500


# =======================================================================
#                           Logging Setup
# =======================================================================
import logging

_LOG_DIR = Path(__file__).parent / "logs"
_LOG_INITIALIZED = False
_FILE_LOGGER: Optional[logging.Logger] = None


def setup_file_logger(script_name: str, session_ts: Optional[str] = None) -> logging.Logger:
    """
    设置文件日志记录器。

    Args:
        script_name: 脚本名称（用于日志文件命名）
        session_ts: 会话时间戳（可选，用于目录分组）

    Returns:
        配置好的 Logger 对象
    """
    global _LOG_INITIALIZED, _FILE_LOGGER

    if _LOG_INITIALIZED and _FILE_LOGGER is not None:
        return _FILE_LOGGER

    # 创建日志目录
    if session_ts:
        log_dir = _LOG_DIR / str(session_ts)
    else:
        log_dir = _LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    # 创建日志文件路径
    log_file = log_dir / f"{script_name}_{int(time.time())}.log"

    # 创建 Logger
    logger = logging.getLogger(f"pegasus.{script_name}")
    logger.setLevel(logging.DEBUG)

    # 文件 Handler
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)

    # 控制台 Handler（仅 INFO 及以上）
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "[%(levelname)s] %(message)s"
    ))
    logger.addHandler(ch)

    logger.propagate = False
    _LOG_INITIALIZED = True
    _FILE_LOGGER = logger

    logger.info(f"Log file: {log_file}")
    return logger


# =======================================================================
#                                   main
# =======================================================================
def main():
    # 解析必要的 CLI 参数，并过滤 ROS2 命名空间标志
    import argparse
    parser = argparse.ArgumentParser(description="Pegasus ROS2 IsaacSimEnv")
    parser.add_argument("--mavros-ns", dest="mavros_ns", type=str, default=None, help="ROS2 namespace for MAVROS (e.g., uav0)")
    parser.add_argument("--launch-file", dest="launch_file", type=str, default=None, help="Path to MAVROS launch file")
    parser.add_argument("--log-dir", dest="log_dir", type=str, default=None, help="Directory for log files")
    parser.add_argument("--no-auto-reset", dest="no_auto_reset", action="store_true", help="Skip automatic reset (arm/takeoff) on startup")
    # 允许通过 --ros-args __ns:=uavX 的方式设置命名空间（由 launch_multi_rospy 传递）
    args = parser.parse_args()

    # 获取环境变量
    env_mavros_ns = os.environ.get("MAVROS_NS")
    mavros_ns = env_mavros_ns if env_mavros_ns else args.mavros_ns
    session_ts = os.environ.get("PEGASUS_SESSION_TS")

    # 设置日志
    script_name = f"rospy_{mavros_ns or 'default'}"
    logger = setup_file_logger(script_name, session_ts)
    logger.info(f"Starting rospy_isaacsim.py with mavros_ns={mavros_ns}")

    rclpy.init()
    env = None
    try:
        env = IsaacSimEnv(mavros_ns=mavros_ns)
        env.get_logger().info("IsaacSimEnv node started.")

        # ============================================================
        # 自动启动 MAVROS（关键修复）
        # ============================================================
        vid = env._vid

        # 确定 MAVROS launch 文件路径
        launch_file: Optional[Path] = None
        if args.launch_file:
            launch_file = Path(args.launch_file)
        else:
            # 默认路径：examples/launch/pegasus_uav_{vid}.launch
            default_launch = Path(__file__).parent / "launch" / f"pegasus_uav_{vid}.launch"
            if default_launch.exists():
                launch_file = default_launch
            else:
                # 尝试生成 launch 文件
                config_path = Path(__file__).parent / "multi_uav_config.json"
                if config_path.exists():
                    try:
                        from pegasus.simulator.logic.vehicle_manager import VehicleManager
                        VehicleManager.generate_mavros_launch_files(str(config_path))
                        if default_launch.exists():
                            launch_file = default_launch
                    except Exception as e:
                        logger.warning(f"Failed to generate MAVROS launch files: {e}")

        # 确定日志目录
        log_dir: Optional[Path] = None
        if args.log_dir:
            log_dir = Path(args.log_dir)
        elif session_ts:
            log_dir = _LOG_DIR / str(session_ts) / "mavros"
        else:
            log_dir = _LOG_DIR / "mavros"
        log_dir.mkdir(parents=True, exist_ok=True)

        # 设置 MAVROS launch 文件并启动
        if launch_file and launch_file.exists():
            env.set_mavros_launch_file(launch_file, log_dir)
            logger.info(f"Starting MAVROS for UAV{vid} with launch file: {launch_file}")

            ok = env.start_mavros()
            if ok:
                logger.info(f"MAVROS started successfully for UAV{vid}")
            else:
                logger.error(f"Failed to start MAVROS for UAV{vid}")

            # 等待 MAVROS 进程稳定
            time.sleep(2.0)
        else:
            logger.warning(f"No MAVROS launch file found for UAV{vid}. FCU connection may fail.")
            logger.warning(f"Expected launch file: examples/launch/pegasus_uav_{vid}.launch")

        # 在 MAVROS 启动后等待 FCU 连接
        logger.info(f"Waiting for FCU connection...")
        env.wait_for_fcu_connection()
        # Configure MAVROS time sync parameters to avoid PX4 timesync RTT warnings
        # try:
        #     env.get_logger().info(f"UAV{env._vid} Configuring MAVROS time sync parameters...")
        #     env.configure_mavros_time_plugin()
            
        #     # Set PX4 parameters as requested
        #     # MPC_TKO_SPEED=5.0, MPC_Z_VEL_MAX_UP=5.0, MPC_Z_VEL_MAX_DN=5.0, MPC_XY_VEL_MAX=10.0
        #     # Also setting MPC_TILTMAX_AIR for pitch/roll limiting if needed, but not explicitly requested yet.
        #     px4_params = {
        #         "MPC_TKO_SPEED": 5.0,
        #         "MPC_Z_VEL_MAX_UP": 5.0,
        #         "MPC_Z_VEL_MAX_DN": 5.0,
        #         "MPC_XY_VEL_MAX": 10.0
        #     }
        #     env.get_logger().info(f"UAV{env._vid} Setting PX4 flight parameters: {px4_params}")
        #     env.set_px4_params(px4_params, timeout_sec=5.0)
            
        # except Exception as e:
        #     env.get_logger().error(f"Failed to configure parameters: {e}")

        if args.no_auto_reset:
            env.get_logger().info("Skipping auto-reset (--no-auto-reset). Waiting for HTTP commands...")
        else:
            env.reset()
            env.get_logger().info("Environment reset complete; switching to hover+HTTP mode.")
        env.start_http_server()
        # Hover + HTTP commands loop (wait for external HTTP commands)
        try:
            env._http_hover_loop()
        except Exception as e:
            env.get_logger().error(f"Hover loop exception: {e}")

    except Exception as e:
        # 异常情况：记录错误但继续执行清理流程
        if env is not None:
            try:
                env.get_logger().error(f"Unhandled exception: {e}")
            except Exception as le:
                print(f"[ERROR] main() unhandled exception log failed: {le}")
        else:
            print(f"Unhandled exception in main(): {e}")

    finally:
        # 释放 ROS 资源
        if env is not None:
            try:
                env.destroy_node()
            except Exception as de:
                print(f"[WARN] destroy_node failed: {de}")
        try:
            rclpy.shutdown()
        except Exception as se:
            print(f"[WARN] rclpy.shutdown failed: {se}")


if __name__ == "__main__":
    main()
