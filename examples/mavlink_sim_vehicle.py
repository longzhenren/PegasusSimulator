#!/usr/bin/env python
"""
MAVLink直接控制仿真环境（mavlink_sim_vehicle.py）

==========================
概述
==========================
本脚本是基于 MAVLink 直接控制的仿真环境，特点：
- 启动 PX4 SITL（与 8_camera_vehicle.py 相同）
- 使用 MAVLink 直接与 PX4 通信（不使用 MAVROS）
- 支持 Mission 模式航点导航
- 内置 MAVLink 控制器，提供 HTTP 接口
- 提供与 8_camera_vehicle.py 完全兼容的 HTTP 接口

==========================
与现有系统的区别
==========================
| 特性           | 8_camera_vehicle.py + rospy | mavlink_sim_vehicle.py    |
|----------------|-----------------------------|-----------------------------|
| 控制后端       | PX4 + MAVROS + ROS2          | PX4 + MAVLink直接通信       |
| 依赖           | PX4-Autopilot, MAVROS, ROS2  | PX4-Autopilot, pymavlink    |
| 启动时间       | ~30s (含ROS2+MAVROS)         | ~15s                        |
| 资源消耗       | 高 (多进程ROS2节点)          | 中                          |
| HTTP仿真端口   | 8081                         | 8081 (兼容)                 |
| HTTP控制端口   | 5009+id (rospy_isaacsim.py)  | 5009+id (内置)              |
| 控制方式       | MAVROS服务                   | MAVLink Mission模式         |

==========================
HTTP 接口（端口 8081，与原系统完全兼容）
==========================
GET  /uav/<id>/pose           - 获取位姿
GET  /uav/<id>/image          - 获取图像 (JSON + Base64)
GET  /uav/<id>/image.png      - 获取图像 (PNG二进制)
GET  /uav/<id>/all            - 获取图像+位姿同步快照
POST /uav/<id>/reset          - 重置UAV位置
GET  /uav/<id>/px4/ready      - 查询PX4就绪状态
GET  /health                  - 健康检查

控制接口（端口 5009+id，与 rospy_isaacsim.py 兼容）：
POST /reset                   - 重置UAV
POST /command                 - 发送控制命令
     {"cmd": "move_to", "x": X, "y": Y, "z": Z}
     {"cmd": "move_to_many", "points": [[x,y,z], ...]}
     {"cmd": "execute_mission", "waypoints": [...]}
     {"cmd": "land"}
     {"cmd": "get_position"}
     {"cmd": "get_status"}
GET  /health                  - 健康检查

==========================
使用方法
==========================
# 启动仿真
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py

# 使用配置文件
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py --config examples/multi_uav_config.json

# 无头模式
ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py --headless

==========================
命令行参数
==========================
--config PATH      UAV配置文件路径（默认：multi_uav_config.json）
--headless         无头模式运行
--sim-port PORT    仿真HTTP端口（默认：8081）
--ctrl-base-port   控制器HTTP端口基础（默认：5009）

"""

import sys
import os
import argparse

# ==============================================================================
# 1. 第一步：解析参数 & 启动 SimulationApp
# (注意：在 SimulationApp 启动前，绝对不要导入 numpy, omni, pxr 等库)
# ==============================================================================

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "multi_uav_config.json")
SIMULATION_ENVIRONMENTS = {
    "Flat Plane": "https://omniverse-content-production.s3-us-west-2.amazonaws.com/Assets/Isaac/5.1/Isaac/Environments/Terrains/flat_plane.usd",
}
DEFAULT_USD_PATH = SIMULATION_ENVIRONMENTS['Flat Plane']

def parse_args():
    parser = argparse.ArgumentParser(description="MAVLink Direct Control Simulation")
    parser.add_argument("--config", type=str, default=CONFIG_PATH, help="UAV config JSON path")
    parser.add_argument("--usd", type=str, default=DEFAULT_USD_PATH, help="USD scene file path")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--no-images", action="store_true", help="Disable camera/image capture and related endpoints")
    parser.add_argument("--sim-port", type=int, default=8081, help="Simulation HTTP port")
    parser.add_argument("--ctrl-base-port", type=int, default=5009, help="Controller HTTP base port")
    return parser.parse_args()

ARGS = parse_args()
USD_PATH = ARGS.usd
IMAGES_ENABLED = not bool(getattr(ARGS, "no_images", False))

# 仿真配置
USE_RASTERIZATION = True
RENDER_THROTTLE = True
RENDER_MAX_FPS = 60.0
CAMERA_RESOLUTION = (480, 480)

# 配置 Isaac Sim 启动参数

APP_CONFIG = {
    # "width": 600,
    # "height": 600,
    "window_width": 1280,
    "window_height": 720,
    "headless": ARGS.headless,
    "max_bounces": 0,
    "samples_per_pixel_per_frame": 1,  # 默认 64，很吃GPU，先降
    "anti_aliasing": 1,  # 0/1 更快（3=高质量）
    # "renderer": "RayTracedLighting",
    "renderer": "Rasterization" if USE_RASTERIZATION else "RayTracedLighting",
    # Ref: https://docs.isaacsim.omniverse.nvidia.com/4.5.0/reference_material/sim_performance_optimization_handbook.html
    "extra_args": [
        # DLSS Performance Mode: When rendering 720p cameras, Auto mode tends to select Quality, so you may see performance impacts by running in Auto mode while rendering cameras at lower resolution.
        "--/rtx/post/dlss/execMode=0",  # 0=Performance,1=Balanced,2=Quality,3=Auto
        # "--/rtx/debugMaterialType=0",  # -1=Regular, 0=Disable Only affect Non-Headless mode, No effect on Headless mode
        # Reduce the Texture Streaming Budget: Texture Streaming Budget (% of GPU memory)
        # "--/rtx-transient/resourcemanager/texturestreaming/memoryBudget=0.6",
        # "--/rtx-transient/resourcemanager/texturestreaming/enabled=false",
        "--exts.\"isaacsim.core.throttling\".enable_async=true",
        "--exts.\"omni.isaac.throttling\".enable_async=true",
        # Enable MDL Disk Cache
        "--mdl-disk-cache=true",
        "--mdl-disk-cache-path=/home/user/.cache/omni/mdl_cache",
        # ========== 仿真加速：禁用实时节流 ==========
        # "--/app/runLoops/main/rateLimitEnabled=false",
        # "--/app/runLoops/main/rateLimitFrequency=0",
        # "--/app/runLoops/rendering/rateLimitEnabled=false",
        # "--/physics/updateToUsd=false",  # 减少 USD 同步开销
        # "--/app/asyncRendering=true",
        # "--/app/asyncRenderingLowLatency=false",
    ],
}

# 启动仿真引擎 (必须在导入其他库之前执行)
from isaacsim import SimulationApp
import carb
simulation_app = SimulationApp(APP_CONFIG)

# ==============================================================================
# 2. 第二步：导入其他依赖库 (numpy, pegasus 等)
# ==============================================================================
import logging
import json
import traceback
import threading
import time
import base64
import csv
import math
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Any

# 现在可以安全导入 numpy 了
import numpy as np 

from PIL import Image
from flask import Flask, jsonify, request, Response
from werkzeug.serving import make_server
from scipy.spatial.transform import Rotation
from pymavlink import mavutil

import omni.timeline
import omni.usd
from pxr import Usd, UsdGeom, UsdPhysics, PhysxSchema

# 配置 Pegasus 路径
sys.path.insert(0, os.path.expanduser("~/PegasusSimulator-5.1/extensions/pegasus.simulator/"))

from pegasus.simulator.params import ROBOTS
from pegasus.simulator.logic.graphical_sensors.monocular_camera import MonocularCamera
from pegasus.simulator.logic.backends.px4_mavlink_backend import PX4MavlinkBackend, PX4MavlinkBackendConfig
from pegasus.simulator.logic.vehicles.multirotor import Multirotor, MultirotorConfig
from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface
from isaacsim.core.api.world import World

# ==============================================================================
# 3. 业务逻辑代码
# ==============================================================================

# 录制设置
RECORD_ENABLE = False
RECORD_FPS = 10.0
RECORD_DIR = os.path.join(os.path.dirname(__file__), "recordings")

# UAV透明度
UAV_TRANSPARENCY_ENABLE = True
UAV_TRANSPARENCY_ALPHA = 0.0
DISABLE_UAV_UAV_COLLISION = True

def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """生成带时间戳的日志消息"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg, flush=True)
    return log_msg


def get_sim_time() -> float:
    """获取Isaac Sim模拟器时间（秒）

    Returns:
        float: 模拟器当前时间（秒），如果获取失败则返回系统时间
    """
    try:
        timeline = omni.timeline.get_timeline_interface()
        return timeline.get_current_time()
    except Exception:
        return time.time()


# -------------------------
# Image Ring Buffer for Async Collection
# -------------------------
class ImageRingBuffer:
    """
    图像环形缓冲区，用于异步图像采集

    使用方法:
    1. start_recording(save_dir) - 开始录制，可指定磁盘保存目录
    2. add_frame() - 添加帧（由仿真循环调用）
    3. stop_recording() - 停止录制并返回所有帧元数据

    特性：
    - 支持大容量（默认5000帧，约4分钟@20Hz）
    - 支持磁盘保存模式，减少内存占用
    - 不使用环形覆盖，保留完整轨迹数据
    """
    def __init__(self, max_frames: int = 5000, target_fps: float = 20.0):
        self.max_frames = max_frames
        self.target_fps = target_fps
        self.min_interval = 1.0 / target_fps

        self._lock = threading.Lock()
        self._recording = False
        self._frames: List[Dict] = []  # [{timestamp, pose, image_path/image_b64, width, height}, ...]
        self._last_capture_time = 0.0
        self._start_time = 0.0

        # 磁盘保存模式
        self._save_to_disk = False
        self._save_dir: Optional[str] = None
        self._frame_counter = 0

    def start_recording(self, save_dir: Optional[str] = None) -> bool:
        """开始录制

        Args:
            save_dir: 可选的磁盘保存目录。如果指定，图像将保存到磁盘而非内存。
                     这可以大幅减少内存占用，支持更长时间的录制。

        Returns:
            是否成功开始录制
        """
        with self._lock:
            self._recording = True
            self._frames = []
            self._last_capture_time = 0.0
            self._start_time = time.time()
            self._frame_counter = 0

            # 设置磁盘保存模式
            self._save_to_disk = save_dir is not None
            self._save_dir = save_dir

            if self._save_to_disk and save_dir:
                try:
                    os.makedirs(save_dir, exist_ok=True)
                    os.makedirs(os.path.join(save_dir, "images"), exist_ok=True)
                    ts_log("[ImageBuffer]", f"Recording to disk: {save_dir}")
                except Exception as e:
                    ts_log("[ImageBuffer]", f"Failed to create save dir: {e}", "ERROR")
                    self._save_to_disk = False
                    self._save_dir = None

            return True

    def stop_recording(self) -> List[Dict]:
        """停止录制并返回所有帧元数据

        Returns:
            帧列表。如果使用磁盘模式，image_b64字段为空，需要从image_path读取。
        """
        with self._lock:
            self._recording = False
            frames = self._frames.copy()
            self._frames = []

            # 如果是磁盘模式，保存CSV索引文件
            if self._save_to_disk and self._save_dir and frames:
                try:
                    csv_path = os.path.join(self._save_dir, "frame_index.csv")
                    with open(csv_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["frame_idx", "timestamp", "image_path",
                                         "pos_x", "pos_y", "pos_z",
                                         "att_w", "att_x", "att_y", "att_z",
                                         "linvel_x", "linvel_y", "linvel_z"])
                        for frame in frames:
                            pose = frame.get("pose", {})
                            pos = pose.get("position", [0, 0, 0])
                            att = pose.get("attitude", [0, 0, 0, 1])
                            vel = pose.get("linear_velocity", [0, 0, 0])
                            writer.writerow([
                                frame.get("frame_idx", 0),
                                frame.get("timestamp", 0),
                                frame.get("image_path", ""),
                                pos[0], pos[1], pos[2],
                                att[3] if len(att) > 3 else 1, att[0], att[1], att[2],
                                vel[0], vel[1], vel[2]
                            ])
                    ts_log("[ImageBuffer]", f"Saved frame index: {csv_path} ({len(frames)} frames)")
                except Exception as e:
                    ts_log("[ImageBuffer]", f"Failed to save frame index: {e}", "WARN")

            return frames

    def is_recording(self) -> bool:
        """检查是否正在录制"""
        with self._lock:
            return self._recording

    def add_frame(self, timestamp: float, pose: Dict, image_b64: str, width: int, height: int) -> bool:
        """
        添加帧到缓冲区（由仿真循环调用）

        Args:
            timestamp: 仿真时间戳
            pose: 位姿数据字典
            image_b64: Base64编码的PNG图像
            width, height: 图像尺寸

        Returns:
            是否成功添加
        """
        with self._lock:
            if not self._recording:
                return False

            # 检查是否达到目标帧率
            now = time.time()
            if now - self._last_capture_time < self.min_interval:
                return False

            # 检查是否达到最大帧数（不覆盖，保留完整数据）
            if len(self._frames) >= self.max_frames:
                # 只记录一次警告
                if len(self._frames) == self.max_frames:
                    ts_log("[ImageBuffer]", f"Max frames ({self.max_frames}) reached, stopping capture", "WARN")
                return False

            frame_idx = self._frame_counter
            self._frame_counter += 1

            # 磁盘保存模式
            if self._save_to_disk and self._save_dir:
                try:
                    ts_ms = int(timestamp * 1000)
                    img_filename = f"img_{frame_idx:06d}_{ts_ms}.png"
                    img_path = os.path.join(self._save_dir, "images", img_filename)

                    # 解码并保存图像
                    img_data = base64.b64decode(image_b64.encode("ascii"))
                    with open(img_path, "wb") as f:
                        f.write(img_data)

                    # 只保存元数据（不含图像数据）
                    frame = {
                        "frame_idx": frame_idx,
                        "timestamp": timestamp,
                        "pose": pose,
                        "image_path": img_path,
                        "image_b64": "",  # 磁盘模式不保存base64
                        "width": width,
                        "height": height,
                    }
                except Exception as e:
                    ts_log("[ImageBuffer]", f"Failed to save frame {frame_idx}: {e}", "WARN")
                    return False
            else:
                # 内存模式
                frame = {
                    "frame_idx": frame_idx,
                    "timestamp": timestamp,
                    "pose": pose,
                    "image_b64": image_b64,
                    "width": width,
                    "height": height,
                }

            self._frames.append(frame)
            self._last_capture_time = now
            return True

    def get_frame_count(self) -> int:
        """获取当前帧数"""
        with self._lock:
            return len(self._frames)

    def get_recording_duration(self) -> float:
        """获取录制时长"""
        with self._lock:
            if not self._recording:
                return 0.0
            return time.time() - self._start_time

    def get_save_dir(self) -> Optional[str]:
        """获取磁盘保存目录"""
        with self._lock:
            return self._save_dir


class MAVLinkController:
    """
    MAVLink直接控制器，使用OFFBOARD模式控制PX4
    提供与 rospy_isaacsim.py 兼容的 HTTP 接口

    重要：使用独立的UDP MAVLink连接到PX4的onboard端口，
    不与HIL后端共享TCP连接（避免lockstep冲突）。
    """

    def __init__(self, uav_id: int, vehicle, px4_backend: PX4MavlinkBackend, port: int):
        self.uav_id = uav_id
        self.vehicle = vehicle
        self.px4_backend = px4_backend
        self.port = port
        self._log_prefix = f"[MAVLink UAV{uav_id}]"

        # MAVLink连接 - 使用独立的UDP连接到onboard端口
        self._mavlink_conn = None
        self._mavlink_lock = threading.Lock()

        # PX4 onboard端口配置（与rcS_minmal.mavlink一致）
        # PX4监听: 14580 + uav_id (local)
        # PX4发送: 14740 + uav_id (remote)
        self._px4_local_port = 14580 + uav_id  # PX4 listens here
        self._ctrl_local_port = 14740 + uav_id  # We listen here (receive from PX4)

        # 状态
        self.mode = "MANUAL"
        self.armed = False
        self.ready = False

        # 目标位置
        self.target_position: Optional[np.ndarray] = None
        self.current_mission_idx = 0
        self.mission_waypoints: List[Dict] = []

        # 到达阈值
        self.reach_threshold = 0.5  # meters

        # Flask app
        self.flask_app = Flask(f"mavlink_ctrl_{uav_id}")
        self._flask_server = None
        self._setup_routes()

        # 后台线程
        self._running = False
        self._heartbeat_thread = None
        self._state_thread = None

    def _get_mavlink_conn(self):
        """获取MAVLink连接（独立的UDP连接）"""
        with self._mavlink_lock:
            return self._mavlink_conn

    def _setup_mavlink_connection(self):
        """建立独立的MAVLink UDP连接到PX4 onboard端口"""
        is_new_connection = False
        with self._mavlink_lock:
            if self._mavlink_conn is not None:
                ts_log(self._log_prefix, "MAVLink connection already established")
                return True

            try:
                # 使用UDP连接到PX4的onboard端口
                # udpout: 发送到指定端口，不监听
                conn_str = f"udpout:127.0.0.1:{self._px4_local_port}"
                ts_log(self._log_prefix, f"Creating UDP MAVLink connection: {conn_str}")

                self._mavlink_conn = mavutil.mavlink_connection(
                    conn_str,
                    source_system=255,  # GCS system ID
                    source_component=0
                )

                # 设置目标系统（PX4）
                self._mavlink_conn.target_system = self.uav_id + 1  # MAV_SYS_ID = px4_instance + 1
                self._mavlink_conn.target_component = 1

                ts_log(self._log_prefix, f"MAVLink UDP connection established to port {self._px4_local_port}")
                is_new_connection = True

            except Exception as e:
                ts_log(self._log_prefix, f"Failed to create MAVLink connection: {e}", "ERROR")
                self._mavlink_conn = None
                return False

        # 配置高动态参数（在锁外调用，避免死锁）
        if is_new_connection:
            self._configure_aggressive_params()

        return True

    def _configure_aggressive_params(self):
        """配置PX4高动态/竞速模式参数

        通过MAVLink PARAM_SET发送参数，优化以下方面：
        1. 解锁最大倾角 - 允许更大的水平加速度
        2. 提高XY跟踪刚度 - 更快响应位置误差
        3. 强化Z轴稳定性 - 防止大倾角时高度掉落
        4. 减少平滑限制 - 允许瞬间大加速度
        """
        conn = self._get_mavlink_conn()
        if conn is None:
            ts_log(self._log_prefix, "Cannot configure params: no MAVLink connection", "WARN")
            return False

        ts_log(self._log_prefix, "Configuring aggressive/high-dynamic parameters...")

        # 参数列表: (参数名, 值, 说明)
        params = [
            # === 解锁最大倾角 (关键参数) ===
            ("MPC_TILTMAX_AIR", 50.0, "Max tilt angle in air (deg)"),
            ("MPC_MAN_TILT_MAX", 60.0, "Max manual tilt (deg)"),

            # === 提高水平跟踪刚度 ===
            ("MPC_XY_P", 2.0, "Position XY P gain"),
            ("MPC_XY_VEL_P_ACC", 4.0, "Velocity to Accel gain"),

            # === 强化Z轴稳定性 (防止掉高) ===
            ("MPC_Z_P", 1.0, "Position Z P gain"),
            ("MPC_Z_VEL_P_ACC", 2.0, "Z Velocity to Accel gain"),
            ("MPC_THR_HOVER", 0.35, "Hover throttle (compensate tilt)"),

            # === 减少平滑限制 ===
            ("MPC_JERK_AUTO", 20.0, "Auto jerk limit (m/s³)"),

            # === 可选: 提高速度限制 ===
            ("MPC_XY_VEL_MAX", 16.0, "Max horizontal velocity (m/s)"),
            ("MPC_Z_VEL_MAX_UP", 4.0, "Max vertical up velocity (m/s)"),
            ("MPC_Z_VEL_MAX_DN", 4.0, "Max vertical down velocity (m/s)"),
        ]

        target_sys = self.uav_id + 1
        target_comp = 1
        success_count = 0

        for param_name, param_value, description in params:
            try:
                # 发送PARAM_SET消息
                # 参数名必须是16字节的bytes，用\0填充
                param_id = param_name.encode('utf-8')
                if len(param_id) < 16:
                    param_id = param_id + b'\0' * (16 - len(param_id))
                else:
                    param_id = param_id[:16]

                conn.mav.param_set_send(
                    target_sys,
                    target_comp,
                    param_id,
                    float(param_value),
                    mavutil.mavlink.MAV_PARAM_TYPE_REAL32
                )

                success_count += 1
                time.sleep(0.05)  # 确保指令不丢失

            except Exception as e:
                ts_log(self._log_prefix, f"Failed to set {param_name}: {e}", "WARN")

        ts_log(self._log_prefix, f"Configured {success_count}/{len(params)} aggressive parameters")
        return success_count == len(params)

    def _setup_routes(self):
        app = self.flask_app

        @app.route('/health', methods=['GET'])
        def health():
            return jsonify({
                "status": "healthy",
                "env_id": f"uav{self.uav_id}:{self.port}",
                "type": "mavlink_direct"
            })

        @app.route('/reset', methods=['POST'])
        def reset():
            try:
                data = request.json or {}
                position = data.get("position")
                yaw_deg = data.get("yaw_deg", 0.0)
                hard = data.get("hard", True)
                force = data.get("force", False)

                if position and len(position) >= 3:
                    # 重置UAV位置（通过仿真端）
                    self._reset_vehicle(position, yaw_deg)
                    self.target_position = np.array(position)

                # 重置PX4状态
                if hard:
                    self._disarm()
                    # 重新配置高动态参数（hard reset后需要重新应用）
                    self._configure_aggressive_params()

                self.ready = True

                return jsonify({
                    "status": "success",
                    "position": position if position else self._get_position().tolist()
                })
            except Exception as e:
                ts_log(self._log_prefix, f"Reset error: {e}", "ERROR")
                return jsonify({"status": "error", "message": str(e)}), 500

        @app.route('/command', methods=['POST'])
        def command():
            try:
                data = request.json or {}
                cmd = data.get("cmd", "")
                force = data.get("force", False)

                if cmd == "move_to":
                    x = float(data.get("x", 0))
                    y = float(data.get("y", 0))
                    z = float(data.get("z", 0))
                    return self._handle_move_to(x, y, z)

                elif cmd == "move_to_many":
                    points = data.get("points", [])
                    return self._handle_move_to_many(points)

                elif cmd == "execute_mission":
                    waypoints = data.get("waypoints", [])
                    return self._handle_execute_mission(waypoints)

                elif cmd == "land":
                    return self._handle_land()

                elif cmd == "get_position":
                    pos = self._get_position()
                    return jsonify({"status": "success", "ok": True, "position": pos.tolist()})

                elif cmd == "get_status":
                    return jsonify({
                        "status": {
                            "mode": self.mode,
                            "armed": self.armed,
                            "ready": self.ready,
                        },
                        "ok": True
                    })

                elif cmd == "arm":
                    ok = self._arm()
                    return jsonify({"status": "success" if ok else "failed", "ok": ok})

                elif cmd == "disarm":
                    ok = self._disarm()
                    return jsonify({"status": "success" if ok else "failed", "ok": ok})

                elif cmd == "set_mode":
                    mode = data.get("mode", "OFFBOARD")
                    ok = self._set_mode(mode)
                    return jsonify({"status": "success" if ok else "failed", "ok": ok, "mode": mode})

                elif cmd == "setpoint":
                    # PVA前馈setpoint命令：位置+速度+加速度+yaw+yaw_rate
                    x = float(data.get("x", 0))
                    y = float(data.get("y", 0))
                    z = float(data.get("z", 0))
                    vx = float(data.get("vx", 0))
                    vy = float(data.get("vy", 0))
                    vz = float(data.get("vz", 0))
                    afx = float(data.get("afx", 0))
                    afy = float(data.get("afy", 0))
                    afz = float(data.get("afz", 0))
                    yaw = float(data.get("yaw", 0))
                    yaw_rate = float(data.get("yaw_rate", 0))
                    # Debug: 每100次记录一次
                    if not hasattr(self, '_setpoint_count'):
                        self._setpoint_count = 0
                    self._setpoint_count += 1
                    if self._setpoint_count % 100 == 1:
                        ts_log(self._log_prefix, f"setpoint #{self._setpoint_count}: pos=({x:.2f},{y:.2f},{z:.2f}) vel=({vx:.2f},{vy:.2f},{vz:.2f})")
                    self._send_velocity_setpoint(x, y, z, vx, vy, vz, afx, afy, afz, yaw, yaw_rate)
                    return jsonify({"status": "success", "ok": True})

                elif cmd == "takeoff":
                    # 直接起飞命令 - 发送MAV_CMD_NAV_TAKEOFF
                    altitude = float(data.get("altitude", 2.5))  # 默认起飞高度2.5m
                    ok = self._takeoff(altitude)
                    return jsonify({"status": "success" if ok else "failed", "ok": ok, "altitude": altitude})

                else:
                    return jsonify({"status": "error", "ok": False, "message": f"Unknown command: {cmd}"}), 400

            except Exception as e:
                ts_log(self._log_prefix, f"Command error: {e}\n{traceback.format_exc()}", "ERROR")
                return jsonify({"status": "error", "ok": False, "message": str(e)}), 500

        @app.route('/uav/<int:uid>/all', methods=['GET'])
        def get_all(uid: int):
            """获取图像和位姿 - 兼容接口"""
            if uid != self.uav_id:
                return jsonify({"error": f"UAV {uid} not found on this controller"}), 404
            try:
                pos = self._get_position()
                att = self._get_attitude()
                vel = self._get_velocity()
                angvel = self._get_angular_velocity()
                acc = self._get_acceleration()

                cam = self._get_camera()
                img_data = {}
                if cam:
                    img, ts = cam.get_last_image_with_timestamp()
                    if img is not None:
                        png_bytes, b64, w, h, c = self._png_bytes_and_b64(img)
                        img_data = {
                            "timestamp": ts,
                            "width": w,
                            "height": h,
                            "channels": c,
                            "encoding": "png_base64",
                            "mime": "image/png",
                            "data": b64,
                            "data_url": f"data:image/png;base64,{b64}",
                        }

                return jsonify({
                    "uav_id": self.uav_id,
                    "image": img_data,
                    "pose": {
                        "timestamp": get_sim_time(),
                        "position": pos.tolist(),
                        "attitude": att.tolist(),
                        "linear_velocity": vel.tolist(),
                        "angular_velocity": angvel.tolist(),
                        "linear_acceleration": acc.tolist(),
                    }
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uid>/depth.png', methods=['GET'])
        def get_depth_png(uid: int):
            """获取深度图像（16位PNG格式）"""
            if uid != self.uav_id:
                return jsonify({"error": f"UAV {uid} not found on this controller"}), 404
            try:
                cam = self._get_camera()
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                if not getattr(cam, 'depth_enabled', False):
                    return jsonify({"error": "Depth not enabled"}), 400
                depth_img, ts = cam.get_last_depth_with_timestamp()
                if depth_img is None:
                    return jsonify({"error": "No depth image cached"}), 503
                png_bytes, b64, w, h, raw_b64 = self._depth_to_png_bytes_and_b64(depth_img)
                return Response(png_bytes, status=200, mimetype='image/png', headers={'Cache-Control': 'no-cache'})
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uid>/depth', methods=['GET'])
        def get_depth(uid: int):
            """获取深度图像（JSON格式）"""
            if uid != self.uav_id:
                return jsonify({"error": f"UAV {uid} not found on this controller"}), 404
            try:
                cam = self._get_camera()
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                if not getattr(cam, 'depth_enabled', False):
                    return jsonify({"error": "Depth not enabled"}), 400
                depth_img, ts = cam.get_last_depth_with_timestamp()
                if depth_img is None:
                    return jsonify({"error": "No depth image cached"}), 503
                png_bytes, b64, w, h, raw_b64 = self._depth_to_png_bytes_and_b64(depth_img)
                return jsonify({
                    "uav_id": self.uav_id,
                    "timestamp": ts,
                    "width": w,
                    "height": h,
                    "encoding": "png16_base64",
                    "mime": "image/png",
                    "max_depth_m": 50.0,
                    "data": b64,
                    "data_url": f"data:image/png;base64,{b64}",
                    "raw_float32_base64": raw_b64,
                    "raw_dtype": "float32",
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

    def _handle_move_to(self, x: float, y: float, z: float) -> Response:
        """处理 move_to 命令 - 使用OFFBOARD模式发送位置设定点"""
        target = np.array([x, y, z])
        self.target_position = target

        ts_log(self._log_prefix, f"move_to: ({x:.2f}, {y:.2f}, {z:.2f})")

        # 确保MAVLink连接已建立
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                return jsonify({"status": "error", "ok": False, "message": "No MAVLink connection"})
            conn = self._get_mavlink_conn()

        target_sys = self.uav_id + 1  # MAV_SYS_ID = px4_instance + 1
        target_comp = 1

        try:
            # 首先发送几个setpoint才能切换到OFFBOARD模式
            ts_log(self._log_prefix, "Sending initial setpoints for OFFBOARD...")
            for _ in range(50):
                self._send_position_setpoint(x, y, z)
                time.sleep(0.02)

            # 总是发送解锁命令（每次move_to都重新解锁，确保armed状态）
            ts_log(self._log_prefix, "Sending arm command...")
            conn.mav.command_long_send(
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0,
                1, 0, 0, 0, 0, 0, 0  # arm
            )
            self.armed = True
            time.sleep(0.5)

            # 发送切换到OFFBOARD模式的命令
            ts_log(self._log_prefix, "Sending OFFBOARD mode command...")
            conn.mav.set_mode_send(
                target_sys,
                mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
                393216  # OFFBOARD mode (PX4 custom mode)
            )
            time.sleep(0.2)

        except Exception as e:
            ts_log(self._log_prefix, f"move_to setup error: {e}", "WARN")

        # 持续发送setpoint并等待到达
        timeout = 60.0
        start = time.time()
        log_interval = 5.0  # 每5秒记录一次
        last_log_time = 0

        while time.time() - start < timeout:
            # 持续发送位置设定点
            self._send_position_setpoint(x, y, z)

            current = self._get_position()
            dist = np.linalg.norm(current - target)

            # 定期记录进度
            now = time.time()
            if now - last_log_time > log_interval:
                ts_log(self._log_prefix, f"Moving to target, current=({current[0]:.2f},{current[1]:.2f},{current[2]:.2f}), dist={dist:.3f}")
                last_log_time = now

            if dist < self.reach_threshold:
                ts_log(self._log_prefix, f"Reached target, dist={dist:.3f}")
                # 继续发送一些setpoint保持位置
                for _ in range(20):
                    self._send_position_setpoint(x, y, z)
                    time.sleep(0.05)
                return jsonify({"status": "success", "ok": True, "reached": True})
            time.sleep(0.05)

        final_pos = self._get_position()
        ts_log(self._log_prefix, f"Timeout waiting for target, final=({final_pos[0]:.2f},{final_pos[1]:.2f},{final_pos[2]:.2f}), dist={np.linalg.norm(final_pos - target):.3f}")
        return jsonify({"status": "timeout", "ok": True, "reached": False})

    def _send_position_setpoint(self, x: float, y: float, z: float):
        """发送位置设定点 (OFFBOARD模式)

        坐标系转换:
        - Isaac Sim 使用 ENU: X=East, Y=North, Z=Up
        - PX4 LOCAL_NED 使用: X=North, Y=East, Z=Down
        - 转换: NED.x = ENU.y, NED.y = ENU.x, NED.z = -ENU.z
        """
        conn = self._get_mavlink_conn()
        if conn is None:
            # 尝试建立连接
            if not self._setup_mavlink_connection():
                ts_log(self._log_prefix, "No MAVLink connection for setpoint", "WARN")
                return
            conn = self._get_mavlink_conn()

        try:
            # 使用SET_POSITION_TARGET_LOCAL_NED发送位置设定点
            # type_mask: 指定哪些字段有效
            # 0x0DF8 = 0b0000_1101_1111_1000
            # 忽略速度、加速度、yaw_rate，只使用位置和yaw
            type_mask = 0x0DF8

            target_sys = self.uav_id + 1  # MAV_SYS_ID = px4_instance + 1
            target_comp = 1

            # 坐标系转换: ENU -> NED
            # NED.x = ENU.y (North = Y in ENU)
            # NED.y = ENU.x (East = X in ENU)
            # NED.z = -ENU.z (Down = -Up)
            ned_x = y    # ENU.y -> NED.x
            ned_y = x    # ENU.x -> NED.y
            ned_z = -z   # ENU.z -> NED.z (z向下为正)

            conn.mav.set_position_target_local_ned_send(
                0,  # time_boot_ms (not used)
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_FRAME_LOCAL_NED,
                type_mask,
                ned_x, ned_y, ned_z,  # 位置 (NED坐标系)
                0, 0, 0,   # 速度
                0, 0, 0,   # 加速度
                0, 0       # yaw, yaw_rate
            )
        except Exception as e:
            ts_log(self._log_prefix, f"Setpoint send error: {e}", "WARN")

    def _send_velocity_setpoint(self, x: float, y: float, z: float,
                                 vx: float, vy: float, vz: float,
                                 ax: float, ay: float, az: float,
                                 yaw: float, yaw_rate: float):
        """发送带速度和加速度前馈的PVA设定点 (OFFBOARD模式)

        参数坐标系: ENU (与Isaac Sim一致)
        - 位置: (x, y, z) - East, North, Up
        - 速度: (vx, vy, vz) - m/s
        - 加速度: (ax, ay, az) - m/s² (加速度前馈)
        - yaw: 航向角 (弧度), ENU坐标系 (East=0, CCW正)
        - yaw_rate: 航向角速率 (rad/s)

        MAVLink转换为NED:
        - NED.x = ENU.y, NED.y = ENU.x, NED.z = -ENU.z
        - 速度同理
        - 加速度同理
        - Yaw转换: NED_yaw = π/2 - ENU_yaw (ENU: East=0,CCW正 -> NED: North=0,CW正)
        - Yaw_rate转换: NED_yaw_rate = -ENU_yaw_rate (方向取反)
        """
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                ts_log(self._log_prefix, "No MAVLink connection for velocity setpoint", "WARN")
                return
            conn = self._get_mavlink_conn()

        try:
            # Type Mask for PVA control (Position + Velocity + Acceleration + Yaw + YawRate)
            # SET_POSITION_TARGET_LOCAL_NED type_mask bits:
            # Bit 0-2: 忽略位置 X/Y/Z -> 000 (使用位置)
            # Bit 3-5: 忽略速度 VX/VY/VZ -> 000 (使用速度)
            # Bit 6-8: 忽略加速度 AFX/AFY/AFZ -> 000 (使用加速度)
            # Bit 9: 力/加速度选择 -> 0 (选择加速度模式，NOT Force模式)
            # Bit 10: 忽略yaw -> 0 (使用yaw)
            # Bit 11: 忽略yaw_rate -> 0 (使用yaw_rate)
            # 二进制: 0b0000_0000_0000_0000 = 0x0000 = 0
            # 注意: Bit 9=1 表示Force模式(PX4不支持)，Bit 9=0 表示Acceleration模式
            type_mask = 0x0000  # 使用Position+Velocity+Acceleration+Yaw+YawRate，加速度模式

            target_sys = self.uav_id + 1
            target_comp = 1

            # 坐标系转换: ENU -> NED
            ned_x = y      # ENU.y -> NED.x (North)
            ned_y = x      # ENU.x -> NED.y (East)
            ned_z = -z     # ENU.z -> NED.z (Down)

            ned_vx = vy    # ENU.vy -> NED.vx
            ned_vy = vx    # ENU.vx -> NED.vy
            ned_vz = -vz   # ENU.vz -> NED.vz

            # 加速度坐标系转换: ENU -> NED (与速度相同)
            ned_ax = ay    # ENU.ay -> NED.ax
            ned_ay = ax    # ENU.ax -> NED.ay
            ned_az = -az   # ENU.az -> NED.az

            # Yaw坐标系转换: ENU -> NED
            # ENU: East=0 (X轴), CCW正 (逆时针)
            # NED: North=0 (X轴), CW正 (顺时针)
            # 转换公式: NED_yaw = π/2 - ENU_yaw
            # 这样 ENU_yaw=0 (East) -> NED_yaw=π/2 (East in NED)
            # ENU_yaw=π/2 (North) -> NED_yaw=0 (North in NED)
            ned_yaw = 0.5 * math.pi - yaw

            # Yaw_rate方向取反: ENU CCW正 -> NED CW正
            ned_yaw_rate = -yaw_rate

            conn.mav.set_position_target_local_ned_send(
                0,  # time_boot_ms
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_FRAME_LOCAL_NED,
                type_mask,
                ned_x, ned_y, ned_z,        # 位置
                ned_vx, ned_vy, ned_vz,     # 速度前馈
                ned_ax, ned_ay, ned_az,     # 加速度前馈
                ned_yaw, ned_yaw_rate       # yaw, yaw_rate
            )
        except Exception as e:
            ts_log(self._log_prefix, f"Velocity setpoint send error: {e}", "WARN")

    def _handle_move_to_many(self, points: List) -> Response:
        """处理 move_to_many 命令"""
        waypoints = []
        for point in points:
            if len(point) >= 3:
                waypoints.append({"x": point[0], "y": point[1], "z": point[2]})

        if not waypoints:
            return jsonify({"status": "error", "ok": False, "message": "No valid waypoints"})

        return self._handle_execute_mission(waypoints)

    def _handle_execute_mission(self, waypoints: List[Dict]) -> Response:
        """处理 execute_mission 命令 - 使用MAVLink Mission协议"""
        ts_log(self._log_prefix, f"execute_mission: {len(waypoints)} waypoints")

        self.mission_waypoints = waypoints
        self.current_mission_idx = 0

        try:
            # 上传任务
            self._upload_mission(waypoints)

            # 解锁
            if not self.armed:
                if not self._arm():
                    return jsonify({"status": "error", "ok": False, "message": "Failed to arm"})

            # 设置Mission模式
            if not self._set_mode("AUTO.MISSION"):
                return jsonify({"status": "error", "ok": False, "message": "Failed to set mission mode"})

            # 开始任务
            self._start_mission()

            # 等待任务完成
            timeout = 120.0 * len(waypoints)  # 每个航点最多2分钟
            start = time.time()

            while time.time() - start < timeout:
                # 检查是否到达最后一个航点
                if waypoints:
                    last_wp = waypoints[-1]
                    target = np.array([last_wp["x"], last_wp["y"], last_wp["z"]])
                    current = self._get_position()
                    dist = np.linalg.norm(current - target)
                    if dist < self.reach_threshold:
                        ts_log(self._log_prefix, "Mission completed - reached final waypoint")
                        break
                time.sleep(0.1)

            return jsonify({
                "status": "success",
                "ok": True,
                "message": "Mission completed",
                "waypoints_count": len(waypoints)
            })

        except Exception as e:
            ts_log(self._log_prefix, f"execute_mission failed: {e}", "ERROR")
            return jsonify({"status": "error", "ok": False, "message": str(e)})

    def _handle_land(self) -> Response:
        """处理 land 命令"""
        ts_log(self._log_prefix, "Landing...")

        try:
            self._set_mode("AUTO.LAND")

            # 等待降落
            timeout = 30.0
            start = time.time()
            while time.time() - start < timeout:
                pos = self._get_position()
                if pos[2] < 0.2:
                    break
                time.sleep(0.1)

            self._disarm()
            return jsonify({"status": "success", "ok": True})

        except Exception as e:
            ts_log(self._log_prefix, f"Land failed: {e}", "ERROR")
            return jsonify({"status": "error", "ok": False, "message": str(e)})

    def _upload_mission(self, waypoints: List[Dict]):
        """上传MAVLink任务"""
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                ts_log(self._log_prefix, "MAVLink not connected, skipping mission upload", "WARN")
                return
            conn = self._get_mavlink_conn()

        target_sys = self.uav_id + 1
        target_comp = 1

        try:
            # 清除现有任务
            conn.mav.mission_clear_all_send(target_sys, target_comp)
            time.sleep(0.1)

            # 发送任务数量
            conn.mav.mission_count_send(
                target_sys,
                target_comp,
                len(waypoints) + 1,  # +1 for takeoff
                mavutil.mavlink.MAV_MISSION_TYPE_MISSION
            )

            # 等待任务请求 - 使用超时避免阻塞
            for i in range(len(waypoints) + 1):
                # 非阻塞轮询，最多等5秒
                start_wait = time.time()
                msg = None
                while time.time() - start_wait < 5:
                    msg = conn.recv_match(type='MISSION_REQUEST', blocking=False)
                    if msg is not None:
                        break
                    msg = conn.recv_match(type='MISSION_REQUEST_INT', blocking=False)
                    if msg is not None:
                        break
                    time.sleep(0.01)

                if msg is None:
                    ts_log(self._log_prefix, f"Timeout waiting for MISSION_REQUEST {i}", "WARN")
                    break

                seq = msg.seq

                if seq == 0:
                    # 第一个航点：起飞 - 使用LOCAL_NED坐标系
                    current_pos = self._get_position()
                    conn.mav.mission_item_send(
                        target_sys,
                        target_comp,
                        seq,
                        mavutil.mavlink.MAV_FRAME_LOCAL_NED,
                        mavutil.mavlink.MAV_CMD_NAV_TAKEOFF,
                        0,  # current
                        1,  # autocontinue
                        0, 0, 0, 0,  # params
                        current_pos[0], current_pos[1], -2.0  # NED: z向下为正
                    )
                else:
                    # 后续航点
                    wp = waypoints[seq - 1]
                    conn.mav.mission_item_send(
                        target_sys,
                        target_comp,
                        seq,
                        mavutil.mavlink.MAV_FRAME_LOCAL_NED,
                        mavutil.mavlink.MAV_CMD_NAV_WAYPOINT,
                        0,  # current
                        1,  # autocontinue
                        0, 0, 0, 0,  # hold time, accept radius, pass radius, yaw
                        float(wp["x"]), float(wp["y"]), -float(wp["z"])  # NED坐标系
                    )

            # 等待任务确认
            start_wait = time.time()
            while time.time() - start_wait < 5:
                msg = conn.recv_match(type='MISSION_ACK', blocking=False)
                if msg is not None:
                    break
                time.sleep(0.01)

            if msg and msg.type == mavutil.mavlink.MAV_MISSION_ACCEPTED:
                ts_log(self._log_prefix, f"Mission uploaded: {len(waypoints)} waypoints")
            else:
                ts_log(self._log_prefix, f"Mission upload failed: {msg}", "WARN")

        except Exception as e:
            ts_log(self._log_prefix, f"Upload mission error: {e}", "ERROR")

    def _arm(self) -> bool:
        """解锁飞机"""
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                ts_log(self._log_prefix, "MAVLink not connected, cannot arm", "WARN")
                return False
            conn = self._get_mavlink_conn()

        target_sys = self.uav_id + 1
        target_comp = 1

        try:
            conn.mav.command_long_send(
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0,
                1, 0, 0, 0, 0, 0, 0  # arm
            )

            # 非阻塞等待确认
            start_wait = time.time()
            while time.time() - start_wait < 5:
                msg = conn.recv_match(type='COMMAND_ACK', blocking=False)
                if msg is not None and msg.result == mavutil.mavlink.MAV_RESULT_ACCEPTED:
                    self.armed = True
                    ts_log(self._log_prefix, "Armed")
                    return True
                time.sleep(0.01)

            # 超时但假设成功（某些情况下ACK可能丢失）
            self.armed = True
            ts_log(self._log_prefix, "Arm command sent (no ACK received)")
            return True

        except Exception as e:
            ts_log(self._log_prefix, f"Arm error: {e}", "ERROR")
            return False

    def _takeoff(self, altitude: float = 2.5) -> bool:
        """发送起飞命令 - MAV_CMD_NAV_TAKEOFF

        Args:
            altitude: 目标起飞高度 (ENU坐标系, 米)

        Returns:
            bool: 命令发送是否成功
        """
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                ts_log(self._log_prefix, "MAVLink not connected, cannot takeoff", "WARN")
                return False
            conn = self._get_mavlink_conn()

        target_sys = self.uav_id + 1
        target_comp = 1

        try:
            # NED坐标系: z向下为正, 所以起飞高度需要取负值
            ned_altitude = -abs(altitude)

            # MAV_CMD_NAV_TAKEOFF参数:
            # param1: Minimum pitch (if airspeed sensor present), desired pitch without sensor
            # param2: Empty
            # param3: Empty
            # param4: Yaw angle (if magnetometer present), ignored without magnetometer
            # param5: Latitude (not used for LOCAL frame)
            # param6: Longitude (not used for LOCAL frame)
            # param7: Altitude (NED)
            conn.mav.command_long_send(
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_CMD_NAV_TAKEOFF,
                0,  # confirmation
                0,  # param1: pitch
                0,  # param2: empty
                0,  # param3: empty
                float('nan'),  # param4: yaw (NaN = use current)
                0,  # param5: latitude (not used for LOCAL)
                0,  # param6: longitude (not used for LOCAL)
                ned_altitude  # param7: altitude (NED, negative = up)
            )

            # 非阻塞等待确认
            start_wait = time.time()
            while time.time() - start_wait < 5:
                msg = conn.recv_match(type='COMMAND_ACK', blocking=False)
                if msg is not None:
                    if msg.result == mavutil.mavlink.MAV_RESULT_ACCEPTED:
                        ts_log(self._log_prefix, f"Takeoff command accepted, target altitude: {altitude}m")
                        return True
                    else:
                        ts_log(self._log_prefix, f"Takeoff command rejected: result={msg.result}", "WARN")
                        # Continue trying - might be wrong ACK
                time.sleep(0.01)

            # 超时但假设成功
            ts_log(self._log_prefix, f"Takeoff command sent to {altitude}m (no ACK received)")
            return True

        except Exception as e:
            ts_log(self._log_prefix, f"Takeoff error: {e}", "ERROR")
            return False

    def _disarm(self) -> bool:
        """锁定飞机"""
        conn = self._get_mavlink_conn()
        if conn is None:
            return True

        target_sys = self.uav_id + 1
        target_comp = 1

        try:
            conn.mav.command_long_send(
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0,
                0, 0, 0, 0, 0, 0, 0  # disarm
            )

            # 非阻塞等待
            start_wait = time.time()
            while time.time() - start_wait < 5:
                msg = conn.recv_match(type='COMMAND_ACK', blocking=False)
                if msg is not None and msg.result == mavutil.mavlink.MAV_RESULT_ACCEPTED:
                    self.armed = False
                    ts_log(self._log_prefix, "Disarmed")
                    return True
                time.sleep(0.01)

            self.armed = False
            return True

        except Exception as e:
            ts_log(self._log_prefix, f"Disarm error: {e}", "ERROR")
            return False

    def _set_mode(self, mode: str) -> bool:
        """设置飞行模式"""
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                ts_log(self._log_prefix, f"MAVLink not connected, cannot set mode to {mode}", "WARN")
                return False
            conn = self._get_mavlink_conn()

        target_sys = self.uav_id + 1

        # PX4模式映射
        mode_mapping = {
            "MANUAL": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 0),
            "STABILIZED": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 65536),
            "OFFBOARD": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 393216),
            "AUTO.MISSION": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 67371008),
            "AUTO.LOITER": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 84148224),
            "AUTO.RTL": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 100925440),
            "AUTO.LAND": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 117702656),
            "AUTO.TAKEOFF": (mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, 134479872),
        }

        if mode not in mode_mapping:
            ts_log(self._log_prefix, f"Unknown mode: {mode}", "WARN")
            return False

        base_mode, custom_mode = mode_mapping[mode]

        try:
            conn.mav.set_mode_send(target_sys, base_mode, custom_mode)

            # 非阻塞等待模式切换确认
            timeout = 5.0
            start = time.time()
            while time.time() - start < timeout:
                msg = conn.recv_match(type='HEARTBEAT', blocking=False)
                if msg is not None:
                    self.mode = mode
                    ts_log(self._log_prefix, f"Mode set to {mode}")
                    return True
                time.sleep(0.01)

            # 超时但假设成功
            self.mode = mode
            ts_log(self._log_prefix, f"Mode command sent to {mode} (no confirmation)")
            return True

        except Exception as e:
            ts_log(self._log_prefix, f"Set mode error: {e}", "ERROR")
            return False

    def _start_mission(self):
        """开始任务"""
        conn = self._get_mavlink_conn()
        if conn is None:
            if not self._setup_mavlink_connection():
                return
            conn = self._get_mavlink_conn()

        target_sys = self.uav_id + 1
        target_comp = 1

        try:
            conn.mav.command_long_send(
                target_sys,
                target_comp,
                mavutil.mavlink.MAV_CMD_MISSION_START,
                0,
                0, 0, 0, 0, 0, 0, 0
            )
            ts_log(self._log_prefix, "Mission started")
        except Exception as e:
            ts_log(self._log_prefix, f"Start mission error: {e}", "ERROR")

    def _reset_vehicle(self, position: List[float], yaw_deg: float = 0.0):
        """重置车辆位置（直接执行）

        注意：可能会产生 PhysX articulation cache 警告，但不影响实际功能。
        """
        try:
            dc = self.vehicle.get_dc_interface()
            stage_prefix = f"/World/uav{self.uav_id}"
            body = dc.get_rigid_body(stage_prefix + "/body")

            from omni.isaac.dynamic_control import _dynamic_control as dc_mod
            new_pose = dc_mod.Transform()

            try:
                new_pose.p = carb.Float3(float(position[0]), float(position[1]), float(position[2]))
            except:
                new_pose.p = [float(position[0]), float(position[1]), float(position[2])]

            yaw = math.radians(float(yaw_deg))
            cy = math.cos(yaw * 0.5)
            sy = math.sin(yaw * 0.5)
            try:
                new_pose.r = carb.Float4(0.0, 0.0, sy, cy)
            except:
                new_pose.r = [0.0, 0.0, sy, cy]

            dc.set_rigid_body_pose(body, new_pose)

            # 重置速度
            try:
                zero = carb.Float3(0.0, 0.0, 0.0)
            except:
                zero = [0.0, 0.0, 0.0]
            dc.set_rigid_body_linear_velocity(body, zero)
            dc.set_rigid_body_angular_velocity(body, zero)

        except Exception as e:
            ts_log(self._log_prefix, f"Reset vehicle error: {e}", "ERROR")

    def _get_position(self) -> np.ndarray:
        """获取当前位置"""
        try:
            return np.array(self.vehicle.state.position)
        except:
            return np.zeros(3)

    def _get_attitude(self) -> np.ndarray:
        """获取当前姿态四元数"""
        try:
            return np.array(self.vehicle.state.attitude)
        except:
            return np.array([0, 0, 0, 1])

    def _get_velocity(self) -> np.ndarray:
        """获取当前线速度"""
        try:
            return np.array(self.vehicle.state.linear_velocity)
        except:
            return np.zeros(3)

    def _get_angular_velocity(self) -> np.ndarray:
        """获取当前角速度"""
        try:
            return np.array(self.vehicle.state.angular_velocity)
        except:
            return np.zeros(3)

    def _get_acceleration(self) -> np.ndarray:
        """获取当前加速度"""
        try:
            return np.array(self.vehicle.state.linear_acceleration)
        except:
            return np.zeros(3)

    def _get_camera(self):
        """获取相机传感器"""
        try:
            for s in getattr(self.vehicle, "_graphical_sensors", []):
                if getattr(s, "sensor_type", "") == "MonocularCamera":
                    return s
        except:
            pass
        return None

    def _png_bytes_and_b64(self, img):
        """转换图像为PNG和Base64"""
        arr = np.array(img)
        if arr.dtype != np.uint8:
            if np.issubdtype(arr.dtype, np.floating):
                arr = np.clip(arr, 0.0, 1.0) * 255.0
            else:
                arr = np.clip(arr, 0, 255)
            arr = arr.astype(np.uint8)
        if arr.ndim == 3 and arr.shape[2] == 1:
            arr = arr[:, :, 0]
        pil_img = Image.fromarray(arr)
        buf = BytesIO()
        pil_img.save(buf, format='PNG')
        png_bytes = buf.getvalue()
        b64 = base64.b64encode(png_bytes).decode('ascii')
        if arr.ndim == 2:
            h, w = arr.shape
            c = 1
        else:
            h, w, c = arr.shape
        return png_bytes, b64, w, h, c

    def _depth_to_png_bytes_and_b64(self, depth_img, max_depth=50.0):
        """Convert depth image (float32, meters) to 16-bit PNG.

        Args:
            depth_img: Depth image in meters (float32)
            max_depth: Maximum depth in meters for normalization

        Returns:
            (png_bytes, b64, width, height, raw_data_b64): PNG bytes, base64, dimensions, raw float32 base64
        """
        arr = np.array(depth_img)
        h, w = arr.shape[:2] if arr.ndim >= 2 else (0, 0)

        # Save raw depth data as base64 encoded float32
        raw_depth_b64 = base64.b64encode(arr.astype(np.float32).tobytes()).decode('ascii')

        # Normalize depth to 0-65535 range for 16-bit PNG
        arr = np.where(np.isfinite(arr), arr, max_depth)
        arr = np.clip(arr, 0.0, max_depth)
        arr_normalized = (arr / max_depth * 65535.0).astype(np.uint16)

        # Save as 16-bit PNG
        pil_img = Image.fromarray(arr_normalized, mode='I;16')
        buf = BytesIO()
        pil_img.save(buf, format='PNG')
        png_bytes = buf.getvalue()
        b64 = base64.b64encode(png_bytes).decode('ascii')

        return png_bytes, b64, w, h, raw_depth_b64

    def start(self, host: str = "127.0.0.1"):
        """启动HTTP服务器和MAVLink连接"""
        self._running = True

        # 启动HTTP服务器
        self._flask_server = make_server(host, self.port, self.flask_app, threaded=True)
        thread = threading.Thread(target=self._flask_server.serve_forever, daemon=True)
        thread.start()
        ts_log(self._log_prefix, f"HTTP server started at http://{host}:{self.port}/")

        # 延迟建立MAVLink连接（等待PX4启动）
        def delayed_mavlink_connect():
            time.sleep(10)  # 等待PX4启动
            self._setup_mavlink_connection()
            # 启动heartbeat发送线程
            self._start_heartbeat_thread()

        connect_thread = threading.Thread(target=delayed_mavlink_connect, daemon=True)
        connect_thread.start()

    def _start_heartbeat_thread(self):
        """启动heartbeat发送线程"""
        def heartbeat_worker():
            ts_log(self._log_prefix, "Heartbeat thread started")
            while self._running:
                conn = self._get_mavlink_conn()
                if conn is not None:
                    try:
                        # 发送GCS heartbeat
                        conn.mav.heartbeat_send(
                            mavutil.mavlink.MAV_TYPE_GCS,
                            mavutil.mavlink.MAV_AUTOPILOT_INVALID,
                            0, 0, 0
                        )
                    except Exception as e:
                        ts_log(self._log_prefix, f"Heartbeat send error: {e}", "WARN")
                time.sleep(1.0)  # 1Hz heartbeat

        self._heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True, name=f"hb_{self.uav_id}")
        self._heartbeat_thread.start()

    def stop(self):
        """停止HTTP服务器和MAVLink连接"""
        self._running = False
        if self._flask_server:
            self._flask_server.shutdown()

        # 关闭独立的MAVLink连接
        with self._mavlink_lock:
            if self._mavlink_conn is not None:
                try:
                    self._mavlink_conn.close()
                except Exception as e:
                    ts_log(self._log_prefix, f"Error closing MAVLink connection: {e}", "WARN")
                self._mavlink_conn = None


class MAVLinkMultiUAVManager:
    """
    多机管理器，使用PX4后端 + MAVLink直接控制
    """
    def __init__(self, pg: PegasusInterface, world: World, config: dict, ctrl_base_port: int = 5009):
        self.pg = pg
        self.world = world
        self.config = config
        self.ctrl_base_port = ctrl_base_port
        self.vehicles: Dict[int, Any] = {}
        self.px4_backends: Dict[int, PX4MavlinkBackend] = {}
        self.mavlink_controllers: Dict[int, MAVLinkController] = {}
        self.camera_resolution = CAMERA_RESOLUTION

    def spawn(self):
        """生成所有UAV"""
        self.pg.load_environment(USD_PATH)

        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            init_pos = v.get("initial_position", [0.0, 0.0, 0.5])
            euler = v.get("initial_orientation_euler_deg", [0.0, 0.0, 0.0])
            quat = Rotation.from_euler("XYZ", euler, degrees=True).as_quat()

            # PX4后端配置（与8_camera_vehicle.py相同）
            px4_cfg_dict = {
                "vehicle_id": vid,
                "px4_autolaunch": bool(v.get("px4_autolaunch", True)),
                "px4_dir": v.get("px4_dir", self.pg.px4_path),
                "sim_speed_factor": v.get("sim_speed_factor", 2.0),
                "px4_vehicle_model": "gazebo-classic_iris_pg",
            }
            mavlink_config = PX4MavlinkBackendConfig(px4_cfg_dict)
            px4_backend = PX4MavlinkBackend(mavlink_config)

            # 配置多旋翼
            config_multirotor = MultirotorConfig()
            if IMAGES_ENABLED:
                camera = MonocularCamera(
                    f"front_camera_{vid}",
                    config={"depth": True, "frequency": 10.0, "resolution": self.camera_resolution},
                )
                config_multirotor.graphical_sensors = [camera]
            else:
                config_multirotor.graphical_sensors = []
            config_multirotor.backends = [px4_backend]  # 使用PX4后端

            # 生成UAV
            ros2_ns = v.get("ros2_namespace", f"uav{vid}")
            prim_path = f"/World/{ros2_ns}"
            vehicle = Multirotor(
                prim_path,
                ROBOTS['Iris'],
                vid,
                init_pos,
                quat,
                config=config_multirotor,
            )

            self.vehicles[vid] = vehicle
            self.px4_backends[vid] = px4_backend

        # 重置世界
        self.world.reset()

        # 配置碰撞过滤
        if DISABLE_UAV_UAV_COLLISION:
            try:
                self._configure_collision_filtering()
            except Exception as e:
                carb.log_warn(f"Collision filtering failed: {e}")

        # 配置透明度
        if UAV_TRANSPARENCY_ENABLE:
            try:
                self._configure_uav_transparency(alpha=UAV_TRANSPARENCY_ALPHA)
            except Exception as e:
                carb.log_warn(f"Transparency setup failed: {e}")

        self.world.reset()

        # 创建MAVLink控制器
        for vid, vehicle in self.vehicles.items():
            port = self.ctrl_base_port + vid
            ctrl = MAVLinkController(vid, vehicle, self.px4_backends[vid], port)
            self.mavlink_controllers[vid] = ctrl
            ctrl.start()

    def _configure_collision_filtering(self):
        """配置碰撞过滤"""
        stage = omni.usd.get_context().get_stage()
        root = stage.GetDefaultPrim()
        if not root:
            root = stage.GetPrimAtPath("/World")
        base = root.GetPath()

        uav_group_path = base.AppendChild("UAVCollisionGroup")
        uav_group = UsdPhysics.CollisionGroup.Define(stage, uav_group_path)

        try:
            rel = uav_group.CreateFilteredGroupsRel()
            rel.SetTargets([uav_group_path])
        except Exception as e:
            carb.log_warn(f"Collision filter setup failed: {e}")

        uav_coll_api = uav_group.GetCollidersCollectionAPI()
        uav_includes = uav_coll_api.GetIncludesRel()
        uav_includes.SetTargets([])

        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("ros2_namespace", f"uav{vid}")
            prim_root = stage.GetPrimAtPath(f"/World/{ns}")
            if not prim_root or not prim_root.IsValid():
                continue
            for prim in Usd.PrimRange(prim_root):
                if prim.HasAPI(UsdPhysics.CollisionAPI) or prim.HasAPI(PhysxSchema.PhysxCollisionAPI):
                    uav_includes.AddTarget(prim.GetPath())

    def _configure_uav_transparency(self, alpha: float):
        """配置UAV透明度"""
        stage = omni.usd.get_context().get_stage()

        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("ros2_namespace", f"uav{vid}")
            prim_root = stage.GetPrimAtPath(f"/World/{ns}")
            if not prim_root or not prim_root.IsValid():
                continue

            if alpha <= 0.0:
                for p in Usd.PrimRange(prim_root):
                    try:
                        cam = UsdGeom.Camera(p)
                        if cam:
                            continue
                        img = UsdGeom.Imageable(p)
                        if img:
                            vis = img.GetVisibilityAttr()
                            if not vis:
                                vis = img.CreateVisibilityAttr()
                            vis.Set(UsdGeom.Tokens.invisible)
                    except:
                        pass


class MAVLinkSimApp:
    """
    MAVLink直接控制仿真应用主类
    """
    def __init__(self):
        self.timeline = omni.timeline.get_timeline_interface()
        self.pg = PegasusInterface()
        self._images_enabled = bool(IMAGES_ENABLED)

        if RENDER_THROTTLE:
            self.pg.set_world_settings(rendering_dt=1.0 / max(RENDER_MAX_FPS, 0.1))
        self.pg._world = World(**self.pg._world_settings)
        self.world = self.pg.world

        # 加载配置
        cfg = self._load_config(ARGS.config)

        # 创建多机管理器
        self.manager = MAVLinkMultiUAVManager(
            self.pg, self.world, cfg,
            ctrl_base_port=ARGS.ctrl_base_port
        )
        self.manager.spawn()

        self._image_buffers: Dict[int, ImageRingBuffer] = {}
        if self._images_enabled:
            for vid in self.manager.vehicles.keys():
                self._image_buffers[vid] = ImageRingBuffer(max_frames=10000, target_fps=20.0)
            ts_log("[MAVLinkSimApp]", f"Created image buffers for {len(self._image_buffers)} UAVs")

        # HTTP服务器
        self.flask_app = Flask(__name__)
        self._flask_server = None
        self._setup_routes()
        self._start_http_server(port=ARGS.sim_port)

        self._record_session_dir = ""
        self._record_last_ts_by_uav = {}
        self._csv_agg_initialized_uav = set()
        self._record_runtime_enable = bool(RECORD_ENABLE) and self._images_enabled
        if self._images_enabled:
            os.makedirs(RECORD_DIR, exist_ok=True)
            self._record_session_dir = os.path.join(RECORD_DIR, f"session_{int(time.time())}")
            os.makedirs(self._record_session_dir, exist_ok=True)

        self.stop_sim = False
        self._last_render_ts = 0.0

    def _load_config(self, path: str) -> dict:
        """加载配置文件"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config not found: {path}")
        with open(path, "r") as f:
            return json.load(f)

    def run(self):
        """主循环"""
        self.timeline.play()

        while simulation_app.is_running() and not self.stop_sim:
            # --no-images模式：禁用渲染，只使用物理引擎推进时间线
            if not self._images_enabled:
                should_render = False
            elif RENDER_THROTTLE:
                now = time.time()
                interval = 1.0 / max(RENDER_MAX_FPS, 0.1)
                if (now - self._last_render_ts) >= interval:
                    should_render = True
                    self._last_render_ts = now
                else:
                    should_render = False
            else:
                should_render = True

            self.world.step(render=should_render)

            if self._images_enabled:
                try:
                    self._capture_to_buffers()
                except Exception:
                    pass

            if self._record_runtime_enable:
                try:
                    self._record_if_due()
                except Exception as e:
                    carb.log_warn(f"Recording error: {e}")

        carb.log_warn("MAVLinkSimApp is closing.")
        self._stop_http_server()
        for ctrl in self.manager.mavlink_controllers.values():
            ctrl.stop()
        self.timeline.stop()
        simulation_app.close()

    def _start_http_server(self, host: str = "127.0.0.1", port: int = 8081):
        """启动HTTP服务器"""
        if self._flask_server is not None:
            return
        self._flask_server = make_server(host, port, self.flask_app, threaded=True)
        self.http_thread = threading.Thread(target=self._flask_server.serve_forever, daemon=True)
        self.http_thread.start()
        carb.log_info(f"MAVLinkSimApp HTTP server started at http://{host}:{port}/")

    def _stop_http_server(self):
        """停止HTTP服务器"""
        try:
            if self._flask_server:
                self._flask_server.shutdown()
                self._flask_server = None
        except Exception as e:
            carb.log_warn(f"Error stopping HTTP server: {e}")

    def _capture_to_buffers(self):
        """捕获图像到环形缓冲区（每个仿真步调用）

        此方法在主循环中调用，检查每个UAV的缓冲区是否正在录制，
        如果是则捕获当前帧。缓冲区内部会进行帧率限制。
        """
        for vid, buffer in self._image_buffers.items():
            if not buffer.is_recording():
                continue

            # 获取车辆和相机
            vehicle = self._get_vehicle(vid)
            if vehicle is None:
                continue

            cam = self._get_camera(vehicle)
            if cam is None:
                continue

            # 获取图像和位姿快照
            img, ts_img = cam.get_last_image_with_timestamp()
            st_snap = cam.get_last_state_snapshot()

            if img is None or st_snap is None or ts_img is None:
                continue

            # 转换图像为Base64 PNG
            try:
                _, b64, w, h, _ = self._png_bytes_and_b64(img)
            except Exception:
                continue

            # 构建位姿数据
            pose_data = {
                "position": st_snap["position"].tolist(),
                "attitude": st_snap["attitude"].tolist(),
                "linear_velocity": st_snap["linear_velocity"].tolist(),
                "angular_velocity": st_snap["angular_velocity"].tolist(),
                "linear_acceleration": st_snap["linear_acceleration"].tolist(),
            }

            # 添加到缓冲区（内部进行帧率限制）
            buffer.add_frame(ts_img, pose_data, b64, w, h)

    def _setup_routes(self):
        """设置HTTP路由"""
        app = self.flask_app

        @app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "healthy", "type": "mavlink_sim"})

        @app.route('/sim_time', methods=['GET'])
        def sim_time():
            """获取当前模拟器时间（秒）"""
            return jsonify({
                "sim_time": get_sim_time(),
                "wall_time": time.time()
            })

        @app.route('/record/<cmd>', methods=['GET'])
        def record_cmd(cmd: str):
            if not self._images_enabled:
                if cmd == "status":
                    return jsonify({"recording": False})
                return jsonify({"error": "images_disabled"}), 404
            if cmd == 'start':
                self._record_runtime_enable = True
                return jsonify({"recording": True})
            if cmd == 'stop':
                self._record_runtime_enable = False
                return jsonify({"recording": False})
            if cmd == 'status':
                return jsonify({"recording": self._record_runtime_enable})
            return jsonify({"error": "Invalid path"}), 404

        @app.route('/uav/<int:uav_id>/pose', methods=['GET'])
        def pose(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"error": f"Vehicle uav{uav_id} not found"}), 404
                st = vehicle.state
                return jsonify({
                    "uav_id": uav_id,
                    "timestamp": get_sim_time(),
                    "position": st.position.tolist(),
                    "attitude": st.attitude.tolist(),
                    "linear_velocity": st.linear_velocity.tolist(),
                    "angular_velocity": st.angular_velocity.tolist(),
                    "linear_acceleration": st.linear_acceleration.tolist(),
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/image.png', methods=['GET'])
        def image_png(uav_id: int):
            try:
                if not self._images_enabled:
                    return jsonify({"error": "images_disabled"}), 404
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"error": f"Vehicle uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                img, ts = cam.get_last_image_with_timestamp()
                if img is None:
                    return jsonify({"error": "No image cached"}), 503
                png_bytes, _, _, _, _ = self._png_bytes_and_b64(img)
                return Response(png_bytes, status=200, mimetype='image/png')
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/image', methods=['GET'])
        def image(uav_id: int):
            try:
                if not self._images_enabled:
                    return jsonify({"error": "images_disabled"}), 404
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"error": f"Vehicle uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                img, ts = cam.get_last_image_with_timestamp()
                if img is None:
                    return jsonify({"error": "No image cached"}), 503
                png_bytes, b64, w, h, c = self._png_bytes_and_b64(img)
                return jsonify({
                    "uav_id": uav_id,
                    "timestamp": ts,
                    "width": w,
                    "height": h,
                    "channels": c,
                    "encoding": "png_base64",
                    "mime": "image/png",
                    "data": b64,
                    "data_url": f"data:image/png;base64,{b64}",
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/all', methods=['GET'])
        def all_info(uav_id: int):
            try:
                if not self._images_enabled:
                    return jsonify({"error": "images_disabled"}), 404
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"error": f"Vehicle uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                img, ts_img = cam.get_last_image_with_timestamp()
                if img is None:
                    return jsonify({"error": "No image cached"}), 503
                st_snap = cam.get_last_state_snapshot()
                if st_snap is None:
                    return jsonify({"error": "No pose snapshot cached"}), 503
                png_bytes, b64, w, h, c = self._png_bytes_and_b64(img)
                return jsonify({
                    "uav_id": uav_id,
                    "image": {
                        "timestamp": ts_img,
                        "width": w,
                        "height": h,
                        "channels": c,
                        "encoding": "png_base64",
                        "mime": "image/png",
                        "data": b64,
                        "data_url": f"data:image/png;base64,{b64}",
                    },
                    "pose": {
                        "timestamp": ts_img,
                        "position": st_snap["position"].tolist(),
                        "attitude": st_snap["attitude"].tolist(),
                        "linear_velocity": st_snap["linear_velocity"].tolist(),
                        "angular_velocity": st_snap["angular_velocity"].tolist(),
                        "linear_acceleration": st_snap["linear_acceleration"].tolist(),
                    }
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/depth.png', methods=['GET'])
        def depth_png(uav_id: int):
            """获取深度图像（16位PNG格式）"""
            try:
                if not self._images_enabled:
                    return jsonify({"error": "images_disabled"}), 404
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"error": f"Vehicle uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                if not getattr(cam, 'depth_enabled', False):
                    return jsonify({"error": "Depth not enabled"}), 400
                depth_img, ts = cam.get_last_depth_with_timestamp()
                if depth_img is None:
                    return jsonify({"error": "No depth image cached"}), 503
                png_bytes, b64, w, h, raw_b64 = self._depth_to_png_bytes_and_b64(depth_img)
                return Response(png_bytes, status=200, mimetype='image/png', headers={'Cache-Control': 'no-cache'})
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/depth', methods=['GET'])
        def depth(uav_id: int):
            """获取深度图像（JSON格式）"""
            try:
                if not self._images_enabled:
                    return jsonify({"error": "images_disabled"}), 404
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"error": f"Vehicle uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    return jsonify({"error": "Camera not found"}), 404
                if not getattr(cam, 'depth_enabled', False):
                    return jsonify({"error": "Depth not enabled"}), 400
                depth_img, ts = cam.get_last_depth_with_timestamp()
                if depth_img is None:
                    return jsonify({"error": "No depth image cached"}), 503
                png_bytes, b64, w, h, raw_b64 = self._depth_to_png_bytes_and_b64(depth_img)
                return jsonify({
                    "uav_id": uav_id,
                    "timestamp": ts,
                    "width": w,
                    "height": h,
                    "encoding": "png16_base64",
                    "mime": "image/png",
                    "max_depth_m": 50.0,
                    "data": b64,
                    "data_url": f"data:image/png;base64,{b64}",
                    "raw_float32_base64": raw_b64,
                    "raw_dtype": "float32",
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/reset', methods=['POST'])
        def reset_uav(uav_id: int):
            try:
                data = request.json or {}
                pos = data.get("position") or data.get("pos") or []
                yaw_deg = data.get("yaw_deg", 0.0)
                if not isinstance(pos, (list, tuple)) or len(pos) < 3:
                    return jsonify({"error": "position must be [x,y,z]"}), 400

                ok, msg = self._reset_uav(uav_id, pos, yaw_deg)
                if not ok:
                    return jsonify({"status": "error", "message": msg}), 500
                return jsonify({"status": "success", "uav_id": uav_id, "position": pos})
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.route('/uav/<int:uav_id>/px4/ready', methods=['GET'])
        def px4_ready(uav_id: int):
            """查询PX4就绪状态"""
            vehicle = self._get_vehicle(uav_id)
            if vehicle is None:
                return jsonify({"status": "error", "ready": False}), 404

            backend = self.manager.px4_backends.get(uav_id)
            if backend is None:
                return jsonify({"status": "error", "ready": False}), 404

            ready = backend.px4_ready_to_takeoff
            return jsonify({"status": "success", "uav_id": uav_id, "ready": ready})

        @app.route('/uav/<int:uav_id>/px4/status', methods=['GET'])
        def px4_status(uav_id: int):
            """查询PX4详细状态"""
            vehicle = self._get_vehicle(uav_id)
            if vehicle is None:
                return jsonify({"status": "error"}), 404

            backend = self.manager.px4_backends.get(uav_id)
            if backend is None:
                return jsonify({"status": "error"}), 404

            return jsonify({
                "status": "success",
                "px4_backend": {
                    "uav_id": uav_id,
                    "is_running": getattr(backend, "_is_running", False),
                    "mavlink_connected": backend._connection is not None,
                    "px4_ready_to_takeoff": backend.px4_ready_to_takeoff,
                    "received_first_actuator": getattr(backend, "_received_first_actuator", False),
                    "received_first_heartbeat": getattr(backend, "_first_heartbeat_received", False),
                    "type": "PX4MavlinkBackend",
                }
            })

        @app.route('/uav/<int:uav_id>/px4/recover', methods=['POST'])
        def px4_recover(uav_id: int):
            """重启PX4进程（硬重置）

            使用后端的 recover_px4() 方法进行完整的恢复序列，包括：
            - 正确的状态重置（heartbeat标志、时间戳等）
            - MAVLink TCP服务器必须在PX4启动之前创建
            - 处理物理循环阻塞期间的大dt值
            """
            vehicle = self._get_vehicle(uav_id)
            if vehicle is None:
                return jsonify({"status": "error", "message": "UAV not found"}), 404

            backend = self.manager.px4_backends.get(uav_id)
            if backend is None:
                return jsonify({"status": "error", "message": "Backend not found"}), 404

            try:
                ts_log(f"[UAV{uav_id}]", "PX4 recover: calling backend.recover_px4()...")

                # 使用后端的完整恢复方法
                backend.recover_px4()

                # 等待PX4就绪（需要一些时间来初始化EKF2）
                ts_log(f"[UAV{uav_id}]", "PX4 recover: waiting for PX4 ready...")
                ready_timeout = 30.0
                ready_start = time.time()
                while time.time() - ready_start < ready_timeout:
                    if backend.px4_ready_to_takeoff:
                        ts_log(f"[UAV{uav_id}]", "PX4 recover: PX4 is ready!")
                        break
                    # 检查连接状态
                    if backend._received_first_hearbeat:
                        ts_log(f"[UAV{uav_id}]", "PX4 recover: heartbeat received, continuing wait for ready_to_takeoff...")
                    time.sleep(0.5)

                ts_log(f"[UAV{uav_id}]", f"PX4 recover: completed, ready={backend.px4_ready_to_takeoff}")
                return jsonify({
                    "status": "success",
                    "uav_id": uav_id,
                    "ready": backend.px4_ready_to_takeoff,
                    "heartbeat_received": backend._received_first_hearbeat
                })

            except Exception as e:
                ts_log(f"[UAV{uav_id}]", f"PX4 recover error: {e}", "ERROR")
                import traceback
                ts_log(f"[UAV{uav_id}]", traceback.format_exc(), "ERROR")
                return jsonify({"status": "error", "message": str(e)}), 500

        # ============================================
        # 图像环形缓冲区端点（异步采集用）
        # ============================================

        @app.route('/uav/<int:uav_id>/buffer/start', methods=['POST'])
        def buffer_start(uav_id: int):
            """开始图像缓冲区录制

            POST /uav/<id>/buffer/start

            请求体（可选）:
            {
                "save_dir": "/path/to/save"  // 可选，指定后图像保存到磁盘
            }

            开始在仿真端缓存图像+位姿数据，无需等待HTTP传输。
            采集器可以专注于发送控制命令，最后一次性获取所有数据。

            如果指定save_dir，图像将直接保存到磁盘，大幅减少内存占用。
            """
            try:
                if not self._images_enabled:
                    return jsonify({"status": "error", "message": "images_disabled"}), 404
                buffer = self._image_buffers.get(uav_id)
                if buffer is None:
                    return jsonify({"status": "error", "message": f"UAV {uav_id} buffer not found"}), 404

                # 获取可选的save_dir参数
                data = request.json or {}
                save_dir = data.get("save_dir", None)

                buffer.start_recording(save_dir=save_dir)
                ts_log(f"[UAV{uav_id}]", f"Image buffer recording started (save_dir={save_dir})")
                return jsonify({
                    "status": "success",
                    "uav_id": uav_id,
                    "recording": True,
                    "save_to_disk": save_dir is not None,
                    "save_dir": save_dir,
                    "message": "Buffer recording started"
                })
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

        @app.route('/uav/<int:uav_id>/buffer/stop', methods=['POST'])
        def buffer_stop(uav_id: int):
            """停止图像缓冲区录制并返回所有帧

            POST /uav/<id>/buffer/stop

            返回格式:
            {
                "status": "success",
                "uav_id": <id>,
                "frame_count": <n>,
                "frames": [
                    {
                        "timestamp": <sim_time>,
                        "pose": {position, attitude, linear_velocity, ...},
                        "image_b64": <base64_png>,
                        "width": <w>,
                        "height": <h>
                    },
                    ...
                ]
            }
            """
            try:
                if not self._images_enabled:
                    return jsonify({"status": "error", "message": "images_disabled"}), 404
                buffer = self._image_buffers.get(uav_id)
                if buffer is None:
                    return jsonify({"status": "error", "message": f"UAV {uav_id} buffer not found"}), 404

                frames = buffer.stop_recording()
                ts_log(f"[UAV{uav_id}]", f"Image buffer recording stopped, {len(frames)} frames collected")

                return jsonify({
                    "status": "success",
                    "uav_id": uav_id,
                    "frame_count": len(frames),
                    "frames": frames
                })
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

        @app.route('/uav/<int:uav_id>/buffer/status', methods=['GET'])
        def buffer_status(uav_id: int):
            """查询缓冲区状态

            GET /uav/<id>/buffer/status

            返回录制状态、当前帧数、录制时长等信息。
            """
            try:
                if not self._images_enabled:
                    return jsonify({"status": "error", "message": "images_disabled"}), 404
                buffer = self._image_buffers.get(uav_id)
                if buffer is None:
                    return jsonify({"status": "error", "message": f"UAV {uav_id} buffer not found"}), 404

                return jsonify({
                    "status": "success",
                    "uav_id": uav_id,
                    "recording": buffer.is_recording(),
                    "frame_count": buffer.get_frame_count(),
                    "duration_s": buffer.get_recording_duration(),
                    "target_fps": buffer.target_fps,
                    "max_frames": buffer.max_frames
                })
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

    def _get_vehicle(self, uav_id: int):
        """获取车辆"""
        return self.manager.vehicles.get(uav_id)

    def _get_camera(self, vehicle):
        """获取相机"""
        if not self._images_enabled:
            return None
        try:
            for s in getattr(vehicle, "_graphical_sensors", []):
                if getattr(s, "sensor_type", "") == "MonocularCamera":
                    return s
        except:
            pass
        return None

    def _reset_uav(self, uav_id: int, position: list, yaw_deg: float = 0.0):
        """重置UAV（直接执行）

        注意：可能会产生 PhysX articulation cache 警告，但不影响实际功能。
        """
        try:
            vehicle = self._get_vehicle(uav_id)
            if vehicle is None:
                return False, f"Vehicle uav{uav_id} not found"

            dc = vehicle.get_dc_interface()
            stage_prefix = f"/World/uav{uav_id}"
            body = dc.get_rigid_body(stage_prefix + "/body")

            from omni.isaac.dynamic_control import _dynamic_control as dc_mod
            new_pose = dc_mod.Transform()

            try:
                new_pose.p = carb.Float3(float(position[0]), float(position[1]), float(position[2]))
            except:
                new_pose.p = [float(position[0]), float(position[1]), float(position[2])]

            yaw = math.radians(float(yaw_deg))
            cy = math.cos(yaw * 0.5)
            sy = math.sin(yaw * 0.5)
            try:
                new_pose.r = carb.Float4(0.0, 0.0, sy, cy)
            except:
                new_pose.r = [0.0, 0.0, sy, cy]

            dc.set_rigid_body_pose(body, new_pose)

            try:
                zero = carb.Float3(0.0, 0.0, 0.0)
            except:
                zero = [0.0, 0.0, 0.0]
            dc.set_rigid_body_linear_velocity(body, zero)
            dc.set_rigid_body_angular_velocity(body, zero)

            return True, "ok"
        except Exception as e:
            return False, str(e)

    def _png_bytes_and_b64(self, img):
        """图像转PNG和Base64"""
        arr = np.array(img)
        if arr.dtype != np.uint8:
            if np.issubdtype(arr.dtype, np.floating):
                arr = np.clip(arr, 0.0, 1.0) * 255.0
            else:
                arr = np.clip(arr, 0, 255)
            arr = arr.astype(np.uint8)
        if arr.ndim == 3 and arr.shape[2] == 1:
            arr = arr[:, :, 0]
        pil_img = Image.fromarray(arr)
        buf = BytesIO()
        pil_img.save(buf, format='PNG')
        png_bytes = buf.getvalue()
        b64 = base64.b64encode(png_bytes).decode('ascii')
        if arr.ndim == 2:
            h, w = arr.shape
            c = 1
        else:
            h, w, c = arr.shape
        return png_bytes, b64, w, h, c

    def _depth_to_png_bytes_and_b64(self, depth_img, max_depth=50.0):
        """Convert depth image (float32, meters) to 16-bit PNG.

        Args:
            depth_img: Depth image in meters (float32)
            max_depth: Maximum depth in meters for normalization

        Returns:
            (png_bytes, b64, width, height, raw_data_b64): PNG bytes, base64, dimensions, raw float32 base64
        """
        arr = np.array(depth_img)
        h, w = arr.shape[:2] if arr.ndim >= 2 else (0, 0)

        # Save raw depth data as base64 encoded float32
        raw_depth_b64 = base64.b64encode(arr.astype(np.float32).tobytes()).decode('ascii')

        # Normalize depth to 0-65535 range for 16-bit PNG
        arr = np.where(np.isfinite(arr), arr, max_depth)
        arr = np.clip(arr, 0.0, max_depth)
        arr_normalized = (arr / max_depth * 65535.0).astype(np.uint16)

        # Save as 16-bit PNG
        pil_img = Image.fromarray(arr_normalized, mode='I;16')
        buf = BytesIO()
        pil_img.save(buf, format='PNG')
        png_bytes = buf.getvalue()
        b64 = base64.b64encode(png_bytes).decode('ascii')

        return png_bytes, b64, w, h, raw_depth_b64

    def _record_if_due(self):
        """按帧率录制"""
        period = 1.0 / max(RECORD_FPS, 0.1)

        for vid, vehicle in self.manager.vehicles.items():
            cam = self._get_camera(vehicle)
            if cam is None:
                continue
            img, ts_img = cam.get_last_image_with_timestamp()
            st_snap = cam.get_last_state_snapshot()
            if img is None or st_snap is None or ts_img is None:
                continue

            last_ts = self._record_last_ts_by_uav.get(vid)
            if last_ts is not None and (ts_img - last_ts) < period:
                continue

            # 保存PNG
            ts_ms = int(ts_img * 1000)
            png_path = os.path.join(self._record_session_dir, f"uav{vid}_{ts_ms}.png")
            try:
                img8 = img.astype('uint8')
                pil_img = Image.fromarray(img8)
                pil_img.save(png_path, format='PNG')
            except Exception as e:
                carb.log_warn(f"Save PNG error: {e}")

            # 保存CSV
            csv_path = os.path.join(self._record_session_dir, f"uav{vid}.csv")
            try:
                h, w, c = img.shape
                if vid not in self._csv_agg_initialized_uav:
                    with open(csv_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            "timestamp_ms", "timestamp_s", "uav_id", "image_filename",
                            "image_width", "image_height", "image_channels",
                            "pos_x", "pos_y", "pos_z",
                            "att_w", "att_x", "att_y", "att_z",
                            "linvel_x", "linvel_y", "linvel_z",
                            "angvel_x", "angvel_y", "angvel_z",
                            "linacc_x", "linacc_y", "linacc_z",
                        ])
                    self._csv_agg_initialized_uav.add(vid)

                with open(csv_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        ts_ms, ts_img, vid, os.path.basename(png_path),
                        w, h, c,
                        float(st_snap['position'][0]),
                        float(st_snap['position'][1]),
                        float(st_snap['position'][2]),
                        float(st_snap['attitude'][3]),
                        float(st_snap['attitude'][0]),
                        float(st_snap['attitude'][1]),
                        float(st_snap['attitude'][2]),
                        float(st_snap['linear_velocity'][0]),
                        float(st_snap['linear_velocity'][1]),
                        float(st_snap['linear_velocity'][2]),
                        float(st_snap['angular_velocity'][0]),
                        float(st_snap['angular_velocity'][1]),
                        float(st_snap['angular_velocity'][2]),
                        float(st_snap['linear_acceleration'][0]),
                        float(st_snap['linear_acceleration'][1]),
                        float(st_snap['linear_acceleration'][2]),
                    ])
            except Exception as e:
                carb.log_warn(f"CSV save error: {e}")

            self._record_last_ts_by_uav[vid] = ts_img


def main():
    app = MAVLinkSimApp()
    app.run()


if __name__ == "__main__":
    main()
