#!/usr/bin/env python
"""
纯Python仿真环境（fast_sim_vehicle.py）

==========================
概述
==========================
本脚本是基于 FastController 的纯 Python 仿真环境，特点：
- 不使用 PX4-Autopilot 或 MAVROS
- 使用纯 Python 实现的 FastController 进行姿态控制
- 提供与 8_camera_vehicle.py 完全兼容的 HTTP 接口
- 支持多架无人机并行仿真
- 更快的启动速度和更低的资源消耗

==========================
与现有系统的区别
==========================
| 特性           | 8_camera_vehicle.py          | fast_sim_vehicle.py      |
|----------------|------------------------------|--------------------------|
| 控制后端       | PX4 SITL + MAVLink           | FastController (纯Python)|
| 依赖           | PX4-Autopilot, MAVROS        | 无外部依赖               |
| 启动时间       | ~30s (含PX4启动)             | ~5s                      |
| 资源消耗       | 高 (多进程)                  | 低 (单进程)              |
| HTTP接口       | 端口 8081                    | 端口 8081 (兼容)         |
| 控制接口       | 需要 rospy_isaacsim.py       | 内置控制服务             |

==========================
HTTP 接口（端口 8081，与原系统完全兼容）
==========================
GET  /uav/<id>/pose           - 获取位姿
GET  /uav/<id>/image          - 获取图像 (JSON + Base64)
GET  /uav/<id>/image.png      - 获取图像 (PNG二进制)
GET  /uav/<id>/all            - 获取图像+位姿同步快照
POST /uav/<id>/reset          - 重置UAV位置
GET  /uav/<id>/px4/ready      - 查询就绪状态 (始终返回 ready=True)
GET  /health                  - 健康检查

控制接口（端口 5009+id，与 rospy_isaacsim.py 兼容）：
POST /reset                   - 重置UAV
POST /command                 - 发送控制命令
     {"cmd": "move_to", "x": X, "y": Y, "z": Z}
     {"cmd": "move_to_many", "points": [[x,y,z], ...]}
     {"cmd": "land"}
     {"cmd": "get_position"}
     {"cmd": "get_status"}
GET  /health                  - 健康检查

==========================
使用方法
==========================
# 启动仿真（与 8_camera_vehicle.py 相同的方式）
ISAACSIM_PYTHON examples/fast_sim_vehicle.py

# 使用配置文件
ISAACSIM_PYTHON examples/fast_sim_vehicle.py --config examples/multi_uav_config.json

# 加载轨迹自动执行
ISAACSIM_PYTHON examples/fast_sim_vehicle.py --trajectory /path/to/traj.json

==========================
命令行参数
==========================
--config PATH      UAV配置文件路径（默认：multi_uav_config.json）
--trajectory PATH  轨迹文件路径（可选，用于自动执行轨迹）
--headless         无头模式运行
--sim-port PORT    仿真HTTP端口（默认：8081）
--ctrl-base-port   控制器HTTP端口基础（默认：5009）

"""

# Imports to start Isaac Sim from this script
import carb
from isaacsim import SimulationApp

import sys
import os
import argparse
import logging
import json
import traceback
import threading
import time
import base64
import csv
import math
import numpy as np
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from PIL import Image
from flask import Flask, jsonify, request, Response
from werkzeug.serving import make_server
from scipy.spatial.transform import Rotation

# -------------------------
# Configuration
# -------------------------
USE_RASTERIZATION = True
RENDER_THROTTLE = True
RENDER_MAX_FPS = 10.0
CAMERA_RESOLUTION = (640, 640)

SIMULATION_ENVIRONMENTS = {
    "Flat Plane": "https://omniverse-content-production.s3-us-west-2.amazonaws.com/Assets/Isaac/4.5/Isaac/Environments/Terrains/flat_plane.usd",
}
USD_PATH = SIMULATION_ENVIRONMENTS['Flat Plane']

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "multi_uav_config.json")

# 录制设置
RECORD_ENABLE = False
RECORD_FPS = 10.0
RECORD_DIR = os.path.join(os.path.dirname(__file__), "recordings")

# UAV透明度
UAV_TRANSPARENCY_ENABLE = True
UAV_TRANSPARENCY_ALPHA = 0.0
DISABLE_UAV_UAV_COLLISION = True

# -------------------------
# Parse command line args before SimulationApp
# -------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Fast Simulation Vehicle (Pure Python)")
    parser.add_argument("--config", type=str, default=CONFIG_PATH, help="UAV config JSON path")
    parser.add_argument("--trajectory", type=str, default=None, help="Trajectory JSON path")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--sim-port", type=int, default=8081, help="Simulation HTTP port")
    parser.add_argument("--ctrl-base-port", type=int, default=5009, help="Controller HTTP base port")
    parser.add_argument("--scale", type=float, default=0.01, help="Coordinate scale")
    parser.add_argument("--z-down", action="store_true", default=True, help="Z-axis down")
    return parser.parse_args()

ARGS = parse_args()

APP_CONFIG = {
    "window_width": 1280,
    "window_height": 720,
    "headless": ARGS.headless,
    "max_bounces": 0 if USE_RASTERIZATION else 1,
    "samples_per_pixel_per_frame": 1 if USE_RASTERIZATION else 16,
    "anti_aliasing": 1,
    "renderer": "Rasterization" if USE_RASTERIZATION else "RayTracedLighting",
    "extra_args": [
        "--/rtx/post/dlss/execMode=0",
        "--exts.\"isaacsim.core.throttling\".enable_async=true",
        "--exts.\"omni.isaac.throttling\".enable_async=true",
        "--mdl-disk-cache=true",
        "--mdl-disk-cache-path=/home/user/.cache/omni/mdl_cache",
    ],
}

simulation_app = SimulationApp(APP_CONFIG)

# -----------------------------------
# Imports after SimulationApp
# -----------------------------------
import omni.timeline
import omni.usd
from pxr import Usd, UsdGeom, UsdPhysics, PhysxSchema, Sdf, Vt, UsdShade
from isaacsim.core.api.world import World

sys.path.insert(0, os.path.expanduser("~/PegasusSimulator-5.1/extensions/pegasus.simulator/"))

from pegasus.simulator.params import ROBOTS
from pegasus.simulator.logic.graphical_sensors.monocular_camera import MonocularCamera
from pegasus.simulator.logic.vehicles.multirotor import Multirotor, MultirotorConfig
from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface
from pegasus.simulator.logic.state import State

# Import FastController
sys.path.insert(0, os.path.dirname(__file__))
from utils.fast_controller import FastController, TrajectoryPoint


@dataclass
class TrajPoint:
    """轨迹点数据类，与 trajectory_data_collector.py 兼容"""
    x: float
    y: float
    z: float
    roll_deg: float
    yaw_deg: float
    pitch_deg: float


class VirtualController:
    """
    虚拟控制器，模拟 rospy_isaacsim.py 的功能
    提供与 rospy_isaacsim.py 完全兼容的 HTTP 接口
    """
    def __init__(self, uav_id: int, vehicle, fast_controller: FastController, port: int):
        self.uav_id = uav_id
        self.vehicle = vehicle
        self.fast_controller = fast_controller
        self.port = port

        # 状态
        self.mode = "OFFBOARD"
        self.armed = True
        self.ready = True

        # 目标位置
        self.target_position: Optional[np.ndarray] = None
        self.hover_target: Optional[np.ndarray] = None

        # 航点队列
        self.waypoint_queue: List[np.ndarray] = []
        self.current_waypoint_idx = 0

        # 到达阈值
        self.reach_threshold = 0.5  # meters

        # 任务锁
        self._task_lock = threading.Lock()
        self._busy = False

        # Flask app
        self.flask_app = Flask(f"controller_{uav_id}")
        self._flask_server = None
        self._setup_routes()

    def _setup_routes(self):
        app = self.flask_app

        @app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "healthy", "env_id": f"uav{self.uav_id}:{self.port}"})

        @app.route('/reset', methods=['POST'])
        def reset():
            try:
                data = request.json or {}
                position = data.get("position")
                yaw_deg = data.get("yaw_deg", 0.0)
                hard = data.get("hard", True)
                force = data.get("force", False)

                if position and len(position) >= 3:
                    # 重置UAV位置
                    self._reset_vehicle(position, yaw_deg)

                    # 设置悬停目标
                    self.hover_target = np.array(position)
                    self.target_position = np.array(position)

                self.mode = "OFFBOARD"
                self.armed = True
                self.ready = True

                return jsonify({
                    "status": "success",
                    "position": position if position else self._get_position().tolist()
                })
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

        @app.route('/command', methods=['POST'])
        def command():
            try:
                data = request.json or {}
                cmd = data.get("cmd", "")
                force = data.get("force", False)

                # 检查锁
                if not force and self._busy:
                    return jsonify({"status": "busy", "ok": False}), 409

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

                elif cmd == "set_px4_ready":
                    # 兼容接口，始终就绪
                    self.ready = data.get("ready", True)
                    return jsonify({"status": "success", "ok": True})

                elif cmd == "get_mission_status":
                    return jsonify({
                        "status": "success",
                        "ok": True,
                        "current_waypoint": self.current_waypoint_idx,
                        "total_waypoints": len(self.waypoint_queue),
                        "mission_state": "completed" if self.current_waypoint_idx >= len(self.waypoint_queue) else "active"
                    })

                else:
                    return jsonify({"status": "error", "ok": False, "message": f"Unknown command: {cmd}"}), 400

            except Exception as e:
                carb.log_warn(f"Controller command error: {e}\n{traceback.format_exc()}")
                return jsonify({"status": "error", "ok": False, "message": str(e)}), 500

        @app.route('/uav/<int:uid>/all', methods=['GET'])
        def get_all(uid: int):
            """获取图像和位姿 - 兼容接口"""
            if uid != self.uav_id:
                return jsonify({"error": f"UAV {uid} not found on this controller"}), 404
            try:
                # 获取位姿
                pos = self._get_position()
                att = self._get_attitude()
                vel = self._get_velocity()
                angvel = self._get_angular_velocity()
                acc = self._get_acceleration()

                # 获取图像
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
                        "timestamp": time.time(),
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
        """处理 move_to 命令"""
        target = np.array([x, y, z])
        self.target_position = target

        # 更新FastController的目标
        if self.fast_controller and hasattr(self.fast_controller, 'trajectory'):
            # 使用简单的悬停目标
            pass

        # 等待到达
        timeout = 60.0
        start = time.time()
        while time.time() - start < timeout:
            current = self._get_position()
            dist = np.linalg.norm(current - target)
            if dist < self.reach_threshold:
                return jsonify({"status": "success", "ok": True, "reached": True})
            time.sleep(0.1)

        return jsonify({"status": "timeout", "ok": True, "reached": False})

    def _handle_move_to_many(self, points: List) -> Response:
        """处理 move_to_many 命令"""
        for point in points:
            if len(point) >= 3:
                resp = self._handle_move_to(point[0], point[1], point[2])
        return jsonify({"status": "success", "ok": True})

    def _handle_execute_mission(self, waypoints: List) -> Response:
        """处理 execute_mission 命令"""
        self.waypoint_queue = [np.array([wp["x"], wp["y"], wp["z"]]) for wp in waypoints]
        self.current_waypoint_idx = 0

        for i, wp in enumerate(self.waypoint_queue):
            self.current_waypoint_idx = i
            self.target_position = wp

            timeout = 60.0
            start = time.time()
            while time.time() - start < timeout:
                current = self._get_position()
                dist = np.linalg.norm(current - wp)
                if dist < self.reach_threshold:
                    break
                time.sleep(0.1)

        self.current_waypoint_idx = len(self.waypoint_queue)
        return jsonify({"status": "success", "ok": True, "message": "Mission completed"})

    def _handle_land(self) -> Response:
        """处理 land 命令"""
        current = self._get_position()
        # 逐渐下降
        target_z = 0.1
        self.target_position = np.array([current[0], current[1], target_z])

        timeout = 30.0
        start = time.time()
        while time.time() - start < timeout:
            pos = self._get_position()
            if pos[2] < 0.2:
                break
            time.sleep(0.1)

        self.armed = False
        return jsonify({"status": "success", "ok": True})

    def _reset_vehicle(self, position: List[float], yaw_deg: float = 0.0):
        """重置车辆位置"""
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
            carb.log_warn(f"Reset vehicle error: {e}")

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
        """启动HTTP服务器"""
        self._flask_server = make_server(host, self.port, self.flask_app, threaded=True)
        thread = threading.Thread(target=self._flask_server.serve_forever, daemon=True)
        thread.start()
        carb.log_info(f"VirtualController for UAV{self.uav_id} started at http://{host}:{self.port}/")

    def stop(self):
        """停止HTTP服务器"""
        if self._flask_server:
            self._flask_server.shutdown()


class FastMultiUAVManager:
    """
    多机管理器，使用 FastController 替代 PX4
    """
    def __init__(self, pg: PegasusInterface, world: World, config: dict, ctrl_base_port: int = 5009):
        self.pg = pg
        self.world = world
        self.config = config
        self.ctrl_base_port = ctrl_base_port
        self.vehicles: Dict[int, Any] = {}
        self.controllers: Dict[int, FastController] = {}
        self.virtual_controllers: Dict[int, VirtualController] = {}
        self.camera_resolution = CAMERA_RESOLUTION

    def spawn(self):
        """生成所有UAV"""
        self.pg.load_environment(USD_PATH)

        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            init_pos = v.get("initial_position", [0.0, 0.0, 0.5])
            euler = v.get("initial_orientation_euler_deg", [0.0, 0.0, 0.0])
            quat = Rotation.from_euler("XYZ", euler, degrees=True).as_quat()

            # 创建FastController
            fast_ctrl = FastController(
                trajectory_file=None,
                results_file=None,
                scale=ARGS.scale,
                z_down=ARGS.z_down,
                uav_id=vid,
            )

            # 配置相机
            camera = MonocularCamera(
                f"front_camera_{vid}",
                config={"depth": True, "frequency": 10.0, "resolution": self.camera_resolution}
            )

            # 配置多旋翼
            config_multirotor = MultirotorConfig()
            config_multirotor.graphical_sensors = [camera]
            config_multirotor.backends = [fast_ctrl]  # 使用FastController作为后端

            # 生成UAV
            prim_path = f"/World/uav{vid}"
            vehicle = Multirotor(
                prim_path,
                ROBOTS['Iris'],
                vid,
                init_pos,
                quat,
                config=config_multirotor,
            )

            self.vehicles[vid] = vehicle
            self.controllers[vid] = fast_ctrl

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

        # 创建虚拟控制器
        for vid, vehicle in self.vehicles.items():
            port = self.ctrl_base_port + vid
            vc = VirtualController(vid, vehicle, self.controllers[vid], port)
            self.virtual_controllers[vid] = vc
            vc.start()

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


class FastSimApp:
    """
    纯Python仿真应用主类
    """
    def __init__(self):
        self.timeline = omni.timeline.get_timeline_interface()
        self.pg = PegasusInterface()

        if RENDER_THROTTLE:
            self.pg.set_world_settings(rendering_dt=1.0 / max(RENDER_MAX_FPS, 0.1))
        self.pg._world = World(**self.pg._world_settings)
        self.world = self.pg.world

        # 加载配置
        cfg = self._load_config(ARGS.config)

        # 创建多机管理器
        self.manager = FastMultiUAVManager(
            self.pg, self.world, cfg,
            ctrl_base_port=ARGS.ctrl_base_port
        )
        self.manager.spawn()

        # HTTP服务器
        self.flask_app = Flask(__name__)
        self._flask_server = None
        self._setup_routes()
        self._start_http_server(port=ARGS.sim_port)

        # 录制状态
        os.makedirs(RECORD_DIR, exist_ok=True)
        self._record_session_dir = os.path.join(RECORD_DIR, f"session_{int(time.time())}")
        os.makedirs(self._record_session_dir, exist_ok=True)
        self._record_last_ts_by_uav = {}
        self._csv_agg_initialized_uav = set()
        self._record_runtime_enable = bool(RECORD_ENABLE)

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
            should_render = True
            if RENDER_THROTTLE:
                now = time.time()
                interval = 1.0 / max(RENDER_MAX_FPS, 0.1)
                if (now - self._last_render_ts) >= interval:
                    should_render = True
                    self._last_render_ts = now
                else:
                    should_render = False

            self.world.step(render=should_render)

            if self._record_runtime_enable:
                try:
                    self._record_if_due()
                except Exception as e:
                    carb.log_warn(f"Recording error: {e}")

        carb.log_warn("FastSimApp is closing.")
        self._stop_http_server()
        for vc in self.manager.virtual_controllers.values():
            vc.stop()
        self.timeline.stop()
        simulation_app.close()

    def _start_http_server(self, host: str = "127.0.0.1", port: int = 8081):
        """启动HTTP服务器"""
        if self._flask_server is not None:
            return
        self._flask_server = make_server(host, port, self.flask_app, threaded=True)
        self.http_thread = threading.Thread(target=self._flask_server.serve_forever, daemon=True)
        self.http_thread.start()
        carb.log_info(f"FastSimApp HTTP server started at http://{host}:{port}/")

    def _stop_http_server(self):
        """停止HTTP服务器"""
        try:
            if self._flask_server:
                self._flask_server.shutdown()
                self._flask_server = None
        except Exception as e:
            carb.log_warn(f"Error stopping HTTP server: {e}")

    def _setup_routes(self):
        """设置HTTP路由"""
        app = self.flask_app

        @app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "healthy", "type": "fast_sim"})

        @app.route('/record/<cmd>', methods=['GET'])
        def record_cmd(cmd: str):
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
                    "timestamp": time.time(),
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
            """兼容接口：始终返回 ready=True"""
            vehicle = self._get_vehicle(uav_id)
            if vehicle is None:
                return jsonify({"status": "error", "ready": False}), 404
            return jsonify({"status": "success", "uav_id": uav_id, "ready": True})

        @app.route('/uav/<int:uav_id>/px4/status', methods=['GET'])
        def px4_status(uav_id: int):
            """兼容接口：返回模拟的PX4状态"""
            vehicle = self._get_vehicle(uav_id)
            if vehicle is None:
                return jsonify({"status": "error"}), 404
            return jsonify({
                "status": "success",
                "px4_backend": {
                    "uav_id": uav_id,
                    "is_running": True,
                    "mavlink_connected": True,
                    "px4_ready_to_takeoff": True,
                    "type": "FastController",
                }
            })

    def _get_vehicle(self, uav_id: int):
        """获取车辆"""
        return self.manager.vehicles.get(uav_id)

    def _get_camera(self, vehicle):
        """获取相机"""
        try:
            for s in getattr(vehicle, "_graphical_sensors", []):
                if getattr(s, "sensor_type", "") == "MonocularCamera":
                    return s
        except:
            pass
        return None

    def _reset_uav(self, uav_id: int, position: list, yaw_deg: float = 0.0):
        """重置UAV"""
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
    app = FastSimApp()
    app.run()


if __name__ == "__main__":
    main()
