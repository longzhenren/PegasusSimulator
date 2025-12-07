#!/usr/bin/env python
"""
Pegasus 仿真应用（8_camera_vehicle.py）

概述
- 启动 Isaac Sim（支持 headless）并按 `multi_uav_config.json` 加载多载具与场景；对外暴露仿真 HTTP 接口以供控制器读取图像/位姿与执行位置重置。
- 支持录制机制：按固定帧率保存 PNG/CSV 到 `recordings/`，可用于后处理或数据集生成。

HTTP 接口（端口 8080）
- `GET /uav/<id>/pose`：载具状态快照
- `GET /uav/<id>/image`、`GET /uav/<id>/image.png`：相机图像（JSON/PNG）
- `GET /uav/<id>/all`：图像 + 位姿汇总
- `POST /uav/<id>/reset`：移动载具到指定位置（可选 yaw），并清零速度与角速度

配置与渲染
- 多机配置：`multi_uav_config.json` 描述 `vehicle_id`、`mavros_namespace`、初始位姿等；启动器用其生成 MAVROS 启动文件。
- 渲染节流：`RENDER_THROTTLE` 与 `RENDER_MAX_FPS` 控制渲染频率，降低 GPU 负载；相机输出受渲染帧产生频率影响。

与控制器的关系
- 控制器可将图像源切换为仿真 HTTP（`/step_http`）；网关将 `/uav/<id>/...` 统一转发到各控制器端口，形成统一入口。

架构与类
- `PegasusApp`：主应用类。
  - 初始化 Isaac 时间线与 `PegasusInterface`、`World`。
  - 加载 `multi_uav_config.json` → `MultiUAVManager.spawn()` 生成多载具（含图形传感器与 PX4/MAVROS 后端配置）。
  - 内置 Flask 应用 `_setup_routes()` 与 `_start_http_server()` 暴露 HTTP 接口。
  - 主循环 `run()`：按节流策略推进 `world.step(render=should_render)` 并在开启录制时保存 PNG/CSV。
- `MultiUAVManager`：
  - 负责读取配置与创建载具；用于定位与状态快照的获取（通过 `VehicleManager`）。
  - 提供 `reset_uav(uav_id, position, yaw)` 将 USD 中的载具刚体位置与速度重置。

录制管线
- 每帧在 `_record_if_due()` 生成相机图像（PNG）与状态快照（CSV）；每个 UAV 在一次会话下独立生成数据文件。
- CSV 聚合包含位姿、姿态、速度角速度与线加速度，便于后处理。

接口示例
- `GET /uav/0/all` 返回示例键：`{"image": {"data": "<base64>"}, "pose": {...}}`
- `POST /uav/0/reset` 请求示例：`{"position": [-88.0, 8.0, 5.0], "yaw_deg": 0.0}`

"""

# Imports to start Isaac Sim from this script
import carb
from isaacsim import SimulationApp

import sys, os
import json
import traceback
import threading
import time
import base64
import csv
import numpy as np
from io import BytesIO
from PIL import Image
from flask import Flask, jsonify, request, Response
from werkzeug.serving import make_server
from scipy.spatial.transform import Rotation

SIMULATION_ENVIRONMENTS = {}
NVIDIA_SIMULATION_ENVIRONMENTS = {
    "Default Environment": "Grid/default_environment.usd",
    "Black Gridroom": "Grid/gridroom_black.usd",
    "Curved Gridroom": "Grid/gridroom_curved.usd",
    "Hospital": "Hospital/hospital.usd",
    "Office": "Office/office.usd",
    "Simple Room": "Simple_Room/simple_room.usd",
    "Warehouse": "Simple_Warehouse/warehouse.usd",
    "Warehouse with Forklifts": "Simple_Warehouse/warehouse_with_forklifts.usd",
    "Warehouse with Shelves": "Simple_Warehouse/warehouse_multiple_shelves.usd",
    "Full Warehouse": "Simple_Warehouse/full_warehouse.usd",
    "Flat Plane": "Terrains/flat_plane.usd",
    "Rough Plane": "Terrains/rough_plane.usd",
    "Slope Plane": "Terrains/slope.usd",
    "Stairs Plane": "Terrains/stairs.usd",
}
for asset in NVIDIA_SIMULATION_ENVIRONMENTS:
    SIMULATION_ENVIRONMENTS[asset] = (
        "https://omniverse-content-production.s3-us-west-2.amazonaws.com/Assets/Isaac/4.5/Isaac/Environments/" + NVIDIA_SIMULATION_ENVIRONMENTS[asset]
    )


# Start Isaac Sim's simulation environment
# Note: this simulation app must be instantiated right after the SimulationApp import, otherwise the simulator will crash
# as this is the object that will load all the extensions and load the actual simulator.
# -------------------------
# Render throttling / renderer
# -------------------------
# RENDER_MAX_FPS 说明：
# - 含义：渲染的最高帧率（上限），单位为 FPS。只在达到该频率时产生新的渲染帧，图形传感器（相机）也仅在渲染帧产生时输出新图像。
# - 主循环节流：当 RENDER_THROTTLE=True 时，主循环通过 should_render 逻辑让 world.step(render=True/False) 以不高于该频率的速率触发渲染；
#   其余物理步依然执行，但不渲染（render=False），因此相机不会更新，能显著降低 GPU 负载。
# - 渲染时钟对齐：在创建 World 之前设置 rendering_dt=1.0/RENDER_MAX_FPS，使 Isaac Sim 的渲染时钟与该上限一致（与官方 API 参数名一致）。
# - 与物理步进的关系：不影响 physics_dt（物理仿真频率）；物理仍按 physics_dt 运行，RENDER_MAX_FPS 只约束图形渲染及图形传感器采样频率。
# - 与相机频率的关系：若相机自身频率低于 RENDER_MAX_FPS，则以相机频率为准；若高于 RENDER_MAX_FPS，则受该上限限制。
# - 建议：多载具/多相机场景下将 RENDER_MAX_FPS 调低（例如 10–20），配合栅格化渲染可显著降低 GPU 消耗。
# - 边界：不应设为 <=0。代码中使用 max(RENDER_MAX_FPS, 0.1) 以避免除零并保持健壮性。
# 切换到 Rasterization（栅格化）以降低 GPU 消耗；如需光追请设为 False
USE_RASTERIZATION = True
# 渲染节流：仅按设定 FPS 渲染；其余物理步不渲染（相机不更新）
RENDER_THROTTLE = True
RENDER_MAX_FPS = 10.0
CAMERA_RESOLUTION = (640, 640)
# USD_PATH = SIMULATION_ENVIRONMENTS['Curved Gridroom']
# USD_PATH = "/home/user/Downloads/Demos/AEC/BrownstoneDemo/World_BrownstoneDemopack_Morning(20Gb).usd"
USD_PATH = "/home/user/export/extract.usd"
# -------------------------
# Recording (global switch)
# -------------------------
# 全局录制开关：是否以10fps频率保存PNG图片和CSV数据到本地（时间戳命名）
RECORD_ENABLE = False
RECORD_FPS = 10.0
RECORD_DIR = os.path.join(os.path.dirname(__file__), "recordings")

# -------------------------
# Multi-UAV Config (JSON only)
# -------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "multi_uav_config.json")

# 是否启用 ROS2 后端（若设为 False 则仅使用 PX4 MAVLink）
ROS2_ENABLE = False
ROS2_CAMERA_ENABLE = False
ROS2_SENSOR_ENABLE = False
ROS2_STATE_ENABLE = False

APP_CONFIG = {
    # "width": 600,
    # "height": 600,
    "window_width": 1280,
    "window_height": 720,
    "headless": False,
    "max_bounces": 0 if USE_RASTERIZATION else 1,  # RT 模式里 bounces 越低越快
    "samples_per_pixel_per_frame": 1 if USE_RASTERIZATION else 16,  # 默认 64，很吃GPU，先降
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
    ],
}
simulation_app = SimulationApp(APP_CONFIG)

# -----------------------------------
# The actual script should start here
# -----------------------------------

import omni.timeline
from isaacsim.core.api.world import World

sys.path.insert(0, os.path.expanduser("~/PegasusSimulator-5.1/extensions/pegasus.simulator/"))

# Import the Pegasus API for simulating drones
from pegasus.simulator.params import ROBOTS
from pegasus.simulator.logic.graphical_sensors.monocular_camera import MonocularCamera
from pegasus.simulator.logic.backends.px4_mavlink_backend import PX4MavlinkBackend, PX4MavlinkBackendConfig
from pegasus.simulator.logic.backends.ros2_backend import ROS2Backend
from pegasus.simulator.logic.vehicles.multirotor import Multirotor, MultirotorConfig
from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface

def load_config_strict(path: str = CONFIG_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Multi-UAV config not found: {path}")
    with open(path, "r") as f:
        return json.load(f)

class MultiUAVManager:
    """
    Read JSON config and spawn multiple multirotors with per-vehicle PX4/MAVROS/ROS2 settings.
    """
    def __init__(self, pg: PegasusInterface, world: World, config: dict):
        self.pg = pg
        self.world = world
        self.config = config
        self.vehicles = []
        self.camera_resolution = CAMERA_RESOLUTION
        # self.world.set_physics_step_size(1.0 / 30.0)
        # self.world.set_min_simulation_frame_rate(30.0)
        # self.world.set_gpu_dynamics_enabled(True)

    def spawn(self):
        # Load environment
        self.pg.load_environment(USD_PATH)
        
        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            init_pos = v.get("initial_position", [0.0, 0.0, 5.0])
            euler = v.get("initial_orientation_euler_deg", [0.0, 0.0, 0.0])
            quat = Rotation.from_euler("XYZ", euler, degrees=True).as_quat()

            # PX4 backend config per vehicle
            px4_cfg_dict = {
                "vehicle_id": vid,
                "px4_autolaunch": bool(v.get("px4_autolaunch", True)),
                "px4_dir": v.get("px4_dir", self.pg.px4_path),
                "sim_speed_factor": v.get("sim_speed_factor", 2.0),
                "px4_vehicle_type": v.get("px4_vehicle_model", "gazebo-classic_iris_pg"),
            }
            mavlink_config = PX4MavlinkBackendConfig(px4_cfg_dict)

            # ROS2 backend (namespaced) for sensors/graphical
            ros2_ns = v.get("ros2_namespace", f"uav{vid}")

            # Configure vehicle sensors
            camera = MonocularCamera(f"front_camera_{vid}", config={"depth": False, "frequency": 10.0, "resolution": self.camera_resolution})
            config_multirotor = MultirotorConfig()
            config_multirotor.graphical_sensors = [camera]
            if ROS2_ENABLE:
                ros2_backend = ROS2Backend(vehicle_id=vid, config={
                    "namespace": ros2_ns,
                    "pub_sensors": ROS2_SENSOR_ENABLE,
                    "pub_graphical_sensors": ROS2_CAMERA_ENABLE,
                    "pub_state": ROS2_STATE_ENABLE,
                    "sub_control": False,
                })
                config_multirotor.backends = [PX4MavlinkBackend(mavlink_config), ros2_backend]
            else:
                config_multirotor.backends = [PX4MavlinkBackend(mavlink_config)]

            # Spawn vehicle with consistent prim path namespace (/World/<ros2_ns>)
            prim_path = f"/World/{ros2_ns}"
            Multirotor(
                prim_path,
                ROBOTS['Iris'],
                vid,
                init_pos,
                quat,
                config=config_multirotor,
            )

        # Reset simulation to initialize assets
        self.world.reset()

class PegasusApp:
    """
    A Template class that serves as an example on how to build a simple Isaac Sim standalone App.
    """
    def __init__(self):
        """
        Method that initializes the PegasusApp and is used to setup the simulation environment.
        """

        # Acquire the timeline that will be used to start/stop the simulation
        self.timeline = omni.timeline.get_timeline_interface()

        # Start the Pegasus Interface
        self.pg = PegasusInterface()

        # Acquire the World, .i.e, the singleton that controls that is a one stop shop for setting up physics,
        # spawning asset primitives, etc.
        # Ensure the rendering_dt matches our desired render rate semantics per Isaac Sim API
        if RENDER_THROTTLE:
            self.pg.set_world_settings(rendering_dt=1.0 / max(RENDER_MAX_FPS, 0.1))
        self.pg._world = World(**self.pg._world_settings)
        self.world = self.pg.world

        # Load environment and vehicles from local JSON config (strict, no defaults)
        cfg = load_config_strict()
        self.manager = MultiUAVManager(self.pg, self.world, cfg)
        self.manager.spawn()

        self.flask_app = Flask(__name__)
        self._flask_server = None
        self.http_thread = None
        self._setup_routes()
        self._start_http_server()

        # Recording state
        os.makedirs(RECORD_DIR, exist_ok=True)
        # 每次启动创建一个带时间戳的会话目录，用于按目录存放
        self._record_session_dir = os.path.join(
            RECORD_DIR, f"session_{int(time.time())}"
        )
        os.makedirs(self._record_session_dir, exist_ok=True)
        self._record_last_ts_by_uav = {}
        # 聚合 CSV 初始化标记（每个 UAV 一个会话 CSV 文件）
        self._csv_agg_initialized_uav = set()
        # 运行时录制开关（初始值来源全局 RECORD_ENABLE，可通过 HTTP 切换）
        self._record_runtime_enable = bool(RECORD_ENABLE)

        # Auxiliar variable for the timeline callback example
        self.stop_sim = False
        # 渲染节流状态
        self._last_render_ts = 0.0

    def run(self):
        """
        Method that implements the application main loop, where the physics steps are executed.
        """

        # Start the simulation
        self.timeline.play()

        # The "infinite" loop
        while simulation_app.is_running() and not self.stop_sim:
            # 根据节流设置决定是否本帧渲染
            should_render = True
            if RENDER_THROTTLE:
                now = time.time()
                interval = 1.0 / max(RENDER_MAX_FPS, 0.1)
                if (now - self._last_render_ts) >= interval:
                    should_render = True
                    self._last_render_ts = now
                else:
                    should_render = False

            # 执行步进；仅在 should_render 为 True 时更新相机帧
            self.world.step(render=should_render)

            # Record frames at fixed RECORD_FPS if runtime-enabled
            if self._record_runtime_enable:
                try:
                    self._record_if_due()
                except Exception as e:
                    carb.log_warn(f"Recording error: {e}")

        # Cleanup and stop
        carb.log_warn("PegasusApp Simulation App is closing.")
        self._stop_http_server()
        self.timeline.stop()
        simulation_app.close()

    # ------------------------------
    # HTTP server lifecycle
    # ------------------------------
    def _start_http_server(self, host: str = "127.0.0.1", port: int = 8080):
        if self._flask_server is not None:
            return
        self._flask_server = make_server(host, port, self.flask_app)
        self.http_thread = threading.Thread(target=self._flask_server.serve_forever, name="uav-http", daemon=True)
        self.http_thread.start()
        carb.log_info(f"UAV HTTP server started at http://{host}:{port}/")

    def _stop_http_server(self):
        try:
            if self._flask_server:
                self._flask_server.shutdown()
                self._flask_server = None
            if self.http_thread:
                self.http_thread.join(timeout=2.0)
                self.http_thread = None
        except Exception as e:
            carb.log_warn(f"Error stopping HTTP server: {e}")


    def _setup_routes(self):
        app = self.flask_app

        @app.route('/record/<cmd>', methods=['GET'])
        def record_cmd(cmd: str):
            if cmd == 'start':
                self._record_runtime_enable = True
                return jsonify({"recording": True})
            if cmd == 'stop':
                self._record_runtime_enable = False
                return jsonify({"recording": False})
            if cmd == 'status':
                return jsonify({"recording": bool(getattr(self, "_record_runtime_enable", False))})
            return jsonify({"error": "Invalid path. Use /record/(start|stop/status)"}), 404

        @app.route('/uav/<int:uav_id>/pose', methods=['GET'])
        def pose(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    carb.log_warn(f"HTTP pose vehicle_missing uav_id={uav_id} path={request.path}")
                    return jsonify({"error": f"Vehicle /World/uav{uav_id} not found"}), 404
                st = vehicle.state
                ts = time.time()
                payload = {
                    "uav_id": uav_id,
                    "timestamp": ts,
                    "position": st.position.tolist(),
                    "attitude": st.attitude.tolist(),
                    "linear_velocity": st.linear_velocity.tolist(),
                    "angular_velocity": st.angular_velocity.tolist(),
                    "linear_acceleration": st.linear_acceleration.tolist(),
                }
                return jsonify(payload)
            except Exception as e:
                carb.log_warn(f"HTTP pose error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "pose"}), 500

        @app.route('/uav/<int:uav_id>/image.png', methods=['GET'])
        def image_png(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    carb.log_warn(f"HTTP image.png vehicle_missing uav_id={uav_id} path={request.path}")
                    return jsonify({"error": f"Vehicle /World/uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    carb.log_warn(f"HTTP image.png camera_missing uav_id={uav_id}")
                    return jsonify({"error": "Camera not found"}), 404
                img, ts = cam.get_last_image_with_timestamp()
                if img is None:
                    carb.log_warn(f"HTTP image.png no_image uav_id={uav_id}")
                    return jsonify({"error": "No image cached yet"}), 503
                png_bytes, b64, w, h, c = self._png_bytes_and_b64(img)
                return Response(png_bytes, status=200, mimetype='image/png', headers={'Cache-Control': 'no-cache'})
            except Exception as e:
                carb.log_warn(f"HTTP image.png error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "image.png"}), 500

        @app.route('/uav/<int:uav_id>/image', methods=['GET'])
        def image(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    carb.log_warn(f"HTTP image vehicle_missing uav_id={uav_id} path={request.path}")
                    return jsonify({"error": f"Vehicle /World/uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    carb.log_warn(f"HTTP image camera_missing uav_id={uav_id}")
                    return jsonify({"error": "Camera not found"}), 404
                img, ts = cam.get_last_image_with_timestamp()
                if img is None:
                    carb.log_warn(f"HTTP image no_image uav_id={uav_id}")
                    return jsonify({"error": "No image cached yet"}), 503
                png_bytes, b64, w, h, c = self._png_bytes_and_b64(img)
                payload = {
                    "uav_id": uav_id,
                    "timestamp": ts,
                    "width": w,
                    "height": h,
                    "channels": c,
                    "encoding": "png_base64",
                    "mime": "image/png",
                    "data": b64,
                    "data_url": "data:image/png;base64," + b64,
                }
                return jsonify(payload)
            except Exception as e:
                carb.log_warn(f"HTTP image error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "image"}), 500

        @app.route('/uav/<int:uav_id>/all', methods=['GET'])
        def all_info(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    carb.log_warn(f"HTTP all vehicle_missing uav_id={uav_id} path={request.path}")
                    return jsonify({"error": f"Vehicle /World/uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    carb.log_warn(f"HTTP all camera_missing uav_id={uav_id}")
                    return jsonify({"error": "Camera not found"}), 404
                img, ts_img = cam.get_last_image_with_timestamp()
                if img is None:
                    carb.log_warn(f"HTTP all no_image uav_id={uav_id}")
                    return jsonify({"error": "No image cached yet"}), 503
                st_snap = cam.get_last_state_snapshot()
                if st_snap is None:
                    carb.log_warn(f"HTTP all no_pose uav_id={uav_id}")
                    return jsonify({"error": "No pose snapshot cached yet"}), 503
                png_bytes, b64, w, h, c = self._png_bytes_and_b64(img)
                payload = {
                    "uav_id": uav_id,
                    "image": {
                        "timestamp": ts_img,
                        "width": w,
                        "height": h,
                        "channels": c,
                        "encoding": "png_base64",
                        "mime": "image/png",
                        "data": b64,
                        "data_url": "data:image/png;base64," + b64,
                    },
                    "pose": {
                        "timestamp": ts_img,
                        "position": st_snap["position"].tolist(),
                        "attitude": st_snap["attitude"].tolist(),
                        "linear_velocity": st_snap["linear_velocity"].tolist(),
                        "angular_velocity": st_snap["angular_velocity"].tolist(),
                        "linear_acceleration": st_snap["linear_acceleration"].tolist(),
                    }
                }
                return jsonify(payload)
            except Exception as e:
                carb.log_warn(f"HTTP all error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "all"}), 500

        @app.route('/uav/<int:uav_id>/reset', methods=['POST'])
        def reset_uav_route(uav_id: int):
            try:
                try:
                    data = request.json or {}
                except Exception:
                    data = {}
                pos = data.get("position") or data.get("pos") or []
                yaw_deg = data.get("yaw_deg")
                if not isinstance(pos, (list, tuple)) or len(pos) < 3:
                    carb.log_warn(f"HTTP reset invalid_position uav_id={uav_id} body={data}")
                    return jsonify({"error": "position must be [x,y,z]"}), 400
                carb.log_info(f"Reset UAV {uav_id} to position: {pos} yaw_deg: {yaw_deg}")
                try:
                    ok, msg = self.reset_uav(uav_id, [float(pos[0]), float(pos[1]), float(pos[2])], yaw_deg)
                except Exception as e:
                    carb.log_warn(f"HTTP reset exec_error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                    return jsonify({"status": "error", "message": f"reset failed. Exception: {e}"}), 500
                if not ok:
                    carb.log_warn(f"HTTP reset failed uav_id={uav_id} msg={msg}")
                    return jsonify({"status": "error", "message": msg or "reset failed"}), 500
                return jsonify({"status": "success", "uav_id": uav_id, "position": [float(pos[0]), float(pos[1]), float(pos[2])], "message": "reset ok"})
            except Exception as e:
                carb.log_warn(f"HTTP reset error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "reset"}), 500

        @app.route('/uav/<int:uav_id>/px4/hard_reset', methods=['POST'])
        def px4_hard_reset_route(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"status": "error", "message": f"Vehicle /World/uav{uav_id} not found"}), 404
                try:
                    backend = None
                    for b in getattr(vehicle, "_backends", []):
                        if isinstance(b, PX4MavlinkBackend):
                            backend = b
                            break
                    if backend is None:
                        return jsonify({"status": "error", "message": "PX4 backend not found"}), 404
                    backend.hard_reboot_px4()
                except Exception as e:
                    carb.log_warn(f"HTTP px4 hard_reset exec_error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                    return jsonify({"status": "error", "message": f"px4 hard reset failed: {e}"}), 500
                return jsonify({"status": "success", "uav_id": uav_id, "message": "px4 hard reset ok"})
            except Exception as e:
                carb.log_warn(f"HTTP px4 hard_reset error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "px4/hard_reset"}), 500

        @app.route('/uav/<int:uav_id>/px4/relaunch', methods=['POST'])
        def px4_relaunch_route(uav_id: int):
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"status": "error", "message": f"Vehicle /World/uav{uav_id} not found"}), 404
                try:
                    backend = None
                    for b in getattr(vehicle, "_backends", []):
                        if isinstance(b, PX4MavlinkBackend):
                            backend = b
                            break
                    if backend is None:
                        return jsonify({"status": "error", "message": "PX4 backend not found"}), 404
                    backend.soft_relaunch_px4()
                    backend.start()
                except Exception as e:
                    carb.log_warn(f"HTTP px4 relaunch exec_error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                    return jsonify({"status": "error", "message": f"px4 relaunch failed: {e}"}), 500
                return jsonify({"status": "success", "uav_id": uav_id, "message": "px4 relaunch ok"})
            except Exception as e:
                carb.log_warn(f"HTTP px4 relaunch error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "px4/relaunch"}), 500

    def _png_bytes_and_b64(self, img):
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

    def _get_vehicle(self, uav_id: int):
        from pegasus.simulator.logic.vehicle_manager import VehicleManager
        vm = VehicleManager.get_vehicle_manager()
        stage_prefix = f"/World/uav{uav_id}"
        v = vm.get_vehicle(stage_prefix)
        if v is not None:
            return v
        try:
            for _, veh in vm.vehicles.items():
                if getattr(veh, "id", None) == uav_id:
                    return veh
        except Exception:
            pass
        return None

    def _get_camera(self, vehicle):
        try:
            for s in getattr(vehicle, "_graphical_sensors", []):
                if getattr(s, "sensor_type", "") == "MonocularCamera":
                    return s
        except Exception:
            pass
        return None

    def reset_uav(self, uav_id: int, position: list, yaw_deg: float = None):
        """Move specified UAV to position (x,y,z) and optional yaw in degrees.
        Returns (ok, message)."""
        try:
            from pegasus.simulator.logic.vehicle_manager import VehicleManager
            vm = VehicleManager.get_vehicle_manager()
            stage_prefix = f"/World/uav{uav_id}"
            vehicle = vm.get_vehicle(stage_prefix)
            if vehicle is None:
                # fallback: search by id
                for _, v in getattr(vm, "vehicles", {}).items():
                    if getattr(v, "id", None) == int(uav_id):
                        vehicle = v
                        break
            if vehicle is None:
                return False, f"Vehicle /World/uav{uav_id} not found"

            dc = vehicle.get_dc_interface()
            body = dc.get_rigid_body(stage_prefix + "/body")
            # Build pose
            import carb
            from pegasus.simulator.logic.state import State
            # Get current pose to preserve orientation if yaw not provided
            pose = dc.get_rigid_body_pose(body)
            # Use carb.Float3 when available; fallback to pxr Gf.Vec3f
            try:
                p = carb.Float3(float(position[0]), float(position[1]), float(position[2]))
            except Exception:
                try:
                    from pxr import Gf
                    p = Gf.Vec3f(float(position[0]), float(position[1]), float(position[2]))
                except Exception:
                    p = [float(position[0]), float(position[1]), float(position[2])]
            # Omni dynamic_control returns Transform with rotation in 'r' (Quat)
            # Preserve current rotation if yaw not provided
            try:
                q = pose.r
            except Exception:
                q = getattr(pose, 'q', None)
            if yaw_deg is not None:
                import math
                yaw = math.radians(float(yaw_deg))
                cy = math.cos(yaw * 0.5)
                sy = math.sin(yaw * 0.5)
                try:
                    q = carb.Float4(0.0, 0.0, sy, cy)
                except Exception:
                    q = [0.0, 0.0, sy, cy]
            from omni.isaac.dynamic_control import _dynamic_control as dc_mod
            new_pose = dc_mod.Transform()
            new_pose.p = p
            try:
                new_pose.r = q
            except Exception:
                try:
                    new_pose.r = carb.Float4(float(getattr(q, 'x')), float(getattr(q, 'y')), float(getattr(q, 'z')), float(getattr(q, 'w')))
                except Exception:
                    if isinstance(q, (list, tuple)) and len(q) == 4:
                        new_pose.r = carb.Float4(float(q[0]), float(q[1]), float(q[2]), float(q[3]))
                    else:
                        new_pose.r = carb.Float4(0.0, 0.0, 0.0, 1.0)
            dc.set_rigid_body_pose(body, new_pose)

            # also reset velocities
            try:
                zero = carb.Float3(0.0, 0.0, 0.0)
            except Exception:
                try:
                    from pxr import Gf
                    zero = Gf.Vec3f(0.0, 0.0, 0.0)
                except Exception:
                    zero = [0.0, 0.0, 0.0]
            dc.set_rigid_body_linear_velocity(body, zero)
            dc.set_rigid_body_angular_velocity(body, zero)
            return True, "ok"
        except Exception as e:
            try:
                carb.log_warn(f"reset_uav error: {e}")
            except Exception:
                pass
            return False, str(e)

    # ------------------------------
    # Recording helpers
    # ------------------------------
    def _get_camera_from_vehicle(self, vehicle):
        try:
            for s in getattr(vehicle, "_graphical_sensors", []):
                if getattr(s, "sensor_type", "") == "MonocularCamera":
                    return s
        except Exception:
            pass
        return None

    def _record_if_due(self):
        """Capture aligned image+pose at RECORD_FPS and save PNG+CSV with timestamp filename."""
        from pegasus.simulator.logic.vehicle_manager import VehicleManager
        vm = VehicleManager.get_vehicle_manager()
        vehicles = list(getattr(vm, "vehicles", {}).values())
        if not vehicles:
            return

        period = 1.0 / max(RECORD_FPS, 0.1)

        for vehicle in vehicles:
            uav_id = getattr(vehicle, "id", None)
            if uav_id is None:
                continue
            cam = self._get_camera_from_vehicle(vehicle)
            if cam is None:
                continue
            img, ts_img = cam.get_last_image_with_timestamp()
            st_snap = cam.get_last_state_snapshot()
            if img is None or st_snap is None or ts_img is None:
                continue

            last_ts = self._record_last_ts_by_uav.get(uav_id)
            if last_ts is not None and (ts_img - last_ts) < period:
                continue

            # Save PNG
            ts_ms = int(ts_img * 1000)
            png_path = os.path.join(self._record_session_dir, f"uav{uav_id}_{ts_ms}.png")
            try:
                img8 = img.astype('uint8')
                pil_img = Image.fromarray(img8)
                pil_img.save(png_path, format='PNG')
            except Exception:
                # Fallback: raw dump if PIL failed
                with open(png_path + ".raw", "wb") as f:
                    f.write(img.astype('uint8').tobytes())

            # 聚合 CSV（每 UAV 一个文件，追加行）
            csv_agg_path = os.path.join(self._record_session_dir, f"uav{uav_id}.csv")
            try:
                h, w, c = img.shape
                # 如果首次创建该 UAV 的聚合 CSV，则写入表头
                if (uav_id not in self._csv_agg_initialized_uav) and (not os.path.exists(csv_agg_path)):
                    with open(csv_agg_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            "timestamp_ms",
                            "timestamp_s",
                            "uav_id",
                            "image_filename",
                            "image_width",
                            "image_height",
                            "image_channels",
                            "pos_x","pos_y","pos_z",
                            "att_w","att_x","att_y","att_z",
                            "linvel_x","linvel_y","linvel_z",
                            "angvel_x","angvel_y","angvel_z",
                            "linacc_x","linacc_y","linacc_z",
                        ])
                    self._csv_agg_initialized_uav.add(uav_id)

                # 追加本帧一行数据
                with open(csv_agg_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        ts_ms,
                        ts_img,
                        uav_id,
                        os.path.basename(png_path),
                        w,
                        h,
                        c,
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
                carb.log_warn(f"CSV aggregate save error: {e}")

            self._record_last_ts_by_uav[uav_id] = ts_img

def main():
    pg_app = PegasusApp()
    
    # Run the application loop
    pg_app.run()

if __name__ == "__main__":
    main()
