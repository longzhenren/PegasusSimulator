#!/usr/bin/env python
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
Pegasus 仿真应用（8_camera_vehicle.py）

==========================
概述
==========================
本脚本是 Pegasus 仿真系统的核心仿真端，负责：
- 启动 Isaac Sim（支持 headless 无头模式）并加载 USD 场景
- 按 `multi_uav_config.json` 配置生成多架无人机（含相机传感器）
- 为每架无人机启动 PX4 SITL 后端（自动启动 PX4 进程）
- 暴露 HTTP 接口供控制器/外部程序读取图像、位姿、执行重置等操作
- 支持录制机制：按固定帧率保存 PNG 图像和 CSV 状态数据

==========================
启动流程与时序
==========================
1. SimulationApp 初始化
   - 解析 APP_CONFIG（含渲染器、加速参数等）
   - 创建 Isaac Sim 仿真实例

2. PegasusApp.__init__()
   a) 设置仿真加速参数（禁用实时节流）
   b) 初始化 PegasusInterface 和 World
   c) 加载 multi_uav_config.json 配置
   d) MultiUAVManager.spawn() 生成所有载具：
      - 加载 USD 环境场景
      - 为每架 UAV 创建 Multirotor 对象
      - 配置 MonocularCamera 图形传感器
      - 配置 PX4MavlinkBackend（自动启动 PX4 SITL）
      - 配置碰撞过滤（UAV 之间不碰撞）
      - 配置透明度（可选，用于多机视觉）
   e) 启动 Flask HTTP 服务器（端口 8081）
   f) 创建录制会话目录

3. PegasusApp.run() 主循环
   - 调用 timeline.play() 开始仿真
   - 循环执行：
     a) 根据 RENDER_THROTTLE 判断是否渲染
     b) world.step(render=should_render) 推进物理+渲染
     c) 若启用录制，调用 _record_if_due() 保存数据

==========================
HTTP 接口（端口 8081）
==========================
GET /uav/<id>/pose
  - 返回指定 UAV 的位姿快照
  - 响应：{"uav_id": 0, "timestamp": 1234.5, "position": [x,y,z],
           "attitude": [qx,qy,qz,qw], "linear_velocity": [...],
           "angular_velocity": [...], "linear_acceleration": [...]}

GET /uav/<id>/image
  - 返回 JSON 格式的相机图像（Base64 编码 PNG）
  - 响应：{"uav_id": 0, "timestamp": 1234.5, "width": 640, "height": 640,
           "data": "<base64>", "data_url": "data:image/png;base64,..."}

GET /uav/<id>/image.png
  - 直接返回 PNG 二进制图像

GET /uav/<id>/all
  - 返回图像和位姿的综合信息（同步快照）
  - 响应：{"uav_id": 0, "image": {...}, "pose": {...}}

POST /uav/<id>/reset
  - 重置 UAV 到指定位置
  - 请求体：{"position": [x, y, z], "yaw_deg": 0.0}
  - 响应：{"status": "success", "uav_id": 0, "position": [...]}

POST /uav/<id>/px4/hard_reset
  - 硬重启 PX4 进程（停止并重新启动）
  - 响应：{"status": "success", "uav_id": 0}

POST /uav/<id>/px4/relaunch
  - 软重启 PX4 进程
  - 响应：{"status": "success", "uav_id": 0}

GET /uav/<id>/px4/ready
  - 查询 PX4 是否就绪（Ready for takeoff）
  - 响应：{"status": "success", "uav_id": 0, "ready": true/false}

GET /record/start
GET /record/stop
GET /record/status
  - 控制和查询录制状态

==========================
调用关系
==========================
                    launch_multi_rospy.py
                           │
                           ▼
        ┌─────────────────────────────────────────┐
        │         8_camera_vehicle.py              │
        │  (Isaac Sim + PX4 SITL + HTTP Server)    │
        │                                          │
        │  HTTP :8081 ◄───────┐                    │
        └─────────┬───────────┼────────────────────┘
                  │           │
                  │ MAVLink   │ HTTP
                  ▼           │
        ┌─────────────────────┴────────────────────┐
        │         rospy_isaacsim.py                │
        │   (ROS2 + MAVROS Controller :5009+id)    │
        │                                          │
        │  HTTP :5009+id ◄─────┐                   │
        └─────────┬────────────┼───────────────────┘
                  │            │
                  │            │ HTTP
                  ▼            │
        ┌──────────────────────┴───────────────────┐
        │         recording_server.py              │
        │      (Gateway + Recording :5008)         │
        └──────────────────────────────────────────┘

==========================
配置文件（multi_uav_config.json）
==========================
{
  "vehicles": [
    {
      "vehicle_id": 0,                      // 载具 ID（唯一）
      "mavros_namespace": "uav0",           // ROS2 命名空间
      "ros2_namespace": "uav0",             // USD prim 路径前缀
      "initial_position": [0.0, 0.0, 0.07], // 初始位置 [x, y, z]
      "initial_orientation_euler_deg": [0, 0, 0], // 初始姿态 [roll, pitch, yaw]
      "px4_autolaunch": true,               // 是否自动启动 PX4
      "px4_dir": "/home/user/PX4-Autopilot", // PX4 源码目录
      "sim_speed_factor": 5.0               // 仿真加速倍率
    },
    ...
  ]
}

==========================
关键参数说明
==========================
渲染与性能参数：
  USE_RASTERIZATION = True    # True=栅格化（快），False=光追（慢但真实）
  RENDER_THROTTLE = True      # 启用渲染节流
  RENDER_MAX_FPS = 10.0       # 最大渲染帧率，影响相机输出频率
  CAMERA_RESOLUTION = (640, 640)  # 相机分辨率

录制参数：
  RECORD_ENABLE = False       # 全局录制开关
  RECORD_FPS = 10.0           # 录制帧率
  RECORD_DIR = "./recordings" # 录制目录

UAV 视觉参数：
  UAV_TRANSPARENCY_ENABLE = True  # 启用 UAV 透明（多机避免遮挡）
  UAV_TRANSPARENCY_ALPHA = 0.0    # 透明度（0=完全透明）
  DISABLE_UAV_UAV_COLLISION = True # 禁用 UAV 之间碰撞

ROS2 参数：
  ROS2_ENABLE = False          # 启用 ROS2 后端（通常由控制器使用）
  ROS2_CAMERA_ENABLE = False   # 启用 ROS2 相机话题发布
  ROS2_SENSOR_ENABLE = False   # 启用 ROS2 传感器话题发布

仿真加速（APP_CONFIG.extra_args）：
  --/app/runLoops/main/rateLimitEnabled=false   # 禁用主循环速率限制
  --/app/runLoops/rendering/rateLimitEnabled=false  # 禁用渲染速率限制
  --/physics/updateToUsd=false  # 禁用物理到 USD 同步（提升性能）
  --/app/asyncRendering=true    # 启用异步渲染

==========================
环境变量
==========================
PEGASUS_SESSION_TS    - 会话时间戳（用于日志分组）

==========================
接口调用示例（Python）
==========================
import requests
import json

base = "http://127.0.0.1:8081"

# 获取 UAV0 的位姿
resp = requests.get(f"{base}/uav/0/pose")
print(resp.json())

# 获取 UAV0 的图像（JSON 格式）
resp = requests.get(f"{base}/uav/0/image")
data = resp.json()
print(f"Image size: {data['width']}x{data['height']}")

# 获取 UAV0 的图像和位姿（同步快照）
resp = requests.get(f"{base}/uav/0/all")
all_data = resp.json()
print(f"Position: {all_data['pose']['position']}")

# 重置 UAV0 到指定位置
resp = requests.post(f"{base}/uav/0/reset", json={
    "position": [0.0, 0.0, 2.0],
    "yaw_deg": 90.0
})
print(resp.json())

# 查询 PX4 是否就绪
resp = requests.get(f"{base}/uav/0/px4/ready")
print(f"PX4 ready: {resp.json()['ready']}")

==========================
接口调用示例（curl）
==========================
# 获取位姿
curl http://127.0.0.1:8081/uav/0/pose

# 获取图像（PNG）
curl -o image.png http://127.0.0.1:8081/uav/0/image.png

# 获取综合信息
curl http://127.0.0.1:8081/uav/0/all

# 重置 UAV 位置
curl -X POST http://127.0.0.1:8081/uav/0/reset \
  -H "Content-Type: application/json" \
  -d '{"position":[0,0,2],"yaw_deg":0}'

# 查询 PX4 状态
curl http://127.0.0.1:8081/uav/0/px4/ready

# 控制录制
curl http://127.0.0.1:8081/record/start
curl http://127.0.0.1:8081/record/stop
curl http://127.0.0.1:8081/record/status

==========================
系统调优建议
==========================
# 提高 UDP buffer（MAVLink 通信）
sudo sysctl -w net.core.rmem_max=26214400
sudo sysctl -w net.core.wmem_max=26214400
sudo sysctl -w net.core.rmem_default=26214400
sudo sysctl -w net.core.wmem_default=26214400

# 减少 TIME_WAIT 连接
sudo sysctl -w net.ipv4.tcp_fin_timeout=15
sudo sysctl -w net.ipv4.tcp_tw_reuse=1

"""

# Imports to start Isaac Sim from this script
from isaacsim import SimulationApp
import carb

import sys, os
import logging
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


LOG_ENABLE = True
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")

# 支持 PEGASUS_SESSION_TS 环境变量进行日志分组
_SESSION_TS = os.environ.get("PEGASUS_SESSION_TS", str(int(time.time())))
LOG_SESSION_DIR = os.path.join(LOG_DIR, _SESSION_TS, "isaac")
LOG_FILE_PATH = os.path.join(LOG_SESSION_DIR, f"8_camera_vehicle.log")


def _setup_file_logger():
    if not LOG_ENABLE:
        return None
    os.makedirs(LOG_SESSION_DIR, exist_ok=True)
    logger = logging.getLogger("pegasus.sim")
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        fh = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(fh)
        logger.propagate = False
    return logger


_FILE_LOGGER = _setup_file_logger()


def _log_to_file(level: str, msg):
    if _FILE_LOGGER is None:
        return
    try:
        text = msg if isinstance(msg, str) else repr(msg)
        if level == "error":
            _FILE_LOGGER.error(text)
        elif level == "warn":
            _FILE_LOGGER.warning(text)
        else:
            _FILE_LOGGER.info(text)
    except Exception:
        pass


_CARB_ORIG_LOG_INFO = getattr(carb, "log_info", None)
_CARB_ORIG_LOG_WARN = getattr(carb, "log_warn", None)
_CARB_ORIG_LOG_ERROR = getattr(carb, "log_error", None)


def _carb_log_info(msg):
    _log_to_file("info", msg)
    try:
        if _CARB_ORIG_LOG_INFO is not None:
            _CARB_ORIG_LOG_INFO(msg)
    except Exception:
        pass


def _carb_log_warn(msg):
    _log_to_file("warn", msg)
    try:
        if _CARB_ORIG_LOG_WARN is not None:
            _CARB_ORIG_LOG_WARN(msg)
    except Exception:
        pass


def _carb_log_error(msg):
    _log_to_file("error", msg)
    try:
        if _CARB_ORIG_LOG_ERROR is not None:
            _CARB_ORIG_LOG_ERROR(msg)
    except Exception:
        pass


carb.log_info = _carb_log_info
carb.log_warn = _carb_log_warn
carb.log_error = _carb_log_error

_log_to_file("info", f"log_file={LOG_FILE_PATH}")


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
USD_PATH = SIMULATION_ENVIRONMENTS['Flat Plane']
# USD_PATH = "/home/user/Downloads/Demos/AEC/BrownstoneDemo/World_BrownstoneDemopack_Morning(20Gb).usd"
# USD_PATH = "/home/user/export/Demo_Environment.usda"
# USD_PATH = "/home/user/Downloads/export/extract.usd"
# -------------------------
# Recording (global switch)
# -------------------------
# 全局录制开关：是否以10fps频率保存PNG图片和CSV数据到本地（时间戳命名）
RECORD_ENABLE = False
RECORD_FPS = 10.0
RECORD_DIR = os.path.join(os.path.dirname(__file__), "recordings")

UAV_TRANSPARENCY_ENABLE = True
UAV_TRANSPARENCY_ALPHA = 0.0
DISABLE_UAV_UAV_COLLISION = True

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
        # ========== 仿真加速：禁用实时节流 ==========
        # "--/app/runLoops/main/rateLimitEnabled=false",
        # "--/app/runLoops/main/rateLimitFrequency=0",
        # "--/app/runLoops/rendering/rateLimitEnabled=false",
        # "--/physics/updateToUsd=false",  # 减少 USD 同步开销
        # "--/app/asyncRendering=true",
        # "--/app/asyncRenderingLowLatency=false",
    ],
}
simulation_app = SimulationApp(APP_CONFIG)

# -----------------------------------
# The actual script should start here
# -----------------------------------

import omni.timeline
import omni.usd
from pxr import Usd, UsdGeom, UsdPhysics, PhysxSchema, Sdf, Vt, UsdShade
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
                # "px4_vehicle_type": v.get("px4_vehicle_model", "gazebo-classic_iris_pg"),
                "px4_vehicle_model": "gazebo-classic_iris_pg",
            }
            mavlink_config = PX4MavlinkBackendConfig(px4_cfg_dict)

            # ROS2 backend (namespaced) for sensors/graphical
            ros2_ns = v.get("ros2_namespace", f"uav{vid}")

            # Configure vehicle sensors
            camera = MonocularCamera(f"front_camera_{vid}", config={"depth": True, "frequency": 10.0, "resolution": self.camera_resolution})
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

        if DISABLE_UAV_UAV_COLLISION:
            try:
                self._configure_collision_filtering()
            except Exception as e:
                carb.log_warn(f"Collision filtering setup failed: {e}")

            self.world.reset()

        if UAV_TRANSPARENCY_ENABLE:
            try:
                self._configure_uav_transparency(alpha=UAV_TRANSPARENCY_ALPHA)
            except Exception as e:
                carb.log_warn(f"Transparency setup failed: {e}")

    def _iter_uav_colliders(self, prim_root: Usd.Prim):
        for prim in Usd.PrimRange(prim_root):
            try:
                if prim.HasAPI(UsdPhysics.CollisionAPI) or prim.HasAPI(PhysxSchema.PhysxCollisionAPI):
                    yield prim
            except Exception:
                continue

    def _iter_uav_gprims(self, prim_root: Usd.Prim):
        for prim in Usd.PrimRange(prim_root):
            try:
                gp = UsdGeom.Gprim(prim)
                if gp:
                    yield gp
            except Exception:
                continue

    def _configure_collision_filtering(self):
        stage = omni.usd.get_context().get_stage()
        carb.log_info("[CollisionFiltering] start")
        root = stage.GetDefaultPrim()
        if not root:
            root = stage.GetPrimAtPath("/World")
            carb.log_info("[CollisionFiltering] defaultPrim missing, using /World")
        base = root.GetPath()
        uav_group_path = base.AppendChild("UAVCollisionGroup")
        world_group_path = base.AppendChild("WorldCollisionGroup")
        uav_group = UsdPhysics.CollisionGroup.Define(stage, uav_group_path)
        world_group = UsdPhysics.CollisionGroup.Define(stage, world_group_path)
        carb.log_info(f"[CollisionFiltering] groups created: {uav_group_path} , {world_group_path}")
        try:
            rel = uav_group.CreateFilteredGroupsRel()
            rel.SetTargets([uav_group_path])
            carb.log_info("[CollisionFiltering] UAVGroup self-filter applied")
        except Exception as e:
            carb.log_warn(f"[CollisionFiltering] set self-filter failed: {e}")

        uav_coll_api = uav_group.GetCollidersCollectionAPI()
        uav_includes = uav_coll_api.GetIncludesRel()
        world_coll_api = world_group.GetCollidersCollectionAPI()
        world_includes = world_coll_api.GetIncludesRel()
        uav_includes.SetTargets([])
        world_includes.SetTargets([])

        assigned_uav = 0
        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("ros2_namespace", f"uav{vid}")
            prim_root = stage.GetPrimAtPath(f"/World/{ns}")
            if not prim_root or not prim_root.IsValid():
                carb.log_warn(f"[CollisionFiltering] UAV root invalid: /World/{ns}")
                continue
            local_count = 0
            for col in self._iter_uav_colliders(prim_root):
                try:
                    uav_includes.AddTarget(col.GetPath())
                    local_count += 1
                except Exception as e:
                    carb.log_warn(f"[CollisionFiltering] assign UAV collider failed at {col.GetPath()}: {e}")
                    continue
            assigned_uav += local_count
            carb.log_info(f"[CollisionFiltering] UAV namespace /World/{ns} assigned {local_count} colliders")
        assigned_world = 0
        world_prim = stage.GetPrimAtPath("/World")
        uav_ns_set = {v.get("ros2_namespace", f"uav{int(v.get('vehicle_id',0))}") for v in self.config.get("vehicles", [])}
        for prim in Usd.PrimRange(world_prim):
            try:
                ppath = prim.GetPath().pathString
                if any(ppath.startswith(f"/World/{ns}") for ns in uav_ns_set):
                    continue
                if not prim.HasAPI(UsdPhysics.CollisionAPI) and not prim.HasAPI(PhysxSchema.PhysxCollisionAPI):
                    continue
                world_includes.AddTarget(prim.GetPath())
                assigned_world += 1
            except Exception as e:
                carb.log_warn(f"[CollisionFiltering] assign World collider failed at {prim.GetPath()}: {e}")
                continue
        carb.log_info(f"[CollisionFiltering] summary: UAV colliders={assigned_uav}, World colliders={assigned_world}")

    def _configure_uav_transparency(self, alpha: float = 0.35):
        alpha = float(max(0.0, min(alpha, 1.0)))
        stage = omni.usd.get_context().get_stage()
        carb.log_info(f"[Transparency] start alpha={alpha}")
        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("ros2_namespace", f"uav{vid}")
            prim_root = stage.GetPrimAtPath(f"/World/{ns}")
            if not prim_root or not prim_root.IsValid():
                carb.log_warn(f"[Transparency] UAV root invalid: /World/{ns}")
                continue
            hidden_count = 0
            if alpha <= 0.0:
                try:
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
                                hidden_count += 1
                                carb.log_info(f"[Transparency] hidden {p.GetPath().pathString}")
                        except Exception as e:
                            carb.log_warn(f"[Transparency] hide failed at {p.GetPath().pathString}: {e}")
                            continue
                except Exception as e:
                    carb.log_warn(f"[Transparency] traversal failed: {e}")
            gprim_count = 0
            opacity_set = 0
            shader_inputs = 0
            for gp in self._iter_uav_gprims(prim_root):
                gprim_count += 1
                try:
                    attr = gp.GetDisplayOpacityAttr()
                    if not attr:
                        attr = gp.CreateDisplayOpacityAttr()
                    attr.Set(Vt.FloatArray([alpha]))
                    opacity_set += 1
                    carb.log_info(f"[Transparency] gprim {gp.GetPrim().GetPath().pathString} opacity={alpha}")
                except Exception as e:
                    carb.log_warn(f"[Transparency] opacity set failed at {gp.GetPrim().GetPath().pathString}: {e}")
                try:
                    mba = UsdShade.MaterialBindingAPI(gp.GetPrim())
                    mat = None
                    try:
                        res = mba.ComputeBoundMaterial()
                        if isinstance(res, tuple):
                            mat = res[0]
                        else:
                            mat = res
                    except Exception:
                        try:
                            binding = mba.GetDirectBinding()
                            mat = binding.GetMaterial()
                        except Exception:
                            mat = None
                    if mat:
                        for child in mat.GetPrim().GetChildren():
                            try:
                                shader = UsdShade.Shader(child)
                                if not shader:
                                    continue
                                sid = shader.GetIdAttr().Get()
                                sid_str = str(sid).lower() if sid is not None else ""
                                if "usdpreviewsurface" in sid_str:
                                    inp = shader.CreateInput("opacity", Sdf.ValueTypeNames.Float)
                                    inp.Set(alpha)
                                    shader_inputs += 1
                                    carb.log_info(f"[Transparency] shader {child.GetPath().pathString} id={sid} input=opacity value={alpha}")
                                else:
                                    inp = shader.CreateInput("opacity_constant", Sdf.ValueTypeNames.Float)
                                    inp.Set(alpha)
                                    shader_inputs += 1
                                    carb.log_info(f"[Transparency] shader {child.GetPath().pathString} id={sid} input=opacity_constant value={alpha}")
                            except Exception as e:
                                carb.log_warn(f"[Transparency] shader input set failed at {child.GetPath().pathString}: {e}")
                except Exception as e:
                    carb.log_warn(f"[Transparency] material binding failed at {gp.GetPrim().GetPath().pathString}: {e}")
            carb.log_info(f"[Transparency] summary /World/{ns}: hidden={hidden_count}, gprims={gprim_count}, opacity_set={opacity_set}, shader_inputs={shader_inputs}")

class PegasusApp:
    """
    A Template class that serves as an example on how to build a simple Isaac Sim standalone App.
    """
    def __init__(self):
        """
        Method that initializes the PegasusApp and is used to setup the simulation environment.
        """
        # # ========== 仿真加速：运行时禁用节流设置 ==========
        # try:
        #     import carb.settings
        #     settings = carb.settings.get_settings()
        #     # 禁用主循环和渲染循环的速率限制
        #     settings.set("/app/runLoops/main/rateLimitEnabled", False)
        #     settings.set("/app/runLoops/main/rateLimitFrequency", 0)
        #     settings.set("/app/runLoops/rendering/rateLimitEnabled", False)
        #     # 禁用物理到 USD 的同步以减少开销
        #     settings.set("/physics/updateToUsd", False)
        #     # 异步渲染设置
        #     settings.set("/app/asyncRendering", True)
        #     settings.set("/app/asyncRenderingLowLatency", False)
        #     carb.log_info("[PegasusApp] Simulation acceleration settings applied: rate limiting disabled")
        # except Exception as e:
        #     carb.log_warn(f"[PegasusApp] Failed to apply acceleration settings: {e}")

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
    def _start_http_server(self, host: str = "127.0.0.1", port: int = 8081):
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

        @app.route('/uav/<int:uav_id>/depth.png', methods=['GET'])
        def depth_png(uav_id: int):
            """获取深度图像（16位PNG格式）"""
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    carb.log_warn(f"HTTP depth.png vehicle_missing uav_id={uav_id}")
                    return jsonify({"error": f"Vehicle /World/uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    carb.log_warn(f"HTTP depth.png camera_missing uav_id={uav_id}")
                    return jsonify({"error": "Camera not found"}), 404
                if not getattr(cam, 'depth_enabled', False):
                    return jsonify({"error": "Depth not enabled for this camera"}), 400
                depth_img, ts = cam.get_last_depth_with_timestamp()
                if depth_img is None:
                    carb.log_warn(f"HTTP depth.png no_depth uav_id={uav_id}")
                    return jsonify({"error": "No depth image cached yet"}), 503
                png_bytes, b64, w, h, raw_b64 = self._depth_to_png_bytes_and_b64(depth_img)
                return Response(png_bytes, status=200, mimetype='image/png', headers={'Cache-Control': 'no-cache'})
            except Exception as e:
                carb.log_warn(f"HTTP depth.png error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "depth.png"}), 500

        @app.route('/uav/<int:uav_id>/depth', methods=['GET'])
        def depth(uav_id: int):
            """获取深度图像（JSON格式，包含16位PNG base64和原始float32 base64）"""
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    carb.log_warn(f"HTTP depth vehicle_missing uav_id={uav_id}")
                    return jsonify({"error": f"Vehicle /World/uav{uav_id} not found"}), 404
                cam = self._get_camera(vehicle)
                if cam is None:
                    carb.log_warn(f"HTTP depth camera_missing uav_id={uav_id}")
                    return jsonify({"error": "Camera not found"}), 404
                if not getattr(cam, 'depth_enabled', False):
                    return jsonify({"error": "Depth not enabled for this camera"}), 400
                depth_img, ts = cam.get_last_depth_with_timestamp()
                if depth_img is None:
                    carb.log_warn(f"HTTP depth no_depth uav_id={uav_id}")
                    return jsonify({"error": "No depth image cached yet"}), 503
                png_bytes, b64, w, h, raw_b64 = self._depth_to_png_bytes_and_b64(depth_img)
                payload = {
                    "uav_id": uav_id,
                    "timestamp": ts,
                    "width": w,
                    "height": h,
                    "encoding": "png16_base64",
                    "mime": "image/png",
                    "max_depth_m": 50.0,
                    "data": b64,
                    "data_url": "data:image/png;base64," + b64,
                    "raw_float32_base64": raw_b64,
                    "raw_dtype": "float32",
                }
                return jsonify(payload)
            except Exception as e:
                carb.log_warn(f"HTTP depth error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "depth"}), 500

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

        @app.route('/uav/<int:uav_id>/px4/recover', methods=['POST'])
        def px4_recover_route(uav_id: int):
            """PX4 崩溃恢复（原子操作，替代 hard_reset 和 relaunch）"""
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
                    backend.recover_px4()
                except Exception as e:
                    carb.log_warn(f"HTTP px4 recover exec_error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                    return jsonify({"status": "error", "message": f"px4 recover failed: {e}"}), 500
                return jsonify({"status": "success", "uav_id": uav_id, "message": "px4 recover ok"})
            except Exception as e:
                carb.log_warn(f"HTTP px4 recover error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "px4/recover"}), 500

        @app.route('/uav/<int:uav_id>/px4/ready', methods=['GET'])
        def px4_ready_route(uav_id: int):
            """查询 PX4 ready to takeoff 状态（从 PX4 日志检测）"""
            try:
                vehicle = self._get_vehicle(uav_id)
                if vehicle is None:
                    return jsonify({"status": "error", "message": f"Vehicle /World/uav{uav_id} not found", "ready": False}), 404
                try:
                    backend = None
                    for b in getattr(vehicle, "_backends", []):
                        if isinstance(b, PX4MavlinkBackend):
                            backend = b
                            break
                    if backend is None:
                        return jsonify({"status": "error", "message": "PX4 backend not found", "ready": False}), 404
                    ready = backend.px4_ready_to_takeoff
                    return jsonify({"status": "success", "uav_id": uav_id, "ready": ready})
                except Exception as e:
                    carb.log_warn(f"HTTP px4 ready exec_error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                    return jsonify({"status": "error", "message": f"px4 ready check failed: {e}", "ready": False}), 500
            except Exception as e:
                carb.log_warn(f"HTTP px4 ready error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "px4/ready", "ready": False}), 500

        @app.route('/uav/<int:uav_id>/px4/status', methods=['GET'])
        def px4_status_route(uav_id: int):
            """查询 PX4 后端详细状态（用于调试和状态机检测）"""
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

                    # 收集 PX4 后端状态
                    px4_status = {
                        "uav_id": uav_id,
                        "is_running": getattr(backend, "_is_running", False),
                        "connection_port": getattr(backend, "_connection_port", ""),
                        "mavlink_connected": backend._connection is not None,
                        "received_first_heartbeat": getattr(backend, "_received_first_hearbeat", False),
                        "received_first_actuator": getattr(backend, "_received_first_actuator", False),
                        "received_actuator": getattr(backend, "_received_actuator", False),
                        "px4_ready_to_takeoff": backend.px4_ready_to_takeoff,
                        "enable_lockstep": getattr(backend, "_enable_lockstep", False),
                        "px4_autolaunch": getattr(backend, "px4_autolaunch", False),
                    }

                    # PX4 进程状态
                    px4_tool = getattr(backend, "px4_tool", None)
                    if px4_tool is not None:
                        px4_process = getattr(px4_tool, "px4_process", None)
                        px4_status["px4_process_alive"] = px4_process is not None and px4_process.poll() is None
                        px4_status["px4_process_pid"] = px4_process.pid if px4_process else None
                        px4_status["px4_log_file"] = getattr(px4_tool, "_log_file_path", "")
                    else:
                        px4_status["px4_process_alive"] = False
                        px4_status["px4_process_pid"] = None
                        px4_status["px4_log_file"] = ""

                    return jsonify({"status": "success", "px4_backend": px4_status})
                except Exception as e:
                    carb.log_warn(f"HTTP px4 status exec_error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                    return jsonify({"status": "error", "message": f"px4 status check failed: {e}"}), 500
            except Exception as e:
                carb.log_warn(f"HTTP px4 status error uav_id={uav_id} err={e} trace={(traceback.format_exc() or '')[:400]}")
                return jsonify({"error": str(e), "endpoint": "px4/status"}), 500

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

    def _depth_to_png_bytes_and_b64(self, depth_img, max_depth=50.0):
        """Convert depth image (float32, meters) to 16-bit PNG.

        Args:
            depth_img: Depth image in meters (float32)
            max_depth: Maximum depth in meters for normalization (default 50m)

        Returns:
            (png_bytes, b64, width, height, raw_data_b64): PNG bytes, base64 PNG, dimensions, and raw float32 base64
        """
        arr = np.array(depth_img)
        h, w = arr.shape[:2] if arr.ndim >= 2 else (0, 0)

        # Save raw depth data as base64 encoded float32
        raw_depth_b64 = base64.b64encode(arr.astype(np.float32).tobytes()).decode('ascii')

        # Normalize depth to 0-65535 range for 16-bit PNG
        # Replace inf/nan with max_depth
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
            carb.log_warn(traceback.format_exc())
        return None

    def _iter_uav_colliders(self, prim_root: Usd.Prim):
        for prim in Usd.PrimRange(prim_root):
            if prim.HasAPI(UsdPhysics.CollisionAPI) or prim.HasAPI(PhysxSchema.PhysxCollisionAPI):
                yield prim

    def _iter_uav_gprims(self, prim_root: Usd.Prim):
        for prim in Usd.PrimRange(prim_root):
            gp = UsdGeom.Gprim(prim)
            if gp:
                yield gp

    def _configure_collision_filtering(self):
        mgr = getattr(self, "manager", None)
        if mgr is not None and hasattr(mgr, "_configure_collision_filtering"):
            return mgr._configure_collision_filtering()

        stage = omni.usd.get_context().get_stage()
        root = stage.GetDefaultPrim()
        if not root:
            root = stage.GetPrimAtPath("/World")
        base = root.GetPath()
        uav_group_path = base.AppendChild("UAVCollisionGroup")
        world_group_path = base.AppendChild("WorldCollisionGroup")
        uav_group = UsdPhysics.CollisionGroup.Define(stage, uav_group_path)
        world_group = UsdPhysics.CollisionGroup.Define(stage, world_group_path)
        try:
            rel = uav_group.CreateFilteredGroupsRel()
            rel.SetTargets([uav_group_path])
        except Exception:
            carb.log_warn(traceback.format_exc())

        uav_coll_api = uav_group.GetCollidersCollectionAPI()
        uav_includes = uav_coll_api.GetIncludesRel()
        world_coll_api = world_group.GetCollidersCollectionAPI()
        world_includes = world_coll_api.GetIncludesRel()
        uav_includes.SetTargets([])
        world_includes.SetTargets([])

        cfg = None
        try:
            cfg = getattr(getattr(self, "manager", None), "config", None)
        except Exception:
            cfg = None
        if cfg is None:
            cfg = load_config_strict()

        for v in cfg.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("ros2_namespace", f"uav{vid}")
            prim_root = stage.GetPrimAtPath(f"/World/{ns}")
            if not prim_root or not prim_root.IsValid():
                continue
            for col in self._iter_uav_colliders(prim_root):
                uav_includes.AddTarget(col.GetPath())

        world_prim = stage.GetPrimAtPath("/World")
        uav_ns_set = {v.get("ros2_namespace", f"uav{int(v.get('vehicle_id',0))}") for v in cfg.get("vehicles", [])}
        for prim in Usd.PrimRange(world_prim):
            ppath = prim.GetPath().pathString
            if any(ppath.startswith(f"/World/{ns}") for ns in uav_ns_set):
                continue
            if not prim.HasAPI(UsdPhysics.CollisionAPI) and not prim.HasAPI(PhysxSchema.PhysxCollisionAPI):
                continue
            world_includes.AddTarget(prim.GetPath())

    def _configure_uav_transparency(self, alpha: float = 0.35):
        alpha = float(max(0.0, min(alpha, 1.0)))
        stage = omni.usd.get_context().get_stage()
        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("ros2_namespace", f"uav{vid}")
            prim_root = stage.GetPrimAtPath(f"/World/{ns}")
            if not prim_root or not prim_root.IsValid():
                continue
            for gp in self._iter_uav_gprims(prim_root):
                try:
                    attr = gp.GetDisplayOpacityAttr()
                    if not attr:
                        attr = gp.CreateDisplayOpacityAttr()
                    attr.Set(Vt.FloatArray([alpha]))
                except Exception:
                    carb.log_warn(traceback.format_exc())

    def _get_camera(self, vehicle):
        try:
            for s in getattr(vehicle, "_graphical_sensors", []):
                if getattr(s, "sensor_type", "") == "MonocularCamera":
                    return s
        except Exception:
            carb.log_warn(traceback.format_exc())
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
                print(traceback.format_exc())
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
            carb.log_warn(traceback.format_exc())
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
