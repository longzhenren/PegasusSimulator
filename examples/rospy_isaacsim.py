#!/usr/bin/env python3
"""
Pegasus ROS2 Socket 控制接口文档

-概述
- 本脚本启动后保持悬停，并通过 TCP Socket 接收按行 JSON 命令。
- 同步保存传感器/图像/状态数据到会话目录（根目录为 `PEGASUS_OUTPUT_DIR` 或 `PEGASUS_SAVE_DIR`）：
  - 传感器：`<GLOBAL_OUTPUT_DIR>/sensors_{session_ts}/...`
  - 相机：`<GLOBAL_OUTPUT_DIR>/camera_{session_ts}/...`
  - 状态：`<GLOBAL_OUTPUT_DIR>/status_{session_ts}/...`

环境变量
- `PEGASUS_SAVE_DIR`：保存根目录，默认 `sensor_data`
- `PEGASUS_OUTPUT_DIR`：全局保存根目录（优先级高于 `PEGASUS_SAVE_DIR`）
- `PEGASUS_CMD_HOST`：命令 socket 监听地址，默认 `127.0.0.1`
- `PEGASUS_CMD_PORT`：命令 socket 监听端口，默认 `8989`
- `PEGASUS_CAMERA_RGB`：RGB 图像话题，默认 `/camera/color/image_raw`
- `PEGASUS_CAMERA_DEPTH`：深度图像话题，默认 `/camera/depth/image_raw`
 - `PEGASUS_SESSION_TS`：会话时间戳（覆盖内部自动生成）；也可用 CLI `--session-ts`

命令格式（每行一个 JSON，通过 TCP socket 发送）
- 通用：可指定 `filename`（写入状态/结果 JSON 到状态目录）
- `{"cmd":"set_session_ts","ts":1234567890,"filename":"set_session_ok.json"}`
  - 通过 socket 外部设定会话时间戳，立即切换保存路径为 `<GLOBAL_OUTPUT_DIR>/*_{ts}`，并将状态写入新的 `status_{ts}` 目录
- `{"cmd":"move_to","x":0.0,"y":0.0,"z":2.0,"filename":"move_to_ok.json"}`
- `{"cmd":"move_to_many","points":[[0,0,2],[1,0,2],[1,1,2]],"filename":"multi_ok.json"}`
- `{"cmd":"land","filename":"land_ok.json"}`
- `{"cmd":"get_position","filename":"position.json"}`
- `{"cmd":"get_status","filename":"status.json"}`
- `{"cmd":"get_sensors","filename":"my_sensors.json"}`
  - 若提供 `filename`，快照保存为 `sensor_data/sensors_{session_ts}/my_sensors.json`
- `{"cmd":"get_images","filename":"frame001"}`
  - 若提供 `filename`，RGB 保存为 `sensor_data/camera_{session_ts}/frame001.png`；深度保存为 `sensor_data/camera_{session_ts}/frame001_depth.png`
  - 也可分别指定：`filename_rgb`、`filename_depth`
- `{"cmd":"save_snapshot","filename":"snapshot_status.json"}`
  - 立即以当前时间戳保存一次传感器与图像快照，并写状态到 `filename`
- `{"cmd":"shutdown","filename":"shutdown_ok.json"}`
  - 请求优雅退出：停止悬停循环后写状态 JSON，再关闭节点

使用示例
1) 启动脚本（可选外部会话时间戳）：
   - `python3 examples/rospy_isaacsim.py --session-ts 1234567890`
2) 使用 `nc` 发送命令（所有状态 JSON 写入 `status_{session_ts}` 子目录）：
   - `nc 127.0.0.1 8989` 后逐行输入：
     - `{"cmd":"set_session_ts","ts":1234567890,"filename":"set_session_ok.json"}`
     - `{"cmd":"move_to","x":0,"y":0,"z":2,"filename":"move_to_ok.json"}`
     - `{"cmd":"get_sensors","filename":"test.json"}`
     - `{"cmd":"get_images","filename":"frame001"}`
     - `{"cmd":"shutdown","filename":"shutdown_ok.json"}`
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
import subprocess
import signal
import select
import socket
from typing import Optional, List, Dict, Any

import numpy as np
try:
    from PIL import Image as PILImage
except Exception:
    PILImage = None
from geometry_msgs.msg import PoseStamped
from mavros_msgs.msg import State, RCIn, PositionTarget
from mavros_msgs.srv import CommandBool, SetMode, CommandLong

# Additional sensor/image message types (generic, not IsaacSim-specific)
from sensor_msgs.msg import Image, Imu, NavSatFix, FluidPressure, MagneticField, Temperature
from geometry_msgs.msg import TwistStamped

# ---------------- Helper: spin rate ----------------
def spin_sleep(node: Node, hz: float):
    """Simple sleep maintaining callbacks via spin_once."""
    period = 1.0 / max(hz, 1.0)
    rclpy.spin_once(node, timeout_sec=0.0)
    time.sleep(period)

# ---------------- Global saving toggles & defaults ----------------
SAVE_ENABLED = True
SAVE_SENSORS = True
SAVE_IMAGES = True
SAVE_DEPTH = True
SAVE_DIR = os.environ.get("PEGASUS_SAVE_DIR", "sensor_data")
GLOBAL_OUTPUT_DIR = os.environ.get("PEGASUS_OUTPUT_DIR", SAVE_DIR)
SESSION_TS_GLOBAL: Optional[int] = None

# Camera topics (can be overridden by env vars)
CAMERA_RGB_TOPIC = os.environ.get("PEGASUS_CAMERA_RGB", "/camera/color/image_raw")
CAMERA_DEPTH_TOPIC = os.environ.get("PEGASUS_CAMERA_DEPTH", "/camera/depth/image_raw")

# Socket host/port (input commands). Commands may include a `filename` field to write status JSON.
DEFAULT_CMD_HOST = os.environ.get("PEGASUS_CMD_HOST", "0.0.0.0")
DEFAULT_CMD_PORT = int(os.environ.get("PEGASUS_CMD_PORT", "8989"))

# Utility: current unix seconds as int
def now_int() -> int:
    return int(time.time())

def ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _numpy_from_ros_image(msg: Image) -> Optional[np.ndarray]:
    try:
        dtype = None
        channels = 1
        enc = msg.encoding.lower() if msg.encoding else ""
        if enc in ("rgb8", "bgr8"):
            dtype = np.uint8
            channels = 3
        elif enc in ("mono8",):
            dtype = np.uint8
            channels = 1
        elif enc in ("16uc1", "mono16"):
            dtype = np.uint16
            channels = 1
        elif enc in ("32fc1",):
            dtype = np.float32
            channels = 1
        else:
            # Fallback assume 8-bit RGB if step matches width*3
            if msg.step == msg.width * 3:
                dtype = np.uint8
                channels = 3
            elif msg.step == msg.width * 2:
                dtype = np.uint16
                channels = 1
            elif msg.step == msg.width * 4:
                dtype = np.float32
                channels = 1
            else:
                dtype = np.uint8
                channels = 1
        arr = np.frombuffer(msg.data, dtype=dtype)
        if channels == 3:
            arr = arr.reshape((msg.height, msg.width, 3))
            if enc == "bgr8":
                arr = arr[:, :, ::-1]
        else:
            arr = arr.reshape((msg.height, msg.width))
        return arr
    except Exception:
        return None

def _save_depth_png(depth: np.ndarray, out_path: str, depth_max: float = 10.0):
    try:
        # normalize to 16-bit PNG
        arr = depth.copy()
        if arr.dtype == np.float32 or arr.dtype == np.float64:
            arr = np.nan_to_num(arr, nan=0.0, posinf=depth_max, neginf=0.0)
            arr = np.clip(arr, 0.0, depth_max)
            arr = (arr / depth_max * 65535.0).astype(np.uint16)
        elif arr.dtype == np.uint16:
            # assume millimeters; convert to meters based on plausible scale if values look large
            # If max > 1000, consider mm and scale to meters then to 16-bit range
            maxv = float(np.max(arr)) if arr.size else 0.0
            if maxv > 1000.0:
                arr_m = arr.astype(np.float32) / 1000.0
                arr_m = np.clip(arr_m, 0.0, depth_max)
                arr = (arr_m / depth_max * 65535.0).astype(np.uint16)
            else:
                arr = np.clip(arr, 0, 65535).astype(np.uint16)
        else:
            # unknown dtype; convert to 16-bit
            arr = arr.astype(np.uint16)
        if PILImage is not None:
            img = PILImage.fromarray(arr, mode='I;16')
            img.save(out_path)
        else:
            # Fallback: save as raw .npy if PIL missing
            np.save(out_path.replace('.png', '.npy'), arr)
    except Exception:
        try:
            np.save(out_path.replace('.png', '.npy'), depth)
        except Exception:
            pass

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
    def __init__(self, session_ts: Optional[int] = None):
        super().__init__("isaac_sim_nav_node")

        # --- params / buffers ---
        self.depth_max_dis = 10.0
        # self.bridge = CvBridge()
        self.current_pose = PoseStamped()
        self.current_state = State()

        # saving/session context
        # Allow external session timestamp override (env/CLI) or module global
        global SESSION_TS_GLOBAL
        if session_ts is not None:
            self.session_ts = session_ts
        elif SESSION_TS_GLOBAL is not None:
            self.session_ts = SESSION_TS_GLOBAL
        else:
            self.session_ts = now_int()
        # keep module global in sync
        SESSION_TS_GLOBAL = self.session_ts
        self.save_enabled = SAVE_ENABLED
        self.save_sensors = SAVE_SENSORS
        self.save_images = SAVE_IMAGES
        self.save_depth = SAVE_DEPTH
        self.save_root = GLOBAL_OUTPUT_DIR
        self.sensors_dir = os.path.join(self.save_root, f"sensors_{self.session_ts}")
        self.camera_dir = os.path.join(self.save_root, f"camera_{self.session_ts}")
        self.status_dir = os.path.join(self.save_root, f"status_{self.session_ts}")
        ensure_dir(self.sensors_dir)
        ensure_dir(self.camera_dir)
        ensure_dir(self.status_dir)

        # Socket command processing
        self.cmd_sock_host = DEFAULT_CMD_HOST
        self.cmd_sock_port = DEFAULT_CMD_PORT
        self.listen_sock = None
        self.client_socks = []
        self._sock_buffers = {}
        self._last_action_tag = None  # int seconds used to name saved files
        self._shutdown_requested = False

        # # --- subscribers (adjust topics to your bridge) ---
        best_effort_qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10
        )

        # --- subscribers ---
        self.create_subscription(State, "/mavros/state", self.state_cb, best_effort_qos)
        self.create_subscription(PoseStamped, "/mavros/local_position/pose", self.pose_cb, best_effort_qos)
        self.create_subscription(RCIn, "/mavros/rc/in", self.rc_in_callback, best_effort_qos)

        # Sensor topics
        self.last_imu: Optional[Imu] = None
        self.last_navsat: Optional[NavSatFix] = None
        self.last_pressure: Optional[FluidPressure] = None
        self.last_mag: Optional[MagneticField] = None
        self.last_temp: Optional[Temperature] = None
        self.last_twist: Optional[TwistStamped] = None
        self.create_subscription(Imu, "/mavros/imu/data", self.imu_cb, best_effort_qos)
        self.create_subscription(NavSatFix, "/mavros/global_position/raw/fix", self.navsat_cb, best_effort_qos)
        self.create_subscription(FluidPressure, "/mavros/imu/static_pressure", self.pressure_cb, best_effort_qos)
        self.create_subscription(MagneticField, "/mavros/imu/mag", self.mag_cb, best_effort_qos)
        self.create_subscription(Temperature, "/mavros/imu/temperature", self.temp_cb, best_effort_qos)
        self.create_subscription(TwistStamped, "/mavros/local_position/velocity", self.twist_cb, best_effort_qos)

        # Image topics (rgb + depth)
        self.last_rgb_img: Optional[Image] = None
        self.last_depth_img: Optional[Image] = None
        try:
            self.create_subscription(Image, CAMERA_RGB_TOPIC, self.rgb_cb, best_effort_qos)
            self.create_subscription(Image, CAMERA_DEPTH_TOPIC, self.depth_cb, best_effort_qos)
        except Exception:
            # Topics may not exist; keep subscribers optional
            pass

        # --- publishers ---
        # 与原始代码一致：发布到 /nav/velocity，由 vel.py 转发到 /mavros/setpoint_raw/local
        self.vel_pub = self.create_publisher(PositionTarget, "/nav/velocity", 10)
        # （若要直接控制 MAVROS，可以改成 /mavros/setpoint_raw/local，但你要求保持现状）

        # --- integrated velocity bridge (/nav/velocity -> /mavros/setpoint_raw/local) ---
        # 直接在本节点内实现原 examples/vel.py 的功能，保持完全一致（30Hz 发布）
        self.mavros_vel_pub = self.create_publisher(PositionTarget, "/mavros/setpoint_raw/local", 10)
        self._bridge_last_velocity = PositionTarget()
        self._bridge_last_velocity.coordinate_frame = PositionTarget.FRAME_LOCAL_NED
        self.create_subscription(PositionTarget, "/nav/velocity", self._bridge_nav_velocity_cb, 10)
        self._bridge_timer = self.create_timer(1.0 / 30.0, self._bridge_publish_velocity)
        try:
            self.get_logger().info("Integrated velocity bridge active (30 Hz)")
        except Exception:
            pass

        # --- services ---
        self.arming_client    = self.create_client(CommandBool, "/mavros/cmd/arming")
        self.set_mode_client  = self.create_client(SetMode,    "/mavros/set_mode")
        self.cmdlong_client   = self.create_client(CommandLong,"/mavros/cmd/command")

        # FCU 连接等待改为外部方法，在启动 MAVROS 之后调用

        # --- 先发送一段时间的零速度（原始逻辑 for _ in range(100)）---
        for _ in range(100):
            self.pub_velocity(0.0, 0.0, 0.0, 0.0)
            spin_sleep(self, 20.0)

        # --- “请求对象”在 ROS2 中直接用 .Request() ---
        self.offb_set_mode = SetMode.Request()
        self.arm_cmd       = CommandBool.Request()
        self.last_req_time = self.get_clock().now()

        # # image buffers
        # self.rgb_image   = np.zeros((480, 640, 3), dtype=np.uint8)
        # self.depth_image = np.zeros((480, 640, 1), dtype=np.float32)

        # RC
        self.current_rc_in = None
        self.offboard_channel_value = 2000  # channel 7 (index 6)

        # 轨迹（可选：若你要跑轨迹）
        self.waypoints = []
        self.init_height = 0.5  # 起飞高度

        # Hover target maintained while waiting for FIFO commands
        self.hover_target = None  # (x, y, z)

        # Setup Socket
        self._setup_socket()

    # ---------- Callbacks ----------
    def state_cb(self, msg: State):
        self.current_state = msg

    def pose_cb(self, msg: PoseStamped):
        self.current_pose = msg

    def rc_in_callback(self, msg: RCIn):
        self.current_rc_in = msg
        if msg.channels and len(msg.channels) > 6:
            self.offboard_channel_value = msg.channels[6]

    # --- Sensor callbacks ---
    def imu_cb(self, msg: Imu):
        self.last_imu = msg

    def navsat_cb(self, msg: NavSatFix):
        self.last_navsat = msg

    def pressure_cb(self, msg: FluidPressure):
        self.last_pressure = msg

    def mag_cb(self, msg: MagneticField):
        self.last_mag = msg

    def temp_cb(self, msg: Temperature):
        self.last_temp = msg

    def twist_cb(self, msg: TwistStamped):
        self.last_twist = msg

    def rgb_cb(self, msg: Image):
        self.last_rgb_img = msg

    def depth_cb(self, msg: Image):
        self.last_depth_img = msg

    # ---------- Core API ----------
    def reset(self):
        # Step 0) Warm-up setpoints (防止进入 RTL/Failsafe)
        self.get_logger().info("Publishing initial dummy setpoints to prevent RTL...")
        for _ in range(50):  # 2.5s at 20Hz
            self.pub_position(0.0, 0.0, 1.0)
            spin_sleep(self, 20.0)
        self.get_logger().info("Initial setpoints published.")

        # Step 1) 切换 OFFBOARD
        self.offb_set_mode.custom_mode = "OFFBOARD"
        while self.current_state.mode != "OFFBOARD":
            resp = call_service_sync(self, self.set_mode_client, self.offb_set_mode, timeout_sec=5.0)
            if resp and getattr(resp, "mode_sent", False):
                self.get_logger().info("***** OFFBOARD enabled *****")
            spin_sleep(self, 2.0)
        self.get_logger().info("Vehicle in OFFBOARD mode.")

        # Step 2) 解锁
        self.arm_cmd.value = True
        while not self.current_state.armed:
            resp = call_service_sync(self, self.arming_client, self.arm_cmd, timeout_sec=5.0)
            if resp and getattr(resp, "success", False):
                self.get_logger().info("***** Vehicle armed *****")
                break
            spin_sleep(self, 2.0)
        self.get_logger().info("Vehicle armed.")

        # Step 3) 起飞逻辑（和你原来一样）
        self.start_height = self.current_pose.pose.position.z
        self.get_logger().info(f"Current height: {self.start_height}")
        target_x, target_y, target_z = 0.0, 0.0, self.start_height + self.init_height
        has_reached_initial_point = False
        while not has_reached_initial_point:
            self.get_logger().info(f"Publishing takeoff setpoint...{target_x}, {target_y}, {target_z}")
            self.pub_position(target_x, target_y, target_z)
            dx = self.current_pose.pose.position.x - target_x
            dy = self.current_pose.pose.position.y - target_y
            dz = self.current_pose.pose.position.z - target_z
            if math.sqrt(dx*dx + dy*dy + dz*dz) < 0.1:
                has_reached_initial_point = True
                self.get_logger().info("***** Reached initial point *****")
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.05)
        self.get_logger().info("Takeoff complete.")

        # set hover target to current pose after takeoff
        self.hover_target = (
            self.current_pose.pose.position.x,
            self.current_pose.pose.position.y,
            self.current_pose.pose.position.z,
        )

    def reboot_px4(self):
        # 原始逻辑：尝试先上“上锁 false”→ 发送 MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN → 等待重连
        try:
            # arming false
            if self.arming_client.wait_for_service(timeout_sec=5.0):
                disarm_req = CommandBool.Request()
                disarm_req.value = False
                try:
                    call_service_sync(self, self.arming_client, disarm_req, timeout_sec=3.0)
                except Exception:
                    pass
        except Exception as e:
            self.get_logger().warn(f"Disarm before reboot: {e}")

        # MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN = 246, param1=1 reboot autopilot
        if not self.cmdlong_client.wait_for_service(timeout_sec=5.0):
            self.get_logger().warn("cmd/command not available, skip reboot.")
            return
        req = CommandLong.Request()
        req.command = 246
        req.param1 = 1.0
        try:
            call_service_sync(self, self.cmdlong_client, req, timeout_sec=5.0)
        except Exception as e:
            self.get_logger().warn(f"PX4 reboot command failed: {e}")

        # 等待 MAVROS 重连（原始用 state.wait_for_message 循环）
        start = self.get_clock().now()
        timeout = Duration(seconds=20.0)
        while (self.get_clock().now() - start) < timeout:
            try:
                state = wait_for_message(self, "/mavros/state", State, timeout=2.0)
                if getattr(state, "connected", False):
                    break
            except Exception:
                pass
        # 再确认服务可用
        self.arming_client.wait_for_service(timeout_sec=10.0)
        self.set_mode_client.wait_for_service(timeout_sec=10.0)
        self.cmdlong_client.wait_for_service(timeout_sec=10.0)
        self.get_logger().info("PX4 reboot complete; MAVROS reconnected.")

    def wait_for_fcu_connection(self):
        """在 MAVROS 启动后阻塞等待 FCU 连接成功。"""
        try:
            self.get_logger().info("Waiting for FCU connection...")
        except Exception:
            pass
        while rclpy.ok() and not getattr(self.current_state, "connected", False):
            spin_sleep(self, 20.0)
        try:
            self.get_logger().info("FCU connected.")
        except Exception:
            pass

    def pub_position(self, target_x, target_y, target_z):
        # 与原始一致：位置控制 + FRAME_LOCAL_NED，通过桥接主题发送
        target_position = PositionTarget()
        target_position.header.stamp = self.get_clock().now().to_msg()
        target_position.coordinate_frame = PositionTarget.FRAME_LOCAL_NED
        target_position.type_mask = (
            PositionTarget.IGNORE_VX | PositionTarget.IGNORE_VY | PositionTarget.IGNORE_VZ |
            PositionTarget.IGNORE_AFX | PositionTarget.IGNORE_AFY | PositionTarget.IGNORE_AFZ |
            PositionTarget.IGNORE_YAW | PositionTarget.IGNORE_YAW_RATE
        )
        target_position.position.x = target_x
        target_position.position.y = target_y
        target_position.position.z = target_z
        self.vel_pub.publish(target_position)

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
        self.vel_pub.publish(target_velocity)

    # ---------- Integrated bridge callbacks ----------
    def _bridge_nav_velocity_cb(self, msg: PositionTarget):
        # 完全复刻 vel.py 的行为：接收 /nav/velocity 的最新指令并缓存
        self._bridge_last_velocity = msg

    def _bridge_publish_velocity(self):
        # 以固定频率将最近一次的速度指令发布到 MAVROS 入口主题
        try:
            self.mavros_vel_pub.publish(self._bridge_last_velocity)
        except Exception:
            pass

    # ---------- Saving helpers ----------
    def _build_sensor_snapshot(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        # State
        st = self.current_state
        data["state"] = {
            "connected": getattr(st, "connected", False),
            "armed": getattr(st, "armed", False),
            "mode": getattr(st, "mode", "")
        }
        # Pose
        p = self.current_pose.pose
        data["pose"] = {
            "position": {"x": p.position.x, "y": p.position.y, "z": p.position.z},
            "orientation": {"x": p.orientation.x, "y": p.orientation.y, "z": p.orientation.z, "w": p.orientation.w}
        }
        # IMU
        if self.last_imu is not None:
            imu = self.last_imu
            data["imu"] = {
                "linear_acceleration": {"x": imu.linear_acceleration.x, "y": imu.linear_acceleration.y, "z": imu.linear_acceleration.z},
                "angular_velocity": {"x": imu.angular_velocity.x, "y": imu.angular_velocity.y, "z": imu.angular_velocity.z}
            }
        # GPS
        if self.last_navsat is not None:
            gps = self.last_navsat
            data["gps"] = {
                "status": int(getattr(gps.status, "status", 0)),
                "latitude": gps.latitude,
                "longitude": gps.longitude,
                "altitude": gps.altitude,
                "position_covariance_type": int(getattr(gps, "position_covariance_type", 0))
            }
        # Barometer
        if self.last_pressure is not None:
            pr = self.last_pressure
            data["barometer"] = {"fluid_pressure": pr.fluid_pressure, "variance": pr.variance}
        # Magnetometer
        if self.last_mag is not None:
            mg = self.last_mag
            data["magnetometer"] = {"x": mg.magnetic_field.x, "y": mg.magnetic_field.y, "z": mg.magnetic_field.z}
        # Temperature
        if self.last_temp is not None:
            tp = self.last_temp
            data["temperature"] = {"temperature": tp.temperature, "variance": tp.variance}
        # Velocity
        if self.last_twist is not None:
            tw = self.last_twist
            data["velocity"] = {
                "linear": {"x": tw.twist.linear.x, "y": tw.twist.linear.y, "z": tw.twist.linear.z},
                "angular": {"x": tw.twist.angular.x, "y": tw.twist.angular.y, "z": tw.twist.angular.z}
            }
        return data

    def save_snapshot(self, action_tag: Optional[int] = None):
        if not self.save_enabled:
            return
        ts = action_tag if action_tag is not None else now_int()
        # Save sensors/state
        if self.save_sensors:
            try:
                ensure_dir(self.sensors_dir)
                snap = self._build_sensor_snapshot()
                out_json = os.path.join(self.sensors_dir, f"sensors_data_{ts}.json")
                with open(out_json, 'w') as f:
                    json.dump(snap, f)
            except Exception as e:
                self.get_logger().warn(f"Failed to save sensors snapshot: {e}")
        # Save images
        if self.save_images and self.last_rgb_img is not None:
            try:
                ensure_dir(self.camera_dir)
                rgb_arr = _numpy_from_ros_image(self.last_rgb_img)
                if rgb_arr is not None and PILImage is not None:
                    rgb_img = PILImage.fromarray(rgb_arr)
                    rgb_path = os.path.join(self.camera_dir, f"camera_image_{ts}.png")
                    rgb_img.save(rgb_path)
                elif rgb_arr is not None:
                    np.save(os.path.join(self.camera_dir, f"camera_image_{ts}.npy"), rgb_arr)
            except Exception as e:
                self.get_logger().warn(f"Failed to save RGB image: {e}")
        # Save depth
        if self.save_depth and self.last_depth_img is not None:
            try:
                depth_arr = _numpy_from_ros_image(self.last_depth_img)
                if depth_arr is not None:
                    depth_path = os.path.join(self.camera_dir, f"depth_image_{ts}.png")
                    _save_depth_png(depth_arr, depth_path, self.depth_max_dis)
            except Exception as e:
                self.get_logger().warn(f"Failed to save depth image: {e}")

    def save_named(self, sensors_filename: Optional[str] = None, rgb_filename: Optional[str] = None, depth_filename: Optional[str] = None) -> Dict[str, Any]:
        saved: Dict[str, Any] = {}
        if not self.save_enabled:
            return saved
        # sensors
        if self.save_sensors and sensors_filename:
            try:
                ensure_dir(self.sensors_dir)
                name = sensors_filename if sensors_filename.endswith('.json') else sensors_filename + '.json'
                out_json = os.path.join(self.sensors_dir, name)
                with open(out_json, 'w') as f:
                    json.dump(self._build_sensor_snapshot(), f)
                saved['sensors'] = out_json
            except Exception as e:
                self.get_logger().warn(f"Failed to save named sensors file: {e}")
        # rgb
        if self.save_images and self.last_rgb_img is not None and rgb_filename:
            try:
                ensure_dir(self.camera_dir)
                name = rgb_filename
                if PILImage is not None:
                    if not name.endswith('.png'):
                        name += '.png'
                    rgb_arr = _numpy_from_ros_image(self.last_rgb_img)
                    if rgb_arr is not None:
                        PILImage.fromarray(rgb_arr).save(os.path.join(self.camera_dir, name))
                        saved['rgb'] = os.path.join(self.camera_dir, name)
                else:
                    if not name.endswith('.npy'):
                        name += '.npy'
                    rgb_arr = _numpy_from_ros_image(self.last_rgb_img)
                    if rgb_arr is not None:
                        np.save(os.path.join(self.camera_dir, name), rgb_arr)
                        saved['rgb'] = os.path.join(self.camera_dir, name)
            except Exception as e:
                self.get_logger().warn(f"Failed to save named RGB file: {e}")
        # depth
        if self.save_depth and self.last_depth_img is not None and depth_filename:
            try:
                ensure_dir(self.camera_dir)
                name = depth_filename
                if not name.endswith('.png'):
                    name += '.png'
                depth_arr = _numpy_from_ros_image(self.last_depth_img)
                if depth_arr is not None:
                    _save_depth_png(depth_arr, os.path.join(self.camera_dir, name), self.depth_max_dis)
                    saved['depth'] = os.path.join(self.camera_dir, name)
            except Exception as e:
                self.get_logger().warn(f"Failed to save named depth file: {e}")
        return saved

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

    # ---------- Socket & Control Interfaces ----------
    def _setup_socket(self):
        try:
            self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listen_sock.bind((self.cmd_sock_host, self.cmd_sock_port))
            self.listen_sock.listen(8)
            self.listen_sock.setblocking(False)
            self.get_logger().info(f"Socket listening at {self.cmd_sock_host}:{self.cmd_sock_port}")
        except Exception as e:
            self.get_logger().warn(f"Failed to setup socket: {e}")
            try:
                if self.listen_sock:
                    self.listen_sock.close()
            except Exception:
                pass
            self.listen_sock = None
            self.client_socks = []
            self._sock_buffers = {}

    def _read_socket_commands(self) -> List[Dict[str, Any]]:
        cmds: List[Dict[str, Any]] = []
        if self.listen_sock is None:
            return cmds
        try:
            rlist, _, _ = select.select([self.listen_sock] + list(self.client_socks), [], [], 0.0)
            for s in rlist:
                if s is self.listen_sock:
                    # accept new connections (non-blocking loop)
                    while True:
                        try:
                            conn, addr = self.listen_sock.accept()
                            conn.setblocking(False)
                            self.client_socks.append(conn)
                            self._sock_buffers[conn] = b""
                            self.get_logger().info(f"Client connected: {addr}")
                        except BlockingIOError:
                            break
                        except Exception:
                            break
                else:
                    try:
                        data = s.recv(65536)
                        if not data:
                            # closed
                            try:
                                s.close()
                            except Exception:
                                pass
                            if s in self.client_socks:
                                self.client_socks.remove(s)
                            self._sock_buffers.pop(s, None)
                            continue
                        buf = self._sock_buffers.get(s, b"") + data
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                cmd = json.loads(line.decode('utf-8'))
                                cmds.append(cmd)
                            except Exception:
                                self.get_logger().warn("Invalid JSON command in socket")
                        self._sock_buffers[s] = buf
                    except Exception:
                        try:
                            s.close()
                        except Exception:
                            pass
                        if s in self.client_socks:
                            self.client_socks.remove(s)
                        self._sock_buffers.pop(s, None)
        except Exception:
            pass
        return cmds

    def _close_socket(self):
        try:
            for s in list(self.client_socks):
                try:
                    s.close()
                except Exception:
                    pass
            self.client_socks = []
            if self.listen_sock:
                try:
                    self.listen_sock.close()
                except Exception:
                    pass
        except Exception:
            pass

    def update_session_ts(self, new_ts: int):
        """Update the current session timestamp and rebuild save directories immediately."""
        try:
            global SESSION_TS_GLOBAL
            SESSION_TS_GLOBAL = int(new_ts)
            self.session_ts = SESSION_TS_GLOBAL
            # rebuild directories
            self.sensors_dir = os.path.join(self.save_root, f"sensors_{self.session_ts}")
            self.camera_dir = os.path.join(self.save_root, f"camera_{self.session_ts}")
            self.status_dir = os.path.join(self.save_root, f"status_{self.session_ts}")
            ensure_dir(self.sensors_dir)
            ensure_dir(self.camera_dir)
            ensure_dir(self.status_dir)
            self.get_logger().info(f"Switched session to {self.session_ts}; new dirs ready.")
        except Exception as e:
            self.get_logger().warn(f"Failed to update session timestamp: {e}")

    def _write_status(self, filename: Optional[str], resp: Dict[str, Any]):
        if not filename:
            return
        try:
            ensure_dir(self.status_dir)
            name = filename if filename.endswith('.json') else filename + '.json'
            out_json = os.path.join(self.status_dir, name)
            with open(out_json, 'w') as f:
                json.dump(resp, f)
        except Exception as e:
            self.get_logger().warn(f"Failed to write status file {filename}: {e}")

    # Interfaces
    def get_position(self) -> Dict[str, float]:
        p = self.current_pose.pose.position
        return {"x": p.x, "y": p.y, "z": p.z}

    def get_status(self) -> Dict[str, Any]:
        st = self.current_state
        return {"connected": getattr(st, "connected", False), "armed": getattr(st, "armed", False), "mode": getattr(st, "mode", "")}

    def move_to(self, x: float, y: float, z: float, save: bool = True):
        tag = now_int()
        self.pub_position(x, y, z)
        self.hover_target = (x, y, z)
        self._last_action_tag = tag
        if save:
            self.save_snapshot(tag)

    def move_to_many(self, points: List[List[float]], threshold: float = 0.1, save_each: bool = True):
        for pt in points:
            if not isinstance(pt, (list, tuple)) or len(pt) < 3:
                continue
            x, y, z = float(pt[0]), float(pt[1]), float(pt[2])
            tag = now_int()
            reached = False
            while rclpy.ok() and not reached:
                self.pub_position(x, y, z)
                dx = self.current_pose.pose.position.x - x
                dy = self.current_pose.pose.position.y - y
                dz = self.current_pose.pose.position.z - z
                if math.sqrt(dx*dx + dy*dy + dz*dz) < threshold:
                    reached = True
                rclpy.spin_once(self, timeout_sec=0.05)
                time.sleep(0.05)
            self.hover_target = (x, y, z)
            self._last_action_tag = tag
            if save_each:
                self.save_snapshot(tag)

    def land(self, descend_rate: float = 0.2, save: bool = True):
        # Descend to start_height (takeoff baseline) then disarm
        tag = now_int()
        target_z = self.start_height
        while rclpy.ok() and (self.current_pose.pose.position.z - target_z) > 0.05:
            x = self.current_pose.pose.position.x
            y = self.current_pose.pose.position.y
            z = max(target_z, self.current_pose.pose.position.z - descend_rate * 0.1)
            self.pub_position(x, y, z)
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.1)
        if save:
            self.save_snapshot(tag)
        # Disarm
        try:
            disarm_req = CommandBool.Request()
            disarm_req.value = False
            call_service_sync(self, self.arming_client, disarm_req, timeout_sec=5.0)
        except Exception:
            pass

    def get_sensor_data(self, filename: Optional[str] = None) -> Dict[str, Any]:
        data = self._build_sensor_snapshot()
        saved = {}
        if filename:
            saved = self.save_named(sensors_filename=filename)
        if saved:
            data['saved'] = saved
        return data

    def get_image_data(self, filename: Optional[str] = None, filename_rgb: Optional[str] = None, filename_depth: Optional[str] = None) -> Dict[str, Any]:
        info: Dict[str, Any] = {}
        if self.last_rgb_img is not None:
            info["rgb"] = {"width": self.last_rgb_img.width, "height": self.last_rgb_img.height, "encoding": self.last_rgb_img.encoding}
        if self.last_depth_img is not None:
            info["depth"] = {"width": self.last_depth_img.width, "height": self.last_depth_img.height, "encoding": self.last_depth_img.encoding}
        # saving using provided names
        rgb_name = filename_rgb
        depth_name = filename_depth
        if filename and not rgb_name:
            rgb_name = filename if PILImage is not None else filename  # extension handled in save_named
        if filename and not depth_name:
            depth_name = filename + "_depth"
        saved = self.save_named(rgb_filename=rgb_name, depth_filename=depth_name)
        if saved:
            info['saved'] = saved
        return info

    def process_command(self, cmd: Dict[str, Any]):
        ctype = cmd.get("cmd") or cmd.get("type")
        resp: Dict[str, Any] = {"ok": True}
        try:
            if ctype == "set_session_ts":
                new_ts = cmd.get("ts") or cmd.get("session_ts")
                if new_ts is None:
                    raise ValueError("set_session_ts requires 'ts' or 'session_ts'")
                new_ts = int(new_ts)
                # update session immediately; status will be written to the NEW status dir
                self.update_session_ts(new_ts)
                resp.update({"message": "session updated", "session_ts": new_ts})
            elif ctype == "move_to":
                x = float(cmd.get("x"))
                y = float(cmd.get("y"))
                z = float(cmd.get("z"))
                self.move_to(x, y, z, save=True)
                resp.update({"position": self.get_position()})
            elif ctype == "move_to_many":
                pts = cmd.get("points") or cmd.get("waypoints") or []
                self.move_to_many(pts, save_each=True)
                resp.update({"position": self.get_position()})
            elif ctype == "land":
                self.land(save=True)
                resp.update({"status": self.get_status()})
            elif ctype == "get_position":
                resp.update({"position": self.get_position()})
            elif ctype == "get_status":
                resp.update({"status": self.get_status()})
            elif ctype == "get_sensors":
                fname = cmd.get("filename")
                resp.update({"sensors": self.get_sensor_data(filename=fname)})
            elif ctype == "get_images":
                fname = cmd.get("filename")
                fname_rgb = cmd.get("filename_rgb")
                fname_depth = cmd.get("filename_depth")
                resp.update({"images": self.get_image_data(filename=fname, filename_rgb=fname_rgb, filename_depth=fname_depth)})
            elif ctype == "save_snapshot":
                tag = now_int()
                self.save_snapshot(tag)
                resp.update({"saved_at": tag})
            elif ctype == "shutdown":
                self._shutdown_requested = True
                resp.update({"message": "Shutdown requested"})
            else:
                resp = {"ok": False, "error": f"Unknown cmd: {ctype}"}
        except Exception as e:
            resp = {"ok": False, "error": str(e)}
        # write status to filename if provided
        self._write_status(cmd.get("filename"), resp)

    def hover_and_socket_loop(self):
        self.get_logger().info("Entering hover + Socket command loop...")
        hz = 30.0  # 与桥接发布频率保持一致
        while rclpy.ok() and not self._shutdown_requested:
            # maintain hover
            if self.hover_target is not None:
                x, y, z = self.hover_target
                self.pub_position(x, y, z)
            # process incoming commands
            for cmd in self._read_socket_commands():
                self.process_command(cmd)
            # aligned saving based on last action tag, optional periodic
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(1.0 / hz)


# =======================================================================
#                                   main
# =======================================================================
def main():
    # Parse optional CLI args for session timestamp and pass remaining to ROS
    import argparse
    parser = argparse.ArgumentParser(description="Pegasus ROS2 IsaacSimEnv")
    parser.add_argument("--session-ts", dest="session_ts", type=int, default=None, help="Override session timestamp (int)")
    args, unknown = parser.parse_known_args()
    # Prefer env var if provided
    env_session = os.environ.get("PEGASUS_SESSION_TS")
    session_ts = None
    if env_session:
        try:
            session_ts = int(env_session)
        except Exception:
            session_ts = None
    if session_ts is None:
        session_ts = args.session_ts
    # keep module global aligned so set_session_ts starts from known value
    try:
        global SESSION_TS_GLOBAL
        if session_ts is not None:
            SESSION_TS_GLOBAL = int(session_ts)
    except Exception:
        SESSION_TS_GLOBAL = None
    # Initialize ROS with remaining args so --ros-args still works
    rclpy.init(args=unknown)
    env = None
    vel_process = None  # 已废弃：不再外部启动 vel.py，保留变量仅作占位
    mavros_process = None
    try:
        env = IsaacSimEnv(session_ts=session_ts)
        env.get_logger().info("IsaacSimEnv node started.")

        # 启动 MAVROS（确保在发送命令前已运行）
        try:
            mavros_cmd = [
                '/opt/ros/humble/bin/ros2', 'launch', 'mavros', 'px4.launch',
                'fcu_url:=udp://:14540@'
            ]
            mavros_process = subprocess.Popen(mavros_cmd, env=os.environ.copy(), preexec_fn=os.setsid)
            env.get_logger().info("MAVROS (px4.launch) started.")
        except Exception as e:
            env.get_logger().warn(f"Failed to start MAVROS: {e}")

        # 在 MAVROS 启动后等待 FCU 连接
        env.wait_for_fcu_connection()

        # 内置桥已启用，无需外部进程
        env.get_logger().info("Integrated velocity bridge running; external vel.py disabled.")

        env.reset()
        env.get_logger().info("Environment reset complete; switching to hover+Socket mode.")

        # Hover + Socket commands loop (wait for external commands)
        env.hover_and_socket_loop()

    except Exception as e:
        # 异常情况：记录错误但继续执行清理流程
        if env is not None:
            try:
                env.get_logger().error(f"Unhandled exception: {e}")
            except Exception:
                pass
        else:
            print(f"Unhandled exception in main(): {e}")

    finally:
        # 无需清理外部 vel.py 进程（未启动）

        # 清理 MAVROS 进程（正常或异常结束都执行）
        if mavros_process is not None and mavros_process.poll() is None:
            if env is not None:
                try:
                    env.get_logger().info("Shutting down MAVROS (px4.launch)...")
                except Exception:
                    pass
            try:
                pgid = os.getpgid(mavros_process.pid)
            except Exception:
                pgid = None
            if pgid is not None:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    try:
                        os.killpg(pgid, sig)
                        mavros_process.wait(timeout=5)
                        break
                    except Exception:
                        pass
                if mavros_process.poll() is None:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except Exception:
                        pass
            else:
                try:
                    mavros_process.terminate()
                    mavros_process.wait(timeout=5)
                except Exception:
                    try:
                        mavros_process.kill()
                    except Exception:
                        pass

        # 释放 ROS 资源
        if env is not None:
            try:
                env._close_socket()
                env.destroy_node()
            except Exception:
                pass
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()