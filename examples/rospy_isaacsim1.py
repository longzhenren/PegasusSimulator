#!/usr/bin/env python3
from __future__ import print_function, division

# --- ROS 2 & System Imports ---
import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
from rclpy.clock import Clock
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from cv_bridge import CvBridge
import numpy as np
import json
import time
import os, sys, subprocess, math

# ROS 2 message/service imports
# from px4_msgs.msg import OffboardControlMode, TrajectorySetpoint, VehicleCommand, VehicleLocalPosition, VehicleStatus

from sensor_msgs.msg import Image as RosImage
from geometry_msgs.msg import PoseStamped
from mavros_msgs.msg import State, RCIn, PositionTarget
from mavros_msgs.srv import CommandBool, SetMode, CommandLong

# ---------------- Helper: spin rate ----------------
def spin_sleep(node: Node, hz: float):
    """Simple sleep maintaining callbacks via spin_once."""
    period = 1.0 / max(hz, 1.0)
    rclpy.spin_once(node, timeout_sec=0.0)
    time.sleep(period)

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
    def __init__(self):
        super().__init__("isaac_sim_nav_node")

        # --- params / buffers ---
        self.depth_max_dis = 10.0
        self.bridge = CvBridge()
        self.current_pose = PoseStamped()
        self.current_state = State()
        self.takeoff_ref_z = None  # ✅ 新增：记录起飞稳定后高度

        # # --- subscribers (adjust topics to your bridge) ---
        # best_effort_qos = QoSProfile(
        #     reliability=ReliabilityPolicy.BEST_EFFORT,
        #     durability=DurabilityPolicy.TRANSIENT_LOCAL,
        #     history=HistoryPolicy.KEEP_LAST,
        #     depth=1
        # )

        best_effort_qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10
        )

        # --- subscribers ---
        self.create_subscription(State, "/mavros/state", self.state_cb, best_effort_qos)
        self.create_subscription(PoseStamped, "/mavros/local_position/pose", self.pose_cb, best_effort_qos)
        self.create_subscription(RCIn, "/mavros/rc/in", self.rc_in_callback, best_effort_qos)

        # --- publishers ---
        # 与原始代码一致：发布到 /nav/velocity，由 vel.py 转发到 /mavros/setpoint_raw/local
        self.vel_pub = self.create_publisher(PositionTarget, "/nav/velocity", 10)
        # （若要直接控制 MAVROS，可以改成 /mavros/setpoint_raw/local，但你要求保持现状）

        # --- services ---
        self.arming_client    = self.create_client(CommandBool, "/mavros/cmd/arming")
        self.set_mode_client  = self.create_client(SetMode,    "/mavros/set_mode")
        self.cmdlong_client   = self.create_client(CommandLong,"/mavros/cmd/command")

        # --- wait FCU connection (原始逻辑：Rate+阻塞直到 connected) ---
        self.get_logger().info("Waiting for FCU connection...")
        while rclpy.ok() and not getattr(self.current_state, "connected", False):
            spin_sleep(self, 20.0)
        self.get_logger().info("FCU connected.")

        # --- 先发送一段时间的零速度（原始逻辑 for _ in range(100)）---
        for _ in range(100):
            self.pub_velocity(0.0, 0.0, 0.0, 0.0)
            spin_sleep(self, 20.0)

        # --- “请求对象”在 ROS2 中直接用 .Request() ---
        self.offb_set_mode = SetMode.Request()
        self.arm_cmd       = CommandBool.Request()
        self.last_req_time = self.get_clock().now()

        # image buffers
        self.rgb_image   = np.zeros((480, 640, 3), dtype=np.uint8)
        self.depth_image = np.zeros((480, 640, 1), dtype=np.float32)

        # RC
        self.current_rc_in = None
        self.offboard_channel_value = 2000  # channel 7 (index 6)

        # 轨迹（可选：若你要跑轨迹）
        self.waypoints = []

    # ---------- Callbacks ----------
    def state_cb(self, msg: State):
        self.current_state = msg

    def pose_cb(self, msg: PoseStamped):
        self.current_pose = msg

    def rc_in_callback(self, msg: RCIn):
        self.current_rc_in = msg
        if msg.channels and len(msg.channels) > 6:
            self.offboard_channel_value = msg.channels[6]

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
        target_x, target_y, target_z = 0.0, 0.0, 0.5 + self.start_height
        has_reached_initial_point = False
        stable_counter = 0  # ✅ 新增：用于检测稳定悬停
        stable_threshold = 10  # 连续若干帧认为稳定

        while not has_reached_initial_point:
            self.pub_position(target_x, target_y, target_z)
            dx = self.current_pose.pose.position.x - target_x
            dy = self.current_pose.pose.position.y - target_y
            dz = self.current_pose.pose.position.z - target_z
            dist = math.sqrt(dx*dx + dy*dy + dz*dz)
            if dist < 0.05:
                stable_counter += 1
            else:
                stable_counter = 0
            if stable_counter > stable_threshold:
                has_reached_initial_point = True
                self.takeoff_ref_z = self.current_pose.pose.position.z  # ✅ 记录起飞稳定高度
                self.get_logger().info(f"***** Reached initial point (ref_z = {self.takeoff_ref_z:.3f}) *****")
            rclpy.spin_once(self, timeout_sec=0.05)
            time.sleep(0.05)
        self.get_logger().info("Takeoff complete.")

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

    # ---------- Step / Action（保持原始含义） ----------
    def step(self, action_str):
        ang_vel, lin_vel, done = self.normalize_action(action_str)

        if done:
            self.pub_velocity(0.0, 0.0, 0.0, 0.0)
            self.get_logger().info("Task Finished by 'Done' action.")
        else:
            vx, vy, vz = lin_vel
            yaw_rate = ang_vel
            self.pub_velocity(vx, vy, vz, yaw_rate)

        time.sleep(1.0)  # 原始 rospy.sleep(1)
        return done

    # ---------- Helpers ----------
    def normalize_action(self, action):
        done = False
        vx = vy = vz = yaw_rate = 0.0
        if action == 'up':
            vz = +0.15   # ENU up
        elif action == 'down':
            vz = -0.15
        elif action == 'turn_left':
            yaw_rate = np.deg2rad(+15.0)
        elif action == 'turn_right':
            yaw_rate = np.deg2rad(-15.0)
        elif action == 'forward':
            vx = +0.15
        elif action == 'Done':
            done = True
        return yaw_rate, [vx, vy, vz], done

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
        for raw, preprocessed in zip(raw_logs, preprocessed_logs):
            x_enu, y_enu, z_enu, roll_enu, yaw_enu, pitch_enu = raw
            x_local, y_local, z_local, roll_local, yaw_local, pitch_local = preprocessed
            # ENU->NED + cm->m（保持你给的转换）
            x_ned =  y_local / 100.0
            y_ned =  x_local / 100.0
            z_ned = -z_local / 100.0
            # ✅ 新增：将高度转换为相对起飞后稳定位置
            if self.takeoff_ref_z is not None:
                z_ned = z_ned - self.takeoff_ref_z
            waypoints.append([x_ned, y_ned, z_ned])
        return waypoints

    def run_trajectory(self):
        for x, y, z in self.waypoints:
            self.pub_position(x, y, z)
            self.get_logger().info(f"Go to waypoint: ({x:.2f}, {y:.2f}, {z:.2f})")
            time.sleep(1.0)  # 同原始风格的简单等待


# =======================================================================
#                                   main
# =======================================================================
def main():
    rclpy.init()
    env = IsaacSimEnv()
    env.get_logger().info("IsaacSimEnv node started.")

    env.reset()
    env.get_logger().info("Environment reset complete.")

    env.load_trajectory("/home/user/PegasusSimulator/examples/vla_output.json")
    env.run_trajectory()

    # 示例：简单执行一个“forward→turn_left→Done”的序列（占位，模拟原始 step 循环）
    for a in ["forward", "turn_left", "Done"]:
        env.get_logger().info(f"Action: {a}")
        done = env.step(a)
        if done:
            break

    env.get_logger().info("Episode finished.")
    env.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()