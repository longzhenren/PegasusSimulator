#!/usr/bin/env python3
import time
from collections import deque

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from mavros_msgs.msg import State, PositionTarget
from sensor_msgs.msg import Imu, NavSatFix
from geometry_msgs.msg import PoseStamped

class OffboardChecker(Node):
    def __init__(self):
        super().__init__('offboard_checker')

        # QoS profiles
        self.qos_rel = QoSProfile(depth=10, history=HistoryPolicy.KEEP_LAST,
                                  reliability=ReliabilityPolicy.RELIABLE)
        self.qos_be  = QoSProfile(depth=10, history=HistoryPolicy.KEEP_LAST,
                                  reliability=ReliabilityPolicy.BEST_EFFORT)

        # State & telemetry
        self.state: State | None = None
        self.imu_ok = False
        self.pose_ok = False
        self.gps_ok = False

        # Setpoint monitors (use dual QoS to avoid mismatch)
        self.last_setpoint_msg: PositionTarget | None = None
        self.setpoint_times = deque(maxlen=100)

        # Subscribers (match MAVROS QoS reality)
        self.create_subscription(State, "/mavros/state", self.state_cb, self.qos_rel)          # usually RELIABLE
        self.create_subscription(Imu, "/mavros/imu/data", self.imu_cb, self.qos_be)            # BEST_EFFORT
        self.create_subscription(PoseStamped, "/mavros/local_position/pose", self.pose_cb, self.qos_be)  # BEST_EFFORT
        self.create_subscription(NavSatFix, "/mavros/global_position/raw/fix", self.gps_cb, self.qos_be) # BEST_EFFORT

        # Setpoint: subscribe with BOTH QoS to be safe
        self.create_subscription(PositionTarget, "/mavros/setpoint_raw/local", self.setpoint_cb, self.qos_rel)
        self.create_subscription(PositionTarget, "/mavros/setpoint_raw/local", self.setpoint_cb, self.qos_be)

        # Periodic checker
        self.timer = self.create_timer(1.0, self.check)

        self.get_logger().info("OffboardChecker v2 started — QoS fixed (BE for sensors, dual for setpoint).")

    # --- Callbacks ---
    def state_cb(self, msg: State):
        self.state = msg

    def imu_cb(self, msg: Imu):
        # Minimal sanity check
        if not any(map(self._is_nan, [msg.linear_acceleration.x, msg.angular_velocity.x])):
            self.imu_ok = True

    def pose_cb(self, msg: PoseStamped):
        if not self._is_nan(msg.pose.position.z):
            self.pose_ok = True

    def gps_cb(self, msg: NavSatFix):
        # status/status>0 表示有 fix（不同固件略有差异，这里只要有数据就认为OK）
        if not self._is_nan(msg.latitude) and not self._is_nan(msg.longitude):
            self.gps_ok = True

    def setpoint_cb(self, msg: PositionTarget):
        self.last_setpoint_msg = msg
        self.setpoint_times.append(time.monotonic())

    # --- Utils ---
    @staticmethod
    def _is_nan(x):
        try:
            return x != x
        except Exception:
            return True

    def _setpoint_hz(self):
        if len(self.setpoint_times) < 2:
            return 0.0
        dt = self.setpoint_times[-1] - self.setpoint_times[0]
        if dt <= 0:
            return 0.0
        return (len(self.setpoint_times) - 1) / dt

    def _setpoint_summary(self):
        msg = self.last_setpoint_msg
        if not msg:
            return "NO MSG"
        # Which control fields are actually used (mask 1=ignore)
        mask = msg.type_mask
        use_pos = not (mask & (PositionTarget.IGNORE_PX | PositionTarget.IGNORE_PY | PositionTarget.IGNORE_PZ))
        use_vel = not (mask & (PositionTarget.IGNORE_VX | PositionTarget.IGNORE_VY | PositionTarget.IGNORE_VZ))
        use_yaw = not (mask & PositionTarget.IGNORE_YAW)
        use_yawrate = not (mask & PositionTarget.IGNORE_YAW_RATE)
        parts = [f"frame={msg.coordinate_frame}",
                 f"use_pos={use_pos}",
                 f"use_vel={use_vel}",
                 f"use_yaw={use_yaw}",
                 f"use_yawrate={use_yawrate}"]
        if use_pos:
            parts.append(f"pos=({msg.position.x:.2f},{msg.position.y:.2f},{msg.position.z:.2f})")
        if use_vel:
            parts.append(f"vel=({msg.velocity.x:.2f},{msg.velocity.y:.2f},{msg.velocity.z:.2f})")
        return " | ".join(parts)

    # --- Periodic check ---
    def check(self):
        # State
        if not self.state:
            self.get_logger().warn("No /mavros/state yet")
            return

        self.get_logger().info(f"STATE connected={self.state.connected} armed={self.state.armed} guided={self.state.guided} mode={self.state.mode}")

        # Sensors
        self.get_logger().info(f"SENSORS imu={'OK' if self.imu_ok else 'MISSING'} "
                               f"pose={'OK' if self.pose_ok else 'MISSING'} "
                               f"gps={'OK' if self.gps_ok else 'MISSING'}")

        # Setpoint stream
        hz = self._setpoint_hz()
        sp_sum = self._setpoint_summary()
        self.get_logger().info(f"SETPOINT  hz≈{hz:.1f}  {sp_sum}")

        # OFFBOARD readiness quick verdict
        ready = (hz >= 2.0) and self.pose_ok and self.imu_ok
        verdict = "READY for OFFBOARD" if ready else "NOT READY for OFFBOARD"
        why = []
        if hz < 2.0:           why.append("setpoint hz<2")
        if not self.pose_ok:   why.append("no local_position")
        if not self.imu_ok:    why.append("no imu")
        if why:
            verdict += "  (" + ", ".join(why) + ")"
        self.get_logger().info(f"VERDICT: {verdict}")

def main():
    rclpy.init()
    node = OffboardChecker()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == "__main__":
    main()