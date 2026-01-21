#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# -*- coding: utf-8 -*-
"""
OFFBOARD Mode Dynamics Collector (Fallback Option)
===================================================

This is a fallback collector that uses OFFBOARD mode instead of Mission mode.
Use this when Mission mode is not working properly.

OFFBOARD mode sends position setpoints directly to PX4, giving more control
but requiring continuous communication.

Usage:
    python3 offboard_dynamics_collector.py \
        --input-dir ~/trajectories \
        --out-dir ~/recordings \
        --uav-ids 0,1,2,3
"""

import argparse
import csv
import glob
import json
import math
import os
import queue
import shutil
import subprocess
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ROS2 imports
try:
    import rclpy
    from rclpy.node import Node
    from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
    from geometry_msgs.msg import PoseStamped, TwistStamped
    from mavros_msgs.msg import State, ExtendedState, PositionTarget
    from mavros_msgs.srv import CommandBool, SetMode, CommandTOL
    HAS_ROS2 = True
except ImportError:
    HAS_ROS2 = False
    print("[WARNING] ROS2 not available.")


# Constants
DEFAULT_SCALE = 0.01
MIN_WAYPOINT_DIST = 0.35
POSITION_TOLERANCE = 0.5  # meters
SETPOINT_RATE = 20  # Hz


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg, flush=True)
    return log_msg


@dataclass(frozen=True)
class TrajPoint:
    x: float
    y: float
    z: float
    roll_deg: float
    yaw_deg: float
    pitch_deg: float


@dataclass
class DynamicsData:
    timestamp: float = 0.0
    pos_x: float = 0.0
    pos_y: float = 0.0
    pos_z: float = 0.0
    att_w: float = 1.0
    att_x: float = 0.0
    att_y: float = 0.0
    att_z: float = 0.0
    linvel_x: float = 0.0
    linvel_y: float = 0.0
    linvel_z: float = 0.0
    angvel_x: float = 0.0
    angvel_y: float = 0.0
    angvel_z: float = 0.0


def _iter_json_files(input_dir: Path, pattern: str) -> List[Path]:
    files = sorted([Path(p) for p in glob.glob(str(input_dir / pattern))])
    return [p for p in files if p.is_file() and p.suffix.lower() == ".json"]


def _load_preprocessed_xyz(json_path: Path) -> List[TrajPoint]:
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs")
    if not isinstance(logs, list):
        raise ValueError("missing preprocessed_logs")
    logs = logs[::2]
    pts: List[TrajPoint] = []
    for row in logs:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        pts.append(TrajPoint(float(row[1]), float(row[0]), float(row[2]),
                             float(row[3]), float(row[4]), float(row[5])))
    if not pts:
        raise ValueError("no valid points")
    return pts


def _load_init_point_xyz(json_path: Path) -> TrajPoint:
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    raw_logs = obj.get("raw_logs", [])
    if not raw_logs:
        raise ValueError("missing init point")
    init = raw_logs[0]
    return TrajPoint(float(init[1]), float(init[0]), float(init[2]),
                     float(init[3]), float(init[4]), float(init[5]))


def _transform_points(pts: List[TrajPoint], scale: float,
                      base_x: float, base_y: float, base_z: float,
                      z_down: bool) -> List[TrajPoint]:
    out = []
    for p in pts:
        x = base_x * scale + p.x * scale
        y = base_y * scale + p.y * scale
        z = base_z * scale - p.z * scale if z_down else base_z * scale + p.z * scale
        out.append(TrajPoint(x, y, z, p.roll_deg, p.yaw_deg, p.pitch_deg))
    return out


def _filter_close_points(pts: List[TrajPoint], min_dist: float = MIN_WAYPOINT_DIST) -> List[TrajPoint]:
    if len(pts) <= 2:
        return pts
    filtered = [pts[0]]
    for i in range(1, len(pts) - 1):
        last = filtered[-1]
        curr = pts[i]
        dist = math.sqrt((curr.x-last.x)**2 + (curr.y-last.y)**2 + (curr.z-last.z)**2)
        if dist >= min_dist:
            filtered.append(curr)
    if len(pts) > 1:
        filtered.append(pts[-1])
    return filtered


def _safe_name(path: Path) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in path.stem) or "traj"


def _write_csv(rows: List[Dict], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("")
        return
    keys = list(rows[0].keys())
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


class OffboardCommander:
    """OFFBOARD mode controller using position setpoints"""

    def __init__(self, node: Node, uav_id: int, namespace_prefix: str = "/uav"):
        self.node = node
        self.uav_id = int(uav_id)
        self.prefix = f"{namespace_prefix}{self.uav_id}"

        self.state: Optional[State] = None
        self.ext_state: Optional[ExtendedState] = None
        self.latest_pose: Optional[PoseStamped] = None
        self.latest_velocity: Optional[TwistStamped] = None

        # Target position for setpoint streaming
        self.target_x = 0.0
        self.target_y = 0.0
        self.target_z = 2.5
        self.streaming = False
        self._stream_thread: Optional[threading.Thread] = None

        qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
            durability=DurabilityPolicy.VOLATILE,
        )

        # Subscribers
        self.node.create_subscription(State, f"{self.prefix}/mavros/state", self._state_cb, qos)
        self.node.create_subscription(ExtendedState, f"{self.prefix}/mavros/extended_state", self._ext_state_cb, qos)
        self.node.create_subscription(PoseStamped, f"{self.prefix}/mavros/local_position/pose", self._pose_cb, qos)
        self.node.create_subscription(TwistStamped, f"{self.prefix}/mavros/local_position/velocity_body", self._velocity_cb, qos)

        # Publisher for position setpoints
        self.setpoint_pub = self.node.create_publisher(
            PoseStamped,
            f"{self.prefix}/mavros/setpoint_position/local",
            10
        )

        # Services
        self.arming_client = self.node.create_client(CommandBool, f"{self.prefix}/mavros/cmd/arming")
        self.set_mode_client = self.node.create_client(SetMode, f"{self.prefix}/mavros/set_mode")
        self.land_client = self.node.create_client(CommandTOL, f"{self.prefix}/mavros/cmd/land")

    def _state_cb(self, msg): self.state = msg
    def _ext_state_cb(self, msg): self.ext_state = msg
    def _pose_cb(self, msg): self.latest_pose = msg
    def _velocity_cb(self, msg): self.latest_velocity = msg

    def get_dynamics(self) -> DynamicsData:
        data = DynamicsData(timestamp=time.time())
        if self.latest_pose:
            data.pos_x = self.latest_pose.pose.position.x
            data.pos_y = self.latest_pose.pose.position.y
            data.pos_z = self.latest_pose.pose.position.z
            data.att_w = self.latest_pose.pose.orientation.w
            data.att_x = self.latest_pose.pose.orientation.x
            data.att_y = self.latest_pose.pose.orientation.y
            data.att_z = self.latest_pose.pose.orientation.z
        if self.latest_velocity:
            data.linvel_x = self.latest_velocity.twist.linear.x
            data.linvel_y = self.latest_velocity.twist.linear.y
            data.linvel_z = self.latest_velocity.twist.linear.z
            data.angvel_x = self.latest_velocity.twist.angular.x
            data.angvel_y = self.latest_velocity.twist.angular.y
            data.angvel_z = self.latest_velocity.twist.angular.z
        return data

    def get_position(self) -> Tuple[float, float, float]:
        if self.latest_pose is None:
            return (0, 0, 0)
        return (
            self.latest_pose.pose.position.x,
            self.latest_pose.pose.position.y,
            self.latest_pose.pose.position.z,
        )

    def wait_connected(self, timeout: float = 60.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.state and self.state.connected:
                return True
            time.sleep(0.1)
        return False

    def is_armed(self) -> bool:
        return self.state.armed if self.state else False

    def is_landed(self) -> bool:
        return (self.ext_state.landed_state == 1) if self.ext_state else False

    def _call_service(self, client, req, timeout: float = 10.0):
        if not client.wait_for_service(timeout_sec=10.0):
            raise TimeoutError("service not available")
        fut = client.call_async(req)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if fut.done():
                return fut.result()
            time.sleep(0.05)
        raise TimeoutError("service call timeout")

    def set_mode(self, mode: str, timeout: float = 10.0) -> bool:
        try:
            req = SetMode.Request()
            req.custom_mode = mode
            self._call_service(self.set_mode_client, req, timeout)
            return True
        except Exception:
            return False

    def arm(self, arm: bool = True, timeout: float = 10.0) -> bool:
        try:
            req = CommandBool.Request()
            req.value = arm
            resp = self._call_service(self.arming_client, req, timeout)
            return resp.success
        except Exception:
            return False

    def start_setpoint_stream(self):
        """Start streaming position setpoints (required for OFFBOARD mode)"""
        if self.streaming:
            return
        self.streaming = True

        def stream_loop():
            rate = 1.0 / SETPOINT_RATE
            while self.streaming:
                msg = PoseStamped()
                msg.header.stamp = self.node.get_clock().now().to_msg()
                msg.header.frame_id = "map"
                msg.pose.position.x = self.target_x
                msg.pose.position.y = self.target_y
                msg.pose.position.z = self.target_z
                msg.pose.orientation.w = 1.0
                self.setpoint_pub.publish(msg)
                time.sleep(rate)

        self._stream_thread = threading.Thread(target=stream_loop, daemon=True)
        self._stream_thread.start()

    def stop_setpoint_stream(self):
        self.streaming = False
        if self._stream_thread:
            self._stream_thread.join(timeout=1.0)

    def set_target(self, x: float, y: float, z: float):
        """Set target position for setpoint stream"""
        self.target_x = x
        self.target_y = y
        self.target_z = z

    def wait_position_reached(self, x: float, y: float, z: float,
                               tolerance: float = POSITION_TOLERANCE,
                               timeout: float = 60.0) -> bool:
        """Wait until current position is within tolerance of target"""
        self.set_target(x, y, z)
        deadline = time.time() + timeout
        while time.time() < deadline:
            curr = self.get_position()
            dist = math.sqrt((curr[0]-x)**2 + (curr[1]-y)**2 + (curr[2]-z)**2)
            if dist < tolerance:
                return True
            time.sleep(0.1)
        return False

    def takeoff(self, altitude: float = 2.5, timeout: float = 60.0) -> bool:
        """Takeoff using OFFBOARD mode"""
        # Set initial position to current + altitude
        curr = self.get_position()
        self.set_target(curr[0], curr[1], altitude)

        # Start streaming before switching to OFFBOARD
        self.start_setpoint_stream()
        time.sleep(2)  # PX4 needs some setpoints before accepting OFFBOARD

        # Switch to OFFBOARD
        if not self.set_mode("OFFBOARD"):
            ts_log(f"[UAV{self.uav_id}]", "Failed to set OFFBOARD mode", "WARN")
            return False

        # Arm
        for _ in range(5):
            if self.arm():
                break
            time.sleep(1)

        if not self.is_armed():
            ts_log(f"[UAV{self.uav_id}]", "Failed to arm", "WARN")
            return False

        # Wait for altitude
        return self.wait_position_reached(curr[0], curr[1], altitude, timeout=timeout)

    def land(self, timeout: float = 120.0) -> bool:
        """Land the vehicle"""
        self.stop_setpoint_stream()
        self.set_mode("AUTO.LAND")
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_landed() and not self.is_armed():
                return True
            time.sleep(0.2)
        return False


class OffboardWorker:
    """Worker for OFFBOARD mode collection"""

    def __init__(self, uav_id: int, commander: OffboardCommander,
                 task_queue: "queue.Queue[Path]", out_dir: Path,
                 scale: float, z_down: bool, max_points: int,
                 waypoint_timeout: float, skip_existing: bool,
                 print_lock: threading.Lock, status_log_path: Path):
        self.uav_id = uav_id
        self.commander = commander
        self.task_queue = task_queue
        self.out_dir = out_dir
        self.scale = scale
        self.z_down = z_down
        self.max_points = max_points
        self.waypoint_timeout = waypoint_timeout
        self.skip_existing = skip_existing
        self.print_lock = print_lock
        self.status_log_path = status_log_path
        self._origin_offset = None

    def _log(self, msg: str, level: str = "INFO"):
        with self.print_lock:
            ts_log(f"[Worker UAV{self.uav_id}]", msg, level)

    def run(self):
        while True:
            try:
                json_path = self.task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._process_one(json_path)
            except Exception as e:
                self._log(f"failed: {e}", "ERROR")
                traceback.print_exc()
            finally:
                self.task_queue.task_done()

    def _process_one(self, json_path: Path):
        traj_name = _safe_name(json_path)
        traj_dir = self.out_dir / traj_name / f"uav{self.uav_id}"
        csv_path = traj_dir / "dynamics_data.csv"

        if self.skip_existing and csv_path.exists():
            self._log(f"skip existing: {traj_name}")
            return

        self._log(f"starting: {traj_name}")

        # Load trajectory
        raw_pts = _load_preprocessed_xyz(json_path)
        init_pos = _load_init_point_xyz(json_path)
        pts = _transform_points(raw_pts, self.scale, init_pos.x, init_pos.y, init_pos.z, self.z_down)
        pts = _filter_close_points(pts)

        if self.max_points > 0:
            raw_pts = raw_pts[:self.max_points]
            pts = pts[:self.max_points]

        if not pts:
            raise ValueError("empty trajectory")

        # Wait for connection
        if not self.commander.wait_connected(60.0):
            raise TimeoutError("connection timeout")

        traj_start_ts = time.time()
        self._origin_offset = None
        rows = []

        # Takeoff
        self._log("taking off...")
        if not self.commander.takeoff(altitude=abs(pts[0].z)):
            raise RuntimeError("takeoff failed")

        self._log(f"navigating {len(pts)} waypoints...")

        # Navigate waypoints
        for i, (p_in, p_cmd) in enumerate(zip(raw_pts, pts)):
            self._log(f"waypoint {i+1}/{len(pts)}: ({p_cmd.x:.2f}, {p_cmd.y:.2f}, {p_cmd.z:.2f})")

            reached = self.commander.wait_position_reached(
                p_cmd.x, p_cmd.y, p_cmd.z,
                tolerance=POSITION_TOLERANCE,
                timeout=self.waypoint_timeout
            )

            if not reached:
                self._log(f"waypoint {i+1} timeout", "WARN")

            # Collect dynamics
            dynamics = self.commander.get_dynamics()
            obs_pos = (dynamics.pos_x, dynamics.pos_y, dynamics.pos_z)
            cmd_pos = (p_cmd.x, p_cmd.y, p_cmd.z)

            if i == 0 and self._origin_offset is None:
                self._origin_offset = tuple(o - c for o, c in zip(obs_pos, cmd_pos))
                self._log(f"origin offset: {self._origin_offset}")

            aligned = tuple(o - off for o, off in zip(obs_pos, self._origin_offset)) if self._origin_offset else obs_pos

            rows.append({
                "traj_json": str(json_path.resolve()),
                "traj_name": traj_name,
                "uav_id": self.uav_id,
                "step_idx": i,
                "timestamp": dynamics.timestamp,
                "cmd_in_x": p_in.x, "cmd_in_y": p_in.y, "cmd_in_z": p_in.z,
                "cmd_in_roll_deg": p_in.roll_deg, "cmd_in_yaw_deg": p_in.yaw_deg, "cmd_in_pitch_deg": p_in.pitch_deg,
                "cmd_x": p_cmd.x, "cmd_y": p_cmd.y, "cmd_z": p_cmd.z,
                "obs_pos_x": dynamics.pos_x, "obs_pos_y": dynamics.pos_y, "obs_pos_z": dynamics.pos_z,
                "obs_aligned_x": aligned[0], "obs_aligned_y": aligned[1], "obs_aligned_z": aligned[2],
                "origin_offset_x": self._origin_offset[0] if self._origin_offset else 0,
                "origin_offset_y": self._origin_offset[1] if self._origin_offset else 0,
                "origin_offset_z": self._origin_offset[2] if self._origin_offset else 0,
                "obs_att_w": dynamics.att_w, "obs_att_x": dynamics.att_x,
                "obs_att_y": dynamics.att_y, "obs_att_z": dynamics.att_z,
                "obs_linvel_x": dynamics.linvel_x, "obs_linvel_y": dynamics.linvel_y, "obs_linvel_z": dynamics.linvel_z,
                "obs_angvel_x": dynamics.angvel_x, "obs_angvel_y": dynamics.angvel_y, "obs_angvel_z": dynamics.angvel_z,
                "ulg_path": "",
            })

        # Land
        self._log("landing...")
        self.commander.land()

        # Save data
        _write_csv(rows, csv_path)
        self._log(f"saved: {csv_path}")


class CollectorNode(Node):
    def __init__(self):
        super().__init__("offboard_dynamics_collector")


def main():
    parser = argparse.ArgumentParser(description="OFFBOARD Mode Dynamics Collector")
    parser.add_argument("--input-dir", type=str, required=True)
    parser.add_argument("--out-dir", type=str, default="./offboard_recordings")
    parser.add_argument("--pattern", type=str, default="*.json")
    parser.add_argument("--uav-ids", type=str, default="0")
    parser.add_argument("--namespace-prefix", type=str, default="/uav")
    parser.add_argument("--scale", type=float, default=DEFAULT_SCALE)
    parser.add_argument("--max-points", type=int, default=0)
    parser.add_argument("--z-down", action="store_true", default=True)
    parser.add_argument("--waypoint-timeout", type=float, default=60.0)
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if not HAS_ROS2:
        raise SystemExit("ROS2 not available")

    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = _iter_json_files(input_dir, args.pattern)
    if not json_files:
        raise SystemExit(f"No JSON files in {input_dir}")

    ts_log("[Main]", f"Found {len(json_files)} files")

    if args.dry_run:
        for f in json_files:
            try:
                pts = _load_preprocessed_xyz(f)
                ts_log("[DryRun]", f"{f.name}: {len(pts)} points")
            except Exception as e:
                ts_log("[DryRun]", f"{f.name}: {e}", "ERROR")
        return

    uav_ids = sorted(set(int(x) for x in args.uav_ids.split(",") if x.strip()))

    rclpy.init()
    node = CollectorNode()
    spin_thread = threading.Thread(target=lambda: rclpy.spin(node), daemon=True)
    spin_thread.start()

    task_queue = queue.Queue()
    for f in json_files:
        task_queue.put(f)

    print_lock = threading.Lock()
    status_log_path = out_dir / "collection_status.csv"
    workers = []

    for vid in uav_ids:
        commander = OffboardCommander(node, vid, args.namespace_prefix)
        worker = OffboardWorker(
            uav_id=vid, commander=commander, task_queue=task_queue,
            out_dir=out_dir, scale=args.scale, z_down=args.z_down,
            max_points=args.max_points, waypoint_timeout=args.waypoint_timeout,
            skip_existing=args.skip_existing, print_lock=print_lock,
            status_log_path=status_log_path
        )
        t = threading.Thread(target=worker.run, daemon=True)
        workers.append(t)
        t.start()
        ts_log("[Main]", f"Started worker for UAV{vid}")

    for t in workers:
        t.join()

    ts_log("[Main]", "All workers completed")
    rclpy.shutdown()


if __name__ == "__main__":
    main()
