#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# -*- coding: utf-8 -*-
"""
Gazebo PX4 Multi-UAV Dynamics Data Collector
=============================================

A lightweight dynamics data collector for PX4 SITL + Gazebo environment.
Designed for parallel multi-UAV data collection without Isaac Sim dependency.

Features:
- Pure ROS2 + MAVROS based (no HTTP dependencies)
- Multi-UAV parallel collection
- Dynamics-only data (no image capture)
- Compatible with standard PX4 SITL + Gazebo setup

Input JSON Format:
{
    "raw_logs": [[x, y, z, roll, yaw, pitch]],  // Initial position
    "preprocessed_logs": [
        [x, y, z, roll, yaw, pitch],  // Waypoints in ENU coordinates
        ...
    ]
}

Output CSV Format (dynamics_data.csv):
- step_idx, timestamp
- cmd_x, cmd_y, cmd_z (command position)
- obs_pos_x, obs_pos_y, obs_pos_z (observed position)
- obs_att_w, obs_att_x, obs_att_y, obs_att_z (quaternion attitude)
- obs_linvel_x, obs_linvel_y, obs_linvel_z (linear velocity)
- obs_angvel_x, obs_angvel_y, obs_angvel_z (angular velocity)

Usage:
    python3 gazebo_dynamics_collector.py \
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
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ROS2 imports
try:
    import rclpy
    from rclpy.node import Node
    from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
    from geometry_msgs.msg import PoseStamped, TwistStamped
    from mavros_msgs.msg import State, ExtendedState, Waypoint, WaypointReached
    from mavros_msgs.srv import CommandBool, SetMode, CommandLong, WaypointPush, WaypointClear, CommandTOL
    HAS_ROS2 = True
except ImportError:
    HAS_ROS2 = False
    print("[WARNING] ROS2 not available. Install ros2 and mavros packages.")


# ============================================================================
# Constants
# ============================================================================

# Coordinate transformation
DEFAULT_SCALE = 0.01  # Scale factor for input coordinates
MIN_WAYPOINT_DIST = 0.35  # Minimum distance between waypoints (meters)

# Timeouts
DEFAULT_CONNECT_TIMEOUT = 60.0
DEFAULT_ARM_TIMEOUT = 30.0
DEFAULT_TAKEOFF_TIMEOUT = 60.0
DEFAULT_WAYPOINT_TIMEOUT = 120.0
DEFAULT_LAND_TIMEOUT = 120.0

# NAV_CMD constants for PX4
NAV_CMD_WAYPOINT = 16
NAV_CMD_TAKEOFF = 22
NAV_CMD_LAND = 21
NAV_CMD_RETURN_TO_LAUNCH = 20


# ============================================================================
# Logging utilities
# ============================================================================

def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """Generate timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg, flush=True)
    return log_msg


def _log_exc(context: str, e: BaseException) -> None:
    """Log exception with traceback"""
    ts_log("[Exception]", f"{context} err={e}", "WARN")
    ts_log("[Exception]", traceback.format_exc(), "WARN")


# ============================================================================
# Data structures
# ============================================================================

@dataclass(frozen=True)
class TrajPoint:
    """Trajectory point with position and orientation"""
    x: float
    y: float
    z: float
    roll_deg: float
    yaw_deg: float
    pitch_deg: float


@dataclass
class DynamicsData:
    """Dynamics observation data"""
    timestamp: float = 0.0
    # Position (NED in PX4, but we convert to ENU for output)
    pos_x: float = 0.0
    pos_y: float = 0.0
    pos_z: float = 0.0
    # Attitude quaternion
    att_w: float = 1.0
    att_x: float = 0.0
    att_y: float = 0.0
    att_z: float = 0.0
    # Linear velocity
    linvel_x: float = 0.0
    linvel_y: float = 0.0
    linvel_z: float = 0.0
    # Angular velocity
    angvel_x: float = 0.0
    angvel_y: float = 0.0
    angvel_z: float = 0.0


# ============================================================================
# Trajectory loading utilities
# ============================================================================

def _iter_json_files(input_dir: Path, pattern: str) -> List[Path]:
    """Find all JSON files matching pattern in input directory"""
    files = sorted([Path(p) for p in glob.glob(str(input_dir / pattern))])
    return [p for p in files if p.is_file() and p.suffix.lower() == ".json"]


def _load_preprocessed_xyz(json_path: Path) -> List[TrajPoint]:
    """Load preprocessed trajectory points from JSON"""
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs")
    if not isinstance(logs, list):
        raise ValueError("missing preprocessed_logs")

    # Sample every 2 points (0.2s/pt -> 0.4s/pt)
    logs = logs[::2]

    pts: List[TrajPoint] = []
    for row_idx, row in enumerate(logs):
        if not isinstance(row, (list, tuple)):
            continue
        if len(row) < 6:
            raise ValueError(f"preprocessed_logs[{row_idx}] expects [x,y,z,roll,yaw,pitch], got len={len(row)}")
        x = float(row[0])
        y = float(row[1])
        z = float(row[2])
        roll = float(row[3])
        yaw = float(row[4])
        pitch = float(row[5])
        # ENU to NEU conversion (swap x and y)
        pts.append(TrajPoint(y, x, z, roll, yaw, pitch))

    if not pts:
        raise ValueError("preprocessed_logs has no valid xyz rows")
    return pts


def _load_init_point_xyz(json_path: Path) -> TrajPoint:
    """Load initial position from JSON"""
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    raw_logs = obj.get("raw_logs", [])
    if not isinstance(raw_logs, list) or not raw_logs:
        raise ValueError("missing init point")
    init = raw_logs[0]
    if not isinstance(init, (list, tuple)) or len(init) < 6:
        raise ValueError("missing init point")

    x = float(init[0])
    y = float(init[1])
    z = float(init[2])
    roll = float(init[3])
    yaw = float(init[4])
    pitch = float(init[5])
    # ENU to NEU conversion
    return TrajPoint(y, x, z, roll, yaw, pitch)


def _transform_points(
    pts: List[TrajPoint],
    scale: float,
    base_x: float,
    base_y: float,
    base_z: float,
    z_down: bool,
) -> List[TrajPoint]:
    """Apply scale and coordinate transformation to trajectory points"""
    out: List[TrajPoint] = []
    for p in pts:
        x = base_x * scale + p.x * scale
        y = base_y * scale + p.y * scale
        if z_down:
            z = base_z * scale - p.z * scale
        else:
            z = base_z * scale + p.z * scale
        out.append(TrajPoint(x, y, z, p.roll_deg, p.yaw_deg, p.pitch_deg))
    return out


def _filter_close_points(pts: List[TrajPoint], min_dist: float = MIN_WAYPOINT_DIST) -> List[TrajPoint]:
    """Filter out waypoints that are too close together"""
    if len(pts) <= 2:
        return pts

    filtered: List[TrajPoint] = [pts[0]]

    for i in range(1, len(pts) - 1):
        last = filtered[-1]
        curr = pts[i]
        dx = curr.x - last.x
        dy = curr.y - last.y
        dz = curr.z - last.z
        dist = math.sqrt(dx * dx + dy * dy + dz * dz)
        if dist >= min_dist:
            filtered.append(curr)

    # Always keep the last point
    if len(pts) > 1:
        filtered.append(pts[-1])

    return filtered


def _safe_name(path: Path) -> str:
    """Convert path to safe filename"""
    s = path.stem
    s2 = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", "."):
            s2.append(ch)
        else:
            s2.append("_")
    return "".join(s2) or "traj"


# ============================================================================
# CSV writing utilities
# ============================================================================

def _write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    """Write list of dicts to CSV file"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("", encoding="utf-8")
        return

    keys = list(rows[0].keys())
    for r in rows[1:]:
        for k in r.keys():
            if k not in keys:
                keys.append(k)

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _norm_abs_path(p: Path) -> str:
    """Get normalized absolute path string"""
    try:
        return str(p.resolve())
    except Exception:
        return str(p.absolute())


# ============================================================================
# ULG file utilities
# ============================================================================

def _is_good_ulg(path: Path) -> bool:
    """Check if ULG file is valid"""
    try:
        if path is None or not path.exists():
            return False
        st = path.stat()
        if st.st_size < 1024:
            return False

        exe = shutil.which("ulog_info")
        if not exe:
            return True

        try:
            proc = subprocess.run(
                [exe, str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5.0,
                check=False,
            )
        except Exception:
            return True

        out = proc.stdout or ""
        for line in out.splitlines():
            t = line.strip()
            if t.startswith("duration:"):
                parts = t.split("duration:", 1)[1].strip().split()
                if parts and parts[0] == "0:00:00":
                    return False
                return True
        return True
    except Exception:
        return True


def _find_latest_ulg(vehicle_id: int, since_ts: float) -> Optional[Path]:
    """Find the latest ULG file for a vehicle"""
    candidates: List[Tuple[float, Path]] = []

    # Search in PX4 temp directories
    for pattern in [f"/tmp/px4_{vehicle_id}_*", f"/tmp/px4_*"]:
        for p in glob.glob(pattern):
            d = Path(p)
            if not d.is_dir():
                continue
            try:
                for fp in d.rglob("*.ulg"):
                    try:
                        st = fp.stat()
                        if st.st_mtime >= since_ts:
                            candidates.append((st.st_mtime, fp))
                    except Exception:
                        continue
            except Exception:
                continue

    # Search in common log directories
    search_roots = [
        Path.home() / ".ros" / "log",
        Path.home() / "PX4-Autopilot" / "build" / "px4_sitl_default" / "rootfs" / "log",
        Path("/tmp"),
    ]

    for base in search_roots:
        if not base.is_dir():
            continue
        try:
            for fp in base.rglob("*.ulg"):
                try:
                    st = fp.stat()
                    if st.st_mtime >= since_ts:
                        candidates.append((st.st_mtime, fp))
                except Exception:
                    continue
        except Exception:
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    for _, fp in candidates:
        if _is_good_ulg(fp):
            return fp

    return candidates[0][1] if candidates else None


# ============================================================================
# MAVROS Commander (ROS2 interface)
# ============================================================================

class MavrosCommander:
    """MAVROS service client wrapper for controlling UAV"""

    def __init__(self, node: Node, uav_id: int, namespace_prefix: str = "/uav"):
        self.node = node
        self.uav_id = int(uav_id)
        self.prefix = f"{namespace_prefix}{self.uav_id}"

        # State variables
        self.state: Optional[State] = None
        self.ext_state: Optional[ExtendedState] = None
        self.reached_seq: int = -1
        self.latest_pose: Optional[PoseStamped] = None
        self.latest_velocity: Optional[TwistStamped] = None

        # QoS profile for best-effort communication
        qos_profile = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
            durability=DurabilityPolicy.VOLATILE,
        )

        # Subscriptions
        self.node.create_subscription(
            State, f"{self.prefix}/mavros/state",
            self._state_cb, qos_profile
        )
        self.node.create_subscription(
            ExtendedState, f"{self.prefix}/mavros/extended_state",
            self._ext_state_cb, qos_profile
        )
        self.node.create_subscription(
            WaypointReached, f"{self.prefix}/mavros/mission/reached",
            self._reached_cb, qos_profile
        )
        self.node.create_subscription(
            PoseStamped, f"{self.prefix}/mavros/local_position/pose",
            self._pose_cb, qos_profile
        )
        self.node.create_subscription(
            TwistStamped, f"{self.prefix}/mavros/local_position/velocity_body",
            self._velocity_cb, qos_profile
        )

        # Service clients
        self.arming_client = self.node.create_client(
            CommandBool, f"{self.prefix}/mavros/cmd/arming"
        )
        self.set_mode_client = self.node.create_client(
            SetMode, f"{self.prefix}/mavros/set_mode"
        )
        self.cmdlong_client = self.node.create_client(
            CommandLong, f"{self.prefix}/mavros/cmd/command"
        )
        self.mission_push_client = self.node.create_client(
            WaypointPush, f"{self.prefix}/mavros/mission/push"
        )
        self.mission_clear_client = self.node.create_client(
            WaypointClear, f"{self.prefix}/mavros/mission/clear"
        )
        self.takeoff_client = self.node.create_client(
            CommandTOL, f"{self.prefix}/mavros/cmd/takeoff"
        )
        self.land_client = self.node.create_client(
            CommandTOL, f"{self.prefix}/mavros/cmd/land"
        )

    def _state_cb(self, msg: State):
        self.state = msg

    def _ext_state_cb(self, msg: ExtendedState):
        self.ext_state = msg

    def _reached_cb(self, msg: WaypointReached):
        self.reached_seq = int(getattr(msg, "wp_seq", -1))

    def _pose_cb(self, msg: PoseStamped):
        self.latest_pose = msg

    def _velocity_cb(self, msg: TwistStamped):
        self.latest_velocity = msg

    def get_dynamics(self) -> DynamicsData:
        """Get current dynamics data"""
        data = DynamicsData()
        data.timestamp = time.time()

        if self.latest_pose is not None:
            pos = self.latest_pose.pose.position
            att = self.latest_pose.pose.orientation
            data.pos_x = pos.x
            data.pos_y = pos.y
            data.pos_z = pos.z
            data.att_w = att.w
            data.att_x = att.x
            data.att_y = att.y
            data.att_z = att.z

        if self.latest_velocity is not None:
            lin = self.latest_velocity.twist.linear
            ang = self.latest_velocity.twist.angular
            data.linvel_x = lin.x
            data.linvel_y = lin.y
            data.linvel_z = lin.z
            data.angvel_x = ang.x
            data.angvel_y = ang.y
            data.angvel_z = ang.z

        return data

    def wait_connected(self, timeout_s: float = 60.0) -> bool:
        """Wait for MAVROS to connect to PX4"""
        deadline = time.time() + float(timeout_s)
        while time.time() < deadline:
            if self.state is not None and bool(getattr(self.state, "connected", False)):
                return True
            time.sleep(0.1)
        return False

    def is_armed(self) -> bool:
        """Check if vehicle is armed"""
        if self.state is None:
            return False
        return bool(getattr(self.state, "armed", False))

    def is_landed(self) -> bool:
        """Check if vehicle has landed"""
        if self.ext_state is None:
            return False
        # LANDED_STATE_ON_GROUND = 1
        return int(getattr(self.ext_state, "landed_state", 0)) == 1

    def get_mode(self) -> str:
        """Get current flight mode"""
        if self.state is None:
            return ""
        return str(getattr(self.state, "mode", ""))

    def _call_service(self, client, req, timeout_s: float, max_retries: int = 5):
        """Call ROS2 service with retry logic"""
        last_err = None
        for attempt in range(max(1, max_retries)):
            if not client.wait_for_service(timeout_sec=min(10.0, timeout_s)):
                last_err = TimeoutError(f"service not available (attempt {attempt + 1}/{max_retries})")
                time.sleep(1.0)
                continue

            try:
                fut = client.call_async(req)
                deadline = time.time() + timeout_s
                while time.time() < deadline:
                    if fut.done():
                        return fut.result()
                    time.sleep(0.05)
                last_err = TimeoutError(f"service call timeout (attempt {attempt + 1}/{max_retries})")
            except Exception as e:
                last_err = e

            time.sleep(1.0)

        raise last_err if last_err else TimeoutError("service not available")

    def set_mode(self, mode: str, timeout_s: float = 10.0) -> bool:
        """Set flight mode"""
        req = SetMode.Request()
        req.custom_mode = str(mode)
        try:
            self._call_service(self.set_mode_client, req, timeout_s=timeout_s)
            return True
        except Exception as e:
            ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"set_mode({mode}) failed: {e}", "WARN")
            return False

    def arm(self, arm: bool = True, timeout_s: float = 10.0) -> bool:
        """Arm or disarm the vehicle"""
        req = CommandBool.Request()
        req.value = bool(arm)
        try:
            resp = self._call_service(self.arming_client, req, timeout_s=timeout_s)
            return bool(getattr(resp, "success", False))
        except Exception as e:
            ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"arm({arm}) failed: {e}", "WARN")
            return False

    def clear_mission(self, timeout_s: float = 10.0) -> bool:
        """Clear current mission"""
        req = WaypointClear.Request()
        try:
            resp = self._call_service(self.mission_clear_client, req, timeout_s=timeout_s)
            return bool(getattr(resp, "success", False))
        except Exception as e:
            ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"clear_mission failed: {e}", "WARN")
            return False

    def push_mission(self, waypoints: List[Waypoint], timeout_s: float = 30.0) -> bool:
        """Upload mission waypoints"""
        req = WaypointPush.Request()
        req.start_index = 0
        req.waypoints = list(waypoints)
        try:
            resp = self._call_service(self.mission_push_client, req, timeout_s=timeout_s)
            return bool(getattr(resp, "success", False))
        except Exception as e:
            ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"push_mission failed: {e}", "WARN")
            return False

    def wait_reached(self, seq: int, timeout_s: float = 120.0) -> bool:
        """Wait for reaching a waypoint"""
        deadline = time.time() + timeout_s
        target = int(seq)
        while time.time() < deadline:
            if int(self.reached_seq) >= target:
                return True
            time.sleep(0.1)
        return False

    def wait_armed(self, timeout_s: float = 30.0) -> bool:
        """Wait for vehicle to be armed"""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.is_armed():
                return True
            time.sleep(0.1)
        return False

    def wait_disarmed(self, timeout_s: float = 60.0) -> bool:
        """Wait for vehicle to be disarmed"""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if not self.is_armed():
                return True
            time.sleep(0.1)
        return False

    def takeoff(self, altitude: float = 2.5, timeout_s: float = 60.0) -> bool:
        """Command takeoff to specified altitude"""
        try:
            req = CommandTOL.Request()
            req.altitude = float(altitude)
            req.latitude = float('nan')
            req.longitude = float('nan')
            req.min_pitch = 0.0
            req.yaw = float('nan')
            resp = self._call_service(self.takeoff_client, req, timeout_s=10.0)
            if not bool(getattr(resp, "success", False)):
                return False

            # Wait for takeoff
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                if self.latest_pose is not None:
                    current_alt = self.latest_pose.pose.position.z
                    if current_alt >= altitude * 0.8:
                        return True
                time.sleep(0.2)
            return False
        except Exception as e:
            ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"takeoff failed: {e}", "WARN")
            return False

    def land(self, timeout_s: float = 120.0) -> bool:
        """Command landing"""
        try:
            # Set LAND mode
            self.set_mode("AUTO.LAND", timeout_s=10.0)

            # Wait for landing
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                if self.is_landed() and not self.is_armed():
                    return True
                time.sleep(0.2)
            return False
        except Exception as e:
            ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"land failed: {e}", "WARN")
            return False


# ============================================================================
# Worker thread for trajectory collection
# ============================================================================

class CollectionWorker:
    """Worker thread for collecting trajectory data"""

    def __init__(
        self,
        uav_id: int,
        commander: MavrosCommander,
        task_queue: "queue.Queue[Path]",
        out_dir: Path,
        scale: float,
        z_down: bool,
        max_points: int,
        waypoint_timeout: float,
        skip_existing: bool,
        print_lock: threading.Lock,
        status_log_path: Path,
    ):
        self.uav_id = int(uav_id)
        self.commander = commander
        self.task_queue = task_queue
        self.out_dir = out_dir
        self.scale = float(scale)
        self.z_down = bool(z_down)
        self.max_points = int(max_points)
        self.waypoint_timeout = float(waypoint_timeout)
        self.skip_existing = bool(skip_existing)
        self.print_lock = print_lock
        self.status_log_path = status_log_path

        # Origin offset for coordinate alignment
        self._origin_offset: Optional[Tuple[float, float, float]] = None

    def _log(self, msg: str, level: str = "INFO") -> None:
        with self.print_lock:
            ts_log(f"[Worker UAV{self.uav_id}]", msg, level)

    def run(self) -> None:
        """Main worker loop"""
        while True:
            try:
                json_path = self.task_queue.get_nowait()
            except queue.Empty:
                return

            try:
                self._process_one(json_path)
            except Exception as e:
                self._log(f"trajectory failed json={json_path} err={e}", "ERROR")
                _log_exc(f"Worker UAV{self.uav_id}", e)
            finally:
                self.task_queue.task_done()

    def _calculate_origin_offset(
        self,
        cmd_pos: Tuple[float, float, float],
        obs_pos: Tuple[float, float, float]
    ) -> Tuple[float, float, float]:
        """Calculate coordinate origin offset"""
        return (
            obs_pos[0] - cmd_pos[0],
            obs_pos[1] - cmd_pos[1],
            obs_pos[2] - cmd_pos[2],
        )

    def _apply_alignment(self, obs_pos: Tuple[float, float, float]) -> Tuple[float, float, float]:
        """Apply coordinate alignment"""
        if self._origin_offset is None:
            return obs_pos
        return (
            obs_pos[0] - self._origin_offset[0],
            obs_pos[1] - self._origin_offset[1],
            obs_pos[2] - self._origin_offset[2],
        )

    def _build_mission_waypoints(self, pts: List[TrajPoint], takeoff_alt: float) -> List[Waypoint]:
        """Build mission waypoints for PX4"""
        waypoints: List[Waypoint] = []

        # First waypoint: Takeoff
        wp_takeoff = Waypoint()
        wp_takeoff.frame = 3  # MAV_FRAME_GLOBAL_RELATIVE_ALT
        wp_takeoff.command = NAV_CMD_TAKEOFF
        wp_takeoff.is_current = True
        wp_takeoff.autocontinue = True
        wp_takeoff.param1 = 0.0  # pitch
        wp_takeoff.param2 = 0.0
        wp_takeoff.param3 = 0.0
        wp_takeoff.param4 = float('nan')  # yaw
        wp_takeoff.x_lat = 0.0  # Will use current position
        wp_takeoff.y_long = 0.0
        wp_takeoff.z_alt = takeoff_alt
        waypoints.append(wp_takeoff)

        # Navigation waypoints
        for i, p in enumerate(pts):
            wp = Waypoint()
            wp.frame = 1  # MAV_FRAME_LOCAL_NED (use local coordinates)
            wp.command = NAV_CMD_WAYPOINT
            wp.is_current = False
            wp.autocontinue = True
            wp.param1 = 0.0  # hold time
            wp.param2 = 0.5  # acceptance radius
            wp.param3 = 0.0  # pass through
            wp.param4 = float('nan')  # yaw
            # Local NED coordinates
            wp.x_lat = p.x
            wp.y_long = p.y
            wp.z_alt = -p.z  # NED: z is negative for altitude
            waypoints.append(wp)

        # Last waypoint: Land
        wp_land = Waypoint()
        wp_land.frame = 3
        wp_land.command = NAV_CMD_LAND
        wp_land.is_current = False
        wp_land.autocontinue = True
        wp_land.param1 = 0.0
        wp_land.param2 = 0.0
        wp_land.param3 = 0.0
        wp_land.param4 = float('nan')
        wp_land.x_lat = pts[-1].x if pts else 0.0
        wp_land.y_long = pts[-1].y if pts else 0.0
        wp_land.z_alt = 0.0
        waypoints.append(wp_land)

        return waypoints

    def _process_one(self, json_path: Path) -> None:
        """Process one trajectory file"""
        traj_name = _safe_name(json_path)
        traj_dir = self.out_dir / traj_name / f"uav{self.uav_id}"
        csv_path = traj_dir / "dynamics_data.csv"

        # Skip if already processed
        if self.skip_existing and csv_path.exists():
            self._log(f"skip existing traj={traj_name}")
            return

        self._log(f"starting trajectory: {traj_name}")

        # Load trajectory
        raw_pts = _load_preprocessed_xyz(json_path)
        init_pos = _load_init_point_xyz(json_path)

        # Transform points
        pts = _transform_points(
            raw_pts, self.scale,
            init_pos.x, init_pos.y, init_pos.z,
            self.z_down
        )

        # Filter close waypoints
        pts_before = len(pts)
        pts = _filter_close_points(pts, MIN_WAYPOINT_DIST)
        if len(pts) < pts_before:
            self._log(f"filtered {pts_before - len(pts)} close waypoints, {len(pts)} remaining")

        # Apply max_points limit
        if self.max_points > 0:
            raw_pts = raw_pts[:self.max_points]
            pts = pts[:self.max_points]

        if not pts:
            raise ValueError("empty trajectory after filtering")

        # Wait for MAVROS connection
        self._log("waiting for MAVROS connection...")
        if not self.commander.wait_connected(timeout_s=DEFAULT_CONNECT_TIMEOUT):
            raise TimeoutError("MAVROS connection timeout")
        self._log("MAVROS connected")

        traj_start_ts = time.time()

        # Reset origin offset
        self._origin_offset = None

        # Collect data
        rows: List[Dict[str, Any]] = []

        # Build and upload mission
        takeoff_alt = abs(pts[0].z) if pts else 2.5
        waypoints = self._build_mission_waypoints(pts, takeoff_alt)

        self._log(f"clearing old mission...")
        self.commander.clear_mission()
        time.sleep(0.5)

        self._log(f"uploading {len(waypoints)} waypoints...")
        if not self.commander.push_mission(waypoints):
            raise RuntimeError("failed to upload mission")
        time.sleep(0.5)

        # Set AUTO.MISSION mode and arm
        self._log("setting AUTO.MISSION mode...")
        if not self.commander.set_mode("AUTO.MISSION"):
            raise RuntimeError("failed to set AUTO.MISSION mode")
        time.sleep(0.5)

        self._log("arming...")
        for attempt in range(5):
            if self.commander.arm(True):
                break
            time.sleep(1.0)

        if not self.commander.wait_armed(timeout_s=DEFAULT_ARM_TIMEOUT):
            raise RuntimeError("arming timeout")
        self._log("armed, starting mission...")

        # Wait for each waypoint and collect data
        # Skip waypoint 0 (takeoff), collect data at waypoints 1 to n-1 (navigation),
        # skip last waypoint (landing)
        for i, (p_in, p_cmd) in enumerate(zip(raw_pts, pts)):
            wp_idx = i + 1  # +1 because first waypoint is takeoff

            self._log(f"waiting for waypoint {i+1}/{len(pts)}: ({p_cmd.x:.2f}, {p_cmd.y:.2f}, {p_cmd.z:.2f})")

            reached = self.commander.wait_reached(wp_idx, timeout_s=self.waypoint_timeout)
            if not reached:
                self._log(f"waypoint {i+1} timeout, continuing...", "WARN")

            # Collect dynamics data
            dynamics = self.commander.get_dynamics()

            # Calculate origin offset at first waypoint
            obs_pos = (dynamics.pos_x, dynamics.pos_y, dynamics.pos_z)
            cmd_pos = (p_cmd.x, p_cmd.y, p_cmd.z)

            if i == 0 and self._origin_offset is None:
                self._origin_offset = self._calculate_origin_offset(cmd_pos, obs_pos)
                self._log(
                    f"origin offset: ({self._origin_offset[0]:.4f}, "
                    f"{self._origin_offset[1]:.4f}, {self._origin_offset[2]:.4f})m"
                )

            # Apply alignment
            aligned_obs = self._apply_alignment(obs_pos)

            # Build data row
            row = {
                "traj_json": _norm_abs_path(json_path),
                "traj_name": traj_name,
                "uav_id": self.uav_id,
                "step_idx": i,
                "timestamp": dynamics.timestamp,
                # Input coordinates
                "cmd_in_x": p_in.x,
                "cmd_in_y": p_in.y,
                "cmd_in_z": p_in.z,
                "cmd_in_roll_deg": p_in.roll_deg,
                "cmd_in_yaw_deg": p_in.yaw_deg,
                "cmd_in_pitch_deg": p_in.pitch_deg,
                # Transformed command
                "cmd_x": p_cmd.x,
                "cmd_y": p_cmd.y,
                "cmd_z": p_cmd.z,
                # Observed position (raw)
                "obs_pos_x": dynamics.pos_x,
                "obs_pos_y": dynamics.pos_y,
                "obs_pos_z": dynamics.pos_z,
                # Observed position (aligned)
                "obs_aligned_x": aligned_obs[0],
                "obs_aligned_y": aligned_obs[1],
                "obs_aligned_z": aligned_obs[2],
                # Origin offset
                "origin_offset_x": self._origin_offset[0] if self._origin_offset else 0.0,
                "origin_offset_y": self._origin_offset[1] if self._origin_offset else 0.0,
                "origin_offset_z": self._origin_offset[2] if self._origin_offset else 0.0,
                # Attitude
                "obs_att_w": dynamics.att_w,
                "obs_att_x": dynamics.att_x,
                "obs_att_y": dynamics.att_y,
                "obs_att_z": dynamics.att_z,
                # Linear velocity
                "obs_linvel_x": dynamics.linvel_x,
                "obs_linvel_y": dynamics.linvel_y,
                "obs_linvel_z": dynamics.linvel_z,
                # Angular velocity
                "obs_angvel_x": dynamics.angvel_x,
                "obs_angvel_y": dynamics.angvel_y,
                "obs_angvel_z": dynamics.angvel_z,
                # ULG path (filled later)
                "ulg_path": "",
            }
            rows.append(row)

        # Wait for landing
        self._log("waiting for mission completion and landing...")
        self.commander.wait_disarmed(timeout_s=DEFAULT_LAND_TIMEOUT)
        self._log("mission completed")

        # Find and copy ULG file
        ulg_path_str = ""
        ulg_src = _find_latest_ulg(self.uav_id, since_ts=traj_start_ts - 5.0)
        if ulg_src is not None and ulg_src.exists():
            ulg_filename = f"px4_uav{self.uav_id}_{int(time.time())}.ulg"
            ulg_dst = traj_dir / ulg_filename
            try:
                ulg_dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(ulg_src), str(ulg_dst))
                ulg_path_str = _norm_abs_path(ulg_dst)
                self._log(f"copied ULG: {ulg_path_str}")
            except Exception as e:
                self._log(f"failed to copy ULG: {e}", "WARN")

        # Update ULG path in all rows
        for r in rows:
            r["ulg_path"] = ulg_path_str

        # Write CSV
        _write_csv(rows, csv_path)
        self._log(f"saved dynamics data: {csv_path}")

        # Append to status log
        self._append_status_log(traj_name, ulg_path_str)

    def _append_status_log(self, traj_name: str, ulg_path: str) -> None:
        """Append to global status log"""
        try:
            row = {
                "traj_name": traj_name,
                "uav_id": self.uav_id,
                "timestamp": time.time(),
                "ulg_path": ulg_path,
                "status": "success"
            }
            with self.print_lock:
                exists = self.status_log_path.exists()
                with self.status_log_path.open("a", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=["traj_name", "uav_id", "timestamp", "ulg_path", "status"])
                    if not exists:
                        w.writeheader()
                    w.writerow(row)
        except Exception as e:
            self._log(f"failed to append status log: {e}", "WARN")


# ============================================================================
# Main collector node
# ============================================================================

class CollectorNode(Node):
    """ROS2 node for trajectory collection"""

    def __init__(self):
        super().__init__("gazebo_dynamics_collector")


def main() -> None:
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Gazebo PX4 Multi-UAV Dynamics Data Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Collect with single UAV
    python3 gazebo_dynamics_collector.py --input-dir ~/trajectories --uav-ids 0

    # Collect with multiple UAVs in parallel
    python3 gazebo_dynamics_collector.py --input-dir ~/trajectories --uav-ids 0,1,2,3

    # Dry run to check files
    python3 gazebo_dynamics_collector.py --input-dir ~/trajectories --dry-run
"""
    )

    # Required arguments
    parser.add_argument("--input-dir", type=str, required=True,
                        help="Directory containing trajectory JSON files")

    # Optional arguments
    parser.add_argument("--pattern", type=str, default="*.json",
                        help="Pattern for matching JSON files (default: *.json)")
    parser.add_argument("--out-dir", type=str, default="./gazebo_recordings",
                        help="Output directory for collected data")
    parser.add_argument("--uav-ids", type=str, default="0",
                        help="Comma-separated list of UAV IDs (default: 0)")
    parser.add_argument("--namespace-prefix", type=str, default="/uav",
                        help="ROS2 namespace prefix for UAVs (default: /uav)")
    parser.add_argument("--scale", type=float, default=DEFAULT_SCALE,
                        help=f"Coordinate scale factor (default: {DEFAULT_SCALE})")
    parser.add_argument("--max-points", type=int, default=0,
                        help="Maximum waypoints per trajectory (0=unlimited)")

    # Z-axis direction
    z_group = parser.add_mutually_exclusive_group()
    z_group.add_argument("--z-down", action="store_true", default=None,
                         help="Z-axis points down (default)")
    z_group.add_argument("--z-up", action="store_true", default=None,
                         help="Z-axis points up")

    # Timeouts
    parser.add_argument("--waypoint-timeout", type=float, default=DEFAULT_WAYPOINT_TIMEOUT,
                        help=f"Waypoint timeout in seconds (default: {DEFAULT_WAYPOINT_TIMEOUT})")

    # Skip existing
    skip_group = parser.add_mutually_exclusive_group()
    skip_group.add_argument("--skip-existing", action="store_true", default=None,
                            help="Skip existing trajectories (default)")
    skip_group.add_argument("--no-skip-existing", action="store_true", default=None,
                            help="Re-collect existing trajectories")

    # Dry run
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Only scan files, don't collect data")

    args = parser.parse_args()

    # Check ROS2 availability
    if not HAS_ROS2:
        raise SystemExit("ROS2 not available. Please source ROS2 and install mavros.")

    # Parse arguments
    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Find JSON files
    json_files = _iter_json_files(input_dir, args.pattern)
    if not json_files:
        raise SystemExit(f"No JSON files found in {input_dir} with pattern={args.pattern}")

    ts_log("[Main]", f"Found {len(json_files)} trajectory files")

    # Dry run mode
    if args.dry_run:
        total_pts = 0
        for fp in json_files:
            try:
                pts = _load_preprocessed_xyz(fp)
                total_pts += len(pts)
                ts_log("[DryRun]", f"{fp.name}: {len(pts)} points")
            except Exception as e:
                ts_log("[DryRun]", f"{fp.name}: error {e}", "ERROR")
        ts_log("[DryRun]", f"Total: {len(json_files)} files, {total_pts} points")
        return

    # Parse UAV IDs
    uav_ids = sorted(set(int(x.strip()) for x in args.uav_ids.split(",") if x.strip()))
    if not uav_ids:
        raise SystemExit("No valid UAV IDs specified")

    ts_log("[Main]", f"Using UAV IDs: {uav_ids}")

    # Determine flags
    z_down = True if (args.z_down is None and args.z_up is None) else bool(args.z_down)
    skip_existing = True if (args.skip_existing is None and args.no_skip_existing is None) else bool(args.skip_existing)

    # Initialize ROS2
    rclpy.init()
    node = CollectorNode()

    # Start ROS2 spin in background
    spin_thread = threading.Thread(target=lambda: rclpy.spin(node), daemon=True)
    spin_thread.start()

    # Create task queue
    task_queue: "queue.Queue[Path]" = queue.Queue()
    for f in json_files:
        task_queue.put(f)

    # Create workers
    print_lock = threading.Lock()
    status_log_path = out_dir / "collection_status.csv"

    workers: List[threading.Thread] = []
    for vid in uav_ids:
        commander = MavrosCommander(node, vid, args.namespace_prefix)

        worker = CollectionWorker(
            uav_id=vid,
            commander=commander,
            task_queue=task_queue,
            out_dir=out_dir,
            scale=args.scale,
            z_down=z_down,
            max_points=args.max_points,
            waypoint_timeout=args.waypoint_timeout,
            skip_existing=skip_existing,
            print_lock=print_lock,
            status_log_path=status_log_path,
        )

        t = threading.Thread(target=worker.run, name=f"worker_uav{vid}", daemon=True)
        workers.append(t)
        t.start()
        ts_log("[Main]", f"Started worker for UAV{vid}")

    # Wait for all workers to complete
    for t in workers:
        t.join()

    ts_log("[Main]", "All workers completed")

    # Cleanup
    try:
        rclpy.shutdown()
    except Exception:
        pass


if __name__ == "__main__":
    main()
