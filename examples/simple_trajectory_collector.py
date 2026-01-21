#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# -*- coding: utf-8 -*-
"""
简易轨迹采集器 - 完全照搬原版mavlink_trajectory_collector控制逻辑

关键点：
1. 起飞阶段使用HTTP setpoint命令
2. 轨迹跟踪阶段使用UDP直连MAVLink
3. 服务端StateRecorder记录数据
"""

import sys
import os
import time
import json
import math
import glob
import shutil
import argparse
import threading
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

try:
    import numpy as np
    from scipy.interpolate import CubicSpline
except ImportError:
    print("ERROR: numpy and scipy are required")
    sys.exit(1)

# ============================================================================
# 工具函数 (照搬原版)
# ============================================================================

def ts_log(prefix: str, msg: str, level: str = "INFO") -> None:
    now = time.time()
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
    msec = int((now - int(now)) * 1000)
    print(f"[{ts}.{msec:03d}] [{level}] {prefix} {msg}", flush=True)

def _http_json(method: str, url: str, payload: Optional[Dict] = None, timeout: float = 30.0) -> Tuple[int, Dict]:
    """HTTP JSON请求 (照搬原版)"""
    # 【修复】即使payload是空字典也要设置Content-Type，避免415错误
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
    else:
        data = None
        headers = {}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.getcode(), json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return e.code, {"error": str(e)}
    except Exception as e:
        return 500, {"error": str(e)}

def _fetch_pose_only(image_base: str, uav_id: int, timeout: float = 0.5) -> Optional[Dict]:
    """获取位姿 (照搬原版)"""
    code, data = _http_json("GET", f"{image_base}/uav/{uav_id}/pose", timeout=timeout)
    if code == 200:
        return data
    return None

def _sim_px4_recover(image_base: str, uav_id: int, timeout: float = 120.0) -> bool:
    """通过仿真端HTTP接口恢复PX4（kill并重启PX4进程）"""
    url = f"{image_base}/uav/{uav_id}/px4/recover"
    ts_log(f"[UAV{uav_id}]", f"Calling PX4 recover: {url}")
    code, obj = _http_json("POST", url, payload={}, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") == "success":
        ts_log(f"[UAV{uav_id}]", f"PX4 recover success: ready={obj.get('ready')}")
        return True
    ts_log(f"[UAV{uav_id}]", f"PX4 recover failed: code={code} resp={obj}", "WARN")
    return False


def cleanup_px4_completely(uav_id: int = None) -> int:
    """
    清理PX4锁文件（不删除临时目录，让仿真端管理）

    【关键】解决 "PX4 server already running" 问题
    只清理锁文件，不删除临时目录（仿真端需要它们）

    Args:
        uav_id: 如果指定，只清理该UAV的文件；否则清理所有

    Returns:
        清理的文件数量
    """
    return cleanup_px4_lock_files_only(uav_id)


def cleanup_px4_lock_files_only(uav_id: int = None) -> int:
    """
    只清理PX4锁文件（不删除临时目录）

    【关键】解决 "PX4 server already running" 问题

    Args:
        uav_id: 如果指定，只清理该UAV的文件；否则清理所有

    Returns:
        清理的文件数量
    """
    cleaned_count = 0
    log_prefix = f"[UAV{uav_id}]" if uav_id is not None else "[PX4Cleanup]"

    # 只清理PX4锁文件（不删除临时目录，仿真端需要它们）
    ts_log(log_prefix, "Cleaning PX4 lock files only...")
    lock_patterns = [
        "/tmp/px4_instance_*",
        "/tmp/px4-*",
        "/tmp/px4_lock-*",
    ]

    for pattern in lock_patterns:
        for lock_file in glob.glob(pattern):
            try:
                if os.path.isfile(lock_file):
                    os.remove(lock_file)
                    ts_log(log_prefix, f"Removed lock file: {lock_file}")
                    cleaned_count += 1
            except Exception as e:
                ts_log(log_prefix, f"Failed to remove {lock_file}: {e}", "WARN")

    # 清理PID文件
    pid_dir = "/tmp/pegasus_px4_pids"
    if os.path.exists(pid_dir):
        if uav_id is not None:
            pid_file = os.path.join(pid_dir, f"px4_{uav_id}.pid")
            if os.path.exists(pid_file):
                try:
                    os.remove(pid_file)
                    ts_log(log_prefix, f"Removed PID file: {pid_file}")
                    cleaned_count += 1
                except Exception as e:
                    ts_log(log_prefix, f"Failed to remove {pid_file}: {e}", "WARN")
        else:
            for pid_file in glob.glob(os.path.join(pid_dir, "px4_*.pid")):
                try:
                    os.remove(pid_file)
                    ts_log(log_prefix, f"Removed PID file: {pid_file}")
                    cleaned_count += 1
                except Exception as e:
                    ts_log(log_prefix, f"Failed to remove {pid_file}: {e}", "WARN")

    if cleaned_count > 0:
        ts_log(log_prefix, f"Cleanup complete: {cleaned_count} lock files removed")
    return cleaned_count

# ============================================================================
# 轨迹平滑器 (照搬原版TrajectorySmoother)
# ============================================================================

class TrajectorySmoother:
    def __init__(self, points: List[List[float]], dt: float = 0.2):
        self.dt = dt
        self.duration = (len(points) - 1) * dt
        self.t_orig = np.array([i * dt for i in range(len(points))])
        
        x = [p[0] for p in points]
        y = [p[1] for p in points]
        z = [p[2] for p in points]
        # raw_logs format: [x, y, z, roll, pitch, yaw] (all angles in degrees)
        roll = np.deg2rad([p[3] for p in points])
        pitch = np.deg2rad([p[4] for p in points])
        yaw = np.unwrap(np.deg2rad([p[5] for p in points]))
        
        self._spline_x = CubicSpline(self.t_orig, x, bc_type='natural')
        self._spline_y = CubicSpline(self.t_orig, y, bc_type='natural')
        self._spline_z = CubicSpline(self.t_orig, z, bc_type='natural')
        self._spline_roll = CubicSpline(self.t_orig, roll, bc_type='natural')
        self._spline_pitch = CubicSpline(self.t_orig, pitch, bc_type='natural')
        self._spline_yaw = CubicSpline(self.t_orig, yaw, bc_type='natural')

    def get_full_state(self, t: float) -> Dict[str, float]:
        t = max(0.0, min(t, self.duration))
        return {
            'x': float(self._spline_x(t)),
            'y': float(self._spline_y(t)),
            'z': float(self._spline_z(t)),
            'vx': float(self._spline_x(t, 1)),
            'vy': float(self._spline_y(t, 1)),
            'vz': float(self._spline_z(t, 1)),
            'ax': float(self._spline_x(t, 2)),
            'ay': float(self._spline_y(t, 2)),
            'az': float(self._spline_z(t, 2)),
            'roll': float(self._spline_roll(t)),
            'pitch': float(self._spline_pitch(t)),
            'yaw': float(self._spline_yaw(t)),
            'roll_rate': float(self._spline_roll(t, 1)),
            'pitch_rate': float(self._spline_pitch(t, 1)),
            'yaw_rate': float(self._spline_yaw(t, 1))
        }
    
    def get_yaw_and_rate(self, t: float) -> Tuple[float, float]:
        t = max(0.0, min(t, self.duration))
        return float(self._spline_yaw(t)), float(self._spline_yaw(t, 1))

# ============================================================================
# MAVLink UDP Setpoint发送器 (照搬原版)
# ============================================================================

class MavlinkSetpointSender:
    def __init__(self, uav_id: int):
        self.uav_id = uav_id
        self.px4_port = 14580 + uav_id
        self._conn = None
        
    def connect(self) -> bool:
        try:
            from pymavlink import mavutil
            conn_str = f"udpout:127.0.0.1:{self.px4_port}"
            self._conn = mavutil.mavlink_connection(conn_str, source_system=255)
            ts_log(f"[UAV{self.uav_id}]", f"UDP MAVLink connected to {conn_str}")
            return True
        except Exception as e:
            ts_log(f"[UAV{self.uav_id}]", f"UDP connection failed: {e}", "ERROR")
            return False

    def send_pva(self, x, y, z, vx, vy, vz, ax, ay, az, yaw_rad, yaw_rate_rad):
        """发送PVA setpoint (照搬原版坐标变换)"""
        if not self._conn:
            return
        from pymavlink import mavutil
        
        # ENU -> NED 坐标变换 (与mavlink_sim_vehicle.py一致)
        ned_x = y
        ned_y = x
        ned_z = -z
        ned_vx = vy
        ned_vy = vx
        ned_vz = -vz
        ned_ax = ay
        ned_ay = ax
        ned_az = -az
        
        # Yaw变换: ENU -> NED (照搬原版)
        ned_yaw = 0.5 * math.pi - yaw_rad + math.pi
        ned_yaw = (ned_yaw + math.pi) % (2 * math.pi) - math.pi
        ned_yaw_rate = -yaw_rate_rad
        
        self._conn.mav.set_position_target_local_ned_send(
            0, self.uav_id + 1, 1,
            mavutil.mavlink.MAV_FRAME_LOCAL_NED,
            0x0000,  # Full PVA
            ned_x, ned_y, ned_z,
            ned_vx, ned_vy, ned_vz,
            ned_ax, ned_ay, ned_az,
            ned_yaw, ned_yaw_rate
        )

# ============================================================================
# SimTimeListener - HTTP polling (reverted from UDP for reliability)
# ============================================================================

class SimTimeListener(threading.Thread):
    """Polls simulator HTTP endpoint for sim_time"""
    
    def __init__(self, url: str = "http://127.0.0.1:8081/sim_time"):
        super().__init__(daemon=True)
        self.url = url
        self._time = 0.0
        self._lock = threading.Lock()
        self._cond = threading.Condition()
        self.running = True

    def run(self):
        while self.running:
            try:
                code, data = _http_json("GET", self.url, timeout=0.5)
                if code == 200:
                    t = float(data.get("sim_time", 0))
                    with self._cond:
                        self._time = t
                        self._cond.notify_all()
            except Exception as e:
                # Silently continue polling, but log if debug needed
                # ts_log("[TimeListener]", f"Poll error: {e}", "DEBUG")
                pass  # Expected during network hiccups
            time.sleep(0.05)  # Poll at 20Hz

    def get_time(self) -> float:
        with self._lock:
            return self._time

    def wait_for_advance(self, last_time: float, timeout: float = 1.0) -> float:
        start = time.time()
        with self._cond:
            while self._time <= last_time:
                if time.time() - start > timeout:
                    return self._time
                self._cond.wait(timeout=0.1)
            return self._time

# ============================================================================
# 主采集函数
# ============================================================================

def run_collector(uav_id: int, json_path: Path, out_dir: Path, scale: float, 
                  time_scale: float, control_base: str, image_base: str):
    ts_log(f"[UAV{uav_id}]", f"Processing {json_path.name}...")
    
    # 加载轨迹
    with open(json_path) as f:
        data = json.load(f)
    raw_logs = data.get("raw_logs", [])
    if not raw_logs:
        ts_log(f"[UAV{uav_id}]", "No raw_logs found", "ERROR")
        return
    
    # 应用scale
    scaled_points = [[r[0]*scale, r[1]*scale, r[2]*scale, r[3], r[4], r[5]] for r in raw_logs]
    smoother = TrajectorySmoother(scaled_points, dt=0.2)
    total_duration = smoother.duration / time_scale
    
    # 输出目录 (直接使用 traj_dir，无 uavX 子目录)
    traj_dir = out_dir / json_path.stem
    traj_dir.mkdir(parents=True, exist_ok=True)
    
    # Collection timing and cmd records
    collection_start_time = datetime.now().isoformat()
    cmd_records = []  # To record command states during trajectory loop
    
    # 轨迹起点
    traj_start = smoother.get_full_state(0)
    # 使用原始高度（地面约0.06m），不做偏移修正
    TARGET_ALT = traj_start['z']  # 直接使用轨迹目标高度
    ALT_TOLERANCE = 0.15  # 放宽到0.15m容差（考虑地面高度波动）
    
    ts_log(f"[UAV{uav_id}]", f"Trajectory: start=({traj_start['x']:.2f},{traj_start['y']:.2f},{traj_start['z']:.2f}), duration={total_duration:.1f}s")

    # 起飞目标位置 (照搬原版逻辑)
    takeoff_target = [traj_start['x'], traj_start['y'], TARGET_ALT]

    # 初始化time_listener为None (避免finally块报错)
    time_listener = None

    try:
        # ========== 0. Kill PX4 + 清理锁文件 + 重建临时目录（彻底清除EKF状态）==========
        # 【关键修复】使用/px4/full_reset API来完全重置EKF状态，解决坐标漂移问题
        ts_log(f"[UAV{uav_id}]", "Step 0: Kill PX4 + cleanup lock files + rebuild temp dir...")

        # 首先使用/px4/kill停止PX4进程
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/px4/kill", payload={}, timeout=30.0)
        if code == 200:
            ts_log(f"[UAV{uav_id}]", f"PX4 kill: {resp.get('message', 'success')}")
        else:
            ts_log(f"[UAV{uav_id}]", f"PX4 kill failed: code={code}, resp={resp}", "WARN")

        # 清理锁文件（仅清理锁文件，不重建临时目录）
        cleanup_count = cleanup_px4_lock_files_only(uav_id)
        ts_log(f"[UAV{uav_id}]", f"Lock file cleanup: {cleanup_count} items")

        # 等待端口释放
        time.sleep(0.5)

        # ========== 1. Teleport UAV (PX4不运行时进行) ==========
        GROUND_Z = 0.06  # Ground level for teleport
        teleport_pos = [traj_start['x'], traj_start['y'], GROUND_Z]
        ts_log(f"[UAV{uav_id}]", f"Step 1: Teleporting to ({teleport_pos[0]:.2f},{teleport_pos[1]:.2f},{teleport_pos[2]:.2f})...")

        try:
            import urllib.request
            import urllib.error
            reset_url = f"{image_base}/uav/{uav_id}/reset"
            reset_payload = json.dumps({"position": teleport_pos, "yaw_deg": 0.0}).encode('utf-8')
            req = urllib.request.Request(reset_url, data=reset_payload, headers={'Content-Type': 'application/json'}, method='POST')
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                reset_result = json.loads(resp.read().decode('utf-8'))
                ts_log(f"[UAV{uav_id}]", f"Teleport result: {reset_result.get('status', 'unknown')}")
        except Exception as e:
            ts_log(f"[UAV{uav_id}]", f"Teleport failed: {e}", "WARN")

        # 验证teleport成功
        time.sleep(0.5)
        MAX_TELEPORT_RETRIES = 5
        TELEPORT_TOLERANCE = 0.5

        for attempt in range(MAX_TELEPORT_RETRIES):
            pose = _fetch_pose_only(image_base, uav_id, timeout=5.0) or {}
            current_pos = pose.get("position") or [0, 0, 0]
            xy_dist = math.sqrt((current_pos[0] - teleport_pos[0])**2 + (current_pos[1] - teleport_pos[1])**2)

            if xy_dist < TELEPORT_TOLERANCE:
                ts_log(f"[UAV{uav_id}]", f"Teleport verified: pos=({current_pos[0]:.2f},{current_pos[1]:.2f},{current_pos[2]:.2f}), dist={xy_dist:.3f}m")
                break
            else:
                ts_log(f"[UAV{uav_id}]", f"Teleport not settled: dist={xy_dist:.2f}m, retry {attempt+1}/{MAX_TELEPORT_RETRIES}")
                try:
                    reset_url = f"{image_base}/uav/{uav_id}/reset"
                    reset_payload = json.dumps({"position": teleport_pos, "yaw_deg": 0.0}).encode('utf-8')
                    req = urllib.request.Request(reset_url, data=reset_payload, headers={'Content-Type': 'application/json'}, method='POST')
                    with urllib.request.urlopen(req, timeout=10.0) as resp:
                        pass
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"Teleport retry error: {e}", "WARN")
                time.sleep(1.0)

        # 最终位置
        pose = _fetch_pose_only(image_base, uav_id, timeout=5.0) or {}
        current_pos = pose.get("position") or teleport_pos
        ts_log(f"[UAV{uav_id}]", f"Teleport complete: pos=({current_pos[0]:.2f},{current_pos[1]:.2f},{current_pos[2]:.2f})")

        # ========== 2. 等待2秒让物理引擎稳定 ==========
        ts_log(f"[UAV{uav_id}]", "Step 2: Waiting 2s for physics to settle...")
        time.sleep(2.0)

        # ========== 3. 启动PX4进程（先重建临时目录清除EKF状态）==========
        # 【关键修复】在启动PX4前重建临时目录，彻底清除EKF状态
        # 这是解决多轨迹采集时坐标漂移问题的核心步骤
        ts_log(f"[UAV{uav_id}]", "Step 3: Starting PX4 process...")

        # 首先调用/px4/rebuild_temp API重建临时目录（清除EKF状态）
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/px4/rebuild_temp", payload={}, timeout=10.0)
        if code == 200:
            ts_log(f"[UAV{uav_id}]", f"PX4 temp dir rebuilt: {resp.get('message', 'success')}")
        else:
            ts_log(f"[UAV{uav_id}]", f"PX4 temp dir rebuild failed (non-critical): code={code}", "WARN")

        px4_start_success = False
        for attempt in range(3):
            code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/px4/start", payload={}, timeout=30.0)
            if code == 200:
                ts_log(f"[UAV{uav_id}]", f"PX4 started: {resp.get('message', 'success')}")
                px4_start_success = True
                break
            else:
                ts_log(f"[UAV{uav_id}]", f"PX4 start failed (attempt {attempt+1}/3): code={code}", "WARN")
                if attempt < 2:
                    # 重试前清理锁文件并等待
                    cleanup_px4_completely(uav_id)
                    time.sleep(2.0)

        if not px4_start_success:
            ts_log(f"[UAV{uav_id}]", f"PX4 start failed after 3 attempts, skipping trajectory", "ERROR")
            return

        # ========== 4. 阻塞等待PX4 ready ==========
        ts_log(f"[UAV{uav_id}]", "Step 4: Waiting for PX4 ready...")
        px4_ready_timeout = 60.0
        px4_ready_start = time.time()
        px4_ready = False
        while time.time() - px4_ready_start < px4_ready_timeout:
            try:
                code, obj = _http_json("GET", f"{image_base}/uav/{uav_id}/px4/ready", timeout=5.0)
                if 200 <= code < 300 and isinstance(obj, dict) and obj.get("ready") is True:
                    ts_log(f"[UAV{uav_id}]", "PX4 ready!")
                    px4_ready = True
                    break
            except Exception as e:
                ts_log(f"[UAV{uav_id}]", f"PX4 ready check error: {e}", "WARN")
            time.sleep(1.0)

        if not px4_ready:
            ts_log(f"[UAV{uav_id}]", "PX4 ready timeout after 60s, skipping trajectory", "ERROR")
            return

        # 额外等待让PX4 EKF完全稳定
        time.sleep(2.0)
        ts_log(f"[UAV{uav_id}]", "Waited 2s for PX4 EKF to stabilize")

        # 【关键修复】发送SET_GPS_GLOBAL_ORIGIN命令强制重置EKF原点
        # 这确保每条轨迹的EKF原点与当前GPS位置一致，避免坐标漂移
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/px4/set_ekf_origin", payload={}, timeout=10.0)
        if code == 200:
            ts_log(f"[UAV{uav_id}]", f"EKF origin set: {resp.get('message', 'success')}")
        else:
            ts_log(f"[UAV{uav_id}]", f"Set EKF origin failed (non-critical): code={code}", "WARN")

        # 【关键修复】PX4使用LOCAL_NED坐标系，原点是当前起飞位置
        # 发送给PX4的坐标应该是相对于轨迹起点的偏移量，而不是Isaac Sim绝对坐标
        #
        # traj_origin: 轨迹起点坐标，用于将轨迹坐标转换为相对坐标
        # position_offset: Isaac实际位置与轨迹起点的微小差异（teleport误差，通常<1cm）
        traj_origin = [traj_start['x'], traj_start['y'], traj_start['z']]

        # position_offset: Isaac当前位置 - 轨迹起点（应该很小，约几mm）
        position_offset = [
            current_pos[0] - traj_start['x'],
            current_pos[1] - traj_start['y'],
            0.0  # Z offset is 0 - use absolute trajectory altitude
        ]

        # 检查offset是否合理
        xy_offset = math.sqrt(position_offset[0]**2 + position_offset[1]**2)
        if xy_offset > 1.0:
            ts_log(f"[UAV{uav_id}]", f"WARNING: Large position offset {xy_offset:.2f}m, teleport may have failed!", "WARN")

        ts_log(f"[UAV{uav_id}]", f"Actual start: ({current_pos[0]:.2f},{current_pos[1]:.2f},{current_pos[2]:.2f})")
        ts_log(f"[UAV{uav_id}]", f"Traj start: ({traj_start['x']:.2f},{traj_start['y']:.2f},{traj_start['z']:.2f})")
        ts_log(f"[UAV{uav_id}]", f"Traj origin for PX4: ({traj_origin[0]:.2f},{traj_origin[1]:.2f},{traj_origin[2]:.2f})")
        ts_log(f"[UAV{uav_id}]", f"Teleport offset: XY=({position_offset[0]:.3f},{position_offset[1]:.3f})")

        # Takeoff target - 使用相对坐标（相对于起飞位置）
        # 起飞时目标是原地悬停在轨迹起点高度，所以XY相对偏移为0+teleport误差
        takeoff_target = [
            position_offset[0],  # 相对X = 0 + teleport误差
            position_offset[1],  # 相对Y = 0 + teleport误差
            TARGET_ALT           # 绝对高度
        ]

        # ========== 2. 检查当前高度 ==========
        current_alt = current_pos[2]
        ts_log(f"[UAV{uav_id}]", f"Current altitude: {current_alt:.2f}m, takeoff target: {TARGET_ALT:.2f}m")

        # ========== 2.5 启动服务端记录 (在起飞前启动，记录完整起飞过程) ==========
        # Search for PX4 ULG file
        ulg_path = ""
        possible_roots = [
            Path(f"/tmp"),
            Path(f"/home/user/PX4-Autopilot/build/px4_sitl_default/rootfs/fs/microsd/log")
        ]

        try:
            candidates = []
            for root in possible_roots:
                if root.exists():
                    if str(root) == "/tmp":
                        instance_dirs = list(root.glob(f"px4_{uav_id}_*"))
                        for idir in instance_dirs:
                            log_dir = idir / "log"
                            if log_dir.exists():
                                candidates.extend(list(log_dir.rglob("*.ulg")))
                    else:
                        candidates.extend(list(root.glob("**/*.ulg")))

            if candidates:
                latest_ulg = max(candidates, key=lambda p: p.stat().st_mtime)
                ulg_path = str(latest_ulg.absolute())
                ts_log(f"[UAV{uav_id}]", f"Found ULG file: {ulg_path}")
            else:
                ts_log(f"[UAV{uav_id}]", "No ULG file found", "WARN")
        except Exception as e:
            ts_log(f"[UAV{uav_id}]", f"Error finding ULG file: {e}", "WARN")

        ts_log(f"[UAV{uav_id}]", "Starting server-side recording (before takeoff)...")
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/buffer/start", {
            "save_dir": str(traj_dir.absolute()),
            "traj_json": str(json_path.absolute()),
            "traj_name": json_path.stem,
            "ulg_path": ulg_path,
            "position_offset": position_offset,
            "scale": scale,
            "time_scale": time_scale
        })
        ts_log(f"[UAV{uav_id}]", f"Buffer start: {code} {resp}")
        buffer_start_time = time.time()

        # 记录起飞开始时的仿真时间 (用于标记起飞阶段obs数量)
        takeoff_start_sim_ts = None

        if current_alt < TARGET_ALT - ALT_TOLERANCE:
            # ========== 3. 起飞流程 - MAVLink发送 + HTTP状态确认 ==========
            ts_log(f"[UAV{uav_id}]", "Step 3: Starting takeoff sequence (via MAVLink)...")

            # 建立MAVLink发送连接
            from pymavlink import mavutil
            px4_send_port = 14580 + uav_id
            send_conn_str = f"udpout:127.0.0.1:{px4_send_port}"
            mav_send = mavutil.mavlink_connection(send_conn_str, source_system=255)
            mav_send.target_system = uav_id + 1
            mav_send.target_component = 1
            ts_log(f"[UAV{uav_id}]", f"MAVLink connected: send to {px4_send_port}")

            yaw_rad = smoother.get_yaw_and_rate(0)[0]

            # ENU -> NED 坐标变换
            ned_x = takeoff_target[1]
            ned_y = takeoff_target[0]
            ned_z = -takeoff_target[2]
            ned_yaw = 0.5 * math.pi - yaw_rad + math.pi
            ned_yaw = (ned_yaw + math.pi) % (2 * math.pi) - math.pi

            def send_setpoint():
                mav_send.mav.set_position_target_local_ned_send(
                    0, uav_id + 1, 1,
                    mavutil.mavlink.MAV_FRAME_LOCAL_NED,
                    0x0DF8,  # Position only (ignore velocity and acceleration)
                    ned_x, ned_y, ned_z,
                    0, 0, 0,
                    0, 0, 0,
                    ned_yaw, 0
                )

            # 3.1 发送setpoints (30次, ~1.5s) - ARM前必须先有setpoint流
            ts_log(f"[UAV{uav_id}]", "Sending pre-ARM setpoints (30x via MAVLink)...")
            for _ in range(30):
                send_setpoint()
                time.sleep(0.05)

            # 3.2 ARM (通过控制器HTTP接口，控制器内部用MAVLink发送)
            ts_log(f"[UAV{uav_id}]", "Arming via controller HTTP API...")
            arm_success = False
            for attempt in range(15):
                # 通过控制器API发送ARM命令 (控制器内部用MAVLink)
                try:
                    arm_cmd = {"cmd": "arm"}
                    code, resp = _http_json("POST", f"{control_base}/command", payload=arm_cmd, timeout=5.0)
                    ts_log(f"[UAV{uav_id}]", f"ARM cmd: code={code}, ok={resp.get('ok')}")
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"ARM cmd error: {e}", "WARN")

                # 继续发送setpoints保持OFFBOARD模式前提
                for _ in range(20):
                    send_setpoint()
                    time.sleep(0.05)

                # 检查ARM状态
                try:
                    status_cmd = {"cmd": "get_status"}
                    code, resp = _http_json("POST", f"{control_base}/command", payload=status_cmd, timeout=2.0)
                    armed = resp.get('status', {}).get('armed')
                    ts_log(f"[UAV{uav_id}]", f"Status check: code={code}, armed={armed}")
                    if code == 200 and armed:
                        arm_success = True
                        ts_log(f"[UAV{uav_id}]", f"Armed confirmed! (attempt {attempt+1})")
                        break
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"Status check error: {e}", "WARN")
                ts_log(f"[UAV{uav_id}]", f"ARM attempt {attempt+1}: waiting...")

            if not arm_success:
                ts_log(f"[UAV{uav_id}]", "Failed to ARM after 10 attempts, skipping trajectory", "ERROR")
                return

            # 3.3 发送setpoints (60次, ~3s) - ARM后继续发送防止RTL
            ts_log(f"[UAV{uav_id}]", "Sending post-ARM setpoints (60x via MAVLink)...")
            for _ in range(60):
                send_setpoint()
                time.sleep(0.05)

            # 3.4 OFFBOARD模式 (通过控制器HTTP接口)
            ts_log(f"[UAV{uav_id}]", "Setting OFFBOARD mode via controller HTTP API...")
            offboard_success = False
            for attempt in range(10):
                # 通过控制器API发送OFFBOARD命令
                try:
                    mode_cmd = {"cmd": "set_mode", "mode": "OFFBOARD"}
                    code, resp = _http_json("POST", f"{control_base}/command", payload=mode_cmd, timeout=5.0)
                    ts_log(f"[UAV{uav_id}]", f"OFFBOARD cmd: code={code}, ok={resp.get('ok')}")
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"OFFBOARD cmd error: {e}", "WARN")

                # 继续发送setpoints
                for _ in range(20):
                    send_setpoint()
                    time.sleep(0.05)

                # 检查模式状态
                try:
                    status_cmd = {"cmd": "get_status"}
                    code, resp = _http_json("POST", f"{control_base}/command", payload=status_cmd, timeout=2.0)
                    mode = resp.get("status", {}).get("mode", "")
                    ts_log(f"[UAV{uav_id}]", f"Status check: code={code}, mode={mode}")
                    if code == 200 and "OFFBOARD" in mode.upper():
                        offboard_success = True
                        ts_log(f"[UAV{uav_id}]", f"OFFBOARD confirmed! (attempt {attempt+1})")
                        break
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"Status check error: {e}", "WARN")
                ts_log(f"[UAV{uav_id}]", f"OFFBOARD attempt {attempt+1}: waiting...")

            if not offboard_success:
                ts_log(f"[UAV{uav_id}]", "Failed to set OFFBOARD after 10 attempts, skipping trajectory", "ERROR")
                return

            # 3.5 Climb to target altitude (MAVLink发送setpoint，HTTP位置确认)
            ts_log(f"[UAV{uav_id}]", f"Climbing to {takeoff_target[2]:.1f}m...")
            takeoff_start = time.time()
            reached_altitude = False
            last_log_time = 0

            while time.time() - takeoff_start < 45.0:
                # 发送setpoint (MAVLink)
                send_setpoint()

                # 定期检查高度 (HTTP)
                now = time.time()
                if now - last_log_time >= 0.5:
                    try:
                        pose = _fetch_pose_only(image_base, uav_id) or {}
                        current_alt = (pose.get("position") or [0, 0, 0])[2]
                        ts_log(f"[UAV{uav_id}]", f"Climbing: {current_alt:.2f}m / {takeoff_target[2]:.2f}m")
                        if current_alt >= takeoff_target[2] - ALT_TOLERANCE:
                            reached_altitude = True
                            ts_log(f"[UAV{uav_id}]", "Target altitude reached!")
                            break
                    except Exception as e:
                        ts_log(f"[UAV{uav_id}]", f"Altitude check error: {e}", "WARN")
                    last_log_time = now

                time.sleep(0.02)

            if not reached_altitude:
                ts_log(f"[UAV{uav_id}]", "ERROR: Did not reach target altitude, aborting trajectory", "ERROR")
                return  # 起飞失败，中止本轨迹

            # 3.6 Stabilize (继续发送setpoint)
            ts_log(f"[UAV{uav_id}]", "Stabilizing...")
            for _ in range(25):
                send_setpoint()
                time.sleep(0.02)

            # 记录起飞结束时的仿真时间
            takeoff_end_sim_ts = time.time()
            ts_log(f"[UAV{uav_id}]", f"Takeoff complete, elapsed: {takeoff_end_sim_ts - buffer_start_time:.2f}s")

        # Initialize MAVLink sender for trajectory tracking
        sender = MavlinkSetpointSender(uav_id)
        if not sender.connect():
            ts_log(f"[UAV{uav_id}]", "ERROR: Failed to connect MAVLink, aborting", "ERROR")
            return

        # ========== 4. 初始化时间同步 (HTTP polling) ==========
        # 在时间同步期间持续发送 setpoint 保持位置
        time_listener = SimTimeListener(f"{image_base}/sim_time")
        time_listener.start()

        # 等待时间同步，同时持续发送 setpoint
        ts_log(f"[UAV{uav_id}]", "Waiting for time sync (sending hold setpoints)...")
        wait_start = time.time()
        hold_setpoint_count = 0
        while time_listener.get_time() <= 0 and time.time() - wait_start < 10.0:
            # 发送保持位置的 setpoint (使用相对坐标，起点处相对偏移为0)
            try:
                state = smoother.get_full_state(0)  # 获取轨迹起点状态
                # 相对坐标：state - traj_origin + position_offset
                hold_rel_x = state['x'] - traj_origin[0] + position_offset[0]
                hold_rel_y = state['y'] - traj_origin[1] + position_offset[1]
                sender.send_pva(
                    hold_rel_x, hold_rel_y, state['z'],
                    0, 0, 0,  # 速度为0
                    0, 0, 0,  # 加速度为0
                    state['yaw'], 0
                )
                hold_setpoint_count += 1
            except Exception as e:
                ts_log(f"[UAV{uav_id}]", f"Hold setpoint error: {e}", "WARN")
            time.sleep(0.05)  # 20Hz

        ts_log(f"[UAV{uav_id}]", f"Time sync complete, sent {hold_setpoint_count} hold setpoints")

        sim_start_ts = time_listener.get_time()
        ts_log(f"[UAV{uav_id}]", f"Sim start time: {sim_start_ts:.3f}s")

        # 记录轨迹开始时的仿真时间 (用于标记起飞阶段 obs 数量)
        trajectory_start_sim_ts = sim_start_ts

        LOOKAHEAD_TIME = 0.18  # 180ms前瞻 (优化为0.2m精度)
        last_cmd_sim_ts = sim_start_ts

        # ========== 5. 轨迹跟踪循环 ==========
        ts_log(f"[UAV{uav_id}]", f"Starting trajectory (duration={total_duration:.1f}s)...")
        
        # Real-time timeout: max 2x expected duration plus buffer
        max_real_time = total_duration * 2 + 30.0
        traj_start_real = time.time()
        
        while True:
            # Real-time timeout check
            if time.time() - traj_start_real > max_real_time:
                ts_log(f"[UAV{uav_id}]", f"Trajectory timeout after {max_real_time:.1f}s real time", "WARN")
                break
            
            current_sim_ts = time_listener.wait_for_advance(last_cmd_sim_ts, timeout=1.0)
            elapsed = (current_sim_ts - sim_start_ts) * time_scale
            
            if current_sim_ts <= last_cmd_sim_ts:
                continue
            last_cmd_sim_ts = current_sim_ts
            
            if elapsed >= smoother.duration:
                break
            
            # 获取前瞻状态
            state = smoother.get_full_state(elapsed + LOOKAHEAD_TIME * time_scale)
            
            # 手动缩放速度和加速度 (Fix for velocity anomaly)
            # Velocity scales linearly with time_scale
            vx_scaled = state['vx'] * time_scale
            vy_scaled = state['vy'] * time_scale
            vz_scaled = state['vz'] * time_scale
            # Acceleration scales with square of time_scale
            ax_scaled = state['ax'] * (time_scale ** 2)
            ay_scaled = state['ay'] * (time_scale ** 2)
            az_scaled = state['az'] * (time_scale ** 2)
            
            # Send via MAVLink UDP (no HTTP for setpoints per user request)
            # 【关键修复】发送相对于轨迹起点的坐标，而不是绝对坐标
            # PX4 LOCAL_NED 坐标系原点是起飞位置，所以需要用 (state - traj_origin + position_offset)
            rel_x = state['x'] - traj_origin[0] + position_offset[0]
            rel_y = state['y'] - traj_origin[1] + position_offset[1]
            rel_z = state['z']  # Z使用绝对高度（从地面算起）

            try:
                sender.send_pva(
                    rel_x, rel_y, rel_z,
                    vx_scaled, vy_scaled, vz_scaled,
                    ax_scaled, ay_scaled, az_scaled,
                    state['yaw'], state['yaw_rate'] * time_scale
                )
            except Exception as e:
                ts_log(f"[UAV{uav_id}]", f"Trajectory setpoint error: {e}, retrying...", "WARN")
                # Retry once
                try:
                    time.sleep(0.01)
                    sender.send_pva(
                        rel_x, rel_y, rel_z,
                        vx_scaled, vy_scaled, vz_scaled,
                        ax_scaled, ay_scaled, az_scaled,
                        state['yaw'], state['yaw_rate'] * time_scale
                    )
                except Exception as e2:
                    ts_log(f"[UAV{uav_id}]", f"Retry failed: {e2}", "ERROR")
            
            # Record command state for CSV generation
            # Note: obs coords are converted to trajectory space via (Isaac_pos - position_offset)
            # So cmd should use raw trajectory coords (state) to match
            cmd_records.append({
                'sim_time': current_sim_ts,
                'cmd_x': state['x'],  # Raw trajectory coords
                'cmd_y': state['y'],  # Raw trajectory coords
                'cmd_z': state['z'],  # Raw trajectory coords
                'cmd_roll': state['roll'],
                'cmd_pitch': state['pitch'],
                'cmd_yaw': state['yaw'],
                'cmd_roll_rate': state['roll_rate'] * time_scale,
                'cmd_pitch_rate': state['pitch_rate'] * time_scale,
                'cmd_yaw_rate': state['yaw_rate'] * time_scale,
                'cmd_vx': vx_scaled,
                'cmd_vy': vy_scaled,
                'cmd_vz': vz_scaled,
                'cmd_ax': ax_scaled,
                'cmd_ay': ay_scaled,
                'cmd_az': az_scaled,
            })
        
        ts_log(f"[UAV{uav_id}]", "Trajectory complete!")
        collection_end_time = datetime.now().isoformat()
        sim_end_ts = time_listener.get_time()
        
        # ========== 8. 停止记录并获取原始观测数据 ==========
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/buffer/stop", {
            "return_raw_observations": True  # Request raw observation data
        })
        ts_log(f"[UAV{uav_id}]", f"Buffer stop: {code} {resp.get('msg', '')}")
        
        raw_observations = resp.get("raw_observations", [])
        state_count = len(raw_observations)
        ts_log(f"[UAV{uav_id}]", f"Received {state_count} raw observations, {len(cmd_records)} cmd records")
        
        # ========== 9. 合并 cmd_records 和 raw_observations ==========
        # Create interpolators for cmd data to align with obs timestamps
        if cmd_records and raw_observations:
            import csv
            
            # Extract cmd data for interpolation
            cmd_times = np.array([r['sim_time'] for r in cmd_records])
            cmd_arrays = {k: np.array([r[k] for r in cmd_records]) 
                          for k in ['cmd_x', 'cmd_y', 'cmd_z', 'cmd_roll', 'cmd_pitch', 'cmd_yaw',
                                   'cmd_roll_rate', 'cmd_pitch_rate', 'cmd_yaw_rate',
                                   'cmd_vx', 'cmd_vy', 'cmd_vz', 'cmd_ax', 'cmd_ay', 'cmd_az']}
            
            # Extract obs times for interpolation
            obs_times = np.array([r.get('sim_time', 0) for r in raw_observations])
            
            # Debug logging for timestamp alignment
            if len(cmd_times) > 0 and len(obs_times) > 0:
                first_cmd_time = cmd_times.min()
                last_cmd_time = cmd_times.max()
                first_obs_time = obs_times.min()
                last_obs_time = obs_times.max()

                ts_log(f"[UAV{uav_id}]", f"CMD times: [{first_cmd_time:.4f}, {last_cmd_time:.4f}] ({len(cmd_times)} records)")
                ts_log(f"[UAV{uav_id}]", f"OBS times: [{first_obs_time:.4f}, {last_obs_time:.4f}] ({len(obs_times)} records)")
                ts_log(f"[UAV{uav_id}]", f"Time offset: OBS started {first_cmd_time - first_obs_time:.4f}s before CMD")

                # Check for overlap
                overlap = min(last_cmd_time, last_obs_time) - max(first_cmd_time, first_obs_time)
                if overlap <= 0:
                    ts_log(f"[UAV{uav_id}]", f"WARNING: No timestamp overlap! CMD and OBS have disjoint time ranges", "WARN")
                else:
                    ts_log(f"[UAV{uav_id}]", f"Valid overlap duration: {overlap:.4f}s")
            else:
                ts_log(f"[UAV{uav_id}]", f"Empty data: cmd_times={len(cmd_times)}, obs_times={len(obs_times)}", "WARN")
                first_cmd_time = 0
                last_cmd_time = 0

            # Filter observations to only include those within CMD time range
            # This fixes the alignment issue where recording starts before trajectory commands
            first_cmd_time = cmd_times.min() if len(cmd_times) > 0 else 0
            last_cmd_time = cmd_times.max() if len(cmd_times) > 0 else float('inf')

            filtered_observations = []
            skipped_before = 0
            skipped_after = 0
            for obs in raw_observations:
                obs_time = obs.get('sim_time', 0)
                if obs_time < first_cmd_time:
                    skipped_before += 1
                elif obs_time > last_cmd_time:
                    skipped_after += 1
                else:
                    filtered_observations.append(obs)

            ts_log(f"[UAV{uav_id}]", f"Filtered observations: {len(filtered_observations)} kept, {skipped_before} before CMD, {skipped_after} after CMD")

            if not filtered_observations:
                ts_log(f"[UAV{uav_id}]", "ERROR: No observations in CMD time range!", "ERROR")

            # Generate data.csv
            csv_path = traj_dir / "data.csv"
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # Header
                writer.writerow([
                    "step_idx", "sim_time", "image_path",
                    "cmd_x", "cmd_y", "cmd_z", "cmd_roll", "cmd_pitch", "cmd_yaw",
                    "cmd_roll_rate", "cmd_pitch_rate", "cmd_yaw_rate",
                    "cmd_vx", "cmd_vy", "cmd_vz", "cmd_ax", "cmd_ay", "cmd_az",
                    "obs_pos_x", "obs_pos_y", "obs_pos_z",
                    "obs_att_w", "obs_att_x", "obs_att_y", "obs_att_z",
                    "obs_linvel_x", "obs_linvel_y", "obs_linvel_z",
                    "obs_angvel_x", "obs_angvel_y", "obs_angvel_z",
                    "obs_linacc_x", "obs_linacc_y", "obs_linacc_z",
                    "obs_roll", "obs_pitch", "obs_yaw"
                ])

                for idx, obs in enumerate(filtered_observations):
                    obs_time = obs.get('sim_time', 0)

                    # Interpolate cmd values at obs timestamp (now guaranteed to be within CMD range)
                    cmd_row = {}
                    for k in cmd_arrays.keys():
                        cmd_row[k] = float(np.interp(obs_time, cmd_times, cmd_arrays[k]))
                    
                    # Extract observation data
                    pos = obs.get('position', [0, 0, 0])
                    att = obs.get('attitude', [1, 0, 0, 0])  # [w, x, y, z]
                    vel = obs.get('linear_velocity', [0, 0, 0])
                    ang_vel = obs.get('angular_velocity', [0, 0, 0])
                    acc = obs.get('linear_acceleration', [0, 0, 0])
                    
                    # Quaternion to Euler (for obs_roll/pitch/yaw)
                    w, x, y, z = att[0], att[1], att[2], att[3]
                    sinr_cosp = 2.0 * (w * x + y * z)
                    cosr_cosp = 1.0 - 2.0 * (x * x + y * y)
                    obs_roll = math.atan2(sinr_cosp, cosr_cosp)
                    sinp = 2.0 * (w * y - z * x)
                    obs_pitch = math.asin(max(-1, min(1, sinp)))
                    siny_cosp = 2.0 * (w * z + x * y)
                    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
                    obs_yaw = math.atan2(siny_cosp, cosy_cosp)
                    
                    # Apply position offset correction for observations
                    corrected_pos = [
                        pos[0] - position_offset[0],
                        pos[1] - position_offset[1],
                        pos[2] - position_offset[2]
                    ]
                    
                    writer.writerow([
                        idx, f"{obs_time:.8f}", "",  # image_path placeholder
                        f"{cmd_row['cmd_x']:.8f}", f"{cmd_row['cmd_y']:.8f}", f"{cmd_row['cmd_z']:.8f}",
                        f"{cmd_row['cmd_roll']:.8f}", f"{cmd_row['cmd_pitch']:.8f}", f"{cmd_row['cmd_yaw']:.8f}",
                        f"{cmd_row['cmd_roll_rate']:.8f}", f"{cmd_row['cmd_pitch_rate']:.8f}", f"{cmd_row['cmd_yaw_rate']:.8f}",
                        f"{cmd_row['cmd_vx']:.8f}", f"{cmd_row['cmd_vy']:.8f}", f"{cmd_row['cmd_vz']:.8f}",
                        f"{cmd_row['cmd_ax']:.8f}", f"{cmd_row['cmd_ay']:.8f}", f"{cmd_row['cmd_az']:.8f}",
                        f"{corrected_pos[0]:.8f}", f"{corrected_pos[1]:.8f}", f"{corrected_pos[2]:.8f}",
                        f"{att[0]:.8f}", f"{att[1]:.8f}", f"{att[2]:.8f}", f"{att[3]:.8f}",
                        f"{vel[0]:.8f}", f"{vel[1]:.8f}", f"{vel[2]:.8f}",
                        f"{ang_vel[0]:.8f}", f"{ang_vel[1]:.8f}", f"{ang_vel[2]:.8f}",
                        f"{acc[0]:.8f}", f"{acc[1]:.8f}", f"{acc[2]:.8f}",
                        f"{obs_roll:.8f}", f"{obs_pitch:.8f}", f"{obs_yaw:.8f}"
                    ])
            
            ts_log(f"[UAV{uav_id}]", f"Generated data.csv with {len(filtered_observations)} rows (filtered from {len(raw_observations)} raw)")
        else:
            ts_log(f"[UAV{uav_id}]", "No data to merge (empty cmd_records or observations)", "WARN")
        
        # ========== 10. 保存元数据和原始数据 ==========
        # Calculate filter stats for metadata (need to handle case where filtering wasn't done)
        filtered_count = len(filtered_observations) if 'filtered_observations' in dir() else state_count
        skipped_before_count = skipped_before if 'skipped_before' in dir() else 0
        skipped_after_count = skipped_after if 'skipped_after' in dir() else 0

        # 计算起飞阶段的 obs 数量 (trajectory_start_sim_ts 之前的 obs)
        takeoff_obs_count = 0
        if raw_observations and 'trajectory_start_sim_ts' in dir():
            for obs in raw_observations:
                if obs.get('sim_time', 0) < trajectory_start_sim_ts:
                    takeoff_obs_count += 1

        meta = {
            "traj_name": json_path.stem,
            "traj_json": str(json_path.absolute()),
            "uav_id": uav_id,
            "scale": scale,
            "time_scale": time_scale,
            "position_offset": position_offset,
            "isaac_start_pos": current_pos,
            "json_start_pos": [traj_start['x'], traj_start['y'], traj_start['z']],
            "collection_start_time": collection_start_time,
            "collection_end_time": collection_end_time,
            "sim_start_ts": sim_start_ts,
            "sim_end_ts": sim_end_ts,
            "trajectory_start_sim_ts": trajectory_start_sim_ts if 'trajectory_start_sim_ts' in dir() else sim_start_ts,
            "trajectory_duration": total_duration,
            "cmd_records_count": len(cmd_records),
            "obs_records_count": state_count,
            "obs_filtered_count": filtered_count,
            "obs_skipped_before_cmd": skipped_before_count,
            "obs_skipped_after_cmd": skipped_after_count,
            "obs_takeoff_phase_count": takeoff_obs_count,
            "ulg_path": ulg_path,
            # Camera parameters for image replay
            "camera": {
                "resolution": [1280, 720],  # Default resolution
                "fov": 90.0,  # Field of view in degrees
                "offset": [0.3, 0.0, 0.0],  # Camera offset from body [x, y, z]
                "rotation": [0.0, 0.0, 180.0]  # Camera rotation [roll, pitch, yaw] degrees
            }
        }
        with open(traj_dir / "metadata.json", "w") as f:
            json.dump(meta, f, indent=4)
        
        # Copy source trajectory JSON
        shutil.copy2(json_path, traj_dir / json_path.name)
        
        # Copy ULG file if available
        if ulg_path and os.path.exists(ulg_path):
            try:
                shutil.copy2(ulg_path, traj_dir / Path(ulg_path).name)
                ts_log(f"[UAV{uav_id}]", f"Copied ULG file to output directory")
            except Exception as e:
                ts_log(f"[UAV{uav_id}]", f"Failed to copy ULG: {e}", "WARN")
        
        # Save raw observations for debugging (optional)
        if raw_observations:
            with open(traj_dir / "raw_observations.json", "w") as f:
                json.dump(raw_observations, f)
        
        ts_log(f"[UAV{uav_id}]", f"Done! Recorded {state_count} observations to {traj_dir}")
        
    except Exception as e:
        import traceback
        ts_log(f"[UAV{uav_id}]", f"Error: {e}\n{traceback.format_exc()}", "ERROR")
    finally:
        if time_listener is not None:
            time_listener.running = False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--uav-id", type=int, default=0)
    parser.add_argument("--input-dir", help="Directory with JSON files (selects by uav_id mod count)")
    parser.add_argument("--json-file", help="Specific JSON file to process")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--scale", type=float, default=0.01)
    parser.add_argument("--time-scale", type=float, default=1.0)
    parser.add_argument("--control-base", default="http://127.0.0.1:5009")
    parser.add_argument("--image-base", default="http://127.0.0.1:8081")
    args = parser.parse_args()
    
    if args.json_file:
        json_path = Path(args.json_file)
    elif args.input_dir:
        files = sorted(glob.glob(os.path.join(args.input_dir, "*.json")))
        if not files:
            print("No JSON files found")
            sys.exit(1)
        json_path = Path(files[args.uav_id % len(files)])
    else:
        print("ERROR: Either --json-file or --input-dir required")
        sys.exit(1)
    
    run_collector(
        uav_id=args.uav_id,
        json_path=json_path,
        out_dir=Path(args.out_dir),
        scale=args.scale,
        time_scale=args.time_scale,
        control_base=args.control_base,
        image_base=args.image_base
    )

