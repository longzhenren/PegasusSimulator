#!/usr/bin/env python3
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
    data = json.dumps(payload).encode("utf-8") if payload else None
    headers = {"Content-Type": "application/json"} if payload else {}
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
        yaw = np.unwrap(np.deg2rad([p[5] for p in points]))  # raw_logs: [x,y,z,roll,pitch,yaw]
        
        self._spline_x = CubicSpline(self.t_orig, x, bc_type='natural')
        self._spline_y = CubicSpline(self.t_orig, y, bc_type='natural')
        self._spline_z = CubicSpline(self.t_orig, z, bc_type='natural')
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
            'yaw': float(self._spline_yaw(t)),
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
# SimTimeListener (照搬原版)
# ============================================================================

class SimTimeListener(threading.Thread):
    def __init__(self, url="http://127.0.0.1:8081/sim_time"):
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
            except:
                pass
            time.sleep(0.1)  # Reduce polling rate to 10Hz to prevent overloading the server

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
    
    # 输出目录
    traj_dir = out_dir / json_path.stem
    uav_dir = traj_dir / f"uav{uav_id}"
    uav_dir.mkdir(parents=True, exist_ok=True)
    
    # 轨迹起点
    traj_start = smoother.get_full_state(0)
    TARGET_ALT = max(traj_start['z'], 0.3)  # 至少0.3米高度
    ALT_TOLERANCE = 0.1
    
    ts_log(f"[UAV{uav_id}]", f"Trajectory: start=({traj_start['x']:.2f},{traj_start['y']:.2f},{traj_start['z']:.2f}), duration={total_duration:.1f}s")
    
    # 起飞目标位置 (照搬原版逻辑)
    takeoff_target = [traj_start['x'], traj_start['y'], TARGET_ALT]
    
    try:
        # ========== 1. Teleport ==========
        ts_log(f"[UAV{uav_id}]", f"Teleporting to ({takeoff_target[0]:.2f}, {takeoff_target[1]:.2f}, 0.07)...")
        _http_json("POST", f"{image_base}/uav/{uav_id}/reset", {
            "position": [takeoff_target[0], takeoff_target[1], 0.07],
            "yaw_deg": math.degrees(smoother.get_yaw_and_rate(0)[0]),
            "hard": True
        })
        time.sleep(2.0)  # 等待teleport生效
        
        # ========== 2. 检查当前高度 ==========
        pose = _fetch_pose_only(image_base, uav_id) or {}
        current_alt = (pose.get("position") or [0, 0, 0])[2]
        ts_log(f"[UAV{uav_id}]", f"Current altitude: {current_alt:.2f}m, target: {TARGET_ALT:.2f}m")
        
        if current_alt < TARGET_ALT - ALT_TOLERANCE:
            # ========== 3. 起飞流程 (照搬原版) ==========
            ts_log(f"[UAV{uav_id}]", "Starting takeoff sequence...")
            
            # 3.1 发送setpoints准备OFFBOARD
            ts_log(f"[UAV{uav_id}]", "Preparing setpoints for OFFBOARD...")
            for _ in range(50):
                cmd = {
                    "cmd": "setpoint",
                    "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.5,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": smoother.get_yaw_and_rate(0)[0], "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{control_base}/command", payload=cmd, timeout=0.5)
                except:
                    pass
                time.sleep(0.02)
            
            # Wait for PX4 to be ready before ARM
            time.sleep(1.0)
            
            # 3.2 ARM (照搬原版) - with more retries and longer waits
            arm_success = False
            for attempt in range(10):  # Increased from 5 to 10 attempts
                try:
                    code, resp = _http_json("POST", f"{control_base}/command", {"cmd": "arm"}, timeout=15.0)
                    ts_log(f"[UAV{uav_id}]", f"ARM attempt {attempt+1}: code={code}, resp={resp}")
                    if 200 <= code < 300 and resp.get("ok"):
                        arm_success = True
                        ts_log(f"[UAV{uav_id}]", "Armed successfully!")
                        break
                    elif code == 500 and "Connection refused" in str(resp):
                        # Port not ready, wait longer
                        ts_log(f"[UAV{uav_id}]", f"Port not ready, waiting...", "WARN")
                        time.sleep(2.0)
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"ARM exception: {e}", "WARN")
                    time.sleep(1.0)
                time.sleep(0.5)
            
            if not arm_success:
                ts_log(f"[UAV{uav_id}]", "WARNING: Failed to ARM", "WARN")
            
            # 3.3 切换OFFBOARD (照搬原版)
            offboard_success = False
            for attempt in range(3):
                try:
                    code, resp = _http_json("POST", f"{control_base}/command", 
                                            {"cmd": "set_mode", "mode": "OFFBOARD"}, timeout=5.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        offboard_success = True
                        ts_log(f"[UAV{uav_id}]", "OFFBOARD mode set!")
                        break
                except:
                    pass
                # 保持setpoint流
                for _ in range(15):
                    try:
                        _http_json("POST", f"{control_base}/command", payload=cmd, timeout=0.5)
                    except:
                        pass
                    time.sleep(0.02)
            
            if not offboard_success:
                ts_log(f"[UAV{uav_id}]", "WARNING: Failed to set OFFBOARD", "WARN")
            
            # 3.4 爬升 (照搬原版)
            ts_log(f"[UAV{uav_id}]", f"Climbing to {TARGET_ALT:.1f}m...")
            takeoff_start = time.time()
            reached_altitude = False
            
            while time.time() - takeoff_start < 45.0:
                cmd = {
                    "cmd": "setpoint",
                    "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.3,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": smoother.get_yaw_and_rate(0)[0], "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{control_base}/command", payload=cmd, timeout=0.5)
                except:
                    pass
                
                # 每0.5秒检查高度
                if int((time.time() - takeoff_start) / 0.5) > int((time.time() - takeoff_start - 0.02) / 0.5):
                    try:
                        pose = _fetch_pose_only(image_base, uav_id) or {}
                        current_alt = (pose.get("position") or [0, 0, 0])[2]
                        ts_log(f"[UAV{uav_id}]", f"Climbing: {current_alt:.2f}m / {TARGET_ALT:.2f}m")
                        if current_alt >= TARGET_ALT - ALT_TOLERANCE:
                            reached_altitude = True
                            ts_log(f"[UAV{uav_id}]", "Target altitude reached!")
                            break
                    except:
                        pass
                time.sleep(0.02)
            
            if not reached_altitude:
                ts_log(f"[UAV{uav_id}]", "WARNING: Did not reach target altitude", "WARN")
            
            # 3.5 稳定 (照搬原版)
            ts_log(f"[UAV{uav_id}]", "Stabilizing...")
            for _ in range(50):
                cmd["vz"] = 0.0
                try:
                    _http_json("POST", f"{control_base}/command", payload=cmd, timeout=0.5)
                except:
                    pass
                time.sleep(0.02)
        
        # ========== 4. 启动服务端记录 ==========
        ts_log(f"[UAV{uav_id}]", "Starting server-side recording...")
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/buffer/start", {
            "save_dir": str(uav_dir.absolute()),
            "traj_json": str(json_path.absolute()),
            "traj_name": json_path.stem,
            "ulg_path": ""  # Will be populated from PX4 logs after collection
        })
        ts_log(f"[UAV{uav_id}]", f"Buffer start: {code} {resp}")
        
        # ========== 5. 初始化UDP发送器 ==========
        sender = MavlinkSetpointSender(uav_id)
        use_udp = sender.connect()
        
        # ========== 6. 初始化时间同步 ==========
        time_listener = SimTimeListener(f"{image_base}/sim_time")
        time_listener.start()
        
        # 等待时间同步
        ts_log(f"[UAV{uav_id}]", "Waiting for time sync...")
        wait_start = time.time()
        while time_listener.get_time() <= 0 and time.time() - wait_start < 10.0:
            time.sleep(0.1)
        
        sim_start_ts = time_listener.get_time()
        ts_log(f"[UAV{uav_id}]", f"Sim start time: {sim_start_ts:.3f}s")
        
        LOOKAHEAD_TIME = 0.2  # 200ms前瞻
        last_cmd_sim_ts = sim_start_ts
        
        # ========== 7. 轨迹跟踪循环 (照搬原版) ==========
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
            
            if use_udp:
                sender.send_pva(
                    state['x'], state['y'], state['z'],
                    state['vx'], state['vy'], state['vz'],
                    state['ax'], state['ay'], state['az'],
                    state['yaw'], state['yaw_rate']
                )
            else:
                # HTTP回退
                cmd = {
                    "cmd": "setpoint",
                    "x": state['x'], "y": state['y'], "z": state['z'],
                    "vx": state['vx'], "vy": state['vy'], "vz": state['vz'],
                    "afx": state['ax'], "afy": state['ay'], "afz": state['az'],
                    "yaw": state['yaw'], "yaw_rate": state['yaw_rate']
                }
                try:
                    _http_json("POST", f"{control_base}/command", payload=cmd, timeout=0.5)
                except:
                    pass
        
        ts_log(f"[UAV{uav_id}]", "Trajectory complete!")
        
        # ========== 8. 停止记录 ==========
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/buffer/stop", {})
        ts_log(f"[UAV{uav_id}]", f"Buffer stop: {code} {resp}")
        state_count = resp.get("state_count", 0)
        
        # ========== 9. 保存元数据 ==========
        meta = {
            "traj_name": json_path.stem,
            "uav_id": uav_id,
            "duration": total_duration,
            "points": state_count,
            "scale": scale,
            "time_scale": time_scale,
        }
        with open(uav_dir / "metadata.json", "w") as f:
            json.dump(meta, f, indent=4)
        shutil.copy2(json_path, traj_dir / json_path.name)
        
        ts_log(f"[UAV{uav_id}]", f"Done! Recorded {state_count} states to {uav_dir}")
        
    except Exception as e:
        import traceback
        ts_log(f"[UAV{uav_id}]", f"Error: {e}\n{traceback.format_exc()}", "ERROR")
    finally:
        time_listener.running = False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--uav-id", type=int, default=0)
    parser.add_argument("--input-dir", help="Directory with JSON files (selects by uav_id mod count)")
    parser.add_argument("--json-file", help="Specific JSON file to process")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--scale", type=float, default=0.01)
    parser.add_argument("--time-scale", type=float, default=2.5)
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

