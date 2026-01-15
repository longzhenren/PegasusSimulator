#!/usr/bin/env python3
# Copyright (c) 2024-2026
# Licensed under the MIT License
"""
MAVLink轨迹数据采集器（mavlink_trajectory_collector.py）

==========================
概述
==========================
本脚本是配合 mavlink_sim_vehicle.py 使用的轨迹数据采集工具，特点：
- 使用 HTTP 接口控制 MAVLink 仿真环境
- 支持 Mission 模式航点导航
- 与 trajectory_data_collector.py 的输入输出格式完全兼容
- 支持多架 UAV 并行采集
- 不依赖 ROS2 或 MAVROS

==========================
与其他采集脚本的区别
==========================
| 特性           | trajectory_data_collector | fast_trajectory_collector | mavlink_trajectory_collector |
|----------------|---------------------------|---------------------------|------------------------------|
| 依赖           | ROS2, MAVROS, PX4         | 无外部依赖                | PX4 (无MAVROS)               |
| 控制方式       | MAVROS服务 + HTTP         | 纯HTTP                    | HTTP + MAVLink Mission       |
| 状态监控       | ROS2话题订阅              | HTTP轮询                  | HTTP轮询                     |
| 仿真环境       | 8_camera_vehicle.py       | fast_sim_vehicle.py       | mavlink_sim_vehicle.py       |
| 输入格式       | JSON轨迹文件              | JSON轨迹文件（相同）      | JSON轨迹文件（相同）         |
| 输出格式       | CSV + PNG + ULG           | CSV + PNG（相同，无ULG）  | CSV + PNG + ULG              |
| 控制精度       | 高（真实飞控）            | 中（简化模型）            | 高（真实飞控）               |

==========================
输入轨迹JSON格式（与原系统完全相同）
==========================
{
  "raw_logs": [
    [x, y, z, roll, yaw, pitch]   // 初始位置（用于重置）
  ],
  "preprocessed_logs": [
    [x, y, z, roll, yaw, pitch],  // 轨迹点序列（ENU坐标系）
    ...
  ]
}

==========================
输出目录结构（与原系统完全相同）
==========================
<out_dir>/
  ├── mission_status.csv           # 全局任务状态日志
  └── <traj_name>/
      └── uav<id>/
          ├── data.csv             # 主数据文件（路径为相对路径）
          ├── metadata.json        # 轨迹元数据
          ├── px4_uav<id>_<ts>.ulg # PX4飞行日志（可选）
          └── images/
              ├── img_000000_<ts_ms>.png
              └── ...

==========================
命令行参数
==========================
--input-dir PATH        轨迹JSON文件目录（必需）
--pattern GLOB          JSON文件匹配模式（默认：*.json）
--out-dir PATH          输出目录（默认：./recordings）
--config PATH           UAV配置文件路径（默认：./multi_uav_config.json）
--uav-ids IDS           指定UAV ID列表，逗号分隔
--control-base URL      控制器基础URL（默认：http://127.0.0.1:5009）
--image-base URL        图像服务基础URL（默认：http://127.0.0.1:8081）
--scale FLOAT           坐标缩放因子（默认：0.01）
--max-points INT        最大轨迹点数（默认：0=不限制）
--z-down / --z-up       Z轴方向（默认：--z-down）
--reset-timeout FLOAT   重置超时时间（默认：120s）
--cmd-timeout FLOAT     命令超时时间（默认：60s）
--image-timeout FLOAT   图像获取超时（默认：10s）
--image-retries INT     图像获取重试次数（默认：30）
--skip-existing         跳过已存在的轨迹（默认）
--no-skip-existing      不跳过已存在的轨迹
--dry-run               仅扫描文件，不执行采集
--use-mission           使用Mission模式（多航点一次性上传）

==========================
使用示例
==========================
# 基础采集（需要先启动 mavlink_sim_vehicle.py）
python examples/mavlink_trajectory_collector.py \\
  --input-dir ~/trajectories \\
  --out-dir ~/recordings \\
  --uav-ids 0

# 多机并行采集
python examples/mavlink_trajectory_collector.py \\
  --input-dir ~/trajectories \\
  --config examples/multi_uav_config.json

# 使用Mission模式（一次性上传所有航点）
python examples/mavlink_trajectory_collector.py \\
  --input-dir ~/trajectories \\
  --use-mission

"""

import argparse
import base64
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
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """生成带时间戳的日志消息"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg, flush=True)
    return log_msg


@dataclass(frozen=True)
class TrajPoint:
    """轨迹点数据类"""
    x: float
    y: float
    z: float
    roll_deg: float
    yaw_deg: float
    pitch_deg: float


# ============================================================================
# TrajectorySmoother: 三次样条插值平滑器
# ============================================================================

try:
    import numpy as np
    from scipy.interpolate import CubicSpline
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    np = None
    CubicSpline = None


class TrajectorySmoother:
    """
    轨迹平滑器：使用三次样条插值对轨迹进行平滑处理

    特性：
    1. 对位置(x,y,z)和姿态(roll,pitch,yaw)进行三次样条插值
    2. 自动处理角度的unwrap（防止±180°跳变）
    3. 提供位置和速度的导数计算（用于前馈控制）

    用法：
        smoother = TrajectorySmoother(points, dt=0.2)
        state = smoother.get_full_state(t)
        # state包含: x,y,z, vx,vy,vz, roll,pitch,yaw, yaw_rate
    """

    def __init__(self, points: List[TrajPoint], dt: float = 0.2):
        """
        初始化平滑器

        Args:
            points: 轨迹点列表
            dt: 原始数据时间间隔（默认0.2s = 5Hz）
        """
        if not HAS_SCIPY:
            raise RuntimeError("TrajectorySmoother requires numpy and scipy. "
                               "Install with: pip install numpy scipy")

        if len(points) < 2:
            raise ValueError("Need at least 2 points for interpolation")

        self.dt = dt
        self.n_points = len(points)
        self.duration = (self.n_points - 1) * dt

        # 提取各维度数据
        self.t_orig = np.array([i * dt for i in range(self.n_points)])

        x_arr = np.array([p.x for p in points])
        y_arr = np.array([p.y for p in points])
        z_arr = np.array([p.z for p in points])

        # 角度转弧度并unwrap
        roll_arr = np.unwrap(np.deg2rad([p.roll_deg for p in points]))
        pitch_arr = np.unwrap(np.deg2rad([p.pitch_deg for p in points]))
        yaw_arr = np.unwrap(np.deg2rad([p.yaw_deg for p in points]))

        # 创建三次样条插值器
        # bc_type='natural' 表示端点处二阶导数为0
        self._spline_x = CubicSpline(self.t_orig, x_arr, bc_type='natural')
        self._spline_y = CubicSpline(self.t_orig, y_arr, bc_type='natural')
        self._spline_z = CubicSpline(self.t_orig, z_arr, bc_type='natural')
        self._spline_roll = CubicSpline(self.t_orig, roll_arr, bc_type='natural')
        self._spline_pitch = CubicSpline(self.t_orig, pitch_arr, bc_type='natural')
        self._spline_yaw = CubicSpline(self.t_orig, yaw_arr, bc_type='natural')

    def get_full_state(self, t: float, for_control: bool = True) -> Dict[str, float]:
        """
        获取时刻t的完整状态（位置、速度、加速度、姿态、角速度）

        Args:
            t: 时间（秒），范围[0, duration]
            for_control: 是否用于控制（True=用于发送setpoint，需要平滑速度和加速度；
                        False=用于数据记录，只有最终航点速度/加速度为0）

        Returns:
            字典包含:
            - x, y, z: 位置 (m)
            - vx, vy, vz: 速度 (m/s)
            - ax, ay, az: 加速度 (m/s²)
            - roll, pitch, yaw: 姿态 (rad)
            - roll_rate, pitch_rate, yaw_rate: 角速度 (rad/s)
        """
        # 限制时间在有效范围内
        t = max(0.0, min(t, self.duration))

        # 位置
        x = float(self._spline_x(t))
        y = float(self._spline_y(t))
        z = float(self._spline_z(t))

        # 姿态
        roll = float(self._spline_roll(t))
        pitch = float(self._spline_pitch(t))
        yaw = float(self._spline_yaw(t))

        if for_control:
            # 控制模式：使用平滑速度和加速度进行前馈控制
            vx = float(self._spline_x(t, 1))
            vy = float(self._spline_y(t, 1))
            vz = float(self._spline_z(t, 1))
            # 加速度 = 二阶导数
            ax = float(self._spline_x(t, 2))
            ay = float(self._spline_y(t, 2))
            az = float(self._spline_z(t, 2))
            roll_rate = float(self._spline_roll(t, 1))
            pitch_rate = float(self._spline_pitch(t, 1))
            yaw_rate = float(self._spline_yaw(t, 1))
        else:
            # 数据记录模式：只有最终航点速度/加速度为0，中间航点使用实际插值
            final_waypoint_threshold = 0.01  # 10ms阈值
            is_at_final_waypoint = (self.duration - t) < final_waypoint_threshold

            if is_at_final_waypoint:
                # 在最终航点，速度和加速度为0（悬停目标）
                vx, vy, vz = 0.0, 0.0, 0.0
                ax, ay, az = 0.0, 0.0, 0.0
                roll_rate, pitch_rate, yaw_rate = 0.0, 0.0, 0.0
            else:
                # 中间航点：使用实际插值速度和加速度
                vx = float(self._spline_x(t, 1))
                vy = float(self._spline_y(t, 1))
                vz = float(self._spline_z(t, 1))
                ax = float(self._spline_x(t, 2))
                ay = float(self._spline_y(t, 2))
                az = float(self._spline_z(t, 2))
                roll_rate = float(self._spline_roll(t, 1))
                pitch_rate = float(self._spline_pitch(t, 1))
                yaw_rate = float(self._spline_yaw(t, 1))

        return {
            'x': x, 'y': y, 'z': z,
            'vx': vx, 'vy': vy, 'vz': vz,
            'ax': ax, 'ay': ay, 'az': az,
            'roll': roll, 'pitch': pitch, 'yaw': yaw,
            'roll_rate': roll_rate, 'pitch_rate': pitch_rate, 'yaw_rate': yaw_rate,
        }

    def get_position(self, t: float) -> Tuple[float, float, float]:
        """获取时刻t的位置"""
        t = max(0.0, min(t, self.duration))
        return (
            float(self._spline_x(t)),
            float(self._spline_y(t)),
            float(self._spline_z(t))
        )

    def get_velocity(self, t: float) -> Tuple[float, float, float]:
        """获取时刻t的速度"""
        t = max(0.0, min(t, self.duration))
        return (
            float(self._spline_x(t, 1)),
            float(self._spline_y(t, 1)),
            float(self._spline_z(t, 1))
        )

    def get_yaw_and_rate(self, t: float) -> Tuple[float, float]:
        """获取时刻t的航向角和角速度"""
        t = max(0.0, min(t, self.duration))
        return (
            float(self._spline_yaw(t)),
            float(self._spline_yaw(t, 1))
        )


# ============================================================================


_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _log_exc(context: str, e: BaseException) -> None:
    ts_log("[Exception]", f"{context} err={e}", "WARN")


def _is_local_url(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        host = ""
    if host in ("localhost", "127.0.0.1", "::1"):
        return True
    return host.startswith("127.")


def _urlopen(req: urllib.request.Request, timeout: float):
    url = getattr(req, "full_url", "") or ""
    if isinstance(url, str) and _is_local_url(url):
        return _NO_PROXY_OPENER.open(req, timeout=timeout)
    return urllib.request.urlopen(req, timeout=timeout)


def _http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Tuple[int, Dict[str, Any]]:
    """发送HTTP JSON请求"""
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with _urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return resp.getcode(), {}
            try:
                return resp.getcode(), json.loads(raw.decode("utf-8"))
            except Exception:
                return resp.getcode(), {"raw": raw.decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = b""
        try:
            obj = json.loads(raw.decode("utf-8")) if raw else {"error": str(e)}
        except Exception:
            obj = {"error": raw.decode("utf-8", errors="replace")}
        return int(getattr(e, "code", 500) or 500), obj


def _iter_json_files(input_dir: Path, pattern: str) -> List[Path]:
    """扫描JSON轨迹文件"""
    files = sorted([Path(p) for p in glob.glob(str(input_dir / pattern))])
    return [p for p in files if p.is_file() and p.suffix.lower() == ".json"]


def _load_preprocessed_xyz(json_path: Path) -> List[TrajPoint]:
    """加载预处理的轨迹点

    注意：preprocessed_logs 是相对坐标系（从原点开始），需要加上 raw_logs[0] 的初始高度
    """
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs")
    if not isinstance(logs, list):
        raise ValueError("missing preprocessed_logs")

    # 获取初始高度（从raw_logs）
    raw_logs = obj.get("raw_logs", [])
    if raw_logs and len(raw_logs[0]) >= 3:
        init_z = float(raw_logs[0][2])  # 初始绝对高度
    else:
        init_z = 0.0

    # 每2个点取一个(0.2s/pt)
    logs = logs[::2]
    pts: List[TrajPoint] = []
    for row_idx, row in enumerate(logs):
        if not isinstance(row, (list, tuple)):
            continue
        if len(row) < 6:
            raise ValueError(f"preprocessed_logs[{row_idx}] expects [x,y,z,roll,yaw,pitch], got len={len(row)}")
        x = float(row[0])
        y = float(row[1])
        # Z = 初始高度 + 相对变化量
        z = init_z + float(row[2])
        roll = float(row[3])
        yaw = float(row[4])
        pitch = float(row[5])
        pts.append(TrajPoint(x, y, z, roll, yaw, pitch))  # Keep as ENU (x=East, y=North, z=Up)
    if not pts:
        raise ValueError("preprocessed_logs has no valid xyz rows")
    return pts


def _load_init_point_xyz(json_path: Path) -> TrajPoint:
    """加载初始位置点"""
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    raw_logs = obj.get("raw_logs", [])
    if not isinstance(raw_logs, list) or not raw_logs:
        raise ValueError("missing init point")
    init = raw_logs[0]
    if not isinstance(init, (list, tuple)) or len(init) < 3:
        raise ValueError("missing init point")
    if len(init) < 6:
        raise ValueError(f"init point expects [x,y,z,roll,yaw,pitch], got len={len(init)}")
    x = float(init[0])
    y = float(init[1])
    z = float(init[2])
    roll = float(init[3])
    yaw = float(init[4])
    pitch = float(init[5])
    return TrajPoint(x, y, z, roll, yaw, pitch)  # Keep as ENU (x=East, y=North, z=Up)


def _load_uav_ids_from_config(config_path: Path) -> List[int]:
    """从配置文件加载UAV ID列表"""
    obj = json.loads(config_path.read_text(encoding="utf-8"))
    vehicles = obj.get("vehicles") or []
    out: List[int] = []
    for v in vehicles:
        try:
            out.append(int(v.get("vehicle_id")))
        except Exception as e:
            _log_exc(f"_load_uav_ids_from_config invalid vehicle_id", e)
            continue
    out = sorted(set(out))
    if not out:
        raise ValueError(f"no vehicle_id found in {config_path}")
    return out


def _decode_png_b64_to_file(b64: str, out_path: Path) -> None:
    """Base64 PNG解码保存到文件"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(b64.encode("ascii"))
    out_path.write_bytes(data)


def _safe_name(path: Path) -> str:
    """生成安全的文件名"""
    s = path.stem
    s2 = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", "."):
            s2.append(ch)
        else:
            s2.append("_")
    return "".join(s2) or "traj"


def _transform_points(
    pts: List[TrajPoint],
    scale: float,
    base_x: float,
    base_y: float,
    base_z: float,
    z_down: bool,
) -> List[TrajPoint]:
    """坐标变换

    轨迹数据是绝对坐标，只需应用scale:
    - x, y, z 直接乘以scale
    - 如果z_down=True，翻转z符号（用于ENU转NED）

    注意：base_x/y/z参数保留但不再使用（轨迹本身就是绝对坐标）
    """
    out: List[TrajPoint] = []
    for p in pts:
        x = p.x * scale
        y = p.y * scale
        if z_down:
            z = -p.z * scale  # ENU to NED: flip z sign
        else:
            z = p.z * scale   # Keep ENU z
        out.append(TrajPoint(x, y, z, p.roll_deg, p.yaw_deg, p.pitch_deg))
    return out


# 最小航点距离（作为fallback）
MIN_WAYPOINT_DIST = float(os.environ.get("PEGASUS_MIN_WAYPOINT_DIST", "0.35"))
# 目标采样间距（米），用于动态等间距采样
TARGET_SAMPLE_DIST = float(os.environ.get("PEGASUS_TARGET_SAMPLE_DIST", "0.8"))
# 最大保留航点数
MAX_WAYPOINTS = int(os.environ.get("PEGASUS_MAX_WAYPOINTS", "30"))


def _filter_close_points(pts: List[TrajPoint], min_dist: float = MIN_WAYPOINT_DIST) -> List[TrajPoint]:
    """
    动态等间距采样航点

    策略：
    1. 计算轨迹总长度
    2. 根据TARGET_SAMPLE_DIST计算期望的采样点数
    3. 沿轨迹累计距离进行等间距采样
    4. 始终保留起点和终点
    """
    if len(pts) <= 2:
        return pts

    # 计算每段距离和累计距离
    seg_dists = []
    for i in range(1, len(pts)):
        dx = pts[i].x - pts[i-1].x
        dy = pts[i].y - pts[i-1].y
        dz = pts[i].z - pts[i-1].z
        seg_dists.append(math.sqrt(dx*dx + dy*dy + dz*dz))

    total_dist = sum(seg_dists)

    if total_dist < 0.1:  # 轨迹太短，只保留首尾
        return [pts[0], pts[-1]]

    # 计算期望采样点数（基于目标采样间距）
    target_count = max(2, min(int(total_dist / TARGET_SAMPLE_DIST) + 1, MAX_WAYPOINTS))

    if target_count >= len(pts):
        # 如果期望点数大于等于原点数，使用原来的最小距离过滤
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
        if len(pts) > 1:
            filtered.append(pts[-1])
        return filtered

    # 等间距采样
    sample_interval = total_dist / (target_count - 1)

    filtered: List[TrajPoint] = [pts[0]]
    cumulative = 0.0
    next_sample_dist = sample_interval

    for i in range(len(seg_dists)):
        cumulative += seg_dists[i]
        # 当累计距离超过下一个采样点时，添加该点
        while cumulative >= next_sample_dist and len(filtered) < target_count - 1:
            filtered.append(pts[i + 1])
            next_sample_dist += sample_interval

    # 确保终点被添加
    if filtered[-1] != pts[-1]:
        filtered.append(pts[-1])

    return filtered


def _fetch_all_info(image_base: str, uav_id: int, timeout: float, retries: int) -> Dict[str, Any]:
    """获取图像和位姿信息"""
    url = f"{image_base}/uav/{uav_id}/all"
    last: Optional[Dict[str, Any]] = None
    for _ in range(max(1, int(retries))):
        code, obj = _http_json("GET", url, payload=None, timeout=timeout)
        if 200 <= code < 300 and isinstance(obj, dict) and obj.get("image") and obj.get("pose"):
            return obj
        last = obj if isinstance(obj, dict) else {"error": "invalid response"}
        time.sleep(0.2)
    raise RuntimeError(f"fetch /all failed uav={uav_id} resp={last}")


def _fetch_depth(image_base: str, uav_id: int, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """获取深度图像信息

    Args:
        image_base: 仿真器HTTP基础URL
        uav_id: UAV ID
        timeout: HTTP超时时间

    Returns:
        Dict: 包含深度图像数据的字典，包括 data (16位PNG base64), width, height, raw_depth_b64 (float32原始数据)
        None: 如果获取失败
    """
    url = f"{image_base}/uav/{uav_id}/depth"
    try:
        code, obj = _http_json("GET", url, payload=None, timeout=timeout)
        if 200 <= code < 300 and isinstance(obj, dict) and obj.get("data"):
            return obj
    except Exception:
        pass
    return None


def _wait_px4_ready(image_base: str, uav_id: int, timeout_s: float = 120.0) -> bool:
    """等待PX4就绪"""
    url = f"{image_base}/uav/{uav_id}/px4/ready"
    deadline = time.time() + float(timeout_s)
    last_log_time = 0.0
    while time.time() < deadline:
        code, obj = _http_json("GET", url, payload=None, timeout=5.0)
        if 200 <= code < 300 and isinstance(obj, dict) and obj.get("ready") is True:
            return True
        now = time.time()
        if now - last_log_time > 5.0:
            ts_log(f"[UAV{uav_id}]", f"Waiting for PX4 ready... (status={obj})")
            last_log_time = now
        time.sleep(0.5)
    return False


def _sim_move_uav(image_base: str, uav_id: int, position: List[float], yaw_deg: float = 0.0, timeout: float = 10.0) -> bool:
    """通过仿真端HTTP接口移动UAV位置（在PX4恢复之前调用）"""
    url = f"{image_base}/uav/{uav_id}/reset"
    payload = {
        "position": [float(position[0]), float(position[1]), float(position[2])],
        "yaw_deg": float(yaw_deg)
    }
    code, obj = _http_json("POST", url, payload=payload, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") == "success":
        return True
    ts_log(f"[UAV{uav_id}]", f"sim_move_uav failed: code={code} resp={obj}", "WARN")
    return False


def _sim_px4_recover(image_base: str, uav_id: int, timeout: float = 120.0) -> bool:
    """通过仿真端HTTP接口恢复PX4（kill并重启PX4进程）"""
    url = f"{image_base}/uav/{uav_id}/px4/recover"
    code, obj = _http_json("POST", url, payload={}, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") == "success":
        return True
    ts_log(f"[UAV{uav_id}]", f"sim_px4_recover failed: code={code} resp={obj}", "WARN")
    return False


# ============================================================================
# 异步图像缓冲区控制函数
# ============================================================================

def _buffer_start(image_base: str, uav_id: int, save_dir: Optional[str] = None, timeout: float = 5.0) -> bool:
    """开始图像缓冲区录制

    在仿真端开始缓存图像和位姿数据，采集器可以专注于发送控制命令，
    最后一次性获取所有数据。

    Args:
        image_base: 仿真器HTTP基础URL (如 http://127.0.0.1:8081)
        uav_id: UAV ID
        save_dir: 可选的磁盘保存目录。如果指定，仿真端将图像保存到磁盘而非内存。
        timeout: HTTP超时时间

    Returns:
        bool: 是否成功启动录制
    """
    url = f"{image_base}/uav/{uav_id}/buffer/start"
    payload = {}
    if save_dir is not None:
        payload["save_dir"] = save_dir
    code, obj = _http_json("POST", url, payload=payload, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") == "success":
        save_info = f" (save_dir={save_dir})" if save_dir else ""
        ts_log(f"[UAV{uav_id}]", f"Buffer recording started{save_info}")
        return True
    ts_log(f"[UAV{uav_id}]", f"buffer_start failed: code={code} resp={obj}", "WARN")
    return False


def _buffer_stop(image_base: str, uav_id: int, timeout: float = 60.0) -> Optional[List[Dict]]:
    """停止图像缓冲区录制并获取所有帧

    Args:
        image_base: 仿真器HTTP基础URL
        uav_id: UAV ID
        timeout: HTTP超时时间（需要足够长以传输所有帧数据）

    Returns:
        List[Dict]: 帧列表，每帧包含 timestamp, pose, image_b64, width, height
        None: 如果失败
    """
    url = f"{image_base}/uav/{uav_id}/buffer/stop"
    code, obj = _http_json("POST", url, payload={}, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") == "success":
        frames = obj.get("frames", [])
        ts_log(f"[UAV{uav_id}]", f"Buffer recording stopped, got {len(frames)} frames")
        return frames
    ts_log(f"[UAV{uav_id}]", f"buffer_stop failed: code={code} resp={obj}", "WARN")
    return None


def _buffer_status(image_base: str, uav_id: int, timeout: float = 5.0) -> Optional[Dict]:
    """查询缓冲区状态

    Returns:
        Dict: 包含 recording, frame_count, duration_s 等信息
        None: 如果失败
    """
    url = f"{image_base}/uav/{uav_id}/buffer/status"
    code, obj = _http_json("GET", url, payload=None, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") == "success":
        return obj
    return None


def _fetch_pose_only(image_base: str, uav_id: int, timeout: float = 1.0) -> Optional[Dict]:
    """仅获取位姿信息（不获取图像，用于轻量级状态查询）

    Args:
        image_base: 仿真器HTTP基础URL
        uav_id: UAV ID
        timeout: HTTP超时时间

    Returns:
        Dict: 位姿信息，包含 position, attitude, linear_velocity 等
        None: 如果失败
    """
    url = f"{image_base}/uav/{uav_id}/pose"
    code, obj = _http_json("GET", url, payload=None, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict):
        return obj
    return None


def _fetch_sim_time(image_base: str, timeout: float = 1.0) -> Optional[float]:
    """获取仿真器当前模拟器时间（秒）

    Args:
        image_base: 仿真器HTTP基础URL
        timeout: HTTP超时时间

    Returns:
        float: 模拟器时间（秒）
        None: 如果失败
    """
    url = f"{image_base}/sim_time"
    code, obj = _http_json("GET", url, payload=None, timeout=timeout)
    if 200 <= code < 300 and isinstance(obj, dict):
        return obj.get("sim_time")
    return None


def _ensure_control_healthy(control_base: str, uav_ids: List[int], timeout_s: float = 60.0) -> None:
    """确保所有控制器健康"""
    deadline = time.time() + float(timeout_s)
    parsed = urlparse(control_base)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "127.0.0.1"
    if parsed.port in (None, 5008):
        base_port = 5009
    else:
        base_port = int(parsed.port)
    remaining = set(int(v) for v in uav_ids)
    last_err: Dict[int, Any] = {}
    while time.time() < deadline and remaining:
        for vid in list(remaining):
            port = base_port + int(vid)
            url = f"{scheme}://{host}:{port}/health"
            code, obj = _http_json("GET", url, payload=None, timeout=5.0)
            if 200 <= code < 300 and isinstance(obj, dict) and obj.get("status") in ("healthy", "ok", "success"):
                remaining.discard(vid)
            else:
                last_err[vid] = obj if isinstance(obj, dict) else {"error": "invalid response"}
        if remaining:
            time.sleep(0.5)
    if remaining:
        raise TimeoutError(f"controllers not healthy for uavs={sorted(remaining)}; last_err={last_err}")


def _build_controller_base(control_base: str, uav_id: int) -> str:
    """构建控制器URL"""
    parsed = urlparse(control_base)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "127.0.0.1"
    if parsed.port in (None, 5008):
        base_port = 5009
    else:
        base_port = int(parsed.port)
    port = base_port + int(uav_id)
    return f"{scheme}://{host}:{port}"


def _controller_reset(
    control_base: str,
    uav_id: int,
    hard: bool,
    force: bool,
    position: Optional[List[float]],
    yaw_deg: Optional[float],
    timeout: float,
) -> None:
    """重置控制器"""
    url = f"{control_base}/reset"
    payload: Dict[str, Any] = {"vid": int(uav_id), "hard": bool(hard), "force": bool(force)}
    if position is not None:
        payload["position"] = [float(position[0]), float(position[1]), float(position[2])]
    if yaw_deg is not None:
        payload["yaw_deg"] = float(yaw_deg)
    code, obj = _http_json("POST", url, payload=payload, timeout=timeout)
    if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("status") != "success":
        raise RuntimeError(f"reset failed uav={uav_id} code={code} resp={obj}")


def _controller_command(
    control_base: str,
    uav_id: int,
    cmd: Dict[str, Any],
    timeout: float,
) -> Dict[str, Any]:
    """发送控制器命令"""
    url = f"{control_base}/command"
    code, obj = _http_json("POST", url, payload=cmd, timeout=timeout)
    if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("ok") is False:
        raise RuntimeError(f"command failed uav={uav_id} cmd={cmd} code={code} resp={obj}")
    return obj


# ============================================================================
# ULG 文件工具函数
# ============================================================================

def _is_good_ulg(path: Path) -> bool:
    """检查ULG文件是否有效"""
    try:
        if path is None or not path.exists():
            return False
        st = path.stat()
        if st.st_size < 1024:  # 小于1KB的文件可能是空的或损坏的
            return False

        # 尝试使用 ulog_info 验证（如果可用）
        exe = shutil.which("ulog_info")
        if not exe:
            return True  # 没有工具，假设有效

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
                # 检查是否有有效时长
                return True
        return True
    except Exception:
        return True


def _find_latest_ulg(vehicle_id: int, since_ts: float) -> Optional[Path]:
    """
    查找指定UAV的最新ULG文件

    Args:
        vehicle_id: UAV ID
        since_ts: 只查找此时间戳之后修改的文件

    Returns:
        ULG文件路径，如果未找到则返回None
    """
    candidates: List[Tuple[float, Path]] = []

    # Isaac Sim 中 PX4 的日志目录
    search_patterns = [
        f"/tmp/px4_{vehicle_id}_*/log/*.ulg",           # PX4 临时日志
        f"/tmp/px4_{vehicle_id}_*/**/*.ulg",            # 递归搜索
        f"{os.getcwd()}/px4_logs/uav{vehicle_id}/**/*.ulg",  # 永久保存目录
    ]

    for pattern in search_patterns:
        try:
            for fp_str in glob.glob(pattern, recursive=True):
                fp = Path(fp_str)
                try:
                    st = fp.stat()
                    if st.st_mtime >= since_ts:
                        candidates.append((st.st_mtime, fp))
                except Exception:
                    continue
        except Exception:
            continue

    # 也搜索通用的 /tmp 目录
    try:
        for p_str in glob.glob("/tmp/px4_*"):
            p = Path(p_str)
            if not p.is_dir():
                continue
            try:
                for fp in p.rglob("*.ulg"):
                    try:
                        st = fp.stat()
                        if st.st_mtime >= since_ts:
                            candidates.append((st.st_mtime, fp))
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception:
        pass

    if not candidates:
        return None

    # 按修改时间降序排列，返回最新的有效文件
    candidates.sort(key=lambda x: x[0], reverse=True)
    for _, fp in candidates:
        if _is_good_ulg(fp):
            return fp

    return candidates[0][1] if candidates else None


def _write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    """写入CSV文件

    浮点数保留8位小数，避免存储过长的精度噪音。
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("", encoding="utf-8")
        return

    # 收集所有可能的字段名
    keys = list(rows[0].keys())
    for r in rows[1:]:
        for k in r.keys():
            if k not in keys:
                keys.append(k)

    # 格式化浮点数为8位小数
    def format_value(v):
        if isinstance(v, float):
            return f"{v:.8f}"
        return v

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            formatted_row = {k: format_value(v) for k, v in r.items()}
            w.writerow(formatted_row)


def _norm_abs_path(p: Path) -> str:
    """标准化绝对路径"""
    try:
        return str(p.resolve())
    except Exception:
        return str(p.absolute())


def _relative_path(p: Path, base: Path) -> str:
    """计算相对于base目录的相对路径

    Args:
        p: 要转换的路径
        base: 基准目录（通常是out_dir）

    Returns:
        相对路径字符串
    """
    try:
        return str(p.resolve().relative_to(base.resolve()))
    except (ValueError, RuntimeError):
        # 如果无法计算相对路径，返回绝对路径
        return _norm_abs_path(p)


class Worker:
    """轨迹采集工作线程"""

    def __init__(
        self,
        uav_id: int,
        control_base: str,
        image_base: str,
        out_dir: Path,
        task_queue: "queue.Queue[Path]",
        scale: float,
        z_down: bool,
        max_points: int,
        reset_timeout: float,
        cmd_timeout: float,
        image_timeout: float,
        image_retries: int,
        skip_existing: bool,
        use_mission: bool,
        time_scale: float,
        collect_images: bool,
        print_lock: threading.Lock,
        status_log_path: Path,
    ):
        self.uav_id = int(uav_id)
        self.control_base = _build_controller_base(control_base, self.uav_id).rstrip("/")
        self.image_base = image_base.rstrip("/")
        self.out_dir = out_dir
        self.task_queue = task_queue
        self.scale = float(scale)
        self.z_down = bool(z_down)
        self.max_points = int(max_points)
        self.reset_timeout = float(reset_timeout)
        self.cmd_timeout = float(cmd_timeout)
        self.image_timeout = float(image_timeout)
        self.image_retries = int(image_retries)
        self.skip_existing = bool(skip_existing)
        self.use_mission = bool(use_mission)
        self.time_scale = float(time_scale)
        self.collect_images = bool(collect_images)
        self.print_lock = print_lock
        self.status_log_path = status_log_path

        # 坐标对齐
        self._origin_offset: Optional[Tuple[float, float, float]] = None

    def _log(self, msg: str, level: str = "INFO") -> None:
        with self.print_lock:
            ts_log(f"[Worker UAV{self.uav_id}]", msg, level)

    def _calculate_origin_offset(self, cmd_pos: Tuple[float, float, float], obs_pos: Tuple[float, float, float]) -> Tuple[float, float, float]:
        """计算坐标偏移量"""
        return (
            obs_pos[0] - cmd_pos[0],
            obs_pos[1] - cmd_pos[1],
            obs_pos[2] - cmd_pos[2],
        )

    def _apply_alignment(self, obs_pos: Tuple[float, float, float]) -> Tuple[float, float, float]:
        """应用坐标对齐"""
        if self._origin_offset is None:
            return obs_pos
        return (
            obs_pos[0] - self._origin_offset[0],
            obs_pos[1] - self._origin_offset[1],
            obs_pos[2] - self._origin_offset[2],
        )

    def _reset_and_wait_ready(
        self,
        position: Optional[List[float]],
        yaw_deg: Optional[float],
        timeout: float,
    ) -> None:
        """
        硬重置流程（参考mavros版本的reboot_px4_hard）：
        1. 先移动UAV到地面位置（在PX4恢复之前）
        2. 通过仿真端恢复PX4（kill并重启PX4进程）
        3. 等待PX4后端就绪
        4. 等待PX4 ready_to_takeoff
        5. ARM并进入OFFBOARD模式
        """
        log_prefix = f"[UAV{self.uav_id}]"

        # Step 1: 先移动UAV到地面位置（在PX4恢复之前）
        if position is not None and len(position) >= 3:
            ground_pos = [float(position[0]), float(position[1]), 0.07]  # 地面高度
            reset_yaw = float(yaw_deg) if yaw_deg is not None else 0.0
            self._log(f"Step 1: Moving UAV to ground position {ground_pos} yaw={reset_yaw}")
            ok = _sim_move_uav(self.image_base, self.uav_id, ground_pos, reset_yaw, timeout=10.0)
            if not ok:
                self._log(f"Step 1 WARNING: sim_move_uav failed", "WARN")
            time.sleep(0.5)  # 等待物理稳定
        else:
            self._log(f"Step 1: Skipped (no position specified)")

        # Step 2: 通过仿真端恢复PX4（原子操作：kill + restart）
        self._log(f"Step 2: Recovering PX4 via simulation endpoint...")
        ok_recover = _sim_px4_recover(self.image_base, self.uav_id, timeout=120.0)
        if not ok_recover:
            self._log(f"Step 2 WARNING: PX4 recovery failed", "WARN")

        # Step 3: 等待PX4后端就绪（确保仿真端和PX4之间的通信正常）
        self._log(f"Step 3: Waiting for PX4 backend to be ready...")
        px4_backend_timeout = 30.0
        px4_backend_start = time.time()
        px4_backend_ok = False
        while time.time() - px4_backend_start < px4_backend_timeout:
            try:
                url = f"{self.image_base}/uav/{self.uav_id}/px4/status"
                code, obj = _http_json("GET", url, payload=None, timeout=5.0)
                if 200 <= code < 300 and isinstance(obj, dict):
                    backend_info = obj.get("px4_backend", {})
                    is_running = backend_info.get("is_running", False)
                    mavlink_connected = backend_info.get("mavlink_connected", False)
                    received_first_actuator = backend_info.get("received_first_actuator", False)
                    if is_running and mavlink_connected and received_first_actuator:
                        px4_backend_ok = True
                        self._log(f"Step 3 OK: PX4 backend is ready (mavlink={mavlink_connected}, actuator={received_first_actuator})")
                        break
            except Exception as e:
                pass
            time.sleep(0.5)

        if not px4_backend_ok:
            self._log(f"Step 3 WARNING: PX4 backend not ready after timeout, continuing...", "WARN")

        # Step 4: 等待PX4 ready_to_takeoff
        self._log(f"Step 4: Waiting for PX4 ready_to_takeoff...")
        px4_ready_timeout = 30.0
        px4_ready = _wait_px4_ready(self.image_base, self.uav_id, timeout_s=px4_ready_timeout)
        if px4_ready:
            self._log(f"Step 4 OK: PX4 ready_to_takeoff")
        else:
            self._log(f"Step 4 WARNING: PX4 ready_to_takeoff timeout, continuing...", "WARN")

        time.sleep(1.0)  # 额外稳定时间

        # Step 5: ARM并进入OFFBOARD模式
        if position is not None and len(position) >= 3:
            hold_pos = [float(position[0]), float(position[1]), float(position[2])]
            self._log(f"Step 5: Arming and setting OFFBOARD mode (target_z={hold_pos[2]:.2f}m)")

            # 先发送setpoints（PX4要求在ARM前有setpoint流）
            for _ in range(50):  # 1秒的setpoints
                cmd = {
                    "cmd": "setpoint",
                    "x": hold_pos[0], "y": hold_pos[1], "z": hold_pos[2],
                    "vx": 0.0, "vy": 0.0, "vz": 0.0,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": 0.0, "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass
                time.sleep(0.02)

            # ARM
            arm_success = False
            for attempt in range(5):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "arm"}, timeout=5.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        arm_success = True
                        self._log(f"Step 5: Armed successfully")
                        break
                except Exception as e:
                    pass
                # 继续发送setpoints
                for _ in range(15):
                    try:
                        _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                    except Exception:
                        pass
                    time.sleep(0.02)

            if not arm_success:
                self._log(f"Step 5 WARNING: Failed to arm after 5 attempts", "WARN")
                return

            # 设置OFFBOARD模式
            offboard_success = False
            for attempt in range(3):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "set_mode", "mode": "OFFBOARD"}, timeout=5.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        offboard_success = True
                        self._log(f"Step 5: OFFBOARD mode set successfully")
                        break
                except Exception:
                    pass
                # 继续发送setpoints
                for _ in range(15):
                    try:
                        _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                    except Exception:
                        pass
                    time.sleep(0.02)

            if not offboard_success:
                self._log(f"Step 5 WARNING: Failed to set OFFBOARD mode", "WARN")

            # 继续发送setpoints保持位置
            for _ in range(25):
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass
                time.sleep(0.02)

            self._log(f"Step 5 complete: Reset sequence finished")
        else:
            self._log(f"Step 5: Skipped (no position specified)")

    def _append_status_log(self, traj_name: str, ulg_path: str = "") -> None:
        """追加状态日志"""
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

    def run(self) -> None:
        """运行工作线程"""
        while True:
            try:
                json_path = self.task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._process_one(json_path)
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] traj failed json={json_path} err={e}", "ERROR")
                self._log(traceback.format_exc(), "ERROR")
            finally:
                self.task_queue.task_done()

    def _process_one(self, json_path: Path) -> None:
        """处理单个轨迹"""
        traj_name = _safe_name(json_path)
        traj_dir = self.out_dir / traj_name / f"uav{self.uav_id}"
        csv_path = traj_dir / "data.csv"
        metadata_path = traj_dir / "metadata.json"
        if self.skip_existing and csv_path.exists() and metadata_path.exists():
            self._log(f"[UAV{self.uav_id}] skip existing traj={traj_name}")
            return

        raw_pts = _load_preprocessed_xyz(json_path)
        init_pos = _load_init_point_xyz(json_path)

        # 应用坐标变换（缩放）到所有轨迹点
        transformed_pts = _transform_points(raw_pts, self.scale, init_pos.x, init_pos.y, init_pos.z, self.z_down)

        # pts用于Mission模式（不过滤，保持完整轨迹）
        pts = transformed_pts

        # # 过滤近点（已禁用，使用完整5Hz轨迹）
        # pts_before = len(pts)
        # pts = _filter_close_points(pts, MIN_WAYPOINT_DIST)
        # if len(pts) < pts_before:
        #     self._log(f"[UAV{self.uav_id}] filtered {pts_before - len(pts)} close waypoints, {len(pts)} remaining")

        if int(self.max_points) > 0:
            transformed_pts = transformed_pts[: int(self.max_points)]
            pts = pts[: int(self.max_points)]
            if not pts:
                raise ValueError(f"max_points={self.max_points} results in empty trajectory")

        self._log(f"[UAV{self.uav_id}] reset before traj={traj_name}")

        # 执行重置 - 重置到轨迹起始位置对应的地面上
        # 注意：transformed_pts 是相对坐标（从原点开始），init_pos 才是绝对初始位置
        # 使用 init_pos 的 XY 坐标（经过 scale 缩放），Z 使用地面高度
        reset_x = init_pos.x * self.scale
        reset_y = init_pos.y * self.scale
        reset_z = 0.1  # 地面高度（略高于0以避免碰撞检测问题）

        self._log(f"[UAV{self.uav_id}] reset position: ({reset_x:.2f}, {reset_y:.2f}, {reset_z:.2f}) from init_pos=({init_pos.x:.1f}, {init_pos.y:.1f}) scale={self.scale}")
        self._reset_and_wait_ready(
            position=[reset_x, reset_y, reset_z],
            yaw_deg=None,
            timeout=self.reset_timeout,
        )

        traj_start_ts = time.time()
        self._log(f"[UAV{self.uav_id}] start traj={traj_name} points={len(pts)} use_mission={self.use_mission}")

        rows: List[Dict[str, Any]] = []
        self._origin_offset = None

        try:
            if self.use_mission:
                # Mission模式：一次性上传所有航点
                self._process_mission_mode(json_path, transformed_pts, pts, traj_dir, rows)
            else:
                # OFFBOARD模式：平滑轨迹跟踪控制
                self._process_offboard_mode(json_path, transformed_pts, pts, traj_dir, rows)

            # 轨迹完成后执行降落
            self._log(f"[UAV{self.uav_id}] trajectory complete, executing land command")
            try:
                _http_json("POST", f"{self.control_base}/command", payload={"cmd": "land"}, timeout=10.0)
                # 等待降落完成
                for _ in range(100):  # 最多10秒
                    try:
                        if self.collect_images:
                            info = _fetch_all_info(self.image_base, self.uav_id, timeout=2.0, retries=1)
                            alt = info.get("pose", {}).get("position", [0, 0, 0])[2]
                        else:
                            pose_info = _fetch_pose_only(self.image_base, self.uav_id, timeout=2.0) or {}
                            alt = (pose_info.get("position") or [0, 0, 0])[2]
                        if alt < 0.15:  # 接近地面
                            break
                    except Exception:
                        pass
                    time.sleep(0.1)
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] land command error: {e}", "WARN")

            time.sleep(1.0)  # 等待物理稳定

            # 查找并复制ULG文件
            ulg_path_str = ""
            ulg_src = _find_latest_ulg(self.uav_id, since_ts=traj_start_ts - 5.0)
            if ulg_src is not None and ulg_src.exists():
                ulg_filename = f"px4_uav{self.uav_id}_{int(time.time())}.ulg"
                ulg_dst = traj_dir / ulg_filename
                try:
                    ulg_dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(ulg_src), str(ulg_dst))
                    ulg_path_str = _relative_path(ulg_dst, self.out_dir)
                    self._log(f"[UAV{self.uav_id}] copied ULG: {ulg_path_str} (src: {ulg_src})")
                except Exception as e:
                    self._log(f"[UAV{self.uav_id}] failed to copy ULG: {e}", "WARN")
            else:
                self._log(f"[UAV{self.uav_id}] no ULG file found for this trajectory", "WARN")

            # 写入CSV（不再包含ulg_path列）
            _write_csv(rows, csv_path)

            # 计算轨迹持续时间
            duration_s = time.time() - traj_start_ts

            # 生成metadata.json
            metadata = {
                "traj_name": traj_name,
                "traj_json": _relative_path(json_path, self.out_dir),
                "uav_id": self.uav_id,
                "start_timestamp": traj_start_ts,
                "duration_s": round(duration_s, 3),
                "total_frames": len(rows),
                "time_scale": self.time_scale,
                "scale": self.scale,
                "control_mode": "mission" if self.use_mission else "offboard",
                "ulg_path": ulg_path_str,
            }
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with metadata_path.open("w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            self._log(f"[UAV{self.uav_id}] done traj={traj_name} csv={csv_path} metadata={metadata_path}")

            self._append_status_log(traj_name, ulg_path_str)

        except Exception as e:
            raise e

    def _process_offboard_mode(self, json_path: Path, transformed_pts: List[TrajPoint], pts: List[TrajPoint],
                                traj_dir: Path, rows: List[Dict]) -> None:
        """
        OFFBOARD模式：基于仿真时间的平滑跟踪控制（异步图像采集版）

        新架构（异步版）：
        [5Hz人工数据] -> [Cubic Spline插值] -> [50Hz位置+速度前馈控制]
                                              -> [仿真端20Hz图像缓冲] -> [轨迹结束后批量获取]

        关键改进：
        1. 控制循环不再阻塞等待HTTP图像传输
        2. 仿真端在后台以20Hz缓存图像+位姿
        3. 轨迹结束后一次性获取所有缓冲帧
        4. 真正实现50Hz控制频率 + 20Hz采样频率
        """
        if not HAS_SCIPY:
            raise RuntimeError("TrajectorySmoother requires scipy. Install with: pip install numpy scipy")

        if len(transformed_pts) < 2:
            self._log(f"[UAV{self.uav_id}] trajectory too short (<2 points)", "WARN")
            return

        # 原始数据时间间隔（5Hz = 0.2s）
        base_dt = 0.2
        # 应用时间缩放（time_scale > 1 会减速，< 1 会加速）
        scaled_dt = base_dt * self.time_scale

        self._log(f"[UAV{self.uav_id}] starting SMOOTH tracking control ({len(transformed_pts)} points, time_scale={self.time_scale:.2f}x, duration={(len(transformed_pts)-1)*scaled_dt:.1f}s)")

        # 创建平滑器（使用变换后的轨迹点，应用时间缩放）
        smoother = TrajectorySmoother(transformed_pts, dt=scaled_dt)

        total_duration = smoother.duration

        # 获取初始位姿和仿真时间戳
        try:
            if self.collect_images:
                info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=5)
                init_pose = info.get("pose", {})
            else:
                init_pose = _fetch_pose_only(self.image_base, self.uav_id, timeout=self.image_timeout) or {}
            uav_init_pos = init_pose.get("position", [0, 0, 0])
            sim_start_ts = float(init_pose.get("timestamp", time.time()))  # 获取仿真时间戳
        except Exception:
            uav_init_pos = [0, 0, 0]
            sim_start_ts = time.time()  # 回退到系统时间

        self._log(f"[UAV{self.uav_id}] initial sim timestamp: {sim_start_ts:.3f}")

        # 计算轨迹偏移
        # 重要：XY偏移在地面计算，Z偏移在起飞后计算
        traj_start = smoother.get_position(0.0)

        # XY偏移：将轨迹XY原点移到UAV当前XY位置
        # Z偏移：初始设为0，等起飞到正确高度后再计算
        # 因为在地面时计算Z偏移会导致 TARGET_ALT 被压低到地面高度！
        self._origin_offset = (
            uav_init_pos[0] - traj_start[0],
            uav_init_pos[1] - traj_start[1],
            0.0  # Z偏移初始为0，起飞后再更新
        )
        self._log(f"[UAV{self.uav_id}] initial trajectory offset: XY=({self._origin_offset[0]:.4f}, {self._origin_offset[1]:.4f})m, Z=0 (will update after takeoff)")
        self._log(f"[UAV{self.uav_id}] UAV pos=({uav_init_pos[0]:.2f}, {uav_init_pos[1]:.2f}, {uav_init_pos[2]:.2f}), traj_start=({traj_start[0]:.2f}, {traj_start[1]:.2f}, {traj_start[2]:.2f})")

        # 辅助函数：应用偏移到位置
        def apply_offset(x, y, z):
            return (
                x + self._origin_offset[0],
                y + self._origin_offset[1],
                z + self._origin_offset[2]
            )

        # === 起飞到目标高度 ===
        # TARGET_ALT直接使用轨迹起点高度（不应用Z偏移，因为Z偏移是0）
        TARGET_ALT = traj_start[2]
        ALT_TOLERANCE = 0.05

        # 起飞目标位置（XY使用offset对齐，Z直接用轨迹起点高度）
        takeoff_target = apply_offset(traj_start[0], traj_start[1], traj_start[2])

        # 检查当前高度
        try:
            if self.collect_images:
                info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=3)
                current_pos = info.get("pose", {}).get("position", [0, 0, 0])
            else:
                pose_info = _fetch_pose_only(self.image_base, self.uav_id, timeout=self.image_timeout) or {}
                current_pos = pose_info.get("position", [0, 0, 0])
            current_alt = current_pos[2]
            self._log(f"[UAV{self.uav_id}] current altitude: {current_alt:.2f}m, target: {TARGET_ALT:.2f}m")
        except Exception as e:
            self._log(f"[UAV{self.uav_id}] failed to check altitude: {e}", "WARN")
            current_alt = 0.0

        # 如果高度不足，执行起飞流程
        if current_alt < TARGET_ALT - ALT_TOLERANCE:
            self._log(f"[UAV{self.uav_id}] altitude too low ({current_alt:.2f}m < {TARGET_ALT:.2f}m), starting takeoff...")

            # 1. 先发送足够的setpoints准备OFFBOARD模式
            self._log(f"[UAV{self.uav_id}] preparing setpoints for OFFBOARD...")
            for _ in range(50):
                cmd = {
                    "cmd": "setpoint",
                    "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.5,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": 0.0, "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass
                time.sleep(0.02)

            # 2. ARM
            arm_success = False
            for attempt in range(5):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "arm"}, timeout=10.0)
                    self._log(f"[UAV{self.uav_id}] ARM response: code={code}, resp={resp}")
                    if 200 <= code < 300 and resp.get("ok"):
                        arm_success = True
                        self._log(f"[UAV{self.uav_id}] armed successfully")
                        break
                    else:
                        self._log(f"[UAV{self.uav_id}] ARM attempt {attempt+1} failed: code={code}", "WARN")
                except Exception as e:
                    self._log(f"[UAV{self.uav_id}] ARM attempt {attempt+1} exception: {e}", "WARN")
                time.sleep(0.3)
            if not arm_success:
                self._log(f"[UAV{self.uav_id}] WARNING: failed to arm", "WARN")

            # 3. 切换到OFFBOARD模式
            offboard_success = False
            for attempt in range(3):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "set_mode", "mode": "OFFBOARD"}, timeout=5.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        offboard_success = True
                        self._log(f"[UAV{self.uav_id}] OFFBOARD mode set")
                        break
                except Exception:
                    pass
                for _ in range(15):
                    try:
                        _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                    except Exception:
                        pass
                    time.sleep(0.02)
            if not offboard_success:
                self._log(f"[UAV{self.uav_id}] WARNING: failed to set OFFBOARD mode", "WARN")

            # 4. 爬升
            self._log(f"[UAV{self.uav_id}] climbing to {TARGET_ALT:.1f}m...")
            takeoff_start = time.time()
            takeoff_timeout = 45.0  # 多机仿真时EKF收敛需要更长时间
            reached_altitude = False

            while time.time() - takeoff_start < takeoff_timeout:
                cmd = {
                    "cmd": "setpoint",
                    "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.3,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": smoother.get_yaw_and_rate(0.0)[0],
                    "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass

                if int((time.time() - takeoff_start) / 0.5) > int((time.time() - takeoff_start - 0.02) / 0.5):
                    try:
                        if self.collect_images:
                            info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=2)
                            current_alt = info.get("pose", {}).get("position", [0, 0, 0])[2]
                        else:
                            pose_info = _fetch_pose_only(self.image_base, self.uav_id, timeout=self.image_timeout) or {}
                            current_alt = (pose_info.get("position") or [0, 0, 0])[2]
                        self._log(f"[UAV{self.uav_id}] climbing: {current_alt:.2f}m / {TARGET_ALT:.2f}m")
                        if current_alt >= TARGET_ALT - ALT_TOLERANCE:
                            reached_altitude = True
                            self._log(f"[UAV{self.uav_id}] reached target altitude!")
                            break
                    except Exception:
                        pass
                time.sleep(0.02)

            if not reached_altitude:
                self._log(f"[UAV{self.uav_id}] WARNING: did not reach target altitude in time", "WARN")

            # 5. 稳定
            self._log(f"[UAV{self.uav_id}] stabilizing at target altitude...")
            for _ in range(50):
                cmd = {
                    "cmd": "setpoint",
                    "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.0,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": smoother.get_yaw_and_rate(0.0)[0],
                    "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass
                time.sleep(0.02)
        else:
            # 高度已满足
            self._log(f"[UAV{self.uav_id}] altitude OK, preparing OFFBOARD mode...")
            for _ in range(30):
                cmd = {
                    "cmd": "setpoint",
                    "x": takeoff_target[0], "y": takeoff_target[1], "z": takeoff_target[2],
                    "vx": 0.0, "vy": 0.0, "vz": 0.0,
                    "afx": 0.0, "afy": 0.0, "afz": 0.0,
                    "yaw": smoother.get_yaw_and_rate(0.0)[0],
                    "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=1.0)
                except Exception:
                    pass
                time.sleep(0.02)

            offboard_success = False
            for attempt in range(3):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "set_mode", "mode": "OFFBOARD"}, timeout=10.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        offboard_success = True
                        self._log(f"[UAV{self.uav_id}] OFFBOARD mode set successfully")
                        break
                except Exception:
                    pass
                time.sleep(0.3)
            if not offboard_success:
                self._log(f"[UAV{self.uav_id}] WARNING: failed to set OFFBOARD mode", "WARN")

        # === 重新获取当前位置并重新计算轨迹偏移 ===
        # 关键：起飞后UAV位置可能漂移，需要重新对齐
        try:
            if self.collect_images:
                info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=3)
                current_pose = info.get("pose", {})
            else:
                current_pose = _fetch_pose_only(self.image_base, self.uav_id, timeout=self.image_timeout) or {}
            current_pos = current_pose.get("position", [0, 0, 0])
            sim_start_ts = float(current_pose.get("timestamp", time.time()))

            # 重新计算轨迹偏移（使用当前实际位置，包括Z轴）
            old_offset = self._origin_offset
            self._origin_offset = (
                current_pos[0] - traj_start[0],
                current_pos[1] - traj_start[1],
                current_pos[2] - traj_start[2]  # Z偏移也更新
            )
            self._log(f"[UAV{self.uav_id}] updated trajectory offset: XY=({self._origin_offset[0]:.4f}, {self._origin_offset[1]:.4f})m, Z={self._origin_offset[2]:.4f}m")
            self._log(f"[UAV{self.uav_id}] current_pos=({current_pos[0]:.2f}, {current_pos[1]:.2f}, {current_pos[2]:.2f}), traj_start=({traj_start[0]:.2f}, {traj_start[1]:.2f}, {traj_start[2]:.2f})")

            # 更新对齐后的起始位置
            aligned_start = apply_offset(traj_start[0], traj_start[1], traj_start[2])
        except Exception:
            sim_start_ts = time.time()
            # Fallback: 使用takeoff_target作为aligned_start
            aligned_start = takeoff_target
        self._log(f"[UAV{self.uav_id}] trajectory start sim timestamp: {sim_start_ts:.3f}")

        buffer_started = False
        if self.collect_images:
            buffer_save_dir = str(traj_dir / "buffer")
            buffer_started = _buffer_start(self.image_base, self.uav_id, save_dir=buffer_save_dir, timeout=5.0)
            if not buffer_started:
                self._log(f"[UAV{self.uav_id}] WARNING: Failed to start image buffer, falling back to sync mode", "WARN")

        # 控制循环50Hz（仿真端独立以20Hz缓存图像）
        CONTROL_HZ = 50.0
        control_dt = 1.0 / CONTROL_HZ  # 0.02s

        # 时间前瞻补偿 (Lookahead) - 用于补偿通信延迟和控制滞后
        # 基础前瞻时间根据 time_scale 缩放（变慢时前瞻时间变小）
        BASE_LOOKAHEAD = 0.1  # 100ms 基础前瞻
        LOOKAHEAD_TIME = BASE_LOOKAHEAD / self.time_scale
        self._log(f"[UAV{self.uav_id}] lookahead time: {LOOKAHEAD_TIME*1000:.0f}ms (base={BASE_LOOKAHEAD*1000:.0f}ms, scale={self.time_scale:.2f})")

        # 严格锁步模式：直接使用模拟器时间，不进行本地预测
        # 这确保控制循环与仿真时间严格同步
        last_sim_ts = sim_start_ts

        # 终点判定参数
        FINAL_DIST_THRESHOLD = 0.3  # 位置阈值（米）
        FINAL_VEL_THRESHOLD = 0.1   # 速度阈值（m/s）
        in_final_mode = False
        final_mode_start = None

        # 缓存当前观测状态（初始化为aligned_start）
        current_obs_pos = list(aligned_start)
        current_obs_vel = [0, 0, 0]

        # 误差统计
        error_sum = 0.0
        error_count = 0

        # HTTP请求节流（仅用于位姿观测，不获取图像）
        obs_fetch_interval = 5  # 每5次循环获取一次位姿（10Hz观测足够）
        loop_counter = 0

        self._log(f"[UAV{self.uav_id}] starting 50Hz control loop with ASYNC image buffering (duration={total_duration:.1f}s)")

        pose_samples: List[Dict[str, Any]] = []
        try:
            while True:
                loop_start = time.time()
                loop_counter += 1

                # === 获取位姿观测（轻量级，不获取图像） ===
                if loop_counter % obs_fetch_interval == 0:
                    try:
                        pose_info = _fetch_pose_only(self.image_base, self.uav_id, timeout=0.05)
                        if pose_info is not None:
                            new_sim_ts = float(pose_info.get("timestamp", 0))
                            new_pos = pose_info.get("position", None)
                            new_vel = pose_info.get("linear_velocity", None)

                            if new_sim_ts > 0:
                                last_sim_ts = new_sim_ts
                            if new_pos is not None and len(new_pos) >= 3:
                                current_obs_pos = new_pos
                            if new_vel is not None and len(new_vel) >= 3:
                                current_obs_vel = new_vel
                            if not self.collect_images:
                                pose_samples.append({
                                    "timestamp": float(new_sim_ts) if new_sim_ts > 0 else float(time.time()),
                                    "pose": {
                                        "position": (pose_info.get("position") or [0, 0, 0])[:3],
                                        "attitude": (pose_info.get("attitude") or [0, 0, 0, 1])[:4],
                                        "linear_velocity": (pose_info.get("linear_velocity") or [0, 0, 0])[:3],
                                        "angular_velocity": (pose_info.get("angular_velocity") or [0, 0, 0])[:3],
                                        "linear_acceleration": (pose_info.get("linear_acceleration") or [0, 0, 0])[:3],
                                    }
                                })
                    except Exception:
                        pass  # 保持上次有效的观测值

                # 严格锁步：直接使用从仿真端获取的模拟器时间
                # 不进行本地时间预测，确保与仿真时间严格同步
                current_sim_ts = last_sim_ts

                # 计算仿真经过时间
                elapsed = current_sim_ts - sim_start_ts

                # 获取轨迹终点状态
                end_state = smoother.get_full_state(total_duration)
                final_pos = apply_offset(end_state['x'], end_state['y'], end_state['z'])

                # === 终点判定逻辑 ===
                if elapsed > total_duration:
                    if not in_final_mode:
                        in_final_mode = True
                        final_mode_start = time.time()
                        self._log(f"[UAV{self.uav_id}] entering FINAL mode (trajectory complete)")

                    dist_to_final = math.sqrt(
                        (current_obs_pos[0] - final_pos[0])**2 +
                        (current_obs_pos[1] - final_pos[1])**2 +
                        (current_obs_pos[2] - final_pos[2])**2
                    )
                    current_speed = math.sqrt(
                        current_obs_vel[0]**2 + current_obs_vel[1]**2 + current_obs_vel[2]**2
                    )

                    # 发送终点保持setpoint
                    cmd = {
                        "cmd": "setpoint",
                        "x": final_pos[0], "y": final_pos[1], "z": final_pos[2],
                        "vx": 0.0, "vy": 0.0, "vz": 0.0,
                        "afx": 0.0, "afy": 0.0, "afz": 0.0,
                        "yaw": end_state['yaw'],
                        "yaw_rate": 0.0
                    }
                    try:
                        _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                    except Exception:
                        pass

                    if dist_to_final < FINAL_DIST_THRESHOLD and current_speed < FINAL_VEL_THRESHOLD:
                        self._log(f"[UAV{self.uav_id}] FINAL reached: dist={dist_to_final:.3f}m, vel={current_speed:.3f}m/s")
                        break

                    if time.time() - final_mode_start > 10.0:
                        self._log(f"[UAV{self.uav_id}] FINAL timeout: dist={dist_to_final:.3f}m, vel={current_speed:.3f}m/s", "WARN")
                        break

                    if int(time.time() - final_mode_start) > int(time.time() - final_mode_start - control_dt):
                        self._log(f"[UAV{self.uav_id}] FINAL mode: dist={dist_to_final:.3f}m, vel={current_speed:.3f}m/s")

                    time.sleep(control_dt)
                    continue

                # === 正常轨迹跟踪 (PVA + Lookahead) ===
                t_now = min(elapsed, total_duration)
                t_control = min(elapsed + LOOKAHEAD_TIME, total_duration)

                state_cmd = smoother.get_full_state(t_control)
                aligned_pos_cmd = apply_offset(state_cmd['x'], state_cmd['y'], state_cmd['z'])

                state_now = smoother.get_full_state(t_now)
                aligned_pos_now = apply_offset(state_now['x'], state_now['y'], state_now['z'])

                # 发送PVA控制setpoint
                cmd = {
                    "cmd": "setpoint",
                    "x": aligned_pos_cmd[0], "y": aligned_pos_cmd[1], "z": aligned_pos_cmd[2],
                    "vx": state_cmd['vx'], "vy": state_cmd['vy'], "vz": state_cmd['vz'],
                    "afx": state_cmd['ax'], "afy": state_cmd['ay'], "afz": state_cmd['az'],
                    "yaw": state_cmd['yaw'],
                    "yaw_rate": state_cmd['yaw_rate']
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass

                # 计算跟踪误差
                tracking_error = math.sqrt(
                    (current_obs_pos[0] - aligned_pos_now[0])**2 +
                    (current_obs_pos[1] - aligned_pos_now[1])**2 +
                    (current_obs_pos[2] - aligned_pos_now[2])**2
                )
                error_sum += tracking_error
                error_count += 1

                # 每秒输出一次误差日志
                if loop_counter % 50 == 0:
                    self._log(f"[UAV{self.uav_id}] t={t_now:.2f}s Error={tracking_error:.3f}m obs=({current_obs_pos[0]:.2f},{current_obs_pos[1]:.2f},{current_obs_pos[2]:.2f})")

                # 控制循环节拍
                loop_elapsed = time.time() - loop_start
                sleep_time = control_dt - loop_elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        finally:
            if self.collect_images:
                self._log(f"[UAV{self.uav_id}] stopping image buffer and processing frames...")
                buffered_frames = _buffer_stop(self.image_base, self.uav_id, timeout=120.0)
                if buffered_frames is not None and len(buffered_frames) > 0:
                    self._log(f"[UAV{self.uav_id}] processing {len(buffered_frames)} buffered frames...")
                    self._process_buffered_frames(
                        buffered_frames, smoother, json_path, traj_dir,
                        rows, apply_offset, total_duration
                    )
                else:
                    self._log(f"[UAV{self.uav_id}] WARNING: No buffered frames received", "WARN")
            else:
                if pose_samples:
                    self._process_pose_samples(
                        pose_samples, smoother, json_path, traj_dir, rows, apply_offset, total_duration
                    )
                else:
                    self._log(f"[UAV{self.uav_id}] WARNING: No pose samples collected", "WARN")

        # 输出最终统计
        avg_error = error_sum / max(error_count, 1)
        self._log(f"[UAV{self.uav_id}] tracking complete: {len(rows)} samples, avg_error={avg_error:.3f}m")

        # 检查最终位置
        try:
            pose_info = _fetch_pose_only(self.image_base, self.uav_id, timeout=2.0)
            if pose_info:
                final_obs_pos = pose_info.get("position", [0, 0, 0])
                end_state = smoother.get_full_state(total_duration)
                dist = math.sqrt(
                    (final_obs_pos[0] - end_state['x'])**2 +
                    (final_obs_pos[1] - end_state['y'])**2 +
                    (final_obs_pos[2] - end_state['z'])**2
                )
                self._log(f"[UAV{self.uav_id}] final position error: {dist:.3f}m")
        except Exception:
            pass

    def _process_buffered_frames(
        self,
        frames: List[Dict],
        smoother: TrajectorySmoother,
        json_path: Path,
        traj_dir: Path,
        rows: List[Dict],
        apply_offset,
        total_duration: float = None
    ) -> None:
        """
        处理仿真端缓冲的帧数据

        Args:
            frames: 缓冲帧列表，每帧包含 timestamp, pose, image_b64, width, height
            smoother: 轨迹平滑器（用于获取目标位置）
            json_path: 轨迹JSON路径
            traj_dir: 输出目录
            rows: 主数据行列表
            apply_offset: 坐标偏移函数
            total_duration: 轨迹总时长（秒），用于判断final_mode帧
        """
        if not frames:
            return

        # 获取第一帧的时间戳作为基准
        base_ts = frames[0].get("timestamp", 0)

        # 使用smoother的duration如果未提供total_duration
        if total_duration is None:
            total_duration = smoother.duration

        for i, frame in enumerate(frames):
            try:
                ts_img = float(frame.get("timestamp", 0))
                img_b64 = frame.get("image_b64", "")
                pose = frame.get("pose", {})

                # 计算相对时间（用于轨迹插值）
                relative_t = ts_img - base_ts if base_ts > 0 else i * 0.05

                # 判断是否为final_mode帧（轨迹已结束，正在收敛到终点）
                is_final_mode = relative_t > total_duration

                # 获取对应时间的目标状态
                sample_state = smoother.get_full_state(relative_t, for_control=False)
                sample_pos = apply_offset(sample_state['x'], sample_state['y'], sample_state['z'])

                # final_mode帧：速度命令应为0（保持在终点）
                if is_final_mode:
                    sample_state['vx'] = 0.0
                    sample_state['vy'] = 0.0
                    sample_state['vz'] = 0.0
                    sample_state['yaw_rate'] = 0.0

                p_in = TrajPoint(
                    sample_state['x'], sample_state['y'], sample_state['z'],
                    math.degrees(sample_state['roll']),
                    math.degrees(sample_state['yaw']),
                    math.degrees(sample_state['pitch'])
                )
                p_cmd = TrajPoint(
                    sample_pos[0], sample_pos[1], sample_pos[2],
                    math.degrees(sample_state['roll']),
                    math.degrees(sample_state['yaw']),
                    math.degrees(sample_state['pitch'])
                )

                # 保存图像（处理磁盘模式和内存模式）
                ts_ms = int(ts_img * 1000.0) if ts_img > 0 else int(time.time() * 1000.0)
                img_name = f"img_{i:06d}_{ts_ms}.png"
                img_path = traj_dir / "images" / img_name
                img_path.parent.mkdir(parents=True, exist_ok=True)

                # 检查是否已经保存到磁盘（磁盘模式）
                src_image_path = frame.get("image_path", "")
                img_b64 = frame.get("image_b64", "")

                if src_image_path and os.path.exists(src_image_path):
                    # 磁盘模式：复制图像到最终位置
                    import shutil
                    try:
                        shutil.copy2(src_image_path, str(img_path))
                    except Exception as e:
                        self._log(f"[UAV{self.uav_id}] frame {i} copy error: {e}", "WARN")
                elif isinstance(img_b64, str) and img_b64:
                    # 内存模式：从base64解码保存
                    _decode_png_b64_to_file(img_b64, img_path)

                # 保存深度图像（可选，失败不影响主流程）
                try:
                    # 检查buffer帧是否包含深度数据
                    depth_b64 = frame.get("depth_b64", "")
                    depth_path_src = frame.get("depth_path", "")
                    depth_name = f"depth_{i:06d}_{ts_ms}.png"
                    depth_path = traj_dir / "depths" / depth_name
                    depth_path.parent.mkdir(parents=True, exist_ok=True)

                    if depth_path_src and os.path.exists(depth_path_src):
                        # 磁盘模式：复制深度图像
                        import shutil
                        shutil.copy2(depth_path_src, str(depth_path))
                    elif isinstance(depth_b64, str) and depth_b64:
                        # 内存模式：从base64解码保存
                        _decode_png_b64_to_file(depth_b64, depth_path)
                    else:
                        # 尝试实时获取深度图像
                        depth_info = _fetch_depth(self.image_base, self.uav_id, timeout=self.image_timeout)
                        if depth_info and depth_info.get("data"):
                            _decode_png_b64_to_file(depth_info["data"], depth_path)
                except Exception:
                    pass  # 深度图像采集是可选的，失败不影响主流程

                # 提取位姿数据
                pos = pose.get("position", [None, None, None])[:3]
                att = pose.get("attitude", [None, None, None, None])[:4]
                lv = pose.get("linear_velocity", [None, None, None])[:3]
                av = pose.get("angular_velocity", [None, None, None])[:3]
                la = pose.get("linear_acceleration", [None, None, None])[:3]

                obs_pos = (
                    float(pos[0]) if pos[0] is not None else 0.0,
                    float(pos[1]) if pos[1] is not None else 0.0,
                    float(pos[2]) if pos[2] is not None else 0.0,
                )

                aligned_obs = self._apply_alignment(obs_pos)

                cmd_velocity = {
                    'vx': sample_state['vx'],
                    'vy': sample_state['vy'],
                    'vz': sample_state['vz'],
                    'yaw': sample_state['yaw'],
                    'yaw_rate': sample_state['yaw_rate']
                }

                # 添加到主数据行（使用相对路径）
                rows.append({
                    "traj_json": _relative_path(json_path, self.out_dir),
                    "traj_name": _safe_name(json_path),
                    "uav_id": self.uav_id,
                    "step_idx": i,
                    "cmd_in_x": p_in.x,
                    "cmd_in_y": p_in.y,
                    "cmd_in_z": p_in.z,
                    "cmd_in_roll_deg": p_in.roll_deg,
                    "cmd_in_yaw_deg": p_in.yaw_deg,
                    "cmd_in_pitch_deg": p_in.pitch_deg,
                    "cmd_x": p_cmd.x,
                    "cmd_y": p_cmd.y,
                    "cmd_z": p_cmd.z,
                    "cmd_roll_deg": p_cmd.roll_deg,
                    "cmd_yaw_deg": p_cmd.yaw_deg,
                    "cmd_pitch_deg": p_cmd.pitch_deg,
                    "image_timestamp_s": ts_img,
                    "image_path": _relative_path(img_path, self.out_dir) if img_path.exists() else "",
                    "obs_pos_x": pos[0],
                    "obs_pos_y": pos[1],
                    "obs_pos_z": pos[2],
                    "obs_aligned_x": aligned_obs[0],
                    "obs_aligned_y": aligned_obs[1],
                    "obs_aligned_z": aligned_obs[2],
                    "origin_offset_x": self._origin_offset[0] if self._origin_offset else 0.0,
                    "origin_offset_y": self._origin_offset[1] if self._origin_offset else 0.0,
                    "origin_offset_z": self._origin_offset[2] if self._origin_offset else 0.0,
                    "obs_att_w": att[3] if len(att) >= 4 else None,
                    "obs_att_x": att[0] if len(att) >= 4 else None,
                    "obs_att_y": att[1] if len(att) >= 4 else None,
                    "obs_att_z": att[2] if len(att) >= 4 else None,
                    "obs_linvel_x": lv[0],
                    "obs_linvel_y": lv[1],
                    "obs_linvel_z": lv[2],
                    "obs_angvel_x": av[0],
                    "obs_angvel_y": av[1],
                    "obs_angvel_z": av[2],
                    "obs_linacc_x": la[0],
                    "obs_linacc_y": la[1],
                    "obs_linacc_z": la[2],
                    "cmd_vx": cmd_velocity.get('vx', 0.0),
                    "cmd_vy": cmd_velocity.get('vy', 0.0),
                    "cmd_vz": cmd_velocity.get('vz', 0.0),
                    "cmd_yaw_rad": cmd_velocity.get('yaw', 0.0),
                    "cmd_yaw_rate": cmd_velocity.get('yaw_rate', 0.0),
                    "is_final_mode": is_final_mode,
                    "relative_time_s": relative_t,
                })

            except Exception as e:
                self._log(f"[UAV{self.uav_id}] frame {i} processing error: {e}", "WARN")

    def _process_pose_samples(
        self,
        samples: List[Dict],
        smoother: TrajectorySmoother,
        json_path: Path,
        traj_dir: Path,
        rows: List[Dict],
        apply_offset,
        total_duration: float,
    ) -> None:
        if not samples:
            return

        base_ts = float(samples[0].get("timestamp", 0.0) or 0.0)
        for i, sample in enumerate(samples):
            try:
                ts_img = float(sample.get("timestamp", 0.0) or 0.0)
                pose = sample.get("pose", {}) or {}
                pos = (pose.get("position") or [None, None, None])[:3]
                att = (pose.get("attitude") or [None, None, None, None])[:4]
                lv = (pose.get("linear_velocity") or [None, None, None])[:3]
                av = (pose.get("angular_velocity") or [None, None, None])[:3]
                la = (pose.get("linear_acceleration") or [None, None, None])[:3]

                relative_t = ts_img - base_ts if base_ts > 0 else i * 0.05
                is_final_mode = relative_t > float(total_duration)

                sample_state = smoother.get_full_state(relative_t, for_control=False)
                sample_pos = apply_offset(sample_state['x'], sample_state['y'], sample_state['z'])

                if is_final_mode:
                    sample_state['vx'] = 0.0
                    sample_state['vy'] = 0.0
                    sample_state['vz'] = 0.0
                    sample_state['yaw_rate'] = 0.0

                p_in = TrajPoint(
                    sample_state['x'], sample_state['y'], sample_state['z'],
                    math.degrees(sample_state['roll']),
                    math.degrees(sample_state['yaw']),
                    math.degrees(sample_state['pitch'])
                )
                p_cmd = TrajPoint(
                    sample_pos[0], sample_pos[1], sample_pos[2],
                    math.degrees(sample_state['roll']),
                    math.degrees(sample_state['yaw']),
                    math.degrees(sample_state['pitch'])
                )

                cmd_velocity = {
                    'vx': sample_state['vx'],
                    'vy': sample_state['vy'],
                    'vz': sample_state['vz'],
                    'yaw': sample_state['yaw'],
                    'yaw_rate': sample_state['yaw_rate']
                }
                if is_final_mode:
                    cmd_velocity['vx'] = 0.0
                    cmd_velocity['vy'] = 0.0
                    cmd_velocity['vz'] = 0.0
                    cmd_velocity['yaw_rate'] = 0.0

                obs_pos = (
                    float(pos[0]) if pos[0] is not None else 0.0,
                    float(pos[1]) if pos[1] is not None else 0.0,
                    float(pos[2]) if pos[2] is not None else 0.0,
                )
                aligned_obs = self._apply_alignment(obs_pos)

                rows.append({
                    "traj_json": _relative_path(json_path, self.out_dir),
                    "traj_name": _safe_name(json_path),
                    "uav_id": self.uav_id,
                    "step_idx": i,
                    "cmd_in_x": p_in.x,
                    "cmd_in_y": p_in.y,
                    "cmd_in_z": p_in.z,
                    "cmd_in_roll_deg": p_in.roll_deg,
                    "cmd_in_yaw_deg": p_in.yaw_deg,
                    "cmd_in_pitch_deg": p_in.pitch_deg,
                    "cmd_x": p_cmd.x,
                    "cmd_y": p_cmd.y,
                    "cmd_z": p_cmd.z,
                    "cmd_roll_deg": p_cmd.roll_deg,
                    "cmd_yaw_deg": p_cmd.yaw_deg,
                    "cmd_pitch_deg": p_cmd.pitch_deg,
                    "image_timestamp_s": ts_img,
                    "image_path": "",
                    "obs_pos_x": pos[0],
                    "obs_pos_y": pos[1],
                    "obs_pos_z": pos[2],
                    "obs_aligned_x": aligned_obs[0],
                    "obs_aligned_y": aligned_obs[1],
                    "obs_aligned_z": aligned_obs[2],
                    "origin_offset_x": self._origin_offset[0] if self._origin_offset else 0.0,
                    "origin_offset_y": self._origin_offset[1] if self._origin_offset else 0.0,
                    "origin_offset_z": self._origin_offset[2] if self._origin_offset else 0.0,
                    "obs_att_w": att[3] if len(att) >= 4 else None,
                    "obs_att_x": att[0] if len(att) >= 4 else None,
                    "obs_att_y": att[1] if len(att) >= 4 else None,
                    "obs_att_z": att[2] if len(att) >= 4 else None,
                    "obs_linvel_x": lv[0],
                    "obs_linvel_y": lv[1],
                    "obs_linvel_z": lv[2],
                    "obs_angvel_x": av[0],
                    "obs_angvel_y": av[1],
                    "obs_angvel_z": av[2],
                    "obs_linacc_x": la[0],
                    "obs_linacc_y": la[1],
                    "obs_linacc_z": la[2],
                    "cmd_vx": cmd_velocity.get('vx', 0.0),
                    "cmd_vy": cmd_velocity.get('vy', 0.0),
                    "cmd_vz": cmd_velocity.get('vz', 0.0),
                    "cmd_yaw_rad": cmd_velocity.get('yaw', 0.0),
                    "cmd_yaw_rate": cmd_velocity.get('yaw_rate', 0.0),
                    "is_final_mode": is_final_mode,
                    "relative_time_s": relative_t,
                })
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] pose sample {i} processing error: {e}", "WARN")

    def _process_offboard_mode_legacy(self, json_path: Path, raw_pts: List[TrajPoint], pts: List[TrajPoint],
                                       traj_dir: Path, rows: List[Dict]) -> None:
        """旧版OFFBOARD模式：逐个航点导航（用于回退）"""
        self._log(f"[UAV{self.uav_id}] starting LEGACY waypoint navigation ({len(pts)} points)")

        for i, (p_in, p_cmd) in enumerate(zip(raw_pts, pts)):
            self._log(f"[UAV{self.uav_id}] moving to waypoint {i}/{len(pts)-1}: ({p_cmd.x:.2f}, {p_cmd.y:.2f}, {p_cmd.z:.2f})")

            try:
                cmd = {"cmd": "move_to", "x": float(p_cmd.x), "y": float(p_cmd.y), "z": float(p_cmd.z), "force": True}
                resp = _controller_command(self.control_base, self.uav_id, cmd, timeout=self.cmd_timeout)
                if not isinstance(resp, dict) or resp.get("ok") is False:
                    raise RuntimeError(f"move_to failed: {resp}")
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] waypoint {i} move_to failed: {e}", "WARN")
                raise

            # 获取图像和位姿
            self._collect_data_point(i, p_in, p_cmd, json_path, traj_dir, rows)

    def _process_mission_mode(self, json_path: Path, transformed_pts: List[TrajPoint], pts: List[TrajPoint],
                               traj_dir: Path, rows: List[Dict]) -> None:
        """Mission模式：一次性上传所有航点"""
        self._log(f"[UAV{self.uav_id}] starting MISSION mode navigation ({len(pts)} points)")

        # 构建航点列表
        waypoints = [{"x": float(p.x), "y": float(p.y), "z": float(p.z)} for p in pts]

        # 发送execute_mission命令
        try:
            cmd = {"cmd": "execute_mission", "waypoints": waypoints, "force": True}
            # 不等待完成，我们自己监控进度
            _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=5.0)
        except Exception as e:
            self._log(f"[UAV{self.uav_id}] execute_mission failed: {e}", "WARN")

        # 监控任务进度并采集数据
        for i, (p_in, p_cmd) in enumerate(zip(transformed_pts, pts)):
            self._log(f"[UAV{self.uav_id}] waiting for waypoint {i}/{len(pts)-1}: ({p_cmd.x:.2f}, {p_cmd.y:.2f}, {p_cmd.z:.2f})")

            # 等待到达航点
            target = (p_cmd.x, p_cmd.y, p_cmd.z)
            timeout = 120.0
            start = time.time()
            reached = False

            while time.time() - start < timeout:
                try:
                    if self.collect_images:
                        info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=3)
                        pose = info.get("pose", {})
                        pos = pose.get("position", [0, 0, 0])
                    else:
                        pose = _fetch_pose_only(self.image_base, self.uav_id, timeout=self.image_timeout) or {}
                        pos = pose.get("position", [0, 0, 0])
                    current = (pos[0], pos[1], pos[2])
                    dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(current, target)))
                    if dist < 0.5:  # 到达阈值
                        reached = True
                        break
                except Exception:
                    pass
                time.sleep(0.1)

            if not reached:
                self._log(f"[UAV{self.uav_id}] waypoint {i} timeout", "WARN")

            # 采集数据
            self._collect_data_point(i, p_in, p_cmd, json_path, traj_dir, rows)

    def _collect_data_point(self, i: int, p_in: TrajPoint, p_cmd: TrajPoint,
                            json_path: Path, traj_dir: Path,
                            rows: List[Dict],
                            cmd_velocity: Optional[Dict[str, float]] = None) -> None:
        """采集单个数据点

        Args:
            cmd_velocity: 可选的速度前馈信息，包含 vx, vy, vz, yaw, yaw_rate
        """
        img_path = Path("")
        ts_img = 0.0
        if self.collect_images:
            info = _fetch_all_info(
                self.image_base,
                self.uav_id,
                timeout=self.image_timeout,
                retries=self.image_retries,
            )
            ts_img = float((info.get("image") or {}).get("timestamp") or 0.0)
            img_b64 = (info.get("image") or {}).get("data") or ""
            ts_ms = int(ts_img * 1000.0) if ts_img > 0 else int(time.time() * 1000.0)
            img_name = f"img_{i:06d}_{ts_ms}.png"
            img_path = traj_dir / "images" / img_name
            if isinstance(img_b64, str) and img_b64:
                _decode_png_b64_to_file(img_b64, img_path)

            try:
                depth_info = _fetch_depth(self.image_base, self.uav_id, timeout=self.image_timeout)
                if depth_info and depth_info.get("data"):
                    depth_name = f"depth_{i:06d}_{ts_ms}.png"
                    depth_path = traj_dir / "depths" / depth_name
                    depth_path.parent.mkdir(parents=True, exist_ok=True)
                    _decode_png_b64_to_file(depth_info["data"], depth_path)
            except Exception:
                pass
            pose = info.get("pose") or {}
        else:
            pose_info = _fetch_pose_only(self.image_base, self.uav_id, timeout=self.image_timeout)
            pose = pose_info or {}
            ts_img = float(pose.get("timestamp", 0.0) or 0.0)
        pos = (pose.get("position") or [None, None, None])[:3]
        att = (pose.get("attitude") or [None, None, None, None])[:4]
        lv = (pose.get("linear_velocity") or [None, None, None])[:3]
        av = (pose.get("angular_velocity") or [None, None, None])[:3]
        la = (pose.get("linear_acceleration") or [None, None, None])[:3]

        # 坐标对齐
        obs_pos = (
            float(pos[0]) if pos[0] is not None else 0.0,
            float(pos[1]) if pos[1] is not None else 0.0,
            float(pos[2]) if pos[2] is not None else 0.0,
        )
        cmd_pos = (float(p_cmd.x), float(p_cmd.y), float(p_cmd.z))

        if i == 0 and self._origin_offset is None:
            self._origin_offset = self._calculate_origin_offset(cmd_pos, obs_pos)
            self._log(
                f"[UAV{self.uav_id}] origin offset: "
                f"({self._origin_offset[0]:.4f}, {self._origin_offset[1]:.4f}, {self._origin_offset[2]:.4f})m"
            )

        aligned_obs = self._apply_alignment(obs_pos)

        rows.append(
            {
                "traj_json": _relative_path(json_path, self.out_dir),
                "traj_name": _safe_name(json_path),
                "uav_id": self.uav_id,
                "step_idx": i,
                "cmd_in_x": p_in.x,
                "cmd_in_y": p_in.y,
                "cmd_in_z": p_in.z,
                "cmd_in_roll_deg": p_in.roll_deg,
                "cmd_in_yaw_deg": p_in.yaw_deg,
                "cmd_in_pitch_deg": p_in.pitch_deg,
                "cmd_x": p_cmd.x,
                "cmd_y": p_cmd.y,
                "cmd_z": p_cmd.z,
                "cmd_roll_deg": p_cmd.roll_deg,
                "cmd_yaw_deg": p_cmd.yaw_deg,
                "cmd_pitch_deg": p_cmd.pitch_deg,
                "image_timestamp_s": ts_img,
                "image_path": _relative_path(img_path, self.out_dir) if img_path.exists() else "",
                "obs_pos_x": pos[0],
                "obs_pos_y": pos[1],
                "obs_pos_z": pos[2],
                "obs_aligned_x": aligned_obs[0],
                "obs_aligned_y": aligned_obs[1],
                "obs_aligned_z": aligned_obs[2],
                "origin_offset_x": self._origin_offset[0] if self._origin_offset else 0.0,
                "origin_offset_y": self._origin_offset[1] if self._origin_offset else 0.0,
                "origin_offset_z": self._origin_offset[2] if self._origin_offset else 0.0,
                "obs_att_w": att[3] if len(att) >= 4 else None,
                "obs_att_x": att[0] if len(att) >= 4 else None,
                "obs_att_y": att[1] if len(att) >= 4 else None,
                "obs_att_z": att[2] if len(att) >= 4 else None,
                "obs_linvel_x": lv[0],
                "obs_linvel_y": lv[1],
                "obs_linvel_z": lv[2],
                "obs_angvel_x": av[0],
                "obs_angvel_y": av[1],
                "obs_angvel_z": av[2],
                "obs_linacc_x": la[0],
                "obs_linacc_y": la[1],
                "obs_linacc_z": la[2],
                # 速度前馈控制输入
                "cmd_vx": cmd_velocity.get('vx', 0.0) if cmd_velocity else 0.0,
                "cmd_vy": cmd_velocity.get('vy', 0.0) if cmd_velocity else 0.0,
                "cmd_vz": cmd_velocity.get('vz', 0.0) if cmd_velocity else 0.0,
                "cmd_yaw_rad": cmd_velocity.get('yaw', 0.0) if cmd_velocity else 0.0,
                "cmd_yaw_rate": cmd_velocity.get('yaw_rate', 0.0) if cmd_velocity else 0.0,
            }
        )


def main() -> None:
    p = argparse.ArgumentParser(description="MAVLink Trajectory Collector")
    p.add_argument("--input-dir", type=str, required=True, help="轨迹JSON文件目录")
    p.add_argument("--pattern", type=str, default="*.json", help="JSON文件匹配模式")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "recordings"), help="输出目录")
    p.add_argument("--config", type=str, default=str(Path(__file__).resolve().parent / "multi_uav_config.json"), help="UAV配置文件")
    p.add_argument("--uav-ids", type=str, default="", help="UAV ID列表，逗号分隔")
    p.add_argument("--control-base", type=str, default="http://127.0.0.1:5009", help="控制器基础URL")
    p.add_argument("--image-base", type=str, default="http://127.0.0.1:8081", help="图像服务基础URL")
    p.add_argument("--scale", type=float, default=0.01, help="坐标缩放因子")
    p.add_argument("--max-points", type=int, default=0, help="最大轨迹点数")
    zg = p.add_mutually_exclusive_group()
    zg.add_argument("--z-down", action="store_true", default=None, help="Z轴向下")
    zg.add_argument("--z-up", action="store_true", default=None, help="Z轴向上")
    p.add_argument("--reset-timeout", type=float, default=120.0, help="重置超时")
    p.add_argument("--cmd-timeout", type=float, default=60.0, help="命令超时")
    p.add_argument("--image-timeout", type=float, default=10.0, help="图像获取超时")
    p.add_argument("--image-retries", type=int, default=30, help="图像获取重试次数")
    sg = p.add_mutually_exclusive_group()
    sg.add_argument("--skip-existing", action="store_true", default=None, help="跳过已存在轨迹")
    sg.add_argument("--no-skip-existing", action="store_true", default=None, help="不跳过已存在轨迹")
    p.add_argument("--dry-run", action="store_true", default=False, help="仅扫描文件")
    p.add_argument("--use-mission", action="store_true", default=False, help="使用Mission模式")
    p.add_argument("--time-scale", type=float, default=2.5, help="轨迹时间缩放因子（>1减速，<1加速，默认2.5）")
    p.add_argument("--no-images", action="store_true", default=False, help="不采集图像信息（RGB/Depth均关闭）")
    args = p.parse_args()

    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = _iter_json_files(input_dir, args.pattern)
    if not json_files:
        raise SystemExit(f"no json files found under {input_dir} with pattern={args.pattern}")

    if bool(args.dry_run):
        total_pts = 0
        for fp in json_files:
            try:
                pts = _load_preprocessed_xyz(fp)
                total_pts += len(pts)
                ts_log("[DryRun]", f"{fp}: {len(pts)} points")
            except Exception as e:
                ts_log("[DryRun]", f"{fp}: error {e}", "ERROR")
        ts_log("[DryRun]", f"files={len(json_files)} total_points={total_pts}")
        return

    if args.uav_ids.strip():
        uav_ids = sorted(set(int(x.strip()) for x in args.uav_ids.split(",") if x.strip()))
    else:
        uav_ids = _load_uav_ids_from_config(Path(args.config).resolve())

    control_base = str(args.control_base).rstrip("/")
    image_base = str(args.image_base).rstrip("/")
    if args.z_down is None and args.z_up is None:
        z_down = False  # Default: ENU (z-up) trajectory data
    else:
        z_down = bool(args.z_down) and not bool(args.z_up)
    if args.skip_existing is None and args.no_skip_existing is None:
        skip_existing = True
    else:
        skip_existing = bool(args.skip_existing) and not bool(args.no_skip_existing)

    _ensure_control_healthy(control_base, uav_ids=uav_ids, timeout_s=120.0)

    q: "queue.Queue[Path]" = queue.Queue()
    for f in json_files:
        q.put(f)

    status_log_path = out_dir / "mission_status.csv"
    print_lock = threading.Lock()
    threads: List[threading.Thread] = []

    for vid in uav_ids:
        w = Worker(
            uav_id=vid,
            control_base=control_base,
            image_base=image_base,
            out_dir=out_dir,
            task_queue=q,
            scale=args.scale,
            z_down=z_down,
            max_points=args.max_points,
            reset_timeout=args.reset_timeout,
            cmd_timeout=args.cmd_timeout,
            image_timeout=args.image_timeout,
            image_retries=args.image_retries,
            skip_existing=skip_existing,
            use_mission=args.use_mission,
            time_scale=args.time_scale,
            collect_images=not bool(args.no_images),
            print_lock=print_lock,
            status_log_path=status_log_path,
        )
        t = threading.Thread(target=w.run, name=f"uav{vid}", daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    ts_log("[Main]", "All workers completed")


if __name__ == "__main__":
    main()
