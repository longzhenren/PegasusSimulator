#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# Copyright (c) 2024-2026
# Licensed under the MIT License
"""
纯Python轨迹数据采集器（fast_trajectory_collector.py）

==========================
概述
==========================
本脚本是纯Python版本的轨迹数据采集工具，特点：
- 不依赖 MAVROS 或 ROS2
- 使用纯 HTTP 接口控制仿真环境
- 与 trajectory_data_collector.py 的输入输出格式完全兼容
- 支持多架 UAV 并行采集
- 与 fast_sim_vehicle.py 配合使用

==========================
与 trajectory_data_collector.py 的区别
==========================
| 特性           | trajectory_data_collector.py | fast_trajectory_collector.py |
|----------------|------------------------------|------------------------------|
| 依赖           | ROS2, MAVROS, PX4            | 无外部依赖                   |
| 控制方式       | MAVROS服务 + HTTP            | 纯HTTP                       |
| 状态监控       | ROS2话题订阅                 | HTTP轮询                     |
| 仿真环境       | 8_camera_vehicle.py          | fast_sim_vehicle.py          |
| 输入格式       | JSON轨迹文件                 | JSON轨迹文件（相同）         |
| 输出格式       | CSV + PNG + ULG              | CSV + PNG（相同，无ULG）     |

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
          ├── data.csv             # 主数据文件
          ├── all_pose_data.csv    # 简化位姿数据
          └── images/
              ├── img_000000_<ts_ms>.png
              └── ...

==========================
命令行参数（与原系统完全相同）
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

==========================
使用示例
==========================
# 基础采集（需要先启动 fast_sim_vehicle.py）
python examples/fast_trajectory_collector.py \\
  --input-dir ~/trajectories \\
  --out-dir ~/recordings \\
  --uav-ids 0

# 多机并行采集
python examples/fast_trajectory_collector.py \\
  --input-dir ~/trajectories \\
  --config examples/multi_uav_config.json

"""

import argparse
import base64
import csv
import glob
import json
import math
import os
import queue
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
    """加载预处理的轨迹点"""
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs")
    if not isinstance(logs, list):
        raise ValueError("missing preprocessed_logs")
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
        z = float(row[2])
        roll = float(row[3])
        yaw = float(row[4])
        pitch = float(row[5])
        pts.append(TrajPoint(y, x, z, roll, yaw, pitch))  # ENU to NEU
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
    return TrajPoint(y, x, z, roll, yaw, pitch)  # ENU to NEU


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
    """坐标变换"""
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


# 最小航点距离
MIN_WAYPOINT_DIST = float(os.environ.get("PEGASUS_MIN_WAYPOINT_DIST", "0.35"))


def _filter_close_points(pts: List[TrajPoint], min_dist: float = MIN_WAYPOINT_DIST) -> List[TrajPoint]:
    """过滤距离过近的航点"""
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

    if len(pts) > 1:
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


def _write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    """写入CSV文件"""
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
    """标准化绝对路径"""
    try:
        return str(p.resolve())
    except Exception:
        return str(p.absolute())


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
        """执行重置并等待就绪"""
        _controller_reset(
            self.control_base,
            self.uav_id,
            hard=True,
            force=True,
            position=position,
            yaw_deg=yaw_deg,
            timeout=timeout,
        )

        # 等待就绪（纯HTTP模式，重置后即就绪）
        self._log(f"[UAV{self.uav_id}] reset complete, waiting for stabilization...")
        time.sleep(1.0)

    def _append_status_log(self, traj_name: str) -> None:
        """追加状态日志"""
        try:
            row = {
                "traj_name": traj_name,
                "uav_id": self.uav_id,
                "timestamp": time.time(),
                "status": "success"
            }
            with self.print_lock:
                exists = self.status_log_path.exists()
                with self.status_log_path.open("a", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=["traj_name", "uav_id", "timestamp", "status"])
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
        pose_all_path = traj_dir / "all_pose_data.csv"
        if self.skip_existing and csv_path.exists() and pose_all_path.exists():
            self._log(f"[UAV{self.uav_id}] skip existing traj={traj_name}")
            return

        raw_pts = _load_preprocessed_xyz(json_path)
        init_pos = _load_init_point_xyz(json_path)

        pts = _transform_points(raw_pts, self.scale, init_pos.x, init_pos.y, init_pos.z, self.z_down)

        # 过滤近点
        pts_before = len(pts)
        pts = _filter_close_points(pts, MIN_WAYPOINT_DIST)
        if len(pts) < pts_before:
            self._log(f"[UAV{self.uav_id}] filtered {pts_before - len(pts)} close waypoints, {len(pts)} remaining")

        if int(self.max_points) > 0:
            raw_pts = raw_pts[: int(self.max_points)]
            pts = pts[: int(self.max_points)]
            if not pts:
                raise ValueError(f"max_points={self.max_points} results in empty trajectory")

        self._log(f"[UAV{self.uav_id}] reset before traj={traj_name}")

        # 执行重置
        self._reset_and_wait_ready(
            position=[init_pos.x * self.scale, init_pos.y * self.scale, init_pos.z * self.scale],
            yaw_deg=None,
            timeout=self.reset_timeout,
        )

        traj_start_ts = time.time()
        self._log(f"[UAV{self.uav_id}] start traj={traj_name} points={len(pts)}")

        rows: List[Dict[str, Any]] = []
        all_pose_rows: List[Dict[str, Any]] = []
        self._origin_offset = None

        try:
            # OFFBOARD模式：逐个航点发送move_to命令
            self._log(f"[UAV{self.uav_id}] starting OFFBOARD waypoint navigation ({len(pts)} points)")

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

                # 获取并保存深度图像（可选，失败不影响主流程）
                try:
                    depth_info = _fetch_depth(self.image_base, self.uav_id, timeout=self.image_timeout)
                    if depth_info and depth_info.get("data"):
                        depth_name = f"depth_{i:06d}_{ts_ms}.png"
                        depth_path = traj_dir / "depths" / depth_name
                        depth_path.parent.mkdir(parents=True, exist_ok=True)
                        _decode_png_b64_to_file(depth_info["data"], depth_path)
                except Exception:
                    pass  # 深度图像采集是可选的，失败不影响主流程

                pose = info.get("pose") or {}
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
                        "traj_json": _norm_abs_path(json_path),
                        "traj_name": traj_name,
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
                        "image_path": _norm_abs_path(img_path) if img_path.exists() else "",
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
                    }
                )

                all_pose_rows.append(
                    {
                        "traj_json": _norm_abs_path(json_path),
                        "traj_name": traj_name,
                        "uav_id": self.uav_id,
                        "step_idx": i,
                        "image_timestamp_s": ts_img,
                        "pos_x": pos[0],
                        "pos_y": pos[1],
                        "pos_z": pos[2],
                        "aligned_pos_x": aligned_obs[0],
                        "aligned_pos_y": aligned_obs[1],
                        "aligned_pos_z": aligned_obs[2],
                        "cmd_roll_deg": p_cmd.roll_deg,
                        "cmd_yaw_deg": p_cmd.yaw_deg,
                        "cmd_pitch_deg": p_cmd.pitch_deg,
                        "att_w": att[3] if len(att) >= 4 else None,
                        "att_x": att[0] if len(att) >= 4 else None,
                        "att_y": att[1] if len(att) >= 4 else None,
                        "att_z": att[2] if len(att) >= 4 else None,
                        "linvel_x": lv[0],
                        "linvel_y": lv[1],
                        "linvel_z": lv[2],
                        "angvel_x": av[0],
                        "angvel_y": av[1],
                        "angvel_z": av[2],
                        "linacc_x": la[0],
                        "linacc_y": la[1],
                        "linacc_z": la[2],
                    }
                )

            # 降落
            self._log(f"[UAV{self.uav_id}] executing land command for traj={traj_name}")
            try:
                land_cmd = {"cmd": "land", "force": True}
                land_resp = _controller_command(self.control_base, self.uav_id, land_cmd, timeout=max(60.0, self.cmd_timeout))
                if not isinstance(land_resp, dict) or land_resp.get("ok") is False:
                    self._log(f"[UAV{self.uav_id}] land command failed: {land_resp}", "WARN")
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] land exception: {e}", "WARN")
            time.sleep(2.0)

            # 重置
            self._log(f"[UAV{self.uav_id}] reset after land traj={traj_name}")
            try:
                self._reset_and_wait_ready(
                    position=[init_pos.x * self.scale, init_pos.y * self.scale, init_pos.z * self.scale],
                    yaw_deg=None,
                    timeout=self.reset_timeout,
                )
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] reset failed traj={traj_name} err={e}", "WARN")

            # 写入CSV
            _write_csv(rows, csv_path)
            if all_pose_rows:
                _write_csv(all_pose_rows, pose_all_path)
            self._log(f"[UAV{self.uav_id}] done traj={traj_name} csv={csv_path} pose_csv={pose_all_path}")

            self._append_status_log(traj_name)

        except Exception as e:
            raise e


def main() -> None:
    p = argparse.ArgumentParser(description="Fast Trajectory Collector (Pure Python)")
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
        z_down = True
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
