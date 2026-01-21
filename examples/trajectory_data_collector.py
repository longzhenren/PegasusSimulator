#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# Copyright (c) 2024-2026 amurzzb@gmail.com
# Licensed under the MIT License
"""
轨迹数据采集器（trajectory_data_collector.py）

==========================
概述
==========================
本脚本是 Pegasus 仿真系统的轨迹数据采集工具，负责：
- 从 JSON 文件加载预处理的轨迹点序列
- 通过 MAVROS 将轨迹转换为 PX4 任务航点并执行飞行
- 在每个航点采集图像和位姿数据
- 保存图像（PNG）、位姿数据（CSV）和 PX4 飞行日志（ULG）
- 支持多架 UAV 并行采集

==========================
启动流程与时序
==========================
1. 参数解析与初始化
   - 解析命令行参数
   - 扫描输入目录下的 JSON 轨迹文件
   - 加载 multi_uav_config.json 获取 UAV ID 列表

2. ROS2 初始化
   - 创建 CollectorNode 节点
   - 为每架 UAV 创建 MavrosMonitor（订阅状态话题）
   - 为每架 UAV 创建 MavrosCommander（服务客户端）
   - 启动 ROS2 spin 线程

3. 健康检查
   - 检测所有控制器 HTTP 端口（5009+id）是否可用
   - 超时 120s 未通过则报错退出

4. 多线程采集
   - 为每架 UAV 创建 Worker 线程
   - 所有 Worker 从共享队列获取轨迹任务
   - 并行执行轨迹采集

5. 单条轨迹采集流程
   a) 加载轨迹点：从 JSON 读取 preprocessed_logs
   b) 坐标变换：按 scale 缩放，ENU→NEU 转换
   c) 重置 UAV：调用控制器 /reset 接口
   d) 等待就绪：等待 OFFBOARD 模式 + PX4 Ready to takeoff
   e) 上传任务：通过 MAVROS 清除并上传航点任务
   f) 解锁起飞：设置 AUTO.MISSION 模式并 arm
   g) 航点采集：
      - 等待到达每个航点（监控 mission/reached）
      - 通过 HTTP 获取图像和位姿（/uav/<id>/all）
      - 保存 PNG 图像和 CSV 数据行
   h) 降落：任务完成后等待降落
   i) 保存日志：查找并复制 PX4 ULG 文件
   j) 输出 CSV：写入 data.csv 和 all_pose_data.csv

6. 清理退出
   - 等待所有 Worker 完成
   - 关闭 ROS2 节点

==========================
命令行参数
==========================
--input-dir PATH        轨迹 JSON 文件目录（必需）
--pattern GLOB          JSON 文件匹配模式（默认：*.json）
--out-dir PATH          输出目录（默认：./recordings）
--config PATH           UAV 配置文件路径（默认：./multi_uav_config.json）
--uav-ids IDS           指定 UAV ID 列表，逗号分隔（默认：从配置文件读取）
--control-base URL      控制器基础 URL（默认：http://127.0.0.1:5009）
--image-base URL        图像服务基础 URL（默认：http://127.0.0.1:8081）
--scale FLOAT           坐标缩放因子（默认：0.01）
--max-points INT        最大轨迹点数，0=不限制（默认：0）
--z-down / --z-up       Z 轴方向（默认：--z-down）
--reset-timeout FLOAT   重置超时时间（默认：240s）
--cmd-timeout FLOAT     命令超时时间（默认：120s）
--image-timeout FLOAT   图像获取超时（默认：10s）
--image-retries INT     图像获取重试次数（默认：30）
--skip-existing         跳过已存在的轨迹（默认）
--no-skip-existing      不跳过已存在的轨迹
--dry-run               仅扫描文件，不执行采集

==========================
轨迹 JSON 文件格式
==========================
{
  "raw_logs": [
    [x, y, z, roll, yaw, pitch],  // 原始初始位置（用于重置）
    ...
  ],
  "preprocessed_logs": [
    [x, y, z, roll, yaw, pitch],  // 预处理后的轨迹点序列
    [x, y, z, roll, yaw, pitch],
    ...
  ]
}

坐标说明：
- 输入坐标系：ENU（东-北-天）
- 输出坐标系：NEU（北-东-天），即 x↔y 交换
- Z 轴：--z-down 时 z = base_z - input_z，--z-up 时 z = base_z + input_z

==========================
输出目录结构
==========================
<out_dir>/
  ├── mission_status.csv           # 全局任务状态日志
  └── <traj_name>/
      └── uav<id>/
          ├── data.csv             # 主数据文件（含命令、观测、图像路径）
          ├── all_pose_data.csv    # 简化位姿数据
          ├── mavros_data.csv      # MAVROS 原始数据（高频采样）
          ├── px4_uav<id>_<ts>.ulg # PX4 飞行日志
          └── images/
              ├── img_000000_<ts_ms>.png
              ├── img_000001_<ts_ms>.png
              └── ...

==========================
CSV 文件字段说明
==========================
data.csv 字段：
  traj_json          - 源 JSON 文件路径
  traj_name          - 轨迹名称
  uav_id             - UAV ID
  step_idx           - 步骤索引
  cmd_in_x/y/z       - 输入坐标（变换前）
  cmd_in_roll/yaw/pitch_deg - 输入姿态角
  cmd_x/y/z          - 命令坐标（变换后）
  cmd_roll/yaw/pitch_deg - 命令姿态角
  image_timestamp_s  - 图像时间戳
  image_path         - 图像文件路径
  obs_pos_x/y/z      - 观测位置
  obs_att_w/x/y/z    - 观测四元数姿态
  obs_linvel_x/y/z   - 观测线速度
  obs_angvel_x/y/z   - 观测角速度
  obs_linacc_x/y/z   - 观测线加速度
  ulg_path           - PX4 日志文件路径

mavros_data.csv 字段：
  sample_idx         - 采样索引
  host_time          - 主机时间戳
  mavros_connected   - MAVROS 连接状态
  died_count         - 断连计数
  state_*            - MAVROS State 消息字段
  pose_*             - 本地位姿字段
  vel_body_*         - 机体速度字段

==========================
调用关系
==========================
                    trajectory_data_collector.py
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  MavrosCommander │ │  MavrosMonitor   │ │     Worker       │
│  (MAVROS 服务)   │ │  (话题订阅)      │ │  (采集线程)      │
└────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
         │                    │                    │
         │ ROS2               │ ROS2               │ HTTP
         ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                    MAVROS + PX4                              │
│              /uav<id>/mavros/...                            │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│           rospy_isaacsim.py (控制器 端口: 5009+id)           │
│                      /reset, /command                       │
└──────────────────────────────┬──────────────────────────────┘
                               │ HTTP
                               ▼
┌─────────────────────────────────────────────────────────────┐
│            8_camera_vehicle.py (仿真端 端口: 8081)           │
│                    /uav/<id>/all, /uav/<id>/image           │
└─────────────────────────────────────────────────────────────┘

==========================
关键类说明
==========================
TrajPoint
  - 轨迹点数据类：x, y, z, roll_deg, yaw_deg, pitch_deg

MavrosCommander
  - MAVROS 服务客户端封装
  - 方法：
    - wait_connected(): 等待 MAVROS 连接
    - wait_ready_to_takeoff(): 等待 PX4 就绪（system_status=3, landed_state=1）
    - set_mode(): 设置飞行模式
    - arm(): 解锁/锁定
    - clear_mission(): 清除任务
    - push_mission(): 上传任务航点
    - wait_reached(): 等待到达航点
    - land_and_wait(): 降落并等待

MavrosMonitor
  - MAVROS 话题订阅器
  - 订阅：pose, state, velocity_body
  - 功能：高频采样记录飞行数据

Worker
  - 轨迹采集工作线程
  - 从共享队列获取任务
  - 执行完整的采集流程

==========================
环境变量
==========================
（无特殊环境变量，所有配置通过命令行参数传入）

==========================
使用示例
==========================
# 基础采集（单机）
ISAACSIM_PYTHON examples/trajectory_data_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --uav-ids 0

# 多机并行采集
ISAACSIM_PYTHON examples/trajectory_data_collector.py \
  --input-dir ~/trajectories \
  --out-dir ~/recordings \
  --config examples/multi_uav_config.json

# 指定缩放和轴向
ISAACSIM_PYTHON examples/trajectory_data_collector.py \
  --input-dir ~/trajectories \
  --scale 0.01 \
  --z-down \
  --max-points 100

# 不跳过已存在的轨迹（重新采集）
ISAACSIM_PYTHON examples/trajectory_data_collector.py \
  --input-dir ~/trajectories \
  --no-skip-existing

# 仅扫描文件（dry run）
ISAACSIM_PYTHON examples/trajectory_data_collector.py \
  --input-dir ~/trajectories \
  --dry-run

==========================
错误处理
==========================
1. MAVROS 连接失败
   - 超时 60s 未连接则抛出 TimeoutError
   - 检查 MAVROS 节点是否正常启动

2. 解锁失败
   - 最多重试 5 次
   - 检查 PX4 是否进入 Ready to takeoff 状态

3. 飞行中断连
   - MavrosMonitor 检测断连超过 5s
   - 标记 mavros_failed，跳过状态日志记录

4. 航点超时
   - 单个航点等待超过 cmd_timeout（默认 120s）
   - 抛出 TimeoutError，继续下一条轨迹

5. 图像获取失败
   - 重试 image_retries 次（默认 30 次）
   - 仍失败则抛出 RuntimeError

"""

import argparse
import atexit
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
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from geometry_msgs.msg import PoseStamped, TwistStamped, TwistWithCovarianceStamped, AccelWithCovarianceStamped
from mavros_msgs.msg import State, ExtendedState, Waypoint, WaypointReached
from mavros_msgs.srv import CommandBool, SetMode, CommandLong, WaypointPush, WaypointClear
from rosidl_runtime_py.convert import message_to_ordereddict
from datetime import datetime


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """
    生成带时间戳的日志消息并输出

    Args:
        prefix: 日志前缀（如 [MavrosCommander]）
        message: 日志内容
        level: 日志级别 (INFO, WARN, ERROR, DEBUG)

    Returns:
        格式化的日志字符串
    """
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
        获取时刻t的完整状态（位置、速度、姿态、角速度）

        Args:
            t: 时间（秒），范围[0, duration]
            for_control: 是否用于控制（True=用于发送setpoint，需要平滑速度；
                        False=用于数据记录，只有最终航点速度为0）

        Returns:
            字典包含:
            - x, y, z: 位置 (m)
            - vx, vy, vz: 速度 (m/s)
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
            # 控制模式：使用平滑速度进行前馈控制
            vx = float(self._spline_x(t, 1))
            vy = float(self._spline_y(t, 1))
            vz = float(self._spline_z(t, 1))
            roll_rate = float(self._spline_roll(t, 1))
            pitch_rate = float(self._spline_pitch(t, 1))
            yaw_rate = float(self._spline_yaw(t, 1))
        else:
            # 数据记录模式：只有最终航点速度为0，中间航点使用实际插值速度
            final_waypoint_threshold = 0.01  # 10ms阈值
            is_at_final_waypoint = (self.duration - t) < final_waypoint_threshold

            if is_at_final_waypoint:
                # 在最终航点，速度为0（悬停目标）
                vx, vy, vz = 0.0, 0.0, 0.0
                roll_rate, pitch_rate, yaw_rate = 0.0, 0.0, 0.0
            else:
                # 中间航点：使用实际插值速度
                vx = float(self._spline_x(t, 1))
                vy = float(self._spline_y(t, 1))
                vz = float(self._spline_z(t, 1))
                roll_rate = float(self._spline_roll(t, 1))
                pitch_rate = float(self._spline_pitch(t, 1))
                yaw_rate = float(self._spline_yaw(t, 1))

        return {
            'x': x, 'y': y, 'z': z,
            'vx': vx, 'vy': vy, 'vz': vz,
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
    ts_log("[Exception]", traceback.format_exc(), "WARN")


def _is_local_url(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception as e:
        # 异常处理：解析 URL 失败，打印堆栈并按非本地处理（可恢复）
        _log_exc("_is_local_url parse failed", e)
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
    files = sorted([Path(p) for p in glob.glob(str(input_dir / pattern))])
    return [p for p in files if p.is_file() and p.suffix.lower() == ".json"]


def _load_preprocessed_xyz(json_path: Path) -> List[TrajPoint]:
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs")
    if not isinstance(logs, list):
        raise ValueError("missing preprocessed_logs")
    # every 2 points(0.2s/pt)
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
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    raw_logs = obj.get("raw_logs", [])
    if not isinstance(raw_logs, list) or not raw_logs:
        raise ValueError("missing init point")
    init = raw_logs[0]
    if not isinstance(init, (list, tuple)) or len(init) < 3:
        raise ValueError("missing init point")
    try:
        ts_log("[InitPoint]", f"init point: {init}", "DEBUG")
        if len(init) < 6:
            raise ValueError(f"init point expects [x,y,z,roll,yaw,pitch], got len={len(init)}")
        x = float(init[0])
        y = float(init[1])
        z = float(init[2])
        roll = float(init[3])
        yaw = float(init[4])
        pitch = float(init[5])
        return TrajPoint(y, x, z, roll, yaw, pitch)  # ENU to NEU
    except Exception as e:
        raise ValueError(f"init point has invalid xyz: {e}") from e


def _load_uav_ids_from_config(config_path: Path) -> List[int]:
    obj = json.loads(config_path.read_text(encoding="utf-8"))
    vehicles = obj.get("vehicles") or []
    out: List[int] = []
    for v in vehicles:
        try:
            out.append(int(v.get("vehicle_id")))
        except Exception as e:
            # 异常处理：解析 vehicle_id 失败，打印堆栈并跳过该条配置（可恢复）
            _log_exc(f"_load_uav_ids_from_config invalid vehicle_id value={getattr(v, 'get', lambda *_: None)('vehicle_id')}", e)
            continue
    out = sorted(set(out))
    if not out:
        raise ValueError(f"no vehicle_id found in {config_path}")
    return out


def _decode_png_b64_to_file(b64: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(b64.encode("ascii"))
    out_path.write_bytes(data)


def _safe_name(path: Path) -> str:
    s = path.stem
    s2 = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", "."):
            s2.append(ch)
        else:
            s2.append("_")
    return "".join(s2) or "traj"


def _is_good_ulg(path: Path) -> bool:
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
        except Exception as e:
            # 异常处理：调用 ulog_info 失败，打印堆栈并按“有效”处理（可恢复）
            _log_exc(f"_is_good_ulg ulog_info failed path={path}", e)
            return True
        out = proc.stdout or ""
        for line in out.splitlines():
            t = line.strip()
            if t.startswith("duration:"):
                parts = t.split("duration:", 1)[1].strip().split()
                if parts:
                    if parts[0] == "0:00:00":
                        return False
                return True
        return True
    except Exception as e:
        # 异常处理：ULG 校验过程异常，打印堆栈并按“有效”处理（可恢复）
        _log_exc(f"_is_good_ulg unexpected failure path={path}", e)
        return True


def _find_latest_ulg(vehicle_id: int, since_ts: float) -> Optional[Path]:
    candidates: List[Tuple[float, Path]] = []
    prefix = f"/tmp/px4_{vehicle_id}_*"
    for p in glob.glob(prefix):
        d = Path(p)
        if not d.is_dir():
            continue
        try:
            for fp in d.rglob("*.ulg"):
                try:
                    st = fp.stat()
                except Exception as e:
                    # 异常处理：读取候选 ULG 文件信息失败，打印堆栈并跳过（可恢复）
                    _log_exc(f"_find_latest_ulg stat failed path={fp}", e)
                    continue
                if st.st_mtime < since_ts:
                    continue
                candidates.append((st.st_mtime, fp))
        except Exception as e:
            # 异常处理：遍历临时目录查找 ULG 失败，打印堆栈并继续（可恢复）
            _log_exc(f"_find_latest_ulg rglob failed dir={d}", e)
            continue
    search_roots: List[Path] = []
    search_roots.append(Path.cwd() / "px4_logs" / f"uav{vehicle_id}")
    examples_dir = Path(__file__).resolve().parent
    search_roots.append(examples_dir / "px4_logs" / f"uav{vehicle_id}")
    repo_root = Path(__file__).resolve().parent.parent
    search_roots.append(repo_root / "px4_logs" / f"uav{vehicle_id}")
    search_roots.append(repo_root / "examples" / "px4_logs" / f"uav{vehicle_id}")
    for base in search_roots:
        if not base.is_dir():
            continue
        try:
            for fp in base.rglob("*.ulg"):
                try:
                    st = fp.stat()
                except Exception as e:
                    # 异常处理：读取候选 ULG 文件信息失败，打印堆栈并跳过（可恢复）
                    _log_exc(f"_find_latest_ulg stat failed path={fp}", e)
                    continue
                if st.st_mtime < since_ts:
                    continue
                candidates.append((st.st_mtime, fp))
        except Exception as e:
            # 异常处理：遍历日志目录查找 ULG 失败，打印堆栈并继续（可恢复）
            _log_exc(f"_find_latest_ulg rglob failed dir={base}", e)
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    for _, fp in candidates:
        if _is_good_ulg(fp):
            return fp
    return candidates[0][1]


def _transform_points(
    pts: List[TrajPoint],
    scale: float,
    base_x: float,
    base_y: float,
    base_z: float,
    z_down: bool,
) -> List[TrajPoint]:
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


# 最小航点距离，略大于 MOVE_REACH_THRESHOLD (0.3m)
MIN_WAYPOINT_DIST = float(os.environ.get("PEGASUS_MIN_WAYPOINT_DIST", "0.35"))


def _filter_close_points(pts: List[TrajPoint], min_dist: float = MIN_WAYPOINT_DIST) -> List[TrajPoint]:
    """过滤掉距离过近的航点，保留第一个点和最后一个点"""
    if len(pts) <= 2:
        return pts

    filtered: List[TrajPoint] = [pts[0]]  # 始终保留第一个点

    for i in range(1, len(pts) - 1):
        last = filtered[-1]
        curr = pts[i]
        dx = curr.x - last.x
        dy = curr.y - last.y
        dz = curr.z - last.z
        dist = math.sqrt(dx * dx + dy * dy + dz * dz)
        if dist >= min_dist:
            filtered.append(curr)

    # 始终保留最后一个点
    if len(pts) > 1:
        filtered.append(pts[-1])

    return filtered


def _fetch_all_info(image_base: str, uav_id: int, timeout: float, retries: int) -> Dict[str, Any]:
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
    """Fetch depth image from simulation.

    Args:
        image_base: Base URL for image service (e.g., http://127.0.0.1:8081)
        uav_id: UAV ID
        timeout: Request timeout in seconds

    Returns:
        Dict with depth data (data, raw_float32_base64, timestamp, etc.) or None if failed
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
    url = f"{control_base}/reset"
    payload: Dict[str, Any] = {"vid": int(uav_id), "hard": bool(hard), "force": bool(force)}
    if position is not None:
        payload["position"] = [float(position[0]), float(position[1]), float(position[2])]
    if yaw_deg is not None:
        payload["yaw_deg"] = float(yaw_deg)
    code, obj = _http_json("POST", url, payload=payload, timeout=timeout)
    if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("status") != "success":
        raise RuntimeError(f"reset failed uav={uav_id} code={code} resp={obj}")
    deadline = time.time() + float(timeout)
    last_status: Dict[str, Any] = {}
    while time.time() < deadline:
        try:
            st = _controller_command(
                control_base,
                uav_id,
                {"cmd": "get_status"},
                timeout=min(5.0, float(timeout)),
            )
            last_status = st if isinstance(st, dict) else {}
            mode = str(((last_status.get("status") or {}).get("mode") or "")).upper()
            if mode == "OFFBOARD":
                return
        except Exception as e:
            last_status = {"error": str(e)}
        time.sleep(0.5)
    raise TimeoutError(f"reset ok but OFFBOARD not reached uav={uav_id} last_status={last_status}")


def _controller_command(
    control_base: str,
    uav_id: int,
    cmd: Dict[str, Any],
    timeout: float,
) -> Dict[str, Any]:
    url = f"{control_base}/command"
    code, obj = _http_json("POST", url, payload=cmd, timeout=timeout)
    if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("ok") is False:
        raise RuntimeError(f"command failed uav={uav_id} cmd={cmd} code={code} resp={obj}")
    return obj


def _write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
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
    try:
        return str(p.resolve())
    except Exception as e:
        # 异常处理：路径解析失败，打印堆栈并回退到 absolute（可恢复）
        _log_exc(f"_norm_abs_path resolve failed path={p}", e)
        return str(p.absolute())

_ULG_COPY_LOCK = threading.Lock()
_ULG_COPY_JOBS: List[Tuple[Path, Path]] = []


def _flush_ulg_copies() -> None:
    with _ULG_COPY_LOCK:
        jobs = list(_ULG_COPY_JOBS)
        _ULG_COPY_JOBS.clear()
    for src, dst in jobs:
        try:
            if not src.exists():
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        except Exception as e:
            # 异常处理：复制 ULG 失败，打印堆栈并继续处理下一个任务（可恢复）
            _log_exc(f"_flush_ulg_copies copy failed src={src} dst={dst}", e)
            continue


atexit.register(_flush_ulg_copies)


def _copy_ulg_now(src: Path, dst: Path) -> bool:
    try:
        if src is None or dst is None:
            return False
        if not src.exists():
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))
        return True
    except Exception:
        return False


def _flatten_msg(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}_{k}"
            _flatten_msg(key, v, out)
        return
    if isinstance(obj, (list, tuple)):
        for idx, v in enumerate(obj):
            key = f"{prefix}_{idx}"
            _flatten_msg(key, v, out)
        return
    out[prefix] = obj


class CollectorNode(Node):
    def __init__(self):
        super().__init__("trajectory_collector_node")


class MavrosCommander:
    def __init__(self, node: Node, uav_id: int):
        self.node = node
        self.uav_id = int(uav_id)
        self.prefix = f"/uav{self.uav_id}"
        self.state = None
        self.ext_state = None
        self.reached_seq = -1

        # Ready to takeoff 状态（实时更新）
        self._ready_to_takeoff = False
        self._ready_lock = threading.Lock()

        qos_profile = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
            durability=DurabilityPolicy.VOLATILE,
        )

        self.node.create_subscription(State, f"{self.prefix}/mavros/state", self._state_cb, qos_profile)
        self.node.create_subscription(ExtendedState, f"{self.prefix}/mavros/extended_state", self._ext_state_cb, qos_profile)
        self.node.create_subscription(WaypointReached, f"{self.prefix}/mavros/mission/reached", self._reached_cb, qos_profile)

        self.arming_client = self.node.create_client(CommandBool, f"{self.prefix}/mavros/cmd/arming")
        self.set_mode_client = self.node.create_client(SetMode, f"{self.prefix}/mavros/set_mode")
        self.cmdlong_client = self.node.create_client(CommandLong, f"{self.prefix}/mavros/cmd/command")
        self.mission_push_client = self.node.create_client(WaypointPush, f"{self.prefix}/mavros/mission/push")
        self.mission_clear_client = self.node.create_client(WaypointClear, f"{self.prefix}/mavros/mission/clear")

    def _update_ready_status(self) -> None:
        """根据当前状态更新 ready_to_takeoff"""
        st = self.state
        es = self.ext_state

        connected = bool(getattr(st, "connected", False)) if st is not None else False
        system_status = int(getattr(st, "system_status", 0)) if st is not None else 0
        landed_state = int(getattr(es, "landed_state", 0)) if es is not None else 0

        # MAV_STATE_STANDBY = 3, LANDED_STATE_ON_GROUND = 1
        ready = connected and system_status == 3 and landed_state == 1

        with self._ready_lock:
            self._ready_to_takeoff = ready

    @property
    def ready_to_takeoff(self) -> bool:
        """获取当前 ready to takeoff 状态"""
        with self._ready_lock:
            return self._ready_to_takeoff

    def get_ready_status(self) -> Dict[str, Any]:
        """获取详细的 ready 状态信息"""
        st = self.state
        es = self.ext_state

        connected = bool(getattr(st, "connected", False)) if st is not None else False
        system_status = int(getattr(st, "system_status", 0)) if st is not None else 0
        landed_state = int(getattr(es, "landed_state", 0)) if es is not None else 0
        armed = bool(getattr(st, "armed", False)) if st is not None else False
        mode = str(getattr(st, "mode", "")) if st is not None else ""

        return {
            "ready_to_takeoff": self.ready_to_takeoff,
            "connected": connected,
            "system_status": system_status,
            "landed_state": landed_state,
            "armed": armed,
            "mode": mode,
        }

    def _state_cb(self, msg):
        self.state = msg
        self._update_ready_status()

    def _ext_state_cb(self, msg):
        self.ext_state = msg
        self._update_ready_status()

    def _reached_cb(self, msg):
        self.reached_seq = int(getattr(msg, "wp_seq", -1))

    def wait_connected(self, timeout_s: float = 30.0) -> bool:
        deadline = time.time() + float(timeout_s)
        while time.time() < deadline:
            st = self.state
            if st is not None and bool(getattr(st, "connected", False)):
                return True
            time.sleep(0.1)
        return False

    def wait_ready_to_takeoff(self, timeout_s: float = 60.0, log_interval: float = 5.0) -> bool:
        """等待 PX4 进入 Ready to takeoff 状态

        检查条件:
        - state.connected == True (已连接)
        - state.system_status == 3 (MAV_STATE_STANDBY, 系统就绪)
        - extended_state.landed_state == 1 (ON_GROUND, 在地面)
        """
        deadline = time.time() + float(timeout_s)
        last_log_time = 0.0

        while time.time() < deadline:
            # 使用实时更新的属性
            if self.ready_to_takeoff:
                return True

            # 定期打印状态
            now = time.time()
            if now - last_log_time >= log_interval:
                status = self.get_ready_status()
                ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"waiting ready_to_takeoff: {status}")
                last_log_time = now

            time.sleep(0.2)

        status = self.get_ready_status()
        ts_log(f"[MavrosCommander UAV{self.uav_id}]", f"wait_ready_to_takeoff timeout, status={status}", "WARN")
        return False

    def _call_service(self, client, req, timeout_s: float, max_retries: int = 10):
        """调用ROS2服务，支持重试机制

        关键修复：增加重试次数和等待时间，处理MAVROS重启后服务不可用的情况
        """
        last_err: Optional[Exception] = None
        for attempt in range(max(1, max_retries)):
            # 等待服务可用，每次重试增加等待时间
            base_wait = min(10.0, float(timeout_s))
            # 前几次快速重试，后面增加等待时间
            wait_timeout = base_wait if attempt < 3 else base_wait + (attempt - 2) * 2.0

            if not client.wait_for_service(timeout_sec=wait_timeout):
                last_err = TimeoutError(f"service not available after {wait_timeout:.1f}s (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    # 等待时间递增：1s, 2s, 3s, ...
                    retry_delay = min(1.0 + attempt * 0.5, 5.0)
                    ts_log(f"[MavrosCommander UAV{self.uav_id}]",
                           f"service not available, retrying in {retry_delay:.1f}s...", "WARN")
                    time.sleep(retry_delay)
                continue

            try:
                fut = client.call_async(req)
                deadline = time.time() + float(timeout_s)
                while time.time() < deadline:
                    if fut.done():
                        return fut.result()
                    time.sleep(0.05)
                last_err = TimeoutError(f"service call timeout (attempt {attempt + 1}/{max_retries})")
            except Exception as e:
                last_err = e
                ts_log(f"[MavrosCommander UAV{self.uav_id}]",
                       f"service call exception: {e}, retrying...", "WARN")

            if attempt < max_retries - 1:
                retry_delay = min(1.0 + attempt * 0.5, 5.0)
                time.sleep(retry_delay)

        raise last_err if last_err else TimeoutError("service not available")

    def wait_mission_services_ready(self, timeout_s: float = 60.0) -> bool:
        """等待MAVROS mission服务就绪

        关键修复：在reset后需要等待MAVROS服务完全初始化，否则mission服务不可用
        """
        deadline = time.time() + float(timeout_s)
        services_to_check = [
            (self.mission_clear_client, "waypoint/clear"),
            (self.mission_push_client, "waypoint/push"),
            (self.set_mode_client, "set_mode"),
            (self.arming_client, "cmd/arming"),
        ]

        ts_log(f"[MavrosCommander UAV{self.uav_id}]", "Waiting for MAVROS services to be ready...")

        all_ready = False
        last_log_time = 0.0
        log_interval = 5.0

        while time.time() < deadline:
            ready_count = 0
            not_ready = []

            for client, name in services_to_check:
                if client.wait_for_service(timeout_sec=1.0):
                    ready_count += 1
                else:
                    not_ready.append(name)

            if ready_count == len(services_to_check):
                all_ready = True
                ts_log(f"[MavrosCommander UAV{self.uav_id}]",
                       f"All MAVROS services ready ({ready_count}/{len(services_to_check)})")
                break

            now = time.time()
            if now - last_log_time >= log_interval:
                ts_log(f"[MavrosCommander UAV{self.uav_id}]",
                       f"Waiting for services: {ready_count}/{len(services_to_check)} ready, "
                       f"not ready: {not_ready}", "WARN")
                last_log_time = now

            time.sleep(1.0)

        return all_ready

    def set_mode(self, mode: str, timeout_s: float = 10.0) -> None:
        req = SetMode.Request()
        req.custom_mode = str(mode)
        self._call_service(self.set_mode_client, req, timeout_s=timeout_s)

    def arm(self, arm: bool, timeout_s: float = 10.0) -> None:
        req = CommandBool.Request()
        req.value = bool(arm)
        self._call_service(self.arming_client, req, timeout_s=timeout_s)

    def command_long(self, command: int, params: List[float], timeout_s: float = 10.0) -> None:
        req = CommandLong.Request()
        req.command = int(command)
        req.confirmation = 0
        p = list(params)[:7] + [float("nan")] * 7
        req.param1 = float(p[0])
        req.param2 = float(p[1])
        req.param3 = float(p[2])
        req.param4 = float(p[3])
        req.param5 = float(p[4])
        req.param6 = float(p[5])
        req.param7 = float(p[6])
        self._call_service(self.cmdlong_client, req, timeout_s=timeout_s)

    def clear_mission(self, timeout_s: float = 10.0) -> None:
        req = WaypointClear.Request()
        self._call_service(self.mission_clear_client, req, timeout_s=timeout_s)

    def push_mission(self, waypoints: List[Waypoint], timeout_s: float = 30.0) -> None:
        req = WaypointPush.Request()
        req.start_index = 0
        req.waypoints = list(waypoints)
        resp = self._call_service(self.mission_push_client, req, timeout_s=timeout_s)
        if resp is None or not bool(getattr(resp, "success", False)):
            raise RuntimeError(f"mission push failed resp={resp}")

    def wait_reached(self, seq: int, timeout_s: float = 120.0) -> bool:
        deadline = time.time() + float(timeout_s)
        target = int(seq)
        while time.time() < deadline:
            if int(self.reached_seq) >= target:
                return True
            time.sleep(0.1)
        return False

    def wait_landed(self, timeout_s: float = 180.0) -> bool:
        deadline = time.time() + float(timeout_s)
        while time.time() < deadline:
            st = self.state
            es = self.ext_state
            armed = bool(getattr(st, "armed", False)) if st is not None else True
            landed_state = int(getattr(es, "landed_state", 0)) if es is not None else 0
            if (not armed) and landed_state != 0:
                return True
            time.sleep(0.2)
        return False

    def land_and_wait(self, timeout_s: float = 180.0) -> bool:
        self.set_mode("AUTO.LAND", timeout_s=10.0)
        self.command_long(21, [float("nan")] * 7, timeout_s=5.0)

        deadline = time.time() + float(timeout_s)
        while time.time() < deadline:
            st = self.state
            es = self.ext_state
            armed = bool(getattr(st, "armed", False)) if st is not None else True
            landed_state = int(getattr(es, "landed_state", 0)) if es is not None else 0
            if (not armed) and landed_state != 0:
                return True
            time.sleep(0.2)
        return False


class MavrosMonitor:
    def __init__(self, node: Node, uav_id: int, freq: float = 20.0):
        self.node = node
        self.uav_id = uav_id
        self.prefix = f"/uav{uav_id}"
        self.latest_pose = None
        self.latest_state = None
        self.latest_vel_body = None
        
        self.recording = False
        self.data_buffer = []
        self.lock = threading.Lock()
        
        self.died_count = 0
        self.max_died_count = 40
        self.max_died_seconds = 5.0
        self._died_since: Optional[float] = None
        self.failed_due_to_death = False
        
        qos_profile = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
            durability=DurabilityPolicy.VOLATILE
        )
        
        self.node.create_subscription(
            PoseStamped,
            f"{self.prefix}/mavros/local_position/pose",
            self._pose_cb,
            qos_profile
        )
        self.node.create_subscription(
            State,
            f"{self.prefix}/mavros/state",
            self._state_cb,
            qos_profile
        )
        # self.node.create_subscription(
        #     TwistWithCovarianceStamped,
        #     f"{self.prefix}/mavros/local_position/velocity_body_cov",
        #     self._vel_cov_cb,
        #     qos_profile
        # )
        self.node.create_subscription(
            TwistStamped,
            f"{self.prefix}/mavros/local_position/velocity_body",
            self._vel_body_cb,
            qos_profile
        )
        # self.node.create_subscription(
        #     AccelWithCovarianceStamped,
        #     f"{self.prefix}/mavros/local_position/accel",
        #     self._accel_cb,
        #     qos_profile
        # )
        
        self.node.create_timer(1.0 / freq, self._timer_cb)

    def _pose_cb(self, msg):
        self.latest_pose = msg

    def _state_cb(self, msg):
        self.latest_state = msg

    def _vel_body_cb(self, msg):
        self.latest_vel_body = msg

    def _timer_cb(self):
        if not self.recording:
            return
        
        alive = False
        if self.latest_state and self.latest_state.connected:
            alive = True
            
        if not alive:
            self.died_count += 1
            if self._died_since is None:
                self._died_since = time.time()
        else:
            self.died_count = 0
            self._died_since = None
            
        died_for = (time.time() - self._died_since) if self._died_since is not None else 0.0
        if self.died_count > self.max_died_count or died_for > self.max_died_seconds:
            self.failed_due_to_death = True

        sample: Dict[str, Any] = {}
        sample["sample_idx"] = len(self.data_buffer)
        sample["host_time"] = time.time()
        sample["mavros_connected"] = alive
        sample["died_count"] = self.died_count

        if self.latest_state is not None:
            state_dict = message_to_ordereddict(self.latest_state)
            _flatten_msg("state", state_dict, sample)

        if self.latest_pose is not None:
            pose_dict = message_to_ordereddict(self.latest_pose)
            _flatten_msg("pose", pose_dict, sample)

        if self.latest_vel_body is not None:
            vel_dict = message_to_ordereddict(self.latest_vel_body)
            _flatten_msg("vel_body", vel_dict, sample)

        with self.lock:
            self.data_buffer.append(sample)

    def start_recording(self):
        with self.lock:
            self.data_buffer = []
            self.died_count = 0
            self._died_since = None
            self.failed_due_to_death = False
            self.recording = True
        ts_log(f"[MavrosMonitor UAV{self.uav_id}]", "Started recording")

    def stop_recording(self):
        self.recording = False
        ts_log(f"[MavrosMonitor UAV{self.uav_id}]", f"Stopped recording, buffer_size={len(self.data_buffer)}")
        with self.lock:
            return list(self.data_buffer), self.failed_due_to_death


class Worker:
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
        monitor: Optional[MavrosMonitor],
        commander: Optional[MavrosCommander],
        status_log_path: Path,
        use_mission_mode: bool = False,
        use_velocity_offboard: bool = False,
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
        self.monitor = monitor
        self.commander = commander
        self.status_log_path = status_log_path
        self.use_mission_mode = bool(use_mission_mode)
        self.use_velocity_offboard = bool(use_velocity_offboard)

        # 坐标对齐相关
        self._origin_offset: Optional[Tuple[float, float, float]] = None

    def _log(self, msg: str, level: str = "INFO") -> None:
        with self.print_lock:
            ts_log(f"[Worker UAV{self.uav_id}]", msg, level)

    def _calculate_origin_offset(self, cmd_pos: Tuple[float, float, float], obs_pos: Tuple[float, float, float]) -> Tuple[float, float, float]:
        """
        计算命令坐标系和观测坐标系之间的原点偏移量

        offset = obs - cmd，应用时 aligned_obs = obs - offset
        """
        return (
            obs_pos[0] - cmd_pos[0],
            obs_pos[1] - cmd_pos[1],
            obs_pos[2] - cmd_pos[2],
        )

    def _apply_alignment(self, obs_pos: Tuple[float, float, float]) -> Tuple[float, float, float]:
        """
        将观测坐标对齐到命令坐标系
        """
        if self._origin_offset is None:
            return obs_pos
        return (
            obs_pos[0] - self._origin_offset[0],
            obs_pos[1] - self._origin_offset[1],
            obs_pos[2] - self._origin_offset[2],
        )

    def _execute_mission_mode(self, pts: List[TrajPoint], traj_name: str) -> bool:
        """
        使用Mission模式执行轨迹

        通过控制器HTTP接口发送execute_mission命令，上传所有航点后等待执行完成

        Returns:
            True if mission completed successfully, False otherwise
        """
        self._log(f"[UAV{self.uav_id}] executing mission mode with {len(pts)} waypoints")

        # 构建航点列表
        waypoints = []
        for i, p in enumerate(pts):
            waypoints.append({
                "x": float(p.x),
                "y": float(p.y),
                "z": float(p.z),
                "yaw_deg": float(p.yaw_deg) if p.yaw_deg is not None else 0.0,
            })

        try:
            # 发送execute_mission命令到控制器
            cmd = {
                "cmd": "execute_mission",
                "waypoints": waypoints,
                "acceptance_radius": 0.5,  # 航点接受半径
                "cruise_speed": 2.0,  # 巡航速度
            }
            resp = _controller_command(self.control_base, self.uav_id, cmd, timeout=self.cmd_timeout)

            if not isinstance(resp, dict) or resp.get("ok") is False:
                self._log(f"[UAV{self.uav_id}] execute_mission failed: {resp}", "WARN")
                return False

            self._log(f"[UAV{self.uav_id}] mission started, waiting for completion...")
            return True

        except Exception as e:
            self._log(f"[UAV{self.uav_id}] execute_mission exception: {e}", "WARN")
            return False

    def _wait_mission_waypoint(self, waypoint_idx: int, timeout: float = 60.0) -> bool:
        """
        等待到达指定航点

        通过控制器HTTP接口查询当前mission状态
        """
        deadline = time.time() + float(timeout)
        last_log_time = 0.0
        log_interval = 5.0

        while time.time() < deadline:
            try:
                cmd = {"cmd": "get_mission_status"}
                resp = _controller_command(self.control_base, self.uav_id, cmd, timeout=5.0)

                if isinstance(resp, dict):
                    current_wp = resp.get("current_waypoint", -1)
                    mission_state = resp.get("mission_state", "unknown")

                    # 如果已经过了这个航点
                    if current_wp > waypoint_idx:
                        return True

                    # 如果任务完成
                    if mission_state in ("completed", "finished"):
                        return True

                    now = time.time()
                    if now - last_log_time >= log_interval:
                        self._log(f"[UAV{self.uav_id}] waiting for waypoint {waypoint_idx}, current={current_wp}, state={mission_state}")
                        last_log_time = now

            except Exception as e:
                self._log(f"[UAV{self.uav_id}] get_mission_status failed: {e}", "WARN")

            time.sleep(0.5)

        return False

    def _process_velocity_offboard_mode(
        self,
        json_path: Path,
        transformed_pts: List[TrajPoint],
        traj_dir: Path,
        rows: List[Dict],
        all_pose_rows: List[Dict],
    ) -> None:
        """
        速度前馈OFFBOARD模式：基于时间的平滑跟踪控制

        架构：
        [5Hz人工数据] -> [Cubic Spline插值] -> [50Hz位置+速度前馈控制] -> [20Hz数据采样]

        与mavlink_trajectory_collector.py的_process_offboard_mode保持一致
        """
        if not HAS_SCIPY:
            raise RuntimeError("TrajectorySmoother requires scipy. Install with: pip install numpy scipy")

        if len(transformed_pts) < 2:
            self._log(f"[UAV{self.uav_id}] trajectory too short (<2 points)", "WARN")
            return

        self._log(f"[UAV{self.uav_id}] starting SMOOTH velocity tracking control ({len(transformed_pts)} points, {(len(transformed_pts)-1)*0.2:.1f}s)")

        # 创建平滑器（使用变换后的轨迹点）
        smoother = TrajectorySmoother(transformed_pts, dt=0.2)
        total_duration = smoother.duration

        # 获取初始位姿
        try:
            info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=5)
            init_pose = info.get("pose", {})
            uav_init_pos = init_pose.get("position", [0, 0, 0])
            sim_start_ts = float(init_pose.get("timestamp", time.time()))
        except Exception:
            uav_init_pos = [0, 0, 0]
            sim_start_ts = time.time()

        self._log(f"[UAV{self.uav_id}] initial sim timestamp: {sim_start_ts:.3f}")

        # 计算轨迹偏移（仅对齐XY，Z使用绝对轨迹高度）
        traj_start = smoother.get_position(0.0)
        self._origin_offset = (
            uav_init_pos[0] - traj_start[0],
            uav_init_pos[1] - traj_start[1],
            0.0  # Z不偏移：使用轨迹的绝对高度
        )
        self._log(f"[UAV{self.uav_id}] trajectory offset: ({self._origin_offset[0]:.4f}, {self._origin_offset[1]:.4f}, {self._origin_offset[2]:.4f})m")

        # 辅助函数：应用偏移到位置
        def apply_offset(x, y, z):
            return (
                x + self._origin_offset[0],
                y + self._origin_offset[1],
                z  # Z使用原始轨迹高度
            )

        aligned_start = apply_offset(traj_start[0], traj_start[1], traj_start[2])

        # === 检查当前高度并起飞 ===
        TARGET_ALT = traj_start[2]
        ALT_TOLERANCE = 0.3

        try:
            info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=3)
            current_pos = info.get("pose", {}).get("position", [0, 0, 0])
            current_alt = current_pos[2]
            self._log(f"[UAV{self.uav_id}] current altitude: {current_alt:.2f}m, target: {TARGET_ALT:.2f}m")
        except Exception as e:
            self._log(f"[UAV{self.uav_id}] failed to check altitude: {e}", "WARN")
            current_alt = 0.0

        # 如果高度不足，执行起飞
        if current_alt < TARGET_ALT - ALT_TOLERANCE:
            self._log(f"[UAV{self.uav_id}] altitude too low, starting takeoff to {TARGET_ALT:.1f}m...")

            # 发送setpoints准备OFFBOARD
            for _ in range(50):
                cmd = {
                    "cmd": "setpoint",
                    "x": aligned_start[0], "y": aligned_start[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.5,
                    "yaw": 0.0, "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass
                time.sleep(0.02)

            # ARM
            for attempt in range(5):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "arm"}, timeout=10.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        self._log(f"[UAV{self.uav_id}] armed successfully")
                        break
                except Exception as e:
                    self._log(f"[UAV{self.uav_id}] ARM attempt {attempt+1} failed: {e}", "WARN")
                time.sleep(0.3)

            # OFFBOARD模式
            for attempt in range(3):
                try:
                    code, resp = _http_json("POST", f"{self.control_base}/command",
                                            payload={"cmd": "set_mode", "mode": "OFFBOARD"}, timeout=5.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        self._log(f"[UAV{self.uav_id}] OFFBOARD mode set")
                        break
                except Exception:
                    pass
                time.sleep(0.3)

            # 爬升
            self._log(f"[UAV{self.uav_id}] climbing to {TARGET_ALT:.1f}m...")
            takeoff_start = time.time()
            takeoff_timeout = 15.0

            while time.time() - takeoff_start < takeoff_timeout:
                cmd = {
                    "cmd": "setpoint",
                    "x": aligned_start[0], "y": aligned_start[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.3,
                    "yaw": smoother.get_yaw_and_rate(0.0)[0],
                    "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass

                if int((time.time() - takeoff_start) / 0.5) > int((time.time() - takeoff_start - 0.02) / 0.5):
                    try:
                        info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=2)
                        current_alt = info.get("pose", {}).get("position", [0, 0, 0])[2]
                        if current_alt >= TARGET_ALT - ALT_TOLERANCE:
                            self._log(f"[UAV{self.uav_id}] reached target altitude!")
                            break
                    except Exception:
                        pass
                time.sleep(0.02)

            # 稳定
            for _ in range(50):
                cmd = {
                    "cmd": "setpoint",
                    "x": aligned_start[0], "y": aligned_start[1], "z": TARGET_ALT,
                    "vx": 0.0, "vy": 0.0, "vz": 0.0,
                    "yaw": smoother.get_yaw_and_rate(0.0)[0],
                    "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass
                time.sleep(0.02)

        # 重新获取位置并更新偏移
        try:
            info = _fetch_all_info(self.image_base, self.uav_id, timeout=self.image_timeout, retries=3)
            current_pose = info.get("pose", {})
            current_pos = current_pose.get("position", [0, 0, 0])
            sim_start_ts = float(current_pose.get("timestamp", time.time()))

            old_offset = self._origin_offset
            self._origin_offset = (
                current_pos[0] - traj_start[0],
                current_pos[1] - traj_start[1],
                0.0
            )
            self._log(f"[UAV{self.uav_id}] updated trajectory offset: ({self._origin_offset[0]:.4f}, {self._origin_offset[1]:.4f})m")
            aligned_start = apply_offset(traj_start[0], traj_start[1], traj_start[2])
        except Exception:
            sim_start_ts = time.time()

        # 控制循环50Hz，数据采集20Hz
        CONTROL_HZ = 50.0
        SAMPLE_HZ = 20.0
        control_dt = 1.0 / CONTROL_HZ
        sample_dt = 1.0 / SAMPLE_HZ

        sample_idx = 0
        max_samples = int(total_duration / sample_dt) + 1
        next_sample_time = 0.0

        # 时间追踪
        last_sim_ts = sim_start_ts
        last_wall_time = time.time()
        sim_speed_factor = 1.0

        # 终点判定
        FINAL_DIST_THRESHOLD = 0.3
        FINAL_VEL_THRESHOLD = 0.1
        in_final_mode = False
        final_mode_start = None

        # 当前观测状态
        current_obs_pos = list(aligned_start)
        current_obs_vel = [0, 0, 0]

        # 误差统计
        error_sum = 0.0
        error_count = 0

        obs_fetch_interval = 2
        loop_counter = 0

        self._log(f"[UAV{self.uav_id}] starting 50Hz control+sampling loop (duration={total_duration:.1f}s)")

        while True:
            loop_start = time.time()
            loop_counter += 1

            # 获取仿真时间戳
            if loop_counter % obs_fetch_interval == 0:
                try:
                    info = _fetch_all_info(self.image_base, self.uav_id, timeout=0.1, retries=1)
                    pose = info.get("pose", {})
                    new_sim_ts = float(pose.get("timestamp", 0))
                    new_pos = pose.get("position", None)
                    new_vel = pose.get("linear_velocity", None)

                    if new_sim_ts > 0:
                        last_sim_ts = new_sim_ts
                        last_wall_time = time.time()
                    if new_pos is not None and len(new_pos) >= 3:
                        current_obs_pos = new_pos
                    if new_vel is not None and len(new_vel) >= 3:
                        current_obs_vel = new_vel
                except Exception:
                    pass

            wall_elapsed = time.time() - last_wall_time
            current_sim_ts = last_sim_ts + wall_elapsed * sim_speed_factor
            elapsed = current_sim_ts - sim_start_ts

            # 终点状态
            end_state = smoother.get_full_state(total_duration)
            final_pos = apply_offset(end_state['x'], end_state['y'], end_state['z'])

            # 终点判定
            if elapsed > total_duration:
                if not in_final_mode:
                    in_final_mode = True
                    final_mode_start = time.time()
                    self._log(f"[UAV{self.uav_id}] entering FINAL mode")

                dist_to_final = math.sqrt(
                    (current_obs_pos[0] - final_pos[0])**2 +
                    (current_obs_pos[1] - final_pos[1])**2 +
                    (current_obs_pos[2] - final_pos[2])**2
                )
                current_speed = math.sqrt(
                    current_obs_vel[0]**2 + current_obs_vel[1]**2 + current_obs_vel[2]**2
                )

                cmd = {
                    "cmd": "setpoint",
                    "x": final_pos[0], "y": final_pos[1], "z": final_pos[2],
                    "vx": 0.0, "vy": 0.0, "vz": 0.0,
                    "yaw": end_state['yaw'],
                    "yaw_rate": 0.0
                }
                try:
                    _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
                except Exception:
                    pass

                if dist_to_final < FINAL_DIST_THRESHOLD and current_speed < FINAL_VEL_THRESHOLD:
                    self._log(f"[UAV{self.uav_id}] FINAL reached: dist={dist_to_final:.3f}m")
                    break

                if time.time() - final_mode_start > 10.0:
                    self._log(f"[UAV{self.uav_id}] FINAL timeout", "WARN")
                    break

                time.sleep(control_dt)
                continue

            # 正常轨迹跟踪
            t = min(elapsed, total_duration)
            state = smoother.get_full_state(t)
            aligned_pos = apply_offset(state['x'], state['y'], state['z'])

            cmd = {
                "cmd": "setpoint",
                "x": aligned_pos[0], "y": aligned_pos[1], "z": aligned_pos[2],
                "vx": state['vx'], "vy": state['vy'], "vz": state['vz'],
                "yaw": state['yaw'],
                "yaw_rate": state['yaw_rate']
            }
            try:
                _http_json("POST", f"{self.control_base}/command", payload=cmd, timeout=0.5)
            except Exception:
                pass

            # 计算跟踪误差
            tracking_error = math.sqrt(
                (current_obs_pos[0] - aligned_pos[0])**2 +
                (current_obs_pos[1] - aligned_pos[1])**2 +
                (current_obs_pos[2] - aligned_pos[2])**2
            )
            error_sum += tracking_error
            error_count += 1

            if sample_idx % 25 == 0:
                self._log(f"[UAV{self.uav_id}] t={t:.2f}s Error={tracking_error:.3f}m")

            # 数据采样
            if elapsed >= next_sample_time and sample_idx < max_samples:
                sample_t = sample_idx * sample_dt
                sample_state = smoother.get_full_state(sample_t, for_control=False)
                sample_pos = apply_offset(sample_state['x'], sample_state['y'], sample_state['z'])

                p_in = TrajPoint(sample_state['x'], sample_state['y'], sample_state['z'],
                                 math.degrees(sample_state['roll']),
                                 math.degrees(sample_state['yaw']),
                                 math.degrees(sample_state['pitch']))
                p_cmd = TrajPoint(sample_pos[0], sample_pos[1], sample_pos[2],
                                  math.degrees(sample_state['roll']),
                                  math.degrees(sample_state['yaw']),
                                  math.degrees(sample_state['pitch']))
                cmd_velocity = {
                    'vx': sample_state['vx'],
                    'vy': sample_state['vy'],
                    'vz': sample_state['vz'],
                    'yaw': sample_state['yaw'],
                    'yaw_rate': sample_state['yaw_rate']
                }

                try:
                    self._collect_data_point(sample_idx, p_in, p_cmd, json_path, traj_dir, rows, all_pose_rows, cmd_velocity)
                except Exception as e:
                    self._log(f"[UAV{self.uav_id}] sample {sample_idx} error: {e}", "WARN")

                sample_idx += 1
                next_sample_time = sample_idx * sample_dt

            # 控制循环节拍
            loop_elapsed = time.time() - loop_start
            sleep_time = control_dt - loop_elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        avg_error = error_sum / max(error_count, 1)
        self._log(f"[UAV{self.uav_id}] tracking complete: {sample_idx} samples, avg_error={avg_error:.3f}m")

    def _collect_data_point(
        self,
        i: int,
        p_in: TrajPoint,
        p_cmd: TrajPoint,
        json_path: Path,
        traj_dir: Path,
        rows: List[Dict],
        all_pose_rows: List[Dict],
        cmd_velocity: Optional[Dict[str, float]] = None,
    ) -> None:
        """采集单个数据点（与mavlink_trajectory_collector.py兼容）"""
        traj_name = _safe_name(json_path)

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

        # Fetch and save depth image
        depth_name = f"depth_{i:06d}_{ts_ms}.png"
        depth_path = traj_dir / "depths" / depth_name
        try:
            depth_info = _fetch_depth(self.image_base, self.uav_id, timeout=self.image_timeout)
            if depth_info and depth_info.get("data"):
                _decode_png_b64_to_file(depth_info["data"], depth_path)
        except Exception:
            pass  # Depth capture is optional, don't fail on errors

        pose = info.get("pose") or {}
        pos = (pose.get("position") or [None, None, None])[:3]
        att = (pose.get("attitude") or [None, None, None, None])[:4]
        lv = (pose.get("linear_velocity") or [None, None, None])[:3]
        av = (pose.get("angular_velocity") or [None, None, None])[:3]
        la = (pose.get("linear_acceleration") or [None, None, None])[:3]

        obs_pos = (
            float(pos[0]) if pos[0] is not None else 0.0,
            float(pos[1]) if pos[1] is not None else 0.0,
            float(pos[2]) if pos[2] is not None else 0.0,
        )
        cmd_pos = (float(p_cmd.x), float(p_cmd.y), float(p_cmd.z))

        if i == 0 and self._origin_offset is None:
            self._origin_offset = self._calculate_origin_offset(cmd_pos, obs_pos)

        aligned_obs = self._apply_alignment(obs_pos)

        rows.append({
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
            # 速度前馈控制输入
            "cmd_vx": cmd_velocity.get('vx', 0.0) if cmd_velocity else 0.0,
            "cmd_vy": cmd_velocity.get('vy', 0.0) if cmd_velocity else 0.0,
            "cmd_vz": cmd_velocity.get('vz', 0.0) if cmd_velocity else 0.0,
            "cmd_yaw_rad": cmd_velocity.get('yaw', 0.0) if cmd_velocity else 0.0,
            "cmd_yaw_rate": cmd_velocity.get('yaw_rate', 0.0) if cmd_velocity else 0.0,
            "ulg_path": "",
        })

        all_pose_rows.append({
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
        })

    def _notify_controller_ready(self, ready: bool) -> bool:
        """通知控制器 PX4 ready 状态"""
        try:
            cmd = {"cmd": "set_px4_ready", "ready": bool(ready)}
            _controller_command(self.control_base, self.uav_id, cmd, timeout=5.0)
            self._log(f"[UAV{self.uav_id}] notified controller px4_ready={ready}")
            return True
        except Exception as e:
            self._log(f"[UAV{self.uav_id}] failed to notify controller px4_ready: {e}")
            return False

    def _get_sim_px4_ready(self, timeout: float) -> Tuple[bool, Dict[str, Any]]:
        url = f"{self.image_base}/uav/{int(self.uav_id)}/px4/ready"
        code, obj = _http_json("GET", url, payload=None, timeout=float(timeout))
        ready = False
        if isinstance(obj, dict):
            ready = bool(obj.get("ready", False))
        return (200 <= int(code) < 300) and ready, (obj if isinstance(obj, dict) else {"raw": obj})

    def _reset_and_wait_ready(
        self,
        position: Optional[List[float]],
        yaw_deg: Optional[float],
        timeout: float,
    ) -> None:
        """执行 reset 并同时等待 OFFBOARD 模式和 PX4 ready to takeoff 状态"""
        # 发送 reset HTTP 请求
        url = f"{self.control_base}/reset"
        payload: Dict[str, Any] = {"vid": int(self.uav_id), "hard": True, "force": True}
        if position is not None:
            payload["position"] = [float(position[0]), float(position[1]), float(position[2])]
        if yaw_deg is not None:
            payload["yaw_deg"] = float(yaw_deg)

        code, obj = _http_json("POST", url, payload=payload, timeout=timeout)
        if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("status") != "success":
            raise RuntimeError(f"reset failed uav={self.uav_id} code={code} resp={obj}")

        self._log(f"[UAV{self.uav_id}] reset HTTP returned, waiting for OFFBOARD + PX4 ready...")

        deadline = time.time() + float(timeout)
        last_status: Dict[str, Any] = {}
        offboard_reached = False
        px4_ready_notified = False
        last_px4_ready: Dict[str, Any] = {}
        log_interval = 5.0
        last_log_time = 0.0

        while time.time() < deadline:
            now = time.time()

            # 检查 OFFBOARD 模式
            if not offboard_reached:
                try:
                    st = _controller_command(
                        self.control_base,
                        self.uav_id,
                        {"cmd": "get_status"},
                        timeout=min(5.0, float(timeout)),
                    )
                    last_status = st if isinstance(st, dict) else {}
                    mode = str(((last_status.get("status") or {}).get("mode") or "")).upper()
                    if mode == "OFFBOARD":
                        offboard_reached = True
                        self._log(f"[UAV{self.uav_id}] OFFBOARD mode reached")
                except Exception as e:
                    last_status = {"error": str(e)}

            # 检查 PX4 ready 状态并通知控制器
            if not px4_ready_notified:
                try:
                    ready, resp = self._get_sim_px4_ready(timeout=min(5.0, float(timeout)))
                    last_px4_ready = resp if isinstance(resp, dict) else {"raw": resp}
                    if ready:
                        px4_ready_notified = True
                        self._log(f"[UAV{self.uav_id}] PX4 ready(/uav/<id>/px4/ready) detected")
                        self._notify_controller_ready(True)
                except Exception as e:
                    last_px4_ready = {"error": str(e)}

            # 两者都满足则返回
            if offboard_reached and px4_ready_notified:
                self._log(f"[UAV{self.uav_id}] reset complete: OFFBOARD + PX4 ready")
                return

            # 定期打印状态
            if now - last_log_time >= log_interval:
                self._log(
                    f"[UAV{self.uav_id}] waiting: offboard={offboard_reached}, "
                    f"px4_ready={px4_ready_notified}, px4_ready_http={last_px4_ready}"
                )
                last_log_time = now

            time.sleep(0.3)

        # 超时处理
        raise TimeoutError(
            f"reset timeout uav={self.uav_id}: offboard={offboard_reached}, "
            f"px4_ready={px4_ready_notified}, last_status={last_status}, px4_ready_http={last_px4_ready}"
        )

    def _append_status_log(self, traj_name: str, ulg_path: str) -> None:
        try:
            row = {
                "traj_name": traj_name,
                "uav_id": self.uav_id,
                "timestamp": time.time(),
                "ulg_path": ulg_path,
                "status": "success"
            }
            # We use print_lock to synchronize file writes as well
            with self.print_lock:
                exists = self.status_log_path.exists()
                with self.status_log_path.open("a", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=["traj_name", "uav_id", "timestamp", "ulg_path", "status"])
                    if not exists:
                        w.writeheader()
                    w.writerow(row)
        except Exception as e:
            self._log(f"failed to append status log: {e}")

    def run(self) -> None:
        while True:
            try:
                json_path = self.task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._process_one(json_path)
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] traj failed json={json_path} err={e}")
            finally:
                self.task_queue.task_done()

    def _process_one(self, json_path: Path) -> None:
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

        # 过滤距离过近的航点，避免在航点处打转
        pts_before = len(pts)
        pts = _filter_close_points(pts, MIN_WAYPOINT_DIST)
        if len(pts) < pts_before:
            self._log(f"[UAV{self.uav_id}] filtered {pts_before - len(pts)} close waypoints (min_dist={MIN_WAYPOINT_DIST:.2f}m), {len(pts)} remaining")

        if int(self.max_points) > 0:
            raw_pts = raw_pts[: int(self.max_points)]
            pts = pts[: int(self.max_points)]
            if not pts:
                raise ValueError(f"max_points={self.max_points} results in empty trajectory")

        self._log(f"[UAV{self.uav_id}] reset(hard=True) before traj={traj_name}")

        # MAVROS 连接检查（可选，HTTP命令不依赖MAVROS）
        mavros_ok = False
        if self.commander is not None:
            try:
                mavros_ok = self.commander.wait_connected(timeout_s=30.0)
                if mavros_ok:
                    self._log(f"[UAV{self.uav_id}] MAVROS connected")
                else:
                    self._log(f"[UAV{self.uav_id}] MAVROS not connected, proceeding with HTTP commands", "WARN")
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] MAVROS check failed: {e}, proceeding with HTTP commands", "WARN")

        # 执行 reset 并同时等待 OFFBOARD + PX4 ready
        self._reset_and_wait_ready(
            position=[init_pos.x * self.scale, init_pos.y * self.scale, init_pos.z * self.scale],
            yaw_deg=None,
            timeout=self.reset_timeout,
        )

        # 使用HTTP命令，不再需要等待MAVROS mission服务
        # MAVROS服务仅用于状态监控（可选）
        self._log(f"[UAV{self.uav_id}] using HTTP commands, skipping MAVROS service check")

        traj_start_ts = time.time()
        self._log(f"[UAV{self.uav_id}] start traj={traj_name} points={len(pts)}")

        if self.monitor:
            self.monitor.start_recording()

        rows: List[Dict[str, Any]] = []
        all_pose_rows: List[Dict[str, Any]] = []

        # 重置坐标对齐偏移量
        self._origin_offset = None

        try:
            # 根据模式选择执行方式
            if self.use_velocity_offboard:
                # 速度前馈OFFBOARD模式：使用平滑轨迹跟踪控制
                self._log(f"[UAV{self.uav_id}] starting VELOCITY OFFBOARD mode ({len(pts)} points)")
                self._process_velocity_offboard_mode(json_path, pts, traj_dir, rows, all_pose_rows)

            elif self.use_mission_mode:
                # Mission模式：一次性上传所有航点，PX4自动执行
                self._log(f"[UAV{self.uav_id}] starting MISSION mode waypoint navigation ({len(pts)} points)")
                mission_started = self._execute_mission_mode(pts, traj_name)
                if not mission_started:
                    self._log(f"[UAV{self.uav_id}] mission mode failed, falling back to OFFBOARD mode", "WARN")
                    # 回退到OFFBOARD模式
                    self.use_mission_mode = False

            # 仅当不使用速度前馈模式时执行标准航点循环
            if not self.use_velocity_offboard:
                if not self.use_mission_mode:
                    # OFFBOARD模式：逐个航点发送move_to命令
                    self._log(f"[UAV{self.uav_id}] starting OFFBOARD waypoint navigation ({len(pts)} points)")

                for i, (p_in, p_cmd) in enumerate(zip(raw_pts, pts)):
                    if self.use_mission_mode:
                        # Mission模式：等待到达当前航点
                        self._log(f"[UAV{self.uav_id}] waiting for mission waypoint {i}/{len(pts)-1}")
                        if not self._wait_mission_waypoint(i, timeout=self.cmd_timeout):
                            self._log(f"[UAV{self.uav_id}] waypoint {i} timeout, continuing...", "WARN")
                    else:
                        # OFFBOARD模式：发送move_to命令到控制器
                        self._log(f"[UAV{self.uav_id}] moving to waypoint {i}/{len(pts)-1}: ({p_cmd.x:.2f}, {p_cmd.y:.2f}, {p_cmd.z:.2f})")

                        try:
                            cmd = {"cmd": "move_to", "x": float(p_cmd.x), "y": float(p_cmd.y), "z": float(p_cmd.z), "force": True}
                            resp = _controller_command(self.control_base, self.uav_id, cmd, timeout=self.cmd_timeout)
                            if not isinstance(resp, dict) or resp.get("ok") is False:
                                raise RuntimeError(f"move_to failed: {resp}")
                        except Exception as e:
                            self._log(f"[UAV{self.uav_id}] waypoint {i} move_to failed: {e}", "WARN")
                            raise

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

                    # Fetch and save depth image
                    depth_name = f"depth_{i:06d}_{ts_ms}.png"
                    depth_path = traj_dir / "depths" / depth_name
                    try:
                        depth_info = _fetch_depth(self.image_base, self.uav_id, timeout=self.image_timeout)
                        if depth_info and depth_info.get("data"):
                            _decode_png_b64_to_file(depth_info["data"], depth_path)
                    except Exception:
                        pass  # Depth capture is optional

                    pose = info.get("pose") or {}
                    pos = (pose.get("position") or [None, None, None])[:3]
                    att = (pose.get("attitude") or [None, None, None, None])[:4]
                    lv = (pose.get("linear_velocity") or [None, None, None])[:3]
                    av = (pose.get("angular_velocity") or [None, None, None])[:3]
                    la = (pose.get("linear_acceleration") or [None, None, None])[:3]

                    # 坐标对齐：在第一个航点时计算偏移量
                    obs_pos = (
                        float(pos[0]) if pos[0] is not None else 0.0,
                        float(pos[1]) if pos[1] is not None else 0.0,
                        float(pos[2]) if pos[2] is not None else 0.0,
                    )
                    cmd_pos = (float(p_cmd.x), float(p_cmd.y), float(p_cmd.z))

                    if i == 0 and self._origin_offset is None:
                        # 计算原点偏移量
                        self._origin_offset = self._calculate_origin_offset(cmd_pos, obs_pos)
                        self._log(
                            f"[UAV{self.uav_id}] origin offset calculated: "
                            f"({self._origin_offset[0]:.4f}, {self._origin_offset[1]:.4f}, {self._origin_offset[2]:.4f})m"
                        )

                    # 应用坐标对齐
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
                            # 原始观测坐标
                            "obs_pos_x": pos[0],
                            "obs_pos_y": pos[1],
                            "obs_pos_z": pos[2],
                            # 对齐后的观测坐标（与命令坐标系对齐）
                            "obs_aligned_x": aligned_obs[0],
                            "obs_aligned_y": aligned_obs[1],
                            "obs_aligned_z": aligned_obs[2],
                            # 坐标系原点偏移量
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
                            "ulg_path": "",
                        }
                    )

                    all_pose_rows.append(
                        {
                            "traj_json": _norm_abs_path(json_path),
                            "traj_name": traj_name,
                            "uav_id": self.uav_id,
                            "step_idx": i,
                            "image_timestamp_s": ts_img,
                            # 原始坐标
                            "pos_x": pos[0],
                            "pos_y": pos[1],
                            "pos_z": pos[2],
                            # 对齐后坐标
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

            # 使用HTTP命令执行降落（不依赖MAVROS）
            self._log(f"[UAV{self.uav_id}] executing HTTP land command for traj={traj_name}")
            try:
                land_cmd = {"cmd": "land", "force": True}
                land_resp = _controller_command(self.control_base, self.uav_id, land_cmd, timeout=max(60.0, self.cmd_timeout))
                if not isinstance(land_resp, dict) or land_resp.get("ok") is False:
                    self._log(f"[UAV{self.uav_id}] HTTP land command failed: {land_resp}", "WARN")
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] HTTP land exception: {e}", "WARN")
            time.sleep(2.0)

            self._log(f"[UAV{self.uav_id}] reset(hard=True) after land traj={traj_name}")
            try:
                self._reset_and_wait_ready(
                    position=[init_pos.x * self.scale, init_pos.y * self.scale, init_pos.z * self.scale],
                    yaw_deg=None,
                    timeout=self.reset_timeout,
                )
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] reset failed traj={traj_name} err={e}")

            ulg_src: Optional[Path] = None
            deadline = time.time() + 60.0
            while time.time() < deadline:
                ulg_src = _find_latest_ulg(self.uav_id, since_ts=traj_start_ts - 5.0)
                if ulg_src is not None and ulg_src.exists() and _is_good_ulg(ulg_src):
                    break
                time.sleep(0.5)
            if ulg_src is None or not ulg_src.exists():
                ulg_src = _find_latest_ulg(self.uav_id, since_ts=0.0)

            ulg_path_str = ""
            if ulg_src is not None and ulg_src.exists():
                try:
                    exe = shutil.which("ulog_info")
                    info_summary = ""
                    if exe:
                        proc = subprocess.run(
                            [exe, str(ulg_src)],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            timeout=5.0,
                            check=False,
                        )
                        out = proc.stdout or ""
                        for line in out.splitlines():
                            t = line.strip()
                            if t.startswith("Logging start time:") or t.startswith("duration:"):
                                if info_summary:
                                    info_summary += " "
                                info_summary += t
                    if info_summary:
                        self._log(f"[UAV{self.uav_id}] selected ulg={ulg_src} {info_summary}")
                except Exception as e:
                    self._log(f"[UAV{self.uav_id}] ulog_info failed for {ulg_src}: {e}")

                ulg_filename = f"px4_uav{self.uav_id}_{int(time.time())}.ulg"
                ulg_dst = traj_dir / ulg_filename
                if _copy_ulg_now(ulg_src, ulg_dst):
                    ulg_path_str = _norm_abs_path(ulg_dst)

            for r in rows:
                r["ulg_path"] = ulg_path_str

            _write_csv(rows, csv_path)
            if all_pose_rows:
                _write_csv(all_pose_rows, pose_all_path)
            self._log(f"[UAV{self.uav_id}] done traj={traj_name} csv={csv_path} pose_csv={pose_all_path}")
            
            # Stop monitoring and save
            mavros_data = []
            mavros_failed = False
            if self.monitor:
                mavros_data, mavros_failed = self.monitor.stop_recording()
                
            if mavros_data:
                mavros_csv_path = traj_dir / "mavros_data.csv"
                try:
                    _write_csv(mavros_data, mavros_csv_path)
                except Exception as e:
                    self._log(f"[UAV{self.uav_id}] failed to write mavros data: {e}")

            if not mavros_failed:
                self._append_status_log(traj_name, ulg_path_str)
            else:
                self._log(f"[UAV{self.uav_id}] mavros failed (died) for traj={traj_name}, skipping status log")
                
        except Exception as e:
            # If monitoring was running, stop it
            if self.monitor and self.monitor.recording:
                self.monitor.stop_recording()
            raise e



def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", type=str, required=True)
    p.add_argument("--pattern", type=str, default="*.json")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "recordings"))
    p.add_argument("--config", type=str, default=str(Path(__file__).resolve().parent / "multi_uav_config.json"))
    p.add_argument("--uav-ids", type=str, default="")
    p.add_argument("--control-base", type=str, default="http://127.0.0.1:5009")
    p.add_argument("--image-base", type=str, default="http://127.0.0.1:8081")
    p.add_argument("--scale", type=float, default=0.01)
    p.add_argument("--max-points", type=int, default=0)
    zg = p.add_mutually_exclusive_group()
    zg.add_argument("--z-down", action="store_true", default=None)
    zg.add_argument("--z-up", action="store_true", default=None)
    p.add_argument("--reset-timeout", type=float, default=240.0)
    p.add_argument("--cmd-timeout", type=float, default=120.0)
    p.add_argument("--image-timeout", type=float, default=10.0)
    p.add_argument("--image-retries", type=int, default=30)
    sg = p.add_mutually_exclusive_group()
    sg.add_argument("--skip-existing", action="store_true", default=None)
    sg.add_argument("--no-skip-existing", action="store_true", default=None)
    p.add_argument("--dry-run", action="store_true", default=False)
    # Mission模式：使用MAVROS航点任务而不是HTTP move_to命令
    p.add_argument("--mission-mode", action="store_true", default=False,
                   help="Use MAVROS mission mode instead of HTTP move_to commands")
    # 速度前馈OFFBOARD模式：使用平滑轨迹跟踪控制
    p.add_argument("--velocity-offboard", action="store_true", default=False,
                   help="Use velocity-based OFFBOARD control with trajectory smoothing")
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

    # Init ROS2 if available
    ros_node = None
    spin_thread = None
    monitors: Dict[int, MavrosMonitor] = {}

    if rclpy:
        try:
            rclpy.init()
            ros_node = CollectorNode()
            ts_log("[Main]", "ROS2 node initialized")
        except Exception as e:
            ts_log("[Main]", f"Failed to initialize ROS2: {e}", "ERROR")

    if args.uav_ids.strip():
        uav_ids = sorted(set(int(x.strip()) for x in args.uav_ids.split(",") if x.strip()))
    else:
        uav_ids = _load_uav_ids_from_config(Path(args.config).resolve())

    # Create monitors before starting spin
    if ros_node:
        for vid in uav_ids:
            try:
                monitors[vid] = MavrosMonitor(ros_node, vid, freq=20.0)
                ts_log("[Main]", f"Created MavrosMonitor for UAV{vid}")
            except Exception as e:
                ts_log("[Main]", f"Failed to create monitor for UAV{vid}: {e}", "ERROR")

    # Start spinning
    if ros_node:
        def _spin():
            try:
                ts_log("[ROS2]", "Starting ROS2 spin...")
                rclpy.spin(ros_node)
                ts_log("[ROS2]", "ROS2 spin finished normally")
            except Exception as e:
                import traceback as _tb
                tb = _tb.format_exc()
                ts_log("[ROS2]", f"ROS2 spin error: {e}", "ERROR")
                ts_log("[ROS2]", f"ROS2 spin traceback:\n{tb}", "ERROR")
        spin_thread = threading.Thread(target=_spin, name="ros_spin", daemon=True)
        spin_thread.start()

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
        monitor = monitors.get(vid)
        commander = None
        if ros_node:
            try:
                commander = MavrosCommander(ros_node, vid)
            except Exception as e:
                ts_log("[Main]", f"Failed to create commander for UAV{vid}: {e}", "ERROR")
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
            monitor=monitor,
            commander=commander,
            status_log_path=status_log_path,
            use_mission_mode=args.mission_mode,
            use_velocity_offboard=args.velocity_offboard,
        )
        t = threading.Thread(target=w.run, name=f"uav{vid}", daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if rclpy and ros_node:
        try:
            rclpy.shutdown()
        except Exception as e:
            # 异常处理：rclpy.shutdown 失败，打印堆栈并继续退出（可恢复）
            _log_exc("rclpy.shutdown failed", e)
        if spin_thread:
            spin_thread.join(timeout=1.0)



if __name__ == "__main__":
    main()
