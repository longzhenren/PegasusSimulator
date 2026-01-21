#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简易轨迹采集器 - 分析版本 (simple_trajectory_collector_analysis.py)

这是一个改进版本的轨迹数据采集器，主要特点和改进：
1. 支持相对飞行：从当前UAV位置开始，而不是teleport到轨迹起点
2. 后处理CSV数据对齐：将记录的数据与原始轨迹坐标对齐
3. 更健壮的起飞序列：多线程setpoint流，更稳定的起飞过程
4. 手动速度/加速度缩放：修复时间缩放带来的速度异常
5. 绝对Z坐标控制：Z轴使用绝对坐标，XY使用相对偏移

核心功能：
- 起飞阶段：使用HTTP setpoint命令进行原地爬升
- 轨迹跟踪：使用UDP MAVLink发送PVA(PVA=Position+Velocity+Acceleration) setpoint
- 数据记录：服务端StateRecorder自动记录位姿、图像和状态数据
- 坐标系统：ENU坐标系输入，通过内部转换处理NED坐标系通信

工作流程：
1. 加载JSON轨迹文件，创建平滑轨迹
2. 获取当前UAV位置，计算相对飞行偏移
3. 原地起飞到目标高度并稳定悬停
4. 初始化UDP MAVLink连接和时间同步
5. 执行轨迹跟踪，发送控制命令
6. 停止记录，后处理数据对齐
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
    """
    时间戳日志函数 - 提供带毫秒精度的时间戳和日志级别的前缀

    Args:
        prefix: 日志前缀，通常是"[UAV{id}]"
        msg: 日志消息内容
        level: 日志级别 ("INFO", "WARN", "ERROR")
    """
    now = time.time()
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
    msec = int((now - int(now)) * 1000)
    print(f"[{ts}.{msec:03d}] [{level}] {prefix} {msg}", flush=True)

def _http_json(method: str, url: str, payload: Optional[Dict] = None, timeout: float = 30.0) -> Tuple[int, Dict]:
    """
    执行HTTP JSON请求的通用函数

    Args:
        method: HTTP方法 ("GET", "POST", "PUT", "DELETE")
        url: 请求URL
        payload: 请求体数据字典 (可选)
        timeout: 请求超时时间(秒)

    Returns:
        Tuple[int, Dict]: (HTTP状态码, 响应数据字典)
    """
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
    """
    获取指定UAV的当前位姿信息 (位置、姿态、速度等)

    Args:
        image_base: 图像服务基础URL
        uav_id: UAV编号
        timeout: 请求超时时间

    Returns:
        位姿数据字典或None (如果请求失败)
    """
    code, data = _http_json("GET", f"{image_base}/uav/{uav_id}/pose", timeout=timeout)
    if code == 200:
        return data
    return None

# ============================================================================
# 轨迹平滑器 (TrajectorySmoother)
# ============================================================================

class TrajectorySmoother:
    """
    轨迹平滑器类 - 使用三次样条插值对轨迹点进行平滑处理

    功能：
    - 对轨迹的x,y,z坐标和yaw角度进行平滑插值
    - 提供位置、速度、加速度的连续函数
    - 支持任意时间点的状态查询

    坐标系：ENU (East-North-Up)
    输入：离散轨迹点 [x, y, z, roll, pitch, yaw]
    输出：连续轨迹函数，可查询任意时刻的状态
    """
    def __init__(self, points: List[List[float]], dt: float = 0.2):
        """
        初始化轨迹平滑器

        Args:
            points: 轨迹点列表，每个点格式为 [x, y, z, roll, pitch, yaw]
                   坐标为ENU坐标系，角度为度数
            dt: 相邻轨迹点的时间间隔(秒)，默认0.2秒

        处理步骤：
        1. 计算轨迹总时长 = (点数-1) * dt
        2. 提取x,y,z坐标和yaw角度
        3. 将yaw角度转换为弧度并进行unwrap处理（避免角度跳变）
        4. 为每个维度创建三次样条插值函数
        """
        self.dt = dt
        self.duration = (len(points) - 1) * dt  # 轨迹总时长
        self.t_orig = np.array([i * dt for i in range(len(points))])  # 时间点序列

        # 提取各维度数据
        x = [p[0] for p in points]
        y = [p[1] for p in points]
        z = [p[2] for p in points]
        yaw = np.unwrap(np.deg2rad([p[5] for p in points]))  # 转换为弧度并unwrap

        # 创建三次样条插值器 (natural边界条件确保平滑)
        self._spline_x = CubicSpline(self.t_orig, x, bc_type='natural')
        self._spline_y = CubicSpline(self.t_orig, y, bc_type='natural')
        self._spline_z = CubicSpline(self.t_orig, z, bc_type='natural')
        self._spline_yaw = CubicSpline(self.t_orig, yaw, bc_type='natural')

    def get_full_state(self, t: float) -> Dict[str, float]:
        """
        获取指定时刻的完整状态 (位置+速度+加速度+姿态)

        Args:
            t: 时间点(秒)，会自动截断到有效范围[0, duration]

        Returns:
            包含完整状态的字典：
            - x,y,z: 位置坐标 (ENU坐标系)
            - vx,vy,vz: 速度分量 (m/s)
            - ax,ay,az: 加速度分量 (m/s²)
            - yaw: 偏航角 (弧度)
            - yaw_rate: 偏航角速度 (弧度/秒)
        """
        t = max(0.0, min(t, self.duration))  # 确保时间在有效范围内
        return {
            'x': float(self._spline_x(t)),           # 位置
            'y': float(self._spline_y(t)),
            'z': float(self._spline_z(t)),
            'vx': float(self._spline_x(t, 1)),       # 速度 (一阶导数)
            'vy': float(self._spline_y(t, 1)),
            'vz': float(self._spline_z(t, 1)),
            'ax': float(self._spline_x(t, 2)),       # 加速度 (二阶导数)
            'ay': float(self._spline_y(t, 2)),
            'az': float(self._spline_z(t, 2)),
            'yaw': float(self._spline_yaw(t)),       # 姿态
            'yaw_rate': float(self._spline_yaw(t, 1)) # 角速度
        }

    def get_yaw_and_rate(self, t: float) -> Tuple[float, float]:
        """
        获取指定时刻的偏航角和偏航角速度

        Args:
            t: 时间点(秒)

        Returns:
            Tuple[float, float]: (yaw角度, yaw角速度)，单位为弧度
        """
        t = max(0.0, min(t, self.duration))
        return float(self._spline_yaw(t)), float(self._spline_yaw(t, 1))

# ============================================================================
# 后处理函数 (Post-Processing: Align CSV Data)
# ============================================================================

def post_process_csv(uav_id: int, uav_dir: Path, diff: Tuple[float, float, float]):
    """
    后处理CSV数据对齐函数

    目的：将记录的观测位置数据与原始JSON轨迹坐标对齐
    原理：相对飞行时，UAV实际起始位置与轨迹起点存在偏移，
          需要将记录的数据反向偏移，使其与原始轨迹对齐

    Args:
        uav_id: UAV编号，用于日志标识
        uav_dir: UAV数据目录，包含data.csv文件
        diff: 偏移量三元组 (dx, dy, dz)
              diff = actual_start_pos - traj_start_pos

    处理逻辑：
    1. 读取原始CSV文件
    2. 找到obs_pos_x/y/z列
    3. 对每个观测位置应用反向偏移：obs_new = obs_old - diff
    4. 安全写入新文件，避免数据损坏
    """
    try:
        data_csv = uav_dir / "data.csv"
        if not data_csv.exists():
            ts_log(f"[UAV{uav_id}]", f"No data.csv found at {data_csv}", "WARN")
            return

        ts_log(f"[UAV{uav_id}]", f"Post-processing CSV for alignment: diff=({diff[0]:.2f}, {diff[1]:.2f}, {diff[2]:.2f})")
        
        # 读取整个CSV文件
        with open(data_csv, 'r') as f:
            lines = f.readlines()

        if not lines:
            return

        # 解析表头，找到观测位置列
        header = lines[0].strip().split(',')
        try:
            idx_x = header.index("obs_pos_x")  # 观测X坐标列索引
            idx_y = header.index("obs_pos_y")  # 观测Y坐标列索引
            idx_z = header.index("obs_pos_z")  # 观测Z坐标列索引
        except ValueError:
            ts_log(f"[UAV{uav_id}]", "Missing obs_pos columns in CSV", "WARN")
            return

        dx, dy, dz = diff
        new_lines = [lines[0]]  # 保留表头

        # 处理每一行数据
        for line in lines[1:]:
            parts = line.strip().split(',')
            if len(parts) > max(idx_x, idx_y, idx_z):
                try:
                    # 应用坐标偏移对齐
                    # 原理：obs_new = obs_old - diff
                    # 例如：如果轨迹起点在(100,100,100)但UAV从(0,0,0)开始飞行
                    # 则diff = (0-100, 0-100, 0-100) = (-100, -100, -100)
                    # 记录的观测位置0需要映射到100：0 - (-100) = 100

                    val_x = float(parts[idx_x])
                    val_y = float(parts[idx_y])
                    val_z = float(parts[idx_z])

                    # 应用反向偏移进行对齐
                    parts[idx_x] = f"{val_x - dx:.8f}"
                    parts[idx_y] = f"{val_y - dy:.8f}"
                    parts[idx_z] = f"{val_z - dz:.8f}"

                    new_lines.append(",".join(parts) + "\n")
                except ValueError:
                    # 解析错误时保留原行
                    new_lines.append(line)
            else:
                # 列数不足时保留原行
                new_lines.append(line)

        # 安全写入：先写临时文件，再覆盖原文件
        temp_csv = uav_dir / "data_aligned.csv"
        with open(temp_csv, 'w') as f:
            f.writelines(new_lines)

        # 原子性替换原文件
        shutil.move(str(temp_csv), str(data_csv))
        ts_log(f"[UAV{uav_id}]", "CSV alignment complete.")
        
    except Exception as e:
        ts_log(f"[UAV{uav_id}]", f"Error in post-processing: {e}", "ERROR")

# ============================================================================
# MAVLink UDP Setpoint发送器 (MavlinkSetpointSender)
# ============================================================================

class MavlinkSetpointSender:
    """
    MAVLink UDP通信类 - 负责发送PVA(PVA=Position+Velocity+Acceleration) setpoint命令

    功能：
    - 建立UDP连接到PX4仿真实例
    - 发送完整的轨迹控制命令（位置+速度+加速度+姿态）
    - 处理ENU到NED坐标系转换
    - 处理角度变换

    MAVLink端口：14580 + uav_id (PX4仿真默认端口)
    坐标系转换：输入ENU坐标系 → 输出NED坐标系 (PX4标准)
    """
    def __init__(self, uav_id: int):
        """
        初始化MAVLink发送器

        Args:
            uav_id: UAV编号，确定PX4端口 (14580 + uav_id)
        """
        self.uav_id = uav_id
        self.px4_port = 14580 + uav_id  # PX4仿真端口计算
        self._conn = None  # MAVLink连接对象

    def connect(self) -> bool:
        """
        建立UDP连接到PX4仿真实例

        Returns:
            bool: 连接成功返回True，失败返回False
        """
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
        """
        发送PVA setpoint命令到PX4

        Args:
            x,y,z: 位置坐标 (ENU坐标系，单位：米)
            vx,vy,vz: 速度分量 (ENU坐标系，单位：m/s)
            ax,ay,az: 加速度分量 (ENU坐标系，单位：m/s²)
            yaw_rad: 偏航角 (ENU坐标系，单位：弧度)
            yaw_rate_rad: 偏航角速度 (单位：弧度/秒)
        """
        """
        发送PVA setpoint - 实现坐标变换和MAVLink消息发送

        坐标变换说明：
        - ENU → NED: PX4使用NED坐标系，但轨迹数据是ENU坐标系
        - 位置变换: [x_enu, y_enu, z_enu] → [y_enu, x_enu, -z_enu]
        - 速度变换: 遵循相同规则，符号变化
        - 加速度变换: 遵循相同规则，符号变化
        - 角度变换: ENU偏航角转换为NED偏航角
        """
        if not self._conn:
            return

        from pymavlink import mavutil

        # ENU → NED 坐标变换 (与mavlink_sim_vehicle.py保持一致)
        # NED坐标系: X=North, Y=East, Z=Down
        # ENU坐标系: X=East, Y=North, Z=Up
        ned_x = y      # ENU_X → NED_Y
        ned_y = x      # ENU_Y → NED_X
        ned_z = -z     # ENU_Z → NED_Z (符号取反)

        # 速度分量变换
        ned_vx = vy    # 速度变换遵循相同规则
        ned_vy = vx
        ned_vz = -vz

        # 加速度分量变换
        ned_ax = ay    # 加速度变换遵循相同规则
        ned_ay = ax
        ned_az = -az

        # 偏航角变换: ENU → NED
        # ENU: 0°=正东，90°=正北
        # NED: 0°=正北，90°=正东
        # 变换公式: ned_yaw = π/2 - enu_yaw + π
        ned_yaw = 0.5 * math.pi - yaw_rad + math.pi
        ned_yaw = (ned_yaw + math.pi) % (2 * math.pi) - math.pi  # 规范化到[-π, π]
        ned_yaw_rate = -yaw_rate_rad  # 角速度符号取反

        # 发送MAVLink SET_POSITION_TARGET_LOCAL_NED消息
        self._conn.mav.set_position_target_local_ned_send(
            0,                      # time_boot_ms (not used)
            self.uav_id + 1,         # target_system (PX4系统ID)
            1,                      # target_component
            mavutil.mavlink.MAV_FRAME_LOCAL_NED,  # 坐标系
            0x0000,                  # type_mask=0 表示完整PVA控制
            ned_x, ned_y, ned_z,     # 位置 (NED)
            ned_vx, ned_vy, ned_vz,  # 速度 (NED)
            ned_ax, ned_ay, ned_az,  # 加速度 (NED)
            ned_yaw, ned_yaw_rate    # 偏航角和角速度
        )

# ============================================================================
# 仿真时间监听器 (SimTimeListener)
# ============================================================================

class SimTimeListener(threading.Thread):
    """
    仿真时间同步监听器 - 监听Isaac Sim的时间以实现精确的轨迹控制

    功能：
    - 后台线程持续监听仿真时间
    - 提供时间同步机制，确保轨迹控制与仿真时间对齐
    - 支持等待时间推进，避免过早发送控制命令

    原理：
    - 轨迹控制需要精确的时间同步
    - Isaac Sim的时间可能不均匀推进
    - 通过HTTP轮询获取当前仿真时间
    """
    def __init__(self, url="http://127.0.0.1:8081/sim_time"):
        """
        初始化时间监听器

        Args:
            url: 仿真时间服务URL，默认为Isaac Sim的时间接口
        """
        super().__init__(daemon=True)  # 守护线程，随主线程退出
        self.url = url
        self._time = 0.0  # 当前仿真时间
        self._lock = threading.Lock()
        self._cond = threading.Condition()  # 条件变量，用于等待时间推进
        self.running = True  # 控制线程运行状态

    def run(self):
        """
        后台监听循环 - 持续获取仿真时间

        频率：10Hz (每100ms获取一次)
        处理：获取时间后通知所有等待的线程
        """
        while self.running:
            try:
                # 获取当前仿真时间
                code, data = _http_json("GET", self.url, timeout=0.5)
                if code == 200:
                    t = float(data.get("sim_time", 0))
                    with self._cond:
                        self._time = t
                        self._cond.notify_all()  # 通知等待的线程
            except:
                pass  # 忽略超时等异常
            time.sleep(0.1)  # 10Hz轮询，避免服务器过载

    def get_time(self) -> float:
        """
        获取当前仿真时间

        Returns:
            float: 当前仿真时间(秒)
        """
        with self._lock:
            return self._time

    def wait_for_advance(self, last_time: float, timeout: float = 1.0) -> float:
        """
        等待仿真时间推进到指定时间之后

        Args:
            last_time: 上次的时间戳
            timeout: 最大等待时间(秒)

        Returns:
            float: 推进后的当前时间
        """
        start = time.time()
        with self._cond:
            # 等待时间推进：确保新时间 > 上次时间
            while self._time <= last_time:
                if time.time() - start > timeout:
                    return self._time  # 超时返回当前时间
                self._cond.wait(timeout=0.1)  # 等待时间更新通知
            return self._time  # 返回推进后的时间

# ============================================================================
# 主轨迹采集函数 (run_collector)
# ============================================================================

def run_collector(uav_id: int, json_path: Path, out_dir: Path, scale: float,
                  time_scale: float, control_base: str, image_base: str):
    """
    执行单架UAV的轨迹数据采集任务 - 分析版本的主要采集逻辑

    核心改进：
    1. 相对飞行：从当前UAV位置开始，而不是teleport
    2. 后处理对齐：将记录数据与原始轨迹坐标对齐
    3. 更稳定的起飞：多线程setpoint流 + 健壮的ARM/OFFBOARD
    4. 手动缩放控制：修复时间缩放对速度/加速度的影响

    Args:
        uav_id: UAV编号 (0-7)
        json_path: 轨迹JSON文件路径
        out_dir: 输出目录
        scale: 轨迹坐标缩放因子 (默认0.01)
        time_scale: 时间缩放因子 (默认2.5)
        control_base: 控制服务基础URL
        image_base: 图像/位姿服务基础URL
    """
    ts_log(f"[UAV{uav_id}]", f"Processing {json_path.name}...")
    
    # ===== 1. 轨迹加载和预处理 =====
    with open(json_path) as f:
        data = json.load(f)
    raw_logs = data.get("raw_logs", [])
    if not raw_logs:
        ts_log(f"[UAV{uav_id}]", "No raw_logs found", "ERROR")
        return

    # 应用坐标缩放 (默认scale=0.01，将轨迹从厘米缩放到米级)
    scaled_points = [[r[0]*scale, r[1]*scale, r[2]*scale, r[3], r[4], r[5]] for r in raw_logs]

    # 创建轨迹平滑器 (使用0.2秒时间间隔)
    smoother = TrajectorySmoother(scaled_points, dt=0.2)
    # 计算实际飞行时长 (考虑时间缩放)
    total_duration = smoother.duration / time_scale

    # ===== 2. 创建输出目录结构 =====
    traj_dir = out_dir / json_path.stem  # 轨迹目录 (以JSON文件名命名)
    uav_dir = traj_dir / f"uav{uav_id}"   # UAV子目录
    uav_dir.mkdir(parents=True, exist_ok=True)

    # ===== 3. 计算目标参数 =====
    traj_start = smoother.get_full_state(0)  # 获取轨迹起点状态
    # Z轴使用绝对高度控制 (分析版本的核心改进)
    TARGET_ALT = max(traj_start['z'], 1.0)  # 确保至少1米高度
    ALT_TOLERANCE = 0.1  # 高度容差 (10cm)

    ts_log(f"[UAV{uav_id}]", f"Trajectory Start (Ref): ({traj_start['x']:.2f}, {traj_start['y']:.2f}, {traj_start['z']:.2f})")
    ts_log(f"[UAV{uav_id}]", f"Target Altitude (Abs): {TARGET_ALT:.2f}m")
    
    # ===== 4. 获取UAV初始位置 (相对飞行核心) =====
    # 分析版本的最大改进：支持相对飞行，从当前位置开始而不是teleport
    pose = None
    wait_pose_start = time.time()
    # 重试逻辑：最多等待120秒获取UAV位姿
    while time.time() - wait_pose_start < 120.0:
        pose = _fetch_pose_only(image_base, uav_id)
        if pose and pose.get("position"):
            break
        # 每5秒打印一次等待信息
        if int(time.time()) % 5 == 0:
            ts_log(f"[UAV{uav_id}]", "Waiting for simulator pose...", "WARN")
        time.sleep(1.0)

    if not pose:
        ts_log(f"[UAV{uav_id}]", "Failed to fetch initial pose after 120s", "ERROR")
        return

    # 提取当前UAV位置 (仿真世界坐标系)
    start_pos_sim = pose.get("position", [0,0,0])
    current_x, current_y, current_z = start_pos_sim

    # 计算起飞目标：保持当前XY位置，爬升到目标高度
    takeoff_target = [current_x, current_y, TARGET_ALT]
    ts_log(f"[UAV{uav_id}]", f"Current Position: ({current_x:.2f}, {current_y:.2f}, {current_z:.2f})")
    ts_log(f"[UAV{uav_id}]", f"Takeoff Target: ({takeoff_target[0]:.2f}, {takeoff_target[1]:.2f}, {takeoff_target[2]:.2f})")
    
    try:
        # ===== 5. 相对飞行起飞序列 (分析版本核心改进) =====
        # 传统方法：teleport到轨迹起点
        # 分析版本：从当前位置原地爬升，更符合真实飞行场景

        if current_z < TARGET_ALT - ALT_TOLERANCE:
            ts_log(f"[UAV{uav_id}]", f"Climbing from {current_z:.2f}m to {TARGET_ALT:.2f}m...")

            # ===== 多线程Setpoint流 (健壮的起飞策略) =====
            # 问题：单次发送setpoint可能被PX4忽略
            # 解决：持续高频发送setpoint命令，确保PX4接收到
            import threading
            stop_streaming = threading.Event()

            # 起飞命令：保持XY位置不变，只控制Z轴爬升
            takeoff_cmd = {
                "cmd": "setpoint",
                "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                "vx": 0.0, "vy": 0.0, "vz": 0.5,  # 0.5m/s爬升速度
                "afx": 0.0, "afy": 0.0, "afz": 0.0,
                "yaw": smoother.get_yaw_and_rate(0)[0], "yaw_rate": 0.0
            }

            def stream_setpoints():
                """
                后台线程：持续发送setpoint命令 (20Hz)
                确保PX4始终接收到最新的控制指令
                """
                while not stop_streaming.is_set():
                    try:
                        _http_json("POST", f"{control_base}/command", payload=takeoff_cmd, timeout=0.1)
                    except: pass  # 忽略超时，持续发送
                    time.sleep(0.05)  # 20Hz发送频率

            # Start streaming
            ts_log(f"[UAV{uav_id}]", "Starting setpoint stream...")
            stream_thread = threading.Thread(target=stream_setpoints)
            stream_thread.start()
            
            # 3.2 Robust ARM
            arm_success = False
            for attempt in range(30):
                ts_log(f"[UAV{uav_id}]", f"Arming attempt {attempt+1}...")
                try:
                    code, resp = _http_json("POST", f"{control_base}/command", {"cmd": "arm"}, timeout=2.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        arm_success = True
                        ts_log(f"[UAV{uav_id}]", "Armed successfully!")
                        break
                except Exception as e:
                    ts_log(f"[UAV{uav_id}]", f"Arm error: {e}", "WARN")
                time.sleep(1.0)
            
            if not arm_success:
                ts_log(f"[UAV{uav_id}]", "WARNING: Failed to ARM", "WARN")

            # 3.3 Robust OFFBOARD
            offboard_success = False
            for attempt in range(10):
                ts_log(f"[UAV{uav_id}]", f"Setting OFFBOARD attempt {attempt+1}...")
                try:
                    code, resp = _http_json("POST", f"{control_base}/command", {"cmd": "set_mode", "mode": "OFFBOARD"}, timeout=2.0)
                    if 200 <= code < 300 and resp.get("ok"):
                        offboard_success = True
                        ts_log(f"[UAV{uav_id}]", "OFFBOARD set!")
                        break
                except Exception as e:
                     ts_log(f"[UAV{uav_id}]", f"Offboard error: {e}", "WARN")
                time.sleep(1.0)
            
            # 等待爬升 (Wait for altitude)
            takeoff_start = time.time()
            ts_log(f"[UAV{uav_id}]", "Climbing (monitoring)...")
            
            # Set climb velocity
            takeoff_cmd["vz"] = 0.5
            
            last_z_log = 0
            while time.time() - takeoff_start < 45.0:
                # We rely on the stream thread sending Position Setpoints (Target Z).
                # But we need to check current altitude.
                if time.time() - last_z_log > 2.0:
                     pose = _fetch_pose_only(image_base, uav_id) or {}
                     current_z = (pose.get("position") or [0, 0, 0])[2]
                     ts_log(f"[UAV{uav_id}]", f"Alt: {current_z:.2f}m / {TARGET_ALT:.2f}m")
                     last_z_log = time.time()
                     if abs(current_z - TARGET_ALT) < ALT_TOLERANCE:
                         ts_log(f"[UAV{uav_id}]", "Target altitude reached")
                         break
                time.sleep(0.1)
            
            # Stop streaming setpoints so we can switch to trajectory setpoints
            stop_streaming.set()
            stream_thread.join()
            
            ts_log(f"[UAV{uav_id}]", "Stabilizing...")
            # Wait for velocity to settle
            stable = False
            for _ in range(100): # 10s timeout
                 pose = _fetch_pose_only(image_base, uav_id)
                 if pose:
                    vel = pose.get("linear_velocity", [0,0,0])
                    v_mag = (vel[0]**2 + vel[1]**2 + vel[2]**2)**0.5
                    if v_mag < 0.05:
                        stable = True
                        break
                 time.sleep(0.1)
                 
            if not stable:
                ts_log(f"[UAV{uav_id}]", "WARNING: Not stable after climb", "WARN")
            
            # 5. Calculate Relative Offset
            pose = _fetch_pose_only(image_base, uav_id)
            if not pose:
                 ts_log(f"[UAV{uav_id}]", "Failed to get pose for offset", "ERROR")
                 return
            
            actual_start_pos = pose['position']
            offset_x = actual_start_pos[0] - traj_start['x']
            offset_y = actual_start_pos[1] - traj_start['y']
            offset_z = 0.0 # Keep Z absolute as per plan? 
            # Original plan: "Adjust XY setpoints... while using the absolute Z-coordinate"
            # So offset_z should be treated carefully.
            # If we want to fly relative in Z too (e.g. if terrain is different), we could.
            # But here "offset_z" for metadata is fine.
            # Actually, `relative_offset_z` in metadata is just informative.
            # But in flight loop:
            # target_z = state['z'] (Absolute from JSON)
            # So we don't apply offset_z to control.
            
            ts_log(f"[UAV{uav_id}]", f"Relative Flight Offset: x={offset_x:.2f}, y={offset_y:.2f}")

            # 6. Start Recording (Server Side)
            # ... (Rest of the code)
        
        # ========== 2. 悬停稳定 (速度 < 0.05m/s) ==========
        ts_log(f"[UAV{uav_id}]", "Stabilizing...")
        stable_start = time.time()
        actual_start_pos = None
        
        while time.time() - stable_start < 10.0:
            # 发送悬停命令 (Velocity=0)
            cmd = {
                "cmd": "setpoint",
                "x": takeoff_target[0], "y": takeoff_target[1], "z": TARGET_ALT,
                "vx": 0.0, "vy": 0.0, "vz": 0.0,
                "afx": 0.0, "afy": 0.0, "afz": 0.0,
                "yaw": smoother.get_yaw_and_rate(0)[0], "yaw_rate": 0.0
            }
            try: _http_json("POST", f"{control_base}/command", payload=cmd, timeout=0.1)
            except: pass
            
            # 检查速度
            pose = _fetch_pose_only(image_base, uav_id)
            if pose:
                vel = pose.get("linear_velocity", [0,0,0])
                v_mag = math.sqrt(vel[0]**2 + vel[1]**2 + vel[2]**2)
                if v_mag < 0.05:
                    actual_start_pos = pose.get("position")
                    ts_log(f"[UAV{uav_id}]", f"Stable! Vel={v_mag:.3f}m/s, Pos={actual_start_pos}")
                    break
            time.sleep(0.05)
            
        if not actual_start_pos:
            ts_log(f"[UAV{uav_id}]", "WARNING: Failed to stabilize completely, using last known pos")
            pose = _fetch_pose_only(image_base, uav_id)
            actual_start_pos = pose.get("position") if pose else takeoff_target

        # ========== 3. 计算相对飞行偏移 ==========
        # Offset = Actual_Start_Pos - Refernece_Traj_Start
        # We only offset XY, Z is absolute per requirements
        offset_x = actual_start_pos[0] - traj_start['x']
        offset_y = actual_start_pos[1] - traj_start['y']
        offset_z = actual_start_pos[2] - traj_start['z'] # Used for CSV alignment later
        
        ts_log(f"[UAV{uav_id}]", f"Relative Flight Offset: dX={offset_x:.2f}, dY={offset_y:.2f}")

        # ========== 4. 启动服务端记录 ==========
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
                        # Search for randomized directories px4_{uav_id}_*
                        # Note: We look for the most recently modified one matching the pattern
                        # /tmp/px4_3_ry1na5q1/log/2026-01-19/04_57_29.ulg
                        # Pattern: px4_{uav_id}_*/log/**/*.ulg
                        # Use rglob for recursive search inside the matching directories
                        # First find the instance directories
                        instance_dirs = list(root.glob(f"px4_{uav_id}_*"))
                        for idir in instance_dirs:
                            log_dir = idir / "log"
                            if log_dir.exists():
                                candidates.extend(list(log_dir.rglob("*.ulg")))
                    else:
                        candidates.extend(list(root.glob("**/*.ulg")))
            
            if candidates:
                # Get the most recent ULG file
                latest_ulg = max(candidates, key=lambda p: p.stat().st_mtime)
                ulg_path = str(latest_ulg.absolute())
                ts_log(f"[UAV{uav_id}]", f"Found ULG file: {ulg_path}")
            else:
                ts_log(f"[UAV{uav_id}]", "No ULG file found", "WARN")
        except Exception as e:
            ts_log(f"[UAV{uav_id}]", f"Error finding ULG file: {e}", "WARN")

        ts_log(f"[UAV{uav_id}]", "Starting server-side recording...")
        code, resp = _http_json("POST", f"{image_base}/uav/{uav_id}/buffer/start", {
            "save_dir": str(uav_dir.absolute()),
            "traj_json": str(json_path.absolute()),
            "traj_name": json_path.stem,
            "ulg_path": ulg_path
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
        
        # RESET LOOKAHEAD for relative flight
        LOOKAHEAD_TIME = 0.2
        last_cmd_sim_ts = sim_start_ts
        
        # ===== 7. 轨迹跟踪循环 (核心控制逻辑) =====
        ts_log(f"[UAV{uav_id}]", f"Starting trajectory (duration={total_duration:.1f}s)...")

        # ===== 超时保护机制 =====
        # 轨迹可能因各种原因卡住，设置最大执行时间
        max_real_time = total_duration * 2 + 30.0  # 2倍预期时长 + 30秒缓冲
        traj_start_real = time.time()

        # ===== 主控制循环 =====
        while True:
            # 实时超时检查 - 防止轨迹执行卡住
            if time.time() - traj_start_real > max_real_time:
                ts_log(f"[UAV{uav_id}]", f"Trajectory timeout after {max_real_time:.1f}s real time", "WARN")
                break

            # ===== 时间同步控制 =====
            # 等待仿真时间推进，确保控制命令与仿真时间对齐
            current_sim_ts = time_listener.wait_for_advance(last_cmd_sim_ts, timeout=1.0)
            # 计算轨迹经过时间 (考虑时间缩放)
            elapsed = (current_sim_ts - sim_start_ts) * time_scale

            # 重复时间检查 (避免重复发送命令)
            if current_sim_ts <= last_cmd_sim_ts:
                continue
            last_cmd_sim_ts = current_sim_ts

            # 轨迹结束检查
            if elapsed >= smoother.duration:
                break

            # ===== 前瞻控制策略 =====
            # 获取稍前时刻的状态，实现前馈控制
            state = smoother.get_full_state(elapsed + LOOKAHEAD_TIME * time_scale)

            # ===== 手动速度/加速度缩放 (关键修复) =====
            # 问题：时间缩放后，速度和加速度需要相应调整
            # 原理：如果时间加快2.5倍，速度也需要加快2.5倍，加速度加快6.25倍
            vx_scaled = state['vx'] * time_scale
            vy_scaled = state['vy'] * time_scale
            vz_scaled = state['vz'] * time_scale
            ax_scaled = state['ax'] * (time_scale ** 2)  # 加速度缩放因子更大
            ay_scaled = state['ay'] * (time_scale ** 2)
            az_scaled = state['az'] * (time_scale ** 2)

            # ===== 相对位置控制 =====
            # XY轴：应用偏移，使UAV从实际位置开始飞行
            # Z轴：保持绝对坐标 (从JSON轨迹直接获取)
            target_x = state['x'] + offset_x  # 相对控制
            target_y = state['y'] + offset_y  # 相对控制
            target_z = state['z']             # 绝对控制
            
            if use_udp:
                sender.send_pva(
                    target_x, target_y, target_z,
                    vx_scaled, vy_scaled, vz_scaled,
                    ax_scaled, ay_scaled, az_scaled,
                    state['yaw'], state['yaw_rate'] * time_scale
                )
            else:
                # HTTP回退
                cmd = {
                    "cmd": "setpoint",
                    "x": target_x, "y": target_y, "z": target_z,
                    "vx": vx_scaled, "vy": vy_scaled, "vz": vz_scaled,
                    "afx": ax_scaled, "afy": ay_scaled, "afz": az_scaled,
                    "yaw": state['yaw'], "yaw_rate": state['yaw_rate'] * time_scale
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
            "relative_offset_x": offset_x,
            "relative_offset_y": offset_y,
            "relative_offset_z": offset_z
        }
        with open(uav_dir / "metadata.json", "w") as f:
            json.dump(meta, f, indent=4)
        shutil.copy2(json_path, traj_dir / json_path.name)
        
        # ========== 10. Post-processing (Align Data) ==========
        post_process_csv(uav_id, uav_dir, (offset_x, offset_y, offset_z))
        
        ts_log(f"[UAV{uav_id}]", f"Done! Recorded {state_count} states to {uav_dir}")
        
    except Exception as e:
        import traceback
        ts_log(f"[UAV{uav_id}]", f"Error: {e}\n{traceback.format_exc()}", "ERROR")

    finally:
        time_listener.running = False

if __name__ == "__main__":
    """
    命令行入口点 - 解析参数并启动轨迹采集

    支持两种使用模式：
    1. 指定单个JSON文件：--json-file path/to/trajectory.json
    2. 指定目录自动选择：--input-dir /path/to/json/dir (按uav_id取模选择文件)
    """
    parser = argparse.ArgumentParser(description="简易轨迹采集器 - 分析版本")
    parser.add_argument("--uav-id", type=int, default=0,
                       help="UAV编号 (0-7), 影响端口和文件选择")
    parser.add_argument("--input-dir",
                       help="JSON文件目录 (按uav_id模数选择文件)")
    parser.add_argument("--json-file",
                       help="指定单个JSON轨迹文件")
    parser.add_argument("--out-dir", required=True,
                       help="输出目录 (必需)")
    parser.add_argument("--scale", type=float, default=0.01,
                       help="轨迹坐标缩放因子 (默认0.01)")
    parser.add_argument("--time-scale", type=float, default=2.5,
                       help="时间缩放因子 (默认2.5)")
    parser.add_argument("--control-base", default="http://127.0.0.1:5009",
                       help="控制服务基础URL")
    parser.add_argument("--image-base", default="http://127.0.0.1:8081",
                       help="图像/位姿服务基础URL")

    args = parser.parse_args()

    # ===== 文件选择逻辑 =====
    if args.json_file:
        # 模式1：直接指定JSON文件
        json_path = Path(args.json_file)
    elif args.input_dir:
        # 模式2：从目录选择 (支持多UAV并行)
        files = sorted(glob.glob(os.path.join(args.input_dir, "*.json")))
        if not files:
            print("No JSON files found")
            sys.exit(1)
        # 按UAV ID取模选择文件，实现多UAV负载均衡
        json_path = Path(files[args.uav_id % len(files)])
    else:
        print("ERROR: Either --json-file or --input-dir required")
        sys.exit(1)

    # ===== 启动采集 =====
    run_collector(
        uav_id=args.uav_id,
        json_path=json_path,
        out_dir=Path(args.out_dir),
        scale=args.scale,
        time_scale=args.time_scale,
        control_base=args.control_base,
        image_base=args.image_base
    )

