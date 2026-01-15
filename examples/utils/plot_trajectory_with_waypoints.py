#!/usr/bin/env python3
"""
轨迹可视化脚本 - 带任务点标注 (plot_trajectory_with_waypoints.py)

功能:
- 绘制原始JSON轨迹、命令轨迹和观测轨迹的对比图
- 在图上显式标注每个任务点/航点的编号
- 分析坐标系转换的正确性
- 识别轨迹跳变位置

坐标系说明:
- JSON输入: ENU坐标系 (x=东, y=北, z=天)
- 数据采集器转换: ENU→NEU (交换x和y)
- 控制器发送: NED坐标系 (x=北, y=东, z=下)
- Isaac Sim观测: ENU坐标系 (x=东, y=北, z=天)
- MAVROS local_position: ENU坐标系 (由MAVROS从NED转换)

使用方法:
  python3 plot_trajectory_with_waypoints.py --recordings-dir ./recordings --traj-name simple_square
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import numpy as np

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not found")


def load_json_trajectory(json_path: Path) -> Tuple[List[float], List[List[float]]]:
    """加载原始JSON轨迹 (ENU坐标系)"""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    raw_logs = data.get('raw_logs', [])
    preprocessed_logs = data.get('preprocessed_logs', [])

    if not raw_logs:
        raise ValueError(f"No raw_logs in {json_path}")
    if not preprocessed_logs:
        raise ValueError(f"No preprocessed_logs in {json_path}")

    init_point = raw_logs[0]
    # 每2个点采样一次(与collector一致)
    preprocessed_logs = preprocessed_logs[::2]

    return init_point, preprocessed_logs


def transform_json_to_cmd_coords(
    init_point: List[float],
    preprocessed_logs: List[List[float]],
    scale: float = 0.01,
    z_down: bool = True
) -> List[Tuple[float, float, float]]:
    """
    模拟trajectory_data_collector的坐标转换逻辑

    转换流程:
    1. ENU→NEU: 交换x和y (在_load_preprocessed_xyz中完成)
    2. 缩放: 乘以scale
    3. 偏移: 加上初始点(也经过NEU转换)
    4. Z方向: z_down时取反

    输出: 命令坐标 (NEU坐标系)
    """
    # 初始点也要ENU→NEU交换
    init_neu_x = init_point[1] * scale  # NEU_x = ENU_y
    init_neu_y = init_point[0] * scale  # NEU_y = ENU_x
    init_z = init_point[2] * scale

    cmd_points = []
    for pt in preprocessed_logs:
        if len(pt) < 3:
            continue
        # ENU→NEU交换
        enu_x, enu_y, enu_z = pt[0], pt[1], pt[2]
        neu_x = enu_y * scale  # NEU_x = ENU_y
        neu_y = enu_x * scale  # NEU_y = ENU_x

        # 加上初始偏移
        cmd_x = init_neu_x + neu_x
        cmd_y = init_neu_y + neu_y

        if z_down:
            cmd_z = init_z - enu_z * scale
        else:
            cmd_z = init_z + enu_z * scale

        cmd_points.append((cmd_x, cmd_y, cmd_z))

    return cmd_points


def load_recorded_data(csv_path: Path) -> Tuple[List[Tuple[float, float, float]],
                                                 List[Tuple[float, float, float]],
                                                 List[Tuple[float, float, float]]]:
    """
    加载采集的数据

    Returns:
        cmd_poses: 命令位置 (cmd_x, cmd_y, cmd_z) - 经过NEU转换的坐标
        obs_poses: 观测位置 (obs_pos_x, obs_pos_y, obs_pos_z) - Isaac Sim ENU坐标
        cmd_in_poses: 原始输入坐标 (cmd_in_x, cmd_in_y, cmd_in_z) - NEU坐标(已交换)
    """
    cmd_poses = []
    obs_poses = []
    cmd_in_poses = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # 命令坐标 (NEU, 经过缩放和偏移)
                cmd_x = float(row.get('cmd_x') or 0)
                cmd_y = float(row.get('cmd_y') or 0)
                cmd_z = float(row.get('cmd_z') or 0)
                cmd_poses.append((cmd_x, cmd_y, cmd_z))

                # 观测坐标 (Isaac Sim ENU)
                obs_x = float(row.get('obs_pos_x') or row.get('pos_x') or 0)
                obs_y = float(row.get('obs_pos_y') or row.get('pos_y') or 0)
                obs_z = float(row.get('obs_pos_z') or row.get('pos_z') or 0)
                obs_poses.append((obs_x, obs_y, obs_z))

                # 原始输入坐标 (NEU, 已交换)
                cmd_in_x = float(row.get('cmd_in_x') or 0)
                cmd_in_y = float(row.get('cmd_in_y') or 0)
                cmd_in_z = float(row.get('cmd_in_z') or 0)
                cmd_in_poses.append((cmd_in_x, cmd_in_y, cmd_in_z))

            except (ValueError, TypeError):
                continue

    return cmd_poses, obs_poses, cmd_in_poses


def load_mavros_data(csv_path: Path) -> List[Tuple[float, float, float]]:
    """
    加载MAVROS位置数据

    MAVROS local_position/pose 使用ENU坐标系
    """
    poses = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # pose_pose_position_x/y/z 是MAVROS ENU坐标
                x = float(row.get('pose_pose_position_x') or 0)
                y = float(row.get('pose_pose_position_y') or 0)
                z = float(row.get('pose_pose_position_z') or 0)
                if x != 0 or y != 0 or z != 0:  # 跳过无效数据
                    poses.append((x, y, z))
            except (ValueError, TypeError):
                continue
    return poses


def load_ulog_trajectory(ulg_path: Path) -> List[Tuple[float, float, float]]:
    """
    从PX4 ULG文件中解析位置轨迹

    使用pyulog解析vehicle_local_position话题
    返回NED坐标系的位置数据
    """
    poses = []
    try:
        from pyulog import ULog
        ulog = ULog(str(ulg_path))

        # 查找vehicle_local_position数据
        for d in ulog.data_list:
            if d.name == 'vehicle_local_position':
                # 获取x, y, z数据
                x_data = d.data.get('x', [])
                y_data = d.data.get('y', [])
                z_data = d.data.get('z', [])

                if len(x_data) > 0:
                    # 下采样以减少数据量
                    step = max(1, len(x_data) // 100)
                    for i in range(0, len(x_data), step):
                        x = float(x_data[i])
                        y = float(y_data[i]) if i < len(y_data) else 0.0
                        z = float(z_data[i]) if i < len(z_data) else 0.0
                        # ULog中的坐标是NED，转换为与其他数据一致的格式
                        # 注意：z在NED中是向下为正，这里保持原样让绘图时处理
                        poses.append((x, y, z))
                break

    except ImportError:
        print("Warning: pyulog not installed, cannot parse ULG files")
    except Exception as e:
        print(f"Warning: Failed to parse ULG file {ulg_path}: {e}")

    return poses


def find_ulg_file(traj_dir: Path) -> Optional[Path]:
    """查找轨迹目录中的ULG文件"""
    # 查找直接在目录下的ulg文件
    for ulg in traj_dir.glob("*.ulg"):
        return ulg
    # 查找子目录中的ulg文件
    for ulg in traj_dir.glob("**/*.ulg"):
        return ulg
    return None


def align_trajectories_by_offset(
    cmd_traj: List[Tuple[float, float, float]],
    obs_traj: List[Tuple[float, float, float]],
    method: str = "first_point"
) -> Tuple[List[Tuple[float, float, float]], Tuple[float, float, float]]:
    """
    将观测轨迹对齐到命令轨迹（使用偏移量）

    Args:
        cmd_traj: 命令轨迹
        obs_traj: 观测轨迹
        method: 对齐方法 ("first_point", "mean", "median")

    Returns:
        aligned_obs: 对齐后的观测轨迹
        offset: 应用的偏移量 (dx, dy, dz)
    """
    if not cmd_traj or not obs_traj:
        return obs_traj, (0.0, 0.0, 0.0)

    n = min(len(cmd_traj), len(obs_traj))

    if method == "first_point":
        offset_x = obs_traj[0][0] - cmd_traj[0][0]
        offset_y = obs_traj[0][1] - cmd_traj[0][1]
        offset_z = obs_traj[0][2] - cmd_traj[0][2]
    elif method == "mean":
        offsets_x = [obs_traj[i][0] - cmd_traj[i][0] for i in range(n)]
        offsets_y = [obs_traj[i][1] - cmd_traj[i][1] for i in range(n)]
        offsets_z = [obs_traj[i][2] - cmd_traj[i][2] for i in range(n)]
        offset_x = np.mean(offsets_x)
        offset_y = np.mean(offsets_y)
        offset_z = np.mean(offsets_z)
    elif method == "median":
        offsets_x = [obs_traj[i][0] - cmd_traj[i][0] for i in range(n)]
        offsets_y = [obs_traj[i][1] - cmd_traj[i][1] for i in range(n)]
        offsets_z = [obs_traj[i][2] - cmd_traj[i][2] for i in range(n)]
        offset_x = np.median(offsets_x)
        offset_y = np.median(offsets_y)
        offset_z = np.median(offsets_z)
    else:
        offset_x, offset_y, offset_z = 0.0, 0.0, 0.0

    aligned = [(x - offset_x, y - offset_y, z - offset_z) for x, y, z in obs_traj]
    return aligned, (offset_x, offset_y, offset_z)


def analyze_coordinate_systems(
    json_traj_enu: List[List[float]],
    cmd_traj: List[Tuple[float, float, float]],
    obs_traj: List[Tuple[float, float, float]],
    scale: float = 0.01
) -> Dict[str, Any]:
    """
    分析坐标系转换的正确性

    检查:
    1. 命令轨迹(NEU)和观测轨迹(ENU)的形状是否一致
    2. 如果将观测轨迹转换为NEU，是否与命令轨迹匹配
    3. 识别可能的坐标系问题
    """
    analysis = {
        "n_json_points": len(json_traj_enu),
        "n_cmd_points": len(cmd_traj),
        "n_obs_points": len(obs_traj),
        "issues": [],
        "suggestions": []
    }

    if not cmd_traj or not obs_traj:
        analysis["issues"].append("Empty trajectory data")
        return analysis

    n = min(len(cmd_traj), len(obs_traj))

    # 计算原始误差 (不转换坐标系，不对齐)
    errors_raw = []
    for i in range(n):
        dx = cmd_traj[i][0] - obs_traj[i][0]
        dy = cmd_traj[i][1] - obs_traj[i][1]
        dz = cmd_traj[i][2] - obs_traj[i][2]
        errors_raw.append(np.sqrt(dx*dx + dy*dy + dz*dz))

    analysis["raw_error_mean"] = np.mean(errors_raw)
    analysis["raw_error_max"] = np.max(errors_raw)

    # 计算坐标偏移（检测坐标系原点差异）
    offset_x = np.median([obs_traj[i][0] - cmd_traj[i][0] for i in range(n)])
    offset_y = np.median([obs_traj[i][1] - cmd_traj[i][1] for i in range(n)])
    offset_z = np.median([obs_traj[i][2] - cmd_traj[i][2] for i in range(n)])
    analysis["origin_offset"] = (offset_x, offset_y, offset_z)

    # 使用偏移对齐后的误差
    obs_aligned, _ = align_trajectories_by_offset(cmd_traj, obs_traj, method="median")
    errors_aligned = []
    for i in range(n):
        dx = cmd_traj[i][0] - obs_aligned[i][0]
        dy = cmd_traj[i][1] - obs_aligned[i][1]
        dz = cmd_traj[i][2] - obs_aligned[i][2]
        errors_aligned.append(np.sqrt(dx*dx + dy*dy + dz*dz))

    analysis["aligned_error_mean"] = np.mean(errors_aligned)
    analysis["aligned_error_max"] = np.max(errors_aligned)

    # 尝试将观测轨迹(ENU)转换为NEU (交换x和y)，看是否能更好匹配
    obs_as_neu = [(y, x, z) for x, y, z in obs_traj]
    errors_swapped = []
    for i in range(n):
        dx = cmd_traj[i][0] - obs_as_neu[i][0]
        dy = cmd_traj[i][1] - obs_as_neu[i][1]
        dz = cmd_traj[i][2] - obs_as_neu[i][2]
        errors_swapped.append(np.sqrt(dx*dx + dy*dy + dz*dz))

    analysis["swapped_error_mean"] = np.mean(errors_swapped)
    analysis["swapped_error_max"] = np.max(errors_swapped)

    # 判断问题类型
    if analysis["aligned_error_mean"] < analysis["raw_error_mean"] * 0.1:
        analysis["issues"].append(f"坐标系原点不一致: 偏移量=({offset_x:.2f}, {offset_y:.2f}, {offset_z:.2f})m")
        analysis["suggestions"].append("使用median方法对齐后误差显著降低")
        analysis["origin_mismatch"] = True
    else:
        analysis["origin_mismatch"] = False

    if analysis["swapped_error_mean"] < analysis["raw_error_mean"] * 0.5:
        analysis["issues"].append("可能存在坐标系方向不匹配 (ENU vs NEU)")
        analysis["suggestions"].append("尝试交换观测数据的X和Y轴")
        analysis["coordinate_mismatch"] = True
    else:
        analysis["coordinate_mismatch"] = False

    # 检查首尾跳变（使用对齐后的误差）
    if len(errors_aligned) > 2:
        first_error = errors_aligned[0]
        last_error = errors_aligned[-1]
        mid_errors = errors_aligned[1:-1] if len(errors_aligned) > 2 else errors_aligned
        mid_mean = np.mean(mid_errors) if mid_errors else 0

        if mid_mean > 0:
            if first_error > mid_mean * 2:
                analysis["issues"].append(f"轨迹起始跳变: 首点误差={first_error:.3f}m, 中间平均={mid_mean:.3f}m")
            if last_error > mid_mean * 2:
                analysis["issues"].append(f"轨迹结束跳变: 末点误差={last_error:.3f}m, 中间平均={mid_mean:.3f}m")

    return analysis


def plot_with_waypoints(
    json_traj_enu: List[List[float]],
    cmd_traj: List[Tuple[float, float, float]],
    obs_traj: List[Tuple[float, float, float]],
    mavros_traj: Optional[List[Tuple[float, float, float]]],
    ulog_traj: Optional[List[Tuple[float, float, float]]],
    traj_name: str,
    output_dir: Path,
    analysis: Dict[str, Any],
    scale: float = 0.01
) -> List[Path]:
    """
    绘制带任务点标注的轨迹对比图

    数据源:
    - cmd_traj: 命令轨迹 (来自trajectory_data_collector)
    - obs_traj: Isaac Sim观测轨迹 (来自/uav/<id>/all接口)
    - mavros_traj: MAVROS位置轨迹 (来自/mavros/local_position/pose话题)
    - ulog_traj: PX4 ULog轨迹 (来自vehicle_local_position)
    """
    if not HAS_MATPLOTLIB:
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    output_files = []

    # 转换数组
    json_arr = np.array([[pt[1]*scale, pt[0]*scale, -pt[2]*scale] for pt in json_traj_enu]) if json_traj_enu else np.array([])  # ENU→NEU with z_down
    cmd_arr = np.array(cmd_traj) if cmd_traj else np.array([])
    obs_arr = np.array(obs_traj) if obs_traj else np.array([])

    # 使用偏移对齐观测轨迹（解决坐标系原点不一致问题）
    obs_aligned, align_offset = align_trajectories_by_offset(cmd_traj, obs_traj, method="median")
    obs_aligned_arr = np.array(obs_aligned) if obs_aligned else np.array([])

    mavros_arr = np.array(mavros_traj) if mavros_traj else np.array([])
    ulog_arr = np.array(ulog_traj) if ulog_traj else np.array([])

    # 对齐MAVROS轨迹
    mavros_aligned_arr = np.array([])
    if len(mavros_arr) > 0 and len(cmd_arr) > 0:
        mavros_aligned, _ = align_trajectories_by_offset(cmd_traj, mavros_traj, method="median")
        mavros_aligned_arr = np.array(mavros_aligned) if mavros_aligned else np.array([])

    # 对齐ULog轨迹 (NED坐标系，需要转换z)
    ulog_aligned_arr = np.array([])
    if len(ulog_arr) > 0 and len(cmd_arr) > 0:
        # ULog是NED，z向下为正，转换为与cmd一致的格式
        ulog_converted = [(x, y, -z) for x, y, z in ulog_traj]
        ulog_aligned, _ = align_trajectories_by_offset(cmd_traj, ulog_converted, method="median")
        ulog_aligned_arr = np.array(ulog_aligned) if ulog_aligned else np.array([])

    # 创建2x2图
    fig, axes = plt.subplots(2, 2, figsize=(18, 16))

    origin_offset = analysis.get("origin_offset", (0, 0, 0))
    aligned_error = analysis.get("aligned_error_mean", 0)

    # 统计数据源
    data_sources = []
    if len(obs_arr) > 0:
        data_sources.append(f"Isaac Sim: {len(obs_arr)} pts")
    if len(mavros_arr) > 0:
        data_sources.append(f"MAVROS: {len(mavros_arr)} pts")
    if len(ulog_arr) > 0:
        data_sources.append(f"ULog: {len(ulog_arr)} pts")

    fig.suptitle(f'Trajectory Analysis: {traj_name}\n'
                 f'Data Sources: {", ".join(data_sources) if data_sources else "None"}\n'
                 f'Raw Error: {analysis.get("raw_error_mean", 0):.3f}m | '
                 f'Aligned Error: {aligned_error:.3f}m | '
                 f'Offset: ({origin_offset[0]:.2f}, {origin_offset[1]:.2f}, {origin_offset[2]:.2f})m',
                 fontsize=11)

    # ========== 左上: XY平面 - 所有数据源对比 ==========
    ax = axes[0, 0]
    ax.set_title('XY Plane - All Data Sources (Aligned)')
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 0], cmd_arr[:, 1], 'g-', label='Command', linewidth=2.5, alpha=0.8)
        ax.scatter(cmd_arr[:, 0], cmd_arr[:, 1], c='green', s=60, marker='s', zorder=10, edgecolors='darkgreen')
        for i, (x, y, z) in enumerate(cmd_traj):
            ax.annotate(f'WP{i}', (x, y), fontsize=8, color='darkgreen', fontweight='bold',
                       xytext=(5, 5), textcoords='offset points')
    if len(obs_aligned_arr) > 0:
        ax.plot(obs_aligned_arr[:, 0], obs_aligned_arr[:, 1], 'r-', label='Isaac Sim (obs)', linewidth=2, alpha=0.7)
        ax.scatter(obs_aligned_arr[:, 0], obs_aligned_arr[:, 1], c='red', s=40, marker='o', zorder=8)
    if len(mavros_aligned_arr) > 0:
        # 下采样MAVROS以便显示
        step = max(1, len(mavros_aligned_arr) // 50)
        mavros_sampled = mavros_aligned_arr[::step]
        ax.plot(mavros_sampled[:, 0], mavros_sampled[:, 1], 'b--', label='MAVROS', linewidth=1.5, alpha=0.6)
        ax.scatter(mavros_sampled[:, 0], mavros_sampled[:, 1], c='blue', s=20, marker='^', zorder=6)
    if len(ulog_aligned_arr) > 0:
        step = max(1, len(ulog_aligned_arr) // 50)
        ulog_sampled = ulog_aligned_arr[::step]
        ax.plot(ulog_sampled[:, 0], ulog_sampled[:, 1], 'm:', label='PX4 ULog', linewidth=1.5, alpha=0.6)
        ax.scatter(ulog_sampled[:, 0], ulog_sampled[:, 1], c='magenta', s=15, marker='x', zorder=5)
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal')

    # ========== 右上: XZ平面 - 高度对比 ==========
    ax = axes[0, 1]
    ax.set_title('XZ Plane - Altitude Comparison (Aligned)')
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 0], cmd_arr[:, 2], 'g-', label='Command', linewidth=2.5, alpha=0.8)
        ax.scatter(cmd_arr[:, 0], cmd_arr[:, 2], c='green', s=60, marker='s', zorder=10)
    if len(obs_aligned_arr) > 0:
        ax.plot(obs_aligned_arr[:, 0], obs_aligned_arr[:, 2], 'r-', label='Isaac Sim', linewidth=2, alpha=0.7)
        ax.scatter(obs_aligned_arr[:, 0], obs_aligned_arr[:, 2], c='red', s=40, marker='o', zorder=8)
    if len(mavros_aligned_arr) > 0:
        step = max(1, len(mavros_aligned_arr) // 50)
        mavros_sampled = mavros_aligned_arr[::step]
        ax.plot(mavros_sampled[:, 0], mavros_sampled[:, 2], 'b--', label='MAVROS', linewidth=1.5, alpha=0.6)
    if len(ulog_aligned_arr) > 0:
        step = max(1, len(ulog_aligned_arr) // 50)
        ulog_sampled = ulog_aligned_arr[::step]
        ax.plot(ulog_sampled[:, 0], ulog_sampled[:, 2], 'm:', label='PX4 ULog', linewidth=1.5, alpha=0.6)
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Z (m)')
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)

    # ========== 左下: YZ平面 ==========
    ax = axes[1, 0]
    ax.set_title('YZ Plane (Aligned)')
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 1], cmd_arr[:, 2], 'g-', label='Command', linewidth=2.5, alpha=0.8)
        ax.scatter(cmd_arr[:, 1], cmd_arr[:, 2], c='green', s=60, marker='s', zorder=10)
    if len(obs_aligned_arr) > 0:
        ax.plot(obs_aligned_arr[:, 1], obs_aligned_arr[:, 2], 'r-', label='Isaac Sim', linewidth=2, alpha=0.7)
        ax.scatter(obs_aligned_arr[:, 1], obs_aligned_arr[:, 2], c='red', s=40, marker='o', zorder=8)
    if len(mavros_aligned_arr) > 0:
        step = max(1, len(mavros_aligned_arr) // 50)
        mavros_sampled = mavros_aligned_arr[::step]
        ax.plot(mavros_sampled[:, 1], mavros_sampled[:, 2], 'b--', label='MAVROS', linewidth=1.5, alpha=0.6)
    if len(ulog_aligned_arr) > 0:
        step = max(1, len(ulog_aligned_arr) // 50)
        ulog_sampled = ulog_aligned_arr[::step]
        ax.plot(ulog_sampled[:, 1], ulog_sampled[:, 2], 'm:', label='PX4 ULog', linewidth=1.5, alpha=0.6)
    ax.set_xlabel('Y (m)')
    ax.set_ylabel('Z (m)')
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)

    # ========== 右下: 分析报告 ==========
    ax = axes[1, 1]
    ax.axis('off')

    issues_text = "\n".join([f"  - {issue}" for issue in analysis.get("issues", [])]) or "  None"
    suggestions_text = "\n".join([f"  - {s}" for s in analysis.get("suggestions", [])]) or "  None"

    # 数据源统计
    data_summary = f"""Data Sources:
  Isaac Sim (obs): {len(obs_arr)} points
  MAVROS: {len(mavros_arr)} points
  PX4 ULog: {len(ulog_arr)} points"""

    report_text = f"""
Trajectory Analysis Report
==================================

{data_summary}

Command Waypoints: {analysis.get('n_cmd_points', 'N/A')} points

Error Analysis (before alignment):
  Mean 3D error: {analysis.get('raw_error_mean', 0):.4f} m
  Max 3D error: {analysis.get('raw_error_max', 0):.4f} m

Error Analysis (after alignment):
  Mean 3D error: {analysis.get('aligned_error_mean', 0):.4f} m
  Max 3D error: {analysis.get('aligned_error_max', 0):.4f} m

Origin Offset (Obs - Cmd):
  X: {origin_offset[0]:.4f} m
  Y: {origin_offset[1]:.4f} m
  Z: {origin_offset[2]:.4f} m

Issues Found:
{issues_text}

Suggestions:
{suggestions_text}
    """
    ax.text(0.05, 0.95, report_text, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()
    output_2d = output_dir / f"{traj_name}_waypoint_analysis.png"
    plt.savefig(output_2d, dpi=150, bbox_inches='tight')
    plt.close(fig)
    output_files.append(output_2d)

    # ========== 创建3D图 ==========
    fig = plt.figure(figsize=(16, 12))
    ax = fig.add_subplot(111, projection='3d')

    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 0], cmd_arr[:, 1], cmd_arr[:, 2],
                'g-', label='Command', linewidth=2.5, alpha=0.8)
        ax.scatter(cmd_arr[:, 0], cmd_arr[:, 1], cmd_arr[:, 2],
                   c='green', s=100, marker='s', depthshade=False, edgecolors='darkgreen')
        for i, (x, y, z) in enumerate(cmd_traj):
            ax.text(x, y, z, f'  WP{i}', fontsize=9, color='darkgreen', fontweight='bold')

    if len(obs_aligned_arr) > 0:
        ax.plot(obs_aligned_arr[:, 0], obs_aligned_arr[:, 1], obs_aligned_arr[:, 2],
                'r-', label='Isaac Sim (aligned)', linewidth=2, alpha=0.7)
        ax.scatter(obs_aligned_arr[:, 0], obs_aligned_arr[:, 1], obs_aligned_arr[:, 2],
                   c='red', s=60, marker='o', depthshade=False)

    if len(mavros_aligned_arr) > 0:
        step = max(1, len(mavros_aligned_arr) // 100)
        mavros_sampled = mavros_aligned_arr[::step]
        ax.plot(mavros_sampled[:, 0], mavros_sampled[:, 1], mavros_sampled[:, 2],
                'b--', label='MAVROS (aligned)', linewidth=1.5, alpha=0.6)

    if len(ulog_aligned_arr) > 0:
        step = max(1, len(ulog_aligned_arr) // 100)
        ulog_sampled = ulog_aligned_arr[::step]
        ax.plot(ulog_sampled[:, 0], ulog_sampled[:, 1], ulog_sampled[:, 2],
                'm:', label='PX4 ULog (aligned)', linewidth=1.5, alpha=0.6)

    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_zlabel('Z (m)')
    ax.set_title(f'3D Trajectory: {traj_name}\n'
                 f'Data Sources: {", ".join(data_sources) if data_sources else "None"}\n'
                 f'Aligned Error: {aligned_error:.4f}m (mean)')
    ax.legend(loc='best')

    plt.tight_layout()
    output_3d = output_dir / f"{traj_name}_waypoint_3d.png"
    plt.savefig(output_3d, dpi=150, bbox_inches='tight')
    plt.close(fig)
    output_files.append(output_3d)

    return output_files


def process_trajectory(
    recordings_dir: Path,
    traj_name: str,
    json_dir: Path,
    output_dir: Path,
    scale: float = 0.01,
    z_down: bool = True
) -> Dict[str, Any]:
    """处理单个轨迹"""
    traj_dir = recordings_dir / traj_name
    if not traj_dir.is_dir():
        return {"error": f"Directory not found: {traj_dir}"}

    results = {"traj_name": traj_name, "uavs": {}}

    for uav_dir in sorted(traj_dir.iterdir()):
        if not uav_dir.is_dir() or not uav_dir.name.startswith('uav'):
            continue

        try:
            uav_id = int(uav_dir.name.replace('uav', ''))
        except ValueError:
            continue

        data_csv = uav_dir / "data.csv"
        mavros_csv = uav_dir / "mavros_data.csv"

        if not data_csv.exists():
            continue

        # 从CSV获取JSON路径
        json_path = None
        with open(data_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                json_path = row.get('traj_json')
                break

        if not json_path:
            continue

        json_path = Path(json_path)
        if not json_path.exists():
            json_path = json_dir / f"{traj_name}.json"

        if not json_path.exists():
            results["uavs"][uav_id] = {"error": f"JSON not found: {json_path}"}
            continue

        try:
            # 加载数据
            init_point, preprocessed_logs = load_json_trajectory(json_path)
            cmd_traj, obs_traj, cmd_in_traj = load_recorded_data(data_csv)

            mavros_traj = None
            if mavros_csv.exists():
                mavros_traj = load_mavros_data(mavros_csv)

            # 加载ULog数据
            ulog_traj = None
            ulg_path = find_ulg_file(uav_dir)
            if ulg_path:
                print(f"  Found ULG file: {ulg_path}")
                ulog_traj = load_ulog_trajectory(ulg_path)

            # 分析坐标系
            analysis = analyze_coordinate_systems(
                preprocessed_logs, cmd_traj, obs_traj, scale
            )

            # 绘图
            plot_files = plot_with_waypoints(
                preprocessed_logs, cmd_traj, obs_traj, mavros_traj, ulog_traj,
                f"{traj_name}_uav{uav_id}", output_dir, analysis, scale
            )

            results["uavs"][uav_id] = {
                "json_points": len(preprocessed_logs),
                "cmd_points": len(cmd_traj),
                "obs_points": len(obs_traj),
                "mavros_points": len(mavros_traj) if mavros_traj else 0,
                "ulog_points": len(ulog_traj) if ulog_traj else 0,
                "ulg_file": str(ulg_path) if ulg_path else None,
                "analysis": analysis,
                "plots": [str(p) for p in plot_files]
            }

        except Exception as e:
            import traceback
            results["uavs"][uav_id] = {"error": str(e), "traceback": traceback.format_exc()}

    return results


def main():
    parser = argparse.ArgumentParser(description='Plot trajectory with waypoint markers')
    parser.add_argument('--recordings-dir', type=str,
                        default='./trajectory_recordings',
                        help='Directory containing recorded trajectories')
    parser.add_argument('--json-dir', type=str,
                        default='./test_traj_batch',
                        help='Directory containing source JSON files')
    parser.add_argument('--output-dir', type=str,
                        default='./trajectory_analysis',
                        help='Output directory for analysis plots')
    parser.add_argument('--traj-name', type=str, default=None,
                        help='Process specific trajectory only')
    parser.add_argument('--scale', type=float, default=0.01,
                        help='Coordinate scale factor')
    parser.add_argument('--max-trajs', type=int, default=0,
                        help='Maximum trajectories to process (0=all)')
    args = parser.parse_args()

    recordings_dir = Path(args.recordings_dir).resolve()
    json_dir = Path(args.json_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not recordings_dir.exists():
        print(f"Error: Recordings directory not found: {recordings_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    if args.traj_name:
        traj_names = [args.traj_name]
    else:
        traj_names = sorted([d.name for d in recordings_dir.iterdir()
                            if d.is_dir() and not d.name.startswith('.')])

    if args.max_trajs > 0:
        traj_names = traj_names[:args.max_trajs]

    all_results = {"trajectories": {}, "summary": {}}

    for i, traj_name in enumerate(traj_names):
        print(f"[{i+1}/{len(traj_names)}] Processing: {traj_name}")
        result = process_trajectory(
            recordings_dir, traj_name, json_dir, output_dir,
            scale=args.scale, z_down=True
        )
        all_results["trajectories"][traj_name] = result

        # 打印分析结果
        for uav_id, uav_result in result.get("uavs", {}).items():
            if "analysis" in uav_result:
                analysis = uav_result["analysis"]
                print(f"  UAV{uav_id}:")
                print(f"    Raw error: {analysis.get('raw_error_mean', 0):.4f}m (mean)")
                print(f"    After swap: {analysis.get('swapped_error_mean', 0):.4f}m (mean)")
                if analysis.get("issues"):
                    for issue in analysis["issues"]:
                        print(f"    Issue: {issue}")

    # 保存结果
    results_json = output_dir / "analysis_results.json"
    with open(results_json, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nResults saved to: {results_json}")


if __name__ == "__main__":
    main()
