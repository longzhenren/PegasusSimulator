#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
轨迹对比绘图脚本 (plot_trajectory_comparison.py)

功能:
- 批量绘制原始JSON轨迹与采集的位姿数据对比图
- 自动处理坐标系变换(ENU→NEU)和缩放(scale)
- 生成2D(XY, XZ, YZ)和3D对比图

使用方法:
  python3 plot_trajectory_comparison.py --recordings-dir ./trajectory_recordings --output-dir ./trajectory_plots
  python3 plot_trajectory_comparison.py --recordings-dir ./trajectory_recordings --traj-name 2025-03-14_14-47-23_log
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
    matplotlib.use('Agg')  # 无头模式
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not found, will skip plotting")


def load_json_trajectory(json_path: Path) -> Tuple[List[float], List[List[float]]]:
    """加载原始JSON轨迹

    Returns:
        init_point: [x, y, z, roll, yaw, pitch] 初始点 (ENU)
        preprocessed_logs: [[x, y, z, roll, yaw, pitch], ...] 轨迹点 (ENU相对坐标)
    """
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


def transform_enu_to_world(
    init_point: List[float],
    preprocessed_logs: List[List[float]],
    scale: float = 0.01,
    z_down: bool = True
) -> List[Tuple[float, float, float]]:
    """将ENU相对坐标转换为世界坐标 (NEU)

    变换过程:
    1. ENU→NEU: 交换x和y
    2. 缩放: 乘以scale
    3. 偏移: 加上初始点
    4. Z方向: z_down时取反

    Args:
        init_point: [x, y, z, roll, yaw, pitch] ENU初始点
        preprocessed_logs: [[x, y, z, roll, yaw, pitch], ...] ENU相对坐标
        scale: 缩放因子
        z_down: 是否Z轴向下

    Returns:
        world_points: [(x, y, z), ...] 世界坐标点 (NEU)
    """
    # 初始点也要ENU→NEU交换
    init_x = init_point[1] * scale  # NEU_x = ENU_y
    init_y = init_point[0] * scale  # NEU_y = ENU_x
    init_z = init_point[2] * scale

    world_points = []
    for pt in preprocessed_logs:
        if len(pt) < 3:
            continue
        # ENU→NEU交换
        enu_x, enu_y, enu_z = pt[0], pt[1], pt[2]
        neu_x = enu_y * scale  # NEU_x = ENU_y
        neu_y = enu_x * scale  # NEU_y = ENU_x

        # 加上初始偏移
        world_x = init_x + neu_x
        world_y = init_y + neu_y

        if z_down:
            world_z = init_z - enu_z * scale
        else:
            world_z = init_z + enu_z * scale

        world_points.append((world_x, world_y, world_z))

    return world_points


def load_recorded_poses(csv_path: Path) -> List[Tuple[float, float, float]]:
    """加载采集的位姿数据

    Returns:
        poses: [(x, y, z), ...] 观测位置
    """
    poses = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                x = float(row.get('pos_x') or row.get('obs_pos_x') or 0)
                y = float(row.get('pos_y') or row.get('obs_pos_y') or 0)
                z = float(row.get('pos_z') or row.get('obs_pos_z') or 0)
                poses.append((x, y, z))
            except (ValueError, TypeError):
                continue
    return poses


def load_command_poses(csv_path: Path) -> List[Tuple[float, float, float]]:
    """加载命令位置数据

    Returns:
        cmd_poses: [(x, y, z), ...] 命令位置
    """
    cmd_poses = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                x = float(row.get('cmd_x') or 0)
                y = float(row.get('cmd_y') or 0)
                z = float(row.get('cmd_z') or 0)
                cmd_poses.append((x, y, z))
            except (ValueError, TypeError):
                continue
    return cmd_poses


def align_trajectories(
    reference: List[Tuple[float, float, float]],
    to_align: List[Tuple[float, float, float]],
    method: str = "first_point"
) -> Tuple[List[Tuple[float, float, float]], Tuple[float, float, float]]:
    """对齐轨迹

    将 to_align 轨迹平移，使其与 reference 轨迹对齐。

    Args:
        reference: 参考轨迹（不变）
        to_align: 需要对齐的轨迹
        method: 对齐方法
            - "first_point": 使用第一个点对齐
            - "mean": 使用所有点的平均偏移量
            - "median": 使用所有点偏移量的中位数（更robust）

    Returns:
        aligned: 对齐后的轨迹
        offset: 应用的偏移量 (dx, dy, dz)
    """
    if not reference or not to_align:
        return to_align, (0.0, 0.0, 0.0)

    n = min(len(reference), len(to_align))

    if method == "first_point":
        # 使用第一个点计算偏移
        offset_x = to_align[0][0] - reference[0][0]
        offset_y = to_align[0][1] - reference[0][1]
        offset_z = to_align[0][2] - reference[0][2]
    elif method == "mean":
        # 使用所有点的平均偏移量
        offsets_x = [to_align[i][0] - reference[i][0] for i in range(n)]
        offsets_y = [to_align[i][1] - reference[i][1] for i in range(n)]
        offsets_z = [to_align[i][2] - reference[i][2] for i in range(n)]
        offset_x = np.mean(offsets_x)
        offset_y = np.mean(offsets_y)
        offset_z = np.mean(offsets_z)
    elif method == "median":
        # 使用中位数（对异常值更robust）
        offsets_x = [to_align[i][0] - reference[i][0] for i in range(n)]
        offsets_y = [to_align[i][1] - reference[i][1] for i in range(n)]
        offsets_z = [to_align[i][2] - reference[i][2] for i in range(n)]
        offset_x = np.median(offsets_x)
        offset_y = np.median(offsets_y)
        offset_z = np.median(offsets_z)
    else:
        raise ValueError(f"Unknown alignment method: {method}")

    # 应用偏移对齐
    aligned = [(x - offset_x, y - offset_y, z - offset_z) for x, y, z in to_align]

    return aligned, (offset_x, offset_y, offset_z)


def compute_errors(
    json_traj: List[Tuple[float, float, float]],
    observed: List[Tuple[float, float, float]],
    align_start: bool = True,
    align_method: str = "median"
) -> Dict[str, float]:
    """计算轨迹误差统计

    Args:
        json_traj: 命令/参考轨迹
        observed: 观测轨迹
        align_start: 是否先对齐再计算误差（处理坐标系偏移）
        align_method: 对齐方法 ("first_point", "mean", "median")

    Returns:
        errors: 误差统计字典
    """
    if not json_traj or not observed:
        return {"error": "empty data"}

    # 对齐轨迹（处理 PX4 本地坐标 vs Isaac Sim 世界坐标的偏移）
    if align_start:
        observed_aligned, offset = align_trajectories(json_traj, observed, method=align_method)
    else:
        observed_aligned = observed
        offset = (0.0, 0.0, 0.0)

    n = min(len(json_traj), len(observed_aligned))
    errors_3d = []
    errors_xy = []
    errors_z = []

    for i in range(n):
        jx, jy, jz = json_traj[i]
        ox, oy, oz = observed_aligned[i]

        dx = jx - ox
        dy = jy - oy
        dz = jz - oz

        errors_3d.append(np.sqrt(dx**2 + dy**2 + dz**2))
        errors_xy.append(np.sqrt(dx**2 + dy**2))
        errors_z.append(abs(dz))

    return {
        "n_points": n,
        "mean_3d_error": np.mean(errors_3d),
        "max_3d_error": np.max(errors_3d),
        "std_3d_error": np.std(errors_3d),
        "mean_xy_error": np.mean(errors_xy),
        "mean_z_error": np.mean(errors_z),
        "alignment_offset": offset,
    }


def plot_trajectory_comparison(
    json_traj: List[Tuple[float, float, float]],
    cmd_traj: List[Tuple[float, float, float]],
    observed_traj: List[Tuple[float, float, float]],
    traj_name: str,
    uav_id: int,
    output_dir: Path,
    errors: Dict[str, float]
) -> List[Path]:
    """绘制轨迹对比图

    生成:
    - 2D XY平面图
    - 2D XZ平面图
    - 2D YZ平面图
    - 3D轨迹图

    注意：observed_traj 会被自动对齐到 cmd_traj（或 json_traj）的起点，
    以处理 PX4 本地坐标和 Isaac Sim 世界坐标之间的偏移。
    """
    if not HAS_MATPLOTLIB:
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    output_files = []

    # 选择参考轨迹用于对齐（优先使用命令轨迹，因为它是实际发送的坐标）
    reference_traj = cmd_traj if cmd_traj else json_traj

    # 对齐 observed 轨迹到参考轨迹（使用 median 方法更 robust）
    if reference_traj and observed_traj:
        observed_aligned, alignment_offset = align_trajectories(reference_traj, observed_traj, method="median")
    else:
        observed_aligned = observed_traj
        alignment_offset = (0.0, 0.0, 0.0)

    # 转换为numpy数组
    json_arr = np.array(json_traj) if json_traj else np.array([])
    cmd_arr = np.array(cmd_traj) if cmd_traj else np.array([])
    obs_arr = np.array(observed_aligned) if observed_aligned else np.array([])

    # 颜色配置
    json_color = 'blue'
    cmd_color = 'green'
    obs_color = 'red'

    # 创建2D对比图 (2x2布局)
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    fig.suptitle(f'Trajectory Comparison: {traj_name} (UAV{uav_id})\n'
                 f'Mean 3D Error: {errors.get("mean_3d_error", 0):.3f}m, '
                 f'Max 3D Error: {errors.get("max_3d_error", 0):.3f}m\n'
                 f'(Observed aligned, offset: ({alignment_offset[0]:.2f}, {alignment_offset[1]:.2f}, {alignment_offset[2]:.2f})m)',
                 fontsize=11)

    # XY平面
    ax = axes[0, 0]
    if len(json_arr) > 0:
        ax.plot(json_arr[:, 0], json_arr[:, 1], 'b-', label='JSON (transformed)', linewidth=2, alpha=0.7)
        ax.scatter(json_arr[0, 0], json_arr[0, 1], c='blue', s=100, marker='o', zorder=5)
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 0], cmd_arr[:, 1], 'g--', label='Command', linewidth=1.5, alpha=0.7)
    if len(obs_arr) > 0:
        ax.plot(obs_arr[:, 0], obs_arr[:, 1], 'r-', label='Observed', linewidth=2, alpha=0.7)
        ax.scatter(obs_arr[0, 0], obs_arr[0, 1], c='red', s=100, marker='x', zorder=5)
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_title('XY Plane (Top View)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal')

    # XZ平面
    ax = axes[0, 1]
    if len(json_arr) > 0:
        ax.plot(json_arr[:, 0], json_arr[:, 2], 'b-', label='JSON (transformed)', linewidth=2, alpha=0.7)
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 0], cmd_arr[:, 2], 'g--', label='Command', linewidth=1.5, alpha=0.7)
    if len(obs_arr) > 0:
        ax.plot(obs_arr[:, 0], obs_arr[:, 2], 'r-', label='Observed', linewidth=2, alpha=0.7)
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Z (m)')
    ax.set_title('XZ Plane (Side View)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # YZ平面
    ax = axes[1, 0]
    if len(json_arr) > 0:
        ax.plot(json_arr[:, 1], json_arr[:, 2], 'b-', label='JSON (transformed)', linewidth=2, alpha=0.7)
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 1], cmd_arr[:, 2], 'g--', label='Command', linewidth=1.5, alpha=0.7)
    if len(obs_arr) > 0:
        ax.plot(obs_arr[:, 1], obs_arr[:, 2], 'r-', label='Observed', linewidth=2, alpha=0.7)
    ax.set_xlabel('Y (m)')
    ax.set_ylabel('Z (m)')
    ax.set_title('YZ Plane (Side View)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 误差统计文本
    ax = axes[1, 1]
    ax.axis('off')
    offset = errors.get('alignment_offset', (0, 0, 0))
    stats_text = f"""
    Trajectory Statistics:
    ----------------------
    Number of points: {errors.get('n_points', 'N/A')}

    3D Position Error (after alignment):
      Mean: {errors.get('mean_3d_error', 0):.4f} m
      Max:  {errors.get('max_3d_error', 0):.4f} m
      Std:  {errors.get('std_3d_error', 0):.4f} m

    XY Plane Error:
      Mean: {errors.get('mean_xy_error', 0):.4f} m

    Z Error:
      Mean: {errors.get('mean_z_error', 0):.4f} m

    Alignment Offset (Isaac - PX4):
      X: {offset[0]:.4f} m
      Y: {offset[1]:.4f} m
      Z: {offset[2]:.4f} m
    """
    ax.text(0.1, 0.9, stats_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()
    output_2d = output_dir / f"{traj_name}_uav{uav_id}_2d.png"
    plt.savefig(output_2d, dpi=150, bbox_inches='tight')
    plt.close(fig)
    output_files.append(output_2d)

    # 创建3D图
    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111, projection='3d')

    if len(json_arr) > 0:
        ax.plot(json_arr[:, 0], json_arr[:, 1], json_arr[:, 2],
                'b-', label='JSON (transformed)', linewidth=2, alpha=0.7)
        ax.scatter(json_arr[0, 0], json_arr[0, 1], json_arr[0, 2],
                   c='blue', s=100, marker='o', label='Start (JSON)')
    if len(cmd_arr) > 0:
        ax.plot(cmd_arr[:, 0], cmd_arr[:, 1], cmd_arr[:, 2],
                'g--', label='Command', linewidth=1.5, alpha=0.7)
    if len(obs_arr) > 0:
        ax.plot(obs_arr[:, 0], obs_arr[:, 1], obs_arr[:, 2],
                'r-', label='Observed', linewidth=2, alpha=0.7)
        ax.scatter(obs_arr[0, 0], obs_arr[0, 1], obs_arr[0, 2],
                   c='red', s=100, marker='x', label='Start (Observed)')

    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_zlabel('Z (m)')
    ax.set_title(f'3D Trajectory: {traj_name} (UAV{uav_id})\n'
                 f'Mean Error: {errors.get("mean_3d_error", 0):.3f}m')
    ax.legend()

    plt.tight_layout()
    output_3d = output_dir / f"{traj_name}_uav{uav_id}_3d.png"
    plt.savefig(output_3d, dpi=150, bbox_inches='tight')
    plt.close(fig)
    output_files.append(output_3d)

    return output_files


def process_single_trajectory(
    recordings_dir: Path,
    traj_name: str,
    json_base_dir: Path,
    output_dir: Path,
    scale: float = 0.01,
    z_down: bool = True
) -> Dict[str, Any]:
    """处理单条轨迹的所有UAV数据"""
    traj_dir = recordings_dir / traj_name
    if not traj_dir.is_dir():
        return {"error": f"Directory not found: {traj_dir}"}

    results = {"traj_name": traj_name, "uavs": {}}

    # 查找所有UAV子目录
    for uav_dir in sorted(traj_dir.iterdir()):
        if not uav_dir.is_dir() or not uav_dir.name.startswith('uav'):
            continue

        try:
            uav_id = int(uav_dir.name.replace('uav', ''))
        except ValueError:
            continue

        # 查找数据文件
        data_csv = uav_dir / "data.csv"
        pose_csv = uav_dir / "all_pose_data.csv"

        if not data_csv.exists() and not pose_csv.exists():
            continue

        csv_to_use = data_csv if data_csv.exists() else pose_csv

        # 从CSV获取JSON路径
        json_path = None
        with open(csv_to_use, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                json_path = row.get('traj_json')
                break

        if not json_path:
            continue

        json_path = Path(json_path)
        if not json_path.exists():
            # 尝试在json_base_dir中查找
            json_path = json_base_dir / f"{traj_name}.json"

        if not json_path.exists():
            results["uavs"][uav_id] = {"error": f"JSON not found: {json_path}"}
            continue

        try:
            # 加载数据
            init_point, preprocessed_logs = load_json_trajectory(json_path)
            json_traj = transform_enu_to_world(init_point, preprocessed_logs, scale, z_down)
            observed_traj = load_recorded_poses(csv_to_use)
            cmd_traj = load_command_poses(csv_to_use) if data_csv.exists() else []

            # 计算误差（优先使用命令轨迹作为参考，因为它是实际发送给UAV的坐标）
            reference_traj = cmd_traj if cmd_traj else json_traj
            errors = compute_errors(reference_traj, observed_traj)

            # 绘图
            plot_files = plot_trajectory_comparison(
                json_traj, cmd_traj, observed_traj,
                traj_name, uav_id, output_dir, errors
            )

            results["uavs"][uav_id] = {
                "json_points": len(json_traj),
                "observed_points": len(observed_traj),
                "errors": errors,
                "plots": [str(p) for p in plot_files]
            }

        except Exception as e:
            results["uavs"][uav_id] = {"error": str(e)}

    return results


def batch_process(
    recordings_dir: Path,
    json_base_dir: Path,
    output_dir: Path,
    traj_names: Optional[List[str]] = None,
    scale: float = 0.01,
    z_down: bool = True,
    max_trajs: int = 0
) -> Dict[str, Any]:
    """批量处理所有轨迹"""
    all_results = {"trajectories": {}, "summary": {}}

    if traj_names:
        dirs_to_process = [recordings_dir / name for name in traj_names]
    else:
        dirs_to_process = sorted([d for d in recordings_dir.iterdir()
                                  if d.is_dir() and not d.name.startswith('.')])

    if max_trajs > 0:
        dirs_to_process = dirs_to_process[:max_trajs]

    total_trajs = 0
    total_errors = []

    for i, traj_dir in enumerate(dirs_to_process):
        traj_name = traj_dir.name
        print(f"[{i+1}/{len(dirs_to_process)}] Processing: {traj_name}")

        result = process_single_trajectory(
            recordings_dir, traj_name, json_base_dir, output_dir, scale, z_down
        )

        all_results["trajectories"][traj_name] = result

        # 收集误差统计
        for uav_id, uav_result in result.get("uavs", {}).items():
            if "errors" in uav_result and "mean_3d_error" in uav_result["errors"]:
                total_errors.append(uav_result["errors"]["mean_3d_error"])
                total_trajs += 1

    # 汇总统计
    if total_errors:
        all_results["summary"] = {
            "total_trajectories": total_trajs,
            "mean_error_all": np.mean(total_errors),
            "max_error_all": np.max(total_errors),
            "min_error_all": np.min(total_errors),
            "std_error_all": np.std(total_errors)
        }

    return all_results


def main():
    parser = argparse.ArgumentParser(description='Plot trajectory comparison')
    parser.add_argument('--recordings-dir', type=str,
                        default='./trajectory_recordings',
                        help='Directory containing recorded trajectories')
    parser.add_argument('--json-dir', type=str,
                        default='/home/user/uav-data/drone/uav-flow-sim/train_data/extracted_json_files',
                        help='Directory containing source JSON files')
    parser.add_argument('--output-dir', type=str,
                        default='./trajectory_plots',
                        help='Output directory for plots')
    parser.add_argument('--traj-name', type=str, default=None,
                        help='Process specific trajectory only')
    parser.add_argument('--scale', type=float, default=0.01,
                        help='Coordinate scale factor')
    parser.add_argument('--z-down', action='store_true', default=True,
                        help='Z axis down mode')
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

    traj_names = [args.traj_name] if args.traj_name else None

    print(f"Processing trajectories from: {recordings_dir}")
    print(f"JSON source: {json_dir}")
    print(f"Output: {output_dir}")

    results = batch_process(
        recordings_dir, json_dir, output_dir,
        traj_names=traj_names,
        scale=args.scale,
        z_down=args.z_down,
        max_trajs=args.max_trajs
    )

    # 打印汇总
    print("\n=== Summary ===")
    summary = results.get("summary", {})
    if summary:
        print(f"Total trajectories: {summary.get('total_trajectories', 0)}")
        print(f"Mean 3D error: {summary.get('mean_error_all', 0):.4f} m")
        print(f"Max 3D error: {summary.get('max_error_all', 0):.4f} m")
        print(f"Min 3D error: {summary.get('min_error_all', 0):.4f} m")

    # 保存结果JSON
    results_json = output_dir / "comparison_results.json"
    with open(results_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {results_json}")


if __name__ == "__main__":
    main()
