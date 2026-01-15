#!/usr/bin/env python3
"""
分析现有轨迹数据并可视化

用于验证坐标变换正确性
"""

import csv
import json
import math
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

import numpy as np

try:
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def load_csv_data(csv_path: Path) -> List[Dict]:
    """加载CSV数据"""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def analyze_trajectory(data_dir: Path):
    """分析单个轨迹目录的数据"""
    csv_path = data_dir / "data.csv"
    if not csv_path.exists():
        print(f"[ERROR] data.csv not found in {data_dir}")
        return None

    rows = load_csv_data(csv_path)
    if not rows:
        print(f"[ERROR] No data in {csv_path}")
        return None

    # 提取坐标数据
    cmd_x = [float(r.get("cmd_x", 0)) for r in rows]
    cmd_y = [float(r.get("cmd_y", 0)) for r in rows]
    cmd_z = [float(r.get("cmd_z", 0)) for r in rows]

    obs_x = [float(r.get("obs_pos_x", 0)) for r in rows]
    obs_y = [float(r.get("obs_pos_y", 0)) for r in rows]
    obs_z = [float(r.get("obs_pos_z", 0)) for r in rows]

    aligned_x = [float(r.get("obs_aligned_x", 0)) for r in rows]
    aligned_y = [float(r.get("obs_aligned_y", 0)) for r in rows]
    aligned_z = [float(r.get("obs_aligned_z", 0)) for r in rows]

    # 原点偏移
    origin_offset_x = float(rows[0].get("origin_offset_x", 0))
    origin_offset_y = float(rows[0].get("origin_offset_y", 0))
    origin_offset_z = float(rows[0].get("origin_offset_z", 0))

    # 计算误差
    errors = []
    for i in range(len(rows)):
        err = math.sqrt(
            (aligned_x[i] - cmd_x[i])**2 +
            (aligned_y[i] - cmd_y[i])**2 +
            (aligned_z[i] - cmd_z[i])**2
        )
        errors.append(err)

    result = {
        "traj_name": rows[0].get("traj_name", "unknown"),
        "uav_id": rows[0].get("uav_id", "0"),
        "num_points": len(rows),
        "cmd": {"x": cmd_x, "y": cmd_y, "z": cmd_z},
        "obs": {"x": obs_x, "y": obs_y, "z": obs_z},
        "aligned": {"x": aligned_x, "y": aligned_y, "z": aligned_z},
        "origin_offset": (origin_offset_x, origin_offset_y, origin_offset_z),
        "errors": errors,
        "mean_error": np.mean(errors),
        "max_error": max(errors),
        "min_error": min(errors),
        "std_error": np.std(errors),
    }

    return result


def visualize_trajectory_analysis(result: Dict, save_path: Path = None):
    """可视化轨迹分析结果"""
    if not HAS_MATPLOTLIB:
        print("[WARN] matplotlib not available")
        return

    fig = plt.figure(figsize=(20, 15))

    cmd = result["cmd"]
    obs = result["obs"]
    aligned = result["aligned"]
    errors = result["errors"]
    offset = result["origin_offset"]

    # 3D轨迹对比
    ax1 = fig.add_subplot(2, 3, 1, projection='3d')
    ax1.plot(cmd["x"], cmd["y"], cmd["z"], 'b-o', label='Commanded', markersize=4, alpha=0.7)
    ax1.plot(aligned["x"], aligned["y"], aligned["z"], 'r-x', label='Aligned Obs', markersize=4, alpha=0.7)
    ax1.set_xlabel('X (m)')
    ax1.set_ylabel('Y (m)')
    ax1.set_zlabel('Z (m)')
    ax1.set_title(f'3D Trajectory: {result["traj_name"]}')
    ax1.legend()

    # XY平面
    ax2 = fig.add_subplot(2, 3, 2)
    ax2.plot(cmd["x"], cmd["y"], 'b-o', label='Commanded', markersize=4, alpha=0.7)
    ax2.plot(aligned["x"], aligned["y"], 'r-x', label='Aligned Obs', markersize=4, alpha=0.7)
    ax2.set_xlabel('X (m)')
    ax2.set_ylabel('Y (m)')
    ax2.set_title('XY Plane')
    ax2.legend()
    ax2.grid(True)
    ax2.axis('equal')

    # XZ平面 (高度剖面)
    ax3 = fig.add_subplot(2, 3, 3)
    ax3.plot(cmd["x"], cmd["z"], 'b-o', label='Commanded', markersize=4)
    ax3.plot(aligned["x"], aligned["z"], 'r-x', label='Aligned Obs', markersize=4)
    ax3.set_xlabel('X (m)')
    ax3.set_ylabel('Z (m)')
    ax3.set_title('XZ Plane (Height Profile)')
    ax3.legend()
    ax3.grid(True)

    # 误差随航点变化
    ax4 = fig.add_subplot(2, 3, 4)
    ax4.bar(range(len(errors)), errors, color='orange', alpha=0.7)
    ax4.axhline(y=result["mean_error"], color='r', linestyle='--',
                label=f'Mean: {result["mean_error"]:.3f}m')
    ax4.axhline(y=result["max_error"], color='darkred', linestyle=':',
                label=f'Max: {result["max_error"]:.3f}m')
    ax4.set_xlabel('Waypoint Index')
    ax4.set_ylabel('Position Error (m)')
    ax4.set_title('Position Error per Waypoint')
    ax4.legend()
    ax4.grid(True)

    # 各轴误差对比
    ax5 = fig.add_subplot(2, 3, 5)
    indices = range(len(errors))
    err_x = [aligned["x"][i] - cmd["x"][i] for i in indices]
    err_y = [aligned["y"][i] - cmd["y"][i] for i in indices]
    err_z = [aligned["z"][i] - cmd["z"][i] for i in indices]
    ax5.plot(indices, err_x, 'r-', label='X error', alpha=0.7)
    ax5.plot(indices, err_y, 'g-', label='Y error', alpha=0.7)
    ax5.plot(indices, err_z, 'b-', label='Z error', alpha=0.7)
    ax5.set_xlabel('Waypoint Index')
    ax5.set_ylabel('Error (m)')
    ax5.set_title('Error by Axis')
    ax5.legend()
    ax5.grid(True)

    # 统计信息
    ax6 = fig.add_subplot(2, 3, 6)
    ax6.axis('off')
    stats_text = f"""
Analysis Results for: {result['traj_name']}
UAV ID: {result['uav_id']}

Number of waypoints: {result['num_points']}

Origin Offset (applied to align coords):
  X: {offset[0]:.4f} m
  Y: {offset[1]:.4f} m
  Z: {offset[2]:.4f} m

Position Tracking Error:
  Mean:  {result['mean_error']:.4f} m
  Max:   {result['max_error']:.4f} m
  Min:   {result['min_error']:.4f} m
  Std:   {result['std_error']:.4f} m

Coordinate Ranges:
  Cmd X: [{min(cmd['x']):.2f}, {max(cmd['x']):.2f}] m
  Cmd Y: [{min(cmd['y']):.2f}, {max(cmd['y']):.2f}] m
  Cmd Z: [{min(cmd['z']):.2f}, {max(cmd['z']):.2f}] m

Quality Assessment:
  {'PASS' if result['mean_error'] < 1.0 else 'WARN'}: Mean error {'< 1m' if result['mean_error'] < 1.0 else '>= 1m'}
  {'PASS' if result['max_error'] < 2.0 else 'WARN'}: Max error {'< 2m' if result['max_error'] < 2.0 else '>= 2m'}
"""
    ax6.text(0.1, 0.9, stats_text, transform=ax6.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {save_path}")

    plt.show()


def analyze_multiple_trajectories(base_dir: Path, output_dir: Path):
    """分析多个轨迹目录"""
    all_results = []

    # 查找所有data.csv文件
    for csv_file in base_dir.rglob("data.csv"):
        data_dir = csv_file.parent
        print(f"Analyzing: {data_dir}")

        result = analyze_trajectory(data_dir)
        if result:
            all_results.append(result)

            # 可视化每个轨迹
            if HAS_MATPLOTLIB:
                save_name = f"{result['traj_name']}_uav{result['uav_id']}_analysis.png"
                save_path = output_dir / save_name
                visualize_trajectory_analysis(result, save_path)

    if not all_results:
        print("No trajectories analyzed")
        return

    # 汇总统计
    print("\n" + "="*70)
    print("Summary Statistics")
    print("="*70)

    mean_errors = [r["mean_error"] for r in all_results]
    max_errors = [r["max_error"] for r in all_results]

    print(f"Total trajectories analyzed: {len(all_results)}")
    print(f"Average mean error: {np.mean(mean_errors):.4f} m")
    print(f"Average max error: {np.mean(max_errors):.4f} m")
    print(f"Overall max error: {max(max_errors):.4f} m")
    print(f"Overall min mean error: {min(mean_errors):.4f} m")

    # 通过/警告判断
    passed = sum(1 for e in mean_errors if e < 1.0)
    print(f"\nTrajectories with mean error < 1m: {passed}/{len(all_results)}")
    print("="*70)

    # 创建汇总图
    if HAS_MATPLOTLIB and len(all_results) > 1:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # 均值误差分布
        axes[0].bar(range(len(mean_errors)), mean_errors, color='steelblue')
        axes[0].axhline(y=np.mean(mean_errors), color='r', linestyle='--',
                        label=f'Avg: {np.mean(mean_errors):.3f}m')
        axes[0].axhline(y=1.0, color='g', linestyle=':', label='Threshold: 1m')
        axes[0].set_xlabel('Trajectory Index')
        axes[0].set_ylabel('Mean Position Error (m)')
        axes[0].set_title('Mean Error per Trajectory')
        axes[0].legend()
        axes[0].grid(True)

        # 误差箱线图
        all_errors = [r["errors"] for r in all_results]
        axes[1].boxplot(all_errors, vert=True)
        axes[1].set_xlabel('Trajectory Index')
        axes[1].set_ylabel('Position Error (m)')
        axes[1].set_title('Error Distribution per Trajectory')
        axes[1].grid(True)

        plt.tight_layout()
        summary_path = output_dir / "summary_analysis.png"
        plt.savefig(summary_path, dpi=150)
        print(f"Saved summary: {summary_path}")
        plt.show()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze trajectory data")
    parser.add_argument("--data-dir", type=str,
                        default="/home/user/PegasusSimulator-5.1/examples/trajectory_recordings_aligned",
                        help="Base directory containing trajectory data")
    parser.add_argument("--output-dir", type=str,
                        default="/home/user/PegasusSimulator-5.1/examples/validation_results",
                        help="Output directory for analysis results")
    parser.add_argument("--single", type=str, help="Analyze single trajectory directory")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.single:
        data_dir = Path(args.single)
        result = analyze_trajectory(data_dir)
        if result:
            print(f"\nTrajectory: {result['traj_name']}")
            print(f"UAV ID: {result['uav_id']}")
            print(f"Points: {result['num_points']}")
            print(f"Mean Error: {result['mean_error']:.4f} m")
            print(f"Max Error: {result['max_error']:.4f} m")
            print(f"Origin Offset: {result['origin_offset']}")

            if HAS_MATPLOTLIB:
                save_path = output_dir / f"{result['traj_name']}_analysis.png"
                visualize_trajectory_analysis(result, save_path)
    else:
        base_dir = Path(args.data_dir)
        analyze_multiple_trajectories(base_dir, output_dir)


if __name__ == "__main__":
    main()
