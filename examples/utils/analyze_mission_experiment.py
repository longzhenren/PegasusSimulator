#!/usr/bin/env python3
"""
Waypoint Mission 模式实验分析脚本

功能:
1. 分析采集的轨迹数据
2. 计算命令位置与实际位置的误差
3. 生成可视化对比图

使用方法:
  # 运行实验（在仿真环境启动后执行）:
  python3 trajectory_data_collector.py \
    --input-dir ./test_mission_input \
    --out-dir ./test_mission_output \
    --mission-mode \
    --config ./multi_uav_config.json

  # 分析结果:
  python3 analyze_mission_experiment.py --data-dir ./test_mission_output
"""

import argparse
import csv
import json
import math
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not available")


def load_data_csv(csv_path: Path) -> List[Dict[str, Any]]:
    """加载data.csv文件"""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def analyze_trajectory(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """分析单条轨迹数据"""
    errors = []
    cmd_positions = []
    obs_positions = []
    aligned_positions = []

    for row in rows:
        try:
            # 命令位置
            cmd_x = float(row.get('cmd_x', 0))
            cmd_y = float(row.get('cmd_y', 0))
            cmd_z = float(row.get('cmd_z', 0))
            cmd_positions.append((cmd_x, cmd_y, cmd_z))

            # 原始观测位置
            obs_x = float(row.get('obs_pos_x', 0))
            obs_y = float(row.get('obs_pos_y', 0))
            obs_z = float(row.get('obs_pos_z', 0))
            obs_positions.append((obs_x, obs_y, obs_z))

            # 对齐后位置
            aligned_x = float(row.get('obs_aligned_x', 0))
            aligned_y = float(row.get('obs_aligned_y', 0))
            aligned_z = float(row.get('obs_aligned_z', 0))
            aligned_positions.append((aligned_x, aligned_y, aligned_z))

            # 计算误差
            dx = cmd_x - aligned_x
            dy = cmd_y - aligned_y
            dz = cmd_z - aligned_z
            error_3d = math.sqrt(dx*dx + dy*dy + dz*dz)
            error_xy = math.sqrt(dx*dx + dy*dy)
            errors.append({
                'step': int(row.get('step_idx', 0)),
                'error_3d': error_3d,
                'error_xy': error_xy,
                'error_z': abs(dz),
                'dx': dx,
                'dy': dy,
                'dz': dz
            })
        except (ValueError, KeyError) as e:
            continue

    if not errors:
        return {'error': 'No valid data points'}

    # 统计分析
    error_3d_values = [e['error_3d'] for e in errors]
    error_xy_values = [e['error_xy'] for e in errors]

    return {
        'num_points': len(errors),
        'cmd_positions': cmd_positions,
        'obs_positions': obs_positions,
        'aligned_positions': aligned_positions,
        'errors': errors,
        'stats': {
            'error_3d': {
                'min': min(error_3d_values),
                'max': max(error_3d_values),
                'mean': sum(error_3d_values) / len(error_3d_values),
                'p50': sorted(error_3d_values)[len(error_3d_values)//2],
                'p90': sorted(error_3d_values)[int(len(error_3d_values)*0.9)],
            },
            'error_xy': {
                'min': min(error_xy_values),
                'max': max(error_xy_values),
                'mean': sum(error_xy_values) / len(error_xy_values),
            }
        }
    }


def plot_trajectory_comparison(
    analysis: Dict[str, Any],
    traj_name: str,
    output_dir: Path
) -> List[Path]:
    """生成轨迹对比可视化图"""
    if not HAS_MATPLOTLIB:
        print("matplotlib not available, skipping plots")
        return []

    cmd_pos = np.array(analysis['cmd_positions'])
    aligned_pos = np.array(analysis['aligned_positions'])
    errors = analysis['errors']

    output_files = []
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. XY平面轨迹对比图
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.plot(cmd_pos[:, 0], cmd_pos[:, 1], 'b-o', label='Command', linewidth=2, markersize=8)
    ax.plot(aligned_pos[:, 0], aligned_pos[:, 1], 'r-s', label='Actual (aligned)', linewidth=2, markersize=8)

    # 绘制误差连线
    for i in range(len(cmd_pos)):
        ax.plot([cmd_pos[i, 0], aligned_pos[i, 0]],
                [cmd_pos[i, 1], aligned_pos[i, 1]],
                'g--', alpha=0.5, linewidth=1)

    ax.set_xlabel('X (m)', fontsize=12)
    ax.set_ylabel('Y (m)', fontsize=12)
    ax.set_title(f'Trajectory Comparison (XY) - {traj_name}', fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal')

    xy_path = output_dir / f'{traj_name}_xy.png'
    plt.savefig(xy_path, dpi=150, bbox_inches='tight')
    plt.close()
    output_files.append(xy_path)

    # 2. 3D轨迹对比图
    fig = plt.figure(figsize=(12, 9))
    ax = fig.add_subplot(111, projection='3d')
    ax.plot(cmd_pos[:, 0], cmd_pos[:, 1], cmd_pos[:, 2], 'b-o', label='Command', linewidth=2, markersize=8)
    ax.plot(aligned_pos[:, 0], aligned_pos[:, 1], aligned_pos[:, 2], 'r-s', label='Actual (aligned)', linewidth=2, markersize=8)

    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_zlabel('Z (m)')
    ax.set_title(f'Trajectory Comparison (3D) - {traj_name}', fontsize=14)
    ax.legend()

    xyz_path = output_dir / f'{traj_name}_3d.png'
    plt.savefig(xyz_path, dpi=150, bbox_inches='tight')
    plt.close()
    output_files.append(xyz_path)

    # 3. 误差分布图
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    steps = [e['step'] for e in errors]
    error_3d = [e['error_3d'] for e in errors]
    error_xy = [e['error_xy'] for e in errors]
    error_z = [e['error_z'] for e in errors]

    # 3D误差随步骤变化
    axes[0, 0].bar(steps, error_3d, color='steelblue', alpha=0.7)
    axes[0, 0].axhline(y=analysis['stats']['error_3d']['mean'], color='r', linestyle='--', label=f"Mean: {analysis['stats']['error_3d']['mean']:.3f}m")
    axes[0, 0].set_xlabel('Waypoint Index')
    axes[0, 0].set_ylabel('3D Error (m)')
    axes[0, 0].set_title('3D Position Error per Waypoint')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)

    # XY误差随步骤变化
    axes[0, 1].bar(steps, error_xy, color='green', alpha=0.7)
    axes[0, 1].axhline(y=analysis['stats']['error_xy']['mean'], color='r', linestyle='--', label=f"Mean: {analysis['stats']['error_xy']['mean']:.3f}m")
    axes[0, 1].set_xlabel('Waypoint Index')
    axes[0, 1].set_ylabel('XY Error (m)')
    axes[0, 1].set_title('Horizontal Position Error per Waypoint')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)

    # Z误差随步骤变化
    axes[1, 0].bar(steps, error_z, color='orange', alpha=0.7)
    axes[1, 0].set_xlabel('Waypoint Index')
    axes[1, 0].set_ylabel('Z Error (m)')
    axes[1, 0].set_title('Vertical Position Error per Waypoint')
    axes[1, 0].grid(True, alpha=0.3)

    # 误差直方图
    axes[1, 1].hist(error_3d, bins=min(20, len(error_3d)), color='steelblue', alpha=0.7, edgecolor='black')
    axes[1, 1].axvline(x=analysis['stats']['error_3d']['mean'], color='r', linestyle='--', label=f"Mean: {analysis['stats']['error_3d']['mean']:.3f}m")
    axes[1, 1].axvline(x=analysis['stats']['error_3d']['p90'], color='orange', linestyle='--', label=f"P90: {analysis['stats']['error_3d']['p90']:.3f}m")
    axes[1, 1].set_xlabel('3D Error (m)')
    axes[1, 1].set_ylabel('Count')
    axes[1, 1].set_title('Error Distribution')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)

    plt.suptitle(f'Error Analysis - {traj_name}', fontsize=14, y=1.02)
    plt.tight_layout()

    error_path = output_dir / f'{traj_name}_errors.png'
    plt.savefig(error_path, dpi=150, bbox_inches='tight')
    plt.close()
    output_files.append(error_path)

    return output_files


def main():
    parser = argparse.ArgumentParser(description='Analyze waypoint mission experiment results')
    parser.add_argument('--data-dir', type=str, required=True, help='Directory containing experiment data')
    parser.add_argument('--output-dir', type=str, default=None, help='Output directory for plots (default: data-dir/analysis)')
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else data_dir / 'analysis'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 查找所有轨迹数据
    csv_files = list(data_dir.rglob('*/uav*/data.csv'))
    if not csv_files:
        # 也尝试直接查找data.csv
        csv_files = list(data_dir.rglob('data.csv'))

    if not csv_files:
        print(f"No data.csv files found in {data_dir}")
        sys.exit(1)

    print(f"Found {len(csv_files)} trajectory data files")

    all_results = []

    for csv_file in csv_files:
        traj_name = csv_file.parent.parent.name if csv_file.parent.name.startswith('uav') else csv_file.parent.name
        print(f"\nAnalyzing: {traj_name}")

        rows = load_data_csv(csv_file)
        if not rows:
            print(f"  No data in {csv_file}")
            continue

        analysis = analyze_trajectory(rows)
        if 'error' in analysis:
            print(f"  Error: {analysis['error']}")
            continue

        print(f"  Points: {analysis['num_points']}")
        print(f"  3D Error - Mean: {analysis['stats']['error_3d']['mean']:.4f}m, "
              f"Max: {analysis['stats']['error_3d']['max']:.4f}m, "
              f"P90: {analysis['stats']['error_3d']['p90']:.4f}m")
        print(f"  XY Error - Mean: {analysis['stats']['error_xy']['mean']:.4f}m")

        # 生成可视化
        plot_files = plot_trajectory_comparison(analysis, traj_name, output_dir)
        for pf in plot_files:
            print(f"  Generated: {pf}")

        all_results.append({
            'trajectory': traj_name,
            'csv_file': str(csv_file),
            'num_points': analysis['num_points'],
            'stats': analysis['stats']
        })

    # 保存汇总结果
    summary_path = output_dir / 'analysis_summary.json'
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSummary saved to: {summary_path}")

    # 打印总体统计
    if all_results:
        print("\n" + "="*60)
        print("OVERALL STATISTICS")
        print("="*60)

        all_mean_errors = [r['stats']['error_3d']['mean'] for r in all_results]
        all_max_errors = [r['stats']['error_3d']['max'] for r in all_results]

        print(f"Trajectories analyzed: {len(all_results)}")
        print(f"Average mean error: {sum(all_mean_errors)/len(all_mean_errors):.4f}m")
        print(f"Worst mean error: {max(all_mean_errors):.4f}m")
        print(f"Best mean error: {min(all_mean_errors):.4f}m")
        print(f"Maximum single-point error: {max(all_max_errors):.4f}m")


if __name__ == '__main__':
    main()
