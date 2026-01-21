#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# -*- coding: utf-8 -*-
"""
轨迹跟随效果分析脚本

分析采集到的轨迹数据，计算：
1. 位置跟踪误差 (cmd vs obs)
2. 姿态跟踪误差
3. 速度跟踪效果
4. 生成可视化图表
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import argparse
import json
from pathlib import Path

def analyze_trajectory(csv_path, output_dir=None, show_plot=False):
    """分析单条轨迹

    坐标系说明：
    - cmd_*: 原始轨迹命令坐标（轨迹空间）
    - obs_*: Isaac仿真器观测坐标（世界空间，已减去position_offset）

    由于轨迹跟踪过程中存在初始偏移（起飞后开始记录），
    我们使用相对坐标进行误差分析：
    - 将两者都转换为相对于各自起点的坐标
    """
    csv_path = Path(csv_path)
    if output_dir is None:
        output_dir = csv_path.parent
    else:
        output_dir = Path(output_dir)

    df = pd.read_csv(csv_path)
    if len(df) < 10:
        print(f"Error: Not enough data in {csv_path} ({len(df)} rows)")
        return None

    # 加载 metadata 获取起飞阶段信息
    metadata_path = csv_path.parent / "metadata.json"
    takeoff_obs_count = 0
    trajectory_start_sim_ts = None
    if metadata_path.exists():
        try:
            import json
            with open(metadata_path) as f:
                meta = json.load(f)
            takeoff_obs_count = meta.get("obs_takeoff_phase_count", 0)
            trajectory_start_sim_ts = meta.get("trajectory_start_sim_ts")
        except Exception as e:
            print(f"Warning: Failed to load metadata: {e}")

    # 裁剪数据：只保留轨迹跟踪阶段的数据 (跳过起飞阶段)
    original_len = len(df)
    if trajectory_start_sim_ts is not None:
        df = df[df['sim_time'] >= trajectory_start_sim_ts].reset_index(drop=True)
        skipped = original_len - len(df)
        if skipped > 0:
            print(f"  裁剪起飞阶段: 跳过 {skipped} 条 obs (trajectory_start_sim_ts={trajectory_start_sim_ts:.2f}s)")

    if len(df) < 10:
        print(f"Error: Not enough data after trimming takeoff phase ({len(df)} rows)")
        return None

    # 转换为相对坐标（相对于各自的起点）
    cmd_x0, cmd_y0, cmd_z0 = df['cmd_x'].iloc[0], df['cmd_y'].iloc[0], df['cmd_z'].iloc[0]
    obs_x0, obs_y0, obs_z0 = df['obs_pos_x'].iloc[0], df['obs_pos_y'].iloc[0], df['obs_pos_z'].iloc[0]

    df['cmd_x_rel'] = df['cmd_x'] - cmd_x0
    df['cmd_y_rel'] = df['cmd_y'] - cmd_y0
    df['cmd_z_rel'] = df['cmd_z'] - cmd_z0
    df['obs_x_rel'] = df['obs_pos_x'] - obs_x0
    df['obs_y_rel'] = df['obs_pos_y'] - obs_y0
    df['obs_z_rel'] = df['obs_pos_z'] - obs_z0

    # 计算位置跟踪误差（使用相对坐标）
    df['err_x'] = df['obs_x_rel'] - df['cmd_x_rel']
    df['err_y'] = df['obs_y_rel'] - df['cmd_y_rel']
    df['err_z'] = df['obs_z_rel'] - df['cmd_z_rel']
    df['err_xy'] = np.sqrt(df['err_x']**2 + df['err_y']**2)
    df['err_xyz'] = np.sqrt(df['err_x']**2 + df['err_y']**2 + df['err_z']**2)

    # 统计指标
    rmse_xy = np.sqrt(np.mean(df['err_xy']**2))
    rmse_xyz = np.sqrt(np.mean(df['err_xyz']**2))
    max_err_xy = df['err_xy'].max()
    max_err_xyz = df['err_xyz'].max()
    mean_err_xy = df['err_xy'].mean()
    mean_err_xyz = df['err_xyz'].mean()

    # 计算姿态误差 (yaw)
    df['err_yaw'] = np.abs(df['obs_yaw'] - df['cmd_yaw'])
    # 处理 -π 到 π 的环绕
    df['err_yaw'] = np.minimum(df['err_yaw'], 2*np.pi - df['err_yaw'])
    mean_err_yaw_deg = np.degrees(df['err_yaw'].mean())

    # 计算速度
    df['cmd_speed'] = np.sqrt(df['cmd_vx']**2 + df['cmd_vy']**2 + df['cmd_vz']**2)
    df['obs_speed'] = np.sqrt(df['obs_linvel_x']**2 + df['obs_linvel_y']**2 + df['obs_linvel_z']**2)

    # 时间范围
    t_start = df['sim_time'].min()
    t_end = df['sim_time'].max()
    duration = t_end - t_start

    results = {
        'file': str(csv_path.name),
        'duration_s': duration,
        'num_points': len(df),
        'rmse_xy_m': rmse_xy,
        'rmse_xyz_m': rmse_xyz,
        'max_err_xy_m': max_err_xy,
        'max_err_xyz_m': max_err_xyz,
        'mean_err_xy_m': mean_err_xy,
        'mean_err_xyz_m': mean_err_xyz,
        'mean_err_yaw_deg': mean_err_yaw_deg,
        'mean_cmd_speed': df['cmd_speed'].mean(),
        'mean_obs_speed': df['obs_speed'].mean(),
    }

    print(f"\n{'='*60}")
    print(f"轨迹分析: {csv_path.parent.name}")
    print(f"{'='*60}")
    print(f"数据点数: {len(df)}")
    print(f"轨迹时长: {duration:.2f}s")
    print(f"\n位置跟踪误差:")
    print(f"  XY RMSE:   {rmse_xy:.4f} m")
    print(f"  XYZ RMSE:  {rmse_xyz:.4f} m")
    print(f"  XY 最大误差: {max_err_xy:.4f} m")
    print(f"  XYZ 最大误差: {max_err_xyz:.4f} m")
    print(f"  XY 平均误差: {mean_err_xy:.4f} m")
    print(f"\n姿态跟踪误差:")
    print(f"  Yaw 平均误差: {mean_err_yaw_deg:.2f}°")
    print(f"\n速度统计:")
    print(f"  命令平均速度: {df['cmd_speed'].mean():.2f} m/s")
    print(f"  观测平均速度: {df['obs_speed'].mean():.2f} m/s")

    # 生成可视化
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle(f'轨迹跟随分析: {csv_path.parent.name} (相对坐标)', fontsize=14)

    # 1. XY 轨迹对比（相对坐标）
    ax = axes[0, 0]
    ax.plot(df['cmd_x_rel'].to_numpy(), df['cmd_y_rel'].to_numpy(), 'b-', label='Command', linewidth=2)
    ax.plot(df['obs_x_rel'].to_numpy(), df['obs_y_rel'].to_numpy(), 'r--', label='Observed', linewidth=1.5, alpha=0.8)
    ax.scatter(0, 0, c='green', s=100, marker='o', zorder=5, label='Start')
    ax.scatter(df['cmd_x_rel'].iloc[-1], df['cmd_y_rel'].iloc[-1], c='blue', s=100, marker='x', zorder=5, label='Cmd End')
    ax.scatter(df['obs_x_rel'].iloc[-1], df['obs_y_rel'].iloc[-1], c='red', s=100, marker='x', zorder=5, label='Obs End')
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_title('XY 轨迹对比 (相对起点)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal')

    # 2. 高度跟踪（相对坐标）
    ax = axes[0, 1]
    t = (df['sim_time'] - t_start).to_numpy()
    ax.plot(t, df['cmd_z_rel'].to_numpy(), 'b-', label='Command Z', linewidth=2)
    ax.plot(t, df['obs_z_rel'].to_numpy(), 'r--', label='Observed Z', linewidth=1.5, alpha=0.8)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Altitude (m)')
    ax.set_title('高度跟踪')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 3. 位置误差随时间变化
    ax = axes[0, 2]
    ax.plot(t, df['err_xy'].to_numpy(), 'b-', label='XY Error', linewidth=1.5)
    ax.plot(t, df['err_xyz'].to_numpy(), 'g-', label='XYZ Error', linewidth=1.5, alpha=0.7)
    ax.axhline(y=rmse_xy, color='b', linestyle='--', label=f'XY RMSE={rmse_xy:.3f}m')
    ax.axhline(y=rmse_xyz, color='g', linestyle='--', label=f'XYZ RMSE={rmse_xyz:.3f}m')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Error (m)')
    ax.set_title('位置跟踪误差')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 4. 各轴误差分布
    ax = axes[1, 0]
    ax.hist(df['err_x'].to_numpy(), bins=30, alpha=0.5, label='X Error')
    ax.hist(df['err_y'].to_numpy(), bins=30, alpha=0.5, label='Y Error')
    ax.hist(df['err_z'].to_numpy(), bins=30, alpha=0.5, label='Z Error')
    ax.set_xlabel('Error (m)')
    ax.set_ylabel('Count')
    ax.set_title('各轴误差分布')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 5. 速度对比
    ax = axes[1, 1]
    ax.plot(t, df['cmd_speed'].to_numpy(), 'b-', label='Command Speed', linewidth=2)
    ax.plot(t, df['obs_speed'].to_numpy(), 'r--', label='Observed Speed', linewidth=1.5, alpha=0.8)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Speed (m/s)')
    ax.set_title('速度跟踪')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 6. Yaw角对比
    ax = axes[1, 2]
    ax.plot(t, np.degrees(df['cmd_yaw'].to_numpy()), 'b-', label='Command Yaw', linewidth=2)
    ax.plot(t, np.degrees(df['obs_yaw'].to_numpy()), 'r--', label='Observed Yaw', linewidth=1.5, alpha=0.8)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Yaw (deg)')
    ax.set_title('Yaw角跟踪')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    # 保存图表
    plot_path = output_dir / 'trajectory_analysis.png'
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"\n图表已保存: {plot_path}")

    if show_plot:
        plt.show()
    else:
        plt.close()

    return results


def analyze_batch(data_dir, output_file=None):
    """批量分析目录下所有轨迹"""
    data_dir = Path(data_dir)

    # 查找所有 data.csv 文件
    csv_files = list(data_dir.glob('*/data.csv'))
    if not csv_files:
        print(f"No data.csv files found in {data_dir}")
        return

    print(f"找到 {len(csv_files)} 条轨迹数据")

    all_results = []
    for csv_path in sorted(csv_files):
        try:
            result = analyze_trajectory(csv_path)
            if result:
                all_results.append(result)
        except Exception as e:
            print(f"Error analyzing {csv_path}: {e}")

    if not all_results:
        print("No valid results")
        return

    # 汇总统计
    print(f"\n{'='*60}")
    print(f"汇总统计 ({len(all_results)} 条轨迹)")
    print(f"{'='*60}")

    rmse_xy_vals = [r['rmse_xy_m'] for r in all_results]
    rmse_xyz_vals = [r['rmse_xyz_m'] for r in all_results]

    print(f"XY RMSE:  平均 {np.mean(rmse_xy_vals):.4f}m, 最大 {np.max(rmse_xy_vals):.4f}m, 最小 {np.min(rmse_xy_vals):.4f}m")
    print(f"XYZ RMSE: 平均 {np.mean(rmse_xyz_vals):.4f}m, 最大 {np.max(rmse_xyz_vals):.4f}m, 最小 {np.min(rmse_xyz_vals):.4f}m")

    # 保存结果
    if output_file:
        output_file = Path(output_file)
    else:
        output_file = data_dir / 'analysis_summary.json'

    summary = {
        'num_trajectories': len(all_results),
        'avg_rmse_xy_m': np.mean(rmse_xy_vals),
        'avg_rmse_xyz_m': np.mean(rmse_xyz_vals),
        'max_rmse_xy_m': np.max(rmse_xy_vals),
        'max_rmse_xyz_m': np.max(rmse_xyz_vals),
        'trajectories': all_results
    }

    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\n汇总结果已保存: {output_file}")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="轨迹跟随效果分析")
    parser.add_argument("--csv", help="单个 data.csv 文件路径")
    parser.add_argument("--dir", help="包含多个轨迹的目录")
    parser.add_argument("--output", help="输出目录或文件")
    parser.add_argument("--show", action="store_true", help="显示图表")
    args = parser.parse_args()

    if args.csv:
        analyze_trajectory(args.csv, args.output, args.show)
    elif args.dir:
        analyze_batch(args.dir, args.output)
    else:
        print("请指定 --csv 或 --dir 参数")
