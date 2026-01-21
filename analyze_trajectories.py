#!/usr/bin/env python3
"""
轨迹质量分析脚本
分析每个采集轨迹的：
1. 平均速度
2. 与原始轨迹的RMSE偏差
3. 各轴误差统计
4. 生成评估报告保存到对应目录
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

def load_raw_trajectory(json_path, scale=0.01):
    """加载原始轨迹JSON并缩放"""
    with open(json_path, 'r') as f:
        data = json.load(f)

    points = data.get('points', [])
    if not points:
        return None

    # 提取坐标和时间
    times = []
    positions = []
    for p in points:
        t = p.get('t', 0)
        x = p.get('x', 0) * scale
        y = p.get('y', 0) * scale
        z = p.get('z', 0) * scale
        times.append(t)
        positions.append([x, y, z])

    return np.array(times), np.array(positions)

def interpolate_trajectory(times, positions, query_times):
    """插值轨迹到指定时间点"""
    if len(times) < 2:
        return None

    # 限制查询时间在有效范围内
    valid_mask = (query_times >= times[0]) & (query_times <= times[-1])
    query_times_valid = query_times[valid_mask]

    if len(query_times_valid) == 0:
        return None, valid_mask

    interp_pos = np.zeros((len(query_times_valid), 3))
    for i in range(3):
        interp_pos[:, i] = np.interp(query_times_valid, times, positions[:, i])

    return interp_pos, valid_mask

def analyze_single_trajectory(traj_dir):
    """分析单个轨迹目录"""
    traj_dir = Path(traj_dir)
    traj_name = traj_dir.name

    # 检查必要文件
    data_csv = traj_dir / "data.csv"
    metadata_json = traj_dir / "metadata.json"

    if not data_csv.exists():
        return None, f"data.csv not found"

    if not metadata_json.exists():
        return None, f"metadata.json not found"

    # 加载metadata
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)

    # 加载观测数据
    try:
        df = pd.read_csv(data_csv)
    except Exception as e:
        return None, f"Failed to load data.csv: {e}"

    if len(df) < 5:
        return None, f"Too few data points: {len(df)}"

    # 提取观测位置和命令位置
    obs_cols = ['obs_pos_x', 'obs_pos_y', 'obs_pos_z']
    cmd_cols = ['cmd_x', 'cmd_y', 'cmd_z']
    vel_cols = ['obs_linvel_x', 'obs_linvel_y', 'obs_linvel_z']

    if not all(col in df.columns for col in obs_cols + cmd_cols):
        return None, f"Missing required columns"

    obs_pos = df[obs_cols].values
    cmd_pos = df[cmd_cols].values
    sim_times = df['sim_time'].values if 'sim_time' in df.columns else np.arange(len(df)) * 0.05

    # 计算速度
    if all(col in df.columns for col in vel_cols):
        velocities = df[vel_cols].values
        speeds = np.linalg.norm(velocities, axis=1)
    else:
        # 从位置差分计算
        dt = np.diff(sim_times)
        dt[dt == 0] = 0.05  # 避免除零
        pos_diff = np.diff(obs_pos, axis=0)
        speeds = np.linalg.norm(pos_diff, axis=1) / dt
        speeds = np.concatenate([[0], speeds])

    # 计算跟踪误差 (obs vs cmd)
    errors = obs_pos - cmd_pos
    error_x = errors[:, 0]
    error_y = errors[:, 1]
    error_z = errors[:, 2]
    error_3d = np.linalg.norm(errors, axis=1)

    # RMSE
    rmse_x = np.sqrt(np.mean(error_x**2))
    rmse_y = np.sqrt(np.mean(error_y**2))
    rmse_z = np.sqrt(np.mean(error_z**2))
    rmse_3d = np.sqrt(np.mean(error_3d**2))

    # MAE
    mae_x = np.mean(np.abs(error_x))
    mae_y = np.mean(np.abs(error_y))
    mae_z = np.mean(np.abs(error_z))
    mae_3d = np.mean(error_3d)

    # 最大误差
    max_error_x = np.max(np.abs(error_x))
    max_error_y = np.max(np.abs(error_y))
    max_error_z = np.max(np.abs(error_z))
    max_error_3d = np.max(error_3d)

    # 速度统计
    avg_speed = np.mean(speeds)
    max_speed = np.max(speeds)

    # 轨迹范围
    traj_range_x = np.max(obs_pos[:, 0]) - np.min(obs_pos[:, 0])
    traj_range_y = np.max(obs_pos[:, 1]) - np.min(obs_pos[:, 1])
    traj_range_z = np.max(obs_pos[:, 2]) - np.min(obs_pos[:, 2])

    # 高度统计
    avg_altitude = np.mean(obs_pos[:, 2])
    min_altitude = np.min(obs_pos[:, 2])
    max_altitude = np.max(obs_pos[:, 2])

    # 加载原始轨迹进行对比
    raw_traj_path = None
    for f in traj_dir.glob("*.json"):
        if f.name != "metadata.json" and "raw_observations" not in f.name:
            raw_traj_path = f
            break

    raw_rmse = None
    if raw_traj_path and raw_traj_path.exists():
        try:
            raw_times, raw_positions = load_raw_trajectory(raw_traj_path, scale=metadata.get('scale', 0.01))
            if raw_times is not None and len(raw_times) > 1:
                # 插值原始轨迹到观测时间点
                interp_raw, valid_mask = interpolate_trajectory(raw_times, raw_positions, sim_times)
                if interp_raw is not None and len(interp_raw) > 0:
                    # 计算与原始轨迹的偏差
                    raw_errors = obs_pos[valid_mask] - interp_raw
                    raw_rmse = np.sqrt(np.mean(np.linalg.norm(raw_errors, axis=1)**2))
        except Exception as e:
            pass

    # 构建评估结果
    result = {
        "trajectory_name": traj_name,
        "analysis_time": datetime.now().isoformat(),
        "data_points": len(df),
        "duration_s": float(sim_times[-1] - sim_times[0]) if len(sim_times) > 1 else 0,

        # 速度统计
        "speed": {
            "avg_m_s": round(float(avg_speed), 4),
            "max_m_s": round(float(max_speed), 4),
        },

        # 跟踪误差 (obs vs cmd)
        "tracking_error": {
            "rmse": {
                "x_m": round(float(rmse_x), 4),
                "y_m": round(float(rmse_y), 4),
                "z_m": round(float(rmse_z), 4),
                "3d_m": round(float(rmse_3d), 4),
            },
            "mae": {
                "x_m": round(float(mae_x), 4),
                "y_m": round(float(mae_y), 4),
                "z_m": round(float(mae_z), 4),
                "3d_m": round(float(mae_3d), 4),
            },
            "max": {
                "x_m": round(float(max_error_x), 4),
                "y_m": round(float(max_error_y), 4),
                "z_m": round(float(max_error_z), 4),
                "3d_m": round(float(max_error_3d), 4),
            },
        },

        # 轨迹范围
        "trajectory_range": {
            "x_m": round(float(traj_range_x), 4),
            "y_m": round(float(traj_range_y), 4),
            "z_m": round(float(traj_range_z), 4),
        },

        # 高度统计
        "altitude": {
            "avg_m": round(float(avg_altitude), 4),
            "min_m": round(float(min_altitude), 4),
            "max_m": round(float(max_altitude), 4),
        },

        # 与原始轨迹偏差
        "raw_trajectory_rmse_m": round(float(raw_rmse), 4) if raw_rmse is not None else None,

        # 质量评估
        "quality": {
            "rmse_3d_ok": bool(rmse_3d < 1.0),  # <1m 目标
            "altitude_ok": bool(min_altitude > 0.3),  # >0.3m
            "data_ok": bool(len(df) >= 10),
        },

        # 元数据
        "metadata": {
            "position_offset": metadata.get("position_offset"),
            "scale": metadata.get("scale"),
            "time_scale": metadata.get("time_scale"),
        }
    }

    # 整体质量评分
    quality_score = 0
    if result["quality"]["rmse_3d_ok"]:
        quality_score += 40
    if result["quality"]["altitude_ok"]:
        quality_score += 30
    if result["quality"]["data_ok"]:
        quality_score += 30

    # 额外评分
    if rmse_3d < 0.5:
        quality_score += 10
    if rmse_3d < 0.3:
        quality_score += 10

    result["quality"]["score"] = min(100, quality_score)
    result["quality"]["grade"] = "A" if quality_score >= 90 else "B" if quality_score >= 70 else "C" if quality_score >= 50 else "F"

    return result, None

def analyze_all_trajectories(data_dir, output_summary=True):
    """分析目录下所有轨迹"""
    data_dir = Path(data_dir)

    if not data_dir.exists():
        print(f"ERROR: Directory not found: {data_dir}")
        return

    # 找到所有轨迹目录
    traj_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir() and not d.name.startswith('.')])

    print(f"Found {len(traj_dirs)} trajectory directories")
    print("="*60)

    results = []
    errors = []

    for i, traj_dir in enumerate(traj_dirs):
        result, error = analyze_single_trajectory(traj_dir)

        if result:
            results.append(result)

            # 保存评估结果到对应目录
            eval_path = traj_dir / "evaluation.json"
            with open(eval_path, 'w') as f:
                json.dump(result, f, indent=2)

            # 进度显示
            if (i + 1) % 50 == 0 or i == len(traj_dirs) - 1:
                print(f"Processed {i+1}/{len(traj_dirs)}: {result['quality']['grade']} ({result['tracking_error']['rmse']['3d_m']:.3f}m)")
        else:
            errors.append((traj_dir.name, error))

    print("="*60)
    print(f"Analysis complete: {len(results)} success, {len(errors)} failed")

    if not results:
        print("No valid results to summarize")
        return

    # 汇总统计
    rmse_3d_list = [r['tracking_error']['rmse']['3d_m'] for r in results]
    rmse_x_list = [r['tracking_error']['rmse']['x_m'] for r in results]
    rmse_y_list = [r['tracking_error']['rmse']['y_m'] for r in results]
    rmse_z_list = [r['tracking_error']['rmse']['z_m'] for r in results]
    speed_list = [r['speed']['avg_m_s'] for r in results]
    scores = [r['quality']['score'] for r in results]

    summary = {
        "analysis_time": datetime.now().isoformat(),
        "data_directory": str(data_dir),
        "total_trajectories": len(traj_dirs),
        "successful_analyses": len(results),
        "failed_analyses": len(errors),

        "rmse_statistics": {
            "x": {
                "mean": round(np.mean(rmse_x_list), 4),
                "std": round(np.std(rmse_x_list), 4),
                "min": round(np.min(rmse_x_list), 4),
                "max": round(np.max(rmse_x_list), 4),
                "median": round(np.median(rmse_x_list), 4),
            },
            "y": {
                "mean": round(np.mean(rmse_y_list), 4),
                "std": round(np.std(rmse_y_list), 4),
                "min": round(np.min(rmse_y_list), 4),
                "max": round(np.max(rmse_y_list), 4),
                "median": round(np.median(rmse_y_list), 4),
            },
            "z": {
                "mean": round(np.mean(rmse_z_list), 4),
                "std": round(np.std(rmse_z_list), 4),
                "min": round(np.min(rmse_z_list), 4),
                "max": round(np.max(rmse_z_list), 4),
                "median": round(np.median(rmse_z_list), 4),
            },
            "3d": {
                "mean": round(np.mean(rmse_3d_list), 4),
                "std": round(np.std(rmse_3d_list), 4),
                "min": round(np.min(rmse_3d_list), 4),
                "max": round(np.max(rmse_3d_list), 4),
                "median": round(np.median(rmse_3d_list), 4),
            },
        },

        "speed_statistics": {
            "mean": round(np.mean(speed_list), 4),
            "std": round(np.std(speed_list), 4),
            "min": round(np.min(speed_list), 4),
            "max": round(np.max(speed_list), 4),
        },

        "quality_distribution": {
            "A_count": sum(1 for r in results if r['quality']['grade'] == 'A'),
            "B_count": sum(1 for r in results if r['quality']['grade'] == 'B'),
            "C_count": sum(1 for r in results if r['quality']['grade'] == 'C'),
            "F_count": sum(1 for r in results if r['quality']['grade'] == 'F'),
            "avg_score": round(np.mean(scores), 2),
        },

        "rmse_targets": {
            "under_0.5m": sum(1 for r in rmse_3d_list if r < 0.5),
            "under_1.0m": sum(1 for r in rmse_3d_list if r < 1.0),
            "under_2.0m": sum(1 for r in rmse_3d_list if r < 2.0),
            "over_2.0m": sum(1 for r in rmse_3d_list if r >= 2.0),
        },
    }

    # 打印摘要
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Trajectories analyzed: {summary['successful_analyses']}/{summary['total_trajectories']}")
    print(f"\nRMSE (3D): {summary['rmse_statistics']['3d']['mean']:.3f} ± {summary['rmse_statistics']['3d']['std']:.3f} m")
    print(f"  X: {summary['rmse_statistics']['x']['mean']:.3f} ± {summary['rmse_statistics']['x']['std']:.3f} m")
    print(f"  Y: {summary['rmse_statistics']['y']['mean']:.3f} ± {summary['rmse_statistics']['y']['std']:.3f} m")
    print(f"  Z: {summary['rmse_statistics']['z']['mean']:.3f} ± {summary['rmse_statistics']['z']['std']:.3f} m")
    print(f"\nAvg Speed: {summary['speed_statistics']['mean']:.2f} m/s")
    print(f"\nQuality Distribution:")
    print(f"  A: {summary['quality_distribution']['A_count']} ({100*summary['quality_distribution']['A_count']/len(results):.1f}%)")
    print(f"  B: {summary['quality_distribution']['B_count']} ({100*summary['quality_distribution']['B_count']/len(results):.1f}%)")
    print(f"  C: {summary['quality_distribution']['C_count']} ({100*summary['quality_distribution']['C_count']/len(results):.1f}%)")
    print(f"  F: {summary['quality_distribution']['F_count']} ({100*summary['quality_distribution']['F_count']/len(results):.1f}%)")
    print(f"\nRMSE Target Achievement:")
    print(f"  < 0.5m: {summary['rmse_targets']['under_0.5m']} ({100*summary['rmse_targets']['under_0.5m']/len(results):.1f}%)")
    print(f"  < 1.0m: {summary['rmse_targets']['under_1.0m']} ({100*summary['rmse_targets']['under_1.0m']/len(results):.1f}%)")
    print(f"  < 2.0m: {summary['rmse_targets']['under_2.0m']} ({100*summary['rmse_targets']['under_2.0m']/len(results):.1f}%)")
    print(f"  ≥ 2.0m: {summary['rmse_targets']['over_2.0m']} ({100*summary['rmse_targets']['over_2.0m']/len(results):.1f}%)")

    # 保存汇总
    if output_summary:
        summary_path = data_dir / "analysis_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nSummary saved to: {summary_path}")

    return summary, results

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_trajectories.py <data_directory>")
        sys.exit(1)

    data_dir = sys.argv[1]
    analyze_all_trajectories(data_dir)
