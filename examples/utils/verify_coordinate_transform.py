#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
坐标转换验证脚本 (verify_coordinate_transform.py)

用于验证trajectory_data_collector中的坐标转换逻辑是否正确。

坐标系说明:
===========
1. 原始JSON输入 (ENU):
   - x = 东 (East)
   - y = 北 (North)
   - z = 天 (Up)

2. trajectory_data_collector 转换后 (NEU):
   - _load_preprocessed_xyz: pts.append(TrajPoint(y, x, z, ...))
   - 即: NEU_x = ENU_y (北), NEU_y = ENU_x (东)

3. _transform_points 处理:
   - x = base_x * scale + p.x * scale  (NEU坐标)
   - y = base_y * scale + p.y * scale  (NEU坐标)
   - z = base_z * scale - p.z * scale  (z_down模式)

4. rospy_isaacsim.py 发送 (NED):
   - 使用 FRAME_LOCAL_NED 直接发送
   - 但输入的是NEU坐标，可能需要再次转换！

5. Isaac Sim 观测 (ENU):
   - /uav/<id>/all 返回的pose是ENU坐标

问题: 命令坐标(NEU)和观测坐标(ENU)不在同一坐标系!

使用方法:
  python3 verify_coordinate_transform.py --json examples/test_traj_batch/simple_square.json
  python3 verify_coordinate_transform.py --csv examples/trajectory_recordings/<traj>/uav0/data.csv
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List, Tuple, Optional
import numpy as np


def load_json_trajectory(json_path: Path) -> Tuple[List[float], List[List[float]]]:
    """加载JSON轨迹"""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    init_point = data.get('raw_logs', [[0,0,0,0,0,0]])[0]
    preprocessed_logs = data.get('preprocessed_logs', [])
    return init_point, preprocessed_logs


def simulate_collector_transform(
    init_point: List[float],
    preprocessed_logs: List[List[float]],
    scale: float = 0.01,
    z_down: bool = True
) -> List[Tuple[float, float, float, str]]:
    """
    模拟trajectory_data_collector的完整转换过程

    Returns:
        List of (x, y, z, description)
    """
    results = []

    # Step 1: 初始点 ENU → NEU
    init_enu = (init_point[0], init_point[1], init_point[2])
    init_neu = (init_point[1], init_point[0], init_point[2])  # 交换x和y

    results.append({
        "step": "init_point",
        "enu": init_enu,
        "neu": init_neu,
        "scaled_neu": (init_neu[0] * scale, init_neu[1] * scale, init_neu[2] * scale)
    })

    # Step 2: 每个轨迹点的转换
    for i, pt in enumerate(preprocessed_logs):
        if len(pt) < 3:
            continue

        # 原始ENU坐标
        enu = (pt[0], pt[1], pt[2])

        # ENU → NEU (在_load_preprocessed_xyz中)
        neu = (pt[1], pt[0], pt[2])  # 交换x和y

        # _transform_points: 缩放和偏移
        base_x = init_neu[0] * scale
        base_y = init_neu[1] * scale
        base_z = init_neu[2] * scale

        cmd_x = base_x + neu[0] * scale
        cmd_y = base_y + neu[1] * scale
        if z_down:
            cmd_z = base_z - neu[2] * scale
        else:
            cmd_z = base_z + neu[2] * scale

        results.append({
            "step": f"point_{i}",
            "enu_input": enu,
            "neu_swapped": neu,
            "cmd_output": (cmd_x, cmd_y, cmd_z)
        })

    return results


def analyze_csv_data(csv_path: Path) -> dict:
    """分析CSV数据中的坐标对比"""
    cmd_points = []
    obs_points = []
    cmd_in_points = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cmd_x = float(row.get('cmd_x', 0))
                cmd_y = float(row.get('cmd_y', 0))
                cmd_z = float(row.get('cmd_z', 0))
                cmd_points.append((cmd_x, cmd_y, cmd_z))

                obs_x = float(row.get('obs_pos_x', 0) or row.get('pos_x', 0))
                obs_y = float(row.get('obs_pos_y', 0) or row.get('pos_y', 0))
                obs_z = float(row.get('obs_pos_z', 0) or row.get('pos_z', 0))
                obs_points.append((obs_x, obs_y, obs_z))

                cmd_in_x = float(row.get('cmd_in_x', 0))
                cmd_in_y = float(row.get('cmd_in_y', 0))
                cmd_in_z = float(row.get('cmd_in_z', 0))
                cmd_in_points.append((cmd_in_x, cmd_in_y, cmd_in_z))
            except (ValueError, TypeError):
                continue

    return {
        "cmd_points": cmd_points,
        "obs_points": obs_points,
        "cmd_in_points": cmd_in_points
    }


def print_coordinate_analysis(
    cmd_points: List[Tuple[float, float, float]],
    obs_points: List[Tuple[float, float, float]]
):
    """打印坐标分析结果"""
    print("\n" + "="*80)
    print("坐标系对比分析")
    print("="*80)

    if not cmd_points or not obs_points:
        print("ERROR: 没有数据可分析")
        return

    n = min(len(cmd_points), len(obs_points))

    print(f"\n总点数: 命令={len(cmd_points)}, 观测={len(obs_points)}, 对比={n}")

    print("\n" + "-"*80)
    print("逐点对比 (前5个点):")
    print("-"*80)
    print(f"{'Idx':<4} {'Cmd X':>10} {'Cmd Y':>10} {'Cmd Z':>10} | {'Obs X':>10} {'Obs Y':>10} {'Obs Z':>10} | {'Err':>8}")

    errors_raw = []
    errors_swapped = []

    for i in range(min(5, n)):
        cx, cy, cz = cmd_points[i]
        ox, oy, oz = obs_points[i]

        # 原始误差
        err_raw = np.sqrt((cx-ox)**2 + (cy-oy)**2 + (cz-oz)**2)
        errors_raw.append(err_raw)

        # 交换xy后的误差 (将ENU观测转为NEU)
        err_swap = np.sqrt((cx-oy)**2 + (cy-ox)**2 + (cz-oz)**2)
        errors_swapped.append(err_swap)

        print(f"{i:<4} {cx:>10.4f} {cy:>10.4f} {cz:>10.4f} | {ox:>10.4f} {oy:>10.4f} {oz:>10.4f} | {err_raw:>8.4f}")

    # 计算所有点的误差
    for i in range(min(5, n), n):
        cx, cy, cz = cmd_points[i]
        ox, oy, oz = obs_points[i]
        errors_raw.append(np.sqrt((cx-ox)**2 + (cy-oy)**2 + (cz-oz)**2))
        errors_swapped.append(np.sqrt((cx-oy)**2 + (cy-ox)**2 + (cz-oz)**2))

    print("\n" + "-"*80)
    print("误差统计:")
    print("-"*80)
    print(f"原始对比 (Cmd NEU vs Obs ENU):")
    print(f"  平均误差: {np.mean(errors_raw):.4f} m")
    print(f"  最大误差: {np.max(errors_raw):.4f} m")
    print(f"  首点误差: {errors_raw[0]:.4f} m")
    print(f"  末点误差: {errors_raw[-1]:.4f} m")

    print(f"\n交换XY后 (Cmd NEU vs Obs ENU→NEU):")
    print(f"  平均误差: {np.mean(errors_swapped):.4f} m")
    print(f"  最大误差: {np.max(errors_swapped):.4f} m")
    print(f"  首点误差: {errors_swapped[0]:.4f} m")
    print(f"  末点误差: {errors_swapped[-1]:.4f} m")

    print("\n" + "-"*80)
    print("诊断结论:")
    print("-"*80)

    if np.mean(errors_swapped) < np.mean(errors_raw):
        print("✓ 检测到坐标系不匹配!")
        print("  - 命令坐标是NEU (x=北, y=东)")
        print("  - 观测坐标是ENU (x=东, y=北)")
        print("  - 建议: 可视化时将观测坐标的x和y交换")
    else:
        print("✓ 坐标系似乎匹配")
        print("  误差可能来自其他原因 (控制延迟、轨迹跟踪误差等)")

    # 检查首尾跳变
    mid_errors = errors_swapped[1:-1] if len(errors_swapped) > 2 else errors_swapped
    mid_mean = np.mean(mid_errors) if mid_errors else 0

    if errors_swapped[0] > mid_mean * 2:
        print(f"\n⚠ 检测到轨迹起始跳变: 首点误差({errors_swapped[0]:.4f}m) > 中间平均({mid_mean:.4f}m)*2")

    if errors_swapped[-1] > mid_mean * 2:
        print(f"\n⚠ 检测到轨迹结束跳变: 末点误差({errors_swapped[-1]:.4f}m) > 中间平均({mid_mean:.4f}m)*2")


def main():
    parser = argparse.ArgumentParser(description='验证坐标转换逻辑')
    parser.add_argument('--json', type=str, help='JSON轨迹文件路径')
    parser.add_argument('--csv', type=str, help='采集数据CSV文件路径')
    parser.add_argument('--scale', type=float, default=0.01, help='缩放因子')
    args = parser.parse_args()

    if args.json:
        json_path = Path(args.json)
        if not json_path.exists():
            print(f"ERROR: JSON file not found: {json_path}")
            sys.exit(1)

        print("="*80)
        print("JSON 轨迹坐标转换验证")
        print("="*80)

        init_point, preprocessed_logs = load_json_trajectory(json_path)
        results = simulate_collector_transform(init_point, preprocessed_logs, args.scale)

        print(f"\n源文件: {json_path}")
        print(f"缩放因子: {args.scale}")
        print(f"轨迹点数: {len(preprocessed_logs)}")

        print("\n" + "-"*80)
        print("转换过程演示 (前5个点):")
        print("-"*80)

        for r in results[:6]:
            step = r["step"]
            if step == "init_point":
                print(f"\n初始点:")
                print(f"  ENU输入: {r['enu']}")
                print(f"  NEU转换: {r['neu']}")
                print(f"  缩放后:  {r['scaled_neu']}")
            else:
                print(f"\n{step}:")
                print(f"  ENU输入: {r['enu_input']}")
                print(f"  NEU转换: {r['neu_swapped']}")
                print(f"  命令输出: {r['cmd_output']}")

        print("\n" + "-"*80)
        print("坐标系转换链路:")
        print("-"*80)
        print("""
JSON (ENU: x=东, y=北, z=天)
    ↓ _load_preprocessed_xyz: swap(x,y)
NEU (x=北, y=东, z=天)
    ↓ _transform_points: scale + offset + z_down
命令坐标 (NEU, scaled)
    ↓ rospy_isaacsim.py: FRAME_LOCAL_NED
PX4 接收 (作为NED处理)
    ↓
实际飞行

Isaac Sim 返回观测:
    ENU (x=东, y=北, z=天)

问题: 命令坐标(NEU) ≠ 观测坐标(ENU)
""")

    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            print(f"ERROR: CSV file not found: {csv_path}")
            sys.exit(1)

        print("\n" + "="*80)
        print("CSV 数据坐标分析")
        print("="*80)
        print(f"\n源文件: {csv_path}")

        data = analyze_csv_data(csv_path)
        print_coordinate_analysis(data["cmd_points"], data["obs_points"])

    if not args.json and not args.csv:
        parser.print_help()
        print("\n请指定 --json 或 --csv 参数")


if __name__ == "__main__":
    main()
