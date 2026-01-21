#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
ULG 和 CSV 数据合并工具

功能：
1. 解析 PX4 ULG 飞行日志，提取动力学参数
2. 与 CSV 轨迹数据按时间戳对齐
3. 输出合并后的数据集

ULG 数据包含：
- actuator_outputs: 电机 PWM/推力输出
- vehicle_attitude: 姿态四元数
- vehicle_local_position: 本地位置和速度
- sensor_combined: IMU 原始数据（加速度、角速度）
- vehicle_angular_velocity: 角速度
- vehicle_acceleration: 加速度

使用方法：
    python merge_ulg_csv.py --csv data.csv --ulg flight.ulg --output merged.csv
    python merge_ulg_csv.py --dir trajectory_recording_dir --output merged.csv

依赖：
    pip install pyulog pandas numpy

作者：Claude Code
日期：2026-01-07
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import json


def check_pyulog():
    """检查并导入 pyulog"""
    try:
        from pyulog import ULog
        return ULog
    except ImportError:
        print("错误: 需要安装 pyulog 库")
        print("请运行: pip install pyulog")
        sys.exit(1)


@dataclass
class ULGData:
    """ULG 数据容器"""
    # 时间戳（微秒）
    timestamps_us: np.ndarray = field(default_factory=lambda: np.array([]))

    # 电机输出 (N个电机)
    motor_outputs: np.ndarray = field(default_factory=lambda: np.array([]))

    # 姿态 (四元数 w, x, y, z)
    attitude_q: np.ndarray = field(default_factory=lambda: np.array([]))

    # 角速度 (rad/s, x, y, z)
    angular_velocity: np.ndarray = field(default_factory=lambda: np.array([]))

    # 本地位置 (m, x, y, z)
    local_position: np.ndarray = field(default_factory=lambda: np.array([]))

    # 本地速度 (m/s, vx, vy, vz)
    local_velocity: np.ndarray = field(default_factory=lambda: np.array([]))

    # IMU 加速度 (m/s^2, x, y, z)
    imu_accel: np.ndarray = field(default_factory=lambda: np.array([]))

    # IMU 角速度 (rad/s, x, y, z)
    imu_gyro: np.ndarray = field(default_factory=lambda: np.array([]))

    # 推力设定值
    thrust_setpoint: np.ndarray = field(default_factory=lambda: np.array([]))

    # 原始 topic 数据
    raw_topics: Dict[str, pd.DataFrame] = field(default_factory=dict)


def parse_ulg(ulg_path: str) -> Optional[ULGData]:
    """
    解析 ULG 文件，提取动力学相关数据

    Args:
        ulg_path: ULG 文件路径

    Returns:
        ULGData 对象，如果解析失败则返回 None
    """
    ULog = check_pyulog()

    if not os.path.exists(ulg_path):
        print(f"警告: ULG 文件不存在: {ulg_path}")
        return None

    file_size = os.path.getsize(ulg_path)
    if file_size < 10000:  # 小于 10KB 基本是空文件
        print(f"警告: ULG 文件过小 ({file_size} bytes): {ulg_path}")
        return None

    try:
        ulog = ULog(ulg_path)
    except Exception as e:
        print(f"警告: 解析 ULG 文件失败: {ulg_path}")
        print(f"  错误: {e}")
        return None

    data = ULGData()

    # 获取所有可用的 topic
    available_topics = {d.name: d for d in ulog.data_list}

    if not available_topics:
        print(f"警告: ULG 文件没有数据: {ulg_path}")
        return None

    print(f"  可用 topics: {list(available_topics.keys())}")

    # 解析 actuator_outputs（电机输出）
    if 'actuator_outputs' in available_topics:
        topic = available_topics['actuator_outputs']
        df = pd.DataFrame(topic.data)
        data.raw_topics['actuator_outputs'] = df
        if 'timestamp' in df.columns:
            data.timestamps_us = df['timestamp'].values
            # 提取电机输出（output[0-7]）
            output_cols = [c for c in df.columns if c.startswith('output[')]
            if output_cols:
                data.motor_outputs = df[output_cols].values

    # 解析 vehicle_attitude（姿态）
    if 'vehicle_attitude' in available_topics:
        topic = available_topics['vehicle_attitude']
        df = pd.DataFrame(topic.data)
        data.raw_topics['vehicle_attitude'] = df
        if all(c in df.columns for c in ['q[0]', 'q[1]', 'q[2]', 'q[3]']):
            data.attitude_q = df[['q[0]', 'q[1]', 'q[2]', 'q[3]']].values

    # 解析 vehicle_angular_velocity（角速度）
    if 'vehicle_angular_velocity' in available_topics:
        topic = available_topics['vehicle_angular_velocity']
        df = pd.DataFrame(topic.data)
        data.raw_topics['vehicle_angular_velocity'] = df
        if all(c in df.columns for c in ['xyz[0]', 'xyz[1]', 'xyz[2]']):
            data.angular_velocity = df[['xyz[0]', 'xyz[1]', 'xyz[2]']].values

    # 解析 vehicle_local_position（本地位置和速度）
    if 'vehicle_local_position' in available_topics:
        topic = available_topics['vehicle_local_position']
        df = pd.DataFrame(topic.data)
        data.raw_topics['vehicle_local_position'] = df
        if all(c in df.columns for c in ['x', 'y', 'z']):
            data.local_position = df[['x', 'y', 'z']].values
        if all(c in df.columns for c in ['vx', 'vy', 'vz']):
            data.local_velocity = df[['vx', 'vy', 'vz']].values

    # 解析 sensor_combined（IMU 数据）
    if 'sensor_combined' in available_topics:
        topic = available_topics['sensor_combined']
        df = pd.DataFrame(topic.data)
        data.raw_topics['sensor_combined'] = df
        accel_cols = ['accelerometer_m_s2[0]', 'accelerometer_m_s2[1]', 'accelerometer_m_s2[2]']
        gyro_cols = ['gyro_rad[0]', 'gyro_rad[1]', 'gyro_rad[2]']
        if all(c in df.columns for c in accel_cols):
            data.imu_accel = df[accel_cols].values
        if all(c in df.columns for c in gyro_cols):
            data.imu_gyro = df[gyro_cols].values

    # 解析 vehicle_thrust_setpoint（推力设定值）
    if 'vehicle_thrust_setpoint' in available_topics:
        topic = available_topics['vehicle_thrust_setpoint']
        df = pd.DataFrame(topic.data)
        data.raw_topics['vehicle_thrust_setpoint'] = df
        thrust_cols = [c for c in df.columns if c.startswith('xyz[')]
        if thrust_cols:
            data.thrust_setpoint = df[thrust_cols].values

    return data


def interpolate_ulg_to_csv_timestamps(
    ulg_data: ULGData,
    csv_timestamps_s: np.ndarray,
    ulg_start_offset_s: float = 0.0
) -> Dict[str, np.ndarray]:
    """
    将 ULG 数据插值到 CSV 时间戳

    Args:
        ulg_data: ULG 数据对象
        csv_timestamps_s: CSV 时间戳（秒，Unix 时间戳）
        ulg_start_offset_s: ULG 开始时间相对于 CSV 的偏移（秒）

    Returns:
        插值后的数据字典
    """
    result = {}

    if len(ulg_data.timestamps_us) == 0:
        return result

    # ULG 时间戳是相对时间（微秒），需要转换
    ulg_ts_s = ulg_data.timestamps_us / 1e6  # 转为秒

    # 如果有 CSV 时间戳，尝试对齐
    # CSV 时间戳是 Unix 时间戳，ULG 是相对时间
    # 假设第一个 ULG 时间对应 CSV 的第一个时间
    if len(csv_timestamps_s) > 0:
        csv_start = csv_timestamps_s[0]
        ulg_relative = ulg_ts_s - ulg_ts_s[0]  # ULG 相对时间
        csv_relative = csv_timestamps_s - csv_start  # CSV 相对时间

        # 为每个 CSV 时间点找到最近的 ULG 数据
        # 使用线性插值
        from scipy import interpolate as scipy_interp

        def safe_interp(ulg_values: np.ndarray, ulg_time: np.ndarray, target_time: np.ndarray) -> np.ndarray:
            """安全的线性插值"""
            if len(ulg_values) == 0 or len(ulg_time) == 0:
                return np.full((len(target_time), ulg_values.shape[1] if len(ulg_values.shape) > 1 else 1), np.nan)

            try:
                if len(ulg_values.shape) == 1:
                    f = scipy_interp.interp1d(ulg_time, ulg_values, kind='linear',
                                               bounds_error=False, fill_value=np.nan)
                    return f(target_time)
                else:
                    result = np.zeros((len(target_time), ulg_values.shape[1]))
                    for i in range(ulg_values.shape[1]):
                        f = scipy_interp.interp1d(ulg_time, ulg_values[:, i], kind='linear',
                                                   bounds_error=False, fill_value=np.nan)
                        result[:, i] = f(target_time)
                    return result
            except Exception as e:
                print(f"    插值警告: {e}")
                return np.full((len(target_time), ulg_values.shape[1] if len(ulg_values.shape) > 1 else 1), np.nan)

        # 插值各个数据
        if len(ulg_data.motor_outputs) > 0:
            result['motor_outputs'] = safe_interp(ulg_data.motor_outputs, ulg_relative, csv_relative)

        if len(ulg_data.attitude_q) > 0:
            # 姿态需要从 vehicle_attitude topic 的时间戳
            if 'vehicle_attitude' in ulg_data.raw_topics:
                att_df = ulg_data.raw_topics['vehicle_attitude']
                if 'timestamp' in att_df.columns:
                    att_ts = (att_df['timestamp'].values / 1e6) - ulg_ts_s[0]
                    result['attitude_q'] = safe_interp(ulg_data.attitude_q, att_ts, csv_relative)

        if len(ulg_data.angular_velocity) > 0:
            if 'vehicle_angular_velocity' in ulg_data.raw_topics:
                av_df = ulg_data.raw_topics['vehicle_angular_velocity']
                if 'timestamp' in av_df.columns:
                    av_ts = (av_df['timestamp'].values / 1e6) - ulg_ts_s[0]
                    result['angular_velocity'] = safe_interp(ulg_data.angular_velocity, av_ts, csv_relative)

        if len(ulg_data.local_position) > 0:
            if 'vehicle_local_position' in ulg_data.raw_topics:
                lp_df = ulg_data.raw_topics['vehicle_local_position']
                if 'timestamp' in lp_df.columns:
                    lp_ts = (lp_df['timestamp'].values / 1e6) - ulg_ts_s[0]
                    result['local_position'] = safe_interp(ulg_data.local_position, lp_ts, csv_relative)
                    if len(ulg_data.local_velocity) > 0:
                        result['local_velocity'] = safe_interp(ulg_data.local_velocity, lp_ts, csv_relative)

        if len(ulg_data.imu_accel) > 0:
            if 'sensor_combined' in ulg_data.raw_topics:
                sc_df = ulg_data.raw_topics['sensor_combined']
                if 'timestamp' in sc_df.columns:
                    sc_ts = (sc_df['timestamp'].values / 1e6) - ulg_ts_s[0]
                    result['imu_accel'] = safe_interp(ulg_data.imu_accel, sc_ts, csv_relative)
                    if len(ulg_data.imu_gyro) > 0:
                        result['imu_gyro'] = safe_interp(ulg_data.imu_gyro, sc_ts, csv_relative)

    return result


def merge_single_trajectory(csv_path: str, ulg_path: str) -> Optional[pd.DataFrame]:
    """
    合并单个轨迹的 CSV 和 ULG 数据

    Args:
        csv_path: CSV 文件路径
        ulg_path: ULG 文件路径

    Returns:
        合并后的 DataFrame
    """
    print(f"处理: {csv_path}")
    print(f"  ULG: {ulg_path}")

    # 读取 CSV
    try:
        csv_df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"  错误: 无法读取 CSV: {e}")
        return None

    # 解析 ULG
    ulg_data = parse_ulg(ulg_path)
    if ulg_data is None:
        print(f"  警告: 无法解析 ULG，仅返回 CSV 数据")
        return csv_df

    # 获取 CSV 时间戳
    if 'image_timestamp_s' in csv_df.columns:
        csv_timestamps = csv_df['image_timestamp_s'].values
    elif 'timestamp_s' in csv_df.columns:
        csv_timestamps = csv_df['timestamp_s'].values
    else:
        print(f"  警告: CSV 中没有时间戳列")
        return csv_df

    # 插值 ULG 数据到 CSV 时间戳
    try:
        interpolated = interpolate_ulg_to_csv_timestamps(ulg_data, csv_timestamps)
    except Exception as e:
        print(f"  警告: 插值失败: {e}")
        return csv_df

    # 添加 ULG 数据列到 CSV
    for key, values in interpolated.items():
        if len(values.shape) == 1:
            csv_df[f'ulg_{key}'] = values
        else:
            for i in range(values.shape[1]):
                csv_df[f'ulg_{key}_{i}'] = values[:, i]

    # 添加 ULG 统计信息
    csv_df['ulg_has_data'] = ulg_data is not None
    csv_df['ulg_num_actuator_samples'] = len(ulg_data.motor_outputs) if ulg_data else 0

    print(f"  合并完成: {len(csv_df)} 行, 新增 {len([c for c in csv_df.columns if c.startswith('ulg_')])} 列")

    return csv_df


def merge_directory(dir_path: str, output_path: str) -> None:
    """
    合并目录中所有轨迹的数据

    Args:
        dir_path: 轨迹记录目录
        output_path: 输出文件路径
    """
    dir_path = Path(dir_path)
    all_dfs = []

    # 查找所有 data.csv 文件
    csv_files = list(dir_path.rglob("data.csv"))
    print(f"找到 {len(csv_files)} 个 CSV 文件")

    for csv_path in csv_files:
        # 查找对应的 ULG 文件
        csv_dir = csv_path.parent
        ulg_files = list(csv_dir.glob("*.ulg"))

        if not ulg_files:
            print(f"警告: 未找到 ULG 文件: {csv_dir}")
            # 仍然读取 CSV
            try:
                df = pd.read_csv(csv_path)
                df['ulg_available'] = False
                all_dfs.append(df)
            except Exception as e:
                print(f"错误: 无法读取 CSV: {e}")
            continue

        ulg_path = ulg_files[0]  # 使用第一个 ULG

        merged_df = merge_single_trajectory(str(csv_path), str(ulg_path))
        if merged_df is not None:
            merged_df['ulg_available'] = True
            merged_df['source_csv'] = str(csv_path)
            merged_df['source_ulg'] = str(ulg_path)
            all_dfs.append(merged_df)

    if not all_dfs:
        print("错误: 没有成功合并任何数据")
        return

    # 合并所有数据
    combined_df = pd.concat(all_dfs, ignore_index=True)

    # 保存
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_path, index=False)
    print(f"\n合并完成: {output_path}")
    print(f"  总行数: {len(combined_df)}")
    print(f"  总列数: {len(combined_df.columns)}")
    print(f"  ULG 数据列: {[c for c in combined_df.columns if c.startswith('ulg_')]}")


def export_ulg_summary(ulg_path: str, output_path: str) -> None:
    """
    导出 ULG 文件的摘要信息（用于调试）

    Args:
        ulg_path: ULG 文件路径
        output_path: 输出 JSON 文件路径
    """
    ULog = check_pyulog()

    try:
        ulog = ULog(ulg_path)
    except Exception as e:
        print(f"错误: 无法解析 ULG: {e}")
        return

    summary = {
        "file": ulg_path,
        "file_size_bytes": os.path.getsize(ulg_path),
        "start_timestamp": ulog.start_timestamp,
        "duration_s": (ulog.last_timestamp - ulog.start_timestamp) / 1e6 if hasattr(ulog, 'last_timestamp') else None,
        "topics": {},
        "info_messages": {},
        "parameters": {}
    }

    # Topics
    for d in ulog.data_list:
        summary["topics"][d.name] = {
            "multi_id": d.multi_id,
            "num_samples": len(d.data.get('timestamp', [])) if isinstance(d.data, dict) else 0,
            "fields": list(d.data.keys()) if isinstance(d.data, dict) else []
        }

    # Info messages
    for msg in ulog.msg_info_dict.items():
        summary["info_messages"][msg[0]] = str(msg[1])

    # Parameters (前100个)
    param_count = 0
    for p in ulog.initial_parameters:
        if param_count >= 100:
            summary["parameters"]["_note"] = "Only first 100 parameters shown"
            break
        summary["parameters"][p[0]] = str(p[1])
        param_count += 1

    # 保存
    with open(output_path, 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"ULG 摘要已保存: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="ULG 和 CSV 数据合并工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 合并单个文件
  python merge_ulg_csv.py --csv data.csv --ulg flight.ulg --output merged.csv

  # 合并整个目录
  python merge_ulg_csv.py --dir trajectory_recordings/2025-01-07_log --output merged.csv

  # 导出 ULG 摘要（调试用）
  python merge_ulg_csv.py --ulg flight.ulg --summary summary.json
        """
    )

    parser.add_argument('--csv', type=str, help='输入 CSV 文件路径')
    parser.add_argument('--ulg', type=str, help='输入 ULG 文件路径')
    parser.add_argument('--dir', type=str, help='轨迹记录目录（包含多个子目录）')
    parser.add_argument('--output', '-o', type=str, help='输出文件路径')
    parser.add_argument('--summary', type=str, help='导出 ULG 摘要到 JSON 文件')

    args = parser.parse_args()

    # 导出 ULG 摘要
    if args.summary and args.ulg:
        export_ulg_summary(args.ulg, args.summary)
        return

    # 合并目录
    if args.dir:
        if not args.output:
            print("错误: 合并目录模式需要指定 --output")
            sys.exit(1)
        merge_directory(args.dir, args.output)
        return

    # 合并单个文件
    if args.csv and args.ulg:
        if not args.output:
            print("错误: 合并文件模式需要指定 --output")
            sys.exit(1)
        merged_df = merge_single_trajectory(args.csv, args.ulg)
        if merged_df is not None:
            merged_df.to_csv(args.output, index=False)
            print(f"保存到: {args.output}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
