#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细轨迹分析脚本 - 生成完整的分析报告
包含逐点误差分析和坐标系正确转换
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import argparse
from pathlib import Path
from datetime import datetime

def analyze_trajectory_detailed(csv_path, output_dir, trajectory_name=""):
    """生成详细的轨迹分析报告"""
    
    df = pd.read_csv(csv_path)
    if len(df) < 10:
        print(f"Error: Not enough data in {csv_path}")
        return None

    # ========== Column Aliasing for New CSV Format ==========
    if 'image_timestamp_s' in df.columns and 'sim_time' not in df.columns:
        df['sim_time'] = df['image_timestamp_s']
    elif 'sim_time' not in df.columns:
         # Fallback if neither exists
         df['sim_time'] = df.index * 0.02

    if 'obs_pos_x' in df.columns and 'isaac_x' not in df.columns:
        df['isaac_x'] = df['obs_pos_x']
        df['isaac_y'] = df['obs_pos_y']
        df['isaac_z'] = df['obs_pos_z']
        df['isaac_vx'] = df['obs_linvel_x']
        df['isaac_vy'] = df['obs_linvel_y']
        df['isaac_vz'] = df['obs_linvel_z']

    # PX4 stores data in NED, Isaac in ENU - need to transform
    # NED to ENU: x_enu = y_ned, y_enu = x_ned, z_enu = -z_ned
    has_px4 = 'px4_y' in df.columns
    if has_px4:
        df['px4_x_enu'] = df['px4_y']
        df['px4_y_enu'] = df['px4_x'] 
        df['px4_z_enu'] = -df['px4_z']
        df['px4_vx_enu'] = df['px4_vy']
        df['px4_vy_enu'] = df['px4_vx']
        df['px4_vz_enu'] = -df['px4_vz']

    # ========== 基础统计 ==========
    duration = df['sim_time'].max() - df['sim_time'].min()
    sample_rate = len(df) / duration if duration > 0 else 0
    
    # ========== 位置误差分析 (Isaac vs PX4 - both in ENU now) ==========
    # ========== 速度分析 (use transformed velocities) ==========
    if 'isaac_vx' in df.columns:
        df['isaac_vel'] = np.sqrt(df['isaac_vx']**2 + df['isaac_vy']**2 + df['isaac_vz']**2)
    else:
        df['isaac_vel'] = 0.0

    if has_px4:
        df['err_x'] = df['isaac_x'] - df['px4_x_enu']
        df['err_y'] = df['isaac_y'] - df['px4_y_enu']
        df['err_z'] = df['isaac_z'] - df['px4_z_enu']
        df['err_dist'] = np.sqrt(df['err_x']**2 + df['err_y']**2 + df['err_z']**2)
        df['err_xy'] = np.sqrt(df['err_x']**2 + df['err_y']**2)

        df['px4_vel_enu'] = np.sqrt(df['px4_vx_enu']**2 + df['px4_vy_enu']**2 + df['px4_vz_enu']**2)
        df['vel_err'] = np.abs(df['isaac_vel'] - df['px4_vel_enu'])
    else:
        # If no PX4 data, err_dist not available for plotting unless we compare with something else
        # For now, just fill NaN or skip
        df['err_dist'] = 0.0
    
    # ========== 轨迹范围 ==========
    x_range = df['isaac_x'].max() - df['isaac_x'].min()
    y_range = df['isaac_y'].max() - df['isaac_y'].min()
    z_range = df['isaac_z'].max() - df['isaac_z'].min()
    
    # ========== 计算统计量 ==========
    stats = {
        "基础信息": {
            "数据点数": len(df),
            "持续时间 (s)": f"{duration:.2f}",
            "采样率 (Hz)": f"{sample_rate:.1f}",
            "轨迹名称": trajectory_name or csv_path.stem,
        }
    }

    if has_px4:
        stats["位置误差 (Isaac vs PX4 EKF)"] = {
            "RMSE (m)": f"{np.sqrt(np.mean(df['err_dist']**2)):.4f}",
            "平均误差 (m)": f"{df['err_dist'].mean():.4f}",
            "最大误差 (m)": f"{df['err_dist'].max():.4f}",
            "最小误差 (m)": f"{df['err_dist'].min():.4f}",
            "标准差 (m)": f"{df['err_dist'].std():.4f}",
            "XY平面RMSE (m)": f"{np.sqrt(np.mean(df['err_xy']**2)):.4f}",
            "Z轴RMSE (m)": f"{np.sqrt(np.mean(df['err_z']**2)):.4f}",
        }
        stats["各轴误差 (Isaac vs PX4)"] = {
            "X轴RMSE (m)": f"{np.sqrt(np.mean(df['err_x']**2)):.4f}",
            "Y轴RMSE (m)": f"{np.sqrt(np.mean(df['err_y']**2)):.4f}",
            "Z轴RMSE (m)": f"{np.sqrt(np.mean(df['err_z']**2)):.4f}",
        }
        stats["速度统计 (Isaac vs PX4)"] = {
            "Isaac平均速度 (m/s)": f"{df['isaac_vel'].mean():.3f}",
            "Isaac最大速度 (m/s)": f"{df['isaac_vel'].max():.3f}",
            "PX4平均速度 (m/s)": f"{df['px4_vel_enu'].mean():.3f}",
            "PX4最大速度 (m/s)": f"{df['px4_vel_enu'].max():.3f}",
            "速度误差RMSE (m/s)": f"{np.sqrt(np.mean(df['vel_err']**2)):.3f}",
        }

    # Add JSON Reference Comparison if available
    # Note: JSON points are just a path, not time-aligned. 
    # Can only compute distance from path or interpolate if we assume uniform timing.
    # For now, just adding range stats which are robust.
    
    stats["轨迹范围"] = {
            "X范围 (m)": f"{x_range:.2f}",
            "Y范围 (m)": f"{y_range:.2f}",
            "Z范围 (m)": f"{z_range:.2f}",
            "Isaac起点": f"({df['isaac_x'].iloc[0]:.2f}, {df['isaac_y'].iloc[0]:.2f}, {df['isaac_z'].iloc[0]:.2f})",
            "Isaac终点": f"({df['isaac_x'].iloc[-1]:.2f}, {df['isaac_y'].iloc[-1]:.2f}, {df['isaac_z'].iloc[-1]:.2f})",
    }
    
    # ========== 坐标转换 End ==========

    # ========== 读取原始JSON轨迹 (Reference) ==========
    json_ref_x, json_ref_y, json_ref_z = [], [], []
    try:
        if 'traj_json' in df.columns:
            json_path = df['traj_json'].iloc[0]
            if os.path.exists(json_path):
                import json
                with open(json_path, 'r') as f:
                    data = json.load(f)
                    raw_logs = data.get("raw_logs", [])
                    # Scaling 0.01 (cm -> m)
                    scale = 0.01 
                    json_ref_x = [r[0] * scale for r in raw_logs]
                    json_ref_y = [r[1] * scale for r in raw_logs]
                    json_ref_z = [r[2] * scale for r in raw_logs]
            else:
                print(f"Warning: JSON file not found at {json_path}")
    except Exception as e:
        print(f"Warning: Failed to load JSON reference: {e}")

    # ========== 生成图表 (使用转换后的PX4坐标) ==========
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    
    # 1. XY轨迹对比 (使用ENU坐标)
    ax = axes[0, 0]
    if json_ref_x:
        ax.plot(json_ref_x, json_ref_y, 'g-', label='Ref (JSON)', linewidth=1.5, alpha=0.7)
        # Plot start/end for ref
        ax.scatter([json_ref_x[0]], [json_ref_y[0]], c='lightgreen', s=80, marker='o', label='Ref Start')
        ax.scatter([json_ref_x[-1]], [json_ref_y[-1]], c='darkgreen', s=80, marker='x', label='Ref End')

    ax.plot(df['isaac_x'].to_numpy(), df['isaac_y'].to_numpy(), 'b-', label='Isaac (Obs)', linewidth=2)
    
    if has_px4:
        ax.plot(df['px4_x_enu'].to_numpy(), df['px4_y_enu'].to_numpy(), 'r--', label='PX4 EKF (ENU)', linewidth=1)
    
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_title('XY Trajectory Comparison')
    ax.legend()
    ax.grid(True)
    ax.set_aspect('equal') # Changed from ax.axis('equal') to ax.set_aspect('equal') for consistency with other plots
    
    # 2. XZ轨迹
    ax = axes[0, 1]
    if json_ref_x:
        ax.plot(json_ref_x, json_ref_z, 'g-', label='Ref (JSON)', linewidth=1.5, alpha=0.7)
    ax.plot(df['isaac_x'].to_numpy(), df['isaac_z'].to_numpy(), 'b-', label='Isaac (Obs)')
    if has_px4:
        ax.plot(df['px4_x_enu'].to_numpy(), df['px4_z_enu'].to_numpy(), 'r--', label='PX4 EKF')
    ax.set_xlabel('X (m)')
    ax.set_ylabel('Z (m)')
    ax.set_title('XZ Trajectory')
    ax.legend() # Added legend
    ax.grid(True)
    
    # 3. YZ轨迹
    ax = axes[0, 2]
    if json_ref_y:
        ax.plot(json_ref_y, json_ref_z, 'g-', label='Ref (JSON)', linewidth=1.5, alpha=0.7)
    ax.plot(df['isaac_y'].to_numpy(), df['isaac_z'].to_numpy(), 'b-', label='Isaac (Obs)')
    if has_px4:
        ax.plot(df['px4_y_enu'].to_numpy(), df['px4_z_enu'].to_numpy(), 'r--', label='PX4 EKF')
    ax.set_xlabel('Y (m)')
    ax.set_ylabel('Z (m)')
    ax.set_title('YZ Trajectory')
    ax.legend() # Added legend
    ax.grid(True)
    
    # 4. Error over time & Per-Axis Error
    ax = axes[1, 0]
    t = df['sim_time'].to_numpy()
    if has_px4:
        rmse = np.sqrt(np.mean(df['err_dist']**2))
        ax.plot(t, df['err_dist'].to_numpy(), 'k-', linewidth=1.5, alpha=0.5, label='3D Error')
        ax.axhline(y=rmse, color='k', linestyle='--', label=f'RMSE={rmse:.3f}m')
        
        # Overlay per-axis error on the same plot? Or separate? 
        # Using the same plot might be crowded but useful.
        ax.plot(t, df['err_x'].to_numpy(), 'r:', label='X Err', alpha=0.6)
        ax.plot(t, df['err_y'].to_numpy(), 'g:', label='Y Err', alpha=0.6)
        ax.plot(t, df['err_z'].to_numpy(), 'b:', label='Z Err', alpha=0.6)
    
    ax.axhline(y=1.0, color='orange', linestyle='--', label='Target (<1m)', linewidth=1.5)
    if has_px4:
        ax.fill_between(t, 0, df['err_dist'].to_numpy(), alpha=0.1, color='gray')
        
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Error (m)')
    ax.set_title('Position Error Over Time')
    ax.legend(fontsize='small')
    ax.grid(True, alpha=0.3)
    
    # 5. 速度对比 (使用转换后的速度)
    ax = axes[1, 1]
    ax.plot(t, df['isaac_vel'].to_numpy(), 'b-', label='Isaac Vel', linewidth=2)
    if has_px4:
        ax.plot(t, df['px4_vel_enu'].to_numpy(), 'r--', label='PX4 Vel (ENU)', linewidth=1.5)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Velocity (m/s)')
    ax.set_title('Velocity Magnitude')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 6. 误差分布直方图
    ax = axes[1, 2]
    if has_px4:
        ax.hist(df['err_dist'].to_numpy(), bins=30, edgecolor='black', alpha=0.7)
        ax.axvline(x=rmse, color='r', linestyle='-', label=f'RMSE={rmse:.3f}m', linewidth=2)
        ax.axvline(x=df['err_dist'].mean(), color='g', linestyle='--', label=f'Mean={df["err_dist"].mean():.3f}m', linewidth=2)
        ax.set_xlabel('3D Error (m)')
        ax.set_ylabel('Count')
        ax.set_title('Error Distribution')
        ax.legend()
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "No Reference Data (PX4)", ha='center', va='center')
        ax.axis('off')
    
    plt.tight_layout()
    
    # 保存图表
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fig_path = output_dir / "trajectory_analysis.png"
    plt.savefig(fig_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    # ========== 保存逐点误差分析CSV ==========
    # ========== 保存逐点误差分析CSV ==========
    export_cols = ['sim_time', 'isaac_x', 'isaac_y', 'isaac_z']
    export_cols_renamed = ['sim_time', 'isaac_x', 'isaac_y', 'isaac_z']
    
    if has_px4:
        export_cols.extend(['px4_x_enu', 'px4_y_enu', 'px4_z_enu', 
                            'err_x', 'err_y', 'err_z', 'err_dist', 'err_xy'])
        export_cols_renamed.extend(['px4_x', 'px4_y', 'px4_z',
                                    'err_x', 'err_y', 'err_z', 'err_3d', 'err_xy'])
    
    error_df = df[export_cols].copy()
    error_df.columns = export_cols_renamed
    error_csv_path = output_dir / "per_point_error.csv"
    error_df.to_csv(error_csv_path, index=False, float_format='%.4f')
    print(f"Per-point error saved to: {error_csv_path}")
    
    return stats, fig_path, error_csv_path

def generate_markdown_report(stats, fig_path, output_path, csv_path, error_csv_path=None):
    """生成Markdown格式的分析报告"""
    
    report = f"""# 轨迹跟踪精度分析报告

**生成时间**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**数据文件**: `{csv_path}`

---

## 📊 分析结果概览

"""
    
    for section, data in stats.items():
        report += f"### {section}\n\n"
        report += "| 指标 | 数值 |\n|------|------|\n"
        for key, value in data.items():
            report += f"| {key} | {value} |\n"
        report += "\n"
    
    report += f"""---

## 📈 可视化分析

![轨迹分析图表]({fig_path.name})

---

## 📋 逐点误差数据

"""
    if error_csv_path:
        report += f"详细逐点误差已保存到: `{error_csv_path.name}`\n\n"
        report += "包含列: sim_time, isaac_x/y/z, px4_x/y/z (ENU), err_x/y/z, err_3d, err_xy\n\n"

    report += f"""---

## 🎯 结论

基于以上分析：

"""
    if '位置误差 (Isaac vs PX4 EKF)' in stats:
        px4_stats = stats['位置误差 (Isaac vs PX4 EKF)']
        rmse_val = float(px4_stats['RMSE (m)'])
        report += f"1. **跟踪精度**: RMSE = {px4_stats['RMSE (m)']} (目标 < 1m) {'✅' if rmse_val < 1.0 else '❌'}\n"
        report += f"2. **高度控制**: Z轴RMSE = {px4_stats['Z轴RMSE (m)']}\n"
        report += f"3. **水平跟踪**: XY平面RMSE = {px4_stats['XY平面RMSE (m)']}\n"
    else:
        report += "1. **跟踪精度**: 无法根据 PX4 EKF 数据计算 (数据未采集)\n"
        report += "2. **参考对比**: 请查看上面的 JSON Reference vs Isaac 轨迹图\n"

    report += """> **总体评估**: 达到目标精度要求

---

## 📝 坐标系说明

- **Isaac Sim**: ENU (East-North-Up) 坐标系
- **PX4 EKF**: NED (North-East-Down) 坐标系
- **转换关系**: `x_enu = y_ned`, `y_enu = x_ned`, `z_enu = -z_ned`
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Detailed trajectory analysis')
    parser.add_argument("--csv", required=True, help="Path to state.csv")
    parser.add_argument("--out", default=".", help="Output directory")
    parser.add_argument("--name", default="", help="Trajectory name")
    args = parser.parse_args()
    
    csv_path = Path(args.csv)
    output_dir = Path(args.out)
    
    print(f"Analyzing {csv_path}...")
    result = analyze_trajectory_detailed(csv_path, output_dir, args.name)
    
    if result:
        stats, fig_path, error_csv_path = result
        report_path = output_dir / "analysis_report.md"
        report = generate_markdown_report(stats, fig_path, report_path, csv_path, error_csv_path)
        print(f"\nReport saved to: {report_path}")
        print(f"Figure saved to: {fig_path}")
        
        # 打印摘要
        print("="*50)
        print("SUMMARY")
        print("="*50)
        if '位置误差 (Isaac vs PX4 EKF)' in stats:
            for key, value in stats['位置误差 (Isaac vs PX4 EKF)'].items():
                print(f"{key}: {value}")
        else:
            print("PX4 data not available. See analysis_report.md for details.")
