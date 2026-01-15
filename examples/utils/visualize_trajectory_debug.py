#!/usr/bin/env python3
"""可视化轨迹数据，检查问题"""
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
from pathlib import Path
import sys

def load_and_visualize(collected_csv: str, input_json: str = None):
    """加载并可视化轨迹数据"""
    # 加载采集的数据
    df = pd.read_csv(collected_csv)
    print(f"采集数据: {len(df)} 个点")
    print(f"列名: {list(df.columns)}")

    # 提取关键数据
    cmd_in_x = df['cmd_in_x'].values
    cmd_in_y = df['cmd_in_y'].values
    cmd_in_z = df['cmd_in_z'].values

    cmd_x = df['cmd_x'].values
    cmd_y = df['cmd_y'].values
    cmd_z = df['cmd_z'].values

    obs_x = df['obs_pos_x'].values
    obs_y = df['obs_pos_y'].values
    obs_z = df['obs_pos_z'].values

    cmd_vx = df['cmd_vx'].values
    cmd_vy = df['cmd_vy'].values
    cmd_vz = df['cmd_vz'].values

    # 打印统计信息
    print("\n=== 数据统计 ===")
    print(f"cmd_in_z 范围: [{cmd_in_z.min():.3f}, {cmd_in_z.max():.3f}]")
    print(f"cmd_z 范围: [{cmd_z.min():.3f}, {cmd_z.max():.3f}]")
    print(f"obs_z 范围: [{obs_z.min():.3f}, {obs_z.max():.3f}]")
    print(f"cmd_vx 范围: [{cmd_vx.min():.3f}, {cmd_vx.max():.3f}]")
    print(f"cmd_vy 范围: [{cmd_vy.min():.3f}, {cmd_vy.max():.3f}]")
    print(f"cmd_vz 范围: [{cmd_vz.min():.3f}, {cmd_vz.max():.3f}]")

    # 计算误差
    pos_error = np.sqrt((cmd_x - obs_x)**2 + (cmd_y - obs_y)**2 + (cmd_z - obs_z)**2)
    print(f"\n位置误差: 平均={pos_error.mean():.3f}m, 最大={pos_error.max():.3f}m")

    # 如果有原始JSON，也加载对比
    if input_json and Path(input_json).exists():
        with open(input_json) as f:
            traj_data = json.load(f)
        waypoints = traj_data.get('waypoints', [])
        print(f"\n原始轨迹: {len(waypoints)} 个航点")
        if waypoints:
            raw_z = [w.get('z', 0) for w in waypoints]
            print(f"原始z范围: [{min(raw_z):.3f}, {max(raw_z):.3f}]")

    # 创建可视化
    fig = plt.figure(figsize=(16, 12))

    # 1. XY平面轨迹
    ax1 = fig.add_subplot(2, 3, 1)
    ax1.plot(cmd_in_x, cmd_in_y, 'g.-', label='输入轨迹 (cmd_in)', alpha=0.7)
    ax1.plot(cmd_x, cmd_y, 'b.-', label='发送命令 (cmd)', alpha=0.7)
    ax1.plot(obs_x, obs_y, 'r.-', label='实际观测 (obs)', alpha=0.7)
    ax1.set_xlabel('X (m)')
    ax1.set_ylabel('Y (m)')
    ax1.set_title('XY平面轨迹')
    ax1.legend()
    ax1.grid(True)
    ax1.axis('equal')

    # 2. Z高度随时间变化
    ax2 = fig.add_subplot(2, 3, 2)
    t = np.arange(len(df)) * 0.2  # 假设5Hz采样
    ax2.plot(t, cmd_in_z, 'g.-', label='输入z (cmd_in_z)')
    ax2.plot(t, cmd_z, 'b.-', label='命令z (cmd_z)')
    ax2.plot(t, obs_z, 'r.-', label='观测z (obs_z)')
    ax2.set_xlabel('时间 (s)')
    ax2.set_ylabel('Z (m)')
    ax2.set_title('高度随时间变化')
    ax2.legend()
    ax2.grid(True)

    # 3. 位置误差
    ax3 = fig.add_subplot(2, 3, 3)
    ax3.plot(t, pos_error, 'k.-')
    ax3.set_xlabel('时间 (s)')
    ax3.set_ylabel('位置误差 (m)')
    ax3.set_title('位置误差')
    ax3.grid(True)

    # 4. 速度命令
    ax4 = fig.add_subplot(2, 3, 4)
    ax4.plot(t, cmd_vx, 'r.-', label='vx')
    ax4.plot(t, cmd_vy, 'g.-', label='vy')
    ax4.plot(t, cmd_vz, 'b.-', label='vz')
    ax4.set_xlabel('时间 (s)')
    ax4.set_ylabel('速度 (m/s)')
    ax4.set_title('速度命令')
    ax4.legend()
    ax4.grid(True)

    # 5. 3D轨迹
    ax5 = fig.add_subplot(2, 3, 5, projection='3d')
    ax5.plot(cmd_in_x, cmd_in_y, cmd_in_z, 'g.-', label='输入轨迹')
    ax5.plot(cmd_x, cmd_y, cmd_z, 'b.-', label='发送命令')
    ax5.plot(obs_x, obs_y, obs_z, 'r.-', label='实际观测')
    ax5.set_xlabel('X (m)')
    ax5.set_ylabel('Y (m)')
    ax5.set_zlabel('Z (m)')
    ax5.set_title('3D轨迹')
    ax5.legend()

    # 6. 速度大小
    ax6 = fig.add_subplot(2, 3, 6)
    vel_mag = np.sqrt(cmd_vx**2 + cmd_vy**2 + cmd_vz**2)
    ax6.plot(t, vel_mag, 'k.-')
    ax6.set_xlabel('时间 (s)')
    ax6.set_ylabel('速度大小 (m/s)')
    ax6.set_title('速度大小 - 航点处应为0!')
    ax6.grid(True)

    plt.tight_layout()
    output_path = collected_csv.replace('.csv', '_debug.png')
    plt.savefig(output_path, dpi=150)
    print(f"\n图表已保存: {output_path}")
    plt.close()

    return df

if __name__ == "__main__":
    # 找几条轨迹来分析
    collected_dir = Path.home() / "uav-data/drone/uav-flow-sim/train_data/collected_isaac_sim"
    input_dir = Path.home() / "uav-data/drone/uav-flow-sim/train_data/extracted_json_files"

    trajs = list(collected_dir.iterdir())[:3]  # 取前3条

    for traj_dir in trajs:
        csv_files = list(traj_dir.glob("*/data.csv"))
        if csv_files:
            csv_path = str(csv_files[0])
            json_path = str(input_dir / f"{traj_dir.name}.json")
            print(f"\n{'='*60}")
            print(f"分析: {traj_dir.name}")
            print('='*60)
            load_and_visualize(csv_path, json_path)
