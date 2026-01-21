#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import argparse
from pathlib import Path

def analyze_trajectory(csv_path, output_dir):
    df = pd.read_csv(csv_path)
    if len(df) < 10:
        print(f"Error: Not enough data in {csv_path}")
        return

    # Tracking Error: Isaac vs PX4 EKF
    # (Assuming Isaac is Ground Truth)
    df['err_x'] = df['isaac_x'] - df['px4_x']
    df['err_y'] = df['isaac_y'] - df['px4_y']
    df['err_z'] = df['isaac_z'] - df['px4_z']
    df['err_dist'] = np.sqrt(df['err_x']**2 + df['err_y']**2 + df['err_z']**2)

    rmse = np.sqrt(np.mean(df['err_dist']**2))
    max_err = df['err_dist'].max()
    mean_err = df['err_dist'].mean()

    print(f"--- Analysis for {csv_path} ---")
    print(f"Total Points: {len(df)}")
    print(f"RMSE: {rmse:.4f}m")
    print(f"Max Error: {max_err:.4f}m")
    print(f"Mean Error: {mean_err:.4f}m")

    # Plotting
    plt.figure(figsize=(15, 10))
    
    # XY Path
    plt.subplot(2, 2, 1)
    plt.plot(df['isaac_x'].to_numpy(), df['isaac_y'].to_numpy(), label='Isaac (GT)')
    plt.plot(df['px4_x'].to_numpy(), df['px4_y'].to_numpy(), '--', label='PX4 EKF')
    plt.xlabel('X (m)')
    plt.ylabel('Y (m)')
    plt.title('XY Trajectory')
    plt.legend()
    plt.grid(True)

    # Altitude
    plt.subplot(2, 2, 2)
    plt.plot(df['sim_time'].to_numpy(), df['isaac_z'].to_numpy(), label='Isaac Z')
    plt.plot(df['sim_time'].to_numpy(), df['px4_z'].to_numpy(), '--', label='PX4 Z')
    plt.xlabel('Sim Time (s)')
    plt.ylabel('Altitude (m)')
    plt.title('Altitude Tracking')
    plt.legend()
    plt.grid(True)

    # Error over time
    plt.subplot(2, 2, 3)
    plt.plot(df['sim_time'].to_numpy(), df['err_dist'].to_numpy(), label='Distance Error')
    plt.axhline(y=rmse, color='r', linestyle='-', label='RMSE')
    plt.xlabel('Sim Time (s)')
    plt.ylabel('Error (m)')
    plt.title('Tracking Error over Time')
    plt.legend()
    plt.grid(True)

    # Velocity comparison
    plt.subplot(2, 2, 4)
    v_isaac = np.sqrt(df['isaac_vx']**2 + df['isaac_vy']**2 + df['isaac_vz']**2)
    v_px4 = np.sqrt(df['px4_vx']**2 + df['px4_vy']**2 + df['px4_vz']**2)
    plt.plot(df['sim_time'].to_numpy(), v_isaac.to_numpy(), label='Isaac Vel')
    plt.plot(df['sim_time'].to_numpy(), v_px4.to_numpy(), '--', label='PX4 Vel')
    plt.xlabel('Sim Time (s)')
    plt.ylabel('Velocity (m/s)')
    plt.title('Velocity Magnitude')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(output_dir / "accuracy_analysis.png")
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--out", default=".")
    args = parser.parse_args()
    
    analyze_trajectory(Path(args.csv), Path(args.out))
