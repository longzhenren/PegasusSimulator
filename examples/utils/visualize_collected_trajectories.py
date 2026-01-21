#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
Visualize collected trajectory data - comparing commanded vs actual positions
with velocity arrows showing direction of movement
"""
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import argparse

def load_trajectory_data(csv_path):
    """Load trajectory data from CSV file"""
    df = pd.read_csv(csv_path)
    return df

def compute_tracking_error(df):
    """Compute tracking error between command and observation"""
    cmd_pos = df[['cmd_x', 'cmd_y', 'cmd_z']].values
    obs_pos = df[['obs_pos_x', 'obs_pos_y', 'obs_pos_z']].values
    errors = np.linalg.norm(cmd_pos - obs_pos, axis=1)
    return errors

def plot_trajectory_3d(df, traj_name, ax=None, show_velocity=True, velocity_scale=0.1, velocity_skip=3):
    """Plot 3D trajectory comparison with optional velocity arrows

    Args:
        df: DataFrame with trajectory data
        traj_name: Name of trajectory for title
        ax: Matplotlib 3D axis (optional)
        show_velocity: Whether to show velocity arrows
        velocity_scale: Scale factor for velocity arrows
        velocity_skip: Show every N-th velocity arrow (to reduce clutter)
    """
    if ax is None:
        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, projection='3d')

    # Convert to numpy arrays for compatibility
    cmd_x = df['cmd_x'].values
    cmd_y = df['cmd_y'].values
    cmd_z = df['cmd_z'].values
    obs_x = df['obs_pos_x'].values
    obs_y = df['obs_pos_y'].values
    obs_z = df['obs_pos_z'].values

    # Command trajectory
    ax.plot(cmd_x, cmd_y, cmd_z,
            'b-', linewidth=2, label='Command', alpha=0.8)
    ax.scatter([cmd_x[0]], [cmd_y[0]], [cmd_z[0]],
               c='blue', s=100, marker='o', label='Cmd Start')
    ax.scatter([cmd_x[-1]], [cmd_y[-1]], [cmd_z[-1]],
               c='blue', s=100, marker='s', label='Cmd End')

    # Actual trajectory
    ax.plot(obs_x, obs_y, obs_z,
            'r-', linewidth=2, label='Actual', alpha=0.8)
    ax.scatter([obs_x[0]], [obs_y[0]], [obs_z[0]],
               c='red', s=100, marker='o')
    ax.scatter([obs_x[-1]], [obs_y[-1]], [obs_z[-1]],
               c='red', s=100, marker='s')

    # Plot velocity arrows
    if show_velocity:
        # Check for velocity columns
        has_cmd_vel = all(col in df.columns for col in ['cmd_vx', 'cmd_vy', 'cmd_vz'])
        has_obs_vel = all(col in df.columns for col in ['obs_linvel_x', 'obs_linvel_y', 'obs_linvel_z'])

        # Command velocity arrows (cyan)
        if has_cmd_vel:
            cmd_vx = df['cmd_vx'].values
            cmd_vy = df['cmd_vy'].values
            cmd_vz = df['cmd_vz'].values

            # Plot every N-th arrow
            indices = range(0, len(cmd_x), velocity_skip)
            ax.quiver(cmd_x[::velocity_skip], cmd_y[::velocity_skip], cmd_z[::velocity_skip],
                     cmd_vx[::velocity_skip] * velocity_scale,
                     cmd_vy[::velocity_skip] * velocity_scale,
                     cmd_vz[::velocity_skip] * velocity_scale,
                     color='cyan', alpha=0.6, arrow_length_ratio=0.3,
                     label='Cmd Velocity')

        # Observed velocity arrows (orange)
        if has_obs_vel:
            obs_vx = df['obs_linvel_x'].values
            obs_vy = df['obs_linvel_y'].values
            obs_vz = df['obs_linvel_z'].values

            ax.quiver(obs_x[::velocity_skip], obs_y[::velocity_skip], obs_z[::velocity_skip],
                     obs_vx[::velocity_skip] * velocity_scale,
                     obs_vy[::velocity_skip] * velocity_scale,
                     obs_vz[::velocity_skip] * velocity_scale,
                     color='orange', alpha=0.6, arrow_length_ratio=0.3,
                     label='Obs Velocity')

    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_zlabel('Z (m)')
    ax.set_title(f'3D Trajectory: {traj_name}')
    ax.legend(loc='upper left', fontsize='small')

    # Z轴从0开始
    z_min = 0
    z_max = max(np.max(cmd_z), np.max(obs_z)) * 1.1
    ax.set_zlim(z_min, z_max)

    return ax

def plot_trajectory_2d(df, traj_name, axes=None, show_velocity=True, velocity_scale=0.1, velocity_skip=3):
    """Plot 2D trajectory comparison (XY, XZ, YZ planes) with velocity arrows"""
    if axes is None:
        fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    # Convert to numpy arrays
    cmd_x = df['cmd_x'].values
    cmd_y = df['cmd_y'].values
    cmd_z = df['cmd_z'].values
    obs_x = df['obs_pos_x'].values
    obs_y = df['obs_pos_y'].values
    obs_z = df['obs_pos_z'].values

    # Check for velocity columns
    has_cmd_vel = all(col in df.columns for col in ['cmd_vx', 'cmd_vy', 'cmd_vz'])
    has_obs_vel = all(col in df.columns for col in ['obs_linvel_x', 'obs_linvel_y', 'obs_linvel_z'])

    if has_cmd_vel:
        cmd_vx = df['cmd_vx'].values
        cmd_vy = df['cmd_vy'].values
        cmd_vz = df['cmd_vz'].values
    if has_obs_vel:
        obs_vx = df['obs_linvel_x'].values
        obs_vy = df['obs_linvel_y'].values
        obs_vz = df['obs_linvel_z'].values

    # XY plane
    axes[0].plot(cmd_x, cmd_y, 'b-', linewidth=2, label='Command')
    axes[0].plot(obs_x, obs_y, 'r-', linewidth=2, label='Actual')
    axes[0].scatter([cmd_x[0]], [cmd_y[0]], c='blue', s=80, marker='o')
    axes[0].scatter([obs_x[0]], [obs_y[0]], c='red', s=80, marker='o')

    if show_velocity:
        if has_cmd_vel:
            axes[0].quiver(cmd_x[::velocity_skip], cmd_y[::velocity_skip],
                          cmd_vx[::velocity_skip] * velocity_scale,
                          cmd_vy[::velocity_skip] * velocity_scale,
                          color='cyan', alpha=0.6, scale=1, scale_units='xy',
                          angles='xy', width=0.005)
        if has_obs_vel:
            axes[0].quiver(obs_x[::velocity_skip], obs_y[::velocity_skip],
                          obs_vx[::velocity_skip] * velocity_scale,
                          obs_vy[::velocity_skip] * velocity_scale,
                          color='orange', alpha=0.6, scale=1, scale_units='xy',
                          angles='xy', width=0.005)

    axes[0].set_xlabel('X (m)')
    axes[0].set_ylabel('Y (m)')
    axes[0].set_title('XY Plane')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[0].axis('equal')

    # XZ plane
    axes[1].plot(cmd_x, cmd_z, 'b-', linewidth=2, label='Command')
    axes[1].plot(obs_x, obs_z, 'r-', linewidth=2, label='Actual')
    axes[1].scatter([cmd_x[0]], [cmd_z[0]], c='blue', s=80, marker='o')
    axes[1].scatter([obs_x[0]], [obs_z[0]], c='red', s=80, marker='o')

    if show_velocity:
        if has_cmd_vel:
            axes[1].quiver(cmd_x[::velocity_skip], cmd_z[::velocity_skip],
                          cmd_vx[::velocity_skip] * velocity_scale,
                          cmd_vz[::velocity_skip] * velocity_scale,
                          color='cyan', alpha=0.6, scale=1, scale_units='xy',
                          angles='xy', width=0.005)
        if has_obs_vel:
            axes[1].quiver(obs_x[::velocity_skip], obs_z[::velocity_skip],
                          obs_vx[::velocity_skip] * velocity_scale,
                          obs_vz[::velocity_skip] * velocity_scale,
                          color='orange', alpha=0.6, scale=1, scale_units='xy',
                          angles='xy', width=0.005)

    axes[1].set_xlabel('X (m)')
    axes[1].set_ylabel('Z (m)')
    axes[1].set_title('XZ Plane')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    # Z轴从0开始
    z_max = max(np.max(cmd_z), np.max(obs_z)) * 1.1
    axes[1].set_ylim(0, z_max)

    # YZ plane
    axes[2].plot(cmd_y, cmd_z, 'b-', linewidth=2, label='Command')
    axes[2].plot(obs_y, obs_z, 'r-', linewidth=2, label='Actual')
    axes[2].scatter([cmd_y[0]], [cmd_z[0]], c='blue', s=80, marker='o')
    axes[2].scatter([obs_y[0]], [obs_z[0]], c='red', s=80, marker='o')

    if show_velocity:
        if has_cmd_vel:
            axes[2].quiver(cmd_y[::velocity_skip], cmd_z[::velocity_skip],
                          cmd_vy[::velocity_skip] * velocity_scale,
                          cmd_vz[::velocity_skip] * velocity_scale,
                          color='cyan', alpha=0.6, scale=1, scale_units='xy',
                          angles='xy', width=0.005)
        if has_obs_vel:
            axes[2].quiver(obs_y[::velocity_skip], obs_z[::velocity_skip],
                          obs_vy[::velocity_skip] * velocity_scale,
                          obs_vz[::velocity_skip] * velocity_scale,
                          color='orange', alpha=0.6, scale=1, scale_units='xy',
                          angles='xy', width=0.005)

    axes[2].set_xlabel('Y (m)')
    axes[2].set_ylabel('Z (m)')
    axes[2].set_title('YZ Plane')
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    # Z轴从0开始
    axes[2].set_ylim(0, z_max)

    return axes

def plot_tracking_error(df, traj_name, ax=None):
    """Plot tracking error over time"""
    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 4))

    errors = compute_tracking_error(df)
    time_idx = np.arange(len(errors))

    ax.plot(time_idx, errors, 'g-', linewidth=2)
    ax.fill_between(time_idx, 0, errors, alpha=0.3, color='green')
    ax.axhline(y=np.mean(errors), color='r', linestyle='--',
               label=f'Mean: {np.mean(errors):.4f}m')
    ax.axhline(y=np.max(errors), color='orange', linestyle=':',
               label=f'Max: {np.max(errors):.4f}m')

    ax.set_xlabel('Step')
    ax.set_ylabel('Tracking Error (m)')
    ax.set_title(f'Tracking Error: {traj_name}')
    ax.legend()
    ax.grid(True, alpha=0.3)

    return ax

def visualize_single_trajectory(csv_path, output_dir=None):
    """Visualize a single trajectory"""
    df = load_trajectory_data(csv_path)
    traj_name = os.path.basename(os.path.dirname(os.path.dirname(csv_path)))

    # Convert to numpy arrays
    cmd_x = df['cmd_x'].values
    cmd_y = df['cmd_y'].values
    cmd_z = df['cmd_z'].values
    obs_x = df['obs_pos_x'].values
    obs_y = df['obs_pos_y'].values
    obs_z = df['obs_pos_z'].values

    # Create figure with multiple subplots
    fig = plt.figure(figsize=(16, 12))

    # 3D trajectory
    ax1 = fig.add_subplot(2, 2, 1, projection='3d')
    plot_trajectory_3d(df, traj_name, ax1)

    # 2D projections
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.plot(cmd_x, cmd_y, 'b-', linewidth=2, label='Command')
    ax2.plot(obs_x, obs_y, 'r-', linewidth=2, label='Actual')
    ax2.set_xlabel('X (m)')
    ax2.set_ylabel('Y (m)')
    ax2.set_title('XY Plane')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.axis('equal')

    # Altitude over time
    ax3 = fig.add_subplot(2, 2, 3)
    ax3.plot(cmd_z, 'b-', linewidth=2, label='Command Z')
    ax3.plot(obs_z, 'r-', linewidth=2, label='Actual Z')
    ax3.set_xlabel('Step')
    ax3.set_ylabel('Z (m)')
    ax3.set_title('Altitude over Time')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    # Z轴从0开始
    z_max = max(np.max(cmd_z), np.max(obs_z)) * 1.1
    ax3.set_ylim(0, z_max)

    # Tracking error
    ax4 = fig.add_subplot(2, 2, 4)
    plot_tracking_error(df, traj_name, ax4)

    plt.suptitle(f'Trajectory Analysis: {traj_name}', fontsize=14, fontweight='bold')
    plt.tight_layout()

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f'{traj_name}_analysis.png')
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved: {output_path}")

    return fig

def visualize_multiple_trajectories(traj_dir, output_dir=None, max_trajs=10):
    """Visualize multiple trajectories from a directory"""
    # Find all data.csv files
    csv_files = []
    for root, dirs, files in os.walk(traj_dir):
        for f in files:
            if f == 'data.csv':
                csv_files.append(os.path.join(root, f))

    csv_files = csv_files[:max_trajs]
    print(f"Found {len(csv_files)} trajectories to visualize")

    # Summary statistics
    all_errors = []
    traj_names = []

    for csv_path in csv_files:
        df = load_trajectory_data(csv_path)
        traj_name = os.path.basename(os.path.dirname(os.path.dirname(csv_path)))
        errors = compute_tracking_error(df)
        all_errors.append({
            'name': traj_name,
            'mean': np.mean(errors),
            'max': np.max(errors),
            'final': errors[-1] if len(errors) > 0 else 0,
            'samples': len(errors)
        })
        traj_names.append(traj_name)

        # Visualize each trajectory
        visualize_single_trajectory(csv_path, output_dir)

    # Summary plot
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    names = [e['name'][:20] for e in all_errors]
    means = [e['mean'] for e in all_errors]
    maxes = [e['max'] for e in all_errors]

    x = np.arange(len(names))
    width = 0.35

    axes[0].bar(x - width/2, means, width, label='Mean Error', color='blue', alpha=0.7)
    axes[0].bar(x + width/2, maxes, width, label='Max Error', color='red', alpha=0.7)
    axes[0].set_xlabel('Trajectory')
    axes[0].set_ylabel('Error (m)')
    axes[0].set_title('Tracking Error Summary')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(names, rotation=45, ha='right')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3, axis='y')

    # Error distribution
    all_mean_errors = [e['mean'] for e in all_errors]
    axes[1].hist(all_mean_errors, bins=20, color='green', alpha=0.7, edgecolor='black')
    axes[1].axvline(x=np.mean(all_mean_errors), color='r', linestyle='--',
                    label=f'Overall Mean: {np.mean(all_mean_errors):.4f}m')
    axes[1].set_xlabel('Mean Tracking Error (m)')
    axes[1].set_ylabel('Count')
    axes[1].set_title('Error Distribution')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()

    if output_dir:
        summary_path = os.path.join(output_dir, 'summary.png')
        plt.savefig(summary_path, dpi=150, bbox_inches='tight')
        print(f"Saved summary: {summary_path}")

    # Print statistics
    print("\n" + "="*60)
    print("TRAJECTORY TRACKING SUMMARY")
    print("="*60)
    for e in all_errors:
        print(f"{e['name'][:30]:30s} | Mean: {e['mean']:.4f}m | Max: {e['max']:.4f}m | Samples: {e['samples']}")
    print("-"*60)
    print(f"{'Overall':30s} | Mean: {np.mean(all_mean_errors):.4f}m")
    print("="*60)

    return all_errors

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Visualize collected trajectory data')
    parser.add_argument('--input', '-i', type=str, required=True,
                        help='Input directory containing trajectory folders or single data.csv')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output directory for plots')
    parser.add_argument('--max', '-m', type=int, default=10,
                        help='Maximum number of trajectories to visualize')
    parser.add_argument('--show', action='store_true',
                        help='Show plots interactively')

    args = parser.parse_args()

    if os.path.isfile(args.input):
        # Single file
        visualize_single_trajectory(args.input, args.output)
    else:
        # Directory
        visualize_multiple_trajectories(args.input, args.output, args.max)

    if args.show:
        plt.show()
