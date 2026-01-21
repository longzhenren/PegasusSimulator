#!/usr/bin/env python3
"""
Trajectory Data Visualization Script
Compare input JSON trajectories with recorded CSV data
"""
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys

def load_input_json(json_path: Path):
    """Load raw_logs from input JSON"""
    with open(json_path) as f:
        data = json.load(f)
    raw_logs = data.get("raw_logs", [])
    # raw_logs format: [x, y, z, roll, pitch, yaw]
    points = []
    for i, r in enumerate(raw_logs):
        points.append({
            "step": i,
            "x": r[0],
            "y": r[1],
            "z": r[2],
            "roll": r[3],
            "pitch": r[4],
            "yaw": r[5]
        })
    return pd.DataFrame(points)

def analyze_trajectory(traj_name: str, recordings_dir: str, input_dir: str, scale: float = 0.01):
    """Analyze a single trajectory: compare JSON input vs recorded CSV"""
    recordings_base = Path(recordings_dir)
    input_base = Path(input_dir)
    
    # Find the trajectory folder
    traj_folder = recordings_base / traj_name
    if not traj_folder.exists():
        print(f"ERROR: Trajectory folder not found: {traj_folder}")
        return None
    
    # Find CSV file
    csv_files = list(traj_folder.glob("*/data.csv"))
    if not csv_files:
        print(f"ERROR: No data.csv found in {traj_folder}")
        return None
    
    csv_path = csv_files[0]
    uav_id = csv_path.parent.name
    
    # Load recorded CSV
    recorded = pd.read_csv(csv_path)
    print(f"Loaded recorded data: {len(recorded)} rows from {csv_path}")
    
    # Load input JSON
    json_name = traj_name + ".json"
    json_path = input_base / json_name
    if not json_path.exists():
        print(f"ERROR: Input JSON not found: {json_path}")
        return None
    
    input_df = load_input_json(json_path)
    print(f"Loaded input trajectory: {len(input_df)} points from {json_path}")
    
    # Apply scale to input (same as collector)
    input_df["x_scaled"] = input_df["x"] * scale
    input_df["y_scaled"] = input_df["y"] * scale
    input_df["z_scaled"] = input_df["z"] * scale
    
    return {
        "traj_name": traj_name,
        "uav_id": uav_id,
        "recorded": recorded,
        "input": input_df,
        "scale": scale
    }

def plot_trajectory_comparison(data: dict, output_path: str):
    """Create visualization comparing input trajectory vs recorded data"""
    recorded = data["recorded"]
    input_df = data["input"]
    
    fig = plt.figure(figsize=(16, 12))
    
    # Get input start position (for offset calculation reference)
    input_start_x = input_df["x_scaled"].iloc[0]
    input_start_y = input_df["y_scaled"].iloc[0]
    input_start_z = input_df["z_scaled"].iloc[0]
    
    # Get recorded start position
    rec_start_x = recorded["obs_pos_x"].iloc[0]
    rec_start_y = recorded["obs_pos_y"].iloc[0]
    rec_start_z = recorded["obs_pos_z"].iloc[0]
    
    print(f"Input trajectory start: ({input_start_x:.2f}, {input_start_y:.2f}, {input_start_z:.2f})")
    print(f"Recorded data start: ({rec_start_x:.2f}, {rec_start_y:.2f}, {rec_start_z:.2f})")
    
    # Calculate offset (how recorded data relates to input)
    offset_x = rec_start_x - input_start_x
    offset_y = rec_start_y - input_start_y
    offset_z = rec_start_z - input_start_z
    print(f"Position offset: ({offset_x:.2f}, {offset_y:.2f}, {offset_z:.2f})")
    
    # Shift input to match recorded frame for comparison
    input_shifted_x = input_df["x_scaled"] + offset_x
    input_shifted_y = input_df["y_scaled"] + offset_y
    input_shifted_z = input_df["z_scaled"] + offset_z
    
    # Convert to numpy for plotting (matplotlib compatibility)
    rec_x = recorded["obs_pos_x"].values
    rec_y = recorded["obs_pos_y"].values
    rec_z = recorded["obs_pos_z"].values
    
    # 1. 3D Trajectory Plot
    ax1 = fig.add_subplot(2, 2, 1, projection='3d')
    ax1.plot(rec_x, rec_y, rec_z, 
             'b-', linewidth=2, label='Recorded', alpha=0.8)
    ax1.plot(input_shifted_x.values, input_shifted_y.values, input_shifted_z.values, 
             'r--', linewidth=2, label='Input (shifted)', alpha=0.6)
    ax1.set_xlabel('X (m)')
    ax1.set_ylabel('Y (m)')
    ax1.set_zlabel('Z (m)')
    ax1.set_title(f'3D Trajectory: {data["traj_name"]}')
    ax1.legend()
    
    # 2. XY Top-down View
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.plot(rec_x, rec_y, 'b-', linewidth=2, label='Recorded')
    ax2.plot(input_shifted_x.values, input_shifted_y.values, 'r--', linewidth=2, label='Input (shifted)')
    ax2.scatter([rec_start_x], [rec_start_y], c='g', s=100, marker='o', label='Start', zorder=5)
    ax2.scatter([rec_x[-1]], [rec_y[-1]], 
                c='r', s=100, marker='x', label='End', zorder=5)
    ax2.set_xlabel('X (m)')
    ax2.set_ylabel('Y (m)')
    ax2.set_title('XY Top-down View')
    ax2.legend()
    ax2.set_aspect('equal')
    ax2.grid(True, alpha=0.3)
    
    # 3. Position vs Time (normalized)
    ax3 = fig.add_subplot(2, 2, 3)
    rec_time = np.linspace(0, 1, len(recorded))
    input_time = np.linspace(0, 1, len(input_df))
    
    ax3.plot(rec_time, rec_x, 'b-', label='Rec X', alpha=0.8)
    ax3.plot(rec_time, rec_y, 'g-', label='Rec Y', alpha=0.8)
    ax3.plot(rec_time, rec_z, 'r-', label='Rec Z', alpha=0.8)
    ax3.plot(input_time, input_shifted_x.values, 'b--', label='Input X', alpha=0.5)
    ax3.plot(input_time, input_shifted_y.values, 'g--', label='Input Y', alpha=0.5)
    ax3.plot(input_time, input_shifted_z.values, 'r--', label='Input Z', alpha=0.5)
    ax3.set_xlabel('Normalized Time')
    ax3.set_ylabel('Position (m)')
    ax3.set_title('Position vs Normalized Time')
    ax3.legend(ncol=2)
    ax3.grid(True, alpha=0.3)
    
    # 4. Altitude comparison
    ax4 = fig.add_subplot(2, 2, 4)
    ax4.plot(rec_time, rec_z, 'b-', linewidth=2, label='Recorded Z')
    ax4.plot(input_time, input_shifted_z.values, 'r--', linewidth=2, label='Input Z (shifted)')
    ax4.plot(input_time, input_df["z_scaled"].values, 'g:', linewidth=2, label='Input Z (original)')
    ax4.set_xlabel('Normalized Time')
    ax4.set_ylabel('Altitude Z (m)')
    ax4.set_title('Altitude Comparison')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Saved plot to: {output_path}")
    plt.close()
    
    # Calculate trajectory shape similarity (relative motion)
    rec_dx = np.diff(recorded["obs_pos_x"].values)
    rec_dy = np.diff(recorded["obs_pos_y"].values)
    input_dx = np.diff(input_df["x_scaled"].values)
    input_dy = np.diff(input_df["y_scaled"].values)
    
    # Normalize to same length for comparison
    if len(rec_dx) > len(input_dx):
        indices = np.linspace(0, len(rec_dx)-1, len(input_dx)).astype(int)
        rec_dx_resampled = rec_dx[indices]
        rec_dy_resampled = rec_dy[indices]
        inp_dx_resampled = input_dx
        inp_dy_resampled = input_dy
    else:
        indices = np.linspace(0, len(input_dx)-1, len(rec_dx)).astype(int)
        rec_dx_resampled = rec_dx
        rec_dy_resampled = rec_dy
        inp_dx_resampled = input_dx[indices]
        inp_dy_resampled = input_dy[indices]
    
    # Correlation of displacement vectors
    corr_dx = np.corrcoef(rec_dx_resampled, inp_dx_resampled)[0, 1]
    corr_dy = np.corrcoef(rec_dy_resampled, inp_dy_resampled)[0, 1]
    
    return {
        "traj_name": data["traj_name"],
        "recorded_points": len(recorded),
        "input_points": len(input_df),
        "offset": [offset_x, offset_y, offset_z],
        "input_start": [input_start_x, input_start_y, input_start_z],
        "recorded_start": [rec_start_x, rec_start_y, rec_start_z],
        "correlation_dx": corr_dx,
        "correlation_dy": corr_dy,
    }

def main():
    recordings_dir = "/home/user/uav-data/trajectory_recordings_new"
    input_dir = "/home/user/uav-data/drone/uav-flow-sim/train_data/extracted_json_files"
    output_dir = "/home/user/.gemini/antigravity/brain/f62f3bd7-6b6a-4616-bf7c-dd2bd739bc77"
    
    # Find available trajectories
    recordings_base = Path(recordings_dir)
    traj_folders = [f for f in recordings_base.iterdir() if f.is_dir()]
    
    print(f"Found {len(traj_folders)} recorded trajectories")
    
    # Analyze first 3 trajectories
    results = []
    for i, traj_folder in enumerate(sorted(traj_folders)[:3]):
        traj_name = traj_folder.name
        print(f"\n{'='*60}")
        print(f"Analyzing trajectory {i+1}: {traj_name}")
        print('='*60)
        
        data = analyze_trajectory(traj_name, recordings_dir, input_dir)
        if data:
            output_path = f"{output_dir}/traj_comparison_{i+1}.png"
            result = plot_trajectory_comparison(data, output_path)
            results.append(result)
    
    # Summary
    print(f"\n{'='*60}")
    print("ANALYSIS SUMMARY")
    print('='*60)
    for r in results:
        print(f"\nTrajectory: {r['traj_name']}")
        print(f"  Input points: {r['input_points']}, Recorded points: {r['recorded_points']}")
        print(f"  Input start: ({r['input_start'][0]:.2f}, {r['input_start'][1]:.2f}, {r['input_start'][2]:.2f})")
        print(f"  Recorded start: ({r['recorded_start'][0]:.2f}, {r['recorded_start'][1]:.2f}, {r['recorded_start'][2]:.2f})")
        print(f"  Position offset: ({r['offset'][0]:.2f}, {r['offset'][1]:.2f}, {r['offset'][2]:.2f})")
        print(f"  Displacement correlation: dX={r['correlation_dx']:.3f}, dY={r['correlation_dy']:.3f}")

if __name__ == "__main__":
    main()
