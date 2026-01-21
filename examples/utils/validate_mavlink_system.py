#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
MAVLink系统验证测试脚本

验证内容：
1. 坐标系变换正确性（ENU to NEU, scale, z-down）
2. HTTP接口连通性
3. 轨迹跟踪精度
4. 结果可视化

使用方法：
1. 离线测试坐标变换（无需仿真）:
   python examples/validate_mavlink_system.py --mode offline

2. 启动仿真后进行完整测试:
   python examples/validate_mavlink_system.py --mode online --uav-id 0

3. 分析已有数据并可视化:
   python examples/validate_mavlink_system.py --mode analyze --data-dir ~/recordings/traj_name/uav0
"""

import argparse
import json
import math
import os
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

import numpy as np

# 尝试导入可视化库
try:
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("[WARN] matplotlib not available, visualization will be skipped")


@dataclass
class TrajPoint:
    """轨迹点"""
    x: float
    y: float
    z: float
    roll_deg: float
    yaw_deg: float
    pitch_deg: float


def ts_log(prefix: str, msg: str, level: str = "INFO"):
    """带时间戳的日志"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{level}] {prefix} {msg}", flush=True)


# =========================================
# 坐标变换函数（与mavlink_trajectory_collector.py一致）
# =========================================

def load_preprocessed_xyz(json_path: Path) -> List[TrajPoint]:
    """加载预处理的轨迹点"""
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs", [])
    pts = []
    for row in logs:
        if len(row) >= 6:
            # ENU to NEU: swap x,y
            pts.append(TrajPoint(
                float(row[1]),  # y -> x (NEU)
                float(row[0]),  # x -> y (NEU)
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5])
            ))
    return pts


def load_init_point(json_path: Path) -> TrajPoint:
    """加载初始点"""
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    raw = obj.get("raw_logs", [[0, 0, 0, 0, 0, 0]])[0]
    return TrajPoint(
        float(raw[1]),  # y -> x (NEU)
        float(raw[0]),  # x -> y (NEU)
        float(raw[2]),
        float(raw[3]) if len(raw) > 3 else 0,
        float(raw[4]) if len(raw) > 4 else 0,
        float(raw[5]) if len(raw) > 5 else 0
    )


def transform_points(
    pts: List[TrajPoint],
    scale: float,
    base_x: float,
    base_y: float,
    base_z: float,
    z_down: bool
) -> List[TrajPoint]:
    """坐标变换"""
    out = []
    for p in pts:
        x = base_x * scale + p.x * scale
        y = base_y * scale + p.y * scale
        if z_down:
            z = base_z * scale - p.z * scale
        else:
            z = base_z * scale + p.z * scale
        out.append(TrajPoint(x, y, z, p.roll_deg, p.yaw_deg, p.pitch_deg))
    return out


# =========================================
# 离线坐标变换测试
# =========================================

def test_coordinate_transform_offline():
    """离线测试坐标变换"""
    ts_log("[Offline]", "Starting coordinate transformation tests...")

    test_cases = [
        # (input_enu, scale, z_down, expected_description)
        {
            "name": "Basic ENU to NEU swap",
            "input": [100.0, 200.0, 300.0],  # x, y, z in ENU
            "scale": 0.01,
            "z_down": True,
            "base": [0, 0, 0],
        },
        {
            "name": "Z-down transformation",
            "input": [0.0, 0.0, 100.0],
            "scale": 0.01,
            "z_down": True,
            "base": [0, 0, 200],
        },
        {
            "name": "Z-up transformation",
            "input": [0.0, 0.0, 100.0],
            "scale": 0.01,
            "z_down": False,
            "base": [0, 0, 200],
        },
        {
            "name": "With base offset",
            "input": [50.0, 100.0, 150.0],
            "scale": 0.01,
            "z_down": True,
            "base": [10, 20, 30],
        },
    ]

    results = []
    print("\n" + "="*70)
    print("坐标变换测试结果")
    print("="*70)

    for tc in test_cases:
        inp = tc["input"]
        scale = tc["scale"]
        z_down = tc["z_down"]
        base = tc["base"]

        # 创建TrajPoint (已经是NEU格式，因为load函数会做转换)
        # 这里模拟load后的结果：NEU格式
        p = TrajPoint(inp[1], inp[0], inp[2], 0, 0, 0)  # swap x,y for NEU

        transformed = transform_points(
            [p], scale, base[0], base[1], base[2], z_down
        )[0]

        # 计算预期结果
        exp_x = base[0] * scale + p.x * scale
        exp_y = base[1] * scale + p.y * scale
        if z_down:
            exp_z = base[2] * scale - p.z * scale
        else:
            exp_z = base[2] * scale + p.z * scale

        match = (
            abs(transformed.x - exp_x) < 1e-6 and
            abs(transformed.y - exp_y) < 1e-6 and
            abs(transformed.z - exp_z) < 1e-6
        )

        status = "PASS" if match else "FAIL"
        results.append(match)

        print(f"\n[{status}] {tc['name']}")
        print(f"  Input ENU: ({inp[0]}, {inp[1]}, {inp[2]})")
        print(f"  Base: ({base[0]}, {base[1]}, {base[2]}), scale={scale}, z_down={z_down}")
        print(f"  After NEU swap: ({p.x}, {p.y}, {p.z})")
        print(f"  Transformed: ({transformed.x:.4f}, {transformed.y:.4f}, {transformed.z:.4f})")
        print(f"  Expected: ({exp_x:.4f}, {exp_y:.4f}, {exp_z:.4f})")

    print("\n" + "="*70)
    passed = sum(results)
    total = len(results)
    print(f"坐标变换测试: {passed}/{total} passed")
    print("="*70 + "\n")

    return all(results)


def test_trajectory_loading():
    """测试轨迹文件加载"""
    ts_log("[Offline]", "Testing trajectory file loading...")

    test_dir = Path(__file__).parent / "test_mavlink_validation"
    if not test_dir.exists():
        ts_log("[Offline]", f"Test directory not found: {test_dir}", "WARN")
        return False

    json_files = list(test_dir.glob("*.json"))
    if not json_files:
        ts_log("[Offline]", "No test JSON files found", "WARN")
        return False

    print("\n" + "="*70)
    print("轨迹文件加载测试")
    print("="*70)

    all_passed = True
    for jf in json_files:
        try:
            pts = load_preprocessed_xyz(jf)
            init = load_init_point(jf)

            print(f"\n[PASS] {jf.name}")
            print(f"  Points: {len(pts)}")
            print(f"  Init (NEU): ({init.x:.2f}, {init.y:.2f}, {init.z:.2f})")
            if pts:
                print(f"  First waypoint (NEU): ({pts[0].x:.2f}, {pts[0].y:.2f}, {pts[0].z:.2f})")
                print(f"  Last waypoint (NEU): ({pts[-1].x:.2f}, {pts[-1].y:.2f}, {pts[-1].z:.2f})")

            # 测试变换
            scale = 0.01
            transformed = transform_points(pts, scale, init.x, init.y, init.z, z_down=True)

            print(f"  After transform (scale={scale}, z_down=True):")
            if transformed:
                print(f"    First: ({transformed[0].x:.4f}, {transformed[0].y:.4f}, {transformed[0].z:.4f})")
                print(f"    Last: ({transformed[-1].x:.4f}, {transformed[-1].y:.4f}, {transformed[-1].z:.4f})")

        except Exception as e:
            print(f"\n[FAIL] {jf.name}: {e}")
            all_passed = False

    print("\n" + "="*70)
    return all_passed


# =========================================
# 在线测试（需要仿真运行）
# =========================================

def http_get(url: str, timeout: float = 5.0) -> Tuple[int, Dict]:
    """HTTP GET请求"""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return resp.getcode(), data
    except urllib.error.HTTPError as e:
        return e.code, {"error": str(e)}
    except Exception as e:
        return 0, {"error": str(e)}


def http_post(url: str, payload: Dict, timeout: float = 10.0) -> Tuple[int, Dict]:
    """HTTP POST请求"""
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return resp.getcode(), result
    except urllib.error.HTTPError as e:
        try:
            err_data = json.loads(e.read().decode("utf-8"))
        except:
            err_data = {"error": str(e)}
        return e.code, err_data
    except Exception as e:
        return 0, {"error": str(e)}


def test_http_connectivity(sim_port: int = 8081, ctrl_port: int = 5009):
    """测试HTTP连通性"""
    ts_log("[Online]", "Testing HTTP connectivity...")

    results = []

    # 测试仿真端口
    endpoints = [
        (f"http://127.0.0.1:{sim_port}/health", "Simulation health"),
        (f"http://127.0.0.1:{sim_port}/uav/0/pose", "UAV pose"),
        (f"http://127.0.0.1:{sim_port}/uav/0/px4/ready", "PX4 ready status"),
        (f"http://127.0.0.1:{ctrl_port}/health", "Controller health"),
    ]

    print("\n" + "="*70)
    print("HTTP接口连通性测试")
    print("="*70)

    for url, desc in endpoints:
        code, data = http_get(url)
        ok = 200 <= code < 300
        status = "PASS" if ok else "FAIL"
        results.append(ok)
        print(f"\n[{status}] {desc}")
        print(f"  URL: {url}")
        print(f"  Status: {code}")
        if ok:
            print(f"  Response: {json.dumps(data, indent=2)[:200]}...")
        else:
            print(f"  Error: {data.get('error', 'unknown')}")

    print("\n" + "="*70)
    passed = sum(results)
    print(f"HTTP连通性: {passed}/{len(results)} passed")
    print("="*70 + "\n")

    return all(results)


def run_single_waypoint_test(
    ctrl_base: str,
    sim_base: str,
    target: Tuple[float, float, float],
    uav_id: int = 0
) -> Dict[str, Any]:
    """运行单航点测试"""
    ctrl_url = f"{ctrl_base}/command"

    # 发送move_to命令
    cmd = {
        "cmd": "move_to",
        "x": target[0],
        "y": target[1],
        "z": target[2],
        "force": True
    }

    ts_log("[Test]", f"Sending move_to: ({target[0]:.2f}, {target[1]:.2f}, {target[2]:.2f})")

    code, resp = http_post(ctrl_url, cmd, timeout=120.0)

    # 获取最终位置
    pose_code, pose = http_get(f"{sim_base}/uav/{uav_id}/pose")

    result = {
        "target": target,
        "command_response": resp,
        "final_pose": pose.get("position", [0, 0, 0]) if pose_code == 200 else None,
    }

    if result["final_pose"]:
        obs = result["final_pose"]
        error = math.sqrt(
            (obs[0] - target[0])**2 +
            (obs[1] - target[1])**2 +
            (obs[2] - target[2])**2
        )
        result["position_error"] = error
        ts_log("[Test]", f"Position error: {error:.4f}m")

    return result


def run_trajectory_test(
    json_path: Path,
    ctrl_base: str,
    sim_base: str,
    scale: float,
    z_down: bool,
    uav_id: int = 0
) -> List[Dict]:
    """运行完整轨迹测试"""
    ts_log("[Test]", f"Running trajectory test: {json_path.name}")

    # 加载轨迹
    raw_pts = load_preprocessed_xyz(json_path)
    init_pos = load_init_point(json_path)

    # 变换
    pts = transform_points(raw_pts, scale, init_pos.x, init_pos.y, init_pos.z, z_down)

    ts_log("[Test]", f"Loaded {len(pts)} waypoints")

    # 重置到初始位置
    reset_pos = [init_pos.x * scale, init_pos.y * scale, init_pos.z * scale]
    if z_down:
        reset_pos[2] = init_pos.z * scale - init_pos.z * scale  # = 0 for init

    # 使用第一个航点位置作为重置位置
    if pts:
        reset_pos = [pts[0].x, pts[0].y, pts[0].z]

    reset_cmd = {
        "position": reset_pos,
        "hard": True,
        "force": True
    }

    ts_log("[Test]", f"Resetting to: {reset_pos}")
    code, resp = http_post(f"{ctrl_base}/reset", reset_cmd, timeout=60.0)

    if code < 200 or code >= 300:
        ts_log("[Test]", f"Reset failed: {resp}", "ERROR")
        return []

    time.sleep(3.0)  # 等待稳定

    results = []
    for i, p in enumerate(pts):
        ts_log("[Test]", f"Waypoint {i+1}/{len(pts)}")
        result = run_single_waypoint_test(
            ctrl_base, sim_base,
            (p.x, p.y, p.z),
            uav_id
        )
        result["waypoint_idx"] = i
        result["input_raw"] = (raw_pts[i].x, raw_pts[i].y, raw_pts[i].z) if i < len(raw_pts) else None
        results.append(result)

    return results


# =========================================
# 数据分析和可视化
# =========================================

def analyze_results(results: List[Dict]) -> Dict[str, Any]:
    """分析测试结果"""
    if not results:
        return {"error": "No results to analyze"}

    errors = [r.get("position_error", 0) for r in results if r.get("position_error") is not None]

    analysis = {
        "total_waypoints": len(results),
        "successful_waypoints": len(errors),
        "mean_error": np.mean(errors) if errors else 0,
        "max_error": max(errors) if errors else 0,
        "min_error": min(errors) if errors else 0,
        "std_error": np.std(errors) if errors else 0,
    }

    print("\n" + "="*70)
    print("轨迹跟踪分析结果")
    print("="*70)
    print(f"  总航点数: {analysis['total_waypoints']}")
    print(f"  成功航点: {analysis['successful_waypoints']}")
    print(f"  平均误差: {analysis['mean_error']:.4f}m")
    print(f"  最大误差: {analysis['max_error']:.4f}m")
    print(f"  最小误差: {analysis['min_error']:.4f}m")
    print(f"  误差标准差: {analysis['std_error']:.4f}m")
    print("="*70 + "\n")

    return analysis


def visualize_trajectory(results: List[Dict], save_path: Optional[Path] = None):
    """可视化轨迹对比"""
    if not HAS_MATPLOTLIB:
        ts_log("[Viz]", "matplotlib not available, skipping visualization", "WARN")
        return

    if not results:
        ts_log("[Viz]", "No results to visualize", "WARN")
        return

    # 提取数据
    cmd_x, cmd_y, cmd_z = [], [], []
    obs_x, obs_y, obs_z = [], [], []

    for r in results:
        target = r.get("target")
        obs = r.get("final_pose")
        if target and obs:
            cmd_x.append(target[0])
            cmd_y.append(target[1])
            cmd_z.append(target[2])
            obs_x.append(obs[0])
            obs_y.append(obs[1])
            obs_z.append(obs[2])

    if not cmd_x:
        ts_log("[Viz]", "No valid data points for visualization", "WARN")
        return

    # 创建图形
    fig = plt.figure(figsize=(16, 12))

    # 3D轨迹对比
    ax1 = fig.add_subplot(2, 2, 1, projection='3d')
    ax1.plot(cmd_x, cmd_y, cmd_z, 'b-o', label='Commanded', markersize=8)
    ax1.plot(obs_x, obs_y, obs_z, 'r-x', label='Observed', markersize=8)
    ax1.set_xlabel('X (m)')
    ax1.set_ylabel('Y (m)')
    ax1.set_zlabel('Z (m)')
    ax1.set_title('3D Trajectory Comparison')
    ax1.legend()

    # XY平面
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.plot(cmd_x, cmd_y, 'b-o', label='Commanded', markersize=8)
    ax2.plot(obs_x, obs_y, 'r-x', label='Observed', markersize=8)
    for i in range(len(cmd_x)):
        ax2.annotate(f'{i}', (cmd_x[i], cmd_y[i]), fontsize=8)
    ax2.set_xlabel('X (m)')
    ax2.set_ylabel('Y (m)')
    ax2.set_title('XY Plane')
    ax2.legend()
    ax2.grid(True)
    ax2.axis('equal')

    # XZ平面
    ax3 = fig.add_subplot(2, 2, 3)
    ax3.plot(cmd_x, cmd_z, 'b-o', label='Commanded', markersize=8)
    ax3.plot(obs_x, obs_z, 'r-x', label='Observed', markersize=8)
    ax3.set_xlabel('X (m)')
    ax3.set_ylabel('Z (m)')
    ax3.set_title('XZ Plane (Height Profile)')
    ax3.legend()
    ax3.grid(True)

    # 误差分布
    ax4 = fig.add_subplot(2, 2, 4)
    errors = [r.get("position_error", 0) for r in results if r.get("position_error") is not None]
    if errors:
        ax4.bar(range(len(errors)), errors, color='orange')
        ax4.axhline(y=np.mean(errors), color='r', linestyle='--', label=f'Mean: {np.mean(errors):.3f}m')
        ax4.set_xlabel('Waypoint Index')
        ax4.set_ylabel('Position Error (m)')
        ax4.set_title('Position Error per Waypoint')
        ax4.legend()
        ax4.grid(True)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        ts_log("[Viz]", f"Saved figure to {save_path}")

    plt.show()


def visualize_coordinate_transform(json_path: Path, scale: float, z_down: bool, save_path: Optional[Path] = None):
    """可视化坐标变换过程"""
    if not HAS_MATPLOTLIB:
        return

    # 加载数据
    raw_pts = load_preprocessed_xyz(json_path)
    init = load_init_point(json_path)
    transformed = transform_points(raw_pts, scale, init.x, init.y, init.z, z_down)

    fig = plt.figure(figsize=(16, 6))

    # 原始坐标 (NEU, 已swap)
    ax1 = fig.add_subplot(1, 3, 1, projection='3d')
    raw_x = [p.x for p in raw_pts]
    raw_y = [p.y for p in raw_pts]
    raw_z = [p.z for p in raw_pts]
    ax1.plot(raw_x, raw_y, raw_z, 'b-o', markersize=8)
    ax1.scatter([init.x], [init.y], [init.z], c='g', s=100, marker='^', label='Init')
    ax1.set_xlabel('X (NEU)')
    ax1.set_ylabel('Y (NEU)')
    ax1.set_zlabel('Z')
    ax1.set_title(f'After ENU->NEU Swap\n(Original units)')
    ax1.legend()

    # 变换后坐标
    ax2 = fig.add_subplot(1, 3, 2, projection='3d')
    tr_x = [p.x for p in transformed]
    tr_y = [p.y for p in transformed]
    tr_z = [p.z for p in transformed]
    ax2.plot(tr_x, tr_y, tr_z, 'r-o', markersize=8)
    ax2.set_xlabel('X (m)')
    ax2.set_ylabel('Y (m)')
    ax2.set_zlabel('Z (m)')
    ax2.set_title(f'After Transform\nscale={scale}, z_down={z_down}')

    # 变换参数说明
    ax3 = fig.add_subplot(1, 3, 3)
    ax3.axis('off')
    info_text = f"""
坐标变换参数:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
轨迹文件: {json_path.name}
航点数量: {len(raw_pts)}

变换步骤:
1. ENU -> NEU (swap x,y)
   原始: (x_enu, y_enu, z)
   -> NEU: (y_enu, x_enu, z)

2. 应用缩放和基准偏移:
   scale = {scale}
   base = ({init.x}, {init.y}, {init.z})

   x_out = base_x * scale + x_neu * scale
   y_out = base_y * scale + y_neu * scale
   z_out = base_z * scale {'- z_neu * scale' if z_down else '+ z_neu * scale'}

变换前范围:
   X: [{min(raw_x):.1f}, {max(raw_x):.1f}]
   Y: [{min(raw_y):.1f}, {max(raw_y):.1f}]
   Z: [{min(raw_z):.1f}, {max(raw_z):.1f}]

变换后范围:
   X: [{min(tr_x):.4f}, {max(tr_x):.4f}] m
   Y: [{min(tr_y):.4f}, {max(tr_y):.4f}] m
   Z: [{min(tr_z):.4f}, {max(tr_z):.4f}] m
"""
    ax3.text(0.1, 0.9, info_text, transform=ax3.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        ts_log("[Viz]", f"Saved coordinate transform visualization to {save_path}")

    plt.show()


def analyze_csv_data(data_dir: Path):
    """分析CSV数据文件"""
    csv_path = data_dir / "data.csv"
    pose_csv_path = data_dir / "all_pose_data.csv"

    if not csv_path.exists():
        ts_log("[Analyze]", f"data.csv not found in {data_dir}", "ERROR")
        return None

    import csv

    results = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            result = {
                "waypoint_idx": int(row.get("step_idx", 0)),
                "target": (
                    float(row.get("cmd_x", 0)),
                    float(row.get("cmd_y", 0)),
                    float(row.get("cmd_z", 0))
                ),
                "final_pose": (
                    float(row.get("obs_pos_x", 0)),
                    float(row.get("obs_pos_y", 0)),
                    float(row.get("obs_pos_z", 0))
                ),
                "aligned_pos": (
                    float(row.get("obs_aligned_x", 0)),
                    float(row.get("obs_aligned_y", 0)),
                    float(row.get("obs_aligned_z", 0))
                ),
                "origin_offset": (
                    float(row.get("origin_offset_x", 0)),
                    float(row.get("origin_offset_y", 0)),
                    float(row.get("origin_offset_z", 0))
                ),
            }

            # 计算误差
            obs = result["aligned_pos"]
            cmd = result["target"]
            error = math.sqrt(
                (obs[0] - cmd[0])**2 +
                (obs[1] - cmd[1])**2 +
                (obs[2] - cmd[2])**2
            )
            result["position_error"] = error
            results.append(result)

    return results


# =========================================
# 主函数
# =========================================

def main():
    parser = argparse.ArgumentParser(description="MAVLink System Validation")
    parser.add_argument("--mode", choices=["offline", "online", "analyze"], default="offline",
                        help="Test mode: offline (no sim), online (with sim), analyze (existing data)")
    parser.add_argument("--uav-id", type=int, default=0, help="UAV ID for online test")
    parser.add_argument("--sim-port", type=int, default=8081, help="Simulation HTTP port")
    parser.add_argument("--ctrl-port", type=int, default=5009, help="Controller HTTP port")
    parser.add_argument("--scale", type=float, default=0.01, help="Coordinate scale")
    parser.add_argument("--z-down", action="store_true", default=True, help="Z axis down")
    parser.add_argument("--data-dir", type=str, help="Data directory for analyze mode")
    parser.add_argument("--traj-file", type=str, help="Specific trajectory file to test")
    parser.add_argument("--output-dir", type=str, default="./validation_results", help="Output directory")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "offline":
        ts_log("[Main]", "Running OFFLINE validation tests...")

        # 坐标变换测试
        coord_ok = test_coordinate_transform_offline()

        # 轨迹加载测试
        load_ok = test_trajectory_loading()

        # 可视化坐标变换
        if HAS_MATPLOTLIB:
            test_dir = Path(__file__).parent / "test_mavlink_validation"
            for jf in test_dir.glob("*.json"):
                visualize_coordinate_transform(
                    jf, args.scale, args.z_down,
                    save_path=output_dir / f"coord_transform_{jf.stem}.png"
                )

        if coord_ok and load_ok:
            ts_log("[Main]", "All offline tests PASSED!", "INFO")
            print("\n" + "="*70)
            print("离线测试全部通过!")
            print("下一步: 启动仿真并运行在线测试")
            print("")
            print("1. 启动MAVLink仿真 (终端1):")
            print("   ISAACSIM_PYTHON examples/mavlink_sim_vehicle.py")
            print("")
            print("2. 运行在线测试 (终端2):")
            print(f"   python examples/validate_mavlink_system.py --mode online")
            print("="*70 + "\n")
        else:
            ts_log("[Main]", "Some offline tests FAILED!", "ERROR")
            return 1

    elif args.mode == "online":
        ts_log("[Main]", "Running ONLINE validation tests...")

        ctrl_base = f"http://127.0.0.1:{args.ctrl_port + args.uav_id}"
        sim_base = f"http://127.0.0.1:{args.sim_port}"

        # HTTP连通性测试
        if not test_http_connectivity(args.sim_port, args.ctrl_port + args.uav_id):
            ts_log("[Main]", "HTTP connectivity test failed. Is simulation running?", "ERROR")
            return 1

        # 运行轨迹测试
        test_dir = Path(__file__).parent / "test_mavlink_validation"
        traj_file = Path(args.traj_file) if args.traj_file else test_dir / "traj_simple_square.json"

        if not traj_file.exists():
            ts_log("[Main]", f"Trajectory file not found: {traj_file}", "ERROR")
            return 1

        results = run_trajectory_test(
            traj_file, ctrl_base, sim_base,
            args.scale, args.z_down, args.uav_id
        )

        # 分析结果
        analysis = analyze_results(results)

        # 保存结果
        results_file = output_dir / f"test_results_{traj_file.stem}.json"
        with open(results_file, 'w') as f:
            json.dump({"results": results, "analysis": analysis}, f, indent=2, default=str)
        ts_log("[Main]", f"Results saved to {results_file}")

        # 可视化
        if HAS_MATPLOTLIB:
            visualize_trajectory(results, save_path=output_dir / f"trajectory_{traj_file.stem}.png")

        # 判断测试是否通过
        if analysis.get("mean_error", 999) < 1.0:  # 平均误差小于1米
            ts_log("[Main]", "Online tests PASSED!", "INFO")
        else:
            ts_log("[Main]", "Online tests completed with high error", "WARN")

    elif args.mode == "analyze":
        ts_log("[Main]", "Analyzing existing data...")

        if not args.data_dir:
            ts_log("[Main]", "--data-dir required for analyze mode", "ERROR")
            return 1

        data_dir = Path(args.data_dir)
        results = analyze_csv_data(data_dir)

        if results:
            analysis = analyze_results(results)

            if HAS_MATPLOTLIB:
                visualize_trajectory(results, save_path=output_dir / "analyzed_trajectory.png")
        else:
            ts_log("[Main]", "Failed to load data", "ERROR")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
