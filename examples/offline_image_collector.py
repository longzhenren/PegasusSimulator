#!/usr/bin/env python
"""
离线图像采集器（offline_image_collector.py）

==========================
概述
==========================
本脚本是独立的图像采集工具，特点：
- 加载复杂USD场景
- 只启动Isaac Sim，不启动仿真，不推进时间线
- 根据CSV中的位姿数据设置相机位置
- 并行采集RGB和深度图像
- 摄像头位置根据mavlink数据收集器输出的CSV中的观测位置和姿态设定

==========================
使用方法
==========================
# 基本用法
ISAACSIM_PYTHON examples/offline_image_collector.py --csv <csv_path> --usd <usd_path>

# 指定输出目录
ISAACSIM_PYTHON examples/offline_image_collector.py --csv <csv_path> --usd <usd_path> --output <output_dir>

# 指定相机参数
ISAACSIM_PYTHON examples/offline_image_collector.py --csv <csv_path> --usd <usd_path> --resolution 640 480 --fov 90

==========================
命令行参数
==========================
--csv PATH          CSV文件路径（包含位姿数据）
--usd PATH          USD场景文件路径
--output PATH       输出目录（默认：与CSV同目录下的images/）
--resolution W H    图像分辨率（默认：640 480）
--fov DEGREES       相机视场角（默认：90）
--depth             启用深度图像采集
--parallel N        并行采集线程数（默认：4）
--camera-offset X Y Z  相机相对于机身的偏移（默认：0.3 0 0）
--camera-rotation R P Y  相机相对于机身的旋转（度，默认：0 0 180）

==========================
CSV输入格式
==========================
必需字段：
- obs_pos_x, obs_pos_y, obs_pos_z: 观测位置（米）
- obs_att_w, obs_att_x, obs_att_y, obs_att_z: 观测姿态（四元数）

可选字段：
- image_timestamp_s: 时间戳
- waypoint_idx: 航点索引

==========================
输出目录结构
==========================
<output_dir>/
  ├── images/           # RGB图像
  │   ├── 000000.png
  │   ├── 000001.png
  │   └── ...
  ├── depths/           # 深度图像（如果启用）
  │   ├── 000000.png
  │   └── ...
  └── metadata.csv      # 采集元数据
"""

import sys
import os
import argparse

# ==============================================================================
# 1. 解析参数 & 启动 SimulationApp
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Offline Image Collector")
    parser.add_argument("--csv", type=str, required=True, help="CSV file with pose data")
    parser.add_argument("--usd", type=str, required=True, help="USD scene file path")
    parser.add_argument("--output", type=str, default=None, help="Output directory")
    parser.add_argument("--resolution", type=int, nargs=2, default=[640, 480], help="Image resolution (W H)")
    parser.add_argument("--fov", type=float, default=90.0, help="Camera field of view (degrees)")
    parser.add_argument("--depth", action="store_true", help="Enable depth image capture")
    parser.add_argument("--parallel", type=int, default=4, help="Number of parallel capture threads")
    parser.add_argument("--camera-offset", type=float, nargs=3, default=[0.3, 0.0, 0.0],
                        help="Camera offset from body (X Y Z)")
    parser.add_argument("--camera-rotation", type=float, nargs=3, default=[0.0, 0.0, 180.0],
                        help="Camera rotation from body (Roll Pitch Yaw in degrees)")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    return parser.parse_args()

ARGS = parse_args()

# 配置 Isaac Sim 启动参数
APP_CONFIG = {
    "window_width": 1280,
    "window_height": 720,
    "headless": ARGS.headless,
    "max_bounces": 0,
    "samples_per_pixel_per_frame": 1,
    "anti_aliasing": 1,
    "renderer": "RayTracedLighting",
}

# 启动仿真引擎
from isaacsim import SimulationApp
simulation_app = SimulationApp(APP_CONFIG)

# ==============================================================================
# 2. 导入其他依赖库
# ==============================================================================
import csv
import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from PIL import Image
from scipy.spatial.transform import Rotation

import carb
import omni.usd
from pxr import Usd, UsdGeom, Gf

# Isaac Sim相机API
from isaacsim.sensors.camera.camera import Camera


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """生成带时间戳的日志消息"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg, flush=True)
    return log_msg


@dataclass
class PoseData:
    """位姿数据"""
    index: int
    position: np.ndarray  # [x, y, z]
    attitude: np.ndarray  # [w, x, y, z] 四元数
    timestamp: float = 0.0
    waypoint_idx: int = 0


def load_poses_from_csv(csv_path: str) -> List[PoseData]:
    """从CSV文件加载位姿数据

    Args:
        csv_path: CSV文件路径

    Returns:
        位姿数据列表
    """
    poses = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            try:
                # 读取位置
                pos_x = float(row.get('obs_pos_x', 0))
                pos_y = float(row.get('obs_pos_y', 0))
                pos_z = float(row.get('obs_pos_z', 0))

                # 读取姿态（四元数）
                att_w = float(row.get('obs_att_w', 1))
                att_x = float(row.get('obs_att_x', 0))
                att_y = float(row.get('obs_att_y', 0))
                att_z = float(row.get('obs_att_z', 0))

                # 读取可选字段
                timestamp = float(row.get('image_timestamp_s', 0))
                waypoint_idx = int(row.get('waypoint_idx', i))

                pose = PoseData(
                    index=i,
                    position=np.array([pos_x, pos_y, pos_z]),
                    attitude=np.array([att_w, att_x, att_y, att_z]),
                    timestamp=timestamp,
                    waypoint_idx=waypoint_idx
                )
                poses.append(pose)
            except (ValueError, KeyError) as e:
                ts_log("[CSV]", f"Skipping row {i}: {e}", "WARN")
                continue

    ts_log("[CSV]", f"Loaded {len(poses)} poses from {csv_path}")
    return poses


def compute_camera_pose(
    body_position: np.ndarray,
    body_attitude: np.ndarray,
    camera_offset: np.ndarray,
    camera_rotation: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """计算相机在世界坐标系中的位姿

    Args:
        body_position: 机身位置 [x, y, z]
        body_attitude: 机身姿态四元数 [w, x, y, z]
        camera_offset: 相机相对于机身的偏移 [x, y, z]
        camera_rotation: 相机相对于机身的旋转 [roll, pitch, yaw] (度)

    Returns:
        (camera_position, camera_quaternion): 相机世界位置和四元数
    """
    # 机身旋转矩阵
    body_rot = Rotation.from_quat([
        body_attitude[1], body_attitude[2], body_attitude[3], body_attitude[0]
    ])  # scipy使用 [x, y, z, w] 格式

    # 相机相对于机身的旋转
    cam_local_rot = Rotation.from_euler('xyz', camera_rotation, degrees=True)

    # 相机在世界坐标系中的位置
    camera_position = body_position + body_rot.apply(camera_offset)

    # 相机在世界坐标系中的旋转
    camera_rot = body_rot * cam_local_rot
    cam_quat_xyzw = camera_rot.as_quat()  # [x, y, z, w]
    camera_quaternion = np.array([
        cam_quat_xyzw[3], cam_quat_xyzw[0], cam_quat_xyzw[1], cam_quat_xyzw[2]
    ])  # 转换为 [w, x, y, z]

    return camera_position, camera_quaternion


class OfflineImageCollector:
    """离线图像采集器"""

    def __init__(
        self,
        usd_path: str,
        resolution: Tuple[int, int] = (640, 480),
        fov: float = 90.0,
        enable_depth: bool = False,
        camera_offset: np.ndarray = None,
        camera_rotation: np.ndarray = None
    ):
        self.usd_path = usd_path
        self.resolution = resolution
        self.fov = fov
        self.enable_depth = enable_depth
        self.camera_offset = camera_offset if camera_offset is not None else np.array([0.3, 0.0, 0.0])
        self.camera_rotation = camera_rotation if camera_rotation is not None else np.array([0.0, 0.0, 180.0])

        self._camera = None
        self._stage = None
        self._initialized = False

    def initialize(self):
        """初始化场景和相机"""
        ts_log("[Collector]", f"Loading USD scene: {self.usd_path}")

        # 加载USD场景
        omni.usd.get_context().open_stage(self.usd_path)
        self._stage = omni.usd.get_context().get_stage()

        if self._stage is None:
            raise RuntimeError(f"Failed to load USD scene: {self.usd_path}")

        ts_log("[Collector]", "USD scene loaded successfully")

        # 创建相机
        camera_prim_path = "/World/OfflineCamera"
        self._camera = Camera(
            prim_path=camera_prim_path,
            frequency=30,
            resolution=self.resolution
        )
        self._camera.initialize()

        # 设置相机属性
        self._camera.set_clipping_range(0.05, 50000.0)
        if self.enable_depth:
            self._camera.add_distance_to_image_plane_to_frame()

        self._initialized = True
        ts_log("[Collector]", f"Camera initialized: {camera_prim_path}")
        ts_log("[Collector]", f"Resolution: {self.resolution}, FOV: {self.fov}, Depth: {self.enable_depth}")

    def capture_at_pose(self, pose: PoseData) -> Dict[str, Any]:
        """在指定位姿处采集图像

        Args:
            pose: 位姿数据

        Returns:
            采集结果字典，包含rgb_image, depth_image(可选), metadata
        """
        if not self._initialized:
            raise RuntimeError("Collector not initialized")

        # 计算相机世界位姿
        cam_pos, cam_quat = compute_camera_pose(
            pose.position,
            pose.attitude,
            self.camera_offset,
            self.camera_rotation
        )

        # 设置相机位姿
        # 转换四元数格式：[w,x,y,z] -> [x,y,z,w] for scipy
        quat_xyzw = np.array([cam_quat[1], cam_quat[2], cam_quat[3], cam_quat[0]])
        self._camera.set_world_pose(cam_pos, quat_xyzw)

        # 渲染一帧以更新相机视图
        simulation_app.update()

        # 获取RGB图像
        rgb_image = self._camera.get_rgba()[:, :, :3]

        result = {
            "index": pose.index,
            "rgb_image": rgb_image,
            "depth_image": None,
            "camera_position": cam_pos.tolist(),
            "camera_quaternion": cam_quat.tolist(),
            "body_position": pose.position.tolist(),
            "body_attitude": pose.attitude.tolist(),
            "timestamp": pose.timestamp,
            "waypoint_idx": pose.waypoint_idx
        }

        # 获取深度图像（如果启用）
        if self.enable_depth:
            try:
                depth_image = self._camera.get_depth()
                result["depth_image"] = depth_image
            except Exception as e:
                ts_log("[Collector]", f"Failed to get depth: {e}", "WARN")

        return result

    def collect_all(
        self,
        poses: List[PoseData],
        output_dir: str,
        progress_callback: callable = None
    ) -> List[Dict[str, Any]]:
        """采集所有位姿的图像

        Args:
            poses: 位姿数据列表
            output_dir: 输出目录
            progress_callback: 进度回调函数

        Returns:
            采集结果元数据列表
        """
        output_path = Path(output_dir)
        images_dir = output_path / "images"
        depths_dir = output_path / "depths"

        images_dir.mkdir(parents=True, exist_ok=True)
        if self.enable_depth:
            depths_dir.mkdir(parents=True, exist_ok=True)

        metadata_list = []
        total = len(poses)

        for i, pose in enumerate(poses):
            try:
                result = self.capture_at_pose(pose)

                # 保存RGB图像
                rgb_path = images_dir / f"{pose.index:06d}.png"
                Image.fromarray(result["rgb_image"]).save(str(rgb_path))

                # 保存深度图像
                depth_path_str = ""
                if self.enable_depth and result["depth_image"] is not None:
                    depth_path = depths_dir / f"{pose.index:06d}.png"
                    # 深度图像保存为16位PNG
                    depth_img = result["depth_image"]
                    depth_scaled = (depth_img * 1000).astype(np.uint16)
                    Image.fromarray(depth_scaled).save(str(depth_path))
                    depth_path_str = str(depth_path.relative_to(output_path))

                # 记录元数据
                metadata = {
                    "index": pose.index,
                    "image_path": str(rgb_path.relative_to(output_path)),
                    "depth_path": depth_path_str,
                    "cam_pos_x": result["camera_position"][0],
                    "cam_pos_y": result["camera_position"][1],
                    "cam_pos_z": result["camera_position"][2],
                    "cam_quat_w": result["camera_quaternion"][0],
                    "cam_quat_x": result["camera_quaternion"][1],
                    "cam_quat_y": result["camera_quaternion"][2],
                    "cam_quat_z": result["camera_quaternion"][3],
                    "body_pos_x": result["body_position"][0],
                    "body_pos_y": result["body_position"][1],
                    "body_pos_z": result["body_position"][2],
                    "timestamp": result["timestamp"],
                    "waypoint_idx": result["waypoint_idx"]
                }
                metadata_list.append(metadata)

                if progress_callback:
                    progress_callback(i + 1, total)
                elif (i + 1) % 10 == 0 or i == 0:
                    ts_log("[Collector]", f"Progress: {i+1}/{total}")

            except Exception as e:
                ts_log("[Collector]", f"Failed to capture pose {pose.index}: {e}", "ERROR")
                traceback.print_exc()

        return metadata_list


def save_metadata_csv(metadata_list: List[Dict], output_path: str):
    """保存元数据到CSV文件"""
    if not metadata_list:
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=metadata_list[0].keys())
        writer.writeheader()
        for row in metadata_list:
            writer.writerow(row)

    ts_log("[Collector]", f"Metadata saved to {output_path}")


def main():
    """主函数"""
    ts_log("[Main]", "Starting Offline Image Collector")

    # 验证输入文件
    csv_path = ARGS.csv
    usd_path = ARGS.usd

    if not os.path.exists(csv_path):
        ts_log("[Main]", f"CSV file not found: {csv_path}", "ERROR")
        return 1

    if not os.path.exists(usd_path):
        ts_log("[Main]", f"USD file not found: {usd_path}", "ERROR")
        return 1

    # 确定输出目录
    if ARGS.output:
        output_dir = ARGS.output
    else:
        output_dir = os.path.join(os.path.dirname(csv_path), "offline_images")

    ts_log("[Main]", f"CSV: {csv_path}")
    ts_log("[Main]", f"USD: {usd_path}")
    ts_log("[Main]", f"Output: {output_dir}")

    # 加载位姿数据
    poses = load_poses_from_csv(csv_path)
    if not poses:
        ts_log("[Main]", "No poses loaded from CSV", "ERROR")
        return 1

    # 创建采集器
    collector = OfflineImageCollector(
        usd_path=usd_path,
        resolution=tuple(ARGS.resolution),
        fov=ARGS.fov,
        enable_depth=ARGS.depth,
        camera_offset=np.array(ARGS.camera_offset),
        camera_rotation=np.array(ARGS.camera_rotation)
    )

    # 初始化
    collector.initialize()

    # 采集图像
    ts_log("[Main]", f"Starting collection of {len(poses)} images...")
    start_time = time.time()

    metadata_list = collector.collect_all(poses, output_dir)

    elapsed = time.time() - start_time
    ts_log("[Main]", f"Collection complete: {len(metadata_list)} images in {elapsed:.1f}s")

    # 保存元数据
    metadata_path = os.path.join(output_dir, "metadata.csv")
    save_metadata_csv(metadata_list, metadata_path)

    ts_log("[Main]", "Done!")
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as e:
        ts_log("[Main]", f"Fatal error: {e}", "ERROR")
        traceback.print_exc()
        exit_code = 1
    finally:
        simulation_app.close()
    sys.exit(exit_code)
