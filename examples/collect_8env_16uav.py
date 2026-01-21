#!/usr/bin/env python3
"""
8环境×2机 并行数据采集脚本

配置：8个独立仿真环境，每个环境2架飞机，共16架飞机并行采集
轨迹来源：~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files/

端口规划：
环境 | HTTP端口 | 控制器基础端口 | UAV IDs
-----|---------|---------------|--------
0    | 8081    | 5009          | 0, 1
1    | 8082    | 5109          | 2, 3
2    | 8083    | 5209          | 4, 5
3    | 8084    | 5309          | 6, 7
4    | 8085    | 5409          | 8, 9
5    | 8086    | 5509          | 10, 11
6    | 8087    | 5609          | 12, 13
7    | 8088    | 5709          | 14, 15
"""

import os
import sys
import json
import time
import glob
import subprocess
import threading
import queue
import argparse
import requests
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil

# 配置
NUM_ENVS = 8
UAVS_PER_ENV = 2
TOTAL_UAVS = NUM_ENVS * UAVS_PER_ENV

TRAJ_DIR = os.path.expanduser("~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files")
OUTPUT_DIR = os.path.expanduser("~/uav-data/drone/uav-flow-sim/sim_collected_data")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "multi_env_configs")
ISAACSIM_PYTHON = os.path.expanduser("~/isaacsim-5.1.0/python.sh")
SIM_SCRIPT = os.path.join(SCRIPT_DIR, "mavlink_sim_vehicle.py")


def get_ports_for_env(env_id: int) -> Tuple[int, int]:
    """获取环境的HTTP端口和控制器基础端口"""
    sim_port = 8081 + env_id
    ctrl_base_port = 5009 + env_id * 100
    return sim_port, ctrl_base_port


def get_uav_env_and_local_id(global_uav_id: int) -> Tuple[int, int]:
    """获取UAV所属的环境ID和环境内局部ID"""
    env_id = global_uav_id // UAVS_PER_ENV
    local_id = global_uav_id % UAVS_PER_ENV
    return env_id, local_id


class MultiEnvManager:
    """8环境管理器"""

    def __init__(self, num_envs: int = 8, uavs_per_env: int = 2):
        self.num_envs = num_envs
        self.uavs_per_env = uavs_per_env
        self.total_uavs = num_envs * uavs_per_env

        self.log_dir = f"/tmp/multi_env_8x2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(CONFIG_DIR, exist_ok=True)

        self.env_processes: List[subprocess.Popen] = []

    def generate_configs(self):
        """生成8个环境的配置文件"""
        print(f"[配置] 生成 {self.num_envs} 个环境配置...")

        uav_id = 0
        for env_id in range(self.num_envs):
            config = {
                "description": f"Environment {env_id}: UAV {uav_id}-{uav_id + self.uavs_per_env - 1}",
                "vehicles": []
            }

            for i in range(self.uavs_per_env):
                vehicle = {
                    "vehicle_id": uav_id,
                    "ros2_namespace": f"uav{uav_id}",
                    "initial_position": [i * 3.0, 0.0, 0.5],
                    "initial_orientation_euler_deg": [0.0, 0.0, 0.0],
                    "px4_autolaunch": True,
                    "px4_dir": "/home/user/PX4-Autopilot",
                    "sim_speed_factor": 1.0
                }
                config["vehicles"].append(vehicle)
                uav_id += 1

            config_path = os.path.join(CONFIG_DIR, f"env{env_id}_{self.uavs_per_env}uav.json")
            with open(config_path, "w") as f:
                json.dump(config, f, indent=2)

        print(f"  生成完成: {self.num_envs} 个配置文件")

    def cleanup(self):
        """清理所有进程"""
        print("[清理] 停止所有仿真进程...")

        for proc in self.env_processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self.env_processes.clear()

        # 杀死残留进程
        subprocess.run(["pkill", "-9", "-f", "mavlink_sim_vehicle"], capture_output=True)
        subprocess.run(["pkill", "-9", "-f", "px4_sitl"], capture_output=True)

        # 清理锁文件
        for pattern in ["/tmp/px4_instance_*", "/tmp/px4-*", "/tmp/pegasus_px4_sitl/*.pid"]:
            subprocess.run(f"rm -rf {pattern}", shell=True, capture_output=True)

        time.sleep(3)
        print("  清理完成")

    def start_all_environments(self) -> bool:
        """启动所有8个仿真环境"""
        print(f"\n[启动] 启动 {self.num_envs} 个仿真环境...")

        for env_id in range(self.num_envs):
            config_file = os.path.join(CONFIG_DIR, f"env{env_id}_{self.uavs_per_env}uav.json")
            sim_port, ctrl_base = get_ports_for_env(env_id)
            log_file = os.path.join(self.log_dir, f"env{env_id}.log")

            print(f"  环境 {env_id}: 端口={sim_port}, UAV={env_id*2}-{env_id*2+1}")

            with open(log_file, "w") as log_f:
                proc = subprocess.Popen(
                    [ISAACSIM_PYTHON, SIM_SCRIPT,
                     "--config", config_file,
                     "--headless",
                     "--no-images",
                     "--sim-port", str(sim_port),
                     "--ctrl-base-port", str(ctrl_base)],
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    cwd=SCRIPT_DIR
                )
                self.env_processes.append(proc)

            # 环境间启动间隔（减少GPU竞争）
            if env_id < self.num_envs - 1:
                print(f"    等待15秒...")
                time.sleep(15)

        return True

    def wait_for_ready(self, timeout: int = 300) -> Tuple[int, int]:
        """等待所有环境和PX4就绪"""
        print(f"\n[等待] 等待所有环境就绪 (超时={timeout}s)...")

        start_time = time.time()

        while time.time() - start_time < timeout:
            http_ready = 0
            px4_ready = 0

            for env_id in range(self.num_envs):
                sim_port, _ = get_ports_for_env(env_id)

                # 检查进程存活
                if env_id < len(self.env_processes):
                    if self.env_processes[env_id].poll() is not None:
                        continue

                try:
                    resp = requests.get(f"http://127.0.0.1:{sim_port}/health", timeout=2)
                    if resp.status_code == 200:
                        http_ready += 1

                        for i in range(self.uavs_per_env):
                            uav_id = env_id * self.uavs_per_env + i
                            try:
                                resp2 = requests.get(
                                    f"http://127.0.0.1:{sim_port}/uav/{uav_id}/px4/ready",
                                    timeout=2
                                )
                                if resp2.status_code == 200 and resp2.json().get("ready"):
                                    px4_ready += 1
                            except Exception:
                                pass
                except Exception:
                    pass

            elapsed = int(time.time() - start_time)
            print(f"  [{elapsed}s] HTTP: {http_ready}/{self.num_envs}, PX4: {px4_ready}/{self.total_uavs}")

            if http_ready == self.num_envs and px4_ready == self.total_uavs:
                print("  全部就绪!")
                return http_ready, px4_ready

            time.sleep(3)

        return http_ready, px4_ready

    def get_sim_port_for_uav(self, global_uav_id: int) -> int:
        """获取指定UAV的仿真端口"""
        env_id = global_uav_id // self.uavs_per_env
        sim_port, _ = get_ports_for_env(env_id)
        return sim_port

    def get_ctrl_port_for_uav(self, global_uav_id: int) -> int:
        """获取指定UAV的控制器端口"""
        env_id = global_uav_id // self.uavs_per_env
        _, ctrl_base = get_ports_for_env(env_id)
        return ctrl_base + global_uav_id


class TrajectoryCollector:
    """单个UAV的轨迹采集器"""

    def __init__(self, uav_id: int, sim_port: int, ctrl_port: int, output_dir: str):
        self.uav_id = uav_id
        self.sim_port = sim_port
        self.ctrl_port = ctrl_port
        self.output_dir = output_dir
        self.sim_url = f"http://127.0.0.1:{sim_port}"
        self.ctrl_url = f"http://127.0.0.1:{ctrl_port}"

    def collect_trajectory(self, traj_file: str) -> Dict:
        """采集单条轨迹"""
        traj_name = os.path.basename(traj_file)
        result = {
            "uav_id": self.uav_id,
            "traj_file": traj_file,
            "traj_name": traj_name,
            "status": "unknown",
        }

        try:
            # 读取轨迹
            with open(traj_file, "r") as f:
                traj_data = json.load(f)

            positions = traj_data.get("positions", [])
            if len(positions) < 2:
                result["status"] = "skipped"
                result["reason"] = "too_few_points"
                return result

            # 获取起始位置
            start_pos = positions[0]
            if isinstance(start_pos, dict):
                start_x, start_y, start_z = start_pos.get("x", 0), start_pos.get("y", 0), start_pos.get("z", 2.5)
            else:
                start_x, start_y, start_z = start_pos[0], start_pos[1], start_pos[2] if len(start_pos) > 2 else 2.5

            # 缩放到合理范围
            scale = 0.01
            start_x *= scale
            start_y *= scale
            start_z = max(abs(start_z * scale), 2.0)

            # 重置UAV位置
            reset_resp = requests.post(
                f"{self.sim_url}/uav/{self.uav_id}/reset",
                json={"position": [start_x, start_y, start_z]},
                timeout=5
            )

            if reset_resp.status_code != 200:
                result["status"] = "error"
                result["reason"] = "reset_failed"
                return result

            time.sleep(1)

            # 开始录制
            requests.post(
                f"{self.sim_url}/uav/{self.uav_id}/buffer/start",
                json={"traj_name": traj_name},
                timeout=5
            )

            # 简化：发送几个位置点
            num_points = min(len(positions), 50)
            for i in range(0, len(positions), max(1, len(positions) // num_points)):
                pos = positions[i]
                if isinstance(pos, dict):
                    x, y, z = pos.get("x", 0), pos.get("y", 0), pos.get("z", 2.5)
                else:
                    x, y, z = pos[0], pos[1], pos[2] if len(pos) > 2 else 2.5

                x *= scale
                y *= scale
                z = max(abs(z * scale), 1.0)

                try:
                    requests.post(
                        f"{self.ctrl_url}/command",
                        json={"cmd": "setpoint", "x": x, "y": y, "z": z, "vx": 0, "vy": 0, "vz": 0,
                              "afx": 0, "afy": 0, "afz": 0, "yaw": 0, "yaw_rate": 0},
                        timeout=2
                    )
                except Exception:
                    pass

                time.sleep(0.1)

            # 停止录制
            stop_resp = requests.post(
                f"{self.sim_url}/uav/{self.uav_id}/buffer/stop",
                json={"return_raw_observations": True},
                timeout=10
            )

            if stop_resp.status_code == 200:
                data = stop_resp.json()
                result["status"] = "success"
                result["state_count"] = data.get("state_count", 0)
            else:
                result["status"] = "error"
                result["reason"] = "buffer_stop_failed"

        except Exception as e:
            result["status"] = "error"
            result["reason"] = str(e)

        return result


class ParallelCollectionManager:
    """并行采集管理器"""

    def __init__(self, env_manager: MultiEnvManager, traj_files: List[str], output_dir: str):
        self.env_manager = env_manager
        self.traj_files = traj_files
        self.output_dir = output_dir

        os.makedirs(output_dir, exist_ok=True)

        # 创建各UAV的采集器
        self.collectors: List[TrajectoryCollector] = []
        for uav_id in range(env_manager.total_uavs):
            sim_port = env_manager.get_sim_port_for_uav(uav_id)
            ctrl_port = env_manager.get_ctrl_port_for_uav(uav_id)
            collector = TrajectoryCollector(uav_id, sim_port, ctrl_port, output_dir)
            self.collectors.append(collector)

        # 轨迹队列
        self.traj_queue = queue.Queue()
        for traj in traj_files:
            self.traj_queue.put(traj)

        self.results = []
        self.results_lock = threading.Lock()

    def worker(self, uav_id: int):
        """单个UAV的工作线程"""
        collector = self.collectors[uav_id]

        while True:
            try:
                traj_file = self.traj_queue.get_nowait()
            except queue.Empty:
                break

            result = collector.collect_trajectory(traj_file)

            with self.results_lock:
                self.results.append(result)
                completed = len(self.results)

            if completed % 100 == 0:
                print(f"  进度: {completed}/{len(self.traj_files)}")

            self.traj_queue.task_done()

    def run(self, max_trajectories: int = None) -> List[Dict]:
        """运行并行采集"""
        if max_trajectories:
            # 限制采集数量（用于测试）
            while self.traj_queue.qsize() > max_trajectories:
                try:
                    self.traj_queue.get_nowait()
                except queue.Empty:
                    break

        total = self.traj_queue.qsize()
        print(f"\n[采集] 开始并行采集 {total} 条轨迹，使用 {self.env_manager.total_uavs} 架飞机...")

        # 启动工作线程
        threads = []
        for uav_id in range(self.env_manager.total_uavs):
            t = threading.Thread(target=self.worker, args=(uav_id,), name=f"collector_{uav_id}")
            t.start()
            threads.append(t)

        # 等待完成
        for t in threads:
            t.join()

        print(f"  采集完成: {len(self.results)} 条轨迹")
        return self.results


def test_8env_startup():
    """测试8环境启动"""
    print("=" * 60)
    print("8环境×2机 启动测试")
    print("=" * 60)

    manager = MultiEnvManager(num_envs=8, uavs_per_env=2)

    try:
        manager.cleanup()
        manager.generate_configs()

        # 记录资源
        start_mem = psutil.virtual_memory().percent

        start_time = time.time()
        manager.start_all_environments()
        http_ready, px4_ready = manager.wait_for_ready(timeout=360)
        startup_time = time.time() - start_time

        end_mem = psutil.virtual_memory().percent

        print("\n" + "=" * 60)
        print("启动测试结果")
        print("=" * 60)
        print(f"配置: {manager.num_envs}环境 × {manager.uavs_per_env}机 = {manager.total_uavs}机")
        print(f"启动时间: {startup_time:.1f}s")
        print(f"HTTP就绪: {http_ready}/{manager.num_envs}")
        print(f"PX4就绪: {px4_ready}/{manager.total_uavs}")
        print(f"内存使用: {start_mem:.1f}% -> {end_mem:.1f}%")
        print(f"状态: {'✓ 成功' if px4_ready == manager.total_uavs else '✗ 失败'}")

        return manager, (http_ready == manager.num_envs and px4_ready == manager.total_uavs)

    except Exception as e:
        print(f"错误: {e}")
        traceback.print_exc()
        manager.cleanup()
        return None, False


def main():
    parser = argparse.ArgumentParser(description="8环境×2机 并行数据采集")
    parser.add_argument("--test-startup", action="store_true", help="仅测试启动")
    parser.add_argument("--max-traj", type=int, default=100, help="最大采集轨迹数（测试用）")
    parser.add_argument("--collect-all", action="store_true", help="采集全部轨迹")
    args = parser.parse_args()

    if args.test_startup:
        manager, success = test_8env_startup()
        if manager:
            input("\n按Enter键清理并退出...")
            manager.cleanup()
        return

    # 完整采集流程
    print("=" * 60)
    print("8环境×2机 并行轨迹采集")
    print("=" * 60)

    # 获取轨迹文件列表
    traj_files = sorted(glob.glob(os.path.join(TRAJ_DIR, "*.json")))
    print(f"找到 {len(traj_files)} 条轨迹")

    if not traj_files:
        print("错误: 未找到轨迹文件")
        return

    # 启动环境
    manager, success = test_8env_startup()

    if not success:
        print("环境启动失败，退出")
        if manager:
            manager.cleanup()
        return

    try:
        # 创建采集管理器
        collector_manager = ParallelCollectionManager(
            manager, traj_files, OUTPUT_DIR
        )

        # 运行采集
        max_traj = None if args.collect_all else args.max_traj
        results = collector_manager.run(max_trajectories=max_traj)

        # 统计结果
        success_count = sum(1 for r in results if r.get("status") == "success")
        error_count = sum(1 for r in results if r.get("status") == "error")
        skipped_count = sum(1 for r in results if r.get("status") == "skipped")

        print("\n" + "=" * 60)
        print("采集结果统计")
        print("=" * 60)
        print(f"成功: {success_count}")
        print(f"失败: {error_count}")
        print(f"跳过: {skipped_count}")
        print(f"总计: {len(results)}")

        # 保存结果
        result_file = os.path.join(OUTPUT_DIR, f"collection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(result_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n结果已保存: {result_file}")

    finally:
        manager.cleanup()


if __name__ == "__main__":
    main()
