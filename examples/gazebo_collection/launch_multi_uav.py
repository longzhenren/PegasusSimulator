#!/usr/bin/env python3
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# -*- coding: utf-8 -*-
"""
Multi-UAV PX4 SITL + Gazebo Launcher (Python Version)
=====================================================

A Python script to launch multiple PX4 SITL instances with Gazebo simulation.
More flexible and easier to configure than the bash version.

Features:
- Automatic port allocation
- Grid-based spawn positions
- Process management and cleanup
- ROS2 MAVROS integration

Usage:
    python3 launch_multi_uav.py --num-uavs 4
    python3 launch_multi_uav.py --num-uavs 8 --start-id 0 --spacing 4.0
"""

import argparse
import atexit
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional


class Colors:
    """Terminal colors"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'


def log_info(msg: str):
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}", flush=True)


def log_warn(msg: str):
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}", flush=True)


def log_error(msg: str):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}", flush=True)


class ProcessManager:
    """Manage subprocess lifecycle"""

    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}

    def start(self, name: str, cmd: List[str], env: Optional[Dict] = None,
              cwd: Optional[str] = None, log_file: Optional[str] = None) -> Optional[subprocess.Popen]:
        """Start a process"""
        try:
            full_env = os.environ.copy()
            if env:
                full_env.update(env)

            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
            if log_file:
                stdout = open(log_file, 'w')
                stderr = stdout

            proc = subprocess.Popen(
                cmd,
                env=full_env,
                cwd=cwd,
                stdout=stdout,
                stderr=stderr,
                preexec_fn=os.setpgrp,
            )

            self.processes[name] = proc
            log_info(f"Started {name} (PID: {proc.pid})")
            return proc

        except Exception as e:
            log_error(f"Failed to start {name}: {e}")
            return None

    def stop_all(self):
        """Stop all processes"""
        log_info("Stopping all processes...")
        for name, proc in list(self.processes.items()):
            try:
                if proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    proc.wait(timeout=5)
                    log_info(f"Stopped {name}")
            except Exception as e:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except:
                    pass
        self.processes.clear()

    def is_running(self, name: str) -> bool:
        """Check if process is running"""
        if name not in self.processes:
            return False
        return self.processes[name].poll() is None


class MultiUAVLauncher:
    """Launch multiple PX4 SITL + MAVROS instances"""

    def __init__(
        self,
        num_uavs: int,
        start_id: int = 0,
        px4_dir: Optional[str] = None,
        spacing: float = 3.0,
        use_gazebo_classic: bool = True,
    ):
        self.num_uavs = num_uavs
        self.start_id = start_id
        self.spacing = spacing
        self.use_gazebo_classic = use_gazebo_classic

        # Find PX4 directory
        if px4_dir:
            self.px4_dir = Path(px4_dir)
        elif os.environ.get("PX4_DIR"):
            self.px4_dir = Path(os.environ["PX4_DIR"])
        else:
            self.px4_dir = Path.home() / "PX4-Autopilot"

        if not self.px4_dir.exists():
            raise FileNotFoundError(f"PX4-Autopilot not found at {self.px4_dir}")

        self.pm = ProcessManager()
        atexit.register(self.cleanup)

    def get_spawn_position(self, index: int) -> tuple:
        """Get spawn position in grid layout"""
        cols = 4
        row = index // cols
        col = index % cols
        x = col * self.spacing
        y = row * self.spacing
        return (x, y, 0)

    def launch_gazebo(self, world: str = "empty") -> bool:
        """Launch Gazebo simulation"""
        log_info("Launching Gazebo...")

        if self.use_gazebo_classic:
            # Gazebo Classic
            world_file = self.px4_dir / "Tools" / "simulation" / "gazebo-classic" / "worlds" / f"{world}.world"
            if not world_file.exists():
                world_file = self.px4_dir / "Tools" / "sitl_gazebo" / "worlds" / f"{world}.world"

            if world_file.exists():
                cmd = ["gazebo", "--verbose", str(world_file)]
            else:
                cmd = ["gazebo", "--verbose"]
        else:
            # Gazebo Sim (Garden/Harmonic)
            cmd = ["gz", "sim", "-r", f"{world}.sdf"]

        proc = self.pm.start("gazebo", cmd, log_file="/tmp/gazebo.log")
        if proc:
            time.sleep(5)
            return True
        return False

    def launch_px4_sitl(self, uav_id: int, x: float, y: float, z: float = 0) -> bool:
        """Launch single PX4 SITL instance"""
        log_info(f"Launching PX4 SITL for UAV {uav_id} at ({x}, {y}, {z})")

        # Create instance directory
        instance_dir = Path(f"/tmp/px4_{uav_id}_{int(time.time())}")
        instance_dir.mkdir(parents=True, exist_ok=True)

        # Environment variables
        env = {
            "PX4_SYS_AUTOSTART": "4001",
            "PX4_SIM_MODEL": "gazebo-classic_iris" if self.use_gazebo_classic else "gz_x500",
            "PX4_SIM_SPEED_FACTOR": "1",
        }

        if self.use_gazebo_classic:
            # For Gazebo Classic, use sitl_multiple_run.sh or direct launch
            env.update({
                "PX4_HOME_LAT": "47.397742",
                "PX4_HOME_LON": "8.545594",
                "PX4_HOME_ALT": "488.0",
            })

            # Use make command for SITL
            build_dir = self.px4_dir / "build" / "px4_sitl_default"
            if not build_dir.exists():
                log_error(f"PX4 build not found. Run: cd {self.px4_dir} && make px4_sitl_default")
                return False

            px4_bin = build_dir / "bin" / "px4"
            if not px4_bin.exists():
                log_error(f"PX4 binary not found at {px4_bin}")
                return False

            cmd = [
                str(px4_bin),
                "-i", str(uav_id),
                "-d", str(instance_dir),
            ]
        else:
            # For Gazebo Sim
            env.update({
                "PX4_GZ_MODEL": "x500",
                "PX4_GZ_MODEL_POSE": f"{x},{y},{z},0,0,0",
            })

            px4_bin = self.px4_dir / "build" / "px4_sitl_default" / "bin" / "px4"
            cmd = [str(px4_bin), "-i", str(uav_id), "-d", str(instance_dir)]

        log_file = str(instance_dir / f"px4_{uav_id}.log")
        proc = self.pm.start(f"px4_{uav_id}", cmd, env=env, cwd=str(self.px4_dir), log_file=log_file)
        return proc is not None

    def launch_mavros(self, uav_id: int) -> bool:
        """Launch MAVROS for single UAV"""
        log_info(f"Launching MAVROS for UAV {uav_id}")

        namespace = f"uav{uav_id}"
        fcu_url = f"udp://:{14540 + uav_id}@127.0.0.1:{14580 + uav_id}"

        cmd = [
            "ros2", "run", "mavros", "mavros_node",
            "--ros-args",
            "-r", f"__ns:=/{namespace}",
            "-p", f"fcu_url:={fcu_url}",
            "-p", "gcs_url:=",
            "-p", f"target_system_id:={uav_id + 1}",
            "-p", "target_component_id:=1",
        ]

        log_file = f"/tmp/mavros_{uav_id}.log"
        proc = self.pm.start(f"mavros_{uav_id}", cmd, log_file=log_file)
        return proc is not None

    def launch_all(self) -> bool:
        """Launch all UAVs with Gazebo and MAVROS"""
        log_info("=" * 50)
        log_info("Multi-UAV PX4 SITL + Gazebo Launcher")
        log_info("=" * 50)
        log_info(f"Number of UAVs: {self.num_uavs}")
        log_info(f"Starting ID: {self.start_id}")
        log_info(f"PX4 Directory: {self.px4_dir}")
        log_info("=" * 50)

        # Launch Gazebo
        if not self.launch_gazebo():
            log_error("Failed to launch Gazebo")
            return False

        # Launch PX4 instances
        for i in range(self.num_uavs):
            uav_id = self.start_id + i
            x, y, z = self.get_spawn_position(i)
            if not self.launch_px4_sitl(uav_id, x, y, z):
                log_error(f"Failed to launch PX4 for UAV {uav_id}")
                return False
            time.sleep(2)

        # Wait for PX4 to initialize
        log_info("Waiting for PX4 instances to initialize...")
        time.sleep(10)

        # Launch MAVROS nodes
        for i in range(self.num_uavs):
            uav_id = self.start_id + i
            if not self.launch_mavros(uav_id):
                log_error(f"Failed to launch MAVROS for UAV {uav_id}")
                return False
            time.sleep(1)

        # Wait for connections
        log_info("Waiting for MAVROS connections...")
        time.sleep(5)

        log_info("=" * 50)
        log_info("All systems launched successfully!")
        log_info("=" * 50)
        log_info("")
        log_info("UAV Topics:")
        for i in range(self.num_uavs):
            uav_id = self.start_id + i
            print(f"  /uav{uav_id}/mavros/state")
            print(f"  /uav{uav_id}/mavros/local_position/pose")
        log_info("")
        log_info("Press Ctrl+C to shutdown all systems")
        log_info("=" * 50)

        return True

    def wait(self):
        """Wait for shutdown"""
        try:
            while True:
                # Check if all processes are still running
                all_running = True
                for i in range(self.num_uavs):
                    uav_id = self.start_id + i
                    if not self.pm.is_running(f"px4_{uav_id}"):
                        log_warn(f"PX4 for UAV {uav_id} stopped unexpectedly")
                        all_running = False

                if not all_running:
                    log_warn("Some processes stopped. Check logs for errors.")

                time.sleep(5)
        except KeyboardInterrupt:
            log_info("Received shutdown signal")

    def cleanup(self):
        """Cleanup all processes"""
        log_info("Cleaning up...")
        self.pm.stop_all()

        # Kill any remaining processes
        for pattern in ["px4", "gazebo", "gz sim", "mavros"]:
            try:
                subprocess.run(["pkill", "-f", pattern], check=False, capture_output=True)
            except:
                pass

        log_info("Cleanup complete")


def main():
    parser = argparse.ArgumentParser(
        description="Launch multiple PX4 SITL instances with Gazebo and MAVROS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--num-uavs", type=int, default=4,
                        help="Number of UAVs to launch (default: 4)")
    parser.add_argument("--start-id", type=int, default=0,
                        help="Starting UAV ID (default: 0)")
    parser.add_argument("--px4-dir", type=str, default=None,
                        help="PX4-Autopilot directory path")
    parser.add_argument("--spacing", type=float, default=3.0,
                        help="Spacing between UAVs in meters (default: 3.0)")
    parser.add_argument("--gazebo-sim", action="store_true",
                        help="Use Gazebo Sim instead of Gazebo Classic")

    args = parser.parse_args()

    try:
        launcher = MultiUAVLauncher(
            num_uavs=args.num_uavs,
            start_id=args.start_id,
            px4_dir=args.px4_dir,
            spacing=args.spacing,
            use_gazebo_classic=not args.gazebo_sim,
        )

        if launcher.launch_all():
            launcher.wait()
        else:
            log_error("Failed to launch all systems")
            sys.exit(1)

    except FileNotFoundError as e:
        log_error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        log_info("Interrupted by user")
    except Exception as e:
        log_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
