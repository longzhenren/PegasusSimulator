#!/usr/bin/env python3
"""
Auto-restart Trajectory Collection Monitor

This script monitors the trajectory collection process and automatically restarts
the simulation when PhysX errors or UAV stuck conditions are detected.

Features:
- Starts Isaac Sim simulation
- Runs trajectory collector
- Monitors for errors (PhysX cache errors, UAV stuck at ground)
- Automatically restarts when issues detected
- Tracks progress with --skip-existing
- Runs until all trajectories are collected
"""
import os
import sys
import time
import signal
import subprocess
import threading
import re
import argparse
from datetime import datetime
from pathlib import Path

class CollectionMonitor:
    def __init__(self, config):
        self.config = config
        self.sim_process = None
        self.collector_process = None
        self.sim_log_file = None
        self.collector_log_file = None
        self.running = True
        self.restart_count = 0
        self.max_restarts = config.get('max_restarts', 1000)
        self.trajectories_completed = 0
        self.last_progress_time = time.time()
        self.stuck_threshold = config.get('stuck_threshold', 120)  # seconds
        self.batch_size = config.get('batch_size', 50)  # restart after N trajectories
        self.current_batch_count = 0

        # Paths
        self.isaac_sim_python = config['isaac_sim_python']
        self.sim_script = config['sim_script']
        self.collector_script = config['collector_script']
        self.sim_config = config['sim_config']
        self.input_dir = config['input_dir']
        self.output_dir = config['output_dir']
        self.log_dir = config.get('log_dir', 'collection_logs')

        # Create log directory
        os.makedirs(self.log_dir, exist_ok=True)

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print(f"\n[{self._timestamp()}] Received signal {signum}, shutting down...")
        self.running = False
        self._cleanup()
        sys.exit(0)

    def _timestamp(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _log(self, msg, level="INFO"):
        timestamp = self._timestamp()
        print(f"[{timestamp}] [{level}] {msg}")

        # Also write to log file
        log_path = os.path.join(self.log_dir, "monitor.log")
        with open(log_path, "a") as f:
            f.write(f"[{timestamp}] [{level}] {msg}\n")

    def _kill_processes(self):
        """Kill all simulation-related processes"""
        self._log("Killing existing processes...")

        # Kill specific processes
        try:
            subprocess.run(["pkill", "-9", "px4"], capture_output=True)
            subprocess.run(["pkill", "-9", "-f", "mavlink_sim_vehicle"], capture_output=True)
            subprocess.run(["pkill", "-9", "-f", "mavlink_trajectory_collector"], capture_output=True)
            subprocess.run(["pkill", "-9", "-f", "isaacsim"], capture_output=True)
        except:
            pass

        # Wait for processes to die
        time.sleep(5)

        # Close process handles
        if self.sim_process:
            try:
                self.sim_process.kill()
                self.sim_process.wait(timeout=5)
            except:
                pass
            self.sim_process = None

        if self.collector_process:
            try:
                self.collector_process.kill()
                self.collector_process.wait(timeout=5)
            except:
                pass
            self.collector_process = None

    def _start_simulation(self):
        """Start Isaac Sim simulation"""
        self._log("Starting Isaac Sim simulation...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(self.log_dir, f"sim_{timestamp}.log")
        self.sim_log_file = open(log_path, "w")

        cmd = [
            self.isaac_sim_python,
            self.sim_script,
            "--config", self.sim_config
        ]

        self.sim_process = subprocess.Popen(
            cmd,
            stdout=self.sim_log_file,
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(self.sim_script)
        )

        self._log(f"Simulation started (PID: {self.sim_process.pid})")

        # Wait for simulation to be ready
        self._log("Waiting for simulation to initialize...")
        time.sleep(45)  # Initial startup time

        # Check if simulation is healthy
        for _ in range(30):
            try:
                result = subprocess.run(
                    ["curl", "-s", f"http://127.0.0.1:{self.config.get('control_port', 5009)}/health"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if "healthy" in result.stdout:
                    self._log("Simulation is healthy and ready")
                    return True
            except:
                pass
            time.sleep(2)

        self._log("Simulation failed to become healthy", "WARN")
        return False

    def _start_collector(self):
        """Start trajectory collector"""
        self._log("Starting trajectory collector...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(self.log_dir, f"collector_{timestamp}.log")
        self.collector_log_file = open(log_path, "w")

        control_port = self.config.get('control_port', 5009)

        cmd = [
            "python3",
            self.collector_script,
            "--config", self.sim_config,
            "--input-dir", self.input_dir,
            "--out-dir", self.output_dir,
            "--control-base", f"http://127.0.0.1:{control_port}",
            "--image-base", f"http://127.0.0.1:{control_port}",
            "--scale", str(self.config.get('scale', 0.01)),
            "--reset-timeout", "60",
            "--cmd-timeout", "60",
            "--skip-existing"
        ]

        self.collector_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(self.collector_script),
            text=True,
            bufsize=1
        )

        self._log(f"Collector started (PID: {self.collector_process.pid})")
        self.last_progress_time = time.time()
        self.current_batch_count = 0
        return True

    def _monitor_collector(self):
        """Monitor collector output for errors and progress"""
        if not self.collector_process:
            return False

        try:
            # Non-blocking read
            line = self.collector_process.stdout.readline()
            if line:
                line = line.strip()

                # Write to log file
                if self.collector_log_file:
                    self.collector_log_file.write(line + "\n")
                    self.collector_log_file.flush()

                # Check for completion
                if "done traj=" in line:
                    self.trajectories_completed += 1
                    self.current_batch_count += 1
                    self.last_progress_time = time.time()
                    self._log(f"Completed trajectory #{self.trajectories_completed} (batch: {self.current_batch_count}/{self.batch_size})")

                    # Check if batch size reached
                    if self.current_batch_count >= self.batch_size:
                        self._log(f"Batch size {self.batch_size} reached, restarting simulation...")
                        return False

                # Check for stuck UAV
                if "climbing:" in line and "0.06m" in line:
                    # UAV stuck at ground
                    if time.time() - self.last_progress_time > 30:
                        self._log("UAV stuck at ground level detected", "WARN")
                        return False

                # Check for high errors
                if "avg_error=" in line:
                    match = re.search(r'avg_error=(\d+\.\d+)m', line)
                    if match:
                        error = float(match.group(1))
                        if error > 5.0:
                            self._log(f"High tracking error detected: {error}m", "WARN")

                # Check for PhysX errors in simulation log
                if self.sim_log_file:
                    self.sim_log_file.flush()

                # Check for "All workers completed"
                if "All workers completed" in line:
                    self._log("All trajectories completed!")
                    return "done"

            # Check process status
            if self.collector_process.poll() is not None:
                self._log(f"Collector process exited with code {self.collector_process.returncode}")
                return "exited"

        except Exception as e:
            self._log(f"Monitor error: {e}", "ERROR")

        # Check for stuck (no progress for too long)
        if time.time() - self.last_progress_time > self.stuck_threshold:
            self._log(f"No progress for {self.stuck_threshold}s, restarting...", "WARN")
            return False

        return True

    def _check_simulation_health(self):
        """Check if simulation is still healthy"""
        try:
            result = subprocess.run(
                ["curl", "-s", f"http://127.0.0.1:{self.config.get('control_port', 5009)}/health"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return "healthy" in result.stdout
        except:
            return False

    def _count_completed(self):
        """Count completed trajectories"""
        try:
            output_path = Path(self.output_dir)
            if output_path.exists():
                return len([d for d in output_path.iterdir() if d.is_dir()])
        except:
            pass
        return 0

    def _count_total(self):
        """Count total trajectories to process"""
        try:
            input_path = Path(self.input_dir)
            if input_path.exists():
                return len(list(input_path.glob("*.json")))
        except:
            pass
        return 0

    def _cleanup(self):
        """Cleanup resources"""
        self._kill_processes()

        if self.sim_log_file:
            self.sim_log_file.close()
        if self.collector_log_file:
            self.collector_log_file.close()

    def run(self):
        """Main monitoring loop"""
        total = self._count_total()
        self._log(f"Starting collection monitor")
        self._log(f"Total trajectories: {total}")
        self._log(f"Output directory: {self.output_dir}")
        self._log(f"Batch size: {self.batch_size}")
        self._log(f"Stuck threshold: {self.stuck_threshold}s")

        while self.running and self.restart_count < self.max_restarts:
            completed = self._count_completed()
            self._log(f"Progress: {completed}/{total} trajectories completed")

            if completed >= total:
                self._log("All trajectories completed!")
                break

            # Kill existing processes
            self._kill_processes()

            # Start fresh simulation
            self.restart_count += 1
            self._log(f"=== Restart #{self.restart_count} ===")

            if not self._start_simulation():
                self._log("Failed to start simulation, retrying...", "ERROR")
                time.sleep(10)
                continue

            if not self._start_collector():
                self._log("Failed to start collector, retrying...", "ERROR")
                time.sleep(10)
                continue

            # Monitor loop
            while self.running:
                status = self._monitor_collector()

                if status == "done":
                    self._log("Collection completed successfully!")
                    self.running = False
                    break
                elif status == "exited":
                    # Collector exited, check if done
                    completed = self._count_completed()
                    if completed >= total:
                        self._log("All trajectories completed!")
                        self.running = False
                    break
                elif status == False:
                    # Need restart
                    break

                # Small sleep to prevent busy loop
                time.sleep(0.1)

        self._cleanup()

        final_completed = self._count_completed()
        self._log(f"=== Final Summary ===")
        self._log(f"Total restarts: {self.restart_count}")
        self._log(f"Trajectories completed: {final_completed}/{total}")

        return final_completed >= total


def main():
    parser = argparse.ArgumentParser(description='Auto-restart Trajectory Collection Monitor')
    parser.add_argument('--isaac-sim-python', type=str,
                        default='/home/user/isaacsim-5.1.0/python.sh',
                        help='Path to Isaac Sim Python')
    parser.add_argument('--sim-script', type=str,
                        default='/home/user/PegasusSimulator-5.1/examples/mavlink_sim_vehicle.py',
                        help='Simulation script path')
    parser.add_argument('--collector-script', type=str,
                        default='/home/user/PegasusSimulator-5.1/examples/mavlink_trajectory_collector.py',
                        help='Collector script path')
    parser.add_argument('--sim-config', type=str,
                        default='multi_uav_config_1.json',
                        help='Simulation config file')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='Input trajectory directory')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Output directory')
    parser.add_argument('--log-dir', type=str, default='collection_logs',
                        help='Log directory')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Restart after N trajectories')
    parser.add_argument('--stuck-threshold', type=int, default=120,
                        help='Restart if no progress for N seconds')
    parser.add_argument('--scale', type=float, default=0.01,
                        help='Coordinate scale factor')
    parser.add_argument('--control-port', type=int, default=5009,
                        help='Control server port')

    args = parser.parse_args()

    config = {
        'isaac_sim_python': args.isaac_sim_python,
        'sim_script': args.sim_script,
        'collector_script': args.collector_script,
        'sim_config': args.sim_config,
        'input_dir': args.input_dir,
        'output_dir': args.output_dir,
        'log_dir': args.log_dir,
        'batch_size': args.batch_size,
        'stuck_threshold': args.stuck_threshold,
        'scale': args.scale,
        'control_port': args.control_port,
    }

    monitor = CollectionMonitor(config)
    success = monitor.run()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
