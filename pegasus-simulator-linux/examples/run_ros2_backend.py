"""
Example: Run ROS2 backend on Linux side.

This script connects to the Windows side (Isaac Sim) and runs a ROS2 backend.
It receives sensor data and state from Windows, publishes to ROS2 topics, and sends control commands back.

Prerequisites:
    - ROS2 installed on this machine
    - Windows side running with NetworkBackend

Usage:
    python run_ros2_backend.py --server-host <Windows_IP>

Example:
    python run_ros2_backend.py --server-host 192.168.1.100
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pegasus.simulator.backends import ROS2Backend, ROS2BackendConfig
from pegasus.simulator.network import BackendRunner


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run ROS2 backend for Pegasus Simulator")
    parser.add_argument("--server-host", type=str, required=True,
                        help="Windows host IP address")
    parser.add_argument("--server-port", type=int, default=5555,
                        help="Windows host port (default: 5555)")
    parser.add_argument("--vehicle-id", type=int, default=0,
                        help="Vehicle ID (default: 0)")
    parser.add_argument("--namespace", type=str, default="drone",
                        help="ROS2 namespace (default: drone)")

    args = parser.parse_args()

    # Create ROS2 backend configuration
    ros2_config = ROS2BackendConfig({
        "vehicle_id": args.vehicle_id,
        "namespace": args.namespace,
        "pub_sensors": True,
        "pub_graphical_sensors": True,
        "pub_state": True,
        "pub_tf": False,
        "sub_control": False
    })

    # Create ROS2 backend
    backend = ROS2Backend(ros2_config)

    # Create and run backend runner
    runner = BackendRunner(
        backend=backend,
        server_host=args.server_host,
        server_port=args.server_port
    )

    print(f"Connecting to Windows simulation at {args.server_host}:{args.server_port}")
    print(f"ROS2 namespace: {args.namespace}")
    print("Press Ctrl+C to stop")

    try:
        runner.run()
    except KeyboardInterrupt:
        print("\nStopped by user")


if __name__ == "__main__":
    main()
