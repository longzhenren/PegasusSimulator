"""
Example: Run PX4 backend on Linux side.

This script connects to the Windows side (Isaac Sim) and runs a PX4 MAVLink backend.
It receives sensor data and state from Windows, and sends control commands back.

Prerequisites:
    - PX4 SITL running on this machine
    - Windows side running with NetworkBackend

Usage:
    python run_px4_backend.py --server-host <Windows_IP>

Example:
    python run_px4_backend.py --server-host 192.168.1.100
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pegasus.simulator.backends import PX4MavlinkBackend, PX4MavlinkBackendConfig
from pegasus.simulator.network import BackendRunner


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run PX4 backend for Pegasus Simulator")
    parser.add_argument("--server-host", type=str, required=True,
                        help="Windows host IP address")
    parser.add_argument("--server-port", type=int, default=5555,
                        help="Windows host port (default: 5555)")
    parser.add_argument("--vehicle-id", type=int, default=0,
                        help="Vehicle ID (default: 0)")
    parser.add_argument("--mavlink-port", type=int, default=4560,
                        help="MAVLink base port (default: 4560)")

    args = parser.parse_args()

    # Create PX4 backend configuration
    px4_config = PX4MavlinkBackendConfig({
        "vehicle_id": args.vehicle_id,
        "connection_type": "tcpin",
        "connection_ip": "localhost",
        "connection_baseport": args.mavlink_port,
        "enable_lockstep": True,
        "num_rotors": 4
    })

    # Create PX4 backend
    backend = PX4MavlinkBackend(px4_config)

    # Create and run backend runner
    runner = BackendRunner(
        backend=backend,
        server_host=args.server_host,
        server_port=args.server_port
    )

    print(f"Connecting to Windows simulation at {args.server_host}:{args.server_port}")
    print(f"PX4 MAVLink listening on localhost:{args.mavlink_port}")
    print("Press Ctrl+C to stop")

    try:
        runner.run()
    except KeyboardInterrupt:
        print("\nStopped by user")


if __name__ == "__main__":
    main()
