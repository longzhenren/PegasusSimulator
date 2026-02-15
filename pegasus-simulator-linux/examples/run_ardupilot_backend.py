"""
Example: Run ArduPilot backend on Linux side.

This script connects to the Windows side (Isaac Sim) and runs an ArduPilot MAVLink backend.
It receives sensor data and state from Windows, and sends control commands back.

Prerequisites:
    - ArduPilot SITL running on this machine
    - Windows side running with NetworkBackend

Usage:
    python run_ardupilot_backend.py --server-host <Windows_IP>

Example:
    python run_ardupilot_backend.py --server-host 192.168.1.100
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pegasus.simulator.backends import ArduPilotMavlinkBackend, ArduPilotMavlinkBackendConfig
from pegasus.simulator.network import BackendRunner


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run ArduPilot backend for Pegasus Simulator")
    parser.add_argument("--server-host", type=str, required=True,
                        help="Windows host IP address")
    parser.add_argument("--server-port", type=int, default=5555,
                        help="Windows host port (default: 5555)")
    parser.add_argument("--vehicle-id", type=int, default=0,
                        help="Vehicle ID (default: 0)")
    parser.add_argument("--mavlink-port", type=int, default=4560,
                        help="MAVLink base port (default: 4560)")

    args = parser.parse_args()

    # Create ArduPilot backend configuration
    ardupilot_config = ArduPilotMavlinkBackendConfig({
        "vehicle_id": args.vehicle_id,
        "connection_type": "tcpin",
        "connection_ip": "localhost",
        "connection_baseport": args.mavlink_port,
        "enable_lockstep": True,
        "num_rotors": 4
    })

    # Create ArduPilot backend
    backend = ArduPilotMavlinkBackend(ardupilot_config)

    # Create and run backend runner
    runner = BackendRunner(
        backend=backend,
        server_host=args.server_host,
        server_port=args.server_port
    )

    print(f"Connecting to Windows simulation at {args.server_host}:{args.server_port}")
    print(f"ArduPilot MAVLink listening on localhost:{args.mavlink_port}")
    print("Press Ctrl+C to stop")

    try:
        runner.run()
    except KeyboardInterrupt:
        print("\nStopped by user")


if __name__ == "__main__":
    main()
