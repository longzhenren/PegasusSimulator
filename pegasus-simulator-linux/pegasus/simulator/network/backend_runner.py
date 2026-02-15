"""Backend runner for Linux side.

This module provides the main entry point for running backends on Linux side.
It connects to the Windows side simulation and runs the specified backend.
"""

import argparse
import sys
from pegasus.simulator.backends import (
    PX4MavlinkBackend,
    PX4MavlinkBackendConfig,
    ArduPilotMavlinkBackend,
    ArduPilotMavlinkBackendConfig,
)
from .simulator_proxy import SimulatorProxy


class BackendRunner:
    """Main runner for backend execution."""

    def __init__(self, backend, server_host: str, server_port: int = 5555,
                 reconnect_interval: float = 2.0, max_reconnect_attempts: int = 10):
        """
        Initialize backend runner.

        Args:
            backend: Backend instance to run
            server_host: Windows host IP address
            server_port: Windows host port
            reconnect_interval: Reconnection interval in seconds
            max_reconnect_attempts: Maximum reconnection attempts
        """
        self._backend = backend
        self._proxy = SimulatorProxy(
            backend=backend,
            server_host=server_host,
            server_port=server_port,
            reconnect_interval=reconnect_interval,
            max_reconnect_attempts=max_reconnect_attempts
        )

    def run(self):
        """Run the backend (blocking)."""
        print(f"[BackendRunner] Starting backend: {type(self._backend).__name__}")
        self._proxy.run()


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(description="Run backend for Pegasus Simulator")

    parser.add_argument("--backend", type=str, required=True,
                        choices=["px4", "ardupilot", "ros2"],
                        help="Backend type to run")
    parser.add_argument("--server-host", type=str, required=True,
                        help="Windows host IP address")
    parser.add_argument("--server-port", type=int, default=5555,
                        help="Windows host port (default: 5555)")
    parser.add_argument("--vehicle-id", type=int, default=0,
                        help="Vehicle ID (default: 0)")
    parser.add_argument("--connection-type", type=str, default="tcpin",
                        help="MAVLink connection type (default: tcpin)")
    parser.add_argument("--connection-ip", type=str, default="localhost",
                        help="MAVLink connection IP (default: localhost)")
    parser.add_argument("--connection-baseport", type=int, default=4560,
                        help="MAVLink connection base port (default: 4560)")
    parser.add_argument("--num-rotors", type=int, default=4,
                        help="Number of rotors (default: 4)")
    parser.add_argument("--reconnect-interval", type=float, default=2.0,
                        help="Reconnection interval in seconds (default: 2.0)")
    parser.add_argument("--max-reconnect-attempts", type=int, default=10,
                        help="Maximum reconnection attempts (default: 10)")

    args = parser.parse_args()

    # Create backend based on type
    if args.backend == "px4":
        config = PX4MavlinkBackendConfig({
            "vehicle_id": args.vehicle_id,
            "connection_type": args.connection_type,
            "connection_ip": args.connection_ip,
            "connection_baseport": args.connection_baseport,
            "enable_lockstep": True,
            "num_rotors": args.num_rotors
        })
        backend = PX4MavlinkBackend(config)

    elif args.backend == "ardupilot":
        config = ArduPilotMavlinkBackendConfig({
            "vehicle_id": args.vehicle_id,
            "connection_type": args.connection_type,
            "connection_ip": args.connection_ip,
            "connection_baseport": args.connection_baseport,
            "enable_lockstep": True,
            "num_rotors": args.num_rotors
        })
        backend = ArduPilotMavlinkBackend(config)

    elif args.backend == "ros2":
        print("ROS2 backend not yet implemented in this runner")
        sys.exit(1)

    else:
        print(f"Unknown backend type: {args.backend}")
        sys.exit(1)

    # Create and run backend runner
    runner = BackendRunner(
        backend=backend,
        server_host=args.server_host,
        server_port=args.server_port,
        reconnect_interval=args.reconnect_interval,
        max_reconnect_attempts=args.max_reconnect_attempts
    )

    runner.run()


if __name__ == "__main__":
    main()
