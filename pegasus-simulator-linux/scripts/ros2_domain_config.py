"""
ROS2 Domain Configuration Tool

This tool helps configure ROS2 domain ID and network settings for remote access.
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path


class ROS2DomainConfig:
    """ROS2 domain configuration manager."""

    def __init__(self):
        self.config_dir = Path.home() / ".ros2_network_config"
        self.env_script = self.config_dir / "setup_env.sh"

    def set_domain(self, domain_id: int, localhost_only: bool = False):
        """
        Set ROS2 domain ID and localhost configuration.

        Args:
            domain_id: Domain ID (0-101)
            localhost_only: If True, restrict to localhost only
        """
        if not 0 <= domain_id <= 101:
            raise ValueError("Domain ID must be between 0 and 101")

        # Set environment variables
        os.environ["ROS_DOMAIN_ID"] = str(domain_id)
        os.environ["ROS_LOCALHOST_ONLY"] = "1" if localhost_only else "0"

        print(f"ROS2 domain configured:")
        print(f"  Domain ID: {domain_id}")
        print(f"  Localhost only: {localhost_only}")

    def get_current_domain(self):
        """Get current ROS2 domain configuration."""
        domain_id = os.environ.get("ROS_DOMAIN_ID", "0")
        localhost_only = os.environ.get("ROS_LOCALHOST_ONLY", "0") == "1"

        return int(domain_id), localhost_only

    def list_nodes(self):
        """List all ROS2 nodes in the current domain."""
        try:
            result = subprocess.run(
                ["ros2", "node", "list"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                nodes = result.stdout.strip().split('\n')
                return [n for n in nodes if n]
            else:
                print(f"Error listing nodes: {result.stderr}")
                return []
        except subprocess.TimeoutExpired:
            print("Timeout while listing nodes")
            return []
        except FileNotFoundError:
            print("ROS2 not found. Please install ROS2 first.")
            return []

    def list_topics(self):
        """List all ROS2 topics in the current domain."""
        try:
            result = subprocess.run(
                ["ros2", "topic", "list"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                topics = result.stdout.strip().split('\n')
                return [t for t in topics if t]
            else:
                print(f"Error listing topics: {result.stderr}")
                return []
        except subprocess.TimeoutExpired:
            print("Timeout while listing topics")
            return []
        except FileNotFoundError:
            print("ROS2 not found. Please install ROS2 first.")
            return []

    def test_connectivity(self, remote_host: str = None):
        """
        Test ROS2 connectivity.

        Args:
            remote_host: Optional remote host to test connectivity with
        """
        print("Testing ROS2 connectivity...")
        print(f"Current domain: {self.get_current_domain()[0]}")
        print()

        print("Nodes in domain:")
        nodes = self.list_nodes()
        if nodes:
            for node in nodes:
                print(f"  - {node}")
        else:
            print("  No nodes found")
        print()

        print("Topics in domain:")
        topics = self.list_topics()
        if topics:
            for topic in topics:
                print(f"  - {topic}")
        else:
            print("  No topics found")
        print()

        if remote_host:
            print(f"Testing connectivity to {remote_host}...")
            # Ping test
            try:
                result = subprocess.run(
                    ["ping", "-c", "3", remote_host],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    print(f"  ✓ Can reach {remote_host}")
                else:
                    print(f"  ✗ Cannot reach {remote_host}")
            except Exception as e:
                print(f"  ✗ Error testing connectivity: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="ROS2 Domain Configuration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set domain ID to 5
  python ros2_domain_config.py --set-domain 5

  # Set domain ID and enable remote access
  python ros2_domain_config.py --set-domain 5 --remote

  # Get current domain configuration
  python ros2_domain_config.py --get-domain

  # List nodes and topics
  python ros2_domain_config.py --list-nodes
  python ros2_domain_config.py --list-topics

  # Test connectivity
  python ros2_domain_config.py --test
  python ros2_domain_config.py --test --remote-host 192.168.1.100
        """
    )

    parser.add_argument(
        "--set-domain",
        type=int,
        metavar="ID",
        help="Set ROS2 domain ID (0-101)"
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Enable remote access (disable localhost-only mode)"
    )
    parser.add_argument(
        "--get-domain",
        action="store_true",
        help="Get current domain configuration"
    )
    parser.add_argument(
        "--list-nodes",
        action="store_true",
        help="List all nodes in the current domain"
    )
    parser.add_argument(
        "--list-topics",
        action="store_true",
        help="List all topics in the current domain"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test ROS2 connectivity"
    )
    parser.add_argument(
        "--remote-host",
        type=str,
        metavar="HOST",
        help="Remote host to test connectivity with"
    )

    args = parser.parse_args()

    config = ROS2DomainConfig()

    # Set domain
    if args.set_domain is not None:
        localhost_only = not args.remote
        config.set_domain(args.set_domain, localhost_only)
        return

    # Get domain
    if args.get_domain:
        domain_id, localhost_only = config.get_current_domain()
        print(f"Current ROS2 domain configuration:")
        print(f"  Domain ID: {domain_id}")
        print(f"  Localhost only: {localhost_only}")
        return

    # List nodes
    if args.list_nodes:
        nodes = config.list_nodes()
        if nodes:
            print("ROS2 nodes:")
            for node in nodes:
                print(f"  - {node}")
        else:
            print("No ROS2 nodes found")
        return

    # List topics
    if args.list_topics:
        topics = config.list_topics()
        if topics:
            print("ROS2 topics:")
            for topic in topics:
                print(f"  - {topic}")
        else:
            print("No ROS2 topics found")
        return

    # Test connectivity
    if args.test:
        config.test_connectivity(args.remote_host)
        return

    # No arguments provided
    parser.print_help()


if __name__ == "__main__":
    main()
