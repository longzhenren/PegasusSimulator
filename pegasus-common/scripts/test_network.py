#!/usr/bin/env python3
"""
Network Connectivity Test Script for Pegasus Simulator

This script tests network connectivity between Windows and Linux hosts.
"""

import sys
import socket
import time
import argparse
from pathlib import Path


class Colors:
    """ANSI color codes."""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_header(text):
    """Print section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}\n")


def print_success(text):
    """Print success message."""
    print(f"{Colors.GREEN}✓{Colors.RESET} {text}")


def print_warning(text):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {text}")


def print_error(text):
    """Print error message."""
    print(f"{Colors.RED}✗{Colors.RESET} {text}")


def print_info(text):
    """Print info message."""
    print(f"{Colors.BLUE}ℹ{Colors.RESET} {text}")


def test_ping(host: str, count: int = 3) -> bool:
    """Test ping connectivity."""
    print_info(f"Pinging {host}...")

    import subprocess
    import platform

    # Determine ping command based on platform
    param = '-n' if platform.system().lower() == 'windows' else '-c'

    try:
        result = subprocess.run(
            ['ping', param, str(count), host],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            print_success(f"Can reach {host}")
            return True
        else:
            print_error(f"Cannot reach {host}")
            return False
    except subprocess.TimeoutExpired:
        print_error(f"Ping timeout to {host}")
        return False
    except Exception as e:
        print_error(f"Ping failed: {e}")
        return False


def test_port(host: str, port: int, timeout: float = 5.0) -> bool:
    """Test if a port is open."""
    print_info(f"Testing port {port} on {host}...")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()

        if result == 0:
            print_success(f"Port {port} is open on {host}")
            return True
        else:
            print_error(f"Port {port} is closed on {host}")
            return False
    except socket.timeout:
        print_error(f"Connection timeout to {host}:{port}")
        return False
    except Exception as e:
        print_error(f"Port test failed: {e}")
        return False


def test_tcp_connection(host: str, port: int, timeout: float = 5.0) -> bool:
    """Test TCP connection."""
    print_info(f"Testing TCP connection to {host}:{port}...")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))

        print_success(f"Successfully connected to {host}:{port}")

        # Try to send a test message
        test_msg = b"PING"
        sock.send(test_msg)
        print_info("Sent test message")

        sock.close()
        return True

    except socket.timeout:
        print_error(f"Connection timeout to {host}:{port}")
        return False
    except ConnectionRefusedError:
        print_error(f"Connection refused by {host}:{port}")
        print_info("  Make sure the server is running")
        return False
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return False


def test_latency(host: str, port: int, iterations: int = 10) -> dict:
    """Test network latency."""
    print_info(f"Testing latency to {host}:{port} ({iterations} iterations)...")

    latencies = []
    successful = 0

    for i in range(iterations):
        try:
            start_time = time.time()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))
            sock.close()

            latency = (time.time() - start_time) * 1000  # Convert to ms
            latencies.append(latency)
            successful += 1

        except Exception:
            pass

    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)

        print_success(f"Latency test completed:")
        print_info(f"  Successful: {successful}/{iterations}")
        print_info(f"  Average: {avg_latency:.2f} ms")
        print_info(f"  Min: {min_latency:.2f} ms")
        print_info(f"  Max: {max_latency:.2f} ms")

        return {
            'successful': successful,
            'total': iterations,
            'avg': avg_latency,
            'min': min_latency,
            'max': max_latency
        }
    else:
        print_error("All latency tests failed")
        return None


def test_bandwidth(host: str, port: int, data_size: int = 1024 * 1024) -> dict:
    """Test network bandwidth."""
    print_info(f"Testing bandwidth to {host}:{port}...")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((host, port))

        # Send test data
        test_data = b'X' * data_size
        start_time = time.time()
        sock.sendall(test_data)
        elapsed = time.time() - start_time

        sock.close()

        bandwidth = (data_size / elapsed) / (1024 * 1024)  # MB/s

        print_success(f"Bandwidth test completed:")
        print_info(f"  Data sent: {data_size / 1024:.2f} KB")
        print_info(f"  Time: {elapsed:.2f} s")
        print_info(f"  Bandwidth: {bandwidth:.2f} MB/s")

        return {
            'data_size': data_size,
            'elapsed': elapsed,
            'bandwidth': bandwidth
        }

    except Exception as e:
        print_error(f"Bandwidth test failed: {e}")
        return None


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Test network connectivity for Pegasus Simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test connectivity to Windows host
  python test_network.py --host 192.168.1.100

  # Test specific port
  python test_network.py --host 192.168.1.100 --port 5555

  # Run all tests including latency and bandwidth
  python test_network.py --host 192.168.1.100 --full

  # Test ROS2 ports
  python test_network.py --host 192.168.1.100 --ros2
        """
    )

    parser.add_argument(
        "--host",
        type=str,
        required=True,
        help="Remote host to test"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5555,
        help="Port to test (default: 5555)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full test suite (including latency and bandwidth)"
    )
    parser.add_argument(
        "--ros2",
        action="store_true",
        help="Test ROS2 DDS ports (7400-7500)"
    )
    parser.add_argument(
        "--latency-iterations",
        type=int,
        default=10,
        help="Number of latency test iterations (default: 10)"
    )

    args = parser.parse_args()

    print(f"\n{Colors.BOLD}Pegasus Simulator - Network Connectivity Test{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*60}{Colors.RESET}\n")

    results = {}

    # Basic connectivity test
    print_header("Basic Connectivity Test")
    results['ping'] = test_ping(args.host)

    # Port test
    print_header(f"Port Test (Port {args.port})")
    results['port'] = test_port(args.host, args.port)

    # TCP connection test
    if results['port']:
        print_header("TCP Connection Test")
        results['tcp'] = test_tcp_connection(args.host, args.port)

    # ROS2 ports test
    if args.ros2:
        print_header("ROS2 DDS Ports Test")
        ros2_results = []
        test_ports = [7400, 7410, 7420, 7430]  # Sample ports
        for port in test_ports:
            result = test_port(args.host, port, timeout=2.0)
            ros2_results.append(result)
        results['ros2'] = any(ros2_results)

    # Full test suite
    if args.full and results.get('port'):
        print_header("Latency Test")
        latency_result = test_latency(args.host, args.port, args.latency_iterations)
        results['latency'] = latency_result is not None

        # Note: Bandwidth test requires server support
        print_header("Bandwidth Test")
        print_warning("Bandwidth test requires server support (skipped)")

    # Summary
    print_header("Summary")

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test, result in results.items():
        if result:
            print_success(f"{test.upper()}: PASS")
        else:
            print_error(f"{test.upper()}: FAIL")

    print(f"\n{Colors.BOLD}Result: {passed}/{total} tests passed{Colors.RESET}\n")

    if passed == total:
        print_success("Network connectivity is good!")
        return 0
    else:
        print_warning("Some tests failed. Check the details above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
