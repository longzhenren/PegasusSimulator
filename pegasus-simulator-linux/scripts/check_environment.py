#!/usr/bin/env python3
"""
Environment Check Script for Pegasus Simulator - Linux Side

This script checks if the Linux environment is properly configured for running
Pegasus Simulator backends (PX4/ArduPilot/ROS2).
"""

import sys
import os
import subprocess
import importlib.util
from pathlib import Path


class Colors:
    """ANSI color codes for terminal output."""
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


def check_python_version():
    """Check Python version."""
    print_header("Python Version Check")

    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"

    if version.major == 3 and version.minor >= 7:
        print_success(f"Python version: {version_str}")
        return True
    else:
        print_error(f"Python version: {version_str} (requires Python 3.7+)")
        return False


def check_package(package_name, import_name=None):
    """Check if a Python package is installed."""
    if import_name is None:
        import_name = package_name

    spec = importlib.util.find_spec(import_name)
    if spec is not None:
        print_success(f"{package_name} is installed")
        return True
    else:
        print_error(f"{package_name} is NOT installed")
        return False


def check_python_packages():
    """Check required Python packages."""
    print_header("Python Packages Check")

    packages = [
        ("numpy", "numpy"),
        ("scipy", "scipy"),
        ("pymavlink", "pymavlink"),
        ("msgpack", "msgpack"),
    ]

    all_installed = True
    for package_name, import_name in packages:
        if not check_package(package_name, import_name):
            all_installed = False

    if not all_installed:
        print_info("\nTo install missing packages:")
        print_info("  pip install numpy scipy pymavlink msgpack")

    return all_installed


def check_ros2():
    """Check if ROS2 is installed."""
    print_header("ROS2 Check")

    # Check if ros2 command is available
    try:
        result = subprocess.run(
            ["ros2", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            version = result.stdout.strip()
            print_success(f"ROS2 installed: {version}")

            # Check ROS_DISTRO
            ros_distro = os.environ.get("ROS_DISTRO")
            if ros_distro:
                print_success(f"ROS_DISTRO: {ros_distro}")
            else:
                print_warning("ROS_DISTRO not set")
                print_info("  Source ROS2 setup: source /opt/ros/<distro>/setup.bash")

            return True
        else:
            print_error("ROS2 command failed")
            return False
    except FileNotFoundError:
        print_warning("ROS2 not found (optional)")
        print_info("  ROS2 is only required if using ROS2Backend")
        print_info("  Install from: https://docs.ros.org/")
        return False
    except Exception as e:
        print_warning(f"Could not check ROS2: {e}")
        return False


def check_px4():
    """Check if PX4 is installed."""
    print_header("PX4 Check")

    # Check common PX4 installation paths
    px4_dir = os.environ.get("PX4_DIR")
    if px4_dir:
        px4_path = Path(px4_dir)
    else:
        px4_path = Path.home() / "PX4-Autopilot"

    if px4_path.exists() and px4_path.is_dir():
        print_success(f"PX4 found at: {px4_path}")

        # Check if PX4 is built
        build_dir = px4_path / "build"
        if build_dir.exists():
            print_success("PX4 is built")
        else:
            print_warning("PX4 is not built")
            print_info(f"  cd {px4_path} && make px4_sitl_default")

        return True
    else:
        print_warning("PX4 not found (optional)")
        print_info("  PX4 is only required if using PX4MavlinkBackend")
        print_info("  Clone from: https://github.com/PX4/PX4-Autopilot")
        return False


def check_ardupilot():
    """Check if ArduPilot is installed."""
    print_header("ArduPilot Check")

    # Check common ArduPilot installation paths
    ardupilot_dir = os.environ.get("ARDUPILOT_DIR")
    if ardupilot_dir:
        ardupilot_path = Path(ardupilot_dir)
    else:
        ardupilot_path = Path.home() / "ardupilot"

    if ardupilot_path.exists() and ardupilot_path.is_dir():
        print_success(f"ArduPilot found at: {ardupilot_path}")

        # Check if ArduPilot is built
        build_dir = ardupilot_path / "build"
        if build_dir.exists():
            print_success("ArduPilot is built")
        else:
            print_warning("ArduPilot is not built")
            print_info(f"  cd {ardupilot_path}/ArduCopter && sim_vehicle.py -w")

        return True
    else:
        print_warning("ArduPilot not found (optional)")
        print_info("  ArduPilot is only required if using ArduPilotMavlinkBackend")
        print_info("  Clone from: https://github.com/ArduPilot/ardupilot")
        return False


def check_network_config():
    """Check network configuration."""
    print_header("Network Configuration Check")

    # Check if can create socket
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.close()
        print_success("Socket creation successful")
    except Exception as e:
        print_error(f"Socket creation failed: {e}")
        return False

    # Check firewall
    print_info("\nChecking firewall...")

    # Check ufw
    try:
        result = subprocess.run(
            ["sudo", "ufw", "status"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            if "Status: active" in result.stdout:
                print_warning("UFW firewall is active")
                if "5555" in result.stdout:
                    print_success("Port 5555 is allowed")
                else:
                    print_warning("Port 5555 is not explicitly allowed")
                    print_info("  sudo ufw allow 5555/tcp")
            else:
                print_info("UFW firewall is inactive")
    except FileNotFoundError:
        print_info("UFW not found, checking firewalld...")

        # Check firewalld
        try:
            result = subprocess.run(
                ["sudo", "firewall-cmd", "--state"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and "running" in result.stdout:
                print_warning("firewalld is running")
                print_info("  Check ports: sudo firewall-cmd --list-all")
        except FileNotFoundError:
            print_info("No firewall detected")
    except Exception as e:
        print_info(f"Could not check firewall: {e}")

    return True


def check_pegasus_installation():
    """Check Pegasus Simulator installation."""
    print_header("Pegasus Simulator Installation Check")

    # Check if pegasus-common is installed
    common_installed = check_package("pegasus-simulator-common", "pegasus.simulator.common")

    # Check if pegasus-simulator-linux is installed
    linux_installed = check_package("pegasus-simulator-linux", "pegasus.simulator.backends")

    if not common_installed or not linux_installed:
        print_info("\nTo install Pegasus Simulator:")
        if not common_installed:
            print_info("  cd pegasus-common && pip install -e .")
        if not linux_installed:
            print_info("  cd pegasus-simulator-linux && pip install -e .")

    return common_installed and linux_installed


def check_network_connectivity(windows_host=None):
    """Check network connectivity to Windows host."""
    print_header("Network Connectivity Check")

    if not windows_host:
        print_info("No Windows host specified, skipping connectivity test")
        print_info("  Run with: python check_environment.py --windows-host <IP>")
        return True

    print_info(f"Testing connectivity to Windows host: {windows_host}")

    # Ping test
    try:
        result = subprocess.run(
            ["ping", "-c", "3", windows_host],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print_success(f"Can reach {windows_host}")
        else:
            print_error(f"Cannot reach {windows_host}")
            return False
    except Exception as e:
        print_error(f"Ping failed: {e}")
        return False

    # Port test
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((windows_host, 5555))
        sock.close()

        if result == 0:
            print_success(f"Port 5555 is open on {windows_host}")
        else:
            print_warning(f"Port 5555 is not accessible on {windows_host}")
            print_info("  Make sure Pegasus Simulator is running on Windows")
            print_info("  Check Windows firewall settings")
    except Exception as e:
        print_warning(f"Port test failed: {e}")

    return True


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Check Linux environment for Pegasus Simulator")
    parser.add_argument("--windows-host", type=str, help="Windows host IP for connectivity test")
    args = parser.parse_args()

    print(f"\n{Colors.BOLD}Pegasus Simulator - Linux Environment Check{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*60}{Colors.RESET}\n")

    results = {
        "Python Version": check_python_version(),
        "Python Packages": check_python_packages(),
        "ROS2": check_ros2(),
        "PX4": check_px4(),
        "ArduPilot": check_ardupilot(),
        "Network Config": check_network_config(),
        "Pegasus Installation": check_pegasus_installation(),
    }

    if args.windows_host:
        results["Network Connectivity"] = check_network_connectivity(args.windows_host)

    # Summary
    print_header("Summary")

    # Count only required checks (exclude optional ones)
    required_checks = ["Python Version", "Python Packages", "Network Config", "Pegasus Installation"]
    if args.windows_host:
        required_checks.append("Network Connectivity")

    passed = sum(results[check] for check in required_checks if check in results)
    total = len(required_checks)

    for check, result in results.items():
        is_required = check in required_checks
        status = "PASS" if result else "FAIL"
        optional = "" if is_required else " (optional)"

        if result:
            print_success(f"{check}: {status}{optional}")
        elif is_required:
            print_error(f"{check}: {status}")
        else:
            print_warning(f"{check}: {status}{optional}")

    print(f"\n{Colors.BOLD}Result: {passed}/{total} required checks passed{Colors.RESET}\n")

    if passed == total:
        print_success("Environment is ready for Pegasus Simulator!")
        return 0
    else:
        print_warning("Some required checks failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
