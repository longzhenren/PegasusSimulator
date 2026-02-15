"""
Environment Check Script for Pegasus Simulator - Windows Side

This script checks if the Windows environment is properly configured for running
Pegasus Simulator with Isaac Sim.
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
    version_str = f"{version.major}.{version.minor}.{version.minor}"

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
        ("msgpack", "msgpack"),
        ("pyyaml", "yaml"),
        ("toml", "toml"),
    ]

    all_installed = True
    for package_name, import_name in packages:
        if not check_package(package_name, import_name):
            all_installed = False

    if not all_installed:
        print_info("\nTo install missing packages:")
        print_info("  pip install numpy scipy msgpack pyyaml toml")

    return all_installed


def check_isaac_sim():
    """Check if Isaac Sim is installed."""
    print_header("Isaac Sim Check")

    # Check common Isaac Sim installation paths
    possible_paths = [
        Path(os.environ.get("ISAAC_SIM_PATH", "")),
        Path("C:/Users") / os.environ.get("USERNAME", "") / "AppData/Local/ov/pkg",
        Path("C:/Program Files/NVIDIA Omniverse/Isaac Sim"),
    ]

    isaac_found = False
    for path in possible_paths:
        if path.exists() and path.is_dir():
            print_success(f"Isaac Sim found at: {path}")
            isaac_found = True
            break

    if not isaac_found:
        print_error("Isaac Sim not found")
        print_info("Please install Isaac Sim from:")
        print_info("  https://developer.nvidia.com/isaac-sim")
        print_info("\nOr set ISAAC_SIM_PATH environment variable")
        return False

    return True


def check_network_config():
    """Check network configuration."""
    print_header("Network Configuration Check")

    # Check if port 5555 is available
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", 5555))
        sock.close()
        print_success("Port 5555 is available")
    except OSError:
        print_warning("Port 5555 is already in use")
        print_info("  This is OK if Pegasus Simulator is already running")

    # Check firewall (Windows)
    print_info("\nChecking Windows Firewall...")
    try:
        result = subprocess.run(
            ["netsh", "advfirewall", "firewall", "show", "rule", "name=all"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if "5555" in result.stdout:
            print_success("Firewall rule for port 5555 found")
        else:
            print_warning("No firewall rule for port 5555")
            print_info("  To add firewall rule, run as Administrator:")
            print_info('  netsh advfirewall firewall add rule name="Pegasus Simulator" dir=in action=allow protocol=TCP localport=5555')
    except Exception as e:
        print_warning(f"Could not check firewall: {e}")

    return True


def check_pegasus_installation():
    """Check Pegasus Simulator installation."""
    print_header("Pegasus Simulator Installation Check")

    # Check if pegasus-common is installed
    common_installed = check_package("pegasus-simulator-common", "pegasus.simulator.common")

    # Check if pegasus-simulator-windows is installed
    windows_installed = check_package("pegasus-simulator-windows", "pegasus.simulator.logic")

    if not common_installed or not windows_installed:
        print_info("\nTo install Pegasus Simulator:")
        if not common_installed:
            print_info("  cd pegasus-common && pip install -e .")
        if not windows_installed:
            print_info("  cd pegasus-simulator-windows && pip install -e .")

    return common_installed and windows_installed


def check_gpu():
    """Check GPU availability."""
    print_header("GPU Check")

    try:
        # Try to detect NVIDIA GPU
        result = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print_success("NVIDIA GPU detected")
            # Parse GPU info
            lines = result.stdout.split('\n')
            for line in lines:
                if 'NVIDIA' in line or 'GeForce' or 'RTX' in line:
                    print_info(f"  {line.strip()}")
            return True
        else:
            print_warning("nvidia-smi command failed")
            return False
    except FileNotFoundError:
        print_error("nvidia-smi not found (NVIDIA drivers not installed?)")
        return False
    except Exception as e:
        print_warning(f"Could not check GPU: {e}")
        return False


def main():
    """Main function."""
    print(f"\n{Colors.BOLD}Pegasus Simulator - Windows Environment Check{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*60}{Colors.RESET}\n")

    results = {
        "Python Version": check_python_version(),
        "Python Packages": check_python_packages(),
        "Isaac Sim": check_isaac_sim(),
        "Network Config": check_network_config(),
        "Pegasus Installation": check_pegasus_installation(),
        "GPU": check_gpu(),
    }

    # Summary
    print_header("Summary")

    passed = sum(results.values())
    total = len(results)

    for check, result in results.items():
        if result:
            print_success(f"{check}: PASS")
        else:
            print_error(f"{check}: FAIL")

    print(f"\n{Colors.BOLD}Result: {passed}/{total} checks passed{Colors.RESET}\n")

    if passed == total:
        print_success("Environment is ready for Pegasus Simulator!")
        return 0
    else:
        print_warning("Some checks failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
