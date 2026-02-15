"""Setup script for pegasus-simulator-linux package."""

from setuptools import setup, find_packages

setup(
    name="pegasus-simulator-linux",
    version="5.1.0",
    description="Pegasus Simulator Linux side - PX4/ROS2/MAVLink control backends",
    author="Pegasus Simulator Team",
    packages=find_packages(),
    install_requires=[
        "pegasus-simulator-common>=5.1.0",
        "numpy>=1.20.0",
        "scipy>=1.7.0",
        "pymavlink>=2.4.0",
        "msgpack>=1.0.0",
        # Optional: rclpy for ROS2 backend
    ],
    python_requires=">=3.7",
    entry_points={
        'console_scripts': [
            'pegasus-backend=pegasus.simulator.network.backend_runner:main',
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
