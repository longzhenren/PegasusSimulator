"""Setup script for pegasus-simulator-windows package."""

from setuptools import setup, find_packages

setup(
    name="pegasus-simulator-windows",
    version="5.1.0",
    description="Pegasus Simulator Windows side - Isaac Sim simulation environment",
    author="Pegasus Simulator Team",
    packages=find_packages(),
    install_requires=[
        "pegasus-simulator-common>=5.1.0",
        "numpy>=1.20.0",
        "scipy>=1.7.0",
        "msgpack>=1.0.0",
        "pyyaml>=5.4.0",
        "toml>=0.10.0",
        # Isaac Sim dependencies are provided by Isaac Sim installation
    ],
    python_requires=">=3.7",
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
