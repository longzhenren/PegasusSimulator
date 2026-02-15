"""Setup script for pegasus-simulator-common package."""

from setuptools import setup, find_packages

setup(
    name="pegasus-simulator-common",
    version="5.1.0",
    description="Common utilities and protocol definitions for Pegasus Simulator",
    author="Pegasus Simulator Team",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.20.0",
        "msgpack>=1.0.0",
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
