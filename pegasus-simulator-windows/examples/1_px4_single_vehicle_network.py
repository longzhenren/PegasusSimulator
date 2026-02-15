"""
Example: Single vehicle with network backend for cross-host simulation.

This example demonstrates how to use the NetworkBackend to run Isaac Sim simulation
on Windows while the control backend (PX4/ArduPilot/ROS2) runs on a Linux machine.

Windows Side (this script):
- Runs Isaac Sim simulation
- Simulates vehicle physics and sensors
- Sends sensor data and state over network
- Receives control commands over network

Linux Side (run separately):
- Runs PX4/ArduPilot SITL or ROS2 backend
- Receives sensor data and state from Windows
- Sends control commands to Windows

Usage:
    1. Start this script on Windows (Isaac Sim side)
    2. Start the backend runner on Linux (e.g., run_px4_backend.py)
    3. The two will connect and communicate over TCP
"""

# Imports to start Isaac Sim
from isaacsim import SimulationApp

# Start Isaac Sim
simulation_app = SimulationApp({"headless": False})

# Import the rest after Isaac Sim is started
import numpy as np
from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface
from pegasus.simulator.logic.vehicles import Multirotor, MultirotorConfig
from pegasus.simulator.logic.sensors import Barometer, IMU, Magnetometer, GPS
from pegasus.simulator.logic.thrusters import QuadraticThrustCurve
from pegasus.simulator.logic.dynamics import LinearDrag
from pegasus.simulator.logic.network import NetworkBackend, NetworkBackendConfig

# Auxiliary scipy and numpy modules
import carb

class PegasusApp:
    """
    A Template class that serves as an example on how to build a simple Isaac Sim standalone App.
    """

    def __init__(self):
        """
        Method that initializes the PegasusApp and is used to setup the simulation environment.
        """

        # Acquire the Pegasus interface
        self.pg = PegasusInterface()

        # Acquire the World
        self.pg._world = self.pg.world
        self.world = self.pg.world

        # Launch one of the worlds (defined in the example config file)
        self.pg.load_environment("Hospital")

        # Create the vehicle configuration
        config_multirotor = MultirotorConfig()

        # Set the vehicle model
        config_multirotor.stage_prefix = "/World/quadrotor"

        # Configure the thrust curve
        config_multirotor.thrust_curve = QuadraticThrustCurve(
            rotor_constant=[5.84e-6, 5.84e-6, 5.84e-6, 5.84e-6],
            rolling_moment_coefficient=[1.0e-6, 1.0e-6, 1.0e-6, 1.0e-6],
            rot_dir=[-1, -1, 1, 1],
            min_rotor_velocity=0.0,
            max_rotor_velocity=1100.0
        )

        # Configure the drag
        config_multirotor.drag = LinearDrag([0.50, 0.30, 0.0])

        # Add sensors
        config_multirotor.sensors = [
            Barometer(),
            IMU(),
            Magnetometer(),
            GPS()
        ]

        # Create network backend configuration
        network_config = NetworkBackendConfig({
            "vehicle_id": 0,
            "server_host": "0.0.0.0",  # Listen on all interfaces
            "server_port": 5555,       # Default port
            "num_rotors": 4,
            "enable_graphical_sensors": False,  # Set to True to send camera/lidar data
            "heartbeat_interval": 1.0,
            "connection_timeout": 5.0
        })

        # Create network backend
        network_backend = NetworkBackend(network_config)

        # Add backend to vehicle config
        config_multirotor.backends = [network_backend]

        # Create the multirotor
        self.drone = Multirotor(
            "/World/quadrotor",
            config_multirotor
        )

        # Reset the simulation
        self.world.reset()

        # Auxiliar variable to handle the timeline
        self.stop_sim = False

    def run(self):
        """
        Method that implements the application main loop, where the physics steps are executed.
        """

        # Start the simulation
        self.world.reset()

        # The "infinite" loop
        while simulation_app.is_running() and not self.stop_sim:
            # Update the UI of the app and perform the physics step
            self.world.step(render=True)

        # Cleanup and stop
        carb.log_warn("PegasusApp Simulation App is closing.")
        self.world.stop()
        simulation_app.close()


def main():
    """
    Main function that runs the simulation.
    """

    # Instantiate the template app
    pg_app = PegasusApp()

    # Run the application loop
    pg_app.run()


if __name__ == "__main__":
    main()
