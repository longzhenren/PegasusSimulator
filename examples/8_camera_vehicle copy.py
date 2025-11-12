#!/usr/bin/env python
"""
| File: 8_camera_vehicle.py
| License: BSD-3-Clause. Copyright (c) 2024, Marcelo Jacinto. All rights reserved.
| Description: This files serves as an example on how to build an app that makes use of the Pegasus API, 
| where the data is send/received through mavlink, the vehicle is controled using mavlink and
| camera data is sent to ROS2 topics at the same time.
"""

# Imports to start Isaac Sim from this script
import carb
from isaacsim import SimulationApp

# Start Isaac Sim's simulation environment
# Note: this simulation app must be instantiated right after the SimulationApp import, otherwise the simulator will crash
# as this is the object that will load all the extensions and load the actual simulator.
simulation_app = SimulationApp({"headless": True})

# -----------------------------------
# The actual script should start here
# -----------------------------------
import omni.timeline
from omni.isaac.core.world import World

from omni.isaac.core.objects import DynamicCuboid
import numpy as np


import sys, os
import json
# 把 Pegasus 包所在的上级目录插到最前面
sys.path.insert(0, os.path.expanduser("~/PegasusSimulator/extensions/pegasus.simulator/"))


# Import the Pegasus API for simulating drones
from pegasus.simulator.params import ROBOTS, SIMULATION_ENVIRONMENTS
from pegasus.simulator.logic.graphical_sensors.monocular_camera import MonocularCamera
from pegasus.simulator.logic.graphical_sensors.lidar import Lidar
from pegasus.simulator.logic.backends.px4_mavlink_backend import PX4MavlinkBackend, PX4MavlinkBackendConfig
from pegasus.simulator.logic.backends.ros2_backend import ROS2Backend
from pegasus.simulator.logic.vehicles.multirotor import Multirotor, MultirotorConfig
from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface

# Auxiliary scipy and numpy modules
from scipy.spatial.transform import Rotation


# -------------------------
# Multi-UAV Config (JSON only)
# -------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "multi_uav_config.json")

def load_config_strict(path: str = CONFIG_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Multi-UAV config not found: {path}")
    with open(path, "r") as f:
        return json.load(f)


class CommStatusManager:
    """
    Deprecated lightweight communication manager (removed).
    """
    pass


class MultiUAVManager:
    """
    Read JSON config and spawn multiple multirotors with per-vehicle PX4/MAVROS/ROS2 settings.
    """
    def __init__(self, pg: PegasusInterface, world: World, config: dict):
        self.pg = pg
        self.world = world
        self.config = config
        self.vehicles = []

    def spawn(self):
        # env = self.config.get("environment", SIMULATION_ENVIRONMENTS.get("Curved Gridroom"))
        # self.pg.load_environment(env)
        self.pg.load_environment("/home/user/export/Demo_Environment.usda")

        for v in self.config.get("vehicles", []):
            vid = int(v.get("vehicle_id", 0))
            init_pos = v.get("initial_position", [0.0, 0.0, 0.07])
            euler = v.get("initial_orientation_euler_deg", [0.0, 0.0, 0.0])
            quat = Rotation.from_euler("XYZ", euler, degrees=True).as_quat()

            # PX4 backend config per vehicle (MAVROS handled externally)
            px4_cfg_dict = {
                "vehicle_id": vid,
                "px4_autolaunch": bool(v.get("px4_autolaunch", True)),
                "px4_dir": v.get("px4_dir", self.pg.px4_path),
                "px4_vehicle_model": getattr(self.pg, "px4_default_airframe", "gazebo-classic_iris"),
                # Use backend defaults for MAVLink connection (PX4 rcS offboard → UDP 14540+id)
            }
            mavlink_config = PX4MavlinkBackendConfig(px4_cfg_dict)

            # ROS2 backend (namespaced) for sensors/graphical
            ros2_ns = v.get("ros2_namespace", f"uav{vid}")
            ros2_backend = ROS2Backend(vehicle_id=vid, config={
                "namespace": ros2_ns,
                "pub_sensors": False,
                "pub_graphical_sensors": bool(v.get("publish_graphical_sensors", True)),
                "pub_state": True,
                "sub_control": False,
            })

            # No communication manager: rely on backend logs and external tools

            # Configure vehicle sensors
            camera = MonocularCamera(f"front_camera_{vid}", config={"update_rate": 60.0})
            config_multirotor = MultirotorConfig()
            config_multirotor.graphical_sensors = [camera]
            config_multirotor.backends = [PX4MavlinkBackend(mavlink_config), ros2_backend]

            # Spawn vehicle with consistent prim path namespace (/World/<ros2_ns>)
            prim_path = f"/World/{ros2_ns}"
            m = Multirotor(
                prim_path,
                ROBOTS['Iris'],
                vid,
                init_pos,
                quat,
                config=config_multirotor,
            )
            self.vehicles.append(m)

        # Reset simulation to initialize assets
        self.world.reset()

    # Communication status tracking removed for simplicity

class PegasusApp:
    """
    A Template class that serves as an example on how to build a simple Isaac Sim standalone App.
    """

    def __init__(self):
        """
        Method that initializes the PegasusApp and is used to setup the simulation environment.
        """

        # Acquire the timeline that will be used to start/stop the simulation
        self.timeline = omni.timeline.get_timeline_interface()

        # Start the Pegasus Interface
        self.pg = PegasusInterface()

        # Acquire the World, .i.e, the singleton that controls that is a one stop shop for setting up physics,
        # spawning asset primitives, etc.
        self.pg._world = World(**self.pg._world_settings)
        self.world = self.pg.world

        # Load environment and vehicles from local JSON config (strict, no defaults)
        cfg = load_config_strict()
        self.manager = MultiUAVManager(self.pg, self.world, cfg)
        self.manager.spawn()

        # cube_2 = self.world.scene.add(
        #     DynamicCuboid(
        #         prim_path="/new_cube_2",
        #         name="cube_1",
        #         position=np.array([-3.0, 0, 2.0]),
        #         scale=np.array([1.0, 1.0, 1.0]),
        #         size=1.0,
        #         color=np.array([255, 0, 0]),
        #     )
        # )

        # Initial communication status snapshot
        # (Removed) Communication status manager

        # Auxiliar variable for the timeline callback example
        self.stop_sim = False

    def run(self):
        """
        Method that implements the application main loop, where the physics steps are executed.
        """

        # Start the simulation
        self.timeline.play()

        # The "infinite" loop
        while simulation_app.is_running() and not self.stop_sim:
            # Update the UI of the app and perform the physics step
            self.world.step(render=True)
            # (Removed) Periodic communication status refresh

        # Cleanup and stop
        carb.log_warn("PegasusApp Simulation App is closing.")
        self.timeline.stop()
        simulation_app.close()

def main():

    # Instantiate the template app
    pg_app = PegasusApp()

    # Run the application loop
    pg_app.run()

if __name__ == "__main__":
    main()
