"""
| File: px4_launch_tool.py
| Author: Marcelo Jacinto (marcelo.jacinto@tecnico.ulisboa.pt)
| Description: Defines an auxiliary tool to launch the PX4 process in the background
| License: BSD-3-Clause. Copyright (c) 2023, Marcelo Jacinto. All rights reserved.
"""

# System tools used to launch the px4 process in the brackground
import os
import tempfile
import subprocess
import traceback


PX4_PID_DIR = "/tmp/pegasus_px4_sitl"
try:
    os.makedirs(PX4_PID_DIR, exist_ok=True)
except Exception as e:
    print(f"[PX4LaunchTool] Create PID dir failed: {e}")
    print(traceback.format_exc())


class PX4LaunchTool:
    """
    A class that manages the start/stop of a px4 process. It requires only the path to the PX4 installation (assuming that
    PX4 was already built with 'make px4_sitl_default none'), the vehicle id and the vehicle model. 
    """

    def __init__(self, px4_dir, vehicle_id: int = 0, px4_model: str = "gazebo-classic_iris", sim_speed_factor: float = 1.0):
        """Construct the PX4LaunchTool object

        Args:
            px4_dir (str): A string with the path to the PX4-Autopilot directory
            vehicle_id (int): The ID of the vehicle. Defaults to 0.
            px4_model (str): The vehicle model. Defaults to "gazebo-classic_iris".
            sim_speed_factor (float): The speed factor for the simulation. Defaults to 1.0.
        """

        print(f"[PX4LaunchTool] vehicle_id={vehicle_id}")
        # Attribute that will hold the px4 process once it is running
        self.px4_process = None

        # The vehicle id (used for the mavlink port open in the system)
        self.vehicle_id = vehicle_id

        # Configurations to whether autostart px4 (SITL) automatically or have the user launch it manually on another
        # terminal
        self.px4_dir = px4_dir
        # self.rc_script = self.px4_dir + "/ROMFS/px4fmu_common/init.d-posix/rcS"
        self.rc_script = "/home/user/PegasusSimulator-5.1/examples/px4/rcS_minmal"

        # Create a temporary filesystem for px4 to write data to/from (and modify the origin rcS files)
        self.root_fs = tempfile.TemporaryDirectory(prefix=f"px4_{self.vehicle_id}_")

        # Set the environment variables that let PX4 know which vehicle model to use internally
        self.environment = os.environ
        self.environment["PX4_SIM_MODEL"] = px4_model
        # Unique UXRCE-DDS port per instance
        self.environment["PX4_UXRCE_DDS_PORT"] = str(8888 + int(self.vehicle_id))
        # Namespace per instance (value only, rcS adds flags when invoking client)
        self.environment["PX4_UXRCE_DDS_NS"] = f"px4_{int(self.vehicle_id) + 1}"
        
        # Set the simulation speed factor
        self.environment["PX4_SIM_SPEED_FACTOR"] = str(sim_speed_factor)

    @staticmethod
    def pid_file_path(vehicle_id: int):
        return os.path.join(PX4_PID_DIR, f"px4_{vehicle_id}.pid")

    def launch_px4(self):
        """
        Method that will launch a px4 instance with the specified configuration
        """
        self.px4_process = subprocess.Popen(
            [
                self.px4_dir + "/build/px4_sitl_default/bin/px4",
                self.px4_dir + "/ROMFS/px4fmu_common/",
                "-s",
                self.rc_script,
                "-i",
                str(self.vehicle_id),
                "-d",
            ],
            cwd=self.root_fs.name,
            shell=False,
            env=self.environment,
        )
        try:
            pid = int(self.px4_process.pid)
            with open(self.pid_file_path(self.vehicle_id), "w") as f:
                f.write(str(pid))
        except Exception as e:
            print(f"[PX4LaunchTool] Write PID failed: {e}")
            print(traceback.format_exc())

    def kill_px4(self):
        """
        Method that will kill a px4 instance with the specified configuration
        """
        if self.px4_process is not None:
            self.px4_process.kill()
            self.px4_process = None
        try:
            p = self.pid_file_path(self.vehicle_id)
            if os.path.exists(p):
                os.remove(p)
        except Exception as e:
            print(f"[PX4LaunchTool] Remove PID file failed: {e}")
            print(traceback.format_exc())

    def __del__(self):
        """
        If the px4 process is still running when the PX4 launch tool object is whiped from memory, then make sure
        we kill the px4 instance so we don't end up with hanged px4 instances
        """

        # Make sure the PX4 process gets killed
        if self.px4_process:
            self.kill_px4()

        # Make sure we clean the temporary filesystem used for the simulation
        self.root_fs.cleanup()


# ---- Code used for debugging the px4 tool ----
def main():

    px4_tool = PX4LaunchTool(os.environ["HOME"] + "/PX4-Autopilot")
    px4_tool.launch_px4()
    
    import time

    time.sleep(20)
    px4_tool.kill_px4()
    # MAVROS managed externally; nothing to kill here


if __name__ == "__main__":
    main()
