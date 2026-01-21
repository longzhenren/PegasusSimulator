#!/usr/bin/env python
# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
| File: fast_controller.py
| Author: Updated based on controller.md
| License: BSD-3-Clause. Copyright (c) 2023, Marcelo Jacinto. All rights reserved.
| Description: Fast attitude controller for quadrotor UAVs based on cascaded PID control.
This controller implements the control architecture described in controller.md:
- Outer loop: Attitude PD control
- Inner loop: Angular velocity PID control
Only UAV control is implemented (no gripper/manipulator control).
"""

# Imports to be able to log to the terminal with fancy colors
import carb

# Imports from the Pegasus library
from pegasus.simulator.logic.state import State
from pegasus.simulator.logic.backends import Backend

# Auxiliary scipy and numpy modules
import numpy as np
from scipy.spatial.transform import Rotation
import json
import csv
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

@dataclass
class TrajectoryPoint:
    """Trajectory point data class compatible with trajectory_data_collector.py"""
    x: float
    y: float
    z: float
    roll_deg: float
    yaw_deg: float
    pitch_deg: float


class FastController(Backend):
    """A fast attitude controller for quadrotors. It implements a cascaded PID control
    architecture with:
    - Outer loop: Position PID control -> desired acceleration
    - Middle loop: Attitude control (geometric) -> desired body rates
    - Inner loop: Angular velocity PID control -> torques

    Based on the control architecture described in controller.md
    """

    def __init__(self,
        trajectory_file: str = None,
        results_file: str = None,
        reverse=False,
        # Position control gains (outer loop)
        Kp=[10.0, 10.0, 10.0],
        Kd=[8.5, 8.5, 8.5],
        Ki=[1.50, 1.50, 1.50],
        # Attitude control gains (from controller.md)
        KAng=[15.0, 15.0, 12.0],
        Kdang=[0.0, 0.0, 0.0],
        # Angular velocity control gains (from controller.md)
        KAng_vel=[0.2, 0.15, 0.32],
        KiAng_vel=[0.2, 0.2, 0.1],
        KiAng_vel_max=[1.0, 0.7, 0.5],
        # Attitude angle limits (safety constraints)
        max_roll_deg=45.0,
        max_pitch_deg=45.0,
        max_yaw_rate_deg=90.0,
        # Trajectory scale and coordinate transform (compatible with data collector)
        scale=0.01,
        z_down=True,
        uav_id=0):

        # The current rotor references [rad/s]
        self.input_ref = [0.0, 0.0, 0.0, 0.0]

        # The current state of the vehicle expressed in the inertial frame (in ENU)
        self.p = np.zeros((3,))                   # The vehicle position
        self.R: Rotation = Rotation.identity()    # The vehicle attitude
        self.w = np.zeros((3,))                   # The angular velocity of the vehicle
        self.v = np.zeros((3,))                   # The linear velocity of the vehicle in the inertial frame
        self.a = np.zeros((3,))                   # The linear acceleration of the vehicle in the inertial frame

        # Define the control gains for position control (outer loop)
        self.Kp = np.diag(Kp)
        self.Kd = np.diag(Kd)
        self.Ki = np.diag(Ki)
        self.int_pos = np.array([0.0, 0.0, 0.0])  # Position error integral

        # Define the attitude control gains (from controller.md)
        self.KAng = np.diag(KAng)           # Attitude proportional gain
        self.Kdang = np.diag(Kdang)         # Attitude derivative gain

        # Define the angular velocity control gains (from controller.md)
        self.KAng_vel = np.diag(KAng_vel)         # Angular velocity proportional gain
        self.KiAng_vel = np.diag(KiAng_vel)       # Angular velocity integral gain
        self.KiAng_vel_max = np.array(KiAng_vel_max)  # Integral saturation limits

        # Integral error for angular velocity
        self.integrated_ang_vel_error = np.array([0.0, 0.0, 0.0])

        # Previous angular velocity for derivative calculation
        self.prev_ang_vel_b = np.array([0.0, 0.0, 0.0])

        # Define the dynamic parameters for the vehicle (from controller.md)
        self.m = 10.0       # Mass in Kg (updated from 1.50 to 10.0)
        self.g = 9.81       # The gravity acceleration ms^-2

        # Inertia matrix (from controller.md)
        # J = diag([7.0, 5.0, 9.0]) × 10^-3 kg·m²
        self.J = np.diag([7.0e-3, 5.0e-3, 9.0e-3])

        # Attitude angle limits (safety constraints in radians)
        self.max_roll_rad = np.deg2rad(float(max_roll_deg))
        self.max_pitch_rad = np.deg2rad(float(max_pitch_deg))
        self.max_yaw_rate_rad = np.deg2rad(float(max_yaw_rate_deg))

        carb.log_info(f"FastController attitude limits: roll=±{max_roll_deg}°, pitch=±{max_pitch_deg}°, yaw_rate=±{max_yaw_rate_deg}°/s")

        # Trajectory parameters (compatible with data collector)
        self.scale = float(scale)
        self.z_down = bool(z_down)
        self.uav_id = int(uav_id)

        # Read the target trajectory from a file
        # Supports both CSV and JSON formats (JSON format is compatible with trajectory_data_collector.py)
        self.index = 0
        self.trajectory_file_path = trajectory_file
        self.trajectory_name = ""

        if trajectory_file is not None:
            trajectory_path = Path(trajectory_file)
            self.trajectory_name = trajectory_path.stem

            if trajectory_path.suffix.lower() == '.json':
                # Load JSON trajectory (compatible with trajectory_data_collector.py)
                self.trajectory = self.read_trajectory_from_json(trajectory_file)
                self.max_index = len(self.trajectory)
                self.total_time = 0.0
            elif trajectory_path.suffix.lower() == '.csv':
                # Load CSV trajectory (legacy format)
                self.trajectory = self.read_trajectory_from_csv(trajectory_file)
                self.max_index, _ = self.trajectory.shape
                self.total_time = 0.0
            else:
                raise ValueError(f"Unsupported trajectory file format: {trajectory_path.suffix}")
        # Use the built-in trajectory hard-coded for this controller
        else:
            # Set the initial time for starting when using the built-in trajectory (the time is also used in this case
            # as the parametric value)
            self.total_time = -5.0
            # Signal that we will not used a received trajectory
            self.trajectory = None
            self.max_index = 1

        self.reverse = reverse

        # Auxiliar variable, so that we only start sending motor commands once we get the state of the vehicle
        self.reveived_first_state = False

        # Lists used for analysing performance statistics
        # Compatible with trajectory_data_collector.py output format
        self.results_files = results_file
        self.time_vector = []
        self.desired_position_over_time = []
        self.position_over_time = []
        self.position_error_over_time = []
        self.velocity_error_over_time = []
        self.atittude_error_over_time = []
        self.attitude_rate_error_over_time = []

        # Additional statistics compatible with data collector format
        self.desired_attitude_over_time = []
        self.attitude_over_time = []
        self.angular_velocity_over_time = []
        self.linear_acceleration_over_time = []
        self.thrust_magnitude_over_time = []
        self.torque_over_time = []

        self.dt = 0.01  # Default timestep, will be updated

    def read_trajectory_from_json(self, file_name: str) -> List[TrajectoryPoint]:
        """Read trajectory from JSON file (compatible with trajectory_data_collector.py)

        JSON format:
        {
          "raw_logs": [[x, y, z, roll, yaw, pitch], ...],  // For reset
          "preprocessed_logs": [[x, y, z, roll, yaw, pitch], ...]  // Trajectory
        }

        Args:
            file_name (str): Path to the JSON trajectory file

        Returns:
            List[TrajectoryPoint]: A list of trajectory points with scaled coordinates
        """
        json_path = Path(file_name)
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Load preprocessed trajectory points
        preprocessed_logs = data.get("preprocessed_logs", [])
        if not preprocessed_logs:
            raise ValueError(f"No preprocessed_logs found in {file_name}")

        # Load initial point for coordinate transformation
        raw_logs = data.get("raw_logs", [])
        if not raw_logs or len(raw_logs[0]) < 6:
            raise ValueError(f"Invalid raw_logs in {file_name}")

        init_point = raw_logs[0]
        base_x, base_y, base_z = init_point[0], init_point[1], init_point[2]

        # Convert to TrajectoryPoint objects with coordinate transformation
        # ENU to NEU: swap x and y (compatible with trajectory_data_collector.py)
        trajectory_points = []
        for i, point in enumerate(preprocessed_logs):
            if len(point) < 6:
                raise ValueError(f"Invalid point at index {i} in preprocessed_logs")

            # Apply ENU to NEU transformation (swap x and y)
            x_enu, y_enu, z_enu = point[0], point[1], point[2]
            x_neu, y_neu = y_enu, x_enu  # Swap x and y

            # Apply scale and base offset
            x = base_x * self.scale + x_neu * self.scale
            y = base_y * self.scale + y_neu * self.scale
            if self.z_down:
                z = base_z * self.scale - z_enu * self.scale
            else:
                z = base_z * self.scale + z_enu * self.scale

            roll_deg, yaw_deg, pitch_deg = point[3], point[4], point[5]

            trajectory_points.append(TrajectoryPoint(
                x=x, y=y, z=z,
                roll_deg=roll_deg,
                yaw_deg=yaw_deg,
                pitch_deg=pitch_deg
            ))

        carb.log_info(f"Loaded {len(trajectory_points)} points from JSON trajectory: {file_name}")
        return trajectory_points

    def read_trajectory_from_csv(self, file_name: str):
        """Auxiliar method used to read the desired trajectory from a CSV file (legacy format)

        Args:
            file_name (str): A string with the name of the trajectory inside the trajectories directory

        Returns:
            np.ndarray: A numpy matrix with the trajectory desired states over time
        """

        # Read the trajectory to a pandas frame
        return np.flip(np.genfromtxt(file_name, delimiter=','), axis=0)


    def start(self):
        """
        Reset the control and trajectory index
        """
        self.reset_statistics()


    def stop(self):
        """
        Stopping the controller. Saving the statistics data in multiple formats:
        1. NPZ file (numpy arrays for plotting)
        2. CSV files compatible with trajectory_data_collector.py output format
        """

        # Check if we should save the statistics to some file or not
        if self.results_files is None:
            return

        results_path = Path(self.results_files)
        results_dir = results_path.parent
        results_dir.mkdir(parents=True, exist_ok=True)

        # Save NPZ format (legacy format for plotting)
        statistics = {}
        statistics["time"] = np.array(self.time_vector)
        statistics["p"] = np.vstack(self.position_over_time) if self.position_over_time else np.array([])
        statistics["desired_p"] = np.vstack(self.desired_position_over_time) if self.desired_position_over_time else np.array([])
        statistics["ep"] = np.vstack(self.position_error_over_time) if self.position_error_over_time else np.array([])
        statistics["ev"] = np.vstack(self.velocity_error_over_time) if self.velocity_error_over_time else np.array([])
        statistics["er"] = np.vstack(self.atittude_error_over_time) if self.atittude_error_over_time else np.array([])
        statistics["ew"] = np.vstack(self.attitude_rate_error_over_time) if self.attitude_rate_error_over_time else np.array([])

        # Additional data
        if self.attitude_over_time:
            statistics["attitude"] = np.vstack(self.attitude_over_time)
        if self.angular_velocity_over_time:
            statistics["angular_velocity"] = np.vstack(self.angular_velocity_over_time)
        if self.linear_acceleration_over_time:
            statistics["linear_acceleration"] = np.vstack(self.linear_acceleration_over_time)
        if self.thrust_magnitude_over_time:
            statistics["thrust_magnitude"] = np.array(self.thrust_magnitude_over_time)
        if self.torque_over_time:
            statistics["torque"] = np.vstack(self.torque_over_time)

        np.savez(self.results_files, **statistics)
        carb.log_warn("Statistics saved to NPZ: " + self.results_files)

        # Save CSV format compatible with trajectory_data_collector.py
        self._save_csv_statistics(results_dir)

        self.reset_statistics()

    def _save_csv_statistics(self, output_dir: Path):
        """Save statistics in CSV format compatible with trajectory_data_collector.py

        Generates two CSV files:
        1. data.csv - Main data file with all measurements
        2. all_pose_data.csv - Simplified pose data
        """
        if not self.time_vector:
            return

        # Prepare data rows
        data_rows: List[Dict[str, Any]] = []
        pose_rows: List[Dict[str, Any]] = []

        for i in range(len(self.time_vector)):
            # Get data at this timestep
            time = self.time_vector[i]
            pos = self.position_over_time[i] if i < len(self.position_over_time) else [0, 0, 0]
            des_pos = self.desired_position_over_time[i] if i < len(self.desired_position_over_time) else [0, 0, 0]
            pos_err = self.position_error_over_time[i] if i < len(self.position_error_over_time) else [0, 0, 0]
            vel_err = self.velocity_error_over_time[i] if i < len(self.velocity_error_over_time) else [0, 0, 0]
            att_err = self.atittude_error_over_time[i] if i < len(self.atittude_error_over_time) else [0, 0, 0]
            att_rate_err = self.attitude_rate_error_over_time[i] if i < len(self.attitude_rate_error_over_time) else [0, 0, 0]

            # Get attitude quaternion if available
            if i < len(self.attitude_over_time):
                att_quat = self.attitude_over_time[i]
            else:
                att_quat = [1, 0, 0, 0]  # w, x, y, z

            # Get angular velocity if available
            if i < len(self.angular_velocity_over_time):
                ang_vel = self.angular_velocity_over_time[i]
            else:
                ang_vel = [0, 0, 0]

            # Get linear acceleration if available
            if i < len(self.linear_acceleration_over_time):
                lin_acc = self.linear_acceleration_over_time[i]
            else:
                lin_acc = [0, 0, 0]

            # Get desired attitude if available
            if i < len(self.desired_attitude_over_time):
                des_att = self.desired_attitude_over_time[i]
            else:
                des_att = {"roll": 0, "pitch": 0, "yaw": 0}

            # data.csv row (compatible with trajectory_data_collector.py)
            data_row = {
                "traj_json": self.trajectory_file_path if self.trajectory_file_path else "",
                "traj_name": self.trajectory_name,
                "uav_id": self.uav_id,
                "step_idx": i,
                "time_s": time,

                # Commanded position and attitude
                "cmd_x": des_pos[0],
                "cmd_y": des_pos[1],
                "cmd_z": des_pos[2],
                "cmd_roll_deg": des_att.get("roll", 0),
                "cmd_yaw_deg": des_att.get("yaw", 0),
                "cmd_pitch_deg": des_att.get("pitch", 0),

                # Observed position (obs_pos)
                "obs_pos_x": pos[0],
                "obs_pos_y": pos[1],
                "obs_pos_z": pos[2],

                # Observed attitude quaternion (obs_att) - w, x, y, z
                "obs_att_w": att_quat[0],
                "obs_att_x": att_quat[1],
                "obs_att_y": att_quat[2],
                "obs_att_z": att_quat[3],

                # Observed velocities
                "obs_linvel_x": vel_err[0],  # Note: currently storing velocity error
                "obs_linvel_y": vel_err[1],
                "obs_linvel_z": vel_err[2],

                # Observed angular velocity
                "obs_angvel_x": ang_vel[0],
                "obs_angvel_y": ang_vel[1],
                "obs_angvel_z": ang_vel[2],

                # Observed linear acceleration
                "obs_linacc_x": lin_acc[0],
                "obs_linacc_y": lin_acc[1],
                "obs_linacc_z": lin_acc[2],

                # Errors
                "pos_error_x": pos_err[0],
                "pos_error_y": pos_err[1],
                "pos_error_z": pos_err[2],
                "att_error_x": att_err[0],
                "att_error_y": att_err[1],
                "att_error_z": att_err[2],
            }

            # all_pose_data.csv row (simplified)
            pose_row = {
                "traj_json": self.trajectory_file_path if self.trajectory_file_path else "",
                "traj_name": self.trajectory_name,
                "uav_id": self.uav_id,
                "step_idx": i,
                "time_s": time,

                # Position
                "pos_x": pos[0],
                "pos_y": pos[1],
                "pos_z": pos[2],

                # Commanded attitude
                "cmd_roll_deg": des_att.get("roll", 0),
                "cmd_yaw_deg": des_att.get("yaw", 0),
                "cmd_pitch_deg": des_att.get("pitch", 0),

                # Attitude quaternion
                "att_w": att_quat[0],
                "att_x": att_quat[1],
                "att_y": att_quat[2],
                "att_z": att_quat[3],

                # Velocities
                "linvel_x": vel_err[0],
                "linvel_y": vel_err[1],
                "linvel_z": vel_err[2],
                "angvel_x": ang_vel[0],
                "angvel_y": ang_vel[1],
                "angvel_z": ang_vel[2],

                # Acceleration
                "linacc_x": lin_acc[0],
                "linacc_y": lin_acc[1],
                "linacc_z": lin_acc[2],
            }

            data_rows.append(data_row)
            pose_rows.append(pose_row)

        # Write CSV files
        data_csv_path = output_dir / "data.csv"
        pose_csv_path = output_dir / "all_pose_data.csv"

        self._write_csv(data_rows, data_csv_path)
        self._write_csv(pose_rows, pose_csv_path)

        carb.log_warn(f"Statistics saved to CSV: {data_csv_path}, {pose_csv_path}")

    def _write_csv(self, rows: List[Dict[str, Any]], output_path: Path):
        """Write rows to CSV file"""
        if not rows:
            return

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def update_sensor(self, sensor_type: str, data):
        """
        Do nothing. For now ignore all the sensor data and just use the state directly for demonstration purposes.
        This is a callback that is called at every physics step.

        Args:
            sensor_type (str): The name of the sensor providing the data
            data (dict): A dictionary that contains the data produced by the sensor
        """
        pass

    def update_state(self, state: State):
        """
        Method that updates the current state of the vehicle. This is a callback that is called at every physics step

        Args:
            state (State): The current state of the vehicle.
        """
        self.p = state.position
        self.R = Rotation.from_quat(state.attitude)
        self.w = state.angular_velocity
        self.v = state.linear_velocity

        self.reveived_first_state = True

    def input_reference(self):
        """
        Method that is used to return the latest target angular velocities to be applied to the vehicle

        Returns:
            A list with the target angular velocities for each individual rotor of the vehicle
        """
        return self.input_ref

    def update(self, dt: float):
        """Method that implements the fast attitude control law and updates the target angular velocities for each rotor.
        This method will be called by the simulation on every physics step

        Args:
            dt (float): The time elapsed between the previous and current function calls (s).
        """

        if self.reveived_first_state == False:
            return

        self.dt = dt

        # -------------------------------------------------
        # Update the references for the controller to track
        # -------------------------------------------------
        self.total_time += dt


        # Check if we need to update to the next trajectory index
        if isinstance(self.trajectory, list):
            # JSON trajectory (list of TrajectoryPoint)
            # Simple waypoint tracking: move to next waypoint when close enough
            if self.index < len(self.trajectory):
                # For JSON trajectories, we don't use time-based indexing
                # The index is controlled externally or by waypoint reaching logic
                pass
        elif self.trajectory is not None:
            # CSV trajectory (numpy array with time column)
            if self.index < self.max_index - 1 and self.total_time >= self.trajectory[self.index + 1, 0]:
                self.index += 1

        # Update using an external trajectory
        if isinstance(self.trajectory, list):
            # JSON trajectory (TrajectoryPoint list)
            if self.index < len(self.trajectory):
                pt = self.trajectory[self.index]
                p_ref = np.array([pt.x, pt.y, pt.z])
                v_ref = np.zeros(3)  # JSON format doesn't include velocity
                a_ref = np.zeros(3)  # JSON format doesn't include acceleration
                j_ref = np.zeros(3)  # JSON format doesn't include jerk
                yaw_ref = np.deg2rad(pt.yaw_deg)
                yaw_rate_ref = 0.0
            else:
                # Hold last position
                pt = self.trajectory[-1]
                p_ref = np.array([pt.x, pt.y, pt.z])
                v_ref = np.zeros(3)
                a_ref = np.zeros(3)
                j_ref = np.zeros(3)
                yaw_ref = np.deg2rad(pt.yaw_deg)
                yaw_rate_ref = 0.0
        elif self.trajectory is not None:
            # CSV trajectory (numpy array)
            # the target positions [m], velocity [m/s], accelerations [m/s^2], jerk [m/s^3], yaw-angle [rad], yaw-rate [rad/s]
            p_ref = np.array([self.trajectory[self.index, 1], self.trajectory[self.index, 2], self.trajectory[self.index, 3]])
            v_ref = np.array([self.trajectory[self.index, 4], self.trajectory[self.index, 5], self.trajectory[self.index, 6]])
            a_ref = np.array([self.trajectory[self.index, 7], self.trajectory[self.index, 8], self.trajectory[self.index, 9]])
            j_ref = np.array([self.trajectory[self.index, 10], self.trajectory[self.index, 11], self.trajectory[self.index, 12]])
            yaw_ref = self.trajectory[self.index, 13]
            yaw_rate_ref = self.trajectory[self.index, 14]
        # Or update the reference using the built-in trajectory
        else:
            s = 0.6
            p_ref = self.pd(self.total_time, s, self.reverse)
            v_ref = self.d_pd(self.total_time, s, self.reverse)
            a_ref = self.dd_pd(self.total_time, s, self.reverse)
            j_ref = self.ddd_pd(self.total_time, s, self.reverse)
            yaw_ref = self.yaw_d(self.total_time, s)
            yaw_rate_ref = self.d_yaw_d(self.total_time, s)

        # -------------------------------------------------
        # Outer Loop: Position Control -> Desired Acceleration
        # -------------------------------------------------

        # Compute the position tracking errors
        ep = self.p - p_ref
        ev = self.v - v_ref
        self.int_pos = self.int_pos + (ep * dt)

        # Compute desired acceleration in world frame
        # This is the output of the position controller
        acceleration_world = -(self.Kp @ ep) - (self.Kd @ ev) - (self.Ki @ self.int_pos) + a_ref

        # -------------------------------------------------
        # Middle Loop: Attitude Control (Geometric Controller)
        # Following the algorithm from controller.md Section 1.2-1.3
        # -------------------------------------------------

        # 1.2 Calculate desired thrust direction
        # T_total = a_world - g
        total_thrust_vector = acceleration_world - np.array([0.0, 0.0, -self.g])

        # z_b = normalize(T_total)
        zb = total_thrust_vector / np.linalg.norm(total_thrust_vector)

        # Apply attitude angle limits to constrain roll and pitch
        # Limit the tilt angle of z_b to prevent excessive roll/pitch
        zb = self._limit_attitude_angles(zb)

        # 1.3 Construct desired rotation matrix
        # x_c = [cos(yaw_des), sin(yaw_des), 0]^T
        xc = np.array([np.cos(yaw_ref), np.sin(yaw_ref), 0.0])

        # y_b = normalize(z_b × x_c)
        yb_unnorm = np.cross(zb, xc)
        yb_norm = np.linalg.norm(yb_unnorm)
        if yb_norm < 1e-6:
            # Handle singularity when z_b is parallel to x_c
            yb = np.array([0.0, 1.0, 0.0])
        else:
            yb = yb_unnorm / yb_norm

        # x_b = y_b × z_b
        xb = np.cross(yb, zb)

        # R_desired = [x_b, y_b, z_b]
        R_des = np.c_[xb, yb, zb]
        R = self.R.as_matrix()

        # 1.4 Calculate attitude error (in axis-angle representation)
        # Convert rotation error to axis-angle
        # Following controller.md section 1.4
        R_err = R.T @ R_des
        axis_angle_err = self.rotation_matrix_to_axis_angle(R_err)

        # Normalize to [-π, π]
        axis_angle_err = (axis_angle_err + np.pi) % (2 * np.pi) - np.pi

        # 1.5 Calculate angular velocity derivative (numerical differentiation)
        # d_error = ω_b^(t) - ω_b^(t-1)
        d_error = self.w - self.prev_ang_vel_b
        self.prev_ang_vel_b = self.w.copy()

        # 1.6 Attitude PD controller
        # ω_fb = K_ang · θ_err + K_d,ang · ω̇_err
        feedback_bodyrates = self.KAng @ axis_angle_err + self.Kdang @ d_error

        # -------------------------------------------------
        # Inner Loop: Angular Velocity PID Control
        # Following the algorithm from controller.md Section 1.7-1.8
        # -------------------------------------------------

        # 1.7 Integral term with anti-windup
        # I_err^(t) = I_err^(t-1) + ω_fb · Δt
        self.integrated_ang_vel_error += feedback_bodyrates * dt

        # Clamp integral error to prevent windup
        self.integrated_ang_vel_error = np.clip(
            self.integrated_ang_vel_error,
            -self.KiAng_vel_max,
            self.KiAng_vel_max
        )

        # 1.8 Final angular velocity PID control
        # α_cmd = (K_ω · (ω_fb - ω_b) + K_i,ω · I_err) / Δt
        final_bodyrates = (
            self.KAng_vel @ (feedback_bodyrates - self.w)
            + self.KiAng_vel @ self.integrated_ang_vel_error
        ) / dt

        # Apply yaw rate limit
        # Limit only the yaw component (z-axis) of angular velocity
        current_yaw_rate = self.w[2]
        desired_yaw_rate = feedback_bodyrates[2]

        # Limit the yaw rate change
        if abs(desired_yaw_rate) > self.max_yaw_rate_rad:
            desired_yaw_rate = np.sign(desired_yaw_rate) * self.max_yaw_rate_rad
            feedback_bodyrates[2] = desired_yaw_rate

            # Recompute final_bodyrates with limited yaw rate
            final_bodyrates = (
                self.KAng_vel @ (feedback_bodyrates - self.w)
                + self.KiAng_vel @ self.integrated_ang_vel_error
            ) / dt

        # -------------------------------------------------
        # Calculate Thrust and Torque
        # Following the algorithm from controller.md Section 1.9-1.10
        # -------------------------------------------------

        # 1.9 Calculate thrust magnitude
        # Get current body Z axis in world frame
        body_z_body = np.array([0.0, 0.0, 1.0])
        body_z_world = R @ body_z_body

        # Project total thrust onto current body Z axis
        # T_mag = max(T_total · z_b,world, 0)
        thrust_magnitude = max(np.dot(total_thrust_vector, body_z_world), 0.0)

        # 1.10 Calculate force and torque
        # F_thrust = T_mag · m
        total_thrust_force = thrust_magnitude * self.m

        # Get the desired total thrust u_1
        u_1 = total_thrust_force

        # τ = J · α_cmd
        torque = self.J @ final_bodyrates

        # -------------------------------------------------
        # Convert to Motor Commands
        # -------------------------------------------------

        # Use the allocation matrix provided by the Multirotor vehicle to convert the desired force and torque
        # to angular velocity [rad/s] references to give to each rotor
        if self.vehicle:
            self.input_ref = self.vehicle.force_and_torques_to_velocities(u_1, torque)

        # ----------------------------
        # Statistics to save for later
        # ----------------------------
        self.time_vector.append(self.total_time)
        self.position_over_time.append(self.p.copy())
        self.desired_position_over_time.append(p_ref.copy())
        self.position_error_over_time.append(ep.copy())
        self.velocity_error_over_time.append(ev.copy())
        self.atittude_error_over_time.append(axis_angle_err.copy())
        self.attitude_rate_error_over_time.append((feedback_bodyrates - self.w).copy())

        # Additional statistics compatible with data collector format
        # Store attitude as quaternion (w, x, y, z)
        quat = self.R.as_quat()  # Returns [x, y, z, w]
        self.attitude_over_time.append([quat[3], quat[0], quat[1], quat[2]])  # Reorder to [w, x, y, z]

        # Store desired attitude angles
        self.desired_attitude_over_time.append({
            "roll": 0.0,  # Not directly computed in geometric controller
            "pitch": 0.0,
            "yaw": np.rad2deg(yaw_ref)
        })

        # Store angular velocity
        self.angular_velocity_over_time.append(self.w.copy())

        # Store linear acceleration (estimated)
        # a = (F_thrust / m) * z_b - g
        self.linear_acceleration_over_time.append(self.a.copy() if hasattr(self, 'a') else np.zeros(3))

        # Store thrust magnitude and torque
        self.thrust_magnitude_over_time.append(thrust_magnitude)
        self.torque_over_time.append(torque.copy())

    def _limit_attitude_angles(self, zb: np.ndarray) -> np.ndarray:
        """Limit the desired body Z-axis direction to constrain roll and pitch angles.

        This function ensures that the tilt angle of the desired body Z-axis (zb)
        does not exceed the specified maximum roll and pitch angles.

        Args:
            zb (np.ndarray): Desired body Z-axis direction (normalized)

        Returns:
            np.ndarray: Limited body Z-axis direction (normalized)
        """
        # Calculate the tilt angle from vertical
        # tilt = arccos(z_component)
        z_component = zb[2]

        # Calculate maximum tilt angle from roll and pitch limits
        # For simplicity, use the smaller of the two limits
        max_tilt = min(self.max_roll_rad, self.max_pitch_rad)

        # If z_component is too small (tilt too large), limit it
        min_z_component = np.cos(max_tilt)

        if z_component < min_z_component:
            # Limit the tilt angle
            # Scale the xy components to achieve the desired tilt
            xy_norm = np.linalg.norm(zb[:2])
            if xy_norm > 1e-6:
                # Calculate the new xy components
                xy_scale = np.sin(max_tilt) / xy_norm
                zb_limited = np.array([
                    zb[0] * xy_scale,
                    zb[1] * xy_scale,
                    min_z_component
                ])
                # Normalize
                zb_limited = zb_limited / np.linalg.norm(zb_limited)

                carb.log_warn(f"Attitude angle limit applied: tilt={np.rad2deg(np.arccos(z_component)):.1f}° > max={np.rad2deg(max_tilt):.1f}°")
                return zb_limited

        return zb

    @staticmethod
    def rotation_matrix_to_axis_angle(R):
        """Convert a rotation matrix to axis-angle representation.

        Args:
            R (np.array): A 3x3 rotation matrix

        Returns:
            np.array: A 3x1 axis-angle vector
        """
        # Use scipy's rotation class for robust conversion
        rot = Rotation.from_matrix(R)
        axis_angle = rot.as_rotvec()
        return axis_angle

    def reset_statistics(self):

        self.index = 0
        # If we received an external trajectory, reset the time to 0.0
        if self.trajectory is not None:
            self.total_time = 0.0
        # if using the internal trajectory, make the parametric value start at -5.0
        else:
            self.total_time = -5.0

        # Reset integral errors
        self.int_pos = np.array([0.0, 0.0, 0.0])
        self.integrated_ang_vel_error = np.array([0.0, 0.0, 0.0])
        self.prev_ang_vel_b = np.array([0.0, 0.0, 0.0])

        # Reset the lists used for analysing performance statistics
        self.time_vector = []
        self.desired_position_over_time = []
        self.position_over_time = []
        self.position_error_over_time = []
        self.velocity_error_over_time = []
        self.atittude_error_over_time = []
        self.attitude_rate_error_over_time = []

        # Additional statistics
        self.desired_attitude_over_time = []
        self.attitude_over_time = []
        self.angular_velocity_over_time = []
        self.linear_acceleration_over_time = []
        self.thrust_magnitude_over_time = []
        self.torque_over_time = []

    # ---------------------------------------------------
    # Definition of an exponential trajectory for example
    # This can be used as a reference if not trajectory file is passed
    # as an argument to the constructor of this class
    # ---------------------------------------------------

    def pd(self, t, s, reverse=False):
        """The desired position of the built-in trajectory

        Args:
            t (float): The parametric value that guides the equation
            s (float): How steep and agressive the curve is
            reverse (bool, optional): Choose whether we want to flip the curve (so that we can have 2 drones almost touching). Defaults to False.

        Returns:
            np.ndarray: A 3x1 array with the x, y ,z desired [m]
        """

        x = t
        z = 1 / s * np.exp(-0.5 * np.power(t/s, 2)) + 1.0
        y = 1 / s * np.exp(-0.5 * np.power(t/s, 2))

        if reverse == True:
            y = -1 / s * np.exp(-0.5 * np.power(t/s, 2)) + 4.5

        return np.array([x,y,z])

    def d_pd(self, t, s, reverse=False):
        """The desired velocity of the built-in trajectory

        Args:
            t (float): The parametric value that guides the equation
            s (float): How steep and agressive the curve is
            reverse (bool, optional): Choose whether we want to flip the curve (so that we can have 2 drones almost touching). Defaults to False.

        Returns:
            np.ndarray: A 3x1 array with the d_x, d_y ,d_z desired [m/s]
        """

        x = 1.0
        y = -(t * np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,3)
        z = -(t * np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,3)

        if reverse == True:
            y = (t * np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,3)

        return np.array([x,y,z])

    def dd_pd(self, t, s, reverse=False):
        """The desired acceleration of the built-in trajectory

        Args:
            t (float): The parametric value that guides the equation
            s (float): How steep and agressive the curve is
            reverse (bool, optional): Choose whether we want to flip the curve (so that we can have 2 drones almost touching). Defaults to False.

        Returns:
            np.ndarray: A 3x1 array with the dd_x, dd_y ,dd_z desired [m/s^2]
        """

        x = 0.0
        y = (np.power(t,2)*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,5) - np.exp(-np.power(t,2)/(2*np.power(s,2)))/np.power(s,3)
        z = (np.power(t,2)*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,5) - np.exp(-np.power(t,2)/(2*np.power(s,2)))/np.power(s,3)

        if reverse == True:
            y = np.exp(-np.power(t,2)/(2*np.power(s,2)))/np.power(s,3) - (np.power(t,2)*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,5)

        return np.array([x,y,z])

    def ddd_pd(self, t, s, reverse=False):
        """The desired jerk of the built-in trajectory

        Args:
            t (float): The parametric value that guides the equation
            s (float): How steep and agressive the curve is
            reverse (bool, optional): Choose whether we want to flip the curve (so that we can have 2 drones almost touching). Defaults to False.

        Returns:
            np.ndarray: A 3x1 array with the ddd_x, ddd_y ,ddd_z desired [m/s^3]
        """
        x = 0.0
        y = (3*t*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,5) - (np.power(t,3)*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,7)
        z = (3*t*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,5) - (np.power(t,3)*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,7)

        if reverse == True:
            y = (np.power(t,3)*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,7) - (3*t*np.exp(-np.power(t,2)/(2*np.power(s,2))))/np.power(s,5)

        return np.array([x,y,z])

    def yaw_d(self, t, s):
        """The desired yaw of the built-in trajectory

        Args:
            t (float): The parametric value that guides the equation
            s (float): How steep and agressive the curve is
            reverse (bool, optional): Choose whether we want to flip the curve (so that we can have 2 drones almost touching). Defaults to False.

        Returns:
            np.ndarray: A float with the desired yaw in rad
        """
        return 0.0

    def d_yaw_d(self, t, s):
        """The desired yaw_rate of the built-in trajectory

        Args:
            t (float): The parametric value that guides the equation
            s (float): How steep and agressive the curve is
            reverse (bool, optional): Choose whether we want to flip the curve (so that we can have 2 drones almost touching). Defaults to False.

        Returns:
            np.ndarray: A float with the desired yaw_rate in rad/s
        """
        return 0.0

    def reset(self):
        """
        Method that when implemented, should handle the reset of the vehicle simulation to its original state
        """
        self.reset_statistics()

    def update_graphical_sensor(self, sensor_type: str, data):
        """
        For this demo we do not care about graphical sensors such as camera, therefore we can ignore this callback
        """
        pass
