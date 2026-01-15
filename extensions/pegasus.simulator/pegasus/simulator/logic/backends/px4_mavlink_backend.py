# Copyright (c) 2024-2026 amurzzb@gmail.com
# Licensed under the MIT License
"""
PX4 MAVLink 后端（px4_mavlink_backend.py）

==========================
概述
==========================
本模块实现 Isaac Sim 与 PX4 飞控之间的 MAVLink 通信后端，负责：
- 发送仿真传感器数据（IMU、GPS、气压计、磁力计）到 PX4
- 接收 PX4 的执行器控制命令（电机推力）
- 管理 PX4 SITL 进程的自动启动和重启
- 支持 lockstep 同步模式（仿真与 PX4 时钟同步）

==========================
类结构
==========================
SensorSource
  - 传感器类型的 MAVLink 位掩码常量

SensorMsg
  - 传感器数据缓冲区（IMU、GPS、气压计、磁力计、视觉等）

ThrusterControl
  - 执行器控制数据处理（MAVLink → 角速度）

PX4MavlinkBackendConfig
  - 后端配置类（连接参数、PX4 启动参数、执行器参数等）

PX4MavlinkBackend
  - 主后端类，继承自 Backend 基类
  - 实现传感器数据发送、控制命令接收、PX4 进程管理

==========================
MAVLink 消息类型
==========================
发送（Isaac Sim → PX4）：
  - HEARTBEAT           - 心跳消息（1Hz）
  - HIL_SENSOR          - 传感器数据（加速度、陀螺仪、磁力计、气压计）
  - HIL_GPS             - GPS 数据
  - HIL_STATE_QUATERNION - 真值状态（可选）
  - VISION_POSITION_ESTIMATE - 视觉定位（可选）

接收（PX4 → Isaac Sim）：
  - HEARTBEAT           - PX4 心跳
  - HIL_ACTUATOR_CONTROLS - 执行器控制命令

==========================
配置参数说明
==========================
PX4MavlinkBackendConfig 参数：
  vehicle_id          - 载具 ID（0, 1, 2, ...）
  connection_type     - 连接类型（tcpin/udpin/tcpout/udpout）
  connection_ip       - 连接 IP（默认 localhost）
  connection_baseport - 基础端口（默认 4560）
  px4_autolaunch      - 是否自动启动 PX4（默认 True）
  px4_dir             - PX4-Autopilot 目录路径
  px4_vehicle_model   - PX4 载具模型（默认 gazebo-classic_iris）
  enable_lockstep     - 启用 lockstep 同步（默认 True）
  num_rotors          - 电机数量（默认 4）
  input_offset        - 电机输入偏移
  input_scaling       - 电机输入缩放
  zero_position_armed - 解锁时零位
  update_rate         - 更新频率（默认 120Hz）
  sim_speed_factor    - 仿真加速倍率（默认 1.0）

==========================
端口规划
==========================
4560 + vehicle_id    - MAVLink TCP lockstep 端口（Isaac Sim 监听）
8888 + vehicle_id    - ROS2 UXRCE-DDS 端口（PX4 使用）

==========================
Lockstep 同步模式
==========================
当 enable_lockstep=True 时：
1. Isaac Sim 发送传感器数据后等待 PX4 返回控制命令
2. PX4 收到传感器数据后计算控制输出并发送
3. Isaac Sim 收到控制命令后才推进下一个物理步
4. 确保仿真时钟与 PX4 时钟严格同步

==========================
主要方法
==========================
start()
  - 启动 MAVLink 后端
  - 初始化连接
  - 自动启动 PX4（如果配置）

stop()
  - 停止 MAVLink 后端
  - 关闭连接
  - 停止 PX4 进程

update(dt)
  - 每物理步调用
  - 发送心跳、传感器数据
  - 接收控制命令
  - 检查 PX4 进程状态

update_sensor(sensor_type, data)
  - 接收仿真传感器数据回调
  - 支持：IMU、GPS、Barometer、Magnetometer

update_state(state)
  - 接收载具状态回调
  - 用于发送真值数据

input_reference()
  - 返回电机角速度参考值列表

hard_reboot_px4()
  - 硬重启 PX4 进程（立即终止）

soft_relaunch_px4()
  - 软重启 PX4 进程（清理后重启）

px4_ready_to_takeoff (property)
  - 返回 PX4 是否处于 "Ready for takeoff" 状态

==========================
调用关系
==========================
┌─────────────────────────────────────────────────────────────┐
│                   Isaac Sim 仿真端                          │
│                                                             │
│  Multirotor ──► PX4MavlinkBackend                          │
│      │              │                                       │
│      │ 传感器数据    │ 控制命令                              │
│      ▼              ▼                                       │
│  [IMU,GPS,...]   [电机角速度]                               │
└─────────┬───────────┬───────────────────────────────────────┘
          │           ▲
          │ MAVLink   │ MAVLink
          ▼           │
┌─────────────────────┴───────────────────────────────────────┐
│                    PX4 SITL                                  │
│              (由 PX4LaunchTool 管理)                         │
│                                                              │
│  TCP 端口: 4560 + vehicle_id                                │
└──────────────────────────────────────────────────────────────┘

==========================
传感器数据流
==========================
物理仿真
    │
    ▼
IMU/GPS/Baro/Mag 传感器
    │
    │ update_sensor()
    ▼
SensorMsg 缓冲区
    │
    │ send_sensor_msgs() / send_gps_msgs()
    ▼
MAVLink HIL_SENSOR / HIL_GPS
    │
    ▼
PX4 飞控

==========================
控制数据流
==========================
PX4 飞控
    │
    │ HIL_ACTUATOR_CONTROLS
    ▼
poll_mavlink_messages()
    │
    │ handle_control()
    ▼
ThrusterControl
    │
    │ input_reference()
    ▼
Multirotor 电机

==========================
使用示例
==========================
from px4_mavlink_backend import PX4MavlinkBackend, PX4MavlinkBackendConfig

# 创建配置
config = PX4MavlinkBackendConfig({
    "vehicle_id": 0,
    "px4_autolaunch": True,
    "px4_dir": "/home/user/PX4-Autopilot",
    "sim_speed_factor": 2.0,
    "enable_lockstep": True,
})

# 创建后端
backend = PX4MavlinkBackend(config)

# 添加到 Multirotor
multirotor_config.backends = [backend]

# 后端会在仿真开始时自动启动

==========================
错误恢复
==========================
1. PX4 进程崩溃
   - update() 中周期性检查 PX4 进程状态
   - 如果进程不存在，自动调用 hard_reboot_px4() 重启

2. MAVLink 连接断开
   - re_initialize_interface() 重新初始化连接
   - 重置心跳等待状态

3. 心跳超时
   - wait_for_first_hearbeat() 等待 PX4 心跳
   - 节流日志输出避免刷屏

==========================
原始文件信息
==========================
| File: px4_mavlink_backend.py
| Author: Marcelo Jacinto (marcelo.jacinto@tecnico.ulisboa.pt)
| Description: File that implements the Mavlink Backend for communication/control with/of the vehicle simulation
| License: BSD-3-Clause. Copyright (c) 2023, Marcelo Jacinto. All rights reserved.
"""
__all__ = ["PX4MavlinkBackend", "PX4MavlinkBackendConfig", "ts_log", "_wait_for_port_release", "_is_port_in_use"]

import carb
import time
import traceback
import numpy as np
from datetime import datetime
from pymavlink import mavutil

from pegasus.simulator.logic.state import State
from pegasus.simulator.logic.backends.backend import Backend, BackendConfig
from pegasus.simulator.logic.interface.pegasus_interface import PegasusInterface
from pegasus.simulator.logic.backends.tools.px4_launch_tool import PX4LaunchTool


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """
    生成带时间戳的日志消息并输出到 carb 日志

    Args:
        prefix: 日志前缀（如 [uav0]）
        message: 日志内容
        level: 日志级别 (INFO, WARN, ERROR, DEBUG)

    Returns:
        格式化的日志字符串
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] {prefix} {message}"

    if level == "ERROR":
        carb.log_error(log_msg)
    elif level == "WARN":
        carb.log_warn(log_msg)
    elif level == "DEBUG":
        carb.log_info(log_msg)  # carb 没有 debug 级别
    else:
        carb.log_info(log_msg)

    return log_msg


def _is_port_in_use(port: int) -> bool:
    """检查端口是否被占用"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('127.0.0.1', port))
            return False
        except OSError:
            return True


def _wait_for_port_release(port: int, timeout: float = 30.0, interval: float = 0.5) -> bool:
    """
    等待端口释放

    Args:
        port: 端口号
        timeout: 超时时间（秒）
        interval: 检测间隔（秒）

    Returns:
        True 如果端口已释放，False 如果超时
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not _is_port_in_use(port):
            return True
        time.sleep(interval)
    return False


class SensorSource:
    """ The binary codes to signal which simulated data is being sent through mavlink

    Atribute:
        | ACCEL (int): mavlink binary code for the accelerometer (0b0000000000111 = 7)
        | GYRO (int): mavlink binary code for the gyroscope (0b0000000111000 = 56)
        | MAG (int): mavlink binary code for the magnetometer (0b0000111000000=448)
        | BARO (int): mavlink binary code for the barometer (0b1101000000000=6656)
        | DIFF_PRESS (int): mavlink binary code for the pressure sensor (0b0010000000000=1024)
    """

    ACCEL: int = 7    
    GYRO: int = 56          
    MAG: int = 448        
    BARO: int = 6656
    DIFF_PRESS: int = 1024


class SensorMsg:
    """
    An auxiliary data class where we write all the sensor data that is going to be sent through mavlink
    """

    def __init__(self):

        # IMU Data
        self.new_imu_data: bool = False
        self.received_first_imu: bool = False
        self.xacc: float = 0.0
        self.yacc: float = 0.0
        self.zacc: float = 0.0
        self.xgyro: float = 0.0
        self.ygyro: float = 0.0
        self.zgyro: float = 0.0

        # Baro Data
        self.new_bar_data: bool = False
        self.abs_pressure: float = 0.0
        self.pressure_alt: float = 0.0
        self.temperature: float = 0.0

        # Magnetometer Data
        self.new_mag_data: bool = False
        self.xmag: float = 0.0
        self.ymag: float = 0.0
        self.zmag: float = 0.0

        # Airspeed Data
        self.new_press_data: bool = False
        self.diff_pressure: float = 0.0

        # GPS Data
        self.new_gps_data: bool = False
        self.fix_type: int = 0
        self.latitude_deg: float = -999
        self.longitude_deg: float = -999
        self.altitude: float = -999
        self.eph: float = 1.0
        self.epv: float = 1.0
        self.velocity: float = 0.0
        self.velocity_north: float = 0.0
        self.velocity_east: float = 0.0
        self.velocity_down: float = 0.0
        self.cog: float = 0.0
        self.satellites_visible: int = 0

        # Vision Pose
        self.new_vision_data: bool = False
        self.vision_x: float = 0.0
        self.vision_y: float = 0.0
        self.vision_z: float = 0.0
        self.vision_roll: float = 0.0
        self.vision_pitch: float = 0.0
        self.vision_yaw: float = 0.0
        self.vision_covariance = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        # Simulation State
        self.new_sim_state: bool = False
        self.sim_attitude = [1.0, 0.0, 0.0, 0.0]  # [w, x, y, z]
        self.sim_acceleration = [0.0, 0.0, 0.0]  # [x,y,z body acceleration]
        self.sim_angular_vel = [0.0, 0.0, 0.0]  # [roll-rate, pitch-rate, yaw-rate] rad/s
        self.sim_lat = 0.0  # [deg]
        self.sim_lon = 0.0  # [deg]
        self.sim_alt = 0.0  # [m]
        self.sim_ind_airspeed = 0.0  # Indicated air speed
        self.sim_true_airspeed = 0.0  # Indicated air speed
        self.sim_velocity_inertial = [0.0, 0.0, 0.0]  # North-east-down [m/s]


class ThrusterControl:
    """
    An auxiliary data class that saves the thrusters command data received via mavlink and 
    scales them into individual angular velocities expressed in rad/s to apply to each rotor
    """

    def __init__(
        self,
        num_rotors: int = 4,
        input_offset=[0, 0, 0, 0],
        input_scaling=[0, 0, 0, 0],
        zero_position_armed=[100, 100, 100, 100],
    ):
        """Initialize the ThrusterControl object

        Args:
            num_rotors (int): The number of rotors that the actual system has 4.
            input_offset (list): A list with the offsets to apply to the rotor values received via mavlink. Defaults to [0, 0, 0, 0].
            input_scaling (list): A list with the scaling to apply to the rotor values received via mavlink. Defaults to [0, 0, 0, 0].
            zero_position_armed (list): Another list of offsets to apply to the rotor values received via mavlink. Defaults to [100, 100, 100, 100].
        """

        self.num_rotors: int = num_rotors

        # Values to scale and offset the rotor control inputs received from PX4
        assert len(input_offset) == self.num_rotors
        self.input_offset = input_offset

        assert len(input_scaling) == self.num_rotors
        self.input_scaling = input_scaling

        assert len(zero_position_armed) == self.num_rotors
        self.zero_position_armed = zero_position_armed

        # The actual speed references to apply to the vehicle rotor joints
        self._input_reference = [0.0 for i in range(self.num_rotors)]

    @property
    def input_reference(self):
        """A list of floats with the angular velocities in rad/s

        Returns:
            list: A list of floats with the angular velocities to apply to each rotor, expressed in rad/s
        """
        return self._input_reference

    def update_input_reference(self, controls):
        """Takes a list with the thrust controls received via mavlink and scales them in order to generated
        the equivalent angular velocities in rad/s

        Args:
            controls (list): A list of ints with thrust controls received via mavlink
        """

        # Check if the number of controls received is correct
        if len(controls) < self.num_rotors:
            carb.log_warn("Did not receive enough inputs for all the rotors")
            return
        

        # Update the desired reference for every rotor (and saturate according to the min and max values)
        for i in range(self.num_rotors):
            
            # Compute the actual velocity reference to apply to each rotor
            self._input_reference[i] = (controls[i] + self.input_offset[i]) * self.input_scaling[i] + self.zero_position_armed[i]


    def zero_input_reference(self):
        """
        When this method is called, the input_reference is updated such that every rotor is stopped
        """
        self._input_reference = [0.0 for i in range(self.num_rotors)]


class PX4MavlinkBackendConfig(BackendConfig):
    """
    An auxiliary data class used to store all the configurations for the mavlink communications.
    """

    def __init__(self, config={}):
        """
        Initialize the PX4MavlinkBackendConfig class

        Args:
            config (dict): A Dictionary that contains all the parameters for configuring the Mavlink interface - it can be empty or only have some of the parameters used by this backend.
        
        Examples:
            The dictionary default parameters are

            >>> {"vehicle_id": 0,           
            >>>  "connection_type": "tcpin",           
            >>>  "connection_ip": "localhost",
            >>>  "connection_baseport": 4560,
            >>>  "px4_autolaunch": True,
            >>>  "mavros_autolaunch": True,
            >>>  "px4_dir": "PegasusInterface().px4_path",
            >>>  "px4_vehicle_model": "gazebo-classic_iris",
            >>>  "enable_lockstep": True,
            >>>  "num_rotors": 4,
            >>>  "input_offset": [0.0, 0.0, 0.0, 0.0],
            >>>  "input_scaling": [1000.0, 1000.0, 1000.0, 1000.0],
            >>>  "zero_position_armed": [100.0, 100.0, 100.0, 100.0],
            >>>  "update_rate": 250.0
            >>> }
        """

        # Configurations for the mavlink communication protocol (note: the vehicle id is sumed to the connection_baseport)
        self.config = config
        
        self.vehicle_id = self.config.get("vehicle_id", 0)
        self.connection_type = self.config.get("connection_type", "tcpin")
        self.connection_ip = self.config.get("connection_ip", "localhost")
        self.connection_baseport = self.config.get("connection_baseport", 4560)

        # Configure whether to launch px4 in the background automatically or not for every vehicle launched
        self.px4_autolaunch: bool = self.config.get("px4_autolaunch", True)
        self.mavros_autolaunch: bool = False
        
        self.px4_dir: str = self.config.get("px4_dir", PegasusInterface().px4_path)
        self.px4_vehicle_model: str = self.config.get("px4_vehicle_model", "gazebo-classic_iris")

        # Configurations to interpret the rotors control messages coming from mavlink
        self.enable_lockstep: bool = self.config.get("enable_lockstep", True)
        self.num_rotors: int = self.config.get("num_rotors", 4)
        self.input_offset = self.config.get("input_offset", [0.0, 0.0, 0.0, 0.0])
        self.input_scaling = self.config.get("input_scaling", [1000.0, 1000.0, 1000.0, 1000.0])
        self.zero_position_armed = self.config.get("zero_position_armed", [100.0, 100.0, 100.0, 100.0])

        # The update rate at which we will be sending data to mavlink (TODO - remove this from here in the future
        # and infer directly from the function calls)
        self.update_rate: float = self.config.get("update_rate", 120.0)  # [Hz]

        # The simulation speed factor
        self.sim_speed_factor: float = self.config.get("sim_speed_factor", 1.0)


class PX4MavlinkBackend(Backend):
    """ The Mavlink Backend used to receive the vehicle's state and sensor data in order to send to PX4 through mavlink. It also
    receives via mavlink the thruster commands to apply to each vehicle rotor.
    """

    def __init__(self, config: PX4MavlinkBackendConfig = PX4MavlinkBackendConfig()):
        """Initialize the PX4MavlinkBackend

        Args:
            config (PX4MavlinkBackendConfig): The configuration class for the PX4MavlinkBackend. Defaults to PX4MavlinkBackendConfig().
        """

        # Initialize the Backend object
        super().__init__(config)

        # Setup the desired mavlink connection port
        # The connection will only be created once the simulation starts
        self.config: PX4MavlinkBackendConfig = config
        self._vehicle_id = self.config.vehicle_id
        self._connection = None
        # Build PyMAVLink URL. For TCP, prefer tcpin; for UDP, prefer udpin.
        self._connection_port = (
            self.config.connection_type
            + ":"
            + self.config.connection_ip
            + ":"
            + str(self.config.connection_baseport + self.config.vehicle_id)
        )

        # Check if we need to autolaunch px4 in the background or not
        self.px4_autolaunch: bool = self.config.px4_autolaunch
        # Check if we need to autolaunch mavlink in the background or not
        
        self.px4_vehicle_model: str = self.config.px4_vehicle_model  # only needed if px4_autolaunch == True
        self.px4_tool: PX4LaunchTool = None
        self.px4_dir: str = self.config.px4_dir

        # Registry for managing PX4 processes per vehicle (persisted via pid files)
        # Prefer reusing existing PX4 if already launched elsewhere
        self._px4_pid_path = PX4LaunchTool.pid_file_path(self._vehicle_id)

        # They are configured and launched by the external control script.

        # Get PX4 log directory from environment or use default
        self._px4_log_dir = self._get_px4_log_dir()

        # Set the update rate used for sending the messages (TODO - remove this hardcoded value from here)
        self._update_rate: float = self.config.update_rate
        self._time_step: float = 1.0 / self._update_rate  # s

        self._is_running: bool = False

        # Vehicle Sensor data to send through mavlink
        self._sensor_data: SensorMsg = SensorMsg()

        # Vehicle Rotor data received from mavlink
        self._rotor_data: ThrusterControl = ThrusterControl(
            self.config.num_rotors, self.config.input_offset, self.config.input_scaling, self.config.zero_position_armed
        )

        # Vehicle actuator control data
        self._num_inputs: int = self.config.num_rotors
        self._input_reference: np.ndarray = np.zeros((self._num_inputs,))
        self._armed: bool = False

        self._input_offset: np.ndarray = np.zeros((self._num_inputs,))
        self._input_scaling: np.ndarray = np.zeros((self._num_inputs,))

        # Select whether lockstep is enabled
        self._enable_lockstep: bool = self.config.enable_lockstep

        # Auxiliar variables to handle the lockstep between receiving sensor data and actuator control
        self._received_first_actuator: bool = False

        self._received_actuator: bool = False

        # Auxiliar variables to check if we have already received an hearbeat from the software in the loop simulation
        self._received_first_hearbeat: bool = False

        self._last_heartbeat_sent_time = 0

        # Throttle 'Waiting for first hearbeat' log to reduce spam
        self._last_waiting_heartbeat_log_time = 0
        self._waiting_heartbeat_log_interval = 3.0  # seconds

        # Auxiliar variables for setting the u_time when sending sensor data to px4
        self._current_utime: int = 0

        # Log 前缀（不依赖 MAVROS 命名空间）
        self._log_prefix = f"[uav{self._vehicle_id}]"

    def _get_px4_log_dir(self):
        """获取 PX4 日志目录"""
        import os
        session_ts = os.environ.get("PEGASUS_SESSION_TS")
        if session_ts:
            cwd = os.getcwd()
            log_dir = os.path.join(cwd, "logs", session_ts, "px4")
            try:
                os.makedirs(log_dir, exist_ok=True)
                return log_dir
            except Exception:
                pass
        # 回退到 None,让 PX4LaunchTool 使用默认路径
        return None

    def update_sensor(self, sensor_type: str, data):
        """Method that is used as callback for the vehicle for every iteration that a sensor produces new data. 
        Only the IMU, GPS, Barometer and  Magnetometer sensor data are stored to be sent through mavlink. Every other 
        sensor data that gets passed to this function is discarded.

        Args:
            sensor_type (str): A name that describes the type of sensor
            data (dict): A dictionary that contains the data produced by the sensor
        """

        if sensor_type == "IMU":
            self.update_imu_data(data)
        elif sensor_type == "GPS":
            self.update_gps_data(data)
        elif sensor_type == "Barometer":
            self.update_bar_data(data)
        elif sensor_type == "Magnetometer":
            self.update_mag_data(data)
        # If the data received is not from one of the above sensors, then this backend does
        # not support that sensor and it will just ignore it
        else:
            pass

    def update_imu_data(self, data):
        """Gets called by the 'update_sensor' method to update the current IMU data

        Args:
            data (dict): The data produced by an IMU sensor
        """

        # Acelerometer data
        self._sensor_data.xacc = data["linear_acceleration"][0]
        self._sensor_data.yacc = data["linear_acceleration"][1]
        self._sensor_data.zacc = data["linear_acceleration"][2]

        # Gyro data
        self._sensor_data.xgyro = data["angular_velocity"][0]
        self._sensor_data.ygyro = data["angular_velocity"][1]
        self._sensor_data.zgyro = data["angular_velocity"][2]

        # Signal that we have new IMU data
        self._sensor_data.new_imu_data = True
        self._sensor_data.received_first_imu = True

    def update_gps_data(self, data):
        """Gets called by the 'update_sensor' method to update the current GPS data

        Args:
            data (dict): The data produced by an GPS sensor
        """

        # GPS data
        self._sensor_data.fix_type = int(data["fix_type"])
        self._sensor_data.latitude_deg = int(data["latitude"] * 10000000)
        self._sensor_data.longitude_deg = int(data["longitude"] * 10000000)
        self._sensor_data.altitude = int(data["altitude"] * 1000)
        self._sensor_data.eph = int(data["eph"])
        self._sensor_data.epv = int(data["epv"])
        self._sensor_data.velocity = int(data["speed"] * 100)
        self._sensor_data.velocity_north = int(data["velocity_north"] * 100)
        self._sensor_data.velocity_east = int(data["velocity_east"] * 100)
        self._sensor_data.velocity_down = int(data["velocity_down"] * 100)
        self._sensor_data.cog = int(data["cog"] * 100)
        self._sensor_data.satellites_visible = int(data["sattelites_visible"])

        # Signal that we have new GPS data
        self._sensor_data.new_gps_data = True

        # Also update the groundtruth for the latitude and longitude
        self._sensor_data.sim_lat = int(data["latitude_gt"] * 10000000)
        self._sensor_data.sim_lon = int(data["longitude_gt"] * 10000000)
        self._sensor_data.sim_alt = int(data["altitude_gt"] * 1000)

    def update_bar_data(self, data):
        """Gets called by the 'update_sensor' method to update the current Barometer data

        Args:
            data (dict): The data produced by an Barometer sensor
        """

        # Barometer data
        self._sensor_data.temperature = data["temperature"]
        self._sensor_data.abs_pressure = data["absolute_pressure"]
        self._sensor_data.pressure_alt = data["pressure_altitude"]

        # Signal that we have new Barometer data
        self._sensor_data.new_bar_data = True

    def update_mag_data(self, data):
        """Gets called by the 'update_sensor' method to update the current Vision data

        Args:
            data (dict): The data produced by an Vision sensor
        """

        # Magnetometer data
        self._sensor_data.xmag = data["magnetic_field"][0]
        self._sensor_data.ymag = data["magnetic_field"][1]
        self._sensor_data.zmag = data["magnetic_field"][2]

        # Signal that we have new Magnetometer data
        self._sensor_data.new_mag_data = True

    def update_vision_data(self, data):
        """Method that 'in the future' will get called by the 'update_sensor' method to update the current Vision data
        This callback is currently not being called (TODO in a future simulator version)
        Args:
            data (dict): The data produced by an Vision sensor
        """

        # Vision or MOCAP data
        self._sensor_data.vision_x = data["x"]
        self._sensor_data.vision_y = data["y"]
        self._sensor_data.vision_z = data["z"]
        self._sensor_data.vision_roll = data["roll"]
        self._sensor_data.vision_pitch = data["pitch"]
        self._sensor_data.vision_yaw = data["yaw"]

        # Signal that we have new vision or mocap data
        self._sensor_data.new_vision_data = True

    def update_state(self, state: State):
        """Method that is used as callback and gets called at every physics step with the current state of the vehicle.
        This state is then stored in order to be sent as groundtruth via mavlink

        Args:
            state (State): The current state of the vehicle.
        """

        # Get the quaternion in the convention [x, y, z, w]
        attitude = state.get_attitude_ned_frd()

        # Rotate the quaternion to the mavlink standard
        self._sensor_data.sim_attitude[0] = attitude[3]
        self._sensor_data.sim_attitude[1] = attitude[0]
        self._sensor_data.sim_attitude[2] = attitude[1]
        self._sensor_data.sim_attitude[3] = attitude[2]

        # Get the angular velocity
        ang_vel = state.get_angular_velocity_frd()
        self._sensor_data.sim_angular_vel[0] = ang_vel[0]
        self._sensor_data.sim_angular_vel[1] = ang_vel[1]
        self._sensor_data.sim_angular_vel[2] = ang_vel[2]

        # Get the acceleration
        acc_vel = state.get_linear_acceleration_ned()
        self._sensor_data.sim_acceleration[0] = int(acc_vel[0] * 1000)
        self._sensor_data.sim_acceleration[1] = int(acc_vel[1] * 1000)
        self._sensor_data.sim_acceleration[2] = int(acc_vel[2] * 1000)

        # Get the latitude, longitude and altitude directly from the GPS

        # Get the linear velocity of the vehicle in the inertial frame
        lin_vel = state.get_linear_velocity_ned()
        self._sensor_data.sim_velocity_inertial[0] = int(lin_vel[0] * 100)
        self._sensor_data.sim_velocity_inertial[1] = int(lin_vel[1] * 100)
        self._sensor_data.sim_velocity_inertial[2] = int(lin_vel[2] * 100)

        # Compute the air_speed - assumed indicated airspeed due to flow aligned with pitot (body x)
        body_vel = state.get_linear_body_velocity_ned_frd()
        self._sensor_data.sim_ind_airspeed = int(body_vel[0] * 100)
        self._sensor_data.sim_true_airspeed = int(np.linalg.norm(lin_vel) * 100)  # TODO - add wind here

        self._sensor_data.new_sim_state = True

    def input_reference(self):
        """Method that when implemented, should return a list of desired angular velocities to apply to the vehicle rotors
        """
        return self._rotor_data.input_reference

    def __del__(self):
        """Gets called when the PX4MavlinkBackend object gets destroyed. When this happens, we make sure
        to close any mavlink connection open for this vehicle.
        """

        # When this object gets destroyed, close the mavlink connection to free the communication port
        try:
            self._connection.close()
            self._connection = None
        except:
            carb.log_info(f"{self._log_prefix} Mavlink connection was not closed, because it was never opened")

    def start(self):
        """Method that handles the begining of the simulation of vehicle. It will try to open the mavlink connection 
        interface and also attemp to launch px4 in a background process if that option as specified in the config class
        """

        # If we are already running the mavlink interface, then ignore the function call
        if self._is_running == True:
            return

        # If the connection no longer exists (we stoped and re-started the stream, then re_intialize the interface)
        if self._connection is None:
            self.re_initialize_interface()

        # Set the flag to signal that the mavlink transmission has started
        self._is_running = True

        # Launch the PX4 in the background if needed
        if self.px4_tool is None:
            carb.log_info(f"{self._log_prefix} Attempting to launch PX4 in background process")
            self.px4_tool = PX4LaunchTool(self.px4_dir, self._vehicle_id, self.px4_vehicle_model, self.config.sim_speed_factor, log_dir=self._px4_log_dir)
            if self.px4_autolaunch:
                # If a pid file exists, verify the process is alive; otherwise relaunch
                try:
                    import os
                    exists = os.path.exists(self._px4_pid_path)
                    alive = self._is_px4_alive() if exists else False
                    if exists and alive:
                        carb.log_info(f"{self._log_prefix} Existing PX4 pid alive ({self._px4_pid_path}); skipping autolaunch")
                    else:
                        if exists and not alive:
                            try:
                                os.remove(self._px4_pid_path)
                                carb.log_warn(f"{self._log_prefix} Removed stale PX4 pid file {self._px4_pid_path}")
                            except Exception as e_rm:
                                carb.log_warn(f"{self._log_prefix} Remove stale PID failed: {e_rm}")
                                carb.log_warn(traceback.format_exc())
                        self.px4_tool.launch_px4()
                except Exception as e:
                    carb.log_warn(f"{self._log_prefix} PID/alive check failed: {e}; autolaunch anyway")
                    carb.log_warn(traceback.format_exc())
                    self.px4_tool.launch_px4()
            
    def stop(self):
        """Method that when called will handle the stopping of the simulation of vehicle. It will make sure that any open
        mavlink connection will be closed and also that the PX4 background process gets killed (if it was auto-initialized)
        """

        # If the simulation was already stoped, then ignore the function call
        if self._is_running == False:
            return

        # Set the flag so that we are no longer running the mavlink interface
        self._is_running = False

        # ============================================================================
        # 【关键】关闭顺序不能修改！必须先杀 PX4，再关闭 MAVLink 连接
        # ============================================================================
        # 原因：
        # 1. Isaac Sim 端是 TCP 服务器（tcpin），PX4 端是 TCP 客户端
        # 2. 如果先关闭服务端连接，PX4 客户端仍在运行，会导致：
        #    - PX4 端 TCP 连接进入 CLOSE_WAIT 状态
        #    - Isaac Sim 端 accepted socket 进入 TIME_WAIT 状态（约 60 秒）
        #    - 新的 bind() 调用失败，报 "Address already in use"
        # 3. 正确顺序：先让 PX4 关闭其 TCP 客户端，再关闭服务端
        # ============================================================================

        # Step 1: 先杀死 PX4 进程，让其 simulator_mavlink 模块关闭 TCP 客户端连接
        if self.px4_tool is not None:
            carb.log_info(f"{self._log_prefix} Attempting to gracefully stop PX4 background process")
            if self.px4_autolaunch:
                try:
                    self.px4_tool.kill_px4_save()  # 使用优雅关闭以保存 ULG 日志
                except Exception as e:
                    ts_log(self._log_prefix, f"PX4 graceful stop failed: {e}", "WARN")
                    ts_log(self._log_prefix, traceback.format_exc(), "WARN")
            self.px4_tool = None

        # Step 2: 等待 PX4 的 TCP 连接完全关闭
        time.sleep(0.5)

        # Step 3: 关闭 Isaac Sim 端的 MAVLink TCP 服务
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as e:
                ts_log(self._log_prefix, f"MAVLink connection close failed: {e}", "WARN")
            self._connection = None

    def recover_px4(self):
        """
        PX4 崩溃恢复（原子操作）

        此方法用于 PX4 进程异常退出后的恢复，保持仿真循环运行。
        关键设计：不重置 _received_first_hearbeat，避免死锁。

        恢复步骤：
        1. 杀死旧 PX4 进程
        2. 关闭旧 MAVLink 连接
        3. 等待端口释放
        4. 创建新 MAVLink 连接（先于 PX4 启动）
        5. 启动新 PX4 进程
        """
        ts_log(self._log_prefix, "=" * 50)
        ts_log(self._log_prefix, "Starting PX4 recovery")
        ts_log(self._log_prefix, "=" * 50)

        try:
            # Step 1: 杀死旧 PX4 进程
            ts_log(self._log_prefix, "Step 1: Killing old PX4 process...")
            if self.px4_tool is not None:
                try:
                    self.px4_tool.kill_px4_save()
                    ts_log(self._log_prefix, "PX4 process killed successfully")
                except Exception as e:
                    ts_log(self._log_prefix, f"Kill PX4 failed: {e}", "WARN")
                    ts_log(self._log_prefix, traceback.format_exc(), "WARN")
            else:
                # 尝试通过 PID 文件杀死
                import os, signal
                if os.path.exists(self._px4_pid_path):
                    try:
                        with open(self._px4_pid_path, "r") as f:
                            pid = int(f.read().strip() or "0")
                        if pid > 0:
                            try:
                                os.kill(pid, signal.SIGKILL)
                                ts_log(self._log_prefix, f"Killed PX4 via PID file (pid={pid})")
                            except ProcessLookupError:
                                ts_log(self._log_prefix, f"PX4 process (pid={pid}) already dead")
                            except Exception as e:
                                ts_log(self._log_prefix, f"SIGKILL PX4 failed: {e}", "WARN")
                                ts_log(self._log_prefix, traceback.format_exc(), "WARN")
                        try:
                            os.remove(self._px4_pid_path)
                        except Exception as e:
                            ts_log(self._log_prefix, f"Remove PX4 pid file failed: {e}", "WARN")
                            ts_log(self._log_prefix, traceback.format_exc(), "WARN")
                    except Exception as e:
                        ts_log(self._log_prefix, f"Read PID file failed: {e}", "WARN")
                        ts_log(self._log_prefix, traceback.format_exc(), "WARN")

            # Step 1.5: 重置关键标志（必须在关闭连接之前！）
            # 这样可以防止 poll_mavlink_messages() 使用 blocking=True 模式
            # 避免物理线程在连接关闭时阻塞或抛出异常
            ts_log(self._log_prefix, "Step 1.5: Resetting flags before closing connection...")
            self._received_first_actuator = False
            self._received_actuator = False
            self._is_running = False  # 告诉 update() 我们正在恢复中

            # 关键修复：先保存旧连接引用，然后立即设置 _connection = None
            # 这样物理线程的 update() 会立即返回，避免使用正在关闭的连接
            old_connection = self._connection
            self._connection = None  # 触发 update() 中的 early return

            # 短暂等待，让物理线程有机会看到 _connection = None 并退出
            time.sleep(0.1)

            # Step 2: 关闭旧 MAVLink 连接
            ts_log(self._log_prefix, "Step 2: Closing old MAVLink connection...")
            if old_connection is not None:
                try:
                    old_connection.close()
                    ts_log(self._log_prefix, "MAVLink connection closed")
                except Exception as e:
                    ts_log(self._log_prefix, f"Close MAVLink failed: {e}", "WARN")
                    ts_log(self._log_prefix, traceback.format_exc(), "WARN")

            # Step 3: 等待端口释放
            mavlink_port = self.config.connection_baseport + self._vehicle_id
            ts_log(self._log_prefix, f"Step 3: Waiting for port {mavlink_port} to be released...")
            if _wait_for_port_release(mavlink_port, timeout=30.0, interval=0.5):
                ts_log(self._log_prefix, f"Port {mavlink_port} released")
            else:
                ts_log(self._log_prefix, f"Port {mavlink_port} release timeout, continuing anyway", "WARN")

            # 清理旧 PID 文件
            import os
            if os.path.exists(self._px4_pid_path):
                try:
                    os.remove(self._px4_pid_path)
                    ts_log(self._log_prefix, "Removed stale PID file")
                except Exception as e:
                    ts_log(self._log_prefix, f"Remove PID file failed: {e}", "WARN")
                    ts_log(self._log_prefix, traceback.format_exc(), "WARN")

            # 额外等待确保资源完全释放
            time.sleep(1.0)

            # Step 4: 创建新 MAVLink 连接（先于 PX4 启动，避免连接竞争）
            # 使用重试机制处理 "Address already in use" 错误（端口可能在 TIME_WAIT 状态）
            ts_log(self._log_prefix, f"Step 4: Creating MAVLink connection: {self._connection_port}")
            max_retries = 10
            retry_delay = 1.0  # 初始重试延迟（秒）

            for attempt in range(max_retries):
                try:
                    self._connection = mavutil.mavlink_connection(self._connection_port)
                    ts_log(self._log_prefix, f"MAVLink connection created (attempt {attempt + 1})")
                    break
                except OSError as e:
                    if e.errno == 98 and attempt < max_retries - 1:  # Address already in use
                        ts_log(self._log_prefix, f"Port still busy, retrying in {retry_delay:.1f}s... (attempt {attempt + 1}/{max_retries})", "WARN")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 5.0)  # 指数退避，最大5秒
                    else:
                        raise

            # 重置必要的状态
            # 关键修复：必须重置 _received_first_hearbeat，否则 update() 中不会调用
            # wait_for_first_hearbeat()，导致 tcpin 连接不会 accept 新 PX4 的连接
            self._received_first_hearbeat = False
            self._received_first_actuator = False
            self._received_actuator = False
            # 关键修复：不重置 _sensor_data，保留当前传感器数据
            # 只重置 new_*_data 标志，让下一次 update_sensor 调用时更新数据
            self._sensor_data.new_imu_data = False
            self._sensor_data.new_gps_data = False
            self._sensor_data.new_bar_data = False
            self._sensor_data.new_mag_data = False
            self._sensor_data.new_press_data = False
            self._sensor_data.new_vision_data = False
            self._sensor_data.new_sim_state = False
            # 注意：保留 received_first_imu = True（如果之前已接收过）
            # 这样 lockstep 机制会正确等待新的 IMU 数据
            self._last_heartbeat_sent_time = 0

            # 关键修复：重置仿真时间戳，新 PX4 进程期望从 0 开始的时间戳
            # 否则会导致 PX4 检测到 "Time jump"，EKF 重置，姿态估计失败
            self._current_utime = 0
            # 关键修复：标记需要跳过下一个大的 dt 值
            # 因为恢复过程中物理循环暂停，恢复后第一个 dt 可能很大（30+ 秒）
            # 这会导致 _current_utime 立即跳到很大的值，触发 PX4 的 "Time jump" 检测
            self._skip_large_dt_count = 10  # 跳过前 10 个帧的大 dt
            ts_log(self._log_prefix, "Reset simulation time (_current_utime = 0, skip_large_dt_count = 10)")

            # 标记为运行状态
            self._is_running = True

            # 重置调试计数器以便追踪恢复后的 update() 调用
            self._update_call_count = 0

            # Step 5: 启动新 PX4 进程
            ts_log(self._log_prefix, "Step 5: Launching new PX4 process...")
            self.px4_tool = PX4LaunchTool(
                self.px4_dir,
                self._vehicle_id,
                self.px4_vehicle_model,
                self.config.sim_speed_factor,
                log_dir=self._px4_log_dir
            )
            self.px4_tool.launch_px4()

            # 重置启动保护期计时器
            self._px4_start_time = time.time()

            # Step 6: 设置传感器标志（不等待，因为物理循环被阻塞）
            # 关键修复：HTTP handler 与物理循环同步，等待会阻塞物理循环，
            # 导致 PX4 无法收到传感器数据（poll timeout），进入不正常状态
            ts_log(self._log_prefix, "Step 6: Setting sensor flags (no wait - physics blocked)")
            self._sensor_data.received_first_imu = True  # 让物理循环恢复后处理

            # Step 7: 跳过等待（物理循环恢复后会处理连接）
            # 关键修复：不要在这里等待，因为物理循环被阻塞，PX4 无法发送 heartbeat
            # 物理循环恢复后，update() 会调用 wait_for_first_hearbeat() 接受连接
            ts_log(self._log_prefix, "Step 7: Skipping heartbeat wait (physics thread will handle)")

            ts_log(self._log_prefix, "=" * 50)
            ts_log(self._log_prefix, "PX4 recovery completed")
            ts_log(self._log_prefix, "=" * 50)

        except Exception as e:
            ts_log(self._log_prefix, f"PX4 recovery failed: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")
            raise

    def reset(self):
        """For now does nothing. Here for compatibility purposes only
        """
        return

    @property
    def px4_ready_to_takeoff(self) -> bool:
        """获取 PX4 ready to takeoff 状态（从 PX4 日志检测）"""
        if self.px4_tool is not None:
            return self.px4_tool.ready_to_takeoff
        return False

    def get_px4_log_file_path(self) -> str:
        """获取 PX4 日志文件路径"""
        return PX4LaunchTool.log_file_path(self._vehicle_id)

    def re_initialize_interface(self):
        """Auxiliar method used to get the MavlinkInterface to reset the MavlinkInterface to its initial state
        """

        self._is_running = False

        # 关键修复：先关闭旧连接，释放端口
        if self._connection is not None:
            try:
                ts_log(self._log_prefix, "Closing old MAVLink connection...")
                self._connection.close()
            except Exception as e:
                ts_log(self._log_prefix, f"Failed to close old connection: {e}", "WARN")
            self._connection = None

        # Restart the sensor data
        self._sensor_data = SensorMsg()

        # 检查并等待端口释放
        mavlink_port = self.config.connection_baseport + self._vehicle_id
        if _is_port_in_use(mavlink_port):
            ts_log(self._log_prefix, f"MAVLink port {mavlink_port} is in use, waiting for release...")
            if _wait_for_port_release(mavlink_port, timeout=30.0, interval=0.5):
                ts_log(self._log_prefix, f"MAVLink port {mavlink_port} released")
            else:
                ts_log(self._log_prefix, f"MAVLink port {mavlink_port} release timeout, attempting connection anyway", "WARN")

        # Restart the connection (with retry logic for TIME_WAIT state)
        ts_log(self._log_prefix, f"Creating MAVLink connection to {self._connection_port}")
        max_retries = 15
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                self._connection = mavutil.mavlink_connection(self._connection_port)
                if attempt > 0:
                    ts_log(self._log_prefix, f"MAVLink connection created (attempt {attempt + 1})")
                break
            except OSError as e:
                if e.errno == 98 and attempt < max_retries - 1:  # Address already in use
                    ts_log(self._log_prefix, f"Port busy (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay:.1f}s...", "WARN")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 5.0)  # 指数退避，最大5秒
                else:
                    raise

        # Auxiliar variables to handle the lockstep between receiving sensor data and actuator control
        self._received_first_actuator: bool = False
        self._received_actuator: bool = False

        # Auxiliar variables to check if we have already received an hearbeat from the software in the loop simulation
        self._received_first_hearbeat: bool = False

        self._last_heartbeat_sent_time = 0
        # Reset throttle for waiting heartbeat log
        self._last_waiting_heartbeat_log_time = 0

    def wait_for_first_hearbeat(self):
        """
        Responsible for waiting for the first hearbeat. This method is locking and will only return
        if an hearbeat is received via mavlink. When this first heartbeat is received poll for mavlink messages
        """

        # Wait for the connection to be established
        if self._connection is None:
            return

        # Throttle waiting heartbeat logs
        now = time.time()
        if (now - self._last_waiting_heartbeat_log_time) >= self._waiting_heartbeat_log_interval:
            ts_log(self._log_prefix, "Waiting for first heartbeat from PX4...", "WARN")
            self._last_waiting_heartbeat_log_time = now
        result = self._connection.wait_heartbeat(blocking=False)

        if result is not None:
            self._received_first_hearbeat = True
            ts_log(self._log_prefix, "Received first heartbeat from PX4")

    def update(self, dt):
        """
        Method that is called at every physics step to send data to px4 and receive the control inputs via mavlink

        Args:
            dt (float): The time elapsed between the previous and current function calls (s).
        """
        # 关键修复：包装整个 update() 方法在 try-except 中，防止异常传播到 Isaac Sim
        # 导致整个物理回调系统崩溃
        try:
            self._update_impl(dt)
        except Exception as e:
            ts_log(self._log_prefix, f"update() exception: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")

    def _update_impl(self, dt):
        # 调试：每次 update() 调用都记录（用于诊断回调是否正常工作）
        if not hasattr(self, '_update_call_count'):
            self._update_call_count = 0
        self._update_call_count += 1
        # if self._update_call_count % 120 == 1:  # 约每秒一次
        #     carb.log_warn(f"{self._log_prefix} update() entry #{self._update_call_count}: "
        #                  f"_is_running={self._is_running}, "
        #                  f"_connection={'OK' if self._connection else 'None'}")

        # Ensure PX4 process is running (auto-recover if killed externally)
        self._ensure_px4_running_periodic()

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Check for the first hearbeat on the first few iterations
        # 关键修复：不要在等待 heartbeat 时提前返回，因为 PX4 在 lockstep 模式下
        # 需要先收到传感器数据才会发送 heartbeat
        if not self._received_first_hearbeat:
            self.wait_for_first_hearbeat()
            # 不要 return，继续发送传感器数据以打破死锁

        # 调试：定期打印连接状态
        # if not hasattr(self, '_debug_log_count'):
        #     self._debug_log_count = 0
        # self._debug_log_count += 1
        # if self._debug_log_count % 500 == 1:  # 每500帧打印一次
        #     carb.log_warn(f"{self._log_prefix} update() called: heartbeat={self._received_first_hearbeat}, "
        #                  f"first_imu={self._sensor_data.received_first_imu}, "
        #                  f"new_imu={self._sensor_data.new_imu_data}, "
        #                  f"imu_data=({self._sensor_data.xacc:.2f},{self._sensor_data.yacc:.2f},{self._sensor_data.zacc:.2f}), "
        #                  f"is_running={self._is_running}")

        # Check if we have already received IMU data. If not, start the lockstep and wait for more data
        # 关键修复：添加超时机制防止无限等待
        if self._sensor_data.received_first_imu:
            if not self._sensor_data.new_imu_data and self._is_running:
                # 检查 IMU 数据是否有效（非零）- 即使 new_imu_data 未设置
                imu_has_valid_data = (
                    abs(self._sensor_data.xacc) > 0.01 or
                    abs(self._sensor_data.yacc) > 0.01 or
                    abs(self._sensor_data.zacc) > 0.01
                )
                if not imu_has_valid_data:
                    # 没有有效 IMU 数据，等待下一个物理步
                    return
                # 有有效数据但 new_imu_data=False，可能是恢复后的第一帧
                # 继续发送数据

        # Check if we have received any mavlink messages
        self.poll_mavlink_messages()

        # Send hearbeats at 1Hz
        if (time.time() - self._last_heartbeat_sent_time) > 1.0 or self._received_first_hearbeat == False:
            self.send_heartbeat()
            self._last_heartbeat_sent_time = time.time()

        # Update the current u_time for px4
        # 关键修复：恢复后的前几帧使用固定小 dt，防止时间跳跃
        if hasattr(self, '_skip_large_dt_count') and self._skip_large_dt_count > 0:
            self._skip_large_dt_count -= 1
            # 使用固定的小 dt（基于配置的更新频率）
            dt = self._time_step
            if self._skip_large_dt_count == 9:  # 第一次跳过时打印日志
                ts_log(self._log_prefix, f"Skipping large dt, using fixed dt={dt:.6f}s")

        self._current_utime += int(dt * 1000000)

        # Send sensor messages
        self.send_sensor_msgs(self._current_utime)

        # Send the GPS messages
        self.send_gps_msgs(self._current_utime)

    def _is_px4_alive(self) -> bool:
        """检查 PX4 进程是否存活。优先检查 px4_tool.px4_process，其次检查 PID 文件。"""
        try:
            # 优先检查 px4_tool 管理的进程
            if self.px4_tool is not None and self.px4_tool.px4_process is not None:
                # poll() 返回 None 表示进程还在运行
                if self.px4_tool.px4_process.poll() is None:
                    return True
                else:
                    return False

            # 回退到 PID 文件检查（用于外部启动的 PX4）
            import os, signal
            if not os.path.exists(self._px4_pid_path):
                return False
            with open(self._px4_pid_path, "r") as f:
                pid = int((f.read().strip() or "0"))
            if pid <= 0:
                return False
            try:
                os.kill(pid, 0)
                return True
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            except Exception:
                return False
        except Exception as e:
            ts_log(self._log_prefix, f"_is_px4_alive error: {e}", "WARN")
            ts_log(self._log_prefix, traceback.format_exc(), "WARN")
            return False

    def _ensure_px4_running_periodic(self):
        # Throttle checks to avoid spamming
        now = time.time()
        if not hasattr(self, "_last_px4_check_time"):
            self._last_px4_check_time = 0.0
            self._px4_check_interval = 5.0  # 检查间隔增加到 5 秒
            self._px4_startup_grace_period = 30.0  # 启动后 30 秒内不检查
            self._px4_start_time = now  # 记录启动时间

        # 启动保护期：刚启动后一段时间内不检查
        if (now - self._px4_start_time) < self._px4_startup_grace_period:
            return

        if (now - self._last_px4_check_time) < self._px4_check_interval:
            return
        self._last_px4_check_time = now

        if not self.px4_autolaunch:
            return
        if self._is_px4_alive():
            return
        try:
            ts_log(self._log_prefix, "PX4 not alive; attempting auto-recovery...", "WARN")
            self.recover_px4()
            self._px4_start_time = time.time()  # 重置启动时间
        except Exception as e:
            ts_log(self._log_prefix, f"Auto-recovery failed: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")

    def poll_mavlink_messages(self):
        """
        Method that is used to check if new mavlink messages were received
        """

        # If we have not received the first hearbeat yet, do not poll for mavlink messages
        if self._received_first_hearbeat == False:
            return

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Check if we need to lock and wait for actuator control data
        needs_to_wait_for_actuator: bool = self._received_first_actuator and self._enable_lockstep

        # Start by assuming that we have not received data for the actuators for the current step
        self._received_actuator = False

        # Use this loop to emulate a do-while loop (make sure this runs at least once)
        try:
            while True:
                # Guard: re-check connection and running state in case recovery started
                if self._connection is None or not self._is_running:
                    break

                # 关键修复：使用超时防止 blocking recv 无限阻塞
                # 如果需要等待 actuator，使用 0.1 秒超时而不是无限阻塞
                # 这样可以在恢复期间及时检测到 _connection=None 或 _is_running=False
                recv_timeout = 0.1 if needs_to_wait_for_actuator else None

                # Try to get a message
                msg = self._connection.recv_match(blocking=needs_to_wait_for_actuator, timeout=recv_timeout)

                # If a message was received
                if msg is not None:

                    # Check if it is of the type that contains actuator controls
                    if msg.id == mavutil.mavlink.MAVLINK_MSG_ID_HIL_ACTUATOR_CONTROLS:

                        self._received_first_actuator = True
                        self._received_actuator = True

                        # Handle the control of the actuation commands received by PX4
                        self.handle_control(msg.time_usec, msg.controls, msg.mode, msg.flags)

                # Check if we do not need to wait for an actuator message or we just received actuator input
                # If so, break out of the infinite loop
                if not needs_to_wait_for_actuator or self._received_actuator:
                    break
        except Exception as e:
            # 连接可能在恢复过程中被关闭，导致 recv_match 抛出异常
            # 这是正常行为，不需要报错
            if self._connection is None or not self._is_running:
                pass  # 恢复中，忽略异常
            else:
                carb.log_warn(f"{self._log_prefix} poll_mavlink_messages exception: {e}")

    def send_heartbeat(self, mav_type=mavutil.mavlink.MAV_TYPE_GENERIC):
        """
        Method that is used to publish an heartbear through mavlink protocol

        Args: 
            mav_type (int): The ID that indicates the type of vehicle. Defaults to MAV_TYPE_GENERIC=0 
        """

        carb.log_info(f"{self._log_prefix} Sending heartbeat")

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Note: to know more about these functions, go to pymavlink->dialects->v20->standard.py
        # This contains the definitions for sending the hearbeat and simulated sensor messages
        self._connection.mav.heartbeat_send(mav_type, mavutil.mavlink.MAV_AUTOPILOT_INVALID, 0, 0, 0)

    def send_sensor_msgs(self, time_usec: int):
        """
        Method that when invoked, will send the simulated sensor data through mavlink

        Args:
            time_usec (int): The total time elapsed since the simulation started
        """
        carb.log_info(f"{self._log_prefix} Sending sensor msgs")

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Check which sensors have new data to send
        fields_updated: int = 0

        # 关键修复：除了检查 new_imu_data 标志外，还检查数据是否有效
        # 这样在恢复后即使标志未设置，只要有有效数据也能发送
        imu_has_valid_data = (
            abs(self._sensor_data.xacc) > 0.01 or
            abs(self._sensor_data.yacc) > 0.01 or
            abs(self._sensor_data.zacc) > 0.01
        )
        if self._sensor_data.new_imu_data or imu_has_valid_data:
            # Set the bit field to signal that we are sending updated accelerometer and gyro data
            fields_updated = fields_updated | SensorSource.ACCEL | SensorSource.GYRO
            self._sensor_data.new_imu_data = False

        if self._sensor_data.new_mag_data:
            # Set the bit field to signal that we are sending updated magnetometer data
            fields_updated = fields_updated | SensorSource.MAG
            self._sensor_data.new_mag_data = False

        if self._sensor_data.new_bar_data:
            # Set the bit field to signal that we are sending updated barometer data
            fields_updated = fields_updated | SensorSource.BARO
            self._sensor_data.new_bar_data = False

        if self._sensor_data.new_press_data:
            # Set the bit field to signal that we are sending updated diff pressure data
            fields_updated = fields_updated | SensorSource.DIFF_PRESS
            self._sensor_data.new_press_data = False

        try:
            self._connection.mav.hil_sensor_send(
                time_usec,
                self._sensor_data.xacc,
                self._sensor_data.yacc,
                self._sensor_data.zacc,
                self._sensor_data.xgyro,
                self._sensor_data.ygyro,
                self._sensor_data.zgyro,
                self._sensor_data.xmag,
                self._sensor_data.ymag,
                self._sensor_data.zmag,
                self._sensor_data.abs_pressure,
                self._sensor_data.diff_pressure,
                self._sensor_data.pressure_alt,
                self._sensor_data.altitude,
                fields_updated,
            )
        except:
            carb.log_warn(f"{self._log_prefix} Could not send sensor data through mavlink")

    def send_gps_msgs(self, time_usec: int):
        """
        Method that is used to send simulated GPS data through the mavlink protocol.

        Args:
            time_usec (int): The total time elapsed since the simulation started
        """
        # # 调试：跟踪GPS发送调用（使用warn级别确保可见）
        # if not hasattr(self, '_gps_send_count'):
        #     self._gps_send_count = 0
        # self._gps_send_count += 1
        # if self._gps_send_count % 500 == 1:  # 每500次调用打印一次
        #     carb.log_warn(f"{self._log_prefix} send_gps_msgs called #{self._gps_send_count}, "
        #                  f"new_gps_data={self._sensor_data.new_gps_data}, "
        #                  f"lat={self._sensor_data.latitude_deg}, lon={self._sensor_data.longitude_deg}")

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Do not send GPS data, if no new data was received
        if not self._sensor_data.new_gps_data:
            return

        self._sensor_data.new_gps_data = False

        # Latitude, longitude and altitude (all in integers)
        try:
            self._connection.mav.hil_gps_send(
                time_usec,
                self._sensor_data.fix_type,
                self._sensor_data.latitude_deg,
                self._sensor_data.longitude_deg,
                self._sensor_data.altitude,
                self._sensor_data.eph,
                self._sensor_data.epv,
                self._sensor_data.velocity,
                self._sensor_data.velocity_north,
                self._sensor_data.velocity_east,
                self._sensor_data.velocity_down,
                self._sensor_data.cog,
                self._sensor_data.satellites_visible,
            )
        except:
            carb.log_warn(f"{self._log_prefix} Could not send gps data through mavlink")

    def send_vision_msgs(self, time_usec: int):
        """
        Method that is used to send simulated vision/mocap data through the mavlink protocol.

        Args:
            time_usec (int): The total time elapsed since the simulation started
        """
        carb.log_info(f"{self._log_prefix} Sending vision/mocap msgs")

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Do not send vision/mocap data, if not new data was received
        if not self._sensor_data.new_vision_data:
            return

        self._sensor_data.new_vision_data = False

        try:
            self._connection.mav.global_vision_position_estimate_send(
                time_usec,
                self._sensor_data.vision_x,
                self._sensor_data.vision_y,
                self._sensor_data.vision_z,
                self._sensor_data.vision_roll,
                self._sensor_data.vision_pitch,
                self._sensor_data.vision_yaw,
                self._sensor_data.vision_covariance,
            )
        except:
            carb.log_warn(f"{self._log_prefix} Could not send vision/mocap data through mavlink")

    def send_ground_truth(self, time_usec: int):
        """
        Method that is used to send the groundtruth data of the vehicle through mavlink

        Args:
            time_usec (int): The total time elapsed since the simulation started
        """

        carb.log_info(f"{self._log_prefix} Sending groundtruth msgs")

        # Guard: mavlink connection may be None during hard reset/relaunch
        if self._connection is None:
            return

        # Do not send vision/mocap data, if not new data was received
        if not self._sensor_data.new_sim_state or self._sensor_data.sim_alt == 0:
            return

        self._sensor_data.new_sim_state = False

        try:
            self._connection.mav.hil_state_quaternion_send(
                time_usec,
                self._sensor_data.sim_attitude,
                self._sensor_data.sim_angular_vel[0],
                self._sensor_data.sim_angular_vel[1],
                self._sensor_data.sim_angular_vel[2],
                self._sensor_data.sim_lat,
                self._sensor_data.sim_lon,
                self._sensor_data.sim_alt,
                self._sensor_data.sim_velocity_inertial[0],
                self._sensor_data.sim_velocity_inertial[1],
                self._sensor_data.sim_velocity_inertial[2],
                self._sensor_data.sim_ind_airspeed,
                self._sensor_data.sim_true_airspeed,
                self._sensor_data.sim_acceleration[0],
                self._sensor_data.sim_acceleration[1],
                self._sensor_data.sim_acceleration[2],
            )
        except:
            carb.log_warn(f"{self._log_prefix} Could not send groundtruth through mavlink")

    def handle_control(self, time_usec, controls, mode, flags):
        """
        Method that when received a control message, compute the forces simulated force that should be applied
        on each rotor of the vehicle

        Args:
            time_usec (int): The total time elapsed since the simulation started - Ignored argument
            controls (list): A list of ints which contains the thrust_control received via mavlink
            flags: Ignored argument
        """

        # Check if the vehicle is armed - Note: here we have to add a +1 since the code for armed is 128, but
        # pymavlink is return 129 (the end of the buffer)
        if mode == mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED + 1:

            carb.log_info(f"{self._log_prefix} Parsing control input")

            # Set the rotor target speeds
            self._rotor_data.update_input_reference(controls)

        # If the vehicle is not armed, do not rotate the propellers
        else:
            self._rotor_data.zero_input_reference()

    def update_graphical_sensor(self, sensor_type: str, data):
        """Method that when implemented, should handle the receival of graphical sensor data

        Args:
            sensor_type (str): A name that describes the type of sensor (for example MonocularCamera)
            data (dict): A dictionary that contains the data produced by the sensor
        """
        pass
 
