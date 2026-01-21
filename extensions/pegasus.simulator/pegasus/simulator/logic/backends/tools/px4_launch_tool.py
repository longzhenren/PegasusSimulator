# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
# Copyright (c) 2024-2026 amurzzb@gmail.com
# Licensed under the MIT License
"""
PX4 启动工具（px4_launch_tool.py）

==========================
概述
==========================
本模块提供 PX4 SITL（Software In The Loop）进程的生命周期管理，负责：
- 启动和停止 PX4 SITL 进程
- 管理 PX4 临时文件系统和日志
- 监控 PX4 日志以检测 "Ready for takeoff" 状态
- 保存 PX4 飞行日志（ULG 文件）到永久目录

==========================
类结构
==========================
PX4LaunchTool
  - 管理单个 PX4 SITL 实例
  - 每个 UAV 对应一个 PX4LaunchTool 实例

==========================
关键属性
==========================
px4_process      - subprocess.Popen 对象，PX4 进程句柄
vehicle_id       - 载具 ID（用于端口分配和日志区分）
px4_dir          - PX4-Autopilot 源码目录
rc_script        - PX4 启动脚本路径（rcS）
root_fs          - 临时文件系统目录（PX4 运行时数据）
environment      - PX4 进程环境变量
ready_to_takeoff - PX4 就绪状态（通过日志监控）

==========================
环境变量（传递给 PX4）
==========================
PX4_SIM_MODEL        - 载具模型名称（如 gazebo-classic_iris_pg）
PX4_UXRCE_DDS_PORT   - ROS2 DDS 端口（8888 + vehicle_id）
PX4_UXRCE_DDS_NS     - ROS2 DDS 命名空间（px4_{vehicle_id+1}）
PX4_SIM_SPEED_FACTOR - 仿真加速倍率

==========================
端口规划
==========================
4560 + vehicle_id    - MAVLink TCP lockstep 端口
8888 + vehicle_id    - ROS2 UXRCE-DDS 端口

==========================
目录结构
==========================
/tmp/pegasus_px4_sitl/
  └── px4_{vehicle_id}.pid    # PID 文件（用于进程管理）

/tmp/pegasus_px4_logs/
  └── px4_{vehicle_id}.log    # PX4 控制台输出日志

/tmp/px4_{vehicle_id}_*/
  └── log/
      └── *.ulg               # PX4 飞行日志（临时）

{cwd}/px4_logs/uav{vehicle_id}/
  └── */
      └── *.ulg               # PX4 飞行日志（永久保存）

==========================
主要方法
==========================
launch_px4()
  - 启动 PX4 SITL 进程
  - 创建临时文件系统
  - 启动日志监控线程
  - 写入 PID 文件

kill_px4()
  - 强制终止 PX4 进程（SIGKILL）
  - 停止日志监控
  - 清理 PID 文件

kill_px4_save()
  - 优雅关闭 PX4 进程（SIGINT → 等待 → SIGKILL）
  - 等待 PX4 保存飞行日志
  - 复制日志到永久目录
  - 清理临时文件

ready_to_takeoff (property)
  - 返回 PX4 是否处于 "Ready for takeoff" 状态
  - 通过日志监控线程实时更新

==========================
日志监控
==========================
日志监控线程 (_log_monitor_worker) 功能：
- 实时读取 PX4 日志文件
- 检测 "Ready for takeoff" 或 "Ready to takeoff" 关键字
- 检测到后设置 ready_to_takeoff = True

==========================
调用关系
==========================
PX4MavlinkBackend
       │
       ▼
PX4LaunchTool ───────► PX4 SITL 进程
       │                    │
       │                    ▼
       │              /tmp/px4_logs/px4_{id}.log
       │                    │
       ▼                    │
日志监控线程 ◄──────────────┘

==========================
使用示例
==========================
from px4_launch_tool import PX4LaunchTool

# 创建 PX4 启动工具
tool = PX4LaunchTool(
    px4_dir="/home/user/PX4-Autopilot",
    vehicle_id=0,
    px4_model="gazebo-classic_iris_pg",
    sim_speed_factor=2.0
)

# 启动 PX4
tool.launch_px4()

# 等待 PX4 就绪
import time
for i in range(60):
    if tool.ready_to_takeoff:
        print(f"Ready after {i}s")
        break
    time.sleep(1)

# 优雅关闭（保存日志）
tool.kill_px4_save()

==========================
注意事项
==========================
1. PX4 需要预先编译：make px4_sitl_default none
2. 临时文件系统在对象销毁时自动清理
3. 飞行日志保存需要调用 kill_px4_save()，不要用 kill_px4()
4. ready_to_takeoff 状态通过后台线程更新，非阻塞
5. 多个 PX4 实例可并行运行，通过 vehicle_id 区分

==========================
原始文件信息
==========================
| File: px4_launch_tool.py
| Author: Marcelo Jacinto (marcelo.jacinto@tecnico.ulisboa.pt)
| Description: Defines an auxiliary tool to launch the PX4 process in the background
| License: BSD-3-Clause. Copyright (c) 2023, Marcelo Jacinto. All rights reserved.
"""

# System tools used to launch the px4 process in the brackground
import os
import re
import shutil
import tempfile
import subprocess
import traceback
import signal
import threading
import time
from datetime import datetime


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """
    生成带时间戳的日志消息并输出

    Args:
        prefix: 日志前缀（如 [PX4LaunchTool uav0]）
        message: 日志内容
        level: 日志级别 (INFO, WARN, ERROR, DEBUG)

    Returns:
        格式化的日志字符串
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg)
    return log_msg


PX4_PID_DIR = "/tmp/pegasus_px4_sitl"


def cleanup_px4_residuals(vehicle_id: int = None, kill_processes: bool = True):
    """
    清理PX4残留文件和进程

    【重要】解决 "PX4 server already running for instance X" 问题

    清理内容：
    1. PX4 锁文件 (/tmp/px4_instance_*, /tmp/px4-*)
    2. PX4 临时目录 (/tmp/px4_*/)
    3. 残留的 PX4 进程（可选）

    Args:
        vehicle_id: 如果指定，只清理该 UAV 的文件；否则清理所有
        kill_processes: 是否杀死残留的 PX4 进程

    Returns:
        清理的文件/进程数量
    """
    import glob
    cleaned_count = 0

    # 1. 清理 PX4 锁文件
    lock_patterns = [
        "/tmp/px4_instance_*",
        "/tmp/px4-*",
    ]

    if vehicle_id is not None:
        # 只清理指定 UAV 的锁文件
        lock_patterns = [
            f"/tmp/px4_instance_{vehicle_id}_*",
            f"/tmp/px4-{vehicle_id}*",
        ]

    for pattern in lock_patterns:
        for lock_file in glob.glob(pattern):
            try:
                if os.path.isfile(lock_file):
                    os.remove(lock_file)
                    ts_log("[PX4Cleanup]", f"Removed lock file: {lock_file}")
                    cleaned_count += 1
                elif os.path.isdir(lock_file):
                    shutil.rmtree(lock_file)
                    ts_log("[PX4Cleanup]", f"Removed lock dir: {lock_file}")
                    cleaned_count += 1
            except Exception as e:
                ts_log("[PX4Cleanup]", f"Failed to remove {lock_file}: {e}", "WARN")

    # 2. 清理 PX4 PID 文件
    if vehicle_id is not None:
        pid_file = os.path.join(PX4_PID_DIR, f"px4_{vehicle_id}.pid")
        if os.path.exists(pid_file):
            try:
                os.remove(pid_file)
                ts_log("[PX4Cleanup]", f"Removed PID file: {pid_file}")
                cleaned_count += 1
            except Exception as e:
                ts_log("[PX4Cleanup]", f"Failed to remove {pid_file}: {e}", "WARN")
    else:
        # 清理所有 PID 文件
        for pid_file in glob.glob(os.path.join(PX4_PID_DIR, "px4_*.pid")):
            try:
                os.remove(pid_file)
                ts_log("[PX4Cleanup]", f"Removed PID file: {pid_file}")
                cleaned_count += 1
            except Exception as e:
                ts_log("[PX4Cleanup]", f"Failed to remove {pid_file}: {e}", "WARN")

    # 3. 杀死残留的 PX4 进程
    # 【修复】多机场景下，只杀死指定vehicle_id的PX4进程，避免误杀其他UAV的进程
    if kill_processes:
        try:
            import subprocess

            if vehicle_id is not None:
                # 【关键修复】只查找指定instance的PX4进程
                # PX4启动参数中使用 -i {vehicle_id}，所以匹配 "-i {vehicle_id}" 或 "-i{vehicle_id}"
                search_pattern = f'px4_sitl_default/bin/px4.*-i\\s*{vehicle_id}\\b'
                result = subprocess.run(
                    ['pgrep', '-f', search_pattern],
                    capture_output=True, text=True, timeout=5
                )
            else:
                # 清理所有PX4进程（仅在全局清理时使用）
                result = subprocess.run(
                    ['pgrep', '-f', 'px4_sitl_default/bin/px4'],
                    capture_output=True, text=True, timeout=5
                )

            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid_str in pids:
                    try:
                        pid = int(pid_str.strip())
                        if pid > 0:
                            os.kill(pid, signal.SIGKILL)
                            if vehicle_id is not None:
                                ts_log("[PX4Cleanup]", f"Killed PX4 process for UAV{vehicle_id}: PID {pid}")
                            else:
                                ts_log("[PX4Cleanup]", f"Killed orphan PX4 process: PID {pid}")
                            cleaned_count += 1
                    except (ValueError, ProcessLookupError):
                        pass
                    except Exception as e:
                        ts_log("[PX4Cleanup]", f"Failed to kill PID {pid_str}: {e}", "WARN")
        except FileNotFoundError:
            ts_log("[PX4Cleanup]", "pgrep not found, skipping process cleanup", "WARN")
        except subprocess.TimeoutExpired:
            ts_log("[PX4Cleanup]", "Process cleanup timed out", "WARN")
        except Exception as e:
            ts_log("[PX4Cleanup]", f"Process cleanup error: {e}", "WARN")

    if cleaned_count > 0:
        ts_log("[PX4Cleanup]", f"Cleanup complete: {cleaned_count} items removed")

    return cleaned_count


def _get_default_log_dir():
    """获取默认日志目录，优先使用 logs/<session_ts>/px4/，否则用 /tmp"""
    session_ts = os.environ.get("PEGASUS_SESSION_TS")
    if session_ts:
        cwd = os.getcwd()
        log_dir = os.path.join(cwd, "logs", session_ts, "px4")
        try:
            os.makedirs(log_dir, exist_ok=True)
            return log_dir
        except Exception as e:
            ts_log("[PX4LaunchTool]", f"Failed to create session log dir: {e}", "WARN")
    # 回退到 /tmp
    fallback = "/tmp/pegasus_px4_logs"
    try:
        os.makedirs(fallback, exist_ok=True)
    except Exception as e:
        ts_log("[PX4LaunchTool]", f"Failed to create fallback log dir: {e}", "WARN")
    return fallback

try:
    os.makedirs(PX4_PID_DIR, exist_ok=True)
except Exception as e:
    ts_log("[PX4LaunchTool]", f"Create PID dir failed: {e}", "ERROR")
    ts_log("[PX4LaunchTool]", traceback.format_exc(), "ERROR")


class PX4LaunchTool:
    """
    A class that manages the start/stop of a px4 process. It requires only the path to the PX4 installation (assuming that
    PX4 was already built with 'make px4_sitl_default none'), the vehicle id and the vehicle model.
    """

    def __init__(self, px4_dir, vehicle_id: int = 0, px4_model: str = "gazebo-classic_iris_pg", sim_speed_factor: float = 1.0, log_dir: str = None):
        """Construct the PX4LaunchTool object

        Args:
            px4_dir (str): A string with the path to the PX4-Autopilot directory
            vehicle_id (int): The ID of the vehicle. Defaults to 0.
            px4_model (str): The vehicle model. Defaults to "gazebo-classic_iris_pg".
            sim_speed_factor (float): The speed factor for the simulation. Defaults to 1.0.
            log_dir (str): Directory to save PX4 logs. If None, uses default from environment.
        """

        self._log_prefix = f"[PX4LaunchTool uav{vehicle_id}]"
        ts_log(self._log_prefix, f"Initializing, vehicle_id={vehicle_id}")
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
        self.environment = os.environ.copy()
        self.environment["PX4_SIM_MODEL"] = px4_model
        # Unique UXRCE-DDS port per instance
        self.environment["PX4_UXRCE_DDS_PORT"] = str(8888 + int(self.vehicle_id))
        # Namespace per instance (value only, rcS adds flags when invoking client)
        self.environment["PX4_UXRCE_DDS_NS"] = f"px4_{int(self.vehicle_id) + 1}"

        # Set the simulation speed factor
        self.environment["PX4_SIM_SPEED_FACTOR"] = str(sim_speed_factor)

        # PX4 日志输出目录和文件路径
        self._log_dir = log_dir or _get_default_log_dir()
        self._log_file_path = None  # 将在 launch_px4 时动态生成
        self._log_file = None
        self._launch_count = 0  # 启动计数，用于生成时间戳

        # Ready to takeoff 状态
        self._ready_to_takeoff = False
        self._ready_lock = threading.Lock()

        # 日志监控线程
        self._log_monitor_thread = None
        self._log_monitor_stop = threading.Event()

    @staticmethod
    def pid_file_path(vehicle_id: int):
        return os.path.join(PX4_PID_DIR, f"px4_{vehicle_id}.pid")

    @staticmethod
    def log_file_path(vehicle_id: int):
        """获取 PX4 当前日志文件路径（用于外部查询最新日志）"""
        log_dir = _get_default_log_dir()
        # 返回最新的日志文件路径（不带时间戳的主文件）
        return os.path.join(log_dir, f"px4_{vehicle_id}.log")

    def _generate_log_file_path(self):
        """生成日志文件路径（首次启动或重启时使用时间戳）"""
        os.makedirs(self._log_dir, exist_ok=True)

        if self._launch_count == 0:
            # 首次启动，不加时间戳
            return os.path.join(self._log_dir, f"px4_{self.vehicle_id}.log")
        else:
            # 重启时，添加时间戳
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            return os.path.join(self._log_dir, f"px4_{self.vehicle_id}_{timestamp}.log")

    @property
    def ready_to_takeoff(self) -> bool:
        """获取 PX4 ready to takeoff 状态"""
        with self._ready_lock:
            return self._ready_to_takeoff

    def _set_ready_to_takeoff(self, value: bool) -> None:
        """设置 ready to takeoff 状态"""
        with self._ready_lock:
            old_val = self._ready_to_takeoff
            self._ready_to_takeoff = value
            if value and not old_val:
                ts_log(self._log_prefix, "Ready to takeoff detected!")

    def _log_monitor_worker(self):
        """日志监控线程，检测 'Ready for takeoff' 关键字"""
        # Ready for takeoff 的正则匹配
        ready_pattern = re.compile(r"Ready\s+(for|to)\s+takeoff", re.IGNORECASE)

        try:
            # 等待日志文件创建
            wait_count = 0
            while not os.path.exists(self._log_file_path) and not self._log_monitor_stop.is_set():
                time.sleep(0.1)
                wait_count += 1
                if wait_count > 100:  # 10秒超时
                    ts_log(self._log_prefix, "Log file not created after 10s", "WARN")
                    return

            with open(self._log_file_path, "r") as f:
                # 从文件开头开始读取（不要 seek 到末尾，因为文件可能被截断重写）
                while not self._log_monitor_stop.is_set():
                    line = f.readline()
                    if line:
                        # 检测 Ready for takeoff
                        if ready_pattern.search(line):
                            self._set_ready_to_takeoff(True)
                    else:
                        # 没有新内容，短暂等待
                        time.sleep(0.05)
        except Exception as e:
            ts_log(self._log_prefix, f"Log monitor error: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")

    def _start_log_monitor(self):
        """启动日志监控线程"""
        self._log_monitor_stop.clear()
        self._ready_to_takeoff = False
        self._log_monitor_thread = threading.Thread(
            target=self._log_monitor_worker,
            name=f"px4_log_monitor_{self.vehicle_id}",
            daemon=True
        )
        self._log_monitor_thread.start()

    def _stop_log_monitor(self):
        """停止日志监控线程"""
        self._log_monitor_stop.set()
        if self._log_monitor_thread is not None:
            self._log_monitor_thread.join(timeout=2.0)
            self._log_monitor_thread = None

    def _ensure_port_free(self, port: int) -> bool:
        """
        确保指定端口可用，仅杀死占用端口的 PX4 相关进程
        
        【重要】仅杀死 PX4 相关进程，避免误杀仿真器主进程：
        - 只杀死进程名包含 "px4" 的进程
        - 跳过 python/isaac/sim 相关进程
        
        Args:
            port: 要检查的端口号
            
        Returns:
            True 如果端口现在可用，False 如果仍被占用
        """
        import socket
        import subprocess
        
        # 检查端口是否被占用
        def is_port_in_use(p):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(('127.0.0.1', p))
                    return False
                except OSError:
                    return True
        
        if not is_port_in_use(port):
            return True
            
        ts_log(self._log_prefix, f"Port {port} is in use, checking for orphan PX4 processes...")
        
        try:
            # 使用 lsof 查找占用端口的进程
            result = subprocess.run(
                ['lsof', '-t', '-i', f'TCP:{port}'],
                capture_output=True, text=True, timeout=5
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                killed_any = False
                for pid_str in pids:
                    try:
                        pid = int(pid_str.strip())
                        if pid <= 0:
                            continue
                        
                        # 获取进程命令行，检查是否是 PX4 进程
                        try:
                            with open(f"/proc/{pid}/cmdline", "r") as f:
                                cmdline = f.read().replace('\x00', ' ').lower()
                        except (FileNotFoundError, PermissionError):
                            # 进程可能已经退出
                            continue
                        
                        # 【关键】只杀死 PX4 相关进程，保护仿真器进程
                        is_px4_process = 'px4' in cmdline or 'px4_sitl' in cmdline
                        is_simulator = 'python' in cmdline or 'isaac' in cmdline or 'sim_vehicle' in cmdline
                        
                        if is_px4_process and not is_simulator:
                            ts_log(self._log_prefix, f"Killing orphan PX4 process: PID {pid}")
                            os.kill(pid, signal.SIGKILL)
                            killed_any = True
                        else:
                            ts_log(self._log_prefix, f"Skipping non-PX4 process: PID {pid} (protected)")
                            
                    except (ValueError, ProcessLookupError):
                        pass
                    except Exception as e:
                        ts_log(self._log_prefix, f"Failed to check/kill PID {pid_str}: {e}", "WARN")
                
                if killed_any:
                    time.sleep(0.5)
                    if not is_port_in_use(port):
                        ts_log(self._log_prefix, f"Port {port} successfully freed")
                        return True
            
            # 不再使用 fuser -k，避免误杀
            if is_port_in_use(port):
                ts_log(self._log_prefix, f"Port {port} still in use by non-PX4 process (OK for simulator)", "WARN")
                return False
            return True
                
        except FileNotFoundError:
            ts_log(self._log_prefix, "lsof not found, skipping port cleanup", "WARN")
        except subprocess.TimeoutExpired:
            ts_log(self._log_prefix, "Port cleanup command timed out", "WARN")
        except Exception as e:
            ts_log(self._log_prefix, f"Port cleanup error: {e}", "WARN")
        
        return not is_port_in_use(port)


    def _save_logs(self):
        """
        因为 self.root_fs 是临时目录，在 cleanup 之前必须把日志挪出来
        """
        save_path = os.path.join(os.getcwd(), "px4_logs", f"uav{self.vehicle_id}")
        os.makedirs(save_path, exist_ok=True)

        # PX4 的日志通常在临时目录下的 log/ 文件夹中
        log_src = os.path.join(self.root_fs.name, "log")
        if os.path.exists(log_src):
            # 拷贝整个日志文件夹
            for folder in os.listdir(log_src):
                src_folder = os.path.join(log_src, folder)
                dst_folder = os.path.join(save_path, folder)
                if os.path.isdir(src_folder):
                    if os.path.exists(dst_folder):
                        shutil.rmtree(dst_folder)
                    shutil.copytree(src_folder, dst_folder)
            ts_log(self._log_prefix, f"Logs saved to: {save_path}")


    def launch_px4(self):
        """
        Method that will launch a px4 instance with the specified configuration
        """
        # 重置 ready 状态
        self._set_ready_to_takeoff(False)

        # 生成日志文件路径（首次启动或重启时使用时间戳）
        self._log_file_path = self._generate_log_file_path()
        self._launch_count += 1

        # 【关键】清理 PX4 锁文件和残留进程，解决 "PX4 server already running" 问题
        ts_log(self._log_prefix, "Cleaning up PX4 residuals before launch...")
        cleanup_px4_residuals(vehicle_id=self.vehicle_id, kill_processes=True)

        # 清理旧的 PID 文件（避免残留导致启动检测失败）
        pid_path = self.pid_file_path(self.vehicle_id)
        if os.path.exists(pid_path):
            try:
                os.remove(pid_path)
                ts_log(self._log_prefix, f"Removed stale PID file: {pid_path}")
            except Exception as e:
                ts_log(self._log_prefix, f"Failed to remove stale PID: {e}", "WARN")
                ts_log(self._log_prefix, traceback.format_exc(), "WARN")

        # 【新增】检查 MAVLink 端口是否可用，如被占用则杀死占用进程
        mavlink_port = 4560 + self.vehicle_id
        self._ensure_port_free(mavlink_port)

        # 打开日志文件用于输出
        try:
            self._log_file = open(self._log_file_path, "w")
            ts_log(self._log_prefix, f"PX4 log will be saved to: {self._log_file_path}")
        except Exception as e:
            ts_log(self._log_prefix, f"Failed to open log file: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")
            self._log_file = None

        # 启动日志监控线程
        self._start_log_monitor()

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
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            pid = int(self.px4_process.pid)
            with open(self.pid_file_path(self.vehicle_id), "w") as f:
                f.write(str(pid))
            ts_log(self._log_prefix, f"PX4 launched, pid={pid}, log={self._log_file_path}")
        except Exception as e:
            ts_log(self._log_prefix, f"Write PID failed: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")

    def kill_px4(self):
        """
        Method that will kill a px4 instance with the specified configuration
        """
        ts_log(self._log_prefix, "Killing PX4 process (hard kill)")
        # 停止日志监控
        self._stop_log_monitor()

        if self.px4_process is not None:
            self.px4_process.kill()
            self.px4_process = None

        # 关闭日志文件
        if self._log_file is not None:
            try:
                self._log_file.close()
            except Exception as e:
                ts_log(self._log_prefix, f"Failed to close log file: {e}", "WARN")
            self._log_file = None

        try:
            p = self.pid_file_path(self.vehicle_id)
            if os.path.exists(p):
                os.remove(p)
        except Exception as e:
            ts_log(self._log_prefix, f"Remove PID file failed: {e}", "WARN")
            ts_log(self._log_prefix, traceback.format_exc(), "WARN")

        # 重置 ready 状态
        self._set_ready_to_takeoff(False)
        ts_log(self._log_prefix, "PX4 process killed")

    def rebuild_temp_directory(self):
        """
        【关键】完全重建PX4临时目录，清除所有EKF状态

        此方法用于解决多轨迹采集时EKF坐标漂移问题。

        问题原因：
        1. PX4 EKF将GPS原点信息保存在临时目录中
        2. 即使杀死PX4进程，临时目录中的状态文件仍然存在
        3. 新PX4进程启动时读取旧状态，导致坐标系偏移

        解决方案：
        1. 删除旧的临时目录（包含所有EKF状态文件）
        2. 创建新的临时目录
        3. PX4启动时将从干净状态开始，EKF从当前位置初始化原点

        调用时机：
        - 在teleport之后、PX4启动之前调用
        - 确保每次轨迹采集都从干净的EKF状态开始
        """
        ts_log(self._log_prefix, "=== Rebuilding PX4 temp directory (clear EKF state) ===")

        try:
            # 1. 保存旧目录路径用于日志
            old_path = self.root_fs.name if self.root_fs else "None"
            ts_log(self._log_prefix, f"Old temp dir: {old_path}")

            # 2. 清理旧临时目录
            if self.root_fs is not None:
                try:
                    self.root_fs.cleanup()
                    ts_log(self._log_prefix, "Old temp dir cleaned up")
                except Exception as e:
                    ts_log(self._log_prefix, f"Cleanup old temp dir failed: {e}", "WARN")

            # 3. 创建新的临时目录
            self.root_fs = tempfile.TemporaryDirectory(prefix=f"px4_{self.vehicle_id}_")
            ts_log(self._log_prefix, f"New temp dir created: {self.root_fs.name}")

            # 4. 重置 ready 状态
            self._set_ready_to_takeoff(False)

            # 5. 重置启动计数（可选，让日志文件名更清晰）
            # self._launch_count = 0  # 不重置，保持日志文件的连续性

            ts_log(self._log_prefix, "=== Temp directory rebuild complete ===")
            return True

        except Exception as e:
            ts_log(self._log_prefix, f"Rebuild temp directory failed: {e}", "ERROR")
            ts_log(self._log_prefix, traceback.format_exc(), "ERROR")
            return False

    def kill_px4_save(self):
        """
        优雅地关闭 PX4 以保存日志
        """
        ts_log(self._log_prefix, "Gracefully shutting down PX4 (save logs)")
        # 停止日志监控
        self._stop_log_monitor()

        if self.px4_process is not None:
            ts_log(self._log_prefix, "Sending SIGINT for graceful shutdown...")
            # 1. 发送中断信号，让 PX4 执行清理逻辑（保存日志）
            self.px4_process.send_signal(signal.SIGINT)
            try:
                # 2. 等待进程正常结束（最多等 6 秒）
                self.px4_process.wait(timeout=6)
                ts_log(self._log_prefix, "PX4 exited gracefully")
            except subprocess.TimeoutExpired:
                # 如果超时还没关掉，再强制杀掉
                ts_log(self._log_prefix, "Shutdown timed out, forcing kill", "WARN")
                self.px4_process.kill()
            # 3. 【关键步骤】将日志从临时目录拷贝到永久目录
            self._save_logs()
            self.px4_process = None

        # 关闭日志文件
        if self._log_file is not None:
            try:
                self._log_file.close()
            except Exception as e:
                ts_log(self._log_prefix, f"Failed to close log file: {e}", "WARN")
            self._log_file = None

        # 清理 PID 文件
        try:
            p = self.pid_file_path(self.vehicle_id)
            if os.path.exists(p):
                os.remove(p)
        except Exception as e:
            ts_log(self._log_prefix, f"Remove PID file failed: {e}", "WARN")
            ts_log(self._log_prefix, traceback.format_exc(), "WARN")

        # 重置 ready 状态
        self._set_ready_to_takeoff(False)
        ts_log(self._log_prefix, "PX4 shutdown complete")

    def __del__(self):
        """
        If the px4 process is still running when the PX4 launch tool object is whiped from memory, then make sure
        we kill the px4 instance so we don't end up with hanged px4 instances
        """

        # Make sure the PX4 process gets killed
        if self.px4_process:
            self.kill_px4_save()

        # Make sure we clean the temporary filesystem used for the simulation
        self.root_fs.cleanup()


# ---- Code used for debugging the px4 tool ----
def main():

    px4_tool = PX4LaunchTool(os.environ["HOME"] + "/PX4-Autopilot")
    px4_tool.launch_px4()

    import time

    # 等待 ready to takeoff
    ts_log("[main]", "Waiting for ready to takeoff...")
    for i in range(60):
        if px4_tool.ready_to_takeoff:
            ts_log("[main]", f"Ready to takeoff after {i} seconds!")
            break
        time.sleep(1)

    time.sleep(5)
    px4_tool.kill_px4()
    # MAVROS managed externally; nothing to kill here


if __name__ == "__main__":
    main()
