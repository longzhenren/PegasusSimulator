#!/usr/bin/env python3
# Copyright (c) 2024-2026 amurzzb@gmail.com
# Licensed under the MIT License
"""
多机启动器（launch_multi_rospy.py）

==========================
概述
==========================
本脚本是 Pegasus 多机仿真系统的主启动入口，负责：
- 预清理残留进程（端口占用、PX4/MAVROS 进程）
- 启动 Isaac Sim 仿真端（8_camera_vehicle.py）
- 等待 PX4 SITL 就绪
- 启动多个 ROS2 控制器（rospy_isaacsim.py）
- 启动网关服务（recording_server.py）
- 监控进程状态，异常/正常退出时清理所有子进程

==========================
进程管理策略
==========================
1. 预清理（pre_clean）：
   - 按端口查找占用进程
   - 按进程名匹配 PX4/MAVROS 相关进程
   - 递归杀死进程树（包括子进程）

2. 进程跟踪：
   - 所有启动的进程注册到全局列表
   - 记录 PID、进程类型、关联信息

3. 异常/终止清理：
   - 捕获 SIGINT、SIGTERM 信号
   - 捕获未处理异常
   - 递归杀死所有子进程树
   - 关闭所有日志文件句柄

==========================
使用方法
==========================
# 基础启动
python launch_multi_rospy.py

# 指定配置文件
python launch_multi_rospy.py --config my_config.json

# 分离模式（启动后退出，进程后台运行）
python launch_multi_rospy.py --detach

# 跳过预清理
python launch_multi_rospy.py --no-pre-clean
"""

import argparse
import atexit
import json
import os
import signal
import sys
import time
import re
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Set
import subprocess as sp
import urllib.request
import concurrent.futures as cf

import psutil


# =======================================================================
#                           Constants
# =======================================================================
ISAACSIM_PYTHON = '/home/user/isaacsim-5.1.0/python.sh'

# 全局进程管理
_MANAGED_PROCS: List[sp.Popen] = []
_CLEANUP_DONE = False
_T_START = time.time()


def _source_env(paths: List[str]) -> Dict[str, str]:
    """Source bash scripts and return resulting environment."""
    cmd_parts = [f"source {os.path.expanduser(p)}" for p in paths]
    cmd = " && ".join(cmd_parts + ["env -0"])
    try:
        out = sp.check_output(['bash', '-lc', cmd], stderr=sp.DEVNULL)
        env_out = {}
        for item in out.split(b'\0'):
            if item:
                k, _, v = item.partition(b'=')
                env_out[k.decode()] = v.decode()
        return env_out
    except Exception:
        return os.environ.copy()


PYTHON311_ENV = _source_env([
    "~/IsaacSim-ros_workspaces/build_ws/humble/humble_ws/install/local_setup.bash",
    "~/IsaacSim-ros_workspaces/build_ws/humble/isaac_sim_ros_ws/install/local_setup.bash",
])


# =======================================================================
#                           Logging
# =======================================================================
def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """
    生成带时间戳的日志消息并输出

    Args:
        prefix: 日志前缀（如 [Launcher]）
        message: 日志内容
        level: 日志级别 (INFO, WARN, ERROR, DEBUG)

    Returns:
        格式化的日志字符串
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed = int(time.time() - _T_START)
    log_msg = f"[{timestamp}] [{level}] [{elapsed}s] {prefix} {message}"
    print(log_msg, flush=True)
    return log_msg


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    return json.loads(path.read_text())


def build_env(base: Dict[str, str], vid: int, mavros_ns: str, session_ts: str) -> Dict[str, str]:
    env = dict(base)
    env["MAVROS_NS"] = mavros_ns
    env["ROS_NAMESPACE"] = f"/{mavros_ns}"
    env["PEGASUS_HTTP_PORT"] = str(5009 + int(vid))
    env["PEGASUS_SESSION_TS"] = str(session_ts)
    return env


# =======================================================================
#                           Process Management
# =======================================================================
def _find_pids_by_port(port: int) -> List[int]:
    """按端口查找占用进程"""
    pids = []
    try:
        for conn in psutil.net_connections(kind='tcp'):
            if conn.laddr and conn.laddr.port == port and conn.pid:
                pids.append(conn.pid)
        for conn in psutil.net_connections(kind='udp'):
            if conn.laddr and conn.laddr.port == port and conn.pid:
                pids.append(conn.pid)
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        pass
    if not pids:
        try:
            cp = sp.run(
                ["lsof", "-i", f":{port}", "-t"],
                stdout=sp.PIPE,
                stderr=sp.DEVNULL,
                check=False,
                text=True,
            )
            if cp.returncode == 0 and cp.stdout:
                pids = [int(line) for line in cp.stdout.splitlines() if line.strip()]
        except Exception:
            pids = []
    return list(set(pids))


def _find_pids_by_name(substrs: List[str]) -> List[int]:
    """按进程名/命令行查找进程"""
    hits = set()
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            name = (p.info.get('name') or '').lower()
            cmd = ' '.join(p.info.get('cmdline') or []).lower()
            if any(s in name or s in cmd for s in substrs):
                hits.add(int(p.info['pid']))
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            pass
    return list(hits)


def _get_process_tree(pid: int) -> Set[int]:
    """
    获取进程树（包括所有子进程、孙进程等）

    Args:
        pid: 根进程 PID

    Returns:
        包含 pid 及其所有后代进程的 PID 集合
    """
    tree = {pid}
    if not psutil:
        return tree

    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            try:
                tree.add(child.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    return tree


def _kill_process_tree(pid: int, sig: int = signal.SIGKILL) -> List[int]:
    """
    杀死进程树（包括所有子进程）

    Args:
        pid: 根进程 PID
        sig: 信号类型（默认 SIGKILL）

    Returns:
        被杀死的 PID 列表
    """
    killed = []
    tree = _get_process_tree(pid)

    # 先杀子进程，再杀父进程（避免父进程先死导致子进程成为孤儿被 init 接管）
    sorted_pids = sorted(tree, reverse=True)  # 子进程 PID 通常比父进程大

    for p in sorted_pids:
        try:
            if psutil:
                proc = psutil.Process(p)
                proc.send_signal(sig)
            else:
                os.kill(p, sig)
            killed.append(p)
        except (OSError, psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return killed



def pre_clean(vehicles: List[Dict], t_start: float) -> List[int]:
    """
    预清理残留进程

    清理策略：
    1. 按端口查找占用进程（MAVLink、HTTP 服务等）
    2. 按进程名匹配（px4、mavros、mavlink）
    3. 递归杀死进程树

    Args:
        vehicles: 载具配置列表
        t_start: 启动时间戳

    Returns:
        被清理的 PID 列表
    """
    ts_log("[PreClean]", "开始预清理残留进程...")

    # 收集需要检查的端口
    candidate_ports = {5008, 8081}  # 网关端口、仿真端口
    for v in vehicles:
        vid = int(v.get("vehicle_id", 0))
        candidate_ports.update([
            4560 + vid,   # MAVLink TCP
            8888 + vid,   # PX4 HTTP
            5009 + vid,   # 控制器 HTTP
            14540 + vid,  # MAVLink UDP (GCS)
            14550 + vid,  # MAVLink UDP
            14580 + vid,  # MAVLink UDP
            14740 + vid,  # MAVLink UDP
        ])

    # 进程名匹配模式
    name_patterns = ["mavros", "px4", "mavlink", "8_camera_vehicle", "rospy_isaacsim"]

    # 收集需要杀死的 PID
    pids_to_kill = set()

    # 按端口查找
    for port in candidate_ports:
        port_pids = _find_pids_by_port(port)
        if port_pids:
            ts_log("[PreClean]", f"端口 {port} 被占用: PIDs={port_pids}", "DEBUG")
            pids_to_kill.update(port_pids)

    # 按进程名查找
    name_pids = _find_pids_by_name(name_patterns)
    if name_pids:
        ts_log("[PreClean]", f"找到相关进程: PIDs={name_pids}", "DEBUG")
        pids_to_kill.update(name_pids)

    # 排除自身
    my_pid = os.getpid()
    pids_to_kill = {p for p in pids_to_kill if p != my_pid}

    if not pids_to_kill:
        ts_log("[PreClean]", "无残留进程")
        return []

    # 展开进程树
    all_pids = set()
    for pid in pids_to_kill:
        all_pids.update(_get_process_tree(pid))
    all_pids.discard(my_pid)

    ts_log("[PreClean]", f"准备清理 {len(all_pids)} 个进程（含子进程）: {sorted(all_pids)}")

    # 并行杀死进程
    killed = []
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(_kill_process_tree, pid, signal.SIGTERM) for pid in pids_to_kill]
        for f in cf.as_completed(futures, timeout=5.0):
            try:
                killed.extend(f.result())
            except Exception:
                pass

    # 等待进程退出
    time.sleep(1.0)

    # 强制杀死仍存活的进程
    still_alive = []
    for pid in all_pids:
        try:
            if psutil:
                if psutil.pid_exists(pid):
                    still_alive.append(pid)
            else:
                os.kill(pid, 0)  # 检查进程是否存在
                still_alive.append(pid)
        except (OSError, psutil.NoSuchProcess):
            pass

    if still_alive:
        ts_log("[PreClean]", f"强制杀死 {len(still_alive)} 个顽固进程: {still_alive}", "WARN")
        for pid in still_alive:
            try:
                _kill_process_tree(pid, signal.SIGKILL)
            except Exception:
                pass
        time.sleep(0.5)

    ts_log("[PreClean]", f"清理完成，共处理 {len(all_pids)} 个进程")
    return list(all_pids)


def register_process(proc: sp.Popen, proc_type: str, **kwargs):
    """
    注册托管进程

    Args:
        proc: Popen 对象
        proc_type: 进程类型（isaac, controller, gateway）
        **kwargs: 附加信息（vehicle_id, mavros_ns 等）
    """
    global _MANAGED_PROCS
    setattr(proc, "_proc_type", proc_type)
    for k, v in kwargs.items():
        setattr(proc, f"_{k}", v)
    _MANAGED_PROCS.append(proc)
    ts_log("[ProcessMgr]", f"注册进程: type={proc_type} pid={proc.pid} {kwargs}", "DEBUG")


def start_process(cmd: List[str], env: Dict[str, str], log_path: Path,
                  proc_type: str = "unknown", **kwargs) -> sp.Popen:
    """
    启动进程并注册到全局管理列表

    Args:
        cmd: 命令列表
        env: 环境变量
        log_path: 日志文件路径
        proc_type: 进程类型
        **kwargs: 附加信息

    Returns:
        Popen 对象
    """
    log_file = log_path.open("w")
    proc = sp.Popen(cmd, stdout=log_file, stderr=sp.STDOUT, env=env)
    setattr(proc, "_log_file", log_file)
    setattr(proc, "_log_path", log_path)
    register_process(proc, proc_type, **kwargs)
    return proc


def terminate_all(procs: List[sp.Popen], timeout: float = 5.0):
    """
    终止所有进程（含子进程树）

    Args:
        procs: 进程列表
        timeout: 等待超时时间
    """
    if not procs:
        return

    ts_log("[ProcessMgr]", f"开始终止 {len(procs)} 个进程...")

    # 第一阶段：发送 SIGTERM
    for p in procs:
        if p.poll() is None:
            try:
                pid = p.pid
                proc_type = getattr(p, "_proc_type", "unknown")
                ts_log("[ProcessMgr]", f"终止进程: type={proc_type} pid={pid}", "DEBUG")

                # 终止进程树
                _kill_process_tree(pid, signal.SIGTERM)
            except Exception as e:
                ts_log("[ProcessMgr]", f"终止失败: pid={p.pid} err={e}", "WARN")

    # 等待进程退出
    t0 = time.time()
    while time.time() - t0 < timeout:
        if all(p.poll() is not None for p in procs):
            break
        time.sleep(0.2)

    # 第二阶段：强制杀死仍存活的进程
    for p in procs:
        if p.poll() is None:
            try:
                pid = p.pid
                proc_type = getattr(p, "_proc_type", "unknown")
                ts_log("[ProcessMgr]", f"强制杀死: type={proc_type} pid={pid}", "WARN")
                _kill_process_tree(pid, signal.SIGKILL)
            except Exception:
                pass

        # 关闭日志文件
        lf = getattr(p, "_log_file", None)
        if lf:
            try:
                lf.close()
            except Exception:
                pass

    ts_log("[ProcessMgr]", "进程终止完成")


def cleanup_all_processes():
    """
    全局清理函数 - 终止所有托管进程

    此函数会被 atexit、信号处理器、异常处理器调用
    使用 _CLEANUP_DONE 标志避免重复清理
    """
    global _CLEANUP_DONE, _MANAGED_PROCS

    if _CLEANUP_DONE:
        return
    _CLEANUP_DONE = True

    ts_log("[Cleanup]", f"开始全局清理，共 {len(_MANAGED_PROCS)} 个托管进程")

    if _MANAGED_PROCS:
        terminate_all(_MANAGED_PROCS, timeout=10.0)

    # 额外清理：确保没有遗漏的子进程
    # 查找可能遗漏的相关进程
    orphan_patterns = ["px4", "mavros", "8_camera_vehicle", "rospy_isaacsim"]
    orphan_pids = _find_pids_by_name(orphan_patterns)
    my_pid = os.getpid()
    orphan_pids = [p for p in orphan_pids if p != my_pid]

    if orphan_pids:
        ts_log("[Cleanup]", f"发现 {len(orphan_pids)} 个可能遗漏的进程: {orphan_pids}", "WARN")
        for pid in orphan_pids:
            try:
                _kill_process_tree(pid, signal.SIGKILL)
            except Exception:
                pass

    ts_log("[Cleanup]", "全局清理完成")


# 注册 atexit 清理
atexit.register(cleanup_all_processes)


# =======================================================================
#                           Wait Functions
# =======================================================================
def _probe_port(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as resp:
            return resp.getcode() == 200
    except Exception:
        return False


def _check_px4_ready(uav_id: int, timeout: float = 2.0) -> bool:
    """通过 HTTP API 检查 PX4 是否就绪。"""
    try:
        url = f"http://127.0.0.1:8081/uav/{uav_id}/px4/ready"
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            if resp.getcode() == 200:
                data = json.loads(resp.read().decode())
                return data.get("ready", False)
    except Exception:
        pass
    return False


def wait_all_px4_ready(vehicles: List[Dict], timeout: float, t_start: float) -> bool:
    """等待所有 PX4 就绪。"""
    start = time.time()
    ready_set = set()
    expected = len(vehicles)

    while time.time() - start < timeout:
        for v in vehicles:
            vid = int(v.get("vehicle_id", 0))
            if vid not in ready_set and _check_px4_ready(vid):
                ready_set.add(vid)
                ts_log("[PX4]", f"UAV{vid} 就绪 ({len(ready_set)}/{expected})")

        if len(ready_set) >= expected:
            return True
        time.sleep(2.0)

    return False


def wait_log_pattern(log_path: Path, pattern: str, timeout: float) -> bool:
    """等待日志文件中出现指定模式。"""
    start = time.time()
    pat = re.compile(pattern, re.IGNORECASE)

    while time.time() - start < timeout:
        if log_path.exists():
            try:
                data = log_path.read_text(errors="ignore")
                if pat.search(data):
                    return True
            except Exception:
                pass
        time.sleep(1.0)
    return False


def wait_isaac_ready(log_path: Path, timeout: float) -> bool:
    """等待 Isaac Sim 就绪。"""
    markers = [
        "app ready",
        "Simulation App Startup Complete",
        "Received first hearbeat"
    ]
    start = time.time()

    while time.time() - start < timeout:
        if log_path.exists():
            try:
                data = log_path.read_text(errors="ignore").lower()
                if any(m.lower() in data for m in markers):
                    return True
            except Exception:
                pass
        time.sleep(1.0)
    return False


# =======================================================================
#                           Signal Handlers
# =======================================================================
def _signal_handler(sig, frame):
    """
    信号处理器 - 处理 SIGINT/SIGTERM

    收到终止信号时：
    1. 打印日志
    2. 调用全局清理函数
    3. 退出程序
    """
    sig_name = signal.Signals(sig).name if hasattr(signal, 'Signals') else str(sig)
    ts_log("[Signal]", f"收到信号 {sig_name}，开始清理...")
    cleanup_all_processes()
    sys.exit(0)


def _exception_handler(exc_type, exc_value, exc_tb):
    """
    未捕获异常处理器

    发生未捕获异常时：
    1. 打印异常信息
    2. 调用全局清理函数
    3. 退出程序
    """
    if exc_type is KeyboardInterrupt:
        ts_log("[Exception]", "KeyboardInterrupt，开始清理...")
    else:
        tb_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))
        ts_log("[Exception]", f"未捕获异常:\n{tb_str}", "ERROR")

    cleanup_all_processes()
    sys.exit(1)


# =======================================================================
#                           Main
# =======================================================================
def main():
    global _T_START
    _T_START = time.time()

    parser = argparse.ArgumentParser(description="Launch multi-UAV simulation")
    default_dir = Path(__file__).resolve().parent

    parser.add_argument("--config", type=str, default=str(default_dir / "multi_uav_config.json"))
    parser.add_argument("--script", type=str, default=str(default_dir / "rospy_isaacsim.py"))
    parser.add_argument("--isaac", type=str, default=str(default_dir / "8_camera_vehicle.py"))
    parser.add_argument("--session-ts", type=str, default=None)
    parser.add_argument("--log-root", type=str, default=str(default_dir / "logs"))
    parser.add_argument("--pre-clean", dest="pre_clean", action="store_true", default=True)
    parser.add_argument("--no-pre-clean", dest="pre_clean", action="store_false")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--ready-timeout", type=float, default=300.0)
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--detach", action="store_true")

    args = parser.parse_args()
    t_start = _T_START

    # 设置信号处理器
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # 设置未捕获异常处理器
    sys.excepthook = _exception_handler

    # 加载配置
    config_path = Path(args.config)
    script_path = Path(args.script)

    try:
        cfg = load_config(config_path)
    except Exception as e:
        ts_log("[Launcher]", f"配置加载失败: {e}", "ERROR")
        sys.exit(1)

    vehicles = cfg.get("vehicles", [])
    if not vehicles:
        ts_log("[Launcher]", "配置中没有载具", "ERROR")
        sys.exit(1)

    # 会话时间戳
    session_ts = args.session_ts or os.environ.get("PEGASUS_SESSION_TS") or str(int(time.time()))

    # 日志目录
    log_root = Path(args.log_root)
    log_dir = log_root / session_ts
    ensure_dir(log_dir)
    ensure_dir(log_dir / "isaac")
    ensure_dir(log_dir / "px4")

    ts_log("[Launcher]", f"========== 启动多机仿真 ==========")
    ts_log("[Launcher]", f"载具数: {len(vehicles)} | 会话: {session_ts}")
    ts_log("[Launcher]", f"日志目录: {log_dir}")

    try:
        # 预清理
        if args.pre_clean:
            pre_clean(vehicles, t_start)

        # 启动 Isaac Sim
        if args.isaac:
            ts_log("[Launcher]", "启动 Isaac Sim...")

            isaac_env = dict(PYTHON311_ENV)
            isaac_env["PEGASUS_SESSION_TS"] = session_ts

            isaac_log = log_dir / "isaac" / "isaacsim.log"
            isaac_cmd = [ISAACSIM_PYTHON, str(Path(args.isaac))]

            isaac_proc = start_process(isaac_cmd, isaac_env, isaac_log, proc_type="isaac")
            ts_log("[Launcher]", f"Isaac Sim 已启动 | PID: {isaac_proc.pid} | 日志: {isaac_log}")

            # 等待 Isaac Sim 就绪
            ts_log("[Launcher]", "等待 Isaac Sim 就绪...")
            if wait_isaac_ready(isaac_log, timeout=600.0):
                ts_log("[Launcher]", "Isaac Sim 就绪")
            else:
                ts_log("[Launcher]", "Isaac Sim 就绪检测超时，继续...", "WARN")

            # 等待所有 PX4 就绪
            ts_log("[Launcher]", f"等待 {len(vehicles)} 架 PX4 就绪...")
            if wait_all_px4_ready(vehicles, timeout=args.ready_timeout, t_start=t_start):
                ts_log("[Launcher]", "所有 PX4 就绪")
            else:
                ts_log("[Launcher]", "PX4 就绪检测超时，继续...", "WARN")

        # 启动控制器
        ts_log("[Launcher]", f"启动 {len(vehicles)} 个控制器...")

        for v in vehicles:
            vid = int(v.get("vehicle_id", 0))
            mavros_ns = v.get("mavros_namespace", f"uav{vid}")

            env = build_env(PYTHON311_ENV, vid, mavros_ns, session_ts)
            ctrl_log = log_dir / f"{mavros_ns}_rospy.log"
            ctrl_cmd = [ISAACSIM_PYTHON, str(script_path), "--mavros-ns", mavros_ns]

            proc = start_process(ctrl_cmd, env, ctrl_log,
                                 proc_type="controller", vehicle_id=vid, mavros_ns=mavros_ns)
            ts_log("[Launcher]", f"控制器 {mavros_ns} 已启动 | PID: {proc.pid}")

        # 等待控制器端口就绪
        ts_log("[Launcher]", "等待控制器端口就绪...")

        ns_port_map = {
            v.get("mavros_namespace", f"uav{v.get('vehicle_id', 0)}"): 5009 + int(v.get("vehicle_id", 0))
            for v in vehicles
        }

        wait_start = time.time()
        all_ok = False
        while time.time() - wait_start < 120.0:
            all_ok = all(_probe_port(p) for p in ns_port_map.values())
            if all_ok:
                break
            time.sleep(1.0)

        if all_ok:
            ts_log("[Launcher]", "所有控制器端口就绪")
        else:
            ts_log("[Launcher]", "部分控制器端口未就绪", "WARN")

        # 启动网关
        ts_log("[Launcher]", "启动网关服务...")
        try:
            gw_env = dict(os.environ)
            gw_env["PEGASUS_SESSION_TS"] = session_ts
            gw_env["PEGASUS_NS_PORT_MAP"] = json.dumps(ns_port_map)
            gw_env["PEGASUS_GATEWAY_PORT"] = "5008"

            gw_script = default_dir / "recording_server.py"
            gw_log = log_dir / "gateway.log"
            gw_proc = start_process([ISAACSIM_PYTHON, str(gw_script)], gw_env, gw_log, proc_type="gateway")
            ts_log("[Launcher]", f"网关已启动 | PID: {gw_proc.pid} | 端口: 5008")
        except Exception as e:
            ts_log("[Launcher]", f"网关启动失败: {e}", "WARN")

        ts_log("[Launcher]", "========== 启动完成 ==========")
        ts_log("[Launcher]", f"托管进程总数: {len(_MANAGED_PROCS)}")

        if args.detach:
            ts_log("[Launcher]", "分离模式，退出启动器（进程将在后台运行）")
            # 分离模式下不注册 atexit 清理
            global _CLEANUP_DONE
            _CLEANUP_DONE = True  # 防止 atexit 清理
            return

        # 监控进程
        ts_log("[Launcher]", "监控进程中... (Ctrl+C 退出)")

        while True:
            # 检查托管进程状态
            alive_count = 0
            for p in list(_MANAGED_PROCS):
                if p.poll() is not None:
                    proc_type = getattr(p, "_proc_type", "unknown")
                    exit_code = p.poll()
                    ts_log("[Monitor]", f"进程退出: type={proc_type} pid={p.pid} code={exit_code}",
                           "WARN" if exit_code != 0 else "INFO")

                    # 关闭日志文件
                    lf = getattr(p, "_log_file", None)
                    if lf:
                        try:
                            lf.close()
                        except Exception:
                            pass

                    _MANAGED_PROCS.remove(p)
                else:
                    alive_count += 1

            # 全部退出则结束
            if alive_count == 0:
                ts_log("[Monitor]", "所有进程已退出")
                break

            time.sleep(2.0)

    except KeyboardInterrupt:
        ts_log("[Launcher]", "收到 Ctrl+C，开始清理...")
        cleanup_all_processes()
    except Exception as e:
        ts_log("[Launcher]", f"发生异常: {e}", "ERROR")
        ts_log("[Launcher]", traceback.format_exc(), "ERROR")
        cleanup_all_processes()
        sys.exit(1)

    ts_log("[Launcher]", "启动器退出")


if __name__ == "__main__":
    main()
