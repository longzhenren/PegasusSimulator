"""
多机启动器（launch_multi_rospy.py）

简化版本：
- 删除日志过滤线程，进程日志直接写入文件
- 仅在关键点打印状态和累计耗时
- 日志保存在 logs/<session_ts>/ 目录下
"""

import argparse
import json
import os
import signal
import sys
import time
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
import subprocess as sp
import urllib.request
import concurrent.futures as cf

try:
    import psutil
except ImportError:
    psutil = None


# =======================================================================
#                           Constants
# =======================================================================
ISAACSIM_PYTHON = '/home/user/isaacsim-5.1.0/python.sh'


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
#                           Utility Functions
# =======================================================================
def log_status(msg: str, t_start: float):
    """打印状态信息，附带时间戳和累计耗时。"""
    elapsed = int(time.time() - t_start)
    now_str = time.strftime("%H:%M:%S")
    print(f"[{now_str}] [{elapsed}s] {msg}")


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
    pids = []
    if psutil:
        for conn in psutil.net_connections(kind='tcp'):
            if conn.laddr and conn.laddr.port == port and conn.pid:
                pids.append(conn.pid)
        for conn in psutil.net_connections(kind='udp'):
            if conn.laddr and conn.laddr.port == port and conn.pid:
                pids.append(conn.pid)
    if not pids:
        try:
            out = sp.check_output(["lsof", "-i", f":{port}", "-t"], stderr=sp.DEVNULL).decode().strip()
            pids = [int(line) for line in out.splitlines() if line]
        except Exception:
            pass
    return list(set(pids))


def _find_pids_by_name(substrs: List[str]) -> List[int]:
    hits = set()
    if psutil:
        for p in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = (p.info.get('name') or '').lower()
                cmd = ' '.join(p.info.get('cmdline') or []).lower()
                if any(s in name or s in cmd for s in substrs):
                    hits.add(int(p.info['pid']))
            except Exception:
                pass
    return list(hits)


def _kill_pid(pid: int):
    try:
        if psutil:
            p = psutil.Process(pid)
            p.kill()
        else:
            os.kill(pid, signal.SIGKILL)
    except Exception:
        pass


def pre_clean(vehicles: List[Dict], t_start: float) -> List[int]:
    """清理残留进程。"""
    log_status("预清理残留进程...", t_start)

    candidate_ports = {5008, 8081}
    for v in vehicles:
        vid = int(v.get("vehicle_id", 0))
        candidate_ports.update([
            4560 + vid, 8888 + vid, 5009 + vid,
            14540 + vid, 14550 + vid, 14580 + vid, 14740 + vid
        ])

    name_patterns = ["mavros", "px4", "mavlink"]

    pids_to_kill = set()
    for port in candidate_ports:
        pids_to_kill.update(_find_pids_by_port(port))
    pids_to_kill.update(_find_pids_by_name(name_patterns))

    my_pid = os.getpid()
    pids_to_kill = [p for p in pids_to_kill if p != my_pid]

    if pids_to_kill:
        with cf.ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(_kill_pid, pids_to_kill))
        log_status(f"已清理 {len(pids_to_kill)} 个进程", t_start)
    else:
        log_status("无残留进程", t_start)

    return pids_to_kill


def start_process(cmd: List[str], env: Dict[str, str], log_path: Path) -> sp.Popen:
    """启动进程，日志直接写入文件。"""
    log_file = log_path.open("w")
    proc = sp.Popen(cmd, stdout=log_file, stderr=sp.STDOUT, env=env)
    setattr(proc, "_log_file", log_file)
    setattr(proc, "_log_path", log_path)
    return proc


def terminate_all(procs: List[sp.Popen], timeout: float = 5.0):
    """终止所有进程。"""
    for p in procs:
        if p.poll() is None:
            p.terminate()

    t0 = time.time()
    while time.time() - t0 < timeout:
        if all(p.poll() is not None for p in procs):
            break
        time.sleep(0.2)

    for p in procs:
        if p.poll() is None:
            p.kill()
        lf = getattr(p, "_log_file", None)
        if lf:
            try:
                lf.close()
            except Exception:
                pass


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
                log_status(f"PX4 uav{vid} 就绪 ({len(ready_set)}/{expected})", t_start)

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
    # Isaac Sim 自己的就绪标记
    markers = [
        "app ready",
        "Simulation App Startup Complete",
        "Received first hearbeat"  # PX4 连接成功的标记
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
#                           Main
# =======================================================================
def main():
    parser = argparse.ArgumentParser(description="Launch multi-UAV simulation")
    default_dir = Path(__file__).resolve().parent

    parser.add_argument("--config", type=str, default=str(default_dir / "multi_uav_config.json"))
    parser.add_argument("--script", type=str, default=str(default_dir / "rospy_isaacsim.py"))
    parser.add_argument("--isaac", type=str, default=str(default_dir / "8_camera_vehicle.py"))
    parser.add_argument("--session-ts", type=str, default=None)
    parser.add_argument("--log-root", type=str, default=str(default_dir / "logs"))
    parser.add_argument("--pre-clean", action="store_true", default=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--ready-timeout", type=float, default=300.0)
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--detach", action="store_true")

    args = parser.parse_args()
    t_start = time.time()

    # 加载配置
    config_path = Path(args.config)
    script_path = Path(args.script)

    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    vehicles = cfg.get("vehicles", [])
    if not vehicles:
        print(f"[ERROR] No vehicles in config")
        sys.exit(1)

    # 会话时间戳
    session_ts = args.session_ts or os.environ.get("PEGASUS_SESSION_TS") or str(int(time.time()))

    # 日志目录
    log_root = Path(args.log_root)
    log_dir = log_root / session_ts
    ensure_dir(log_dir)
    ensure_dir(log_dir / "isaac")
    ensure_dir(log_dir / "px4")  # 为 PX4 日志创建目录

    log_status(f"启动多机仿真 | 载具数: {len(vehicles)} | 会话: {session_ts}", t_start)

    # 预清理
    if args.pre_clean:
        pre_clean(vehicles, t_start)

    procs: List[sp.Popen] = []
    isaac_proc = None

    # 信号处理
    def signal_handler(sig, frame):
        log_status("收到终止信号，清理进程...", t_start)
        terminate_all(procs)
        if isaac_proc and isaac_proc.poll() is None:
            isaac_proc.terminate()
            time.sleep(0.5)
            if isaac_proc.poll() is None:
                isaac_proc.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 启动 Isaac Sim
    if args.isaac:
        log_status("启动 Isaac Sim...", t_start)

        isaac_env = dict(PYTHON311_ENV)
        isaac_env["PEGASUS_SESSION_TS"] = session_ts

        isaac_log = log_dir / "isaac" / "isaacsim.log"
        isaac_cmd = [ISAACSIM_PYTHON, str(Path(args.isaac))]

        isaac_proc = start_process(isaac_cmd, isaac_env, isaac_log)
        log_status(f"Isaac Sim 已启动 | PID: {isaac_proc.pid} | 日志: {isaac_log}", t_start)

        # 等待 Isaac Sim 就绪
        log_status("等待 Isaac Sim 就绪...", t_start)
        if wait_isaac_ready(isaac_log, timeout=600.0):
            log_status("Isaac Sim 就绪", t_start)
        else:
            log_status("警告: Isaac Sim 就绪检测超时，继续...", t_start)

        # 等待所有 PX4 就绪
        log_status(f"等待 {len(vehicles)} 架 PX4 就绪...", t_start)
        if wait_all_px4_ready(vehicles, timeout=args.ready_timeout, t_start=t_start):
            log_status("所有 PX4 就绪", t_start)
        else:
            log_status("警告: PX4 就绪检测超时，继续...", t_start)

    # 启动控制器
    log_status(f"启动 {len(vehicles)} 个控制器...", t_start)

    for v in vehicles:
        vid = int(v.get("vehicle_id", 0))
        mavros_ns = v.get("mavros_namespace", f"uav{vid}")

        env = build_env(PYTHON311_ENV, vid, mavros_ns, session_ts)
        ctrl_log = log_dir / f"{mavros_ns}_rospy.log"
        ctrl_cmd = [ISAACSIM_PYTHON, str(script_path), "--mavros-ns", mavros_ns]

        proc = start_process(ctrl_cmd, env, ctrl_log)
        setattr(proc, "_vehicle_id", vid)
        setattr(proc, "_mavros_ns", mavros_ns)
        procs.append(proc)

        log_status(f"控制器 {mavros_ns} 已启动 | PID: {proc.pid}", t_start)

    # 等待控制器端口就绪
    log_status("等待控制器端口就绪...", t_start)

    ns_port_map = {v.get("mavros_namespace", f"uav{v.get('vehicle_id', 0)}"): 5009 + int(v.get("vehicle_id", 0)) for v in vehicles}

    wait_start = time.time()
    while time.time() - wait_start < 120.0:
        all_ok = all(_probe_port(p) for p in ns_port_map.values())
        if all_ok:
            break
        time.sleep(1.0)

    if all_ok:
        log_status("所有控制器端口就绪", t_start)
    else:
        log_status("警告: 部分控制器端口未就绪", t_start)

    # 启动网关
    log_status("启动网关服务...", t_start)
    try:
        gw_env = dict(os.environ)
        gw_env["PEGASUS_SESSION_TS"] = session_ts
        gw_env["PEGASUS_NS_PORT_MAP"] = json.dumps(ns_port_map)
        gw_env["PEGASUS_GATEWAY_PORT"] = "5008"

        gw_script = default_dir / "recording_server.py"
        gw_log = log_dir / "gateway.log"
        gw_proc = start_process([ISAACSIM_PYTHON, str(gw_script)], gw_env, gw_log)
        log_status(f"网关已启动 | PID: {gw_proc.pid} | 端口: 5008", t_start)
    except Exception as e:
        log_status(f"警告: 网关启动失败: {e}", t_start)

    log_status("========== 启动完成 ==========", t_start)
    log_status(f"日志目录: {log_dir}", t_start)

    if args.detach:
        log_status("分离模式，退出启动器", t_start)
        return

    # 监控进程
    log_status("监控进程中... (Ctrl+C 退出)", t_start)

    try:
        while True:
            # 检查 Isaac Sim
            if isaac_proc and isaac_proc.poll() is not None:
                log_status(f"Isaac Sim 退出 (code={isaac_proc.poll()})", t_start)
                lf = getattr(isaac_proc, "_log_file", None)
                if lf:
                    lf.close()
                isaac_proc = None

            # 检查控制器
            for p in list(procs):
                if p.poll() is not None:
                    ns = getattr(p, "_mavros_ns", "unknown")
                    log_status(f"控制器 {ns} 退出 (code={p.poll()})", t_start)
                    lf = getattr(p, "_log_file", None)
                    if lf:
                        lf.close()
                    procs.remove(p)

            # 全部退出则结束
            if not procs and not isaac_proc:
                break

            time.sleep(2.0)
    except KeyboardInterrupt:
        signal_handler(None, None)

    log_status("所有进程已退出", t_start)
    terminate_all(procs)


if __name__ == "__main__":
    main()
