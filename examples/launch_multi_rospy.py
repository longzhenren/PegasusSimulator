"""
多机启动器（launch_multi_rospy.py）

概述
- 根据 `multi_uav_config.json` 启动多架载具：生成 MAVROS 多机启动文件并为每架载具启动控制器 `rospy_isaacsim.py`。
- 支持统一 HTTP 网关（`recording_server.py`），将 `/uav/<id>/...` 转发到各控制器端口，便于多机统一入口与录制。
- 并行分组启动：通过 `--parallel` 控制并行分组数（默认 4），每组内按顺序等待上一架日志 `Takeoff complete` 再启动下一架。
- 端口健康与自动重启：控制器 HTTP 端口（5009+id）如 60s 未就绪，自动清理端口并重启控制器（10s 冷却）。
- 快速预清理：并行收集并强杀端口占用与进程名匹配（mavros/px4）的残留进程；支持 `--force` 跳过确认。
- 退出清理：在退出信号与正常结束时强杀 MAVROS 进程组，关闭日志句柄，并清理所有子进程；可选清理现有 Isaac Sim。

启动时序与稳定性
- 先启动仿真 `8_camera_vehicle.py`；可监控日志 `Ready for takeoff!` 达到载具数以提升稳定性。
- 启动 MAVROS；可监控 `Startup script returned successfully` 达到载具数后进入连接检查。
- 启动控制器：按 `--parallel` 并行分组启动，组内顺序等待上一架 `Takeoff complete` 日志再继续。

使用示例
  ISAACSIM_PYTHON examples/launch_multi_rospy.py \
    --config examples/multi_uav_config.json \
    --script examples/rospy_isaacsim.py \
    --isaac examples/8_camera_vehicle.py \
    --parallel 4 --pre-clean --force

日志位置
- 控制器：`examples/logs/<session_ts>/<namespace>/rospy.log`
- 仿真：`examples/logs/<session_ts>/isaac/isaacsim.log`
- MAVROS：`examples/logs/<session_ts>/mavros/ros2_mavros_launch.log`
- 网关：控制台输出健康映射与转发状态（端口 5008）
"""

import argparse
import json
import os
import signal
import sys
import time
import re
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import subprocess as sp
import urllib.request
import concurrent.futures as cf
import psutil

def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    try:
        return json.loads(path.read_text())
    except Exception as e:
        raise RuntimeError(f"Failed to read JSON config {path}: {e}")

def build_env(base: Dict[str, str], vid: int, mavros_ns: str, session_ts: Optional[str]) -> Dict[str, str]:
    env = dict(base)
    env["MAVROS_NS"] = mavros_ns
    # Per-controller HTTP port; gateway will route '/uavX/*' to these
    env["ROS_NAMESPACE"] = f"/{mavros_ns}"
    # Reserve 5008 for gateway; controllers start from 5009
    env["PEGASUS_HTTP_PORT"] = str(5009 + int(vid))
    if session_ts:
        env["PEGASUS_SESSION_TS"] = str(session_ts)
    return env

def _source_env(paths: List[str]) -> Dict[str, str]:
    cmd_parts: List[str] = []
    for p in paths:
        p_abs = os.path.expanduser(p)
        cmd_parts.append(f"source {p_abs}")
    cmd = " && ".join(cmd_parts + ["env -0"])
    out = sp.check_output(['bash', '-lc', cmd])
    env_out: Dict[str, str] = {}
    for item in out.split(b'\0'):
        if not item:
            continue
        k, _, v = item.partition(b'=')
        env_out[k.decode()] = v.decode()
    return env_out

# For ISAACSIM version >= 5.0 work together with ros2 humble
# DO NOT use or source system ros2 and packages (linked with system Python 3.10)
# USE separate Python 3.11 installed with ISAACSIM instead
# Ref: https://github.com/isaac-sim/IsaacSim-ros_workspaces
ISAACSIM_PYTHON = '/home/user/isaacsim-5.1.0/python.sh'
SYS_PYTHON = '/usr/bin/python3'
ISAACSIM_ROS2_CMD = '/home/user/IsaacSim-ros_workspaces/build_ws/humble/humble_ws/install/bin/ros2'
PYTHON311_ENV = _source_env([
        "~/IsaacSim-ros_workspaces/build_ws/humble/humble_ws/install/local_setup.bash",
        "~/IsaacSim-ros_workspaces/build_ws/humble/isaac_sim_ros_ws/install/local_setup.bash",
        ])
PYTHON310_ENV = _source_env([
        "/opt/ros/humble/setup.bash",
        ])

def ensure_dir(p: Path): 
    p.mkdir(parents=True, exist_ok=True)
    
def wait_log_count(log_path: Path, regex: str, expected_count: int, timeout: float, interval: float = 1.0, label: str = "") -> bool:
    start = time.time()
    last_len = 0
    pat = re.compile(regex, re.IGNORECASE | re.MULTILINE)
    seen = 0
    last_seen = -1
    while time.time() - start < timeout:
        if log_path.exists():
            data = log_path.read_text(errors="ignore")
            if len(data) != last_len:
                last_len = len(data)
                seen = len(pat.findall(data))
                if seen != last_seen:
                    last_seen = seen
                    print(f"[READY] {label or regex} count: {seen}/{expected_count}")
                if seen >= expected_count:
                    return True
        time.sleep(interval)
    return False

def _probe_port(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as resp:
            return resp.getcode() == 200
    except Exception:
        return False

def _generate_mavros_launch_from_config(cfg_path: Path) -> List[Tuple[int, Path]]:
    try:
        cfg = json.loads(cfg_path.read_text())
    except Exception:
        cfg = {"vehicles": []}
    vehicles = cfg.get("vehicles", [])
    out: List[Tuple[int, Path]] = []
    for v in vehicles:
        vid = int(v.get("vehicle_id", 0))
        ns = v.get("mavros_namespace", f"uav{vid}")
        tcp_port = 5760 + vid
        fcu_local = 14740 + vid
        fcu_remote = 14580 + vid
        use_tcp = bool(v.get("mavros_use_tcp", False))
        if use_tcp:
            fcu_url = f"tcp://127.0.0.1:{tcp_port}"
        else:
            fcu_url = f"udp://:{fcu_local}@127.0.0.1:{fcu_remote}"
        tgt_system = 1 + vid
        lines: List[str] = []
        lines.append("<launch>")
        lines.append("    <group>")
        lines.append(f"        <arg name=\"id\" default=\"{vid}\" />")
        lines.append(f"        <arg name=\"fcu_url\" default=\"{fcu_url}\" />")
        lines.append(f"        <include file=\"{cfg_path.parent}/launch/px4_custom.launch\">")
        lines.append(f"            <arg name=\"tgt_system\" value=\"{tgt_system}\" />")
        lines.append(f"            <arg name=\"namespace\" value=\"/{ns}/mavros\" />")
        lines.append("            <arg name=\"time_sync\" value=\"False\" />")
        lines.append(f"            <arg name=\"timesync_rate\" value=\"0.0\" />")
        lines.append("        </include>")
        lines.append("    </group>")
        lines.append("</launch>")
        content = "\n".join(lines)
        out_path = cfg_path.parent / f"launch/pegasus_uav_{vid}.launch"
        out_path.write_text(content)
        out.append((vid, out_path))
    return out


def start_instance(ISAACSIM_PYTHON: str, script_path: Path, mavros_ns: Optional[str], env: Dict[str, str], log_path: Path, prefix: Optional[str] = None, to_console: bool = False) -> sp.Popen:
    cmd: List[str] = [ISAACSIM_PYTHON, str(script_path)]
    if mavros_ns:
        cmd += ["--mavros-ns", mavros_ns]    
    # Print the command that will be executed
    print(
        f"[INFO] Executing command: {' '.join(cmd)} | "
        f"MAVROS_NS={env.get('MAVROS_NS')} "
        f"ROS_NAMESPACE={env.get('ROS_NAMESPACE')} PEGASUS_HTTP_PORT={env.get('PEGASUS_HTTP_PORT')}"
    )
    
    # When streaming to console, tee output with prefix
    if to_console:
        # Ensure unbuffered python output in children
        env2 = dict(env)
        env2["PYTHONUNBUFFERED"] = "1"
        log_f = log_path.open("w")
        proc = sp.Popen(
            cmd,
            stdout=sp.PIPE,
            stderr=sp.STDOUT,
            env=env2,
            bufsize=1,
            universal_newlines=True,
        )
        setattr(proc, "_log_file", log_f)

        import threading
        import re

        def _pick_color(tag: str) -> str:
            palette = ["31","32","33","34","35","36","37","91","92","93","94","95","96"]
            m = re.search(r"(\d+)", tag or "")
            idx = int(m.group(1)) if m else 0
            return palette[idx % len(palette)]

        def _reader(stream, file, tag: str):
            color = _pick_color(tag or "")
            pre = f"\033[{color}m{tag}\033[0m" if tag else ""
            for line in stream:
                line = line.rstrip("\n")
                if tag:
                    print(f"{pre} {line}")
                else:
                    print(line)
                file.write(line + "\n")
                file.flush()


        t = threading.Thread(target=_reader, args=(proc.stdout, log_f, prefix or ""), daemon=True)
        t.start()
        setattr(proc, "_reader_thread", t)
        return proc
    else:
        log_f = log_path.open("w")
        proc = sp.Popen(cmd, stdout=log_f, stderr=sp.STDOUT, env=env)
        # attach file handle for cleanup
        setattr(proc, "_log_file", log_f)
        return proc


def terminate_all(procs: List[sp.Popen], timeout: float = 5.0):
    # Graceful terminate, then kill if needed
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
        # close log files
        lf = getattr(p, "_log_file", None)
        if lf:
            lf.close()


def main():
    parser = argparse.ArgumentParser(description="Launch multiple rospy_isaacsim.py controllers from JSON config")
    default_dir = Path(__file__).resolve().parent
    isaacsim_path_default = os.environ.get("ISAACSIM_PATH", "/home/user/isaacsim-5.1.0")
    parser.add_argument("--config", type=str, default=str(default_dir / "multi_uav_config.json"), help="Path to multi-UAV JSON config")
    parser.add_argument("--script", type=str, default=str(default_dir / "rospy_isaacsim.py"), help="Path to control script")
    parser.add_argument("--python", type=str, default=os.path.join(isaacsim_path_default, "python.sh"), help="Python executable for child processes")
    parser.add_argument("--isaac", type=str, default=str(default_dir / "8_camera_vehicle.py"), help="IsaacSim scene script to run before controllers")
    parser.add_argument("--session-ts", type=str, default=None, help="Override session timestamp (string or int)")
    parser.add_argument("--log-root", type=str, default=str(default_dir / "logs"), help="Root directory for logs")
    parser.add_argument("--pre-clean", action="store_true", default=True, help="Scan and kill residual processes occupying ports or named mavros/px4")
    parser.add_argument("--force", action="store_true", help="Skip Y/N confirmation when pre-clean is requested")
    parser.add_argument("--detach", action="store_true", help="Do not wait; exit after starting children")
    parser.add_argument("--console-logs", action="store_true", default=True, help="Stream child logs to console with prefixes while saving to files")
    parser.add_argument("--ready-timeout", type=float, default=500.0, help="Timeout for waiting for process to be ready (seconds)")
    parser.add_argument("--kill-isaac", action="store_true", help="启动前杀掉已有 Isaac Sim 进程")
    parser.add_argument("--parallel", type=int, default=4, help="控制器并行分组数（每组内顺序等待 OFFBOARD）")
    args = parser.parse_args()

    config_path = Path(args.config)
    script_path = Path(args.script)

    if not script_path.exists():
        print(f"[ERROR] Control script not found: {script_path}")
        sys.exit(1)

    # Load mandatory JSON config; do not auto-initialize defaults
    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    vehicles = cfg.get("vehicles", [])
    if not vehicles:
        print(f"[ERROR] No vehicles found in config {config_path}")
        sys.exit(1)
    expected_aircraft = len(vehicles)

    # Unify session timestamp across instances: cli > env > now
    t_start = time.time()
    session_ts = args.session_ts or os.environ.get("PEGASUS_SESSION_TS") or str(int(time.time()))
    base_env = os.environ.copy()
    base_env["PEGASUS_SESSION_TS"] = session_ts

    # Prepare log root
    log_root = Path(args.log_root)
    ensure_dir(log_root)

    # --- Pre-clean phase: kill by port and by name ---
    cleaned_pids: List[int] = []

    def _confirm(prompt: str) -> bool:
        if args.force:
            return True
        try:
            ans = input(f"{prompt} [y/N]: ").strip().lower()
            return ans in ("y", "yes")
        except Exception:
            return False

    def _find_pids_by_port(port: int) -> List[int]:
        pids: List[int] = []
        # Try psutil first
        if psutil is not None:
            for conn in psutil.net_connections(kind='tcp'):
                if conn.laddr and conn.laddr.port == port and conn.pid:
                    pids.append(conn.pid)
            for conn in psutil.net_connections(kind='udp'):
                if conn.laddr and conn.laddr.port == port and conn.pid:
                    pids.append(conn.pid)
        # Fallback to lsof
        if not pids:
            try:
                out = sp.check_output(["lsof", "-i", f":{port}", "-t"], stderr=sp.DEVNULL).decode().strip()
                for line in out.splitlines():
                    pids.append(int(line))
            except Exception:
                # Fallback to ss
                out = sp.check_output(["ss", "-lnptu"], stderr=sp.DEVNULL).decode().splitlines()
                for line in out:
                    if f":{port} " in line or line.strip().endswith(f":{port}"):
                        # Extract pid via users:(("name",pid=123,fd=...))
                        if "pid=" in line:
                            seg = line.split("pid=")[1]
                            pid = int(seg.split(",")[0])
                            pids.append(pid)
        return list(sorted(set(pids)))

    def _kill_pid(pid: int, hard: bool = False):
        if psutil is not None:
            try:
                p = psutil.Process(pid)
                if hard:
                    p.kill()
                else:
                    p.terminate()
                    try:
                        p.wait(timeout=1)
                    except Exception:
                        p.kill()
            except Exception:
                os.kill(pid, signal.SIGKILL if hard else signal.SIGTERM)
                print(f"[WARN] kill pid fallback for {pid}")
        else:
            os.kill(pid, signal.SIGKILL if hard else signal.SIGTERM)
        cleaned_pids.append(pid)

    def _find_pids_by_name(substrs: List[str]) -> List[int]:
        hits: set[int] = set()
        if psutil is not None:
            for p in psutil.process_iter(['pid', 'name', 'cmdline']):
                name = (p.info.get('name') or '').lower()
                cmd = ' '.join(p.info.get('cmdline') or []).lower()
                if any(s in name or s in cmd for s in substrs):
                    hits.add(int(p.info['pid']))
        out = sp.check_output(["ps", "-eo", "pid,comm,args"], stderr=sp.DEVNULL).decode().splitlines()
        for line in out:
            low = line.lower()
            if any(s in low for s in substrs):
                pid = int(low.split()[0])
                hits.add(pid)
        return list(sorted(hits))

    def _restart_px4_for_vid(vid: int, base_url: str = "http://127.0.0.1:8081") -> bool:
        try:
            url_hr = f"{base_url}/uav/{vid}/px4/hard_reset"
            req_hr = urllib.request.Request(url_hr, data=b"{}", method="POST")
            req_hr.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req_hr, timeout=15) as resp:
                _ = resp.read()
            time.sleep(1.0)
            # url_rl = f"{base_url}/uav/{vid}/px4/relaunch"
            # req_rl = urllib.request.Request(url_rl, data=b"{}", method="POST")
            # req_rl.add_header("Content-Type", "application/json")
            # with urllib.request.urlopen(req_rl, timeout=15) as resp:
            #     _ = resp.read()
            print(f"[PX4] UAV{vid} hard_reset requested")
            return True
        except Exception as e:
            print(f"[PX4] UAV{vid} hard_reset failed: {e}")
            return False

    if args.pre_clean:
        # Aggregate candidate ports (command ports and PX4 MAVLink base ports)
        candidate_ports = set()
        candidate_ports.add(5008) # HTTP Gateway Port
        candidate_ports.add(8081) # ISAAC HTTP Port
        for v in vehicles:
            vid = int(v.get("vehicle_id", 0))
            candidate_ports.add(14540 + vid) 
            candidate_ports.add(14550 + vid) 
            candidate_ports.add(14580 + vid) 
            candidate_ports.add(4560 + vid) 
            candidate_ports.add(8888 + vid) # ROS2 UXRCE DDS Port
            candidate_ports.add(5009 + vid) # HTTP Command Port
        name_patterns = ["mavros", "px4", "px4-sitl", "mavros_node", "ros2 launch", "mavlink"]
        print(f"[CLEAN] Candidate ports: {sorted(candidate_ports)}; names: {name_patterns}")
        # if _confirm("Proceed to kill processes occupying listed ports and matching names?"):
            # aggregate PIDs then kill in parallel to speed up
        agg_pids: set[int] = set()
            # collect by ports quickly via ss/psutil
        for port in candidate_ports:
            for pid in _find_pids_by_port(port):
                agg_pids.add(int(pid))
        for pid in _find_pids_by_name(name_patterns):
            agg_pids.add(int(pid))
        to_kill = [pid for pid in sorted(agg_pids) if pid != os.getpid()]
        if to_kill:
            with cf.ThreadPoolExecutor(max_workers=16) as ex:
                list(ex.map(lambda pid: _kill_pid(pid, True), to_kill))
        print(f"[CLEAN] Killed PIDs: {sorted(set(cleaned_pids))}")

    # --- Optional: kill existing Isaac Sim processes before launching scene ---
    if args.kill_isaac:
        try:
            isaac_path_hint = "/home/user/isaacsim-5.1.0"
            isaac_patterns = [
                "isaacsim",
                "Isaac-Sim",
                "omni.isaac",
                "Kit/Isaac-Sim",
            ]
            if isaac_path_hint:
                isaac_patterns.append(isaac_path_hint.lower())
            print(f"[CLEAN] Killing Isaac Sim processes by patterns: {isaac_patterns}")
            for pid in _find_pids_by_name([p.lower() for p in isaac_patterns]):
                if pid != os.getpid():
                    _kill_pid(pid)
            killed = sorted(set(cleaned_pids))
            if killed:
                print(f"[CLEAN] IsaacSim PIDs killed: {killed}")
            else:
                print("[CLEAN] No IsaacSim processes found to kill.")
        except Exception as e:
            print(f"[WARN] Failed to kill IsaacSim processes: {e}")

    procs: List[sp.Popen] = []
    mavros_procs: Dict[int, sp.Popen] = {}

    def _signal_handler(sig, frame):
        print(f"\n[SIGNAL] Received {sig}. Cleaning up child processes...")
        terminate_all(procs)
        try:
            for vid, mp in list(mavros_procs.items()):
                try:
                    pgid = os.getpgid(mp.pid)
                    os.killpg(pgid, signal.SIGTERM)
                    time.sleep(0.5)
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:
                    print(traceback.format_exc())
                try:
                    lf = getattr(mp, "_log_file", None)
                    if lf:
                        lf.close()
                except Exception:
                    print(traceback.format_exc())
        except Exception:
            print(traceback.format_exc())
        # also stop isaac sim if running
        if 'isaac_proc' in globals() or 'isaac_proc' in locals():
            if isaac_proc is not None:
                isaac_proc.terminate()
                time.sleep(0.5)
                isaac_proc.kill()
        sys.exit(0)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # --- Integrate Isaac Sim scene startup (optional, launched first) ---
    isaac_proc = None
    if args.isaac:
        try:
            isaac_cmd = [ISAACSIM_PYTHON, str(Path(args.isaac))]
            isaac_log_dir = log_root / str(session_ts) / "isaac"
            ensure_dir(isaac_log_dir)
            isaac_log = isaac_log_dir / "isaacsim.log"
            isaac_log_f = isaac_log.open("w")
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[INFO] 场景加载中... | 当前时间: {now_str} | 耗时: {int(time.time() - t_start)}s")
            # Start Isaac, capture stdout for filtering
            try:
                isaac_env = _source_env([
                    "~/IsaacSim-ros_workspaces/build_ws/humble/humble_ws/install/local_setup.bash",
                    "~/IsaacSim-ros_workspaces/build_ws/humble/isaac_sim_ros_ws/install/local_setup.bash",
                ])
                if session_ts:
                    isaac_env["PEGASUS_SESSION_TS"] = str(session_ts)
            except Exception as e:
                print(f"[WARN] Failed to source Isaac ROS env: {e}")
                isaac_env = dict(base_env)
            print(f"[LAUNCH] ISAACSIM scene: {args.isaac} cmd: {isaac_cmd}")
            isaac_proc = sp.Popen(isaac_cmd, stdout=sp.PIPE, stderr=sp.STDOUT, env=isaac_env, bufsize=1, universal_newlines=True)
            setattr(isaac_proc, "_log_file", isaac_log_f)
            print(f"[LAUNCH] ISAACSIM scene: {args.isaac} pid={isaac_proc.pid} logs: {isaac_log}")

            # Filter, write to file, and optionally tee to console
            import threading, re
            def _pick_color(tag: str) -> str:
                palette = ["31","32","33","34","35","36","37","91","92","93","94","95","96"]
                m = re.search(r"(uav\d+)", tag or "")
                idx = int(m.group(0)[3:]) if m else 0
                return palette[idx % len(palette)]
            def _isaac_log_filter(proc, file, tag: str):
                color = _pick_color(tag or "[isaac]")
                pre = f"\033[{color}m{tag}\033[0m" if tag else ""
                re_ready = re.compile(r"INFO\s*\[commander\].*?Ready for takeoff!", re.IGNORECASE)
                re_poll = re.compile(r"ERROR\s*\[simulator_mavlink\].*?poll timeout", re.IGNORECASE)
                ready_seen = False
                try:
                    for raw in proc.stdout:
                        line = raw.rstrip("\n")
                        if re_ready.search(line):
                            ready_seen = True
                        filter_list = [
                            "[Warning] [omni.usd]",
                            "[Warning] [omni.kit]",
                            "[Warning] [omni.graph]",
                            "[Warning] [omni.replicator]",
                            "[Warning] [omni.isaac]",
                            "startup",
                            "deprecated",
                            "usd",
                            "USD",
                            "mdl",
                            "MDL",
                            "hydra",
                            "[omni.neuraylib.plugin]",
                        ]
                        if any(f in line for f in filter_list):
                            continue
                        if (not ready_seen) and re_poll.search(line):
                            continue
                        if not line.strip():
                            continue
                        file.write(line + "\n")
                        file.flush()
                        if args.console_logs:
                            if tag:
                                print(f"{pre} {line}")
                            else:
                                print(line)
                except Exception as e:
                    print(f"[WARN] Isaac log filter error: {e}")
            t = threading.Thread(target=_isaac_log_filter, args=(isaac_proc, isaac_log_f, "[isaac]"), daemon=True)
            t.start()

            # Wait for simulator readiness by watching log markers, then gate on PX4 commander messages
            def _wait_for_isaac_ready(log_path: Path, timeout: float = 3600.0) -> bool:
                start = time.time()
                last_size = 0
                ready_markers = [
                    "Simulator connected on TCP port",
                    "World reset",
                    "PegasusApp Simulation App is closing."  # unlikely, but end marker
                ]
                while time.time() - start < timeout:
                    try:
                        if log_path.exists():
                            data = log_path.read_text()
                            if len(data) != last_size:
                                last_size = len(data)
                                # check markers
                                low = data.lower()
                                if any(m.lower() in low for m in ready_markers):
                                    return True
                    except Exception as e:
                        print(f"[WARN] Isaac log reader error: {e}")
                    time.sleep(1.0)
                return False

            if not _wait_for_isaac_ready(isaac_log):
                print("[WARN] Isaac Sim readiness not confirmed within timeout; continuing to launch controllers.")
        except Exception as e:
            print(f"[WARN] Failed to start ISAACSIM scene: {e}")
        
    if expected_aircraft > 0 and args.isaac:
        isaac_log_path = log_root / str(session_ts) / "isaac" / "isaacsim.log"
        print(f"[INFO] 等待所有飞机就绪（Ready for takeoff!）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
        ok = wait_log_count(isaac_log_path, r"INFO\s*\[commander\].*?Ready for takeoff!", expected_aircraft, timeout=float(args.ready_timeout), interval=2.0, label="Commander Ready for takeoff!")
        if not ok:
            print(f"[WARN] Did not observe '{expected_aircraft}' Ready for takeoff! messages within timeout; proceeding anyway.")
    # --- Start MAVROS multi after readiness gate ---
    try:
        cfg_path = Path(args.config)
        launch_paths = _generate_mavros_launch_from_config(cfg_path)
        mavros_log_dir = log_root / str(session_ts) / 'mavros'
        ensure_dir(mavros_log_dir)
        
        print(f"[INFO] 等待所有PX4启动脚本返回成功（Startup script returned successfully）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
        isaac_log_path = log_root / str(session_ts) / 'isaac' / 'isaacsim.log'
        ok_px4 = wait_log_count(isaac_log_path, r"Startup script returned successfully", expected_aircraft, timeout=float(args.ready_timeout), interval=1.0, label="'Startup script returned successfully'")
        if not ok_px4:
            print(f"[WARN] Did not observe {expected_aircraft} 'Startup script returned successfully' entries within timeout; proceeding to launch MAVROS anyway.")

        mavros_procs: Dict[int, sp.Popen] = {}
        for vid, lp in launch_paths:
            log_path_i = mavros_log_dir / f'uav{vid}.log'
            log_f_i = log_path_i.open('w')
            cmd_i = [ISAACSIM_PYTHON, ISAACSIM_ROS2_CMD, "launch", lp]
            print(f"[LAUNCH] MAVROS cmd: {cmd_i}")
            p_i = sp.Popen(cmd_i, stdout=log_f_i, stderr=sp.STDOUT, preexec_fn=os.setsid)
            setattr(p_i, '_log_file', log_f_i)
            mavros_procs[vid] = p_i
            procs.append(p_i)
            print(f"[LAUNCH] MAVROS started: {lp} pid={p_i.pid} logs: {log_path_i}")
    except Exception as e:
        print(f"[WARN] Failed to start MAVROS multi: {e}")
    
    # Wait until all mavros connected
    print(f"[INFO] 等待所有MAVROS连接就绪（Connected）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    connected = 0
    for vid, p in mavros_procs.items():
        log_path_i = mavros_log_dir / f'uav{vid}.log'
        ok_m_i = wait_log_count(log_path_i, r"UID", 1, timeout=float(args.ready_timeout), interval=2.0, label=f"MAVROS Connected uav{vid}")
        if ok_m_i:
            connected += 1
    if connected < expected_aircraft:
        print(f"[WARN] MAVROS connected count: {connected}/{expected_aircraft}")
    
    print(f"[INFO] 所有MAVROS连接就绪!（Connected）：{connected} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    
    import threading
    def _launch_group(group_list: List[Dict[str, Any]], group_name: str):
        prev_log_path: Optional[Path] = None
        for v in group_list:
            vid = int(v.get("vehicle_id", 0))
            mavros_ns = v.get("mavros_namespace", f"uav{vid}")
            env = build_env(PYTHON311_ENV, vid, mavros_ns, session_ts)
            log_dir = log_root / str(session_ts)
            ensure_dir(log_dir)
            log_path = log_dir / f"{mavros_ns}_rospy.log"
            try:
                if prev_log_path is not None:
                    ok_prev = wait_log_count(prev_log_path, r"Takeoff complete", 1, timeout=float(args.ready_timeout), interval=1.0, label=f"{group_name} prev Takeoff complete")
                    if not ok_prev:
                        print(f"[WARN] {group_name} previous vehicle did not reach Takeoff complete within timeout; continuing")
                print(f"[PARAMS] vehicle_id={vid} mavros_ns={mavros_ns} group={group_name}")
                proc = start_instance(ISAACSIM_PYTHON, script_path, mavros_ns, env, log_path, prefix=f"[uav{vid}]", to_console=args.console_logs)
                setattr(proc, "_is_controller", True)
                setattr(proc, "_vehicle_id", vid)
                setattr(proc, "_mavros_ns", mavros_ns)
                procs.append(proc)
                print(f"[LAUNCH] vehicle_id= {vid} pid= {proc.pid} logs: {log_path} group={group_name}")
                prev_log_path = log_path
            except Exception as e:
                print(f"[ERROR] Failed to launch vehicle_id={vid} in {group_name}: {e}")
    par = max(1, int(args.parallel))
    groups: List[List[Dict[str, Any]]] = [vehicles[i::par] for i in range(par)]
    threads: List[threading.Thread] = []
    for idx, grp in enumerate(groups):
        print(f"[INFO] 启动控制器组 {idx+1}/{par}：{len(grp)} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
        t = threading.Thread(target=_launch_group, args=(grp, f"group{idx+1}"), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
        
    print(f"[INFO] 正在确认所有飞机起飞状态（Takeoff complete）... | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    for p in list(procs):
        if getattr(p, "_is_controller", False):
            ctrl_log = getattr(p, "_log_path", None)
            ctrl_ns = getattr(p, "_mavros_ns", "unknown")
            if ctrl_log:
                # 阻塞直到该架飞机完成起飞
                ok_takeoff = wait_log_count(
                    ctrl_log, 
                    r"Takeoff complete", 
                    1, 
                    timeout=float(args.ready_timeout), 
                    interval=1.0, 
                    label=f"Final sync: {ctrl_ns} Takeoff complete"
                )
                if not ok_takeoff:
                    print(f"[WARN] {ctrl_ns} 未能在超时时间内完成起飞，将继续执行后续检查。")
    
    print(f"[INFO] 所有飞机已确认起飞完成！准备进入端口健康检测阶段。 | 耗时: {int(time.time() - t_start)}s")
    print(f"[INFO] 等待所有控制器HTTP端口就绪（Healthy）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    # 循环检测，直到所有端口健康检查通过
    NS_PORT_MAP = {}
    for v in vehicles:
        vid = int(v.get("vehicle_id", 0))
        ns = v.get("mavros_namespace", f"uav{vid}")
        NS_PORT_MAP[ns] = 5009 + vid
    last_healthy_time: Dict[str, float] = {ns: time.time() if _probe_port(p) else time.time() for ns, p in NS_PORT_MAP.items()}
    restart_cooldown: Dict[str, float] = {ns: 0.0 for ns in NS_PORT_MAP.keys()}
    timeout_sec = 60.0
    cooldown_sec = 10.0
    while True:
        PORT_STATUS = {ns: _probe_port(p) for ns, p in NS_PORT_MAP.items()}
        now = time.time()
        for ns, ok in PORT_STATUS.items():
            if ok:
                last_healthy_time[ns] = now
            else:
                if (now - last_healthy_time[ns]) >= timeout_sec and (now - restart_cooldown[ns]) >= cooldown_sec:
                    try:
                        vid = int(ns.replace('uav', ''))
                        port = NS_PORT_MAP[ns]
                        print(f"[RESTART] Timeout: ns={ns} port={port} not healthy for {timeout_sec}s; restarting controller and PX4")
                        # Kill processes occupying the command port
                        pids = _find_pids_by_port(port)
                        for kpid in pids:
                            if kpid != os.getpid():
                                _kill_pid(kpid)
                        _restart_px4_for_vid(vid)
                        # Relaunch controller
                        env = build_env(PYTHON311_ENV, vid, ns, session_ts)
                        log_dir = log_root / str(session_ts)
                        ensure_dir(log_dir)
                        log_path = log_dir / f"{ns}_rospy.log"
                        new_p = start_instance(ISAACSIM_PYTHON, script_path, ns, env, log_path, prefix=f"[uav{vid}]", to_console=args.console_logs)
                        setattr(new_p, "_is_controller", True)
                        setattr(new_p, "_vehicle_id", vid)
                        setattr(new_p, "_mavros_ns", ns)
                        procs.append(new_p)
                        restart_cooldown[ns] = now
                        print(f"[RESTART] ns={ns} vehicle_id={vid} pid={new_p.pid} logs: {log_path}")
                    except Exception as e:
                        print(f"[ERROR] Failed to restart controller for ns={ns}: {e}")
        if all(PORT_STATUS.values()):
            break
        time.sleep(1.0)
    print(f"[INFO] 所有控制器HTTP端口已就绪（Healthy）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    print(f"[INFO] 开始启动记录 + 网关服务器（Recording+Gateway）| 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    # Start recording+gateway server on base port 5008
    try:
        import json as _json
        ns_port_map = {}
        for v in vehicles:
            vid = int(v.get("vehicle_id", 0))
            ns = v.get("mavros_namespace", f"uav{vid}")
            ns_port_map[ns] = 5009 + vid
        # sanity check: ensure 5008 not used by controllers
        if 5008 in ns_port_map.values():
            print("[ERROR] Port 5008 reserved for gateway but appears in controller map; adjusting mapping base.")
            ns_port_map = {k: (5009 + int(k[3:])) for k in ns_port_map.keys()}
        gw_env = dict(base_env)
        gw_env['PEGASUS_NS_PORT_MAP'] = _json.dumps(ns_port_map)
        gw_env['PEGASUS_GATEWAY_PORT'] = '5008'
        # Run integrated recording+gateway server
        rec_srv = Path(__file__).resolve().parent / 'recording_server.py'
        gateway_proc = sp.Popen([ISAACSIM_PYTHON, str(rec_srv)], env=gw_env)
        print(f"[LAUNCH] Recording+Gateway pid={gateway_proc.pid} port=5008")
    except Exception as e:
        print(f"[WARN] Failed to start gateway: {e}")

    print(f"[INFO] 记录 + 网关服务器已启动（Recording+Gateway）| 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    
    if args.detach:
        print(f"[INFO] Detached mode. Spawned {len(procs)} processes.")
        return

    print("[INFO] Waiting for child processes. Press Ctrl+C to stop.")
    print(f"[INFO] Session TS: {session_ts} | Log root: {log_root} | 耗时: {int(time.time() - t_start)}s")
    # Wait for children and report exits
    try:
        restart_backoff = {}
        while True:
            alive = 0
            for idx, p in enumerate(list(procs)):
                rc = p.poll()
                if rc is None:
                    alive += 1
                else:
                    print(f"[EXIT] PID {p.pid} finished with code {rc}")
                    lf = getattr(p, "_log_file", None)
                    if lf:
                        lf.close()
                    procs.remove(p)
                    try:
                        if getattr(p, "_is_controller", False):
                            vid = int(getattr(p, "_vehicle_id", 0))
                            mavros_ns = getattr(p, "_mavros_ns", f"uav{vid}")
                            env = build_env(PYTHON311_ENV, vid, mavros_ns, session_ts)
                            log_dir = log_root / str(session_ts)
                            ensure_dir(log_dir)
                            log_path = log_dir / f"{mavros_ns}_rospy.log"
                            now = time.time()
                            last = restart_backoff.get(vid, 0.0)
                            if now - last < 5.0:
                                time.sleep(5.0 - (now - last))
                            if rc != 0:
                                print(f"[RESTART] vehicle_id={vid} namespace={mavros_ns}")
                                port = 5009 + vid
                                pids = _find_pids_by_port(port)
                                for kpid in pids:
                                    if kpid != os.getpid():
                                        _kill_pid(kpid)
                                print(f"[RESTART] cleaned port {port} PIDs: {sorted(set(pids))}")
                                new_p = start_instance(ISAACSIM_PYTHON, script_path, mavros_ns, env, log_path, prefix=f"[uav{vid}]", to_console=args.console_logs)
                                setattr(new_p, "_is_controller", True)
                                setattr(new_p, "_vehicle_id", vid)
                                setattr(new_p, "_mavros_ns", mavros_ns)
                                restart_backoff[vid] = time.time()
                                procs.append(new_p)
                                alive += 1
                                print(f"[RESTART] vehicle_id= {vid} pid= {new_p.pid} logs: {log_path}")
                            restart_backoff[vid] = time.time()
                    except Exception as e:
                        print(f"[ERROR] Failed to restart controller: {e}")
            # also monitor isaac_proc
            if isaac_proc is not None:
                rc = isaac_proc.poll()
                if rc is not None:
                    print(f"[EXIT] ISAACSIM PID {isaac_proc.pid} finished with code {rc}")
                    lf = getattr(isaac_proc, "_log_file", None)
                    if lf:
                        lf.close()
                    isaac_proc = None
            if alive == 0 and (isaac_proc is None):
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        _signal_handler("KeyboardInterrupt", None)

    print(f"[INFO] All controllers finished. | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    try:
        for vid, mp in list(mavros_procs.items()):
            try:
                pgid = os.getpgid(mp.pid)
                os.killpg(pgid, signal.SIGTERM)
                time.sleep(0.5)
                os.killpg(pgid, signal.SIGKILL)
            except Exception:
                print(traceback.format_exc())
            try:
                lf = getattr(mp, "_log_file", None)
                if lf:
                    lf.close()
            except Exception:
                print(traceback.format_exc())
    except Exception:
        print(traceback.format_exc())
    terminate_all(procs)
    print(f"[INFO] All MAVROS processes terminated. | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 耗时: {int(time.time() - t_start)}s")
    if cleaned_pids:
        print(f"[INFO] Cleaned PIDs at start: {sorted(set(cleaned_pids))}")


if __name__ == "__main__":
    main()
