#!/usr/bin/env python3
"""
多机启动器（launch_multi_rospy.py）

概述
- 根据 `multi_uav_config.json` 启动多架载具：生成 MAVROS 多机启动文件并为每架载具启动控制器 `rospy_isaacsim.py`。
- 支持统一 HTTP 网关（`recording_server.py`），将 `/uav/<id>/...` 转发到各控制器端口，便于多机统一入口与录制。
- 提供预清理（端口/进程占用）与信号处理，确保异常退出时清理子进程与日志文件句柄。

启动时序与稳定性
- 先启动仿真 `8_camera_vehicle.py`；等待日志出现 `'Startup script returned successfully'`，且数量达到载具数。
- 再启动 MAVROS；若需要可等待 `'Ready for takeoff!'` 信息达到载具数以保证控制器更稳定地进入 OFFBOARD。

使用示例
  python3 examples/launch_multi_rospy.py \
    --config examples/multi_uav_config.json \
    --script examples/rospy_isaacsim.py \
    --isaac examples/8_camera_vehicle.py \
    --pre-clean --force

日志位置
- 控制器：`examples/logs/<session_ts>/<namespace>/rospy.log`
- 仿真：`examples/logs/<session_ts>/isaac/isaacsim.log`
- MAVROS：`examples/logs/<session_ts>/mavros/ros2_mavros_launch.log`
- 网关：控制台输出健康映射与转发状态
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

try:
    import psutil  # optional, used for robust port/name scanning
except Exception:
    psutil = None

CONTROL_UP_INTERVAL = 2.0

def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    try:
        return json.loads(path.read_text())
    except Exception as e:
        raise RuntimeError(f"Failed to read JSON config {path}: {e}")


def ensure_dir(p: Path):
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def wait_log_count(log_path: Path, regex: str, expected_count: int, timeout: float, interval: float = 1.0, label: str = "") -> bool:
    start = time.time()
    last_len = 0
    pat = re.compile(regex, re.IGNORECASE | re.MULTILINE)
    seen = 0
    last_seen = -1
    while time.time() - start < timeout:
        try:
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
        except Exception:
            pass
        time.sleep(interval)
    return False

def _generate_mavros_launch_from_config(cfg_path: Path) -> Path:
    try:
        cfg = json.loads(cfg_path.read_text())
    except Exception:
        cfg = {"vehicles": []}
    vehicles = cfg.get("vehicles", [])
    lines: List[str] = []
    lines.append("<launch>")
    for v in vehicles:
        vid = int(v.get("vehicle_id", 0))
        ns = v.get("mavros_namespace", f"uav{vid}")
        tcp_port = 5760 + vid
        fcu_local = 14540 + vid
        fcu_remote = 14580 + vid
        use_tcp = bool(v.get("mavros_use_tcp", False))
        if use_tcp:
            fcu_url = f"tcp://127.0.0.1:{tcp_port}"
        else:
            fcu_url = f"udp://:{fcu_local}@127.0.0.1:{fcu_remote}"
        tgt_system = 1 + vid
        lines.append("    <group>")
        lines.append(f"        <arg name=\"id\" default=\"{vid}\" />")
        lines.append(f"        <arg name=\"fcu_url\" default=\"{fcu_url}\" />")
        lines.append("        <include file=\"$(find-pkg-share mavros)/launch/px4.launch\">")
        lines.append(f"            <arg name=\"tgt_system\" value=\"{tgt_system}\" />")
        lines.append(f"            <arg name=\"namespace\" value=\"/{ns}/mavros\" />")
        lines.append("            <arg name=\"time_sync\" value=\"False\" />")
        lines.append("        </include>")
        lines.append("    </group>")
    lines.append("</launch>")
    content = "\n".join(lines)
    # 和cfg_path保存到相同目录
    path = cfg_path.parent / f"pegasus_multi_uav.launch"
    try:
        path.write_text(content)
    except Exception:
        pass
    return path

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


def start_instance(python_exe: str, script_path: Path, mavros_ns: Optional[str], env: Dict[str, str], log_path: Path, prefix: Optional[str] = None, to_console: bool = False) -> sp.Popen:
    cmd: List[str] = [python_exe, str(script_path)]
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
            try:
                for line in stream:
                    line = line.rstrip("\n")
                    if tag:
                        print(f"{pre} {line}")
                    else:
                        print(line)
                    try:
                        file.write(line + "\n")
                        file.flush()
                    except Exception:
                        pass
            except Exception:
                pass

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
            try:
                p.terminate()
            except Exception:
                pass
    t0 = time.time()
    while time.time() - t0 < timeout:
        if all(p.poll() is not None for p in procs):
            break
        time.sleep(0.2)
    for p in procs:
        if p.poll() is None:
            try:
                p.kill()
            except Exception:
                pass
        # close log files
        lf = getattr(p, "_log_file", None)
        try:
            if lf:
                lf.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Launch multiple rospy_isaacsim.py controllers from JSON config")
    default_dir = Path(__file__).resolve().parent
    parser.add_argument("--config", type=str, default=str(default_dir / "multi_uav_config.json"), help="Path to multi-UAV JSON config")
    parser.add_argument("--script", type=str, default=str(default_dir / "rospy_isaacsim.py"), help="Path to control script")
    parser.add_argument("--python", type=str, default=sys.executable, help="Python executable for child processes")
    parser.add_argument("--isaac", type=str, default=str(default_dir / "8_camera_vehicle.py"), help="IsaacSim scene script to run before controllers")
    parser.add_argument("--session-ts", type=str, default=None, help="Override session timestamp (string or int)")
    parser.add_argument("--log-root", type=str, default=str(default_dir / "logs"), help="Root directory for logs")
    parser.add_argument("--pre-clean", action="store_true", default=True, help="Scan and kill residual processes occupying ports or named mavros/px4")
    parser.add_argument("--force", action="store_true", help="Skip Y/N confirmation when pre-clean is requested")
    parser.add_argument("--detach", action="store_true", help="Do not wait; exit after starting children")
    parser.add_argument("--console-logs", action="store_true", default=True, help="Stream child logs to console with prefixes while saving to files")
    parser.add_argument("--ready-timeout", type=float, default=500.0, help="Timeout for waiting for process to be ready (seconds)")
    parser.add_argument("--kill-isaac", action="store_true", help="启动前杀掉已有 Isaac Sim 进程")
    args = parser.parse_args()

    config_path = Path(args.config)
    script_path = Path(args.script)
    python_exe = args.python

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
            try:
                for conn in psutil.net_connections(kind='tcp'):
                    if conn.laddr and conn.laddr.port == port and conn.pid:
                        pids.append(conn.pid)
                for conn in psutil.net_connections(kind='udp'):
                    if conn.laddr and conn.laddr.port == port and conn.pid:
                        pids.append(conn.pid)
            except Exception:
                pass
        # Fallback to lsof
        if not pids:
            try:
                out = sp.check_output(["lsof", "-i", f":{port}", "-t"], stderr=sp.DEVNULL).decode().strip()
                for line in out.splitlines():
                    try:
                        pids.append(int(line))
                    except Exception:
                        pass
            except Exception:
                # Fallback to ss
                try:
                    out = sp.check_output(["ss", "-lnptu"], stderr=sp.DEVNULL).decode().splitlines()
                    for line in out:
                        if f":{port} " in line or line.strip().endswith(f":{port}"):
                            # Extract pid via users:(("name",pid=123,fd=...))
                            if "pid=" in line:
                                try:
                                    seg = line.split("pid=")[1]
                                    pid = int(seg.split(",")[0])
                                    pids.append(pid)
                                except Exception:
                                    pass
                except Exception:
                    pass
        return list(sorted(set(pids)))

    def _kill_pid(pid: int):
        try:
            if psutil is not None:
                p = psutil.Process(pid)
                p.terminate()
                try:
                    p.wait(timeout=3)
                except Exception:
                    p.kill()
            else:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.2)
                try:
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass
            cleaned_pids.append(pid)
        except Exception:
            pass

    def _find_pids_by_name(substrs: List[str]) -> List[int]:
        hits: List[int] = []
        if psutil is not None:
            try:
                for p in psutil.process_iter(['pid', 'name', 'cmdline']):
                    name = (p.info.get('name') or '').lower()
                    cmd = ' '.join(p.info.get('cmdline') or []).lower()
                    if any(s in name or s in cmd for s in substrs):
                        hits.append(p.info['pid'])
            except Exception:
                pass
        else:
            try:
                out = sp.check_output(["ps", "-eo", "pid,comm,args"], stderr=sp.DEVNULL).decode().splitlines()
                for line in out:
                    low = line.lower()
                    if any(s in low for s in substrs):
                        try:
                            pid = int(low.split()[0])
                            hits.append(pid)
                        except Exception:
                            pass
            except Exception:
                pass
        return list(sorted(set(hits)))

    if args.pre_clean:
        # Aggregate candidate ports (command ports and PX4 MAVLink base ports)
        candidate_ports = set()
        candidate_ports.add(5008) # HTTP Gateway Port
        candidate_ports.add(8080) # ISAAC HTTP Port
        for v in vehicles:
            vid = int(v.get("vehicle_id", 0))
            candidate_ports.add(14540 + vid) 
            candidate_ports.add(14550 + vid) 
            candidate_ports.add(14580 + vid) 
            candidate_ports.add(4560 + vid) 
            candidate_ports.add(8888 + vid) # ROS2 UXRCE DDS Port
            candidate_ports.add(5009 + vid) # HTTP Command Port
        name_patterns = ["mavros", "px4", "px4-sitl", "mavros_node", "ros2 launch mavros"]
        print(f"[CLEAN] Candidate ports: {sorted(candidate_ports)}; names: {name_patterns}")
        if _confirm("Proceed to kill processes occupying listed ports and matching names?"):
            # kill by port
            for port in sorted(candidate_ports):
                pids = _find_pids_by_port(port)
                for pid in pids:
                    # don't kill self
                    if pid != os.getpid():
                        _kill_pid(pid)
            # kill by name
            for pid in _find_pids_by_name(name_patterns):
                if pid != os.getpid():
                    _kill_pid(pid)
            print(f"[CLEAN] Killed PIDs: {sorted(set(cleaned_pids))}")
        else:
            print("[CLEAN] Skipped by user.")

    # --- Optional: kill existing Isaac Sim processes before launching scene ---
    if args.kill_isaac:
        try:
            isaac_path_hint = os.environ.get("ISAACSIM_PATH", "")
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

    def _signal_handler(sig, frame):
        print(f"\n[SIGNAL] Received {sig}. Cleaning up child processes...")
        terminate_all(procs)
        # also stop isaac sim if running
        try:
            if 'isaac_proc' in globals() or 'isaac_proc' in locals():
                if isaac_proc is not None:
                    try:
                        isaac_proc.terminate()
                        time.sleep(0.5)
                        isaac_proc.kill()
                    except Exception:
                        pass
        except Exception:
            pass
        sys.exit(0)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # --- Integrate Isaac Sim scene startup (optional, launched first) ---
    isaac_proc = None
    if args.isaac:
        try:
            isaac_py = os.environ.get("ISAACSIM_PYTHON")
            if not isaac_py:
                # fallback to ISAACSIM_PATH/python.sh, else to repo tools/packman/python.sh
                candidate = os.path.join(os.environ.get("ISAACSIM_PATH", ""), "python.sh")
                if os.path.isfile(candidate):
                    isaac_py = candidate
                else:
                    repo_py = Path(__file__).resolve().parents[1] / "tools" / "packman" / "python.sh"
                    isaac_py = str(repo_py)
            isaac_cmd = [isaac_py, str(Path(args.isaac))]
            isaac_log_dir = log_root / str(session_ts) / "isaac"
            ensure_dir(isaac_log_dir)
            isaac_log = isaac_log_dir / "isaacsim.log"
            isaac_log_f = isaac_log.open("w")
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[提示] 场景加载中... 当前时间: {now_str}")
            # Start Isaac, capture stdout for filtering
            isaac_proc = sp.Popen(isaac_cmd, stdout=sp.PIPE, stderr=sp.STDOUT, env=base_env, bufsize=1, universal_newlines=True)
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
                            "has been deprecated",
                            "[omni.neuraylib.plugin]",
                        ]
                        if any(f in line for f in filter_list):
                            continue
                        if (not ready_seen) and re_poll.search(line):
                            continue
                        if not line.strip():
                            continue
                        try:
                            file.write(line + "\n")
                            file.flush()
                        except Exception:
                            pass
                        if args.console_logs:
                            try:
                                if tag:
                                    print(f"{pre} {line}")
                                else:
                                    print(line)
                            except Exception:
                                pass
                except Exception:
                    pass
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
                    except Exception:
                        pass
                    time.sleep(1.0)
                return False

            if not _wait_for_isaac_ready(isaac_log):
                print("[WARN] Isaac Sim readiness not confirmed within timeout; continuing to launch controllers.")
        except Exception as e:
            print(f"[WARN] Failed to start ISAACSIM scene: {e}")
    # --- Start MAVROS multi after readiness gate ---
    try:
        cfg_path = Path(args.config)
        launch_path = _generate_mavros_launch_from_config(cfg_path)
        ros_env = dict(base_env)
        try:
            out = sp.check_output(['bash', '-lc', 'source /opt/ros/humble/setup.bash && env -0'])
            for item in out.split(b'\0'):
                if not item:
                    continue
                k, _, v = item.partition(b'=')
                try:
                    ros_env[k.decode()] = v.decode()
                except Exception:
                    pass
        except Exception as e:
            print(f"[WARN] Failed to source ROS 2 env: {e}")
        mavros_log_dir = log_root / str(session_ts) / 'mavros'
        ensure_dir(mavros_log_dir)
        mavros_log = mavros_log_dir / 'ros2_mavros_launch.log'
        mavros_log_f = mavros_log.open('w')

        isaac_log_path = log_root / str(session_ts) / 'isaac' / 'isaacsim.log'
        ok_px4 = wait_log_count(isaac_log_path, r"Startup script returned successfully", expected_aircraft, timeout=float(args.ready_timeout), interval=1.0, label="'Startup script returned successfully'")
        if not ok_px4:
            print(f"[WARN] Did not observe {expected_aircraft} 'Startup script returned successfully' entries within timeout; proceeding to launch MAVROS anyway.")

        mavros_cmd = ['bash', '-lc', f'source /opt/ros/humble/setup.bash && ros2 launch {launch_path}']
        mavros_proc = sp.Popen(mavros_cmd, env=ros_env, stdout=mavros_log_f, stderr=sp.STDOUT, preexec_fn=os.setsid)
        setattr(mavros_proc, '_log_file', mavros_log_f)
        procs.append(mavros_proc)
        print(f"[LAUNCH] MAVROS multi started: {launch_path} pid={mavros_proc.pid} logs: {mavros_log}")
    except Exception as e:
        print(f"[WARN] Failed to start MAVROS multi: {e}")
    if expected_aircraft > 0 and args.isaac:
        isaac_log_path = log_root / str(session_ts) / "isaac" / "isaacsim.log"
        print(f"[提示] 等待所有飞机就绪（Ready for takeoff!）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        ok = wait_log_count(isaac_log_path, r"INFO\s*\[commander\].*?Ready for takeoff!", expected_aircraft, timeout=float(args.ready_timeout), interval=2.0, label="Commander Ready for takeoff!")
        if not ok:
            print(f"[WARN] Did not observe '{expected_aircraft}' Ready for takeoff! messages within timeout; proceeding anyway.")

    # Launch per vehicle (controllers)
    for idx, v in enumerate(vehicles):
        vid = int(v.get("vehicle_id", 0))
        mavros_ns = v.get("mavros_namespace", f"uav{vid}")

        env = build_env(base_env, vid, mavros_ns, session_ts)

        # logs/<session_ts>/<ns>/rospy.log
        log_dir = log_root / str(session_ts)
        ensure_dir(log_dir)
        log_path = log_dir / f"{mavros_ns}_rospy.log"
        try:
            print(f"[PARAMS] vehicle_id={vid} mavros_ns={mavros_ns}")
            proc = start_instance(python_exe, script_path, mavros_ns, env, log_path, prefix=f"[uav{vid}]", to_console=args.console_logs)
            setattr(proc, "_is_controller", True)
            setattr(proc, "_vehicle_id", vid)
            setattr(proc, "_mavros_ns", mavros_ns)
            procs.append(proc)
            print(f"[LAUNCH] vehicle_id= {vid} pid= {proc.pid} logs: {log_path}")
            time.sleep(CONTROL_UP_INTERVAL)
        except Exception as e:
            print(f"[ERROR] Failed to launch vehicle_id={vid}: {e}")

    print(f"[提示] 等待所有控制器HTTP端口就绪（Healthy）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    # 循环检测，直到所有端口健康检查通过
    while True:
        try:
            PORT_STATUS = {ns: _probe_port(p) for ns, p in NS_PORT_MAP.items()}
            print(f"[GW] namespaces={ {ns: {'port': p, 'alive': PORT_STATUS.get(ns)} for ns, p in NS_PORT_MAP.items()} }")
            if all(PORT_STATUS.values()):
                break
        except Exception:
            pass
        time.sleep(1.0)
    print(f"[提示] 所有控制器HTTP端口已就绪（Healthy）：{expected_aircraft} 架 | 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[提示] 开始启动记录 + 网关服务器（Recording+Gateway）| 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
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
        gateway_proc = sp.Popen([python_exe, str(rec_srv)], env=gw_env)
        print(f"[LAUNCH] Recording+Gateway pid={gateway_proc.pid} port=5008")
    except Exception as e:
        print(f"[WARN] Failed to start gateway: {e}")

    if args.detach:
        print(f"[INFO] Detached mode. Spawned {len(procs)} processes.")
        return

    print("[INFO] Waiting for child processes. Press Ctrl+C to stop.")
    print(f"[INFO] Session TS: {session_ts} | Log root: {log_root}")
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
                    try:
                        if lf:
                            lf.close()
                    except Exception:
                        pass
                    try:
                        procs.remove(p)
                    except Exception:
                        pass
                    try:
                        if getattr(p, "_is_controller", False):
                            vid = int(getattr(p, "_vehicle_id", 0))
                            mavros_ns = getattr(p, "_mavros_ns", f"uav{vid}")
                            env = build_env(base_env, vid, mavros_ns, session_ts)
                            log_dir = log_root / str(session_ts)
                            ensure_dir(log_dir)
                            log_path = log_dir / f"{mavros_ns}_rospy.log"
                            now = time.time()
                            last = restart_backoff.get(vid, 0.0)
                            if now - last < 5.0:
                                time.sleep(5.0 - (now - last))
                            if rc != 0:
                                print(f"[RESTART] vehicle_id={vid} namespace={mavros_ns}")
                                try:
                                    port = 5009 + vid
                                    pids = _find_pids_by_port(port)
                                    for kpid in pids:
                                        if kpid != os.getpid():
                                            _kill_pid(kpid)
                                    print(f"[RESTART] cleaned port {port} PIDs: {sorted(set(pids))}")
                                except Exception:
                                    pass
                                new_p = start_instance(python_exe, script_path, mavros_ns, env, log_path, prefix=f"[uav{vid}]", to_console=args.console_logs)
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
                    try:
                        if lf:
                            lf.close()
                    except Exception:
                        pass
                    isaac_proc = None
            if alive == 0 and (isaac_proc is None):
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        _signal_handler("KeyboardInterrupt", None)

    print("[INFO] All controllers finished.")
    # Ensure MAVROS process group is terminated when leaving
    try:
        terminate_all(procs)
    except Exception:
        pass
    if cleaned_pids:
        print(f"[INFO] Cleaned PIDs at start: {sorted(set(cleaned_pids))}")


if __name__ == "__main__":
    main()
