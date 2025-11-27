import json
from pathlib import Path
import time
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
        lines.append("            <arg name=\"time_sync\" value=\"True\" />")
        lines.append("        </include>")
        lines.append("    </group>")
    lines.append("</launch>")
    content = "\n".join(lines)
    path = Path("/tmp") / f"pegasus_multi_uav_{int(time.time())}.launch"
    try:
        path.write_text(content)
    except Exception:
        pass
    return path

if __name__ == "__main__":
    import sys
    cfg_path = Path(sys.argv[1])
    _generate_mavros_launch_from_config(cfg_path)