#!/usr/bin/env python3
'''
python3 examples/trajectory_data_collector.py \
  --input-dir ~/uav-data/drone/uav-flow-sim/train_data/extracted_json_files \
  --pattern "*.json" \
  --out-dir /home/user/uav-data/trajectory_recordings \
  --control-base http://127.0.0.1:5009 \
  --image-base http://127.0.0.1:8081
'''

import argparse
import atexit
import base64
import csv
import glob
import json
import os
import queue
import shutil
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class TrajPoint:
    x: float
    y: float
    z: float


_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _is_local_url(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        host = ""
    if host in ("localhost", "127.0.0.1", "::1"):
        return True
    return host.startswith("127.")


def _urlopen(req: urllib.request.Request, timeout: float):
    url = getattr(req, "full_url", "") or ""
    if isinstance(url, str) and _is_local_url(url):
        return _NO_PROXY_OPENER.open(req, timeout=timeout)
    return urllib.request.urlopen(req, timeout=timeout)


def _http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Tuple[int, Dict[str, Any]]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with _urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return resp.getcode(), {}
            try:
                return resp.getcode(), json.loads(raw.decode("utf-8"))
            except Exception:
                return resp.getcode(), {"raw": raw.decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = b""
        try:
            obj = json.loads(raw.decode("utf-8")) if raw else {"error": str(e)}
        except Exception:
            obj = {"error": raw.decode("utf-8", errors="replace")}
        return int(getattr(e, "code", 500) or 500), obj


def _iter_json_files(input_dir: Path, pattern: str) -> List[Path]:
    files = sorted([Path(p) for p in glob.glob(str(input_dir / pattern))])
    return [p for p in files if p.is_file() and p.suffix.lower() == ".json"]


def _load_preprocessed_xyz(json_path: Path) -> List[TrajPoint]:
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    logs = obj.get("preprocessed_logs")
    if not isinstance(logs, list):
        raise ValueError("missing preprocessed_logs")
    pts: List[TrajPoint] = []
    for row in logs:
        if isinstance(row, (list, tuple)) and len(row) >= 3:
            try:
                pts.append(TrajPoint(float(row[0]), float(row[1]), float(row[2])))
            except Exception:
                continue
    if not pts:
        raise ValueError("preprocessed_logs has no valid xyz rows")
    return pts

def _load_init_point_xyz(json_path: Path) -> TrajPoint:
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    init = obj.get("raw_logs", [])[0]
    if not isinstance(init, (list, tuple)) or len(init) < 3:
        raise ValueError("missing init point")
    try:
        print(f"init point: {init}")
        return TrajPoint(float(init[0]), float(init[1]), float(init[2]))
    except Exception:
        raise ValueError("init point has invalid xyz")


def _load_uav_ids_from_config(config_path: Path) -> List[int]:
    obj = json.loads(config_path.read_text(encoding="utf-8"))
    vehicles = obj.get("vehicles") or []
    out: List[int] = []
    for v in vehicles:
        try:
            out.append(int(v.get("vehicle_id")))
        except Exception:
            continue
    out = sorted(set(out))
    if not out:
        raise ValueError(f"no vehicle_id found in {config_path}")
    return out


def _decode_png_b64_to_file(b64: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(b64.encode("ascii"))
    out_path.write_bytes(data)


def _safe_name(path: Path) -> str:
    s = path.stem
    s2 = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", "."):
            s2.append(ch)
        else:
            s2.append("_")
    return "".join(s2) or "traj"


def _find_latest_ulg(vehicle_id: int, since_ts: float) -> Optional[Path]:
    prefix = f"/tmp/px4_{vehicle_id}_*"
    cand_dirs = [Path(p) for p in glob.glob(prefix)]
    newest: Optional[Tuple[float, Path]] = None
    for d in cand_dirs:
        if not d.is_dir():
            continue
        try:
            for p in d.rglob("*.ulg"):
                try:
                    st = p.stat()
                except Exception:
                    continue
                if st.st_mtime < since_ts:
                    continue
                if newest is None or st.st_mtime > newest[0]:
                    newest = (st.st_mtime, p)
        except Exception:
            continue
    return newest[1] if newest else None


def _transform_points(
    pts: List[TrajPoint],
    scale: float,
    base_x: float,
    base_y: float,
    base_z: float,
    z_down: bool,
) -> List[TrajPoint]:
    out: List[TrajPoint] = []
    for p in pts:
        x = base_x + p.x * scale
        y = base_y + p.y * scale
        if z_down:
            z = base_z - p.z * scale
        else:
            z = base_z + p.z * scale
        out.append(TrajPoint(x, y, z))
    return out


def _fetch_all_info(image_base: str, uav_id: int, timeout: float, retries: int) -> Dict[str, Any]:
    url = f"{image_base}/uav/{uav_id}/all"
    last: Optional[Dict[str, Any]] = None
    for _ in range(max(1, int(retries))):
        code, obj = _http_json("GET", url, payload=None, timeout=timeout)
        if 200 <= code < 300 and isinstance(obj, dict) and obj.get("image") and obj.get("pose"):
            return obj
        last = obj if isinstance(obj, dict) else {"error": "invalid response"}
        time.sleep(0.2)
    raise RuntimeError(f"fetch /all failed uav={uav_id} resp={last}")


def _is_gateway_control_base(control_base: str) -> bool:
    u = urlparse(control_base)
    port = u.port
    if port is None:
        return False
    return int(port) == 5008


def _controller_base_for_vid(control_base: str, uav_id: int) -> str:
    u = urlparse(control_base)
    host = u.hostname or "127.0.0.1"
    scheme = u.scheme or "http"
    port = u.port
    if port is None:
        raise ValueError(f"control_base must include port: {control_base}")
    return f"{scheme}://{host}:{int(port) + int(uav_id)}"


def _ensure_control_healthy(control_base: str, uav_ids: List[int], timeout_s: float = 60.0) -> None:
    deadline = time.time() + float(timeout_s)
    if _is_gateway_control_base(control_base):
        url = f"{control_base}/health"
        while time.time() < deadline:
            code, obj = _http_json("GET", url, payload=None, timeout=5.0)
            if 200 <= code < 300 and isinstance(obj, dict):
                ns = obj.get("namespaces") or {}
                if not ns:
                    return
                ok = True
                for vid in uav_ids:
                    key = f"uav{vid}"
                    alive = bool((ns.get(key) or {}).get("alive"))
                    if not alive:
                        ok = False
                        break
                if ok:
                    return
            time.sleep(0.5)
        raise TimeoutError(f"gateway not healthy for uavs={uav_ids}")

    while time.time() < deadline:
        ok = True
        for vid in uav_ids:
            base = _controller_base_for_vid(control_base, vid)
            code, _ = _http_json("GET", f"{base}/health", payload=None, timeout=2.0)
            if not (code is not None and 200 <= code < 300):
                ok = False
                break
        if ok:
            return
        time.sleep(0.5)
    raise TimeoutError(f"controllers not healthy for uavs={uav_ids}")


def _controller_reset(
    control_base: str,
    uav_id: int,
    hard: bool,
    force: bool,
    position: Optional[List[float]],
    yaw_deg: Optional[float],
    timeout: float,
) -> None:
    if _is_gateway_control_base(control_base):
        url = f"{control_base}/uav/{uav_id}/reset"
    else:
        url = f"{_controller_base_for_vid(control_base, uav_id)}/reset"
    payload: Dict[str, Any] = {"vid": int(uav_id), "hard": bool(hard), "force": bool(force)}
    if position is not None:
        payload["position"] = [float(position[0]), float(position[1]), float(position[2])]
    if yaw_deg is not None:
        payload["yaw_deg"] = float(yaw_deg)
    code, obj = _http_json("POST", url, payload=payload, timeout=timeout)
    if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("status") != "success":
        raise RuntimeError(f"reset failed uav={uav_id} code={code} resp={obj}")


def _controller_command(
    control_base: str,
    uav_id: int,
    cmd: Dict[str, Any],
    timeout: float,
) -> Dict[str, Any]:
    if _is_gateway_control_base(control_base):
        url = f"{control_base}/uav/{uav_id}/command"
    else:
        url = f"{_controller_base_for_vid(control_base, uav_id)}/command"
    code, obj = _http_json("POST", url, payload=cmd, timeout=timeout)
    if not (200 <= code < 300) or not isinstance(obj, dict) or obj.get("ok") is False:
        raise RuntimeError(f"command failed uav={uav_id} cmd={cmd} code={code} resp={obj}")
    return obj


def _write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("", encoding="utf-8")
        return
    keys = list(rows[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _norm_abs_path(p: Path) -> str:
    try:
        return str(p.resolve())
    except Exception:
        return str(p.absolute())

_ULG_COPY_LOCK = threading.Lock()
_ULG_COPY_JOBS: List[Tuple[Path, Path]] = []


def _flush_ulg_copies() -> None:
    with _ULG_COPY_LOCK:
        jobs = list(_ULG_COPY_JOBS)
        _ULG_COPY_JOBS.clear()
    for src, dst in jobs:
        try:
            if not src.exists():
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        except Exception:
            continue


atexit.register(_flush_ulg_copies)


def _schedule_ulg_copy(src: Path, dst: Path) -> bool:
    try:
        if src is None or dst is None:
            return False
        if not src.exists():
            return False
        with _ULG_COPY_LOCK:
            _ULG_COPY_JOBS.append((src, dst))
        return True
    except Exception:
        return False


class Worker:
    def __init__(
        self,
        uav_id: int,
        control_base: str,
        image_base: str,
        out_dir: Path,
        task_queue: "queue.Queue[Path]",
        scale: float,
        base_x: float,
        base_y: float,
        base_z: float,
        z_down: bool,
        reset_timeout: float,
        cmd_timeout: float,
        image_timeout: float,
        image_retries: int,
        skip_existing: bool,
        print_lock: threading.Lock,
    ):
        self.uav_id = int(uav_id)
        self.control_base = control_base.rstrip("/")
        self.image_base = image_base.rstrip("/")
        self.out_dir = out_dir
        self.task_queue = task_queue
        self.scale = float(scale)
        self.base_x = float(base_x)
        self.base_y = float(base_y)
        self.base_z = float(base_z)
        self.z_down = bool(z_down)
        self.reset_timeout = float(reset_timeout)
        self.cmd_timeout = float(cmd_timeout)
        self.image_timeout = float(image_timeout)
        self.image_retries = int(image_retries)
        self.skip_existing = bool(skip_existing)
        self.print_lock = print_lock

    def _log(self, msg: str) -> None:
        with self.print_lock:
            print(msg, flush=True)

    def run(self) -> None:
        init_pos = [self.base_x, self.base_y, self.base_z]
        try:
            self._log(f"[UAV{self.uav_id}] init reset(hard=True) pos={init_pos}")
            _controller_reset(
                self.control_base,
                self.uav_id,
                hard=True,
                force=True,
                position=init_pos,
                yaw_deg=None,
                timeout=self.reset_timeout,
            )
        except Exception as e:
            self._log(f"[UAV{self.uav_id}] init reset failed: {e}")
            return

        while True:
            try:
                json_path = self.task_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._process_one(json_path)
            except Exception as e:
                self._log(f"[UAV{self.uav_id}] traj failed json={json_path} err={e}")
            finally:
                self.task_queue.task_done()

    def _process_one(self, json_path: Path) -> None:
        traj_name = _safe_name(json_path)
        traj_dir = self.out_dir / traj_name / f"uav{self.uav_id}"
        csv_path = traj_dir / "data.csv"
        if self.skip_existing and csv_path.exists():
            self._log(f"[UAV{self.uav_id}] skip existing traj={traj_name}")
            return

        raw_pts = _load_preprocessed_xyz(json_path)
        init_pos = _load_init_point_xyz(json_path)
        pts = _transform_points(raw_pts, self.scale, self.base_x, self.base_y, self.base_z, self.z_down)

        traj_start_ts = time.time()
        self._log(f"[UAV{self.uav_id}] start traj={traj_name} points={len(pts)}")
        rows: List[Dict[str, Any]] = []

        for i, (p_in, p_cmd) in enumerate(zip(raw_pts, pts)):
            _controller_command(
                self.control_base,
                self.uav_id,
                {"cmd": "move_to", "x": p_cmd.x, "y": p_cmd.y, "z": p_cmd.z, "force": True},
                timeout=self.cmd_timeout,
            )

            info = _fetch_all_info(
                self.image_base,
                self.uav_id,
                timeout=self.image_timeout,
                retries=self.image_retries,
            )
            ts_img = float((info.get("image") or {}).get("timestamp") or 0.0)
            img_b64 = (info.get("image") or {}).get("data") or ""
            ts_ms = int(ts_img * 1000.0) if ts_img > 0 else int(time.time() * 1000.0)
            img_name = f"img_{i:06d}_{ts_ms}.png"
            img_path = traj_dir / "images" / img_name
            if isinstance(img_b64, str) and img_b64:
                _decode_png_b64_to_file(img_b64, img_path)

            pose = info.get("pose") or {}
            pos = (pose.get("position") or [None, None, None])[:3]
            att = (pose.get("attitude") or [None, None, None, None])[:4]
            lv = (pose.get("linear_velocity") or [None, None, None])[:3]
            av = (pose.get("angular_velocity") or [None, None, None])[:3]
            la = (pose.get("linear_acceleration") or [None, None, None])[:3]

            rows.append(
                {
                    "traj_json": _norm_abs_path(json_path),
                    "traj_name": traj_name,
                    "uav_id": self.uav_id,
                    "step_idx": i,
                    "cmd_in_x": p_in.x,
                    "cmd_in_y": p_in.y,
                    "cmd_in_z": p_in.z,
                    "cmd_x": p_cmd.x,
                    "cmd_y": p_cmd.y,
                    "cmd_z": p_cmd.z,
                    "image_timestamp_s": ts_img,
                    "image_path": _norm_abs_path(img_path) if img_path.exists() else "",
                    "obs_pos_x": pos[0],
                    "obs_pos_y": pos[1],
                    "obs_pos_z": pos[2],
                    "obs_att_w": att[3] if len(att) >= 4 else None,
                    "obs_att_x": att[0] if len(att) >= 4 else None,
                    "obs_att_y": att[1] if len(att) >= 4 else None,
                    "obs_att_z": att[2] if len(att) >= 4 else None,
                    "obs_linvel_x": lv[0],
                    "obs_linvel_y": lv[1],
                    "obs_linvel_z": lv[2],
                    "obs_angvel_x": av[0],
                    "obs_angvel_y": av[1],
                    "obs_angvel_z": av[2],
                    "obs_linacc_x": la[0],
                    "obs_linacc_y": la[1],
                    "obs_linacc_z": la[2],
                    "ulg_path": "",
                }
            )

        try:
            _controller_command(self.control_base, self.uav_id, {"cmd": "land", "force": True}, timeout=max(60.0, self.cmd_timeout))
        except Exception as e:
            self._log(f"[UAV{self.uav_id}] land failed traj={traj_name} err={e}")

        time.sleep(2.0)
        ulg_src = _find_latest_ulg(self.uav_id, since_ts=traj_start_ts)
        ulg_filename = ""
        if ulg_src is not None and ulg_src.exists():
            ulg_filename = f"px4_uav{self.uav_id}_{int(time.time())}.ulg"
            ulg_dst = traj_dir / ulg_filename
            if not _schedule_ulg_copy(ulg_src, ulg_dst):
                ulg_filename = ""

        ulg_path_str = ulg_filename
        for r in rows:
            r["ulg_path"] = ulg_path_str

        _write_csv(rows, csv_path)

        self._log(f"[UAV{self.uav_id}] done traj={traj_name} csv={csv_path}")

        init_pos = [self.base_x, self.base_y, self.base_z]
        self._log(f"[UAV{self.uav_id}] reset(hard=True) after traj={traj_name}")
        _controller_reset(
            self.control_base,
            self.uav_id,
            hard=True,
            force=True,
            position=init_pos,
            yaw_deg=None,
            timeout=self.reset_timeout,
        )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", type=str, required=True)
    p.add_argument("--pattern", type=str, default="*.json")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "trajectory_recordings"))
    p.add_argument("--config", type=str, default=str(Path(__file__).resolve().parent / "multi_uav_config.json"))
    p.add_argument("--uav-ids", type=str, default="")
    p.add_argument("--control-base", type=str, default="http://127.0.0.1:5008")
    p.add_argument("--image-base", type=str, default="http://127.0.0.1:8081")
    p.add_argument("--scale", type=float, default=0.01)
    p.add_argument("--base-x", type=float, default=0.0)
    p.add_argument("--base-y", type=float, default=0.0)
    p.add_argument("--base-z", type=float, default=2.0)
    zg = p.add_mutually_exclusive_group()
    zg.add_argument("--z-down", action="store_true", default=None)
    zg.add_argument("--z-up", action="store_true", default=None)
    p.add_argument("--reset-timeout", type=float, default=240.0)
    p.add_argument("--cmd-timeout", type=float, default=120.0)
    p.add_argument("--image-timeout", type=float, default=10.0)
    p.add_argument("--image-retries", type=int, default=30)
    sg = p.add_mutually_exclusive_group()
    sg.add_argument("--skip-existing", action="store_true", default=None)
    sg.add_argument("--no-skip-existing", action="store_true", default=None)
    p.add_argument("--dry-run", action="store_true", default=False)
    args = p.parse_args()

    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = _iter_json_files(input_dir, args.pattern)
    if not json_files:
        raise SystemExit(f"no json files found under {input_dir} with pattern={args.pattern}")

    if bool(args.dry_run):
        total_pts = 0
        for fp in json_files:
            try:
                pts = _load_preprocessed_xyz(fp)
                total_pts += len(pts)
                print(f"{fp}: {len(pts)} points")
            except Exception as e:
                print(f"{fp}: error {e}")
        print(f"files={len(json_files)} total_points={total_pts}")
        return

    if args.uav_ids.strip():
        uav_ids = sorted(set(int(x.strip()) for x in args.uav_ids.split(",") if x.strip()))
    else:
        uav_ids = _load_uav_ids_from_config(Path(args.config).resolve())

    control_base = str(args.control_base).rstrip("/")
    image_base = str(args.image_base).rstrip("/")
    if args.z_down is None and args.z_up is None:
        z_down = True
    else:
        z_down = bool(args.z_down) and not bool(args.z_up)
    if args.skip_existing is None and args.no_skip_existing is None:
        skip_existing = True
    else:
        skip_existing = bool(args.skip_existing) and not bool(args.no_skip_existing)

    _ensure_control_healthy(control_base, uav_ids=uav_ids, timeout_s=120.0)

    q: "queue.Queue[Path]" = queue.Queue()
    for f in json_files:
        q.put(f)

    print_lock = threading.Lock()
    threads: List[threading.Thread] = []
    for vid in uav_ids:
        w = Worker(
            uav_id=vid,
            control_base=control_base,
            image_base=image_base,
            out_dir=out_dir,
            task_queue=q,
            scale=args.scale,
            base_x=args.base_x,
            base_y=args.base_y,
            base_z=args.base_z,
            z_down=z_down,
            reset_timeout=args.reset_timeout,
            cmd_timeout=args.cmd_timeout,
            image_timeout=args.image_timeout,
            image_retries=args.image_retries,
            skip_existing=skip_existing,
            print_lock=print_lock,
        )
        t = threading.Thread(target=w.run, name=f"uav{vid}", daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
