import os
import json
from typing import Any, Dict, List, Tuple

import requests
from flask import Flask, request, jsonify


class CompatProxy:
    def __init__(self) -> None:
        self.mode = str(os.environ.get("ENV_COMPAT_BACKEND", "rospy")).strip().lower()
        self.timeout = float(os.environ.get("ENV_COMPAT_TIMEOUT", "60"))

        self.rospy_url = str(os.environ.get("ROSPY_BASE_URL", "http://127.0.0.1:5009")).rstrip("/")
        self.multi_base_port = int(os.environ.get("MULTI_ROSPY_BASE_PORT", "5009"))
        vids = str(os.environ.get("MULTI_ROSPY_VIDS", "0")).strip()
        self.multi_vids: List[int] = [int(x) for x in vids.split(",") if x.strip()]

        self.pgsim_url = str(os.environ.get("PGSIM_BASE_URL", "http://127.0.0.1:8080")).rstrip("/")
        pgsim_ids = str(os.environ.get("PGSIM_UAV_IDS", "0")).strip()
        self.pgsim_uav_ids: List[int] = [int(x) for x in pgsim_ids.split(",") if x.strip()]
        self.pgsim_default_pos: List[float] = [
            float(os.environ.get("PGSIM_DEFAULT_X", "0.0")),
            float(os.environ.get("PGSIM_DEFAULT_Y", "0.0")),
            float(os.environ.get("PGSIM_DEFAULT_Z", "0.5")),
        ]

        self.batch_size = 0
        self.batch_map: List[Tuple[str, int]] = []

    def _rospy_health(self, base: str) -> Tuple[bool, Dict[str, Any]]:
        try:
            url = f"{base}/health"
            r = requests.get(url, timeout=self.timeout)
            ok = bool(r.status_code == 200)
            data = r.json() if ok else {"status": "error", "code": r.status_code}
            return ok, {"status": "healthy" if ok else "error", "detail": data}
        except Exception as e:
            return False, {"status": "error", "error": str(e)}

    def _pgsim_health(self, base: str, uav_id: int) -> Tuple[bool, Dict[str, Any]]:
        try:
            url = f"{base}/uav/{int(uav_id)}/pose"
            r = requests.get(url, timeout=self.timeout)
            ok = bool(r.status_code == 200)
            data = r.json() if ok else {"status": "error", "code": r.status_code}
            return ok, {"status": "healthy" if ok else "error", "detail": data}
        except Exception as e:
            return False, {"status": "error", "error": str(e)}

    def health(self) -> Tuple[bool, Dict[str, Any]]:
        if self.mode == "rospy":
            ok, d = self._rospy_health(self.rospy_url)
            return ok, d
        if self.mode == "multi_rospy":
            agg = []
            all_ok = True
            for vid in self.multi_vids:
                base = f"http://127.0.0.1:{self.multi_base_port + int(vid)}"
                ok, d = self._rospy_health(base)
                all_ok = all_ok and ok
                agg.append({"vid": int(vid), "base": base, "result": d})
            return all_ok, {"status": "healthy" if all_ok else "error", "results": agg}
        if self.mode == "pgsim":
            agg = []
            all_ok = True
            for uid in self.pgsim_uav_ids:
                ok, d = self._pgsim_health(self.pgsim_url, int(uid))
                all_ok = all_ok and ok
                agg.append({"uav_id": int(uid), "base": self.pgsim_url, "result": d})
            return all_ok, {"status": "healthy" if all_ok else "error", "results": agg}
        return False, {"status": "error", "error": f"unknown mode {self.mode}"}

    def reset(self, batch_size: int) -> Dict[str, Any]:
        self.batch_size = max(1, int(batch_size))
        self.batch_map = []
        images: List[List[str]] = []
        json_names: List[str] = []
        if self.mode == "rospy":
            base = self.rospy_url
            try:
                payload = {"force": True, "hard": True}
                requests.post(f"{base}/reset", json=payload, timeout=self.timeout)
            except Exception:
                pass
            self.batch_map.append((base, 0))
            json_names.append("uav0")
            images.append([])
            return {"status": "success", "images": images, "json_names": json_names, "message": "reset ok", "batch_size": 1}
        if self.mode == "multi_rospy":
            for i in range(self.batch_size):
                vid = self.multi_vids[i] if i < len(self.multi_vids) else self.multi_vids[-1]
                base = f"http://127.0.0.1:{self.multi_base_port + int(vid)}"
                try:
                    payload = {"force": True, "hard": True, "vid": int(vid)}
                    requests.post(f"{base}/reset", json=payload, timeout=self.timeout)
                except Exception:
                    pass
                self.batch_map.append((base, int(vid)))
                json_names.append(f"uav{int(vid)}")
                images.append([])
            return {"status": "success", "images": images, "json_names": json_names, "message": "reset ok", "batch_size": len(images)}
        if self.mode == "pgsim":
            for i in range(self.batch_size):
                uid = self.pgsim_uav_ids[i] if i < len(self.pgsim_uav_ids) else self.pgsim_uav_ids[-1]
                try:
                    payload = {"position": self.pgsim_default_pos}
                    requests.post(f"{self.pgsim_url}/uav/{int(uid)}/reset", json=payload, timeout=self.timeout)
                except Exception:
                    pass
                self.batch_map.append((self.pgsim_url, int(uid)))
                json_names.append(f"uav{int(uid)}")
                images.append([])
            return {"status": "success", "images": images, "json_names": json_names, "message": "reset ok", "batch_size": len(images)}
        return {"status": "error", "message": f"unknown mode {self.mode}"}

    def step(self, actions_batch: List[List[List[float]]], per_step: bool = False) -> Dict[str, Any]:
        images_batch: List[List[str]] = []
        done_batch: List[bool] = []
        if self.mode == "rospy":
            base = self.rospy_url
            try:
                simp: List[List[float]] = []
                first = actions_batch[0] if actions_batch else []
                for pt in first:
                    if isinstance(pt, (list, tuple)) and len(pt) >= 3:
                        simp.append([float(pt[0]), float(pt[1]), float(pt[2])])
                payload = {"actions": [simp], "per_step": bool(per_step), "force": True}
                r = requests.post(f"{base}/step", json=payload, timeout=self.timeout)
                r.raise_for_status()
                obj = r.json()
                ims = obj.get("images") or []
                dns = obj.get("dones") or obj.get("done") or []
                images_batch = ims if isinstance(ims, list) else []
                done_batch = [bool(x) for x in (dns if isinstance(dns, list) else [])]
            except Exception:
                images_batch = [[]]
                done_batch = [False]
            return {"status": "success", "images": images_batch, "done": done_batch, "message": "step ok", "batch_size": len(images_batch)}
        if self.mode == "multi_rospy":
            for i in range(self.batch_size):
                base, vid = self.batch_map[i] if i < len(self.batch_map) else (f"http://127.0.0.1:{self.multi_base_port}", 0)
                try:
                    seq = actions_batch[i] if i < len(actions_batch) else actions_batch[0] if actions_batch else []
                    simp: List[List[float]] = []
                    for pt in seq:
                        if isinstance(pt, (list, tuple)) and len(pt) >= 3:
                            simp.append([float(pt[0]), float(pt[1]), float(pt[2])])
                    payload = {"actions": [simp], "per_step": bool(per_step), "force": True}
                    r = requests.post(f"{base}/step", json=payload, timeout=self.timeout)
                    r.raise_for_status()
                    obj = r.json()
                    ims = obj.get("images") or []
                    dns = obj.get("dones") or obj.get("done") or []
                    images_batch.append(ims[0] if ims else [])
                    done_batch.append(bool(dns[0]) if dns else False)
                except Exception:
                    images_batch.append([])
                    done_batch.append(False)
            return {"status": "success", "images": [[x for x in y] if isinstance(y, list) else [] for y in images_batch], "done": [bool(x) for x in done_batch], "message": "step ok", "batch_size": len(images_batch)}
        if self.mode == "pgsim":
            for i in range(self.batch_size):
                base, uid = self.batch_map[i] if i < len(self.batch_map) else (self.pgsim_url, self.pgsim_uav_ids[0] if self.pgsim_uav_ids else 0)
                seq = actions_batch[i] if i < len(actions_batch) else actions_batch[0] if actions_batch else []
                frames: List[str] = []
                if seq and per_step:
                    for pt in seq:
                        if isinstance(pt, (list, tuple)) and len(pt) >= 3:
                            pos = [float(pt[0]), float(pt[1]), float(pt[2])]
                            try:
                                requests.post(f"{base}/uav/{int(uid)}/reset", json={"position": pos}, timeout=self.timeout)
                                rr = requests.get(f"{base}/uav/{int(uid)}/image", timeout=self.timeout)
                                obj = rr.json()
                                b64 = obj.get("data") or (obj.get("image") or {}).get("data")
                                if isinstance(b64, str) and b64:
                                    frames.append(b64)
                            except Exception:
                                pass
                else:
                    last = None
                    for pt in seq:
                        if isinstance(pt, (list, tuple)) and len(pt) >= 3:
                            last = [float(pt[0]), float(pt[1]), float(pt[2])]
                    try:
                        if last is not None:
                            requests.post(f"{base}/uav/{int(uid)}/reset", json={"position": last}, timeout=self.timeout)
                        rr = requests.get(f"{base}/uav/{int(uid)}/image", timeout=self.timeout)
                        obj = rr.json()
                        b64 = obj.get("data") or (obj.get("image") or {}).get("data")
                        if isinstance(b64, str) and b64:
                            frames.append(b64)
                    except Exception:
                        pass
                images_batch.append(frames)
                done_batch.append(False)
            return {"status": "success", "images": images_batch, "done": done_batch, "message": "step ok", "batch_size": len(images_batch)}
        return {"status": "error", "message": f"unknown mode {self.mode}"}


app = Flask(__name__)
compat = CompatProxy()


@app.route("/health", methods=["GET"])
def health_route():
    ok, payload = compat.health()
    code = 200 if ok else 500
    return jsonify(payload), code


@app.route("/reset", methods=["POST"])
def reset_route():
    data = request.json or {}
    try:
        bs = int(data.get("batch_size", 1))
    except Exception:
        bs = 1
    resp = compat.reset(bs)
    return jsonify(resp)


@app.route("/step", methods=["POST"])
def step_route():
    data = request.json or {}
    actions = data.get("actions") or []
    per_step = bool(data.get("per_step", False))
    resp = compat.step(actions, per_step=per_step)
    return jsonify(resp)


def main() -> None:
    port = int(os.environ.get("ENV_COMPAT_PORT", "5010"))
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)


if __name__ == "__main__":
    main()

