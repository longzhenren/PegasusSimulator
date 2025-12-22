#!/usr/bin/env python3
"""
Gateway Server（统一 HTTP 网关 + 录制浏览）

概述
- 统一入口：将 `/{uav/<id>/reset|step|command}` 请求按命名空间映射转发到控制器实例（`rospy_isaacsim.py`）。
- 录制浏览：列出 `recordings/` 目录并支持打包为 ZIP（由 `8_camera_vehicle.py` 生成PNG/CSV）。

端点
- `GET /health`：健康检查与命名空间端口存活探测。
- `POST/GET /uav/<id>/{reset|step|command}`：透明转发到对应的控制器 HTTP 服务。
- `GET /sessions`、`GET /start_zip`、`GET /progress/<task_id>`、`GET /zip/<task_id>`、`GET /files/<session>/<filename>`：录制浏览与打包。

环境变量
- `PEGASUS_NS_PORT_MAP`：JSON 字典，形如 `{"uav0":5009,"uav1":5010,...}`；若缺省则从 `multi_uav_config.json` 自动生成。
- `PEGASUS_GATEWAY_PORT`：网关监听端口，默认 `5008`（由启动器设置）。

与系统的关系
- 由 `launch_multi_rospy.py` 启动，控制器端口默认 `5009 + vid` 与网关映射一致；仿真端图像服务默认在 `8081`。
"""
import os
import io
import json
import time
import zipfile
import mimetypes
import threading
from urllib.parse import unquote
import urllib.request, urllib.error
from flask import Flask, request, jsonify, Response
import subprocess as sp
import signal
from pathlib import Path

# Directory where recordings (session_*) are saved by 8_camera_vehicle.py
RECORD_DIR = os.path.join(os.path.dirname(__file__), "recordings")

# Global zip task registry for progress tracking
ZIP_TASKS = {}
TASK_COUNTER = 0
TASK_LOCK = threading.Lock()

def list_sessions():
    sessions = []
    if not os.path.isdir(RECORD_DIR):
        return sessions
    for name in sorted(os.listdir(RECORD_DIR)):
        path = os.path.join(RECORD_DIR, name)
        if not os.path.isdir(path):
            continue
        try:
            st = os.stat(path)
            stats = session_stats(name)
            sessions.append({
                "name": name,
                "mtime": st.st_mtime,
                "counts": stats,
            })
        except Exception:
            pass
    return sessions

def session_stats(session_name: str):
    path = os.path.join(RECORD_DIR, session_name)
    stats = {"files": 0, "png": 0, "csv": 0}
    if not os.path.isdir(path):
        return stats
    for name in os.listdir(path):
        p = os.path.join(path, name)
        if not os.path.isfile(p):
            continue
        stats["files"] += 1
        lower = name.lower()
        if lower.endswith(".png"):
            stats["png"] += 1
        elif lower.endswith(".csv"):
            stats["csv"] += 1
    return stats

def _safe_session_path(session_name: str):
    base = os.path.abspath(RECORD_DIR)
    target = os.path.abspath(os.path.join(RECORD_DIR, session_name))
    if not target.startswith(base + os.sep):
        return None
    if not os.path.isdir(target):
        return None
    return target

def _create_zip_task(session_name: str, ext: str = None, uav: str = None, since: int = None, until: int = None):
    global TASK_COUNTER
    with TASK_LOCK:
        TASK_COUNTER += 1
        task_id = str(TASK_COUNTER)
    session_path = _safe_session_path(session_name)
    if session_path is None:
        raise FileNotFoundError("Invalid session")

    # Pre-scan files to compute total
    files = []
    for name in os.listdir(session_path):
        path = os.path.join(session_path, name)
        if not os.path.isfile(path):
            continue
        if ext and not name.lower().endswith("." + ext.lower()):
            continue
        if uav is not None and not name.startswith(f"uav{uav}"):
            continue
        # Timestamp filtering from filename (uav<ID>_<ts_ms>.<ext>)
        try:
            base = os.path.splitext(name)[0]
            ts_ms = int(base.split("_")[-1])
        except Exception:
            ts_ms = None
        if since is not None and (ts_ms is None or ts_ms < since):
            continue
        if until is not None and (ts_ms is None or ts_ms > until):
            continue
        files.append((name, path))

    buf = io.BytesIO()
    task = {
        "id": task_id,
        "state": "pending",
        "session": session_name,
        "ext": ext,
        "uav": uav,
        "since": since,
        "until": until,
        "total": len(files),
        "processed": 0,
        "created_at": time.time(),
        "result_bytes": None,
        "filename": f"{session_name}_{int(time.time())}.zip",
    }
    ZIP_TASKS[task_id] = task

    def _worker():
        task["state"] = "running"
        try:
            with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for name, path in files:
                    zf.write(path, arcname=os.path.join(session_name, name))
                    task["processed"] += 1
            task["result_bytes"] = buf.getvalue()
            task["state"] = "done"
        except Exception as e:
            task["state"] = "error"
            task["error"] = str(e)

    threading.Thread(target=_worker, name=f"zip-{task_id}", daemon=True).start()
    return task_id

NS_PORT_MAP = {}
PORT_STATUS = {}

# _NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

_PORT_PROBE_LOCK = threading.Lock()
_LAST_PROBE_TS = 0.0

_FORWARD_TIMEOUT_RESET_S = float(os.environ.get("PEGASUS_GATEWAY_TIMEOUT_RESET", "300"))
_FORWARD_TIMEOUT_STEP_S = float(os.environ.get("PEGASUS_GATEWAY_TIMEOUT_STEP", "120"))
_FORWARD_TIMEOUT_COMMAND_S = float(os.environ.get("PEGASUS_GATEWAY_TIMEOUT_COMMAND", "60"))

def _probe_port(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as resp:
            return resp.getcode() == 200
    except Exception:
        return False

def _refresh_port_status(min_interval_s: float = 0.5) -> None:
    global _LAST_PROBE_TS
    now = time.time()
    if (now - _LAST_PROBE_TS) < float(min_interval_s):
        return
    with _PORT_PROBE_LOCK:
        now2 = time.time()
        if (now2 - _LAST_PROBE_TS) < float(min_interval_s):
            return
        for ns, port in (NS_PORT_MAP or {}).items():
            try:
                PORT_STATUS[ns] = _probe_port(int(port))
            except Exception:
                PORT_STATUS[ns] = False
        _LAST_PROBE_TS = now2

# Legacy BaseHTTPRequestHandler implementation removed in favor of Flask app


def _render_index() -> str:
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Recording Server</title>
  <style>
    body { font-family: system-ui, Arial, sans-serif; padding: 16px; }
    h1 { font-size: 20px; }
    .session { border: 1px solid #ddd; padding: 8px; margin: 8px 0; }
    .controls { margin-top: 6px; }
    button { margin-right: 8px; }
    .task { border: 1px dashed #aaa; padding: 8px; margin: 8px 0; }
    .progress { width: 240px; height: 12px; background: #eee; position: relative; }
    .bar { height: 100%; background: #4caf50; width: 0%; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; }
  </style>
</head>
<body>
  <h1>录制列表（按目录）</h1>
  <div id="sessions"></div>
  <h2>打包任务</h2>
  <div id="tasks"></div>
  <script>
  async function loadSessions() {
    const res = await fetch('/sessions');
    const js = await res.json();
    const list = js.sessions || [];
    const root = document.getElementById('sessions');
    root.innerHTML = '';
    list.forEach(s => {
      const div = document.createElement('div');
      div.className = 'session';
      const m = new Date(s.mtime * 1000).toLocaleString();
      const c = s.counts || {files:0,png:0,csv:0};
      div.innerHTML = `
        <div class=\"mono\">${s.name}</div>
        <div>更新时间：${m}</div>
        <div>文件数：${c.files}（PNG: ${c.png}，CSV: ${c.csv}）</div>
        <div class=\"controls\">
          <button onclick=\"startZip('${s.name}','all')\">打包全部</button>
          <button onclick=\"startZip('${s.name}','png')\">只打包 PNG</button>
          <button onclick=\"startZip('${s.name}','csv')\">只打包 CSV</button>
        </div>
      `;
      root.appendChild(div);
    });
  }

  async function startZip(session, ext) {
    const res = await fetch(`/start_zip?session=${encodeURIComponent(session)}&ext=${encodeURIComponent(ext)}`);
    const js = await res.json();
    if (!js.task_id) { alert('启动打包失败: ' + JSON.stringify(js)); return; }
    addTask(js.task_id, session, ext);
  }

  const tasks = {};
  function addTask(id, session, ext) {
    const root = document.getElementById('tasks');
    const div = document.createElement('div');
    div.className = 'task';
    div.id = 'task_'+id;
    div.innerHTML = `
      <div class=\"mono\">任务 #${id} / ${session} / ${ext}</div>
      <div class=\"progress\"><div class=\"bar\" id=\"bar_${id}\"></div></div>
      <div id=\"info_${id}\">初始化...</div>
      <div id=\"link_${id}\"></div>
    `;
    root.appendChild(div);
    tasks[id] = setInterval(() => updateTask(id), 500);
  }

  async function updateTask(id) {
    const res = await fetch(`/progress/${id}`);
    const js = await res.json();
    const total = js.total || 0;
    const processed = js.processed || 0;
    const state = js.state || 'unknown';
    const info = document.getElementById('info_'+id);
    const bar = document.getElementById('bar_'+id);
    const link = document.getElementById('link_'+id);
    const pct = total > 0 ? Math.round(processed*100/total) : (state==='done'?100:0);
    bar.style.width = pct + '%';
    info.textContent = `状态：${state}，进度：${processed}/${total}`;
    if (state === 'done') {
      clearInterval(tasks[id]);
      tasks[id] = null;
      link.innerHTML = `<a href=\"/zip/${id}\">下载ZIP（${js.filename||('session_'+id+'.zip')}）</a>`;
    }
    if (state === 'error') {
      clearInterval(tasks[id]);
      tasks[id] = null;
      link.textContent = '错误：' + (js.error || '未知错误');
    }
  }

  loadSessions();
  </script>
</body>
</html>
    """

app = Flask(__name__)

def _route_to_controller(uid: int, sub: str):
    ns = f"uav{uid}"
    port = NS_PORT_MAP.get(ns)
    if not port:
        return jsonify({"error": "namespace not found", "ns": ns}), 404
    url = f"http://127.0.0.1:{port}/{sub}"
    if sub == "reset":
        timeout_s = _FORWARD_TIMEOUT_RESET_S
    elif sub == "step":
        timeout_s = _FORWARD_TIMEOUT_STEP_S
    else:
        timeout_s = _FORWARD_TIMEOUT_COMMAND_S
    body = request.get_data() if request.content_length and request.content_length > 0 else None
    req = urllib.request.Request(url, data=body, method=request.method)
    ct = request.headers.get('Content-Type')
    if ct:
        req.add_header('Content-Type', ct)
    ac = request.headers.get('Accept')
    if ac:
        req.add_header('Accept', ac)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            hdr_ct = resp.headers.get('Content-Type', 'application/json')
            return Response(raw, status=resp.getcode(), headers={'Content-Type': hdr_ct, 'Content-Length': str(len(raw))})
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = json.dumps({'error': str(e)}).encode('utf-8')
        hdr_ct = e.headers.get('Content-Type', 'application/json')
        return Response(raw, status=e.code, headers={'Content-Type': hdr_ct, 'Content-Length': str(len(raw))})
    except Exception as e:
        msg = str(e)
        if 'Connection refused' in msg or 'timed out' in msg:
            return jsonify({"error": msg, "ns": ns, "port": port, "hint": "controller unreachable"}), 502
        return jsonify({'error': msg}), 502

@app.route('/health', methods=['GET', 'POST'])
def health():
    _refresh_port_status()
    return jsonify({"status": "ok", "namespaces": {k: {"port": NS_PORT_MAP.get(k), "alive": bool(PORT_STATUS.get(k))} for k in NS_PORT_MAP.keys()}})

@app.route('/uav/<int:uid>/<sub>', methods=['GET', 'POST'])
def uav_forward(uid: int, sub: str):
    if sub not in ("reset", "step", "command"):
        return jsonify({"error": "unknown endpoint"}), 404
    resp = _route_to_controller(uid, sub)
    try:
        if sub == "reset" and getattr(resp, "status_code", 500) < 300:
            _restart_mavros(uid)
    except Exception as e:
        print(f"[WARN] restart mavros on reset failed: {e}")
    return resp

def _find_mavros_pids(uid: int):
    try:
        out = sp.check_output(["ps", "-eo", "pid,args"], stderr=sp.DEVNULL).decode().splitlines()
    except Exception as e:
        print(f"[WARN] _find_mavros_pids ps failed: {e}")
        return []
    hits = []
    token = f"pegasus_uav_{uid}.launch"
    for line in out:
        low = line.lower()
        if ("ros2" in low) and ("launch" in low) and (token in low):
            try:
                pid = int(low.split()[0])
                hits.append(pid)
            except Exception as e:
                print(f"[WARN] parse pid failed: {e}")
    return list(sorted(set(hits)))

def _restart_mavros(uid: int) -> bool:
    try:
        ros2_cmd = os.environ.get("ISAACSIM_ROS2_CMD", "/home/user/IsaacSim-ros_workspaces/build_ws/humble/humble_ws/install/bin/ros2")
        launch_file = str(Path(os.path.dirname(__file__)) / "launch" / f"pegasus_uav_{uid}.launch")
        for pid in _find_mavros_pids(uid):
            try:
                pgid = os.getpgid(int(pid))
                os.killpg(pgid, signal.SIGTERM)
                time.sleep(0.5)
                os.killpg(pgid, signal.SIGKILL)
            except Exception as e:
                print(f"[WARN] killpg failed for pid {pid}: {e}")
        sp.Popen([ros2_cmd, "launch", launch_file], preexec_fn=os.setsid)
        return True
    except Exception as e:
        print(f"[WARN] _restart_mavros failed: {e}")
        return False

@app.route('/', methods=['GET'])
def index():
    return Response(_render_index(), status=200, mimetype='text/html; charset=utf-8')

@app.route('/sessions', methods=['GET'])
def sessions():
    return jsonify({"dir": RECORD_DIR, "sessions": list_sessions()})

@app.route('/start_zip', methods=['GET'])
def start_zip():
    session = request.args.get("session")
    ext = request.args.get("ext")
    if ext == "all":
        ext = None
    uav = request.args.get("uav")
    since = request.args.get("since")
    until = request.args.get("until")
    try:
        since = int(since) if since is not None else None
    except Exception:
        since = None
    try:
        until = int(until) if until is not None else None
    except Exception:
        until = None
    if not session:
        return jsonify({"error": "missing session"}), 400
    try:
        task_id = _create_zip_task(session, ext, uav, since, until)
    except FileNotFoundError:
        return jsonify({"error": "invalid session"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"task_id": task_id})

@app.route('/progress/<task_id>', methods=['GET'])
def progress(task_id: str):
    task = ZIP_TASKS.get(task_id)
    if not task:
        return jsonify({"error": "task not found"}), 404
    return jsonify({
        "id": task_id,
        "state": task.get("state"),
        "total": task.get("total"),
        "processed": task.get("processed"),
        "filename": task.get("filename"),
        "session": task.get("session"),
        "error": task.get("error"),
    })

@app.route('/zip/<task_id>', methods=['GET'])
def zip_download(task_id: str):
    task = ZIP_TASKS.get(task_id)
    if not task:
        return jsonify({"error": "task not found"}), 404
    if task.get("state") != "done" or task.get("result_bytes") is None:
        return jsonify({"error": "task not completed"}), 409
    data = task["result_bytes"]
    filename = task.get("filename", f"session_{task_id}.zip")
    return Response(data, status=200, headers={
        'Content-Type': 'application/zip',
        'Content-Disposition': f'attachment; filename="{filename}"',
        'Content-Length': str(len(data)),
    })

@app.route('/files/<session>/<path:filename>', methods=['GET'])
def file_download(session: str, filename: str):
    session_path = _safe_session_path(session)
    if session_path is None:
        return jsonify({"error": "invalid session"}), 404
    path = os.path.join(session_path, filename)
    if not os.path.isfile(path):
        return jsonify({"error": "file not found", "name": filename}), 404
    with open(path, "rb") as f:
        data = f.read()
    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    return Response(data, status=200, headers={
        'Content-Type': ctype,
        'Content-Disposition': f'attachment; filename="{os.path.basename(filename)}"',
        'Content-Length': str(len(data)),
    })

@app.route('/<path:_path>', methods=['GET', 'POST'])
def unknown(_path: str):
    return jsonify({"error": "unknown endpoint"}), 404

def main(host: str = "0.0.0.0", port: int = 5008):
    os.makedirs(RECORD_DIR, exist_ok=True)
    global NS_PORT_MAP
    try:
        NS_PORT_MAP = json.loads(os.environ.get("PEGASUS_NS_PORT_MAP", "{}")) or {}
    except Exception:
        NS_PORT_MAP = {}
    if not NS_PORT_MAP:
        try:
            cfg_path = os.environ.get("PEGASUS_CONFIG_PATH") or os.path.join(os.path.dirname(__file__), "multi_uav_config.json")
            with open(cfg_path, "r") as f:
                cfg = json.load(f)
            base_port = int(os.environ.get("PEGASUS_NS_BASE_PORT", "5009"))
            temp = {}
            for v in cfg.get("vehicles", []):
                vid = int(v.get("vehicle_id", 0))
                ns = v.get("mavros_namespace", f"uav{vid}")
                temp[ns] = base_port + vid
            NS_PORT_MAP = temp
        except Exception:
            NS_PORT_MAP = {}
    try:
        PORT_STATUS.update({ns: _probe_port(p) for ns, p in NS_PORT_MAP.items()})
        print(f"[GW] namespaces={ {ns: {'port': p, 'alive': PORT_STATUS.get(ns)} for ns, p in NS_PORT_MAP.items()} }")
    except Exception as e:
        print(f"[WARN] probe port failed: {e}")
    gw_port = int(os.environ.get("PEGASUS_GATEWAY_PORT", str(port)))
    print(f"Recording+Gateway server ready: http://{host}:{gw_port}/ (serving {RECORD_DIR}) namespaces={list(NS_PORT_MAP.keys())}")
    app.run(host=host, port=gw_port, threaded=True)


if __name__ == "__main__":
    main()
