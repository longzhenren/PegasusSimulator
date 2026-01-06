#!/usr/bin/env python3
"""
HTTP 网关与录制服务器（recording_server.py）

==========================
概述
==========================
本脚本提供两大核心功能：
1. HTTP 网关：将 `/uav/<id>/...` 请求转发到对应的控制器实例
2. 录制浏览：列出、打包和下载仿真录制数据（由 8_camera_vehicle.py 生成）

==========================
启动流程
==========================
1. 解析环境变量配置
   - PEGASUS_NS_PORT_MAP: 命名空间到端口的映射
   - PEGASUS_GATEWAY_PORT: 网关监听端口（默认 5008）

2. 加载配置（若 NS_PORT_MAP 为空）
   - 读取 multi_uav_config.json
   - 自动生成端口映射（5009+vid）

3. 初始化 Flask 应用并注册路由

4. 启动 HTTP 服务器

==========================
HTTP 网关接口（端口 5008）
==========================
请求转发（透明代理）：
  POST /uav/<id>/reset
    - 转发到控制器 http://127.0.0.1:{5009+id}/reset
    - 超时：300s（由 PEGASUS_GATEWAY_TIMEOUT_RESET 配置）

  POST /uav/<id>/step
    - 转发到控制器 http://127.0.0.1:{5009+id}/step
    - 超时：120s（由 PEGASUS_GATEWAY_TIMEOUT_STEP 配置）

  POST /uav/<id>/command
    - 转发到控制器 http://127.0.0.1:{5009+id}/command
    - 超时：60s（由 PEGASUS_GATEWAY_TIMEOUT_COMMAND 配置）

健康检查：
  GET /health
    - 返回网关状态和各命名空间端口存活状态
    - 响应：{"status": "ok", "namespaces": {"uav0": {"port": 5009, "alive": true}, ...}}

==========================
录制浏览接口
==========================
GET /
  - 返回 HTML 页面，展示录制会话列表和打包功能

GET /sessions
  - 返回所有录制会话列表
  - 响应：{"dir": "/path/to/recordings", "sessions": [{
      "name": "session_1234567890",
      "mtime": 1234567890.0,
      "counts": {"files": 100, "png": 50, "csv": 50}
    }, ...]}

GET /start_zip?session=<name>&ext=<ext>&uav=<id>&since=<ts>&until=<ts>
  - 启动异步 ZIP 打包任务
  - 参数：
    - session: 会话名称（必需）
    - ext: 文件类型过滤（png/csv/all，可选）
    - uav: 指定 UAV ID 过滤（可选）
    - since/until: 时间戳范围过滤（可选，毫秒）
  - 响应：{"task_id": "1"}

GET /progress/<task_id>
  - 查询 ZIP 打包进度
  - 响应：{
      "id": "1",
      "state": "running",  // pending/running/done/error
      "total": 100,
      "processed": 50,
      "filename": "session_xxx_12345.zip"
    }

GET /zip/<task_id>
  - 下载已完成的 ZIP 文件
  - 响应：application/zip 二进制流

GET /files/<session>/<filename>
  - 下载单个录制文件
  - 响应：对应 MIME 类型的文件内容

==========================
调用关系
==========================
                           外部客户端 / trajectory_data_collector.py
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    recording_server.py                           │
│                  (HTTP 网关 端口: 5008)                          │
│                                                                  │
│   /uav/<id>/reset ────┐                                         │
│   /uav/<id>/step  ────┼──► 转发到 rospy_isaacsim.py (5009+id)   │
│   /uav/<id>/command ──┘                                         │
│                                                                  │
│   /sessions, /zip, ... ──► 录制文件浏览/打包                     │
└──────────────────────────────────────────────────────────────────┘

==========================
环境变量
==========================
PEGASUS_NS_PORT_MAP          JSON 格式的命名空间端口映射
                             示例：{"uav0":5009,"uav1":5010}

PEGASUS_GATEWAY_PORT         网关监听端口（默认：5008）

PEGASUS_CONFIG_PATH          配置文件路径（默认：./multi_uav_config.json）

PEGASUS_NS_BASE_PORT         控制器基础端口（默认：5009）

PEGASUS_GATEWAY_TIMEOUT_RESET    reset 请求超时（默认：300s）
PEGASUS_GATEWAY_TIMEOUT_STEP     step 请求超时（默认：120s）
PEGASUS_GATEWAY_TIMEOUT_COMMAND  command 请求超时（默认：60s）

==========================
录制目录结构
==========================
recordings/
  └── session_<timestamp>/
      ├── uav0_<ts_ms>.png    # UAV0 相机图像
      ├── uav0.csv            # UAV0 状态聚合 CSV
      ├── uav1_<ts_ms>.png
      ├── uav1.csv
      └── ...

CSV 文件字段：
  timestamp_ms, timestamp_s, uav_id, image_filename,
  image_width, image_height, image_channels,
  pos_x, pos_y, pos_z,
  att_w, att_x, att_y, att_z,
  linvel_x, linvel_y, linvel_z,
  angvel_x, angvel_y, angvel_z,
  linacc_x, linacc_y, linacc_z

==========================
使用示例（Python）
==========================
import requests

base = "http://127.0.0.1:5008"

# 健康检查
resp = requests.get(f"{base}/health")
print(resp.json())

# 通过网关发送 reset 命令到 UAV0
resp = requests.post(f"{base}/uav/0/reset", json={
    "vid": 0,
    "position": [0, 0, 2],
    "yaw_deg": 0,
    "hard": True,
    "force": True
}, timeout=300)
print(resp.json())

# 通过网关发送 command 到 UAV0
resp = requests.post(f"{base}/uav/0/command", json={
    "cmd": "move_to",
    "x": 1.0, "y": 0.5, "z": 2.0,
    "force": True
}, timeout=60)
print(resp.json())

# 获取录制会话列表
resp = requests.get(f"{base}/sessions")
sessions = resp.json()["sessions"]
print(f"Found {len(sessions)} sessions")

# 启动 ZIP 打包（仅 PNG 文件）
resp = requests.get(f"{base}/start_zip", params={
    "session": sessions[0]["name"],
    "ext": "png"
})
task_id = resp.json()["task_id"]

# 查询打包进度
import time
while True:
    resp = requests.get(f"{base}/progress/{task_id}")
    progress = resp.json()
    print(f"Progress: {progress['processed']}/{progress['total']}")
    if progress["state"] == "done":
        break
    time.sleep(1)

# 下载 ZIP 文件
resp = requests.get(f"{base}/zip/{task_id}")
with open("recordings.zip", "wb") as f:
    f.write(resp.content)

==========================
使用示例（curl）
==========================
# 健康检查
curl http://127.0.0.1:5008/health

# 通过网关发送命令
curl -X POST http://127.0.0.1:5008/uav/0/reset \
  -H "Content-Type: application/json" \
  -d '{"vid":0,"position":[0,0,2],"hard":true,"force":true}'

curl -X POST http://127.0.0.1:5008/uav/0/command \
  -H "Content-Type: application/json" \
  -d '{"cmd":"get_position"}'

# 获取会话列表
curl http://127.0.0.1:5008/sessions

# 启动打包
curl "http://127.0.0.1:5008/start_zip?session=session_xxx&ext=png"

# 查询进度
curl http://127.0.0.1:5008/progress/1

# 下载 ZIP
curl -o recordings.zip http://127.0.0.1:5008/zip/1

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
