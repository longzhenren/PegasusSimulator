#!/usr/bin/env python3
"""
HTTP 接口详测脚本（controller直连与gateway对比）

目标
- 覆盖：输入参数、输入格式、调用接口、请求格式、等待时间、状态码、返回值、cookie变化、异常信息、解析结果。
- 对比：直接调用控制器接口 vs 通过网关转发的接口，输出差异。

前置
- 网关：`http://127.0.0.1:5008`；控制器（默认）：`http://127.0.0.1:5009+vid`。
- 仿真HTTP：`http://127.0.0.1:8080`。
"""
import time
import json
import requests
from http.cookies import SimpleCookie

GATEWAY = "http://127.0.0.1:5008"
CTRL_BASE = "http://127.0.0.1:{port}"
SIM_BASE = "http://127.0.0.1:8080"

def print_req_info(label, method, url, json_payload=None, timeout=None):
    print(f"\n[{label}] method={method} url={url}")
    print(f"payload(JSON)={json.dumps(json_payload, ensure_ascii=False)}")
    print(f"timeout={timeout}")

def print_resp_info(label, resp):
    print(f"[{label}] status_code={resp.status_code}")
    print(f"[{label}] headers={dict(resp.headers)}")
    print(f"[{label}] cookies(before)={resp.cookies.get_dict()}")
    try:
        js = resp.json()
        body = js
        parsed = True
    except Exception:
        body = resp.text
        parsed = False
    print(f"[{label}] parsed_json={parsed}")
    print(f"[{label}] body_sample={str(body)[:400]}")
    print(f"[{label}] elapsed={resp.elapsed.total_seconds()}s")
    return body

def req(method, url, payload=None, timeout=5, session=None):
    s = session or requests.Session()
    print_req_info("REQ", method, url, payload, timeout)
    try:
        if method.upper() == "POST":
            r = s.post(url, json=payload, timeout=timeout)
        else:
            r = s.get(url, timeout=timeout)
    except requests.RequestException as e:
        print(f"[REQ] exception={e}")
        raise
    body = print_resp_info("RESP", r)
    return r.status_code, body, r.cookies

def controller_port(uav: int) -> int:
    return 5009 + int(uav)

def compare(label, gw_tuple, direct_tuple):
    gw_code, gw_body, gw_ck = gw_tuple
    dc_code, dc_body, dc_ck = direct_tuple
    print(f"\n[COMPARE:{label}]\n- gateway: code={gw_code} cookies={gw_ck.get_dict()}\n- direct : code={dc_code} cookies={dc_ck.get_dict()}")
    try:
        print("- gateway.body keys:", list(gw_body.keys())[:20])
        print("- direct.body  keys:", list(dc_body.keys())[:20])
    except Exception:
        print("- body not dict; showing samples")
        print("- gateway sample:", str(gw_body)[:200])
        print("- direct  sample:", str(dc_body)[:200])

def main():
    uav = 0
    port = controller_port(uav)
    ctrl = CTRL_BASE.format(port=port)

    # 1) reset
    payload = {"position": [-88.0, 8.0, 5.0], "yaw_deg": 0.0}
    gw = req("POST", f"{GATEWAY}/uav/{uav}/reset", payload, timeout=10)
    direct = req("POST", f"{ctrl}/reset", payload, timeout=10)
    compare("reset", gw, direct)
    time.sleep(1.0)

    # 2) get_position
    payload = {"cmd": "get_position"}
    gw = req("POST", f"{GATEWAY}/uav/{uav}/command", payload)
    direct = req("POST", f"{ctrl}/command", payload)
    compare("get_position", gw, direct)
    pos = gw[1].get("position") if isinstance(gw[1], dict) else None
    pos = pos or {"x": 0.0, "y": 0.0, "z": 1.0}
    origin = (pos.get("x", 0.0), pos.get("y", 0.0), pos.get("z", 1.0))

    # 3) move_to, 4 directions
    def move_payload(dx, dy, dz):
        x0, y0, z0 = origin
        return {"cmd": "move_to", "x": x0 + dx, "y": y0 + dy, "z": z0 + dz}
    for name, d in [("forward",(1.0,0.0,0.0)), ("back",(-1.0,0.0,0.0)), ("left",(0.0,1.0,0.0)), ("right",(0.0,-1.0,0.0))]:
        payload = move_payload(*d)
        print(f"\n=== MOVE {name} ===")
        gw = req("POST", f"{GATEWAY}/uav/{uav}/command", payload)
        direct = req("POST", f"{ctrl}/command", payload)
        compare(f"move_{name}", gw, direct)
        time.sleep(0.5)

    # 4) sim /all
    gw_sim = req("GET", f"{SIM_BASE}/uav/{uav}/all")
    print("\n[SIM ALL] parsed keys:", list(gw_sim[1].keys()) if isinstance(gw_sim[1], dict) else "n/a")

if __name__ == "__main__":
    main()
