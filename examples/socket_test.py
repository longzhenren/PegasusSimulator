#!/usr/bin/env python3
"""
Socket 测试脚本：连接 127.0.0.1:8989，逐行发送全套命令，验证 IsaacSimEnv 的 Socket 接口。

用法：
- 确保被测节点已运行：`python3 examples/rospy_isaacsim.py --session-ts 1234567890`
- 运行本脚本：`python3 examples/socket_test.py --host 127.0.0.1 --port 8989 --out ./sensor_data --session-ts 1234567890`

说明：
- 本脚本写入的文件名将与被测节点的会话目录组合，例如 status/sensors/camera 子目录。
"""

import os
import json
import time
import argparse
import socket

def write_cmd(sock: socket.socket, cmd: dict):
    line = json.dumps(cmd) + "\n"
    sock.sendall(line.encode('utf-8'))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default=os.environ.get('PEGASUS_CMD_HOST', '127.0.0.1'), help='命令 Socket 主机')
    parser.add_argument('--port', type=int, default=int(os.environ.get('PEGASUS_CMD_PORT', '8989')), help='命令 Socket 端口')
    parser.add_argument('--out', default=os.environ.get('PEGASUS_OUTPUT_DIR', os.environ.get('PEGASUS_SAVE_DIR', 'sensor_data')), help='全局输出根目录（用于验证）')
    parser.add_argument('--session-ts', type=int, default=None, help='会话时间戳（需与被测节点一致）')
    args = parser.parse_args()

    # connect socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.port))

    # Commands sequence
    cmds = [
        {"cmd": "set_session_ts", "ts": 1234567890, "filename": "set_session_ok.json"},
        {"cmd": "get_status", "filename": "status.json"},
        {"cmd": "get_position", "filename": "position.json"},
        {"cmd": "move_to", "x": 0.0, "y": 0.0, "z": 2.0, "filename": "move_to_ok.json"},
        {"cmd": "get_sensors", "filename": "sensors_after_move.json"},
        {"cmd": "get_images", "filename": "frame001"},
        {"cmd": "move_to_many", "points": [[0.5, 0.0, 2.0], [0.5, 0.5, 2.0], [0.0, 0.5, 2.0]], "filename": "multi_ok.json"},
        {"cmd": "get_images", "filename_rgb": "rgb_after_multi.png", "filename_depth": "depth_after_multi.png"},
        {"cmd": "save_snapshot", "filename": "snapshot_status.json"},
        {"cmd": "land", "filename": "land_ok.json"},
        {"cmd": "shutdown", "filename": "shutdown_ok.json"},
    ]

    for c in cmds:
        write_cmd(sock, c)
        time.sleep(0.5)

    # Optional: quick verification of files existence
    root = args.out
    if args.session_ts:
        status_path = os.path.join(root, f"status_{args.session_ts}")
        sensors_path = os.path.join(root, f"sensors_{args.session_ts}")
        camera_path = os.path.join(root, f"camera_{args.session_ts}")
        print(f"Status dir:  {status_path}")
        print(f"Sensors dir: {sensors_path}")
        print(f"Camera dir:  {camera_path}")

        # Collect expected files
        expect_status = [
            'set_session_ok.json',
            'status.json',
            'position.json',
            'move_to_ok.json',
            'multi_ok.json',
            'snapshot_status.json',
            'land_ok.json',
            'shutdown_ok.json',
            'frame001.json',
        ]
        expect_sensors = [
            'sensors_after_move.json',
        ]
        expect_camera_any = [
            'frame001.png', 'frame001.npy',  # depending on PIL availability
            'frame001_depth.png',
            'rgb_after_multi.png', 'rgb_after_multi.npy',
            'depth_after_multi.png',
        ]

        # Verify
        def check_dir(dir_path, expected_files):
            ok = True
            if not os.path.isdir(dir_path):
                print(f"Directory missing: {dir_path}")
                return False
            existing = set(os.listdir(dir_path))
            for fname in expected_files:
                if fname not in existing:
                    ok = False
            print(f"Existing in {dir_path}: {sorted(existing)}")
            return ok

        # Status files
        status_ok = check_dir(status_path, expect_status)
        # Sensors snapshots
        sensors_ok = check_dir(sensors_path, expect_sensors)
        # Camera: at least one of any expected files should exist
        camera_ok = False
        if os.path.isdir(camera_path):
            cam_existing = set(os.listdir(camera_path))
            print(f"Existing in {camera_path}: {sorted(cam_existing)}")
            for fname in expect_camera_any:
                if fname in cam_existing:
                    camera_ok = True
                    break
        print("Summary:")
        print(f"  Status OK:  {status_ok}")
        print(f"  Sensors OK: {sensors_ok}")
        print(f"  Camera OK:  {camera_ok}")
    else:
        print("Skip verification: --session-ts not provided.\n"
              "Provide --session-ts to validate files under session directories.")

    try:
        sock.close()
    except Exception:
        pass

if __name__ == '__main__':
    main()