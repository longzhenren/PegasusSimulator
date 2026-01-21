# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)
"""
测试环境服务器的客户端
用于测试env_server.py的reset和step接口
"""

import requests
import json
import base64
import argparse
import time
import os
from io import BytesIO
from PIL import Image
import numpy as np
from typing import List, Dict, Any, Optional
import threading
from urllib.parse import urlparse


class EnvServerClient:
    def __init__(self, server_url: str = "http://127.0.0.1:5010"):
        """初始化客户端
        
        Args:
            server_url: 环境服务器的基础URL（不包含接口路径）
        """
        self.server_url = server_url.rstrip('/')
        self.reset_url = f"{self.server_url}/reset"
        self.step_url = f"{self.server_url}/step"
        self.health_url = f"{self.server_url}/health"
    
    def health_check(self) -> bool:
        """检查服务器健康状态"""
        try:
            response = requests.get(self.health_url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"✓ 服务器健康: {data}")
                return True
            else:
                print(f"✗ 服务器响应异常: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ 无法连接到服务器: {e}")
            return False
    
    def reset(self, batch_size: int = 1) -> Optional[Dict[str, Any]]:
        """重置环境

        Args:
            batch_size: 批大小，默认为1

        Returns:
            响应字典，包含status, images, message等字段
        """
        payload = {
            "batch_size": batch_size
        }

        try:
            response = requests.post(
                self.reset_url,
                json=payload,
                timeout=200
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") == "success":
                images = result.get("images", [])
                json_names = result.get("json_names", [])
                print(f"✓ 环境重置成功, batch_size={len(images)}, json_names={json_names}")
                return result
            else:
                print(f"✗ 环境重置失败: {result.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            print(f"✗ 重置请求失败: {e}")
            return None
    
    def step(self, actions_batch: List[List[List[float]]]) -> Optional[Dict[str, Any]]:
        """执行动作

        Args:
            actions_batch: 形如 [[action_seq], ...] 的批次动作，每个action_seq是动作列表

        Returns:
            响应字典，包含status, images, done, message
        """
        payload = {
            "actions": actions_batch
        }

        try:
            response = requests.post(
                self.step_url,
                json=payload,
                timeout=300
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") == "success":
                done_list = result.get("done", [])
                print(f"✓ 批次执行成功, batch_size={len(actions_batch)}, done={done_list}")
                return result
            else:
                print(f"✗ 执行动作失败: {result.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            print(f"✗ 执行动作请求失败: {e}")
            return None
    
    def decode_image(self, img_base64: str) -> Image.Image:
        """解码base64图像"""
        img_data = base64.b64decode(img_base64)
        img = Image.open(BytesIO(img_data))
        return img
    
    def save_image(self, img_base64: str, save_path: str) -> bool:
        """保存图像到文件"""
        try:
            img = self.decode_image(img_base64)
            img.save(save_path)
            print(f"✓ 图像已保存: {save_path}")
            return True
        except Exception as e:
            print(f"✗ 保存图像失败: {e}")
            return False


def test_basic_operations(client: EnvServerClient):
    """测试基本操作"""
    print("\n=== 测试基本操作 ===")
    
    # 测试健康检查
    if not client.health_check():
        print("服务器不可用，退出测试")
        return
    
    # 测试reset
    batch_size = 2
    reset_result = client.reset(batch_size=batch_size)
    if reset_result is None:
        print("重置失败，退出测试")
        return

    # instruction = reset_result.get("instructions", "")[0]
    # print(f"instruction: {instruction}")
    # json_file = reset_result.get("json_names", [])[0]
    # print(f"json_file: {json_file}")
    
    # 保存初始图像
    images = reset_result.get("images", [])
    if images:
        for idx, img_list in enumerate(images):
            if not img_list:
                continue
            save_path = f"test_reset_image_{idx}.png"
            client.save_image(img_list[0], save_path)
    

    print("\n执行测试动作...")
    action_seq = [
        [0.0, 0.0, 0.0, np.radians(1.5)],
        [0.0, 0.0, 0.0, np.radians(3.0)], 
        [0.0, 0.0, 0.0, np.radians(4.5)], 
        [0.0, 0.0, 0.0, np.radians(6.0)], 
        [0.0, 0.0, 0.0, np.radians(30)], 
    ]
    actions_batch = [action_seq for _ in range(batch_size)]
    
    step_result = client.step(actions_batch)
    if step_result:
        step_images = step_result.get("images", [])
        done_status = step_result.get("done", [])
        for idx, img_list in enumerate(step_images):
            if not img_list:
                continue
            save_path = f"test_step_image_{idx}.png"
            client.save_image(img_list[0], save_path)
        print(f"Done状态: {done_status}")


def test_done_logic(client: EnvServerClient):
    """测试done逻辑：连续小位移应该触发done"""
    print("\n=== 测试done逻辑 ===")
    
    # 重置环境
    batch_size = 2
    reset_result = client.reset(batch_size=batch_size)
    if reset_result is None:
        print("重置失败，退出测试")
        return

    # instruction = reset_result.get("instructions", "")[0]
    # print(f"instruction: {instruction}")
    # json_file = reset_result.get("json_names", [])[0]
    # print(f"json_file: {json_file}")
    
    # 执行连续的小位移动作（应该触发done）
    print("执行连续小位移动作（应该触发done）...")
    small_actions = []
    for i in range(15):  # 执行15个非常小的动作
        small_actions.append([0.1, 0.1, 0.0, np.radians(0.1)])  # 非常小的位移
    
    # 分批执行，每批2个动作
    done_flags = [False] * batch_size
    for i in range(0, len(small_actions), 2):
        batch = small_actions[i:i+2]
        if not batch:
            break
        # 第一条轨迹使用小动作，第二条轨迹使用较大动作以保持未完成
        large_motion_batch = [[0.0, 0.0, 0.0, np.radians(1.5) * (i+1)] for _ in batch]
        step_result = client.step([batch, large_motion_batch])
        if step_result:
            done_list = step_result.get("done", [])
            current_done = [bool(flag) for flag in done_list]
            print(f"  批次 {i//2 + 1}: done列表={current_done}")
            for idx, flag in enumerate(current_done):
                done_flags[idx] = done_flags[idx] or flag
            if current_done and current_done[0]:
                print("  ✓ Done逻辑触发成功！")
                break
        time.sleep(0.2)
    
    if not done_flags[0]:
        print("  ⚠ 注意：done逻辑未触发（可能需要更多小位移）")
    if done_flags[1]:
        print("  ⚠ 第二条轨迹意外触发done，请检查动作设计")


def main():
    parser = argparse.ArgumentParser(description='测试环境服务器客户端')
    parser.add_argument("-u", "--url", 
                       default="http://127.0.0.1:5010",
                       help='环境服务器URL')
    parser.add_argument("-t", "--test", 
                       choices=["basic", "done", "all"],
                       default="all",
                       help='测试类型')
    parser.add_argument("--start-proxy", action="store_true")
    parser.add_argument("--backend", choices=["rospy", "multi_rospy", "pgsim"], default="rospy")
    parser.add_argument("--proxy-port", type=int, default=None)
    parser.add_argument("--rospy-url", type=str, default=None)
    parser.add_argument("--multi-base-port", type=int, default=None)
    parser.add_argument("--multi-vids", type=str, default=None)
    parser.add_argument("--pgsim-url", type=str, default=None)
    parser.add_argument("--pgsim-uav-ids", type=str, default=None)
    
    args = parser.parse_args()
    
    if args.start_proxy:
        p = urlparse(args.url)
        port = args.proxy_port if args.proxy_port is not None else (p.port if p.port else 5010)
        os.environ["ENV_COMPAT_PORT"] = str(port)
        os.environ["ENV_COMPAT_BACKEND"] = str(args.backend)
        if args.backend == "rospy":
            if args.rospy_url:
                os.environ["ROSPY_BASE_URL"] = str(args.rospy_url)
        elif args.backend == "multi_rospy":
            if args.multi_base_port is not None:
                os.environ["MULTI_ROSPY_BASE_PORT"] = str(int(args.multi_base_port))
            if args.multi_vids:
                os.environ["MULTI_ROSPY_VIDS"] = str(args.multi_vids)
        elif args.backend == "pgsim":
            if args.pgsim_url:
                os.environ["PGSIM_BASE_URL"] = str(args.pgsim_url)
            if args.pgsim_uav_ids:
                os.environ["PGSIM_UAV_IDS"] = str(args.pgsim_uav_ids)
        def _run_proxy():
            import env_compat_proxy as _compat
            _compat.app.run(host='0.0.0.0', port=port, threaded=True, debug=False, use_reloader=False)
        t = threading.Thread(target=_run_proxy, daemon=True)
        t.start()
        client_probe = EnvServerClient(args.url)
        start = time.time()
        while time.time() - start < 5.0:
            if client_probe.health_check():
                break
            time.sleep(0.2)

    client = EnvServerClient(args.url)
    
    if args.test == "basic" or args.test == "all":
        test_basic_operations(client)
    
    if args.test == "done" or args.test == "all":
        test_done_logic(client)


if __name__ == "__main__":
    main()
