# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)

import sys
sys.path.append('./')
import json
import base64
import time
import logging
import argparse
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence
import numpy as np
from PIL import Image
import gym
import gym_unrealcv
import cv2
from flask import Flask, request, jsonify
from gym_unrealcv.envs.wrappers import time_dilation, configUE, augmentation
import random

# 导入轨迹检查函数
from check.check_trajectory import check_trajectory
from pathlib import Path

logger = logging.getLogger(__name__)

# ====== Constants ======
SLEEP_SHORT_S: float = 1.0
SLEEP_AFTER_RESET_S: float = 2.0
ACTION_SMALL_DELTA_POS: float = 3.0
ACTION_SMALL_DELTA_YAW: float = 1.0
ACTION_SMALL_STEPS: int = 10


class EnvServer:
    def __init__(self, cfg: Dict[str, Any]):
        """初始化环境服务器
        
        Args:
            cfg: 配置字典，包含：
                - env_id: 环境ID
                - time_dilation: 时间膨胀参数
                - seed: 随机种子
                - http_port: HTTP服务器端口
        """
        self.cfg = cfg
        self.env_id = cfg.get("env_id", "UnrealTrack-DowntownWest-ContinuousColor-v0")
        self.time_dilation = cfg.get("time_dilation", 10)
        self.seed = cfg.get("seed", 0)
        self.port = cfg.get("http_port", 5008)
        
        # 初始化环境
        logger.info(f"正在初始化环境: {self.env_id}")
        self.env = gym.make(self.env_id)
        if int(self.time_dilation) > 0:
            self.env = time_dilation.TimeDilationWrapper(self.env, int(self.time_dilation))
        self.env.unwrapped.agents_category = ['drone']
        self.env = configUE.ConfigUEWrapper(self.env, resolution=(256, 256))
        self.env = augmentation.RandomPopulationWrapper(self.env, 2, 2, random_target=False)
        self.env.seed(int(self.seed))
        self.env.reset()
        self.env.unwrapped.unrealcv.set_viewport(self.env.unwrapped.player_list[0])
        self.env.unwrapped.unrealcv.set_phy(self.env.unwrapped.player_list[0], 0)
        logger.info(f"环境配置: {self.env.unwrapped.unrealcv.get_camera_config()}")
        
        # 初始化物体
        time.sleep(SLEEP_SHORT_S)
        self.env.unwrapped.unrealcv.new_obj("bp_character_C", "BP_Character_21", [0, 0, 0])
        self.env.unwrapped.unrealcv.set_appearance("BP_Character_21", 0)
        self.env.unwrapped.unrealcv.set_obj_rotation("BP_Character_21", [0, 0, 0])
        time.sleep(SLEEP_SHORT_S)
        self.env.unwrapped.unrealcv.new_obj("BP_BaseCar_C", "BP_Character_22", [1000, 0, 0])
        self.env.unwrapped.unrealcv.set_appearance("BP_Character_22", 2)
        self.env.unwrapped.unrealcv.set_obj_rotation("BP_Character_22", [0, 0, 0])
        self.env.unwrapped.unrealcv.set_phy("BP_Character_22", 0)
        time.sleep(SLEEP_SHORT_S)
        
        logger.info("环境初始化完成")
        
        # 批次状态
        self.batch_states: List[Dict[str, Any]] = []
        self.batch_size: int = 0
        
        # JSON文件名（用于计算reward）
        self.json_name: Optional[str] = None
        self.json_name_list: List[Optional[str]] = []
        # self.json_name = '2025-03-30_11-49-38.json'  #测试使用 成功
        # self.json_name = '2025-03-30_12-30-02.json'  #测试使用 失败 
        
        # 测试用例数据
        default_test_json_dir = Path(__file__).parent / "test_jsons"
        self.test_json_dir = Path(cfg.get("test_json_dir", default_test_json_dir)).resolve()
        if not self.test_json_dir.exists():
            logger.warning(f"指定的test_json目录不存在: {self.test_json_dir}")
            self.test_json_files: List[Path] = []
        else:
            self.test_json_files = sorted(self.test_json_dir.glob("*.json"))
            if not self.test_json_files:
                logger.warning(f"test_json目录中未找到JSON文件: {self.test_json_dir}")
            else:
                random.shuffle(self.test_json_files)
        self._test_json_index: int = 0
        self.current_instruction: Optional[str] = None
        self.current_instruction_list: List[Optional[str]] = []
        
        # 设置Flask应用
        self.app = Flask(__name__)
        self.setup_routes()
    
    def set_cam(self) -> None:
        """根据当前物体姿态设置相机位置"""
        x, y, z = self.env.unwrapped.unrealcv.get_obj_location(self.env.unwrapped.player_list[0])
        roll, yaw, pitch = self.env.unwrapped.unrealcv.get_obj_rotation(self.env.unwrapped.player_list[0])
        cam_loc = [x, y, z]
        cam_rot = [roll, pitch, yaw]
        self.env.unwrapped.unrealcv.set_cam(0, cam_loc, cam_rot)
    
    def create_obj_if_needed(self, obj_info: Optional[Dict[str, Any]]) -> None:
        """根据obj_info创建或放置物体"""
        if obj_info is None:
            return
        use_obj = obj_info.get('use_obj', None)
        obj_id = obj_info.get('obj_id', None)
        obj_pos = obj_info.get('obj_pos', None)
        obj_rot = obj_info.get('obj_rot', None)
        
        if use_obj == 1:
            self.env.unwrapped.unrealcv.set_appearance("BP_Character_21", obj_id)
            self.env.unwrapped.unrealcv.set_obj_location("BP_Character_21", obj_pos)
            self.env.unwrapped.unrealcv.set_obj_rotation("BP_Character_21", obj_rot)
            self.env.unwrapped.unrealcv.set_obj_location("BP_Character_22", [0, 0, -1000])
            self.env.unwrapped.unrealcv.set_obj_location("BP_Character_21", obj_pos)
        elif use_obj == 2:
            self.env.unwrapped.unrealcv.set_appearance("BP_Character_22", 2)
            self.env.unwrapped.unrealcv.set_obj_location("BP_Character_22", [obj_pos[0], obj_pos[1], 0])
            self.env.unwrapped.unrealcv.set_obj_rotation("BP_Character_22", obj_rot)
            self.env.unwrapped.unrealcv.set_phy("BP_Character_22", 0)
            self.env.unwrapped.unrealcv.set_obj_location("BP_Character_21", [0, 0, -1000])
            self.env.unwrapped.unrealcv.set_obj_location("BP_Character_22", [obj_pos[0], obj_pos[1], 0])
        
        if use_obj in [1, 2]:
            logger.debug(f"物体创建完成: {self.env.unwrapped.unrealcv.get_camera_config()}")
            time.sleep(SLEEP_SHORT_S)
    
    def load_next_test_case(self) -> Optional[Dict[str, Any]]:
        """顺序加载下一个测试用例JSON"""
        if not self.test_json_files:
            logger.error("test_json文件列表为空，无法执行reset")
            return None
        
        if self._test_json_index >= len(self.test_json_files):
            self._test_json_index = 0
            if len(self.test_json_files) > 1:
                random.shuffle(self.test_json_files)
        
        json_path = self.test_json_files[self._test_json_index]
        self._test_json_index += 1
        
        try:
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            data["_json_path"] = json_path
            return data
        except Exception as e:
            logger.error(f"加载测试用例失败: {json_path}, 错误: {e}")
            return None
    
    def _prepare_obj_info(self, test_case: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """根据测试用例准备物体信息"""
        use_obj = test_case.get("use_obj")
        if use_obj not in [1, 2]:
            return None
        target_pos = test_case.get("target_pos")
        if not (isinstance(target_pos, Sequence) and len(target_pos) >= 6):
            logger.warning(f"测试用例{test_case.get('_json_path')}缺少有效的target_pos，跳过物体创建")
            return None
        obj_info = {
            "use_obj": use_obj,
            "obj_id": test_case.get("obj_id"),
            "target_pos": list(target_pos[:6]),
            "obj_pos": list(target_pos[:3]),
            "obj_rot": list(target_pos[3:6])
        }
        return obj_info
    
    def _set_state_pose(self, state: Dict[str, Any], sleep_after: bool = False, apply_obj: bool = False) -> None:
        """根据状态设置环境中的无人机姿态"""
        pos = state.get("current_pos")
        yaw = state.get("current_yaw")
        if pos is None:
            pos = state["initial_pos"][0:3]
        if yaw is None:
            yaw = state["initial_yaw"]
        self.env.unwrapped.unrealcv.set_obj_location(
            self.env.unwrapped.player_list[0],
            pos
        )
        self.env.unwrapped.unrealcv.set_rotation(
            self.env.unwrapped.player_list[0],
            yaw - 180
        )
        if apply_obj:
            self.create_obj_if_needed(state.get("obj_info"))
        self.set_cam()
        if sleep_after:
            time.sleep(SLEEP_AFTER_RESET_S)
    
    def _apply_initial_state(self, state: Dict[str, Any]) -> np.ndarray:
        """将环境重置到state的初始姿态并返回当前图像"""
        state["current_pos"] = list(state["initial_pos"][0:3])
        state["current_yaw"] = float(state["initial_yaw"])
        state["last_pose"] = None
        state["small_count"] = 0
        self._set_state_pose(state, sleep_after=True, apply_obj=True)
        return self.env.unwrapped.unrealcv.get_image(0, 'lit')
    
    def transform_to_global(self, x: float, y: float, initial_yaw: float) -> tuple:
        """将相对坐标转换为全局坐标"""
        theta = np.radians(initial_yaw)
        cos_theta = np.cos(theta)
        sin_theta = np.sin(theta)
        global_x = x * cos_theta - y * sin_theta
        global_y = x * sin_theta + y * cos_theta
        return global_x, global_y
    
    def normalize_angle(self, angle: float) -> float:
        """归一化角度到[-180, 180]"""
        angle = angle % 360
        if angle > 180:
            angle -= 360
        return angle

    def calculate_trajectory_reward(self, json_filename: str, project_root: Optional[str] = None) -> int:
        """计算轨迹的reward
        
        根据JSON文件名查找对应的轨迹文件和test_json文件，调用check_trajectory函数进行检查。
        
        Args:
            json_filename: JSON文件名（例如：2025-05-12_21-56-20.json 或 2025-05-12_21-56-20）
            project_root: 项目根目录路径（可选，如果不提供则自动推断）
        
        Returns:
            int: reward值，1表示成功，0表示失败
        
        文件查找路径：
            - 轨迹文件：results/UnrealTrack-DowntownWest-ContinuousColor-v0/qwenoft/{filename}
            - test_json文件：test_jsons/{filename}（包含世界坐标和初始位置）
        """
        try:
            # 如果未提供项目根目录，尝试自动推断
            if project_root is None:
                # 从当前文件位置推断项目根目录
                current_file = Path(__file__)
                project_root = current_file.parent
            else:
                project_root = Path(project_root)
            
            # 确保文件名以.json结尾
            if not json_filename.endswith('.json'):
                json_filename = json_filename + '.json'
            
            # 构建轨迹文件路径：results/UnrealTrack-DowntownWest-ContinuousColor-v0/qwenoft/{filename}
            trajectory_path = project_root / 'results' / 'UnrealTrack-DowntownWest-ContinuousColor-v0' / 'qwenoft' / json_filename
            
            # 检查轨迹文件是否存在
            if not trajectory_path.exists():
                logger.error(f"轨迹文件不存在: {trajectory_path}")
                return 0
            
            # 构建test_json文件路径：test_jsons/{filename}（包含世界坐标和初始位置）
            test_json_path = project_root / 'test_jsons' / json_filename
            
            # 检查test_json文件是否存在
            if not test_json_path.exists():
                logger.error(f"test_json文件不存在: {test_json_path}")
                return 0
            
            logger.info(f"开始计算轨迹reward: {json_filename}")
            logger.debug(f"轨迹文件: {trajectory_path}")
            logger.debug(f"test_json文件: {test_json_path}")
            
            # 调用check_trajectory函数检查轨迹，传入test_json路径
            result = check_trajectory(str(trajectory_path), str(project_root), str(test_json_path))
            
            # check_trajectory返回1表示成功，0表示失败
            # 直接作为reward返回
            reward = result
            
            logger.info(f"轨迹reward计算完成: {json_filename}, reward={reward} ({'成功' if reward == 1 else '失败'})")
            return reward
            
        except Exception as e:
            import traceback
            logger.error(f"计算轨迹reward时发生错误: {e}\n{traceback.format_exc()}")
            # 发生错误时返回0（失败）
            return 0
    
    def setup_routes(self):
        """设置Flask路由"""
        
        @self.app.route('/reset', methods=['POST'])
        def reset():
            """重置环境接口
            
            请求体:
                {
                    "batch_size": int  # 可选，默认1
                }
            
            返回:
                {
                    "status": "success",
                    "images": [[img_base64], ...],
                    "message": "Environment reset successfully",
                    "batch_size": int,
                    "json_names": [...],
                    "instructions": [...]
                }
            """
            try:
                data = request.json or {}
                batch_size = data.get("batch_size", 1)
                if not isinstance(batch_size, int) or batch_size <= 0:
                    return jsonify({
                        "status": "error",
                        "message": "batch_size must be a positive integer."
                    }), 400
                
                test_case = self.load_next_test_case()
                if test_case is None:
                    return jsonify({
                        "status": "error",
                        "message": "No available test case for reset."
                    }), 500
                
                json_path = test_case.get("_json_path")
                json_name = json_path.name if isinstance(json_path, Path) else None
                initial_pos = test_case.get("initial_pos")
                if not (isinstance(initial_pos, (list, tuple)) and len(initial_pos) >= 5):
                    return jsonify({
                        "status": "error",
                        "message": f"Test case {json_name} missing valid initial_pos."
                    }), 500
                
                instruction = test_case.get("instruction")
                self.current_instruction = instruction
                obj_info_template = self._prepare_obj_info(test_case)
                
                self.batch_size = batch_size
                self.batch_states = []
                self.json_name_list = []
                self.current_instruction_list = []
                
                images_batch: List[List[str]] = []
                instructions: List[Optional[str]] = []
                json_names: List[Optional[str]] = []
                
                # 准备首个状态并应用到环境
                first_state_obj_info = None
                if obj_info_template is not None:
                    first_state_obj_info = {
                        "use_obj": obj_info_template["use_obj"],
                        "obj_id": obj_info_template.get("obj_id"),
                        "target_pos": list(obj_info_template["target_pos"]),
                        "obj_pos": list(obj_info_template["obj_pos"]),
                        "obj_rot": list(obj_info_template["obj_rot"])
                    }
                first_state = {
                    "initial_pos": list(initial_pos),
                    "initial_yaw": float(initial_pos[4]),
                    "obj_info": first_state_obj_info,
                    "json_name": json_name,
                    "instruction": instruction,
                    "done": False
                }
                image = self._apply_initial_state(first_state)
                img_pil = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
                img_io = BytesIO()
                img_pil.save(img_io, format='PNG')
                img_base64 = base64.b64encode(img_io.getvalue()).decode('utf-8')
                
                for idx in range(batch_size):
                    if idx == 0:
                        state = first_state
                        image_base64 = img_base64
                    else:
                        obj_info = None
                        if obj_info_template is not None:
                            obj_info = {
                                "use_obj": obj_info_template["use_obj"],
                                "obj_id": obj_info_template.get("obj_id"),
                                "target_pos": list(obj_info_template["target_pos"]),
                                "obj_pos": list(obj_info_template["obj_pos"]),
                                "obj_rot": list(obj_info_template["obj_rot"])
                            }
                        state = {
                            "initial_pos": list(initial_pos),
                            "initial_yaw": float(initial_pos[4]),
                            "obj_info": obj_info,
                            "json_name": json_name,
                            "instruction": instruction,
                            "done": False,
                            "current_pos": list(initial_pos[:3]),
                            "current_yaw": float(initial_pos[4]),
                            "last_pose": None,
                            "small_count": 0
                        }
                        image_base64 = img_base64
                    
                    self.batch_states.append(state)
                    self.json_name_list.append(json_name)
                    self.current_instruction_list.append(instruction)
                    images_batch.append([image_base64])
                    instructions.append(instruction)
                    json_names.append(json_name)
                
                if self.json_name_list:
                    self.json_name = self.json_name_list[-1]
                else:
                    self.json_name = None
                
                logger.info(f"环境重置成功，batch_size={batch_size}")
                response_payload: Dict[str, Any] = {
                    "status": "success",
                    "images": images_batch,
                    "message": "Environment reset successfully",
                    "batch_size": batch_size,
                    "json_names": json_names,
                    "instructions": instructions
                }
                print(images_batch)
                return jsonify(response_payload)
                
            except Exception as e:
                import traceback
                logger.error(f"Reset error: {e}\n{traceback.format_exc()}")
                return jsonify({
                    "status": "error",
                    "message": str(e) + "\n" + traceback.format_exc()
                }), 500
        
        @self.app.route('/step', methods=['POST'])
        def step():
            """执行动作接口
            
            请求体:
                {
                    "actions": [
                        [[x, y, z, yaw], ...],  # 每个batch的action序列, B, t, 4
                        ...
                    ]
                }
            
            返回:
                {
                    "status": "success",
                    "images": [[img_base64], ...],
                    "done": [bool, ...],
                    "message": "Step executed successfully",
                    "batch_size": int
                }
            """
            try:
                data = request.json or {}
                actions_batch = data.get("actions", None)
                
                if actions_batch is None or not isinstance(actions_batch, list) or len(actions_batch) == 0:
                    return jsonify({
                        "status": "error",
                        "message": "actions is required and must be a non-empty list"
                    }), 400
                
                if not self.batch_states:
                    return jsonify({
                        "status": "error",
                        "message": "Environment not reset. Please call /reset first."
                    }), 400
                
                if len(actions_batch) != len(self.batch_states):
                    logger.warning(f"Received actions batch size {len(actions_batch)} "
                                   f"does not match server batch size {len(self.batch_states)}")
                
                images_batch: List[List[str]] = []
                done_batch: List[bool] = []
                
                for batch_idx, state in enumerate(self.batch_states):
                    actions = actions_batch[batch_idx] if batch_idx < len(actions_batch) else []
                    if actions is None:
                        actions = []
                    
                    self._set_state_pose(state)
                    
                    initial_x, initial_y, initial_z = state["initial_pos"][0:3]
                    initial_yaw = state["initial_yaw"]
                    
                    valid_action_count = 0
                    image = None
                    done_flag = state.get("done", False)
                    
                    for i, action_pose in enumerate(actions):
                        if not (isinstance(action_pose, (list, tuple)) and len(action_pose) >= 4):
                            logger.warning(f"Invalid action element at batch {batch_idx}, index {i}: {action_pose}")
                            continue
                        
                        valid_action_count += 1
                        relative_x, relative_y = float(action_pose[0]), float(action_pose[1])
                        relative_z = float(action_pose[2])
                        relative_yaw = float(np.degrees(action_pose[3]))
                        relative_yaw = (relative_yaw + 180) % 360 - 180
                        
                        global_x, global_y = self.transform_to_global(relative_x, relative_y, initial_yaw)
                        absolute_yaw = self.normalize_angle(relative_yaw + initial_yaw)
                        absolute_pos = [
                            global_x + initial_x,
                            global_y + initial_y,
                            relative_z + initial_z,
                            absolute_yaw
                        ]
                        state["current_pos"] = absolute_pos[:3]
                        state["current_yaw"] = absolute_pos[3]
                        
                        self.env.unwrapped.unrealcv.set_obj_location(
                            self.env.unwrapped.player_list[0], 
                            absolute_pos[:3]
                        )
                        self.env.unwrapped.unrealcv.set_rotation(
                            self.env.unwrapped.player_list[0], 
                            absolute_pos[3] - 180
                        )
                        self.set_cam()
                        
                        if i == len(actions) - 1:
                            image = self.env.unwrapped.unrealcv.get_image(0, 'lit')
                        
                        pose_now = [relative_x, relative_y, relative_z, relative_yaw]
                        last_pose = state.get("last_pose")
                        if last_pose is not None:
                            diffs = [abs(a - b) for a, b in zip(pose_now, last_pose)]
                            if all(d < ACTION_SMALL_DELTA_POS for d in diffs[:3]) and diffs[3] < ACTION_SMALL_DELTA_YAW:
                                state["small_count"] = state.get("small_count", 0) + 1
                            else:
                                state["small_count"] = 0
                            if state["small_count"] >= ACTION_SMALL_STEPS:
                                done_flag = True
                                logger.info(f"批次 {batch_idx} 检测到连续 {ACTION_SMALL_STEPS} 步位移变化很小，自动结束任务")
                        state["last_pose"] = pose_now
                        
                        time.sleep(0.1)
                    
                    if valid_action_count == 0:
                        self.set_cam()
                        image = self.env.unwrapped.unrealcv.get_image(0, 'lit')
                    
                    if image is None:
                        self.set_cam()
                        image = self.env.unwrapped.unrealcv.get_image(0, 'lit')
                    
                    img_pil = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
                    img_io = BytesIO()
                    img_pil.save(img_io, format='PNG')
                    img_base64 = base64.b64encode(img_io.getvalue()).decode('utf-8')
                    
                    state["current_pos"] = state.get("current_pos", state["initial_pos"][:3])
                    state["current_yaw"] = state.get("current_yaw", state["initial_yaw"])
                    
                    state["done"] = done_flag
                    images_batch.append([img_base64])
                    done_batch.append(done_flag)
                
                return jsonify({
                    "status": "success",
                    "images": images_batch,
                    "dones": done_batch,
                    "message": "Step executed successfully",
                    "batch_size": len(images_batch)
                })
                
            except Exception as e:
                import traceback
                logger.error(f"Step error: {e}\n{traceback.format_exc()}")
                return jsonify({
                    "status": "error",
                    "message": str(e) + "\n" + traceback.format_exc()
                }), 500
        
        @self.app.route('/health', methods=['GET'])
        def health():
            """健康检查接口"""
            return jsonify({
                "status": "healthy",
                "env_id": self.env_id
            })
        
        @self.app.route('/rewards', methods=['POST'])
        def rewards():
            """计算批次轨迹的rewards接口
            
            请求体:
                {
                    "trajectories": [
                        [[x, y, z, roll, pitch, yaw], ...],  # 第一个轨迹 [N, 6]
                        [[x, y, z, roll, pitch, yaw], ...],  # 第二个轨迹 [N, 6]
                        ...
                    ]  # batch size of trajectory, each traj is [N, 6]
                }
            
            返回:
                {
                    "rewards": [0.0, 1.0, ...]  # batch size of reward, 每个轨迹对应一个reward
                }
            """
            try:
                data = request.json or {}
                trajectories = data.get("trajectories", None)
                
                if trajectories is None or not isinstance(trajectories, list):
                    return jsonify({
                        "status": "error",
                        "message": "trajectories is required and must be a list"
                    }), 400
                
                if len(trajectories) == 0:
                    return jsonify({
                        "status": "error",
                        "message": "trajectories list cannot be empty"
                    }), 400
                
                # 检查json_name_list是否已设置
                if not self.json_name_list or len(self.json_name_list) != len(trajectories):
                    return jsonify({
                        "status": "error",
                        "message": f"json_name_list length ({len(self.json_name_list) if self.json_name_list else 0}) does not match trajectories length ({len(trajectories)})"
                    }), 400
                
                # 自动推断项目根目录
                current_file = Path(__file__)
                project_root = current_file.parent
                
                # 创建临时目录用于保存轨迹文件
                temp_dir = project_root / 'temp_trajectories'
                temp_dir.mkdir(exist_ok=True)
                
                rewards_list: List[float] = []
                
                # 处理每个轨迹
                for idx, trajectory in enumerate(trajectories):
                    if not isinstance(trajectory, list) or len(trajectory) == 0:
                        logger.warning(f"轨迹 {idx} 为空或格式不正确，reward设为0")
                        rewards_list.append(0.0)
                        continue
                    
                    # 获取对应的json_name
                    json_name = self.json_name_list[idx] if idx < len(self.json_name_list) else None
                    if json_name is None:
                        logger.warning(f"轨迹 {idx} 没有对应的json_name，reward设为0")
                        rewards_list.append(0.0)
                        continue
                    
                    # 确保文件名以.json结尾
                    json_filename = json_name
                    if not json_filename.endswith('.json'):
                        json_filename = json_filename + '.json'
                    
                    try:
                        # 将轨迹转换为JSON格式
                        # 轨迹格式: [[x, y, z, roll, pitch, yaw], ...]
                        # 需要转换为: [{"state": [[x, y, z], [roll, pitch, yaw]]}, ...]
                        trajectory_json = []
                        for point in trajectory:
                            if not isinstance(point, (list, tuple)) or len(point) < 6:
                                continue
                            x, y, z = float(point[0]), float(point[1]), float(point[2])
                            roll, pitch, yaw = float(point[3]), float(point[4]), float(point[5])
                            trajectory_json.append({
                                "state": [
                                    [x, y, z],
                                    [roll, pitch, yaw]
                                ]
                            })
                        
                        if len(trajectory_json) == 0:
                            logger.warning(f"轨迹 {idx} 没有有效点，reward设为0")
                            rewards_list.append(0.0)
                            continue
                        
                        # 保存轨迹到临时文件
                        temp_trajectory_path = temp_dir / json_filename
                        with open(temp_trajectory_path, 'w', encoding='utf-8') as f:
                            json.dump(trajectory_json, f, indent=2)
                        
                        # 构建test_json文件路径：test_jsons/{filename}
                        test_json_path = project_root / 'test_jsons' / json_filename
                        
                        # 检查test_json文件是否存在
                        if not test_json_path.exists():
                            logger.error(f"轨迹 {idx} 的test_json文件不存在: {test_json_path}")
                            rewards_list.append(0.0)
                            continue
                        
                        logger.debug(f"计算轨迹 {idx} 的reward: {json_filename}")
                        
                        # 调用check_trajectory函数检查轨迹
                        result = check_trajectory(str(temp_trajectory_path), str(project_root), str(test_json_path))
                        
                        # check_trajectory返回1表示成功，0表示失败
                        # 转换为float类型
                        reward = float(result)
                        rewards_list.append(reward)
                        
                        logger.debug(f"轨迹 {idx} reward计算完成: {json_filename}, reward={reward}")
                        
                    except Exception as e:
                        import traceback
                        logger.error(f"计算轨迹 {idx} 的reward时发生错误: {e}\n{traceback.format_exc()}")
                        rewards_list.append(0.0)
                
                logger.info(f"批次reward计算完成，共 {len(rewards_list)} 个轨迹")
                return jsonify({
                    "rewards": rewards_list
                })
                
            except Exception as e:
                import traceback
                logger.error(f"计算rewards时发生错误: {e}\n{traceback.format_exc()}")
                return jsonify({
                    "status": "error",
                    "message": f"计算rewards时发生错误: {str(e)}"
                }), 500
    
    def run(self):
        """启动HTTP服务器"""
        logger.info(f"启动环境服务器，监听端口: {self.port}")
        self.app.run(host='0.0.0.0', port=self.port, threaded=True)
    
    def close(self):
        """关闭环境"""
        if self.env is not None:
            self.env.close()
            logger.info("环境已关闭")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='UAV环境服务器')
    parser.add_argument("-e", "--env_id", 
                       default="UnrealTrack-DowntownWest-ContinuousColor-v0",
                       help='环境ID')
    parser.add_argument("-t", "--time_dilation", 
                       default=10, type=int,
                       help='时间膨胀参数')
    parser.add_argument("-s", "--seed", 
                       default=0, type=int,
                       help='随机种子')
    parser.add_argument("-p", "--port", 
                       default=5008, type=int,
                       help='HTTP服务器端口')
    parser.add_argument('--log_level', 
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='日志级别')
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
    )
    
    # 配置
    cfg = {
        "env_id": args.env_id,
        "time_dilation": args.time_dilation,
        "seed": args.seed,
        "http_port": args.port
    }
    
    # 创建并运行服务器
    server = EnvServer(cfg)
    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭...")
    finally:
        server.close()


if __name__ == "__main__":
    main()

