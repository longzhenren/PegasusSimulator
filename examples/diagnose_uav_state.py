#!/usr/bin/env python3
# Copyright (c) 2024-2026 amurzzb@gmail.com
# Licensed under the MIT License
"""
UAV 状态机诊断工具（diagnose_uav_state.py）

==========================
概述
==========================
一键诊断工具，用于分析 UAV 系统状态：
- 解析最新运行日志，提取关键事件和错误
- 查询运行中程序的状态机状态接口
- 生成综合诊断报告

==========================
使用方法
==========================
# 诊断所有 UAV
python diagnose_uav_state.py

# 诊断指定 UAV
python diagnose_uav_state.py --uav 0

# 仅分析日志
python diagnose_uav_state.py --log-only

# 仅查询实时状态
python diagnose_uav_state.py --live-only

# 指定日志目录
python diagnose_uav_state.py --log-dir ./logs/20240101_120000

# 输出到文件
python diagnose_uav_state.py --output report.txt

==========================
诊断内容
==========================
1. 日志分析：
   - PX4 启动/停止事件
   - Ready to takeoff 状态
   - MAVLink 连接状态
   - 错误和警告统计
   - 恢复操作记录

2. 实时状态查询：
   - PX4 后端状态（via 8081）
   - MAVROS 连接状态（via 5009+id）
   - 飞行状态（armed, mode）

3. 综合诊断：
   - 状态机一致性检查
   - 潜在问题识别
   - 建议操作

==========================
输出格式
==========================
================================================================================
                        UAV 状态机诊断报告
                    生成时间: 2024-01-01 12:00:00
================================================================================

[UAV 0 诊断结果]
--------------------------------------------------------------------------------

1. 实时状态
   - PX4 进程: 运行中 (PID: 12345)
   - MAVLink 连接: 已连接
   - MAVROS 连接: 已连接
   - 飞行模式: OFFBOARD
   - Armed 状态: True
   - Ready to takeoff: True

2. 日志分析摘要
   - 分析日志文件: /tmp/pegasus_px4_logs/px4_0.log
   - 日志时间范围: 12:00:00 - 12:30:00
   - 错误数量: 2
   - 警告数量: 5
   - 恢复操作: 1 次

3. 关键事件时间线
   [12:00:01] PX4 launched, pid=12345
   [12:00:15] Ready to takeoff detected!
   [12:15:30] [WARN] MAVLink heartbeat timeout
   [12:15:35] Starting recovery...
   [12:16:00] Recovery completed successfully

4. 诊断结论
   [OK] 系统状态正常
   或
   [WARN] 发现潜在问题:
   - MAVLink 连接不稳定，建议检查网络
   - PX4 进程已停止，需要恢复

================================================================================
"""

import os
import re
import sys
import json
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict


def ts_log(prefix: str, message: str, level: str = "INFO") -> str:
    """生成带时间戳的日志消息"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_msg = f"[{timestamp}] [{level}] {prefix} {message}"
    print(log_msg)
    return log_msg


@dataclass
class LogEvent:
    """日志事件"""
    timestamp: str
    level: str
    prefix: str
    message: str
    raw_line: str


@dataclass
class UAVDiagnosis:
    """单个 UAV 的诊断结果"""
    uav_id: int

    # 实时状态
    px4_process_alive: bool = False
    px4_process_pid: Optional[int] = None
    px4_ready_to_takeoff: bool = False
    mavlink_connected: bool = False
    received_first_heartbeat: bool = False
    received_first_actuator: bool = False

    mavros_connected: bool = False
    flight_mode: str = ""
    armed: bool = False

    # 日志分析
    log_file: str = ""
    log_time_range: Tuple[str, str] = ("", "")
    error_count: int = 0
    warn_count: int = 0
    recovery_count: int = 0

    # 关键事件
    key_events: List[LogEvent] = field(default_factory=list)
    errors: List[LogEvent] = field(default_factory=list)
    warnings: List[LogEvent] = field(default_factory=list)

    # 诊断结论
    status: str = "UNKNOWN"  # OK, WARN, ERROR
    issues: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)

    # 原始状态数据
    px4_status_raw: Dict[str, Any] = field(default_factory=dict)
    controller_status_raw: Dict[str, Any] = field(default_factory=dict)


class LogAnalyzer:
    """日志分析器"""

    # 日志行格式正则
    LOG_PATTERN = re.compile(
        r"\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{3})\]\s+"
        r"\[(\w+)\]\s+"
        r"(\[[\w\s]+\])\s+"
        r"(.*)"
    )

    # 关键事件正则
    KEY_PATTERNS = {
        "px4_launch": re.compile(r"PX4 launched.*pid=(\d+)", re.IGNORECASE),
        "px4_killed": re.compile(r"PX4 process killed|Killing PX4", re.IGNORECASE),
        "ready_to_takeoff": re.compile(r"Ready\s+(for|to)\s+takeoff", re.IGNORECASE),
        "recovery_start": re.compile(r"Starting (hard |soft )?reboot|Starting recovery|recover_px4", re.IGNORECASE),
        "recovery_complete": re.compile(r"(Hard |Soft )?reboot completed|recovery completed", re.IGNORECASE),
        "mavlink_connected": re.compile(r"MAVLink connection established|received first heartbeat", re.IGNORECASE),
        "mavlink_timeout": re.compile(r"MAVLink.*timeout|heartbeat timeout", re.IGNORECASE),
        "armed": re.compile(r"Vehicle armed|\*+ Vehicle armed \*+", re.IGNORECASE),
        "disarmed": re.compile(r"Vehicle disarmed|disarm", re.IGNORECASE),
        "offboard": re.compile(r"OFFBOARD (enabled|mode)|mode.*OFFBOARD", re.IGNORECASE),
    }

    def __init__(self, log_dir: Optional[str] = None):
        self.log_dir = log_dir or self._get_default_log_dir()

    def _get_default_log_dir(self) -> str:
        """获取默认日志目录"""
        # 优先使用最新的会话目录
        base_log_dir = Path(__file__).parent / "logs"
        if base_log_dir.exists():
            sessions = sorted(base_log_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
            for session in sessions:
                if session.is_dir() and session.name.isdigit():
                    return str(session)

        # 回退到 /tmp
        return "/tmp/pegasus_px4_logs"

    def find_log_files(self, uav_id: Optional[int] = None) -> Dict[int, str]:
        """查找日志文件"""
        result = {}
        log_path = Path(self.log_dir)

        # 搜索多个可能的位置
        search_paths = [
            log_path / "px4",
            log_path / "isaac",
            log_path / "rospy",
            log_path,
            Path("/tmp/pegasus_px4_logs"),
        ]

        for search_path in search_paths:
            if not search_path.exists():
                continue

            for f in search_path.glob("px4_*.log"):
                match = re.match(r"px4_(\d+)(?:_\d+)?\.log", f.name)
                if match:
                    vid = int(match.group(1))
                    if uav_id is None or vid == uav_id:
                        # 选择最新的日志文件
                        if vid not in result or f.stat().st_mtime > Path(result[vid]).stat().st_mtime:
                            result[vid] = str(f)

        return result

    def parse_log_line(self, line: str) -> Optional[LogEvent]:
        """解析单行日志"""
        match = self.LOG_PATTERN.match(line.strip())
        if match:
            return LogEvent(
                timestamp=match.group(1),
                level=match.group(2),
                prefix=match.group(3),
                message=match.group(4),
                raw_line=line.strip()
            )
        return None

    def analyze_log_file(self, log_file: str) -> Tuple[List[LogEvent], List[LogEvent], List[LogEvent], int]:
        """分析日志文件，返回 (关键事件, 错误, 警告, 恢复次数)"""
        key_events = []
        errors = []
        warnings = []
        recovery_count = 0

        if not os.path.exists(log_file):
            return key_events, errors, warnings, recovery_count

        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    event = self.parse_log_line(line)
                    if event is None:
                        # 尝试匹配非标准格式的关键事件
                        for name, pattern in self.KEY_PATTERNS.items():
                            if pattern.search(line):
                                key_events.append(LogEvent(
                                    timestamp="",
                                    level="INFO",
                                    prefix="",
                                    message=line.strip(),
                                    raw_line=line.strip()
                                ))
                                if name == "recovery_start":
                                    recovery_count += 1
                                break
                        continue

                    # 检查日志级别
                    if event.level == "ERROR":
                        errors.append(event)
                    elif event.level in ("WARN", "WARNING"):
                        warnings.append(event)

                    # 检查关键事件
                    for name, pattern in self.KEY_PATTERNS.items():
                        if pattern.search(event.message):
                            key_events.append(event)
                            if name == "recovery_start":
                                recovery_count += 1
                            break
        except Exception as e:
            ts_log("[LogAnalyzer]", f"Failed to analyze log file {log_file}: {e}", "ERROR")

        return key_events, errors, warnings, recovery_count


class LiveStateQuerier:
    """实时状态查询器"""

    def __init__(self, sim_base_url: str = "http://127.0.0.1:8081",
                 controller_base_port: int = 5009):
        self.sim_base_url = sim_base_url
        self.controller_base_port = controller_base_port

    def query_px4_status(self, uav_id: int, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """查询 PX4 后端状态"""
        url = f"{self.sim_base_url}/uav/{uav_id}/px4/status"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("px4_backend", {})
        except urllib.error.URLError as e:
            ts_log("[LiveStateQuerier]", f"Failed to query PX4 status for UAV{uav_id}: {e}", "WARN")
            return None
        except Exception as e:
            ts_log("[LiveStateQuerier]", f"Unexpected error querying PX4 status for UAV{uav_id}: {e}", "ERROR")
            return None

    def query_px4_ready(self, uav_id: int, timeout: float = 5.0) -> Optional[bool]:
        """查询 PX4 ready to takeoff 状态"""
        url = f"{self.sim_base_url}/uav/{uav_id}/px4/ready"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("ready", False)
        except Exception as e:
            ts_log("[LiveStateQuerier]", f"Failed to query PX4 ready for UAV{uav_id}: {e}", "WARN")
            return None

    def query_controller_status(self, uav_id: int, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """查询控制器状态"""
        port = self.controller_base_port + uav_id
        url = f"http://127.0.0.1:{port}/command"
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps({"cmd": "get_status"}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("status", {})
        except Exception as e:
            ts_log("[LiveStateQuerier]", f"Failed to query controller status for UAV{uav_id}: {e}", "WARN")
            return None

    def query_controller_health(self, uav_id: int, timeout: float = 5.0) -> bool:
        """查询控制器健康状态"""
        port = self.controller_base_port + uav_id
        url = f"http://127.0.0.1:{port}/health"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("status") == "healthy"
        except Exception as e:
            ts_log("[LiveStateQuerier]", f"Failed to query controller health for UAV{uav_id}: {e}", "WARN")
            return False


class UAVDiagnosticTool:
    """UAV 诊断工具主类"""

    def __init__(self, log_dir: Optional[str] = None,
                 sim_base_url: str = "http://127.0.0.1:8081",
                 controller_base_port: int = 5009):
        self.log_analyzer = LogAnalyzer(log_dir)
        self.live_querier = LiveStateQuerier(sim_base_url, controller_base_port)

    def diagnose_uav(self, uav_id: int, log_only: bool = False, live_only: bool = False) -> UAVDiagnosis:
        """诊断单个 UAV"""
        diag = UAVDiagnosis(uav_id=uav_id)

        # 查询实时状态
        if not log_only:
            self._query_live_state(diag)

        # 分析日志
        if not live_only:
            self._analyze_logs(diag)

        # 生成诊断结论
        self._generate_conclusion(diag)

        return diag

    def _query_live_state(self, diag: UAVDiagnosis):
        """查询实时状态"""
        uav_id = diag.uav_id

        # 查询 PX4 状态
        px4_status = self.live_querier.query_px4_status(uav_id)
        if px4_status:
            diag.px4_status_raw = px4_status
            diag.px4_process_alive = px4_status.get("px4_process_alive", False)
            diag.px4_process_pid = px4_status.get("px4_process_pid")
            diag.px4_ready_to_takeoff = px4_status.get("px4_ready_to_takeoff", False)
            diag.mavlink_connected = px4_status.get("mavlink_connected", False)
            diag.received_first_heartbeat = px4_status.get("received_first_heartbeat", False)
            diag.received_first_actuator = px4_status.get("received_first_actuator", False)

        # 查询控制器状态
        ctrl_status = self.live_querier.query_controller_status(uav_id)
        if ctrl_status:
            diag.controller_status_raw = ctrl_status
            diag.mavros_connected = ctrl_status.get("connected", False)
            diag.flight_mode = ctrl_status.get("mode", "")
            diag.armed = ctrl_status.get("armed", False)

    def _analyze_logs(self, diag: UAVDiagnosis):
        """分析日志"""
        uav_id = diag.uav_id

        # 查找日志文件
        log_files = self.log_analyzer.find_log_files(uav_id)
        if uav_id not in log_files:
            diag.issues.append(f"未找到 UAV{uav_id} 的日志文件")
            return

        log_file = log_files[uav_id]
        diag.log_file = log_file

        # 分析日志
        key_events, errors, warnings, recovery_count = self.log_analyzer.analyze_log_file(log_file)

        diag.key_events = key_events
        diag.errors = errors
        diag.warnings = warnings
        diag.error_count = len(errors)
        diag.warn_count = len(warnings)
        diag.recovery_count = recovery_count

        # 提取时间范围
        if key_events:
            timestamps = [e.timestamp for e in key_events if e.timestamp]
            if timestamps:
                diag.log_time_range = (min(timestamps), max(timestamps))

    def _generate_conclusion(self, diag: UAVDiagnosis):
        """生成诊断结论"""
        issues = []
        suggestions = []

        # 检查 PX4 状态
        if not diag.px4_process_alive:
            issues.append("PX4 进程未运行")
            suggestions.append("检查仿真端是否启动，或调用 /px4/recover 恢复")
        elif not diag.px4_ready_to_takeoff:
            issues.append("PX4 未达到 Ready to takeoff 状态")
            suggestions.append("检查 PX4 日志是否有初始化错误")

        # 检查 MAVLink 连接
        if not diag.mavlink_connected:
            issues.append("MAVLink 未连接")
            suggestions.append("检查 MAVLink 端口 (4560+id) 是否可用")
        elif not diag.received_first_heartbeat:
            issues.append("未收到 PX4 心跳")
            suggestions.append("PX4 可能处于异常状态，尝试 /px4/recover")

        # 检查 MAVROS 连接
        if not diag.mavros_connected:
            issues.append("MAVROS 未连接")
            suggestions.append("检查 MAVROS 进程是否运行")

        # 检查日志中的错误
        if diag.error_count > 0:
            issues.append(f"日志中发现 {diag.error_count} 个错误")
            suggestions.append("检查日志文件了解详细错误信息")

        # 设置状态
        if not issues:
            diag.status = "OK"
        elif any("进程未运行" in i or "未连接" in i for i in issues):
            diag.status = "ERROR"
        else:
            diag.status = "WARN"

        diag.issues = issues
        diag.suggestions = suggestions

    def diagnose_all(self, log_only: bool = False, live_only: bool = False) -> List[UAVDiagnosis]:
        """诊断所有可发现的 UAV"""
        results = []

        # 从日志文件发现 UAV
        log_files = self.log_analyzer.find_log_files()
        uav_ids = set(log_files.keys())

        # 尝试查询 UAV 0-3（常见配置）
        for i in range(4):
            if self.live_querier.query_controller_health(i):
                uav_ids.add(i)

        for uav_id in sorted(uav_ids):
            diag = self.diagnose_uav(uav_id, log_only=log_only, live_only=live_only)
            results.append(diag)

        return results


class ReportGenerator:
    """报告生成器"""

    def __init__(self, width: int = 80):
        self.width = width

    def generate_report(self, diagnoses: List[UAVDiagnosis]) -> str:
        """生成诊断报告"""
        lines = []

        # 报告头
        lines.append("=" * self.width)
        lines.append(self._center("UAV 状态机诊断报告"))
        lines.append(self._center(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"))
        lines.append("=" * self.width)
        lines.append("")

        if not diagnoses:
            lines.append("[WARN] 未发现任何 UAV")
            lines.append("")
            return "\n".join(lines)

        for diag in diagnoses:
            lines.extend(self._generate_uav_section(diag))
            lines.append("")

        # 报告尾
        lines.append("=" * self.width)

        return "\n".join(lines)

    def _center(self, text: str) -> str:
        """居中文本"""
        return text.center(self.width)

    def _generate_uav_section(self, diag: UAVDiagnosis) -> List[str]:
        """生成单个 UAV 的报告段落"""
        lines = []

        # UAV 标题
        status_icon = {"OK": "[OK]", "WARN": "[WARN]", "ERROR": "[ERROR]"}.get(diag.status, "[?]")
        lines.append(f"[UAV {diag.uav_id} 诊断结果] {status_icon}")
        lines.append("-" * self.width)
        lines.append("")

        # 1. 实时状态
        lines.append("1. 实时状态")
        lines.append(f"   - PX4 进程: {'运行中' if diag.px4_process_alive else '未运行'}"
                    + (f" (PID: {diag.px4_process_pid})" if diag.px4_process_pid else ""))
        lines.append(f"   - MAVLink 连接: {'已连接' if diag.mavlink_connected else '未连接'}")
        lines.append(f"   - 收到首次心跳: {'是' if diag.received_first_heartbeat else '否'}")
        lines.append(f"   - 收到首次 Actuator: {'是' if diag.received_first_actuator else '否'}")
        lines.append(f"   - Ready to takeoff: {'是' if diag.px4_ready_to_takeoff else '否'}")
        lines.append(f"   - MAVROS 连接: {'已连接' if diag.mavros_connected else '未连接'}")
        lines.append(f"   - 飞行模式: {diag.flight_mode or 'N/A'}")
        lines.append(f"   - Armed 状态: {'是' if diag.armed else '否'}")
        lines.append("")

        # 2. 日志分析摘要
        lines.append("2. 日志分析摘要")
        if diag.log_file:
            lines.append(f"   - 日志文件: {diag.log_file}")
            if diag.log_time_range[0]:
                lines.append(f"   - 时间范围: {diag.log_time_range[0]} - {diag.log_time_range[1]}")
            lines.append(f"   - 错误数量: {diag.error_count}")
            lines.append(f"   - 警告数量: {diag.warn_count}")
            lines.append(f"   - 恢复操作: {diag.recovery_count} 次")
        else:
            lines.append("   - (未找到日志文件)")
        lines.append("")

        # 3. 关键事件时间线
        lines.append("3. 关键事件时间线")
        if diag.key_events:
            # 只显示最近 10 个事件
            recent_events = diag.key_events[-10:]
            for event in recent_events:
                ts = event.timestamp if event.timestamp else "N/A"
                msg = event.message[:60] + "..." if len(event.message) > 60 else event.message
                level_tag = f"[{event.level}] " if event.level in ("WARN", "ERROR") else ""
                lines.append(f"   [{ts}] {level_tag}{msg}")
        else:
            lines.append("   (无关键事件)")
        lines.append("")

        # 4. 诊断结论
        lines.append("4. 诊断结论")
        if diag.status == "OK":
            lines.append("   [OK] 系统状态正常")
        else:
            lines.append(f"   [{diag.status}] 发现问题:")
            for issue in diag.issues:
                lines.append(f"      - {issue}")
            if diag.suggestions:
                lines.append("   建议操作:")
                for sug in diag.suggestions:
                    lines.append(f"      - {sug}")

        return lines


def main():
    parser = argparse.ArgumentParser(
        description="UAV 状态机诊断工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                    # 诊断所有 UAV
  %(prog)s --uav 0            # 诊断 UAV 0
  %(prog)s --log-only         # 仅分析日志
  %(prog)s --live-only        # 仅查询实时状态
  %(prog)s --output report.txt  # 输出到文件
        """
    )

    parser.add_argument("--uav", type=int, help="指定 UAV ID")
    parser.add_argument("--log-only", action="store_true", help="仅分析日志，不查询实时状态")
    parser.add_argument("--live-only", action="store_true", help="仅查询实时状态，不分析日志")
    parser.add_argument("--log-dir", type=str, help="指定日志目录")
    parser.add_argument("--sim-url", type=str, default="http://127.0.0.1:8081", help="仿真端 URL")
    parser.add_argument("--controller-port", type=int, default=5009, help="控制器基础端口")
    parser.add_argument("--output", "-o", type=str, help="输出文件路径")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")

    args = parser.parse_args()

    ts_log("[Diagnose]", "Starting UAV diagnostic tool...")

    # 创建诊断工具
    tool = UAVDiagnosticTool(
        log_dir=args.log_dir,
        sim_base_url=args.sim_url,
        controller_base_port=args.controller_port
    )

    # 执行诊断
    if args.uav is not None:
        diagnoses = [tool.diagnose_uav(args.uav, log_only=args.log_only, live_only=args.live_only)]
    else:
        diagnoses = tool.diagnose_all(log_only=args.log_only, live_only=args.live_only)

    # 生成报告
    if args.json:
        # JSON 格式输出
        output = json.dumps([{
            "uav_id": d.uav_id,
            "status": d.status,
            "px4_process_alive": d.px4_process_alive,
            "px4_process_pid": d.px4_process_pid,
            "px4_ready_to_takeoff": d.px4_ready_to_takeoff,
            "mavlink_connected": d.mavlink_connected,
            "received_first_heartbeat": d.received_first_heartbeat,
            "mavros_connected": d.mavros_connected,
            "flight_mode": d.flight_mode,
            "armed": d.armed,
            "log_file": d.log_file,
            "error_count": d.error_count,
            "warn_count": d.warn_count,
            "recovery_count": d.recovery_count,
            "issues": d.issues,
            "suggestions": d.suggestions,
        } for d in diagnoses], indent=2, ensure_ascii=False)
    else:
        # 文本报告
        generator = ReportGenerator()
        output = generator.generate_report(diagnoses)

    # 输出
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        ts_log("[Diagnose]", f"Report saved to: {args.output}")
    else:
        print(output)

    ts_log("[Diagnose]", "Diagnostic complete.")

    # 返回状态码
    has_error = any(d.status == "ERROR" for d in diagnoses)
    has_warn = any(d.status == "WARN" for d in diagnoses)
    if has_error:
        sys.exit(2)
    elif has_warn:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
