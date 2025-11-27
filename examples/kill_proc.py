import psutil

# 定义要查找并终止的进程名称
process_names = [
    "isaac",    # Isaac进程
    "px4",      # PX4进程
    "ros2",     # ROS2进程
    "mavros",   # MAVROS进程
    "maklink"   # Maklink进程
]

def kill_process_by_name(name):
    """根据进程名称查找并逐个确认终止进程"""
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        if name.lower() in proc.info['name'].lower():
            try:
                # 显示进程信息
                print(f"找到进程: {proc.info['name']} (PID: {proc.info['pid']})")
                # 用户确认
                confirm = input(f"是否终止进程 {proc.info['name']} (PID: {proc.info['pid']})? (y/n): ").strip().lower()
                if confirm == 'y':
                    proc.terminate()  # 终止进程
                    try:
                        proc.wait(timeout=3)  # 等待进程结束，最多等待3秒
                        print(f"进程 {proc.info['name']} (PID: {proc.info['pid']}) 已终止")
                    except psutil.TimeoutExpired:
                        print(f"进程 {proc.info['name']} (PID: {proc.info['pid']}) 未在超时内退出，继续...")
                else:
                    print(f"跳过进程 {proc.info['name']} (PID: {proc.info['pid']})")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # 如果进程不存在或者无法访问
                print(f"无法终止进程 {proc.info['name']} (PID: {proc.info['pid']})")
            except KeyboardInterrupt:
                # 处理中断情况，防止程序异常退出
                print("\n操作被中断，退出程序...")
                break

def main():
    """主函数，依次结束所有相关进程"""
    for process_name in process_names:
        input(f"即将检查并终止所有名为 '{process_name}' 的进程。按Enter继续...")
        kill_process_by_name(process_name)
    
    print("所有指定的进程已终止！")

if __name__ == "__main__":
    main()