#!/bin/bash

SESSION_NAME="pegasus_session"

# 检查是否存在同名的 tmux 会话，如果存在，先杀掉它
tmux has-session -t $SESSION_NAME 2>/dev/null
if [ $? != 1 ]; then
    echo "Session $SESSION_NAME already exists, killing it..."
    tmux kill-session -t $SESSION_NAME
fi

# 启动新的 tmux 会话（后台运行）
tmux new-session -d -s $SESSION_NAME

# Pane 1: 运行 check.py.py
tmux send-keys -t $SESSION_NAME "python3 '/home/user/PegasusSimulator/examples/check.py'" C-m

# 分割 Pane 2：水平分割，运行 watch 监控 ROS2 主题
tmux split-window -h -t $SESSION_NAME
tmux send-keys -t $SESSION_NAME "watch -n 5 ros2 topic list" C-m

# Pane 3：垂直分割，运行 MAVROS PX4
tmux split-window -v -t $SESSION_NAME:.0
tmux send-keys -t $SESSION_NAME "ros2 launch mavros px4.launch fcu_url:=udp://:14540@" C-m

# Pane 4：垂直分割，延迟 2 秒后启动 velocity_controller
tmux split-window -v -t $SESSION_NAME:.1
tmux send-keys -t $SESSION_NAME "sleep 2 && python3 /home/user/PegasusSimulator/examples/vel.py --ros-args --remap __name:=velocity_controller" C-m

# Pane 5：启动 ISAACSIM_PYTHON 进行执行
tmux split-window -v -t $SESSION_NAME:.2
tmux send-keys -t $SESSION_NAME "sleep 2 && ISAACSIM_PYTHON /home/user/PegasusSimulator/examples/1_px4_single_vehicle.py" C-m

# 调整布局为平铺模式
tmux select-layout -t $SESSION_NAME tiled

# 定义一个清理函数，用于在退出时关闭所有进程
function cleanup {
    tmux kill-session -t $SESSION_NAME
}

# 使用 trap 捕捉 tmux 退出信号并执行清理函数
trap cleanup EXIT

# 附加到会话
tmux attach-session -t $SESSION_NAME