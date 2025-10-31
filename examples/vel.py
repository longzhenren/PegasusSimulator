#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from mavros_msgs.msg import PositionTarget

class VelocityController(Node):
    def __init__(self):
        # Initialize the node with a name
        super().__init__("velocity_controller")

        # Initialize last velocity command (default is zero velocity)
        self.last_velocity = PositionTarget()
        self.last_velocity.coordinate_frame = PositionTarget.FRAME_LOCAL_NED

        # Publisher for MAVROS velocity commands
        self.pub = self.create_publisher(PositionTarget, "/mavros/setpoint_raw/local", 10)

        # Subscriber for velocity commands
        self.create_subscription(PositionTarget, "/nav/velocity", self.cmd_vel_callback, 10)

        # Publish at a fixed rate (e.g., 10 Hz)
        self.timer = self.create_timer(1 / 30, self.publish_velocity)  # 30 Hz rate

        self.get_logger().info("Velocity Controller Node Started")

    def cmd_vel_callback(self, msg):
        """Update the last velocity command when receiving a new one."""
        self.last_velocity = msg

    def publish_velocity(self):
        """Publish the last velocity command."""
        self.pub.publish(self.last_velocity)


def main(args=None):
    rclpy.init(args=args)

    velocity_controller = VelocityController()

    try:
        rclpy.spin(velocity_controller)
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up before shutting down the node
        velocity_controller.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()