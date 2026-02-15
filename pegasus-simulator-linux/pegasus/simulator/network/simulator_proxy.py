"""Simulator proxy that connects to Windows side and feeds data to backend.

This proxy acts as a bridge between the network communication layer and the backend.
It receives sensor data and state from the Windows side (Isaac Sim) and passes it to
the backend. It also gets control commands from the backend and sends them to Windows.
"""

import socket
import threading
import time
import numpy as np
from typing import Optional
from pegasus.simulator.common import State
from pegasus.simulator.protocol import MessageType
from pegasus.simulator.protocol.constants import (
    DEFAULT_SERVER_PORT,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_RECONNECT_INTERVAL,
    DEFAULT_MAX_RECONNECT_ATTEMPTS,
    MAX_BUFFER_SIZE
)
from .message_handler import MessageHandler


class SimulatorProxy:
    """
    Proxy that connects to Windows side simulation and feeds data to backend.
    """

    def __init__(self, backend, server_host: str, server_port: int = DEFAULT_SERVER_PORT,
                 reconnect_interval: float = DEFAULT_RECONNECT_INTERVAL,
                 max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS):
        """
        Initialize simulator proxy.

        Args:
            backend: Backend instance (PX4MavlinkBackend, ArduPilotMavlinkBackend, ROS2Backend)
            server_host: Windows host IP address
            server_port: Windows host port
            reconnect_interval: Reconnection interval in seconds
            max_reconnect_attempts: Maximum reconnection attempts (0 for infinite)
        """
        self._backend = backend
        self._server_host = server_host
        self._server_port = server_port
        self._reconnect_interval = reconnect_interval
        self._max_reconnect_attempts = max_reconnect_attempts

        # Network components
        self._socket = None
        self._connected = False

        # Message handling
        vehicle_id = getattr(backend.config, 'vehicle_id', 0)
        self._message_handler = MessageHandler(vehicle_id)

        # Threads
        self._receive_thread = None
        self._send_thread = None
        self._heartbeat_thread = None
        self._running = False

        # State
        self._current_state = None
        self._state_lock = threading.Lock()

        # Timing
        self._last_heartbeat_time = 0
        self._dt = 0.01  # Default time step

        print(f"[SimulatorProxy] Initialized for {server_host}:{server_port}")

    def connect(self) -> bool:
        """
        Connect to Windows side simulation.

        Returns:
            True if connected successfully, False otherwise
        """
        attempts = 0

        while self._max_reconnect_attempts == 0 or attempts < self._max_reconnect_attempts:
            try:
                print(f"[SimulatorProxy] Connecting to {self._server_host}:{self._server_port}...")

                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.connect((self._server_host, self._server_port))
                self._connected = True

                print(f"[SimulatorProxy] Connected successfully")
                return True

            except Exception as e:
                attempts += 1
                print(f"[SimulatorProxy] Connection failed (attempt {attempts}): {e}")

                if self._max_reconnect_attempts > 0 and attempts >= self._max_reconnect_attempts:
                    print(f"[SimulatorProxy] Max reconnection attempts reached")
                    return False

                print(f"[SimulatorProxy] Retrying in {self._reconnect_interval} seconds...")
                time.sleep(self._reconnect_interval)

        return False

    def start(self):
        """Start the proxy threads."""
        if not self._connected:
            if not self.connect():
                raise RuntimeError("Failed to connect to simulation")

        self._running = True

        # Start backend
        self._backend.start()

        # Start receive thread
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._receive_thread.start()

        # Start send thread
        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._send_thread.start()

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        print(f"[SimulatorProxy] Started")

    def stop(self):
        """Stop the proxy threads."""
        print(f"[SimulatorProxy] Stopping...")
        self._running = False
        self._connected = False

        # Stop backend
        self._backend.stop()

        # Close socket
        if self._socket:
            try:
                self._socket.close()
            except:
                pass
            self._socket = None

        print(f"[SimulatorProxy] Stopped")

    def run(self):
        """Run the proxy (blocking)."""
        self.start()

        try:
            # Keep running until interrupted
            while self._running:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print(f"[SimulatorProxy] Interrupted by user")
        finally:
            self.stop()

    # Private methods

    def _receive_loop(self):
        """Receive messages from Windows side."""
        buffer = b""

        while self._running and self._connected:
            try:
                # Receive data
                data = self._socket.recv(MAX_BUFFER_SIZE)
                if not data:
                    print("[SimulatorProxy] Connection closed by server")
                    self._connected = False
                    break

                buffer += data

                # Process complete messages
                while True:
                    message, bytes_consumed = self._message_handler.unpack_and_deserialize(buffer)

                    if message is None:
                        break  # Wait for more data

                    # Remove processed bytes from buffer
                    buffer = buffer[bytes_consumed:]

                    # Process message
                    self._process_message(message)

            except Exception as e:
                if self._running:
                    print(f"[SimulatorProxy] Error receiving message: {e}")
                self._connected = False
                break

    def _process_message(self, message: dict):
        """
        Process received message from Windows side.

        Args:
            message: Message dictionary
        """
        msg_type = message.get("type")

        if msg_type == MessageType.STATE_UPDATE.value:
            # Update state
            data = message.get("data", {})
            state = self._create_state_from_data(data)

            with self._state_lock:
                self._current_state = state

            # Pass to backend
            self._backend.update_state(state)

        elif msg_type == MessageType.SENSOR_UPDATE.value:
            # Pass sensor data to backend
            data = message.get("data", {})
            sensor_type = data.get("sensor_type")
            sensor_data = data.get("sensor_data")

            if sensor_type and sensor_data:
                self._backend.update_sensor(sensor_type, sensor_data)

        elif msg_type == MessageType.GRAPHICAL_SENSOR_UPDATE.value:
            # Pass graphical sensor data to backend
            data = message.get("data", {})
            sensor_type = data.get("sensor_type")
            sensor_data = data.get("sensor_data")

            if sensor_type and sensor_data:
                self._backend.update_graphical_sensor(sensor_type, sensor_data)

        elif msg_type == MessageType.HEARTBEAT.value:
            # Heartbeat received, connection is alive
            pass

        else:
            print(f"[SimulatorProxy] Unknown message type: {msg_type}")

    def _send_loop(self):
        """Send control commands to Windows side."""
        while self._running and self._connected:
            try:
                # Update backend
                self._backend.update(self._dt)

                # Get control commands from backend
                rotor_velocities = self._backend.input_reference()

                if rotor_velocities:
                    # Create and send control command message
                    message = self._message_handler.create_control_command(rotor_velocities)
                    packed = self._message_handler.serialize_and_pack(message)
                    self._send_data(packed)

                # Sleep to match simulation rate (100 Hz)
                time.sleep(self._dt)

            except Exception as e:
                if self._running:
                    print(f"[SimulatorProxy] Error in send loop: {e}")

    def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self._running:
            try:
                if self._connected:
                    current_time = time.time()
                    if current_time - self._last_heartbeat_time >= DEFAULT_HEARTBEAT_INTERVAL:
                        message = self._message_handler.create_heartbeat()
                        packed = self._message_handler.serialize_and_pack(message)
                        self._send_data(packed)
                        self._last_heartbeat_time = current_time

                time.sleep(0.1)  # Check every 100ms

            except Exception as e:
                if self._running:
                    print(f"[SimulatorProxy] Error sending heartbeat: {e}")

    def _send_data(self, data: bytes):
        """
        Send data to Windows side.

        Args:
            data: Data to send
        """
        if not self._connected or not self._socket:
            return

        try:
            self._socket.sendall(data)
        except Exception as e:
            print(f"[SimulatorProxy] Error sending data: {e}")
            self._connected = False

    def _create_state_from_data(self, data: dict) -> State:
        """
        Create State object from message data.

        Args:
            data: State data dictionary

        Returns:
            State object
        """
        state = State()

        # Set position
        if "position" in data:
            state.position = np.array(data["position"])

        # Set attitude (quaternion)
        if "attitude" in data:
            state.attitude = np.array(data["attitude"])

        # Set linear velocity
        if "linear_velocity" in data:
            state.linear_velocity = np.array(data["linear_velocity"])

        # Set linear body velocity
        if "linear_body_velocity" in data:
            state.linear_body_velocity = np.array(data["linear_body_velocity"])

        # Set angular velocity
        if "angular_velocity" in data:
            state.angular_velocity = np.array(data["angular_velocity"])

        # Set linear acceleration
        if "linear_acceleration" in data:
            state.linear_acceleration = np.array(data["linear_acceleration"])

        return state
