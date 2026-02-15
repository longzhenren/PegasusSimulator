"""Network backend for cross-host simulation.

This backend sends sensor data and state over network to a remote backend,
and receives control commands from the remote backend.
"""

import socket
import threading
import time
from typing import List, Optional
from pegasus.simulator.common import State
from pegasus.simulator.protocol import MessageType
from pegasus.simulator.protocol.constants import (
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_CONNECTION_TIMEOUT,
    MAX_BUFFER_SIZE
)
from .message_handler import MessageHandler


class NetworkBackendConfig:
    """Configuration for NetworkBackend."""

    def __init__(self, config={}):
        """
        Initialize network backend configuration.

        Args:
            config: Configuration dictionary with the following keys:
                - vehicle_id: Vehicle ID (default: 0)
                - server_host: Server host to bind to (default: "0.0.0.0")
                - server_port: Server port to bind to (default: 5555)
                - num_rotors: Number of rotors (default: 4)
                - enable_graphical_sensors: Enable graphical sensor transmission (default: False)
                - heartbeat_interval: Heartbeat interval in seconds (default: 1.0)
                - connection_timeout: Connection timeout in seconds (default: 5.0)
        """
        self.vehicle_id = config.get("vehicle_id", 0)
        self.server_host = config.get("server_host", DEFAULT_SERVER_HOST)
        self.server_port = config.get("server_port", DEFAULT_SERVER_PORT)
        self.num_rotors = config.get("num_rotors", 4)
        self.enable_graphical_sensors = config.get("enable_graphical_sensors", False)
        self.heartbeat_interval = config.get("heartbeat_interval", DEFAULT_HEARTBEAT_INTERVAL)
        self.connection_timeout = config.get("connection_timeout", DEFAULT_CONNECTION_TIMEOUT)


class NetworkBackend:
    """
    Backend that sends sensor data and state over network to a remote backend.
    Receives control commands from the remote backend.
    """

    def __init__(self, config: NetworkBackendConfig):
        """
        Initialize network backend.

        Args:
            config: Network backend configuration
        """
        self.config = config
        self._vehicle = None

        # Network components
        self._server = None
        self._client_socket = None
        self._client_address = None
        self._connected = False

        # Control data
        self._input_reference = [0.0] * self.config.num_rotors
        self._input_lock = threading.Lock()

        # Message handling
        self._message_handler = MessageHandler(self.config.vehicle_id)

        # Threads
        self._server_thread = None
        self._receive_thread = None
        self._heartbeat_thread = None
        self._running = False

        # Timing
        self._last_heartbeat_time = 0
        self._last_received_time = 0

        print(f"[NetworkBackend] Initialized for vehicle {self.config.vehicle_id}")

    def initialize(self, vehicle):
        """
        Initialize the backend with vehicle reference.

        Args:
            vehicle: Vehicle object
        """
        self._vehicle = vehicle
        print(f"[NetworkBackend] Initialized with vehicle")

    @property
    def vehicle(self):
        """Get the vehicle associated with this backend."""
        return self._vehicle

    def start(self):
        """Start the network server and threads."""
        self._running = True

        try:
            # Start TCP server
            self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server.bind((self.config.server_host, self.config.server_port))
            self._server.listen(1)

            # Start server thread to accept connections
            self._server_thread = threading.Thread(target=self._accept_connections, daemon=True)
            self._server_thread.start()

            # Start heartbeat thread
            self._heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
            self._heartbeat_thread.start()

            print(f"[NetworkBackend] Server started on {self.config.server_host}:{self.config.server_port}")

        except Exception as e:
            print(f"[NetworkBackend] Error starting server: {e}")
            self._running = False

    def stop(self):
        """Stop the network server and threads."""
        print(f"[NetworkBackend] Stopping...")
        self._running = False
        self._connected = False

        # Close client socket
        if self._client_socket:
            try:
                self._client_socket.close()
            except:
                pass
            self._client_socket = None

        # Close server socket
        if self._server:
            try:
                self._server.close()
            except:
                pass
            self._server = None

        print(f"[NetworkBackend] Stopped")

    def reset(self):
        """Reset the backend."""
        with self._input_lock:
            self._input_reference = [0.0] * self.config.num_rotors
        print(f"[NetworkBackend] Reset")

    def update_sensor(self, sensor_type: str, data):
        """
        Send sensor data over network.

        Args:
            sensor_type: Type of sensor (IMU, GPS, Barometer, Magnetometer)
            data: Sensor data dictionary
        """
        if not self._connected:
            return

        try:
            message = self._message_handler.create_sensor_update(sensor_type, data)
            packed = self._message_handler.serialize_and_pack(message)
            self._send_data(packed)
        except Exception as e:
            print(f"[NetworkBackend] Error sending sensor data: {e}")

    def update_graphical_sensor(self, sensor_type: str, data):
        """
        Send graphical sensor data over network.

        Args:
            sensor_type: Type of sensor (MonocularCamera, Lidar)
            data: Sensor data dictionary
        """
        if not self._connected or not self.config.enable_graphical_sensors:
            return

        try:
            message = self._message_handler.create_graphical_sensor_update(sensor_type, data)
            # Force compression for large graphical data
            packed = self._message_handler.serialize_and_pack(message, compress=True)
            self._send_data(packed)
        except Exception as e:
            print(f"[NetworkBackend] Error sending graphical sensor data: {e}")

    def update_state(self, state: State):
        """
        Send vehicle state over network.

        Args:
            state: Vehicle state object
        """
        if not self._connected:
            return

        try:
            message = self._message_handler.create_state_update(state)
            packed = self._message_handler.serialize_and_pack(message)
            self._send_data(packed)
        except Exception as e:
            print(f"[NetworkBackend] Error sending state: {e}")

    def input_reference(self) -> List[float]:
        """
        Return the latest control commands received from network.

        Returns:
            List of rotor angular velocities (rad/s)
        """
        with self._input_lock:
            return self._input_reference.copy()

    def update(self, dt: float):
        """
        Called at every physics step.

        Args:
            dt: Time step in seconds
        """
        # Check connection timeout
        if self._connected:
            if time.time() - self._last_received_time > self.config.connection_timeout:
                print("[NetworkBackend] Connection timeout, disconnecting")
                self._connected = False
                with self._input_lock:
                    self._input_reference = [0.0] * self.config.num_rotors

    # Private methods

    def _accept_connections(self):
        """Accept incoming connections from backends."""
        while self._running:
            try:
                self._server.settimeout(1.0)  # Allow checking _running flag
                try:
                    client_socket, client_address = self._server.accept()
                except socket.timeout:
                    continue

                print(f"[NetworkBackend] Client connected from {client_address}")

                # Close previous connection if exists
                if self._client_socket:
                    try:
                        self._client_socket.close()
                    except:
                        pass

                self._client_socket = client_socket
                self._client_address = client_address
                self._connected = True
                self._last_received_time = time.time()

                # Start receive thread
                if self._receive_thread and self._receive_thread.is_alive():
                    # Wait for previous thread to finish
                    pass

                self._receive_thread = threading.Thread(target=self._receive_messages, daemon=True)
                self._receive_thread.start()

            except Exception as e:
                if self._running:
                    print(f"[NetworkBackend] Error accepting connection: {e}")
                    time.sleep(1.0)

    def _receive_messages(self):
        """Receive messages from the backend."""
        buffer = b""

        while self._running and self._connected:
            try:
                # Receive data
                data = self._client_socket.recv(MAX_BUFFER_SIZE)
                if not data:
                    print("[NetworkBackend] Client disconnected")
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
                    self._last_received_time = time.time()

            except Exception as e:
                if self._running:
                    print(f"[NetworkBackend] Error receiving message: {e}")
                self._connected = False
                break

    def _process_message(self, message):
        """
        Process received message.

        Args:
            message: Message dictionary
        """
        msg_type = message.get("type")

        if msg_type == MessageType.CONTROL_COMMAND.value:
            # Update control commands
            data = message.get("data", {})
            rotor_velocities = data.get("rotor_velocities", [])

            if len(rotor_velocities) == self.config.num_rotors:
                with self._input_lock:
                    self._input_reference = rotor_velocities
            else:
                print(f"[NetworkBackend] Invalid rotor velocities length: {len(rotor_velocities)}")

        elif msg_type == MessageType.HEARTBEAT.value:
            # Heartbeat received, connection is alive
            pass

        else:
            print(f"[NetworkBackend] Unknown message type: {msg_type}")

    def _send_data(self, data: bytes):
        """
        Send data to client.

        Args:
            data: Data to send
        """
        if not self._connected or not self._client_socket:
            return

        try:
            self._client_socket.sendall(data)
        except Exception as e:
            print(f"[NetworkBackend] Error sending data: {e}")
            self._connected = False

    def _send_heartbeats(self):
        """Send periodic heartbeats."""
        while self._running:
            try:
                if self._connected:
                    current_time = time.time()
                    if current_time - self._last_heartbeat_time >= self.config.heartbeat_interval:
                        message = self._message_handler.create_heartbeat()
                        packed = self._message_handler.serialize_and_pack(message)
                        self._send_data(packed)
                        self._last_heartbeat_time = current_time

                time.sleep(0.1)  # Check every 100ms

            except Exception as e:
                if self._running:
                    print(f"[NetworkBackend] Error sending heartbeat: {e}")
