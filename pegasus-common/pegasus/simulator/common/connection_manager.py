"""Connection manager with enhanced error handling and reconnection logic."""

import socket
import time
import threading
from typing import Optional, Callable
from enum import Enum


class ConnectionState(Enum):
    """Connection state enumeration."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


class ConnectionManager:
    """Manages network connection with automatic reconnection."""

    def __init__(self,
                 host: str,
                 port: int,
                 reconnect_interval: float = 2.0,
                 max_reconnect_attempts: int = 0,
                 connection_timeout: float = 10.0,
                 on_connected: Optional[Callable] = None,
                 on_disconnected: Optional[Callable] = None,
                 on_error: Optional[Callable] = None):
        """
        Initialize connection manager.

        Args:
            host: Server host
            port: Server port
            reconnect_interval: Interval between reconnection attempts (seconds)
            max_reconnect_attempts: Maximum reconnection attempts (0 for infinite)
            connection_timeout: Connection timeout (seconds)
            on_connected: Callback when connected
            on_disconnected: Callback when disconnected
            on_error: Callback on error
        """
        self.host = host
        self.port = port
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.connection_timeout = connection_timeout

        self._socket: Optional[socket.socket] = None
        self._state = ConnectionState.DISCONNECTED
        self._state_lock = threading.Lock()
        self._reconnect_thread: Optional[threading.Thread] = None
        self._running = False
        self._reconnect_attempts = 0

        # Callbacks
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._on_error = on_error

    @property
    def state(self) -> ConnectionState:
        """Get current connection state."""
        with self._state_lock:
            return self._state

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.state == ConnectionState.CONNECTED

    def connect(self) -> bool:
        """
        Connect to server.

        Returns:
            True if connected successfully, False otherwise
        """
        with self._state_lock:
            if self._state == ConnectionState.CONNECTED:
                return True
            self._state = ConnectionState.CONNECTING

        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.connection_timeout)
            self._socket.connect((self.host, self.port))

            with self._state_lock:
                self._state = ConnectionState.CONNECTED

            self._reconnect_attempts = 0

            if self._on_connected:
                self._on_connected()

            return True

        except socket.timeout:
            self._handle_connection_error(f"Connection timeout to {self.host}:{self.port}")
            return False
        except socket.error as e:
            self._handle_connection_error(f"Connection error to {self.host}:{self.port}: {e}")
            return False
        except Exception as e:
            self._handle_connection_error(f"Unexpected error connecting to {self.host}:{self.port}: {e}")
            return False

    def disconnect(self):
        """Disconnect from server."""
        self._running = False

        with self._state_lock:
            if self._state == ConnectionState.DISCONNECTED:
                return
            self._state = ConnectionState.DISCONNECTED

        if self._socket:
            try:
                self._socket.close()
            except:
                pass
            self._socket = None

        if self._on_disconnected:
            self._on_disconnected()

    def start_auto_reconnect(self):
        """Start automatic reconnection thread."""
        self._running = True
        self._reconnect_thread = threading.Thread(target=self._auto_reconnect_loop, daemon=True)
        self._reconnect_thread.start()

    def stop_auto_reconnect(self):
        """Stop automatic reconnection thread."""
        self._running = False
        if self._reconnect_thread:
            self._reconnect_thread.join(timeout=1.0)

    def send(self, data: bytes) -> bool:
        """
        Send data through connection.

        Args:
            data: Data to send

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.is_connected or not self._socket:
            return False

        try:
            self._socket.sendall(data)
            return True
        except socket.error as e:
            self._handle_send_error(f"Send error: {e}")
            return False
        except Exception as e:
            self._handle_send_error(f"Unexpected send error: {e}")
            return False

    def receive(self, buffer_size: int) -> Optional[bytes]:
        """
        Receive data from connection.

        Args:
            buffer_size: Buffer size

        Returns:
            Received data or None if error
        """
        if not self.is_connected or not self._socket:
            return None

        try:
            data = self._socket.recv(buffer_size)
            if not data:
                self._handle_disconnection("Connection closed by remote")
                return None
            return data
        except socket.timeout:
            return None
        except socket.error as e:
            self._handle_receive_error(f"Receive error: {e}")
            return None
        except Exception as e:
            self._handle_receive_error(f"Unexpected receive error: {e}")
            return None

    def _auto_reconnect_loop(self):
        """Automatic reconnection loop."""
        while self._running:
            if not self.is_connected:
                with self._state_lock:
                    if self._state == ConnectionState.DISCONNECTED:
                        self._state = ConnectionState.RECONNECTING

                if self._should_attempt_reconnect():
                    self._reconnect_attempts += 1
                    if self.connect():
                        continue
                    time.sleep(self.reconnect_interval)
                else:
                    with self._state_lock:
                        self._state = ConnectionState.FAILED
                    break

            time.sleep(0.1)

    def _should_attempt_reconnect(self) -> bool:
        """Check if should attempt reconnection."""
        if self.max_reconnect_attempts == 0:
            return True
        return self._reconnect_attempts < self.max_reconnect_attempts

    def _handle_connection_error(self, error_msg: str):
        """Handle connection error."""
        with self._state_lock:
            self._state = ConnectionState.DISCONNECTED

        if self._socket:
            try:
                self._socket.close()
            except:
                pass
            self._socket = None

        if self._on_error:
            self._on_error(error_msg)

    def _handle_send_error(self, error_msg: str):
        """Handle send error."""
        self._handle_disconnection(error_msg)

    def _handle_receive_error(self, error_msg: str):
        """Handle receive error."""
        self._handle_disconnection(error_msg)

    def _handle_disconnection(self, reason: str):
        """Handle disconnection."""
        with self._state_lock:
            if self._state == ConnectionState.CONNECTED:
                self._state = ConnectionState.DISCONNECTED

        if self._socket:
            try:
                self._socket.close()
            except:
                pass
            self._socket = None

        if self._on_disconnected:
            self._on_disconnected()

        if self._on_error:
            self._on_error(reason)
