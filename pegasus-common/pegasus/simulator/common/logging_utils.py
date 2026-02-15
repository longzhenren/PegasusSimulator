"""Enhanced logging utilities for Pegasus Simulator network communication."""

import logging
import sys
from pathlib import Path
from datetime import datetime


class NetworkLogger:
    """Centralized logger for network communication."""

    def __init__(self, name: str, log_dir: str = None, log_level: int = logging.INFO):
        """
        Initialize network logger.

        Args:
            name: Logger name
            log_dir: Directory for log files (None for console only)
            log_level: Logging level
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self.logger.handlers.clear()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # File handler (if log_dir specified)
        if log_dir:
            log_path = Path(log_dir)
            log_path.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = log_path / f"{name}_{timestamp}.log"

            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(
                '[%(asctime)s] [%(name)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

            self.logger.info(f"Logging to file: {log_file}")

    def debug(self, msg: str):
        """Log debug message."""
        self.logger.debug(msg)

    def info(self, msg: str):
        """Log info message."""
        self.logger.info(msg)

    def warning(self, msg: str):
        """Log warning message."""
        self.logger.warning(msg)

    def error(self, msg: str):
        """Log error message."""
        self.logger.error(msg)

    def critical(self, msg: str):
        """Log critical message."""
        self.logger.critical(msg)

    def exception(self, msg: str):
        """Log exception with traceback."""
        self.logger.exception(msg)


class ConnectionStats:
    """Track connection statistics."""

    def __init__(self):
        self.messages_sent = 0
        self.messages_received = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        self.connection_attempts = 0
        self.successful_connections = 0
        self.failed_connections = 0
        self.disconnections = 0
        self.reconnections = 0
        self.errors = 0
        self.start_time = datetime.now()
        self.last_message_time = None
        self.last_heartbeat_time = None

    def record_message_sent(self, size: int):
        """Record a sent message."""
        self.messages_sent += 1
        self.bytes_sent += size
        self.last_message_time = datetime.now()

    def record_message_received(self, size: int):
        """Record a received message."""
        self.messages_received += 1
        self.bytes_received += size
        self.last_message_time = datetime.now()

    def record_connection_attempt(self):
        """Record a connection attempt."""
        self.connection_attempts += 1

    def record_successful_connection(self):
        """Record a successful connection."""
        self.successful_connections += 1

    def record_failed_connection(self):
        """Record a failed connection."""
        self.failed_connections += 1

    def record_disconnection(self):
        """Record a disconnection."""
        self.disconnections += 1

    def record_reconnection(self):
        """Record a reconnection."""
        self.reconnections += 1

    def record_error(self):
        """Record an error."""
        self.errors += 1

    def record_heartbeat(self):
        """Record a heartbeat."""
        self.last_heartbeat_time = datetime.now()

    def get_uptime(self) -> float:
        """Get uptime in seconds."""
        return (datetime.now() - self.start_time).total_seconds()

    def get_message_rate(self) -> tuple:
        """Get message rate (sent/s, received/s)."""
        uptime = self.get_uptime()
        if uptime > 0:
            return (self.messages_sent / uptime, self.messages_received / uptime)
        return (0.0, 0.0)

    def get_bandwidth(self) -> tuple:
        """Get bandwidth (bytes/s sent, bytes/s received)."""
        uptime = self.get_uptime()
        if uptime > 0:
            return (self.bytes_sent / uptime, self.bytes_received / uptime)
        return (0.0, 0.0)

    def get_summary(self) -> dict:
        """Get statistics summary."""
        uptime = self.get_uptime()
        msg_rate = self.get_message_rate()
        bandwidth = self.get_bandwidth()

        return {
            'uptime_seconds': uptime,
            'messages_sent': self.messages_sent,
            'messages_received': self.messages_received,
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received,
            'message_rate_sent': msg_rate[0],
            'message_rate_received': msg_rate[1],
            'bandwidth_sent': bandwidth[0],
            'bandwidth_received': bandwidth[1],
            'connection_attempts': self.connection_attempts,
            'successful_connections': self.successful_connections,
            'failed_connections': self.failed_connections,
            'disconnections': self.disconnections,
            'reconnections': self.reconnections,
            'errors': self.errors,
            'last_message_time': self.last_message_time,
            'last_heartbeat_time': self.last_heartbeat_time
        }

    def print_summary(self, logger: NetworkLogger = None):
        """Print statistics summary."""
        summary = self.get_summary()

        lines = [
            "=== Connection Statistics ===",
            f"Uptime: {summary['uptime_seconds']:.1f}s",
            f"Messages: {summary['messages_sent']} sent, {summary['messages_received']} received",
            f"Bytes: {summary['bytes_sent']} sent, {summary['bytes_received']} received",
            f"Message rate: {summary['message_rate_sent']:.2f} sent/s, {summary['message_rate_received']:.2f} received/s",
            f"Bandwidth: {summary['bandwidth_sent']:.2f} B/s sent, {summary['bandwidth_received']:.2f} B/s received",
            f"Connections: {summary['connection_attempts']} attempts, {summary['successful_connections']} successful, {summary['failed_connections']} failed",
            f"Disconnections: {summary['disconnections']}, Reconnections: {summary['reconnections']}",
            f"Errors: {summary['errors']}",
            f"Last message: {summary['last_message_time']}",
            f"Last heartbeat: {summary['last_heartbeat_time']}"
        ]

        msg = '\n'.join(lines)
        if logger:
            logger.info(msg)
        else:
            print(msg)
