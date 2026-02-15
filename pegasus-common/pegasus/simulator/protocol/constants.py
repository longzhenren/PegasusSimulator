"""Protocol constants for network communication."""

# Default network settings
DEFAULT_SERVER_HOST = "0.0.0.0"
DEFAULT_SERVER_PORT = 5555
DEFAULT_HEARTBEAT_INTERVAL = 1.0  # seconds
DEFAULT_CONNECTION_TIMEOUT = 5.0  # seconds
DEFAULT_RECONNECT_INTERVAL = 2.0  # seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 10

# Message size limits
MAX_MESSAGE_SIZE = 100 * 1024 * 1024  # 100 MB
MAX_BUFFER_SIZE = 4096  # 4 KB for receiving

# Compression settings
COMPRESSION_THRESHOLD = 10 * 1024  # 10 KB - compress messages larger than this
COMPRESSION_LEVEL = 6  # zlib compression level (1-9)
