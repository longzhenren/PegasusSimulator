"""Message serialization using MessagePack."""

import msgpack
import zlib
from typing import Dict, Any, Tuple
from .constants import COMPRESSION_THRESHOLD, COMPRESSION_LEVEL


class MessageSerializer:
    """Handles message serialization and deserialization using MessagePack."""

    @staticmethod
    def serialize(message: Dict[str, Any], compress: bool = None) -> Tuple[bytes, bool]:
        """
        Serialize message to bytes using MessagePack.

        Args:
            message: Message dictionary to serialize
            compress: Force compression on/off. If None, auto-compress based on size

        Returns:
            Tuple of (serialized_data, was_compressed)
        """
        # Pack with MessagePack
        packed = msgpack.packb(message, use_bin_type=True)

        # Determine if compression should be used
        if compress is None:
            compress = len(packed) > COMPRESSION_THRESHOLD

        # Compress if needed
        if compress:
            packed = zlib.compress(packed, level=COMPRESSION_LEVEL)
            return packed, True

        return packed, False

    @staticmethod
    def deserialize(data: bytes, compressed: bool = False) -> Dict[str, Any]:
        """
        Deserialize bytes to message using MessagePack.

        Args:
            data: Serialized message data
            compressed: Whether the data is compressed

        Returns:
            Deserialized message dictionary
        """
        # Decompress if needed
        if compressed:
            data = zlib.decompress(data)

        # Unpack with MessagePack
        return msgpack.unpackb(data, raw=False)

    @staticmethod
    def pack_message(data: bytes, compressed: bool) -> bytes:
        """
        Pack message with length header and compression flag.

        Format: [4 bytes length][1 byte compressed flag][data]

        Args:
            data: Serialized message data
            compressed: Whether the data is compressed

        Returns:
            Packed message with header
        """
        length = len(data)
        compressed_flag = b'\x01' if compressed else b'\x00'
        return length.to_bytes(4, byteorder='big') + compressed_flag + data

    @staticmethod
    def unpack_message(buffer: bytes) -> Tuple[bytes, bool, int]:
        """
        Unpack message from buffer with length header.

        Args:
            buffer: Buffer containing packed message

        Returns:
            Tuple of (message_data, compressed, total_bytes_consumed)
            Returns (None, False, 0) if buffer doesn't contain complete message
        """
        # Need at least 5 bytes for header (4 length + 1 compressed flag)
        if len(buffer) < 5:
            return None, False, 0

        # Read length
        length = int.from_bytes(buffer[:4], byteorder='big')

        # Read compressed flag
        compressed = buffer[4] == 1

        # Check if we have the complete message
        total_size = 5 + length
        if len(buffer) < total_size:
            return None, False, 0

        # Extract message data
        data = buffer[5:total_size]

        return data, compressed, total_size
