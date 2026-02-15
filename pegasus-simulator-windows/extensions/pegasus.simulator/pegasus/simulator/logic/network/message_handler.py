"""Message handling utilities for network communication."""

import time
from typing import Dict, Any, Optional
from pegasus.simulator.protocol import MessageType, Message, MessageSerializer
from pegasus.simulator.common import State


class MessageHandler:
    """Handles message creation and processing for network communication."""

    def __init__(self, vehicle_id: int):
        """
        Initialize message handler.

        Args:
            vehicle_id: ID of the vehicle this handler is for
        """
        self._vehicle_id = vehicle_id
        self._serializer = MessageSerializer()
        self._sequence_number = 0

    def create_state_update(self, state: State) -> Dict[str, Any]:
        """
        Create a STATE_UPDATE message from vehicle state.

        Args:
            state: Vehicle state object

        Returns:
            Message dictionary
        """
        return {
            "type": MessageType.STATE_UPDATE.value,
            "vehicle_id": self._vehicle_id,
            "timestamp": time.time(),
            "sequence_number": self._get_sequence_number(),
            "data": {
                "position": state.position.tolist(),
                "attitude": state.attitude.tolist(),
                "linear_velocity": state.linear_velocity.tolist(),
                "linear_body_velocity": state.linear_body_velocity.tolist(),
                "angular_velocity": state.angular_velocity.tolist(),
                "linear_acceleration": state.linear_acceleration.tolist()
            }
        }

    def create_sensor_update(self, sensor_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a SENSOR_UPDATE message.

        Args:
            sensor_type: Type of sensor (IMU, GPS, Barometer, Magnetometer)
            data: Sensor data dictionary

        Returns:
            Message dictionary
        """
        return {
            "type": MessageType.SENSOR_UPDATE.value,
            "vehicle_id": self._vehicle_id,
            "timestamp": time.time(),
            "sequence_number": self._get_sequence_number(),
            "data": {
                "sensor_type": sensor_type,
                "sensor_data": data
            }
        }

    def create_graphical_sensor_update(self, sensor_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a GRAPHICAL_SENSOR_UPDATE message.

        Args:
            sensor_type: Type of sensor (MonocularCamera, Lidar)
            data: Sensor data dictionary

        Returns:
            Message dictionary
        """
        return {
            "type": MessageType.GRAPHICAL_SENSOR_UPDATE.value,
            "vehicle_id": self._vehicle_id,
            "timestamp": time.time(),
            "sequence_number": self._get_sequence_number(),
            "data": {
                "sensor_type": sensor_type,
                "sensor_data": data
            }
        }

    def create_heartbeat(self) -> Dict[str, Any]:
        """
        Create a HEARTBEAT message.

        Returns:
            Message dictionary
        """
        return {
            "type": MessageType.HEARTBEAT.value,
            "vehicle_id": self._vehicle_id,
            "timestamp": time.time(),
            "data": {}
        }

    def serialize_and_pack(self, message: Dict[str, Any], compress: Optional[bool] = None) -> bytes:
        """
        Serialize and pack a message for transmission.

        Args:
            message: Message dictionary
            compress: Force compression on/off, None for auto

        Returns:
            Packed message bytes ready for transmission
        """
        data, compressed = self._serializer.serialize(message, compress=compress)
        return self._serializer.pack_message(data, compressed)

    def unpack_and_deserialize(self, buffer: bytes) -> tuple:
        """
        Unpack and deserialize a message from buffer.

        Args:
            buffer: Buffer containing packed message

        Returns:
            Tuple of (message_dict, bytes_consumed) or (None, 0) if incomplete
        """
        data, compressed, bytes_consumed = self._serializer.unpack_message(buffer)

        if data is None:
            return None, 0

        message = self._serializer.deserialize(data, compressed)
        return message, bytes_consumed

    def _get_sequence_number(self) -> int:
        """Get next sequence number."""
        seq = self._sequence_number
        self._sequence_number += 1
        return seq
