"""Message type definitions for network protocol."""

from enum import Enum
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional


class MessageType(Enum):
    """Message types for communication between Windows and Linux sides."""

    # Simulation → Backend
    STATE_UPDATE = "STATE_UPDATE"
    SENSOR_UPDATE = "SENSOR_UPDATE"
    GRAPHICAL_SENSOR_UPDATE = "GRAPHICAL_SENSOR_UPDATE"

    # Backend → Simulation
    CONTROL_COMMAND = "CONTROL_COMMAND"

    # Bidirectional
    HEARTBEAT = "HEARTBEAT"
    VEHICLE_REGISTER = "VEHICLE_REGISTER"
    VEHICLE_UNREGISTER = "VEHICLE_UNREGISTER"

    # Connection management
    ACK = "ACK"
    ERROR = "ERROR"


@dataclass
class Message:
    """Base message structure for all communication."""

    type: str  # MessageType enum value
    vehicle_id: int
    timestamp: float
    data: Dict[str, Any]
    sequence_number: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message from dictionary."""
        return cls(**data)
