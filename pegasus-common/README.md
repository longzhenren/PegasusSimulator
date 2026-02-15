# Pegasus Simulator Common

Common utilities and protocol definitions shared between Windows (IsaacSim) and Linux (PX4/ROS2/MAVLink) sides of Pegasus Simulator.

## Overview

This package provides:
- **State representation**: Vehicle state class with coordinate frame conversions (ENU/NED, FLU/FRD)
- **Network protocol**: Message type definitions for cross-host communication
- **Serialization**: MessagePack-based serialization with automatic compression
- **Constants**: Default network settings and protocol parameters

## Installation

```bash
cd pegasus-common
pip install -e .
```

## Components

### State Class (`pegasus.simulator.common.state`)

Represents vehicle state with support for multiple coordinate frames:
- Position, attitude (quaternion), velocities, accelerations
- Conversion methods between ENU/NED and FLU/FRD frames

### Protocol Messages (`pegasus.simulator.protocol.messages`)

Defines message types for network communication:
- `STATE_UPDATE`: Vehicle state updates
- `SENSOR_UPDATE`: Sensor data (IMU, GPS, Barometer, Magnetometer)
- `GRAPHICAL_SENSOR_UPDATE`: Camera and LiDAR data
- `CONTROL_COMMAND`: Rotor velocity commands
- `HEARTBEAT`: Connection health monitoring
- `VEHICLE_REGISTER/UNREGISTER`: Vehicle lifecycle management

### Serialization (`pegasus.simulator.protocol.serialization`)

MessagePack-based serialization with:
- Automatic compression for large messages (>10KB)
- Message framing with length headers
- Efficient binary encoding

### Constants (`pegasus.simulator.protocol.constants`)

Default configuration values:
- Network ports and timeouts
- Compression thresholds
- Buffer sizes

### Logging Utilities (`pegasus.simulator.common.logging_utils`)

Centralized logging and statistics:
- `NetworkLogger`: Console and file logging with timestamps
- `ConnectionStats`: Track messages, bandwidth, errors, uptime
- Automatic log rotation and formatting

### Connection Manager (`pegasus.simulator.common.connection_manager`)

Enhanced connection management:
- Automatic reconnection with configurable retry logic
- Connection state tracking (DISCONNECTED, CONNECTING, CONNECTED, RECONNECTING, FAILED)
- Error handling with callbacks
- Thread-safe operations

## Usage Example

```python
from pegasus.simulator.common import State
from pegasus.simulator.protocol import MessageType, Message, MessageSerializer

# Create a state update message
message = Message(
    type=MessageType.STATE_UPDATE.value,
    vehicle_id=0,
    timestamp=time.time(),
    data={
        "position": [0.0, 0.0, 1.0],
        "attitude": [0.0, 0.0, 0.0, 1.0],
        # ... other state data
    }
)

# Serialize
serializer = MessageSerializer()
data, compressed = serializer.serialize(message.to_dict())
packed = serializer.pack_message(data, compressed)

# Deserialize
msg_data, compressed, bytes_consumed = serializer.unpack_message(packed)
message_dict = serializer.deserialize(msg_data, compressed)
```

## Dependencies

- numpy: Numerical computations
- msgpack: Efficient binary serialization

## License

BSD 3-Clause License
