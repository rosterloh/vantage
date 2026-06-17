# Telemetry

## ADDED Requirements

### Requirement: Device telemetry over the data channel
The robot SHALL send device telemetry (CPU usage, memory usage, temperatures, and
similar host metrics) to a connected client over a WebRTC data channel, displayed
beside the video.

#### Scenario: Telemetry shown next to the stream
- **GIVEN** a client is connected to a robot
- **WHEN** the robot samples its device metrics
- **THEN** the client receives them and displays them alongside the live video

### Requirement: Bidirectional connection from day one
The peer connection SHALL establish bidirectional data channels at connection time
so that a future operator→robot control channel does not require renegotiation.

#### Scenario: Connection is bidirectional-ready
- **GIVEN** a client connects to a robot
- **WHEN** the data channels are established
- **THEN** a return (operator→robot) channel exists and is reserved for control

### Requirement: Channel reliability matched to data type
Telemetry SHALL use a data-channel reliability mode appropriate to its type:
reliable-ordered for discrete events, and an unreliable/unordered mode for any
high-rate continuous stream to avoid head-of-line blocking.

#### Scenario: High-rate stream does not block
- **GIVEN** a high-rate continuous telemetry stream
- **WHEN** a packet is lost
- **THEN** subsequent samples are not blocked waiting for retransmission

### Requirement: Shared message types
Telemetry and control message types SHALL be defined once in `vantage-protocol` and
used unchanged by robot, coordinator, and client.

#### Scenario: No schema drift
- **GIVEN** a telemetry type defined in `vantage-protocol`
- **WHEN** the robot serializes it and the client deserializes it
- **THEN** both use the identical type with no separate redefinition
