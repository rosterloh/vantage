# Video Streaming

## ADDED Requirements

### Requirement: One-way video over WebRTC
The robot SHALL send a single H.264 video track to a connected client over a WebRTC
peer connection, as a `sendonly` transceiver. There SHALL be no client→robot video.

#### Scenario: Operator views the live camera
- **GIVEN** a client has selected a registered robot
- **WHEN** the WebRTC connection is established
- **THEN** the client receives and renders the robot's live camera stream

### Requirement: Automatic optimal-path selection
The connection SHALL use ICE to select the best available path: a direct LAN path
when peers share a network, a direct path across NAT via STUN otherwise, and a TURN
relay only when no direct path works.

#### Scenario: Same-network peers connect directly
- **GIVEN** robot and client are on the same network
- **WHEN** ICE completes
- **THEN** media flows over the direct host path without using the relay

#### Scenario: Blocked path falls back to relay
- **GIVEN** no direct path between robot and client is reachable
- **WHEN** ICE completes
- **THEN** media flows via the TURN relay

### Requirement: Encode once, fan out to multiple consumers
When multiple clients view the same robot, the robot SHALL encode the video once and
fan the encoded stream out to each consumer rather than re-encoding per viewer.

#### Scenario: Second viewer joins
- **GIVEN** one client is already viewing a robot
- **WHEN** a second client connects to the same robot
- **THEN** both receive video and the robot's encoder count does not increase

### Requirement: Immediate startup for new viewers
A newly connected viewer SHALL begin decoding without waiting for the next periodic
keyframe.

#### Scenario: Mid-stream join
- **GIVEN** a robot is already streaming to one viewer
- **WHEN** a new viewer joins
- **THEN** a keyframe is produced so the new viewer renders video promptly

### Requirement: Demand-driven streaming
The robot SHALL add streaming work when a consumer connects and remove it when the
consumer disconnects.

#### Scenario: Last viewer leaves
- **GIVEN** a robot is streaming to a single viewer
- **WHEN** that viewer disconnects
- **THEN** the robot tears down that consumer's sink branch

### Requirement: Adaptive bitrate
The robot SHALL adapt encoded bitrate to available bandwidth using WebRTC congestion
control feedback.

#### Scenario: Bandwidth drops on a relayed link
- **GIVEN** a client is viewing over a constrained link
- **WHEN** available bandwidth falls
- **THEN** the robot reduces bitrate to maintain a live, low-latency stream
