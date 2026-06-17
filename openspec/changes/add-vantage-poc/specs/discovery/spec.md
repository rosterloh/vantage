# Discovery

## ADDED Requirements

### Requirement: Robot registration and liveness
A robot SHALL register with the coordinator on startup and maintain a heartbeat so
the coordinator can present an accurate list of available stream providers.

#### Scenario: Robot becomes discoverable
- **GIVEN** the coordinator is running
- **WHEN** a robot starts and registers with an identity and capabilities
- **THEN** the robot appears in the coordinator's provider list

#### Scenario: Stale robot expires
- **GIVEN** a registered robot has stopped sending heartbeats
- **WHEN** the heartbeat timeout elapses
- **THEN** the coordinator removes the robot from the provider list

### Requirement: Client discovery
A client SHALL be able to query the coordinator for the current list of available
robots and present it for selection.

#### Scenario: Operator lists robots
- **GIVEN** one or more robots are registered
- **WHEN** a client requests the provider list
- **THEN** it receives each robot's identity and connection metadata

### Requirement: LAN-local fast path
The system SHALL support mDNS-based discovery on a local network as an optional fast
path and offline fallback when the coordinator is unreachable.

#### Scenario: Coordinator unreachable on a LAN
- **GIVEN** a robot and a client are on the same LAN and the coordinator is unreachable
- **WHEN** the client scans via mDNS
- **THEN** it discovers the robot and can proceed to connect
