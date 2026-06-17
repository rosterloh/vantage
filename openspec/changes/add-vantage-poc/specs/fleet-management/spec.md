# Fleet Management

## ADDED Requirements

### Requirement: Provider count
The coordinator SHALL report how many stream providers (robots) are currently
available.

#### Scenario: Robots come and go
- **GIVEN** robots register and de-register over time
- **WHEN** the provider count is queried
- **THEN** it reflects the number of currently-live robots

### Requirement: Consumer count
The coordinator SHALL report how many consumer clients are currently connected,
derived from session lifecycle.

#### Scenario: Viewers connect and disconnect
- **GIVEN** clients connect to and disconnect from robots
- **WHEN** the consumer count is queried
- **THEN** it reflects the number of currently-active viewer sessions

### Requirement: Session-derived statistics
Fleet statistics SHALL be derived from observed session lifecycle events rather than
self-reported counts, so the numbers stay accurate as connections drop.

#### Scenario: Ungraceful disconnect
- **GIVEN** an active viewer session
- **WHEN** the client disappears without a clean disconnect
- **THEN** the coordinator eventually reconciles the session and the consumer count decreases

### Requirement: TURN credential provisioning
The coordinator SHALL provide ICE server configuration (STUN and TURN) to peers. For
the PoC this MAY be static TURN credentials; ephemeral credentials are deferred.

#### Scenario: Peer obtains ICE servers
- **GIVEN** a peer is about to establish a connection
- **WHEN** it requests ICE configuration
- **THEN** it receives the STUN and TURN server details needed for ICE
