# Camera Sharing

## ADDED Requirements

### Requirement: Camera not monopolised by the stream
The streamer SHALL NOT prevent other on-robot processes from receiving camera
frames. Capturing for the WebRTC stream MUST NOT lock the camera device away from
the ROS2 graph.

#### Scenario: ROS2 and the stream run together
- **GIVEN** the robot is streaming video to a client
- **WHEN** a ROS2 node requests camera frames
- **THEN** both the stream and the ROS2 consumer receive frames concurrently

### Requirement: Raw image availability (hard requirement)
The robot SHALL make raw `sensor_msgs/Image` available to the ROS2 graph. This is a
hard requirement; an encoded-only path is not acceptable.

#### Scenario: Raw frames published
- **GIVEN** the camera is active
- **WHEN** the ROS2 raw-image branch is enabled
- **THEN** uncompressed `sensor_msgs/Image` messages are published to ROS2

### Requirement: Camera info published
Because the streamer owns the camera device, the robot SHALL publish `camera_info`
alongside the raw image stream for ROS2 consumers that require it.

#### Scenario: Consumer needs calibration
- **GIVEN** a ROS2 consumer subscribes to the raw image topic
- **WHEN** it requests the matching `camera_info`
- **THEN** `camera_info` is available and corresponds to the published frames

### Requirement: Optional compressed image
The robot MAY additionally provide `sensor_msgs/CompressedImage` (JPEG), but it SHALL NOT
be derived from the H.264 WebRTC stream when provided.

#### Scenario: Compressed requested
- **GIVEN** a ROS2 consumer subscribes to the compressed image topic
- **WHEN** the compressed branch is enabled
- **THEN** JPEG `CompressedImage` messages are published, independent of the H.264 track
