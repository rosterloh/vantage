//! Byte tunnels carried over WebRTC data channels. Today the only tunnel is SSH:
//! the operator opens a reliable, ordered channel labelled [`SSH_LABEL`] and the
//! robot bridges it to its own `sshd`. The channel itself carries raw TCP bytes —
//! no framing — so one channel is exactly one TCP connection and a channel close
//! is EOF.

/// Label of an operator-created SSH tunnel data channel.
pub const SSH_LABEL: &str = "ssh";

/// Capability tag a robot advertises in `RobotInfo::capabilities` when it accepts
/// SSH tunnels.
pub const SSH_CAPABILITY: &str = "ssh";
