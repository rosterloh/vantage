use serde::{Deserialize, Serialize};

/// Label of the operator→robot control data channel, distinct from `telemetry`.
pub const CONTROL_LABEL: &str = "control";

/// Teleop command sent operator→robot over the control data channel.
///
/// Sent unreliable/unordered (latest-command-wins); the robot's disconnect
/// watchdog — not retransmission — provides the safety guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlMsg {
    /// Normalized teleop command; `linear` and `angular` each in [-1.0, 1.0].
    Move { linear: f32, angular: f32 },
    /// Liveness beat so the watchdog does not trip during an idle hold.
    KeepAlive,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_msg_round_trips_each_variant() {
        for msg in [ControlMsg::Move { linear: 0.5, angular: -0.25 }, ControlMsg::KeepAlive] {
            let bytes = crate::codec::encode(&msg).unwrap();
            assert_eq!(crate::codec::decode::<ControlMsg>(&bytes).unwrap(), msg);
        }
    }
}
