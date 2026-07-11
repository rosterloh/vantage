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

/// Wire envelope for control-channel messages. `seq` strictly increases per sender
/// so reordered/duplicated datagrams on the unordered channel can be rejected;
/// `sent_ms` (milliseconds since the sender's arbitrary epoch) lets the robot
/// estimate command age without synchronized clocks and drop commands too old to
/// act on safely.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub seq: u64,
    pub sent_ms: u64,
    pub msg: ControlMsg,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_msg_round_trips_each_variant() {
        for msg in [
            ControlMsg::Move {
                linear: 0.5,
                angular: -0.25,
            },
            ControlMsg::KeepAlive,
        ] {
            let bytes = crate::codec::encode(&msg).unwrap();
            assert_eq!(crate::codec::decode::<ControlMsg>(&bytes).unwrap(), msg);
        }
    }

    #[test]
    fn control_envelope_round_trips() {
        let env = ControlEnvelope {
            seq: 7,
            sent_ms: 1234,
            msg: ControlMsg::Move {
                linear: 0.5,
                angular: -0.25,
            },
        };
        let bytes = crate::codec::encode(&env).unwrap();
        assert_eq!(
            crate::codec::decode::<ControlEnvelope>(&bytes).unwrap(),
            env
        );
    }

    #[test]
    fn bare_control_msg_fails_envelope_decode() {
        let bytes = crate::codec::encode(&ControlMsg::KeepAlive).unwrap();
        assert!(crate::codec::decode::<ControlEnvelope>(&bytes).is_err());
    }
}
