use serde::{Deserialize, Serialize};

use crate::ids::{RobotId, SessionId};

/// What a robot advertises and what clients see in the discovery list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RobotInfo {
    pub id: RobotId,
    pub name: String,
    /// Free-form capability tags, e.g. ["h264", "telemetry"].
    pub capabilities: Vec<String>,
}

/// One ICE server entry handed to peers (STUN has no creds; TURN does).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IceServer {
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
}

/// The SDP/ICE payloads that the coordinator relays verbatim between peers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Signal {
    Offer { sdp: String },
    Answer { sdp: String },
    Ice { candidate: String, sdp_mline_index: u32 },
}

/// robot -> coordinator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RobotMsg {
    Register(RobotInfo),
    Heartbeat,
    /// Signalling aimed at a specific client session.
    Signal { to: SessionId, signal: Signal },
}

/// client -> coordinator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    ListRobots,
    Connect { robot: RobotId },
    /// Signalling aimed at the robot of the client's active session.
    Signal { signal: Signal },
}

/// coordinator -> robot or client
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Sent to a client in reply to ListRobots.
    RobotList { robots: Vec<RobotInfo> },
    /// Sent to a robot when a client opens a session with it.
    ClientConnected { session: SessionId },
    /// Sent to a robot when a client session ends.
    ClientDisconnected { session: SessionId },
    /// Sent to a client once its Connect is accepted.
    Connected { robot: RobotId, session: SessionId },
    /// ICE servers for either peer.
    IceServers { servers: Vec<IceServer> },
    /// Relayed signalling. `from` is the peer session for the robot side.
    Signal { from: Option<SessionId>, signal: Signal },
    /// Coordinator-side error string.
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_round_trips_each_variant() {
        for s in [
            Signal::Offer { sdp: "v=0...".into() },
            Signal::Answer { sdp: "v=0...".into() },
            Signal::Ice { candidate: "candidate:1 1 udp ...".into(), sdp_mline_index: 0 },
        ] {
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(s, serde_json::from_str::<Signal>(&json).unwrap());
        }
    }

    #[test]
    fn robot_register_round_trips() {
        let msg = RobotMsg::Register(RobotInfo {
            id: RobotId("robot-1".into()),
            name: "Atlas".into(),
            capabilities: vec!["h264".into(), "telemetry".into()],
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(msg, serde_json::from_str::<RobotMsg>(&json).unwrap());
    }

    #[test]
    fn ice_server_omits_empty_creds() {
        let stun = IceServer { urls: vec!["stun:stun.l.google.com:19302".into()], username: None, credential: None };
        let json = serde_json::to_string(&stun).unwrap();
        assert!(!json.contains("username"), "STUN entry must not serialize null creds: {json}");
    }
}
