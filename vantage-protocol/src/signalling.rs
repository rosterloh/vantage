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

/// STUN-only ICE default: the public STUN server, no TURN. Host candidates carry a
/// same-LAN link, and this is the base the coordinator extends with TURN when set.
pub fn default_ice() -> Vec<IceServer> {
    vec![IceServer {
        urls: vec!["stun:stun.l.google.com:19302".into()],
        username: None,
        credential: None,
    }]
}

/// The SDP/ICE payloads that the coordinator relays verbatim between peers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Signal {
    Offer {
        sdp: String,
    },
    Answer {
        sdp: String,
    },
    Ice {
        candidate: String,
        sdp_mline_index: u32,
    },
}

/// robot -> coordinator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RobotMsg {
    Register(RobotInfo),
    Heartbeat,
    /// Signalling aimed at a specific client session.
    Signal {
        to: SessionId,
        signal: Signal,
    },
}

/// client -> coordinator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    ListRobots,
    Connect {
        robot: RobotId,
    },
    /// Signalling aimed at the robot of the client's active session.
    Signal {
        signal: Signal,
    },
    /// How a dial went; sent once per dial, success or failure.
    DialReport(DialReport),
}

/// Furthest checkpoint a client dial reached. Ordered: each implies the ones before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DialStage {
    SignallingConnected,
    RobotSelected,
    OfferReceived,
    AnswerSent,
    /// Data channel open: the dial succeeded.
    Ready,
}

/// One client dial's outcome, for fleet-level connection diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DialReport {
    pub stage: DialStage,
    pub duration_ms: u64,
    /// ICE candidate types (`host`/`srflx`/`prflx`/`relay`) of the selected pair, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_candidate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_candidate: Option<String>,
    /// Why the dial stopped short of `Ready`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When each checkpoint was reached. Absent from older clients.
    #[serde(default)]
    pub timings: DialTimings,
}

/// Milliseconds from dial start to each checkpoint, where it was reached.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DialTimings {
    /// Robot's offer arrived (covers robot selection and the robot creating it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offer_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer_ms: Option<u64>,
    /// ICE connectivity established.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ice_ms: Option<u64>,
    /// DTLS handshake done (peer connection `connected`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtls_ms: Option<u64>,
    /// Data channel open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_ms: Option<u64>,
    /// First decoded video frame (time to first frame).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_frame_ms: Option<u64>,
}

impl DialTimings {
    /// `(checkpoint, ms since the previous reached checkpoint)` in time order.
    /// Video and the data channel both start after DTLS, so either may come first.
    pub fn segments(&self) -> Vec<(&'static str, u64)> {
        let mut marks: Vec<(&'static str, u64)> = [
            ("offer", self.offer_ms),
            ("answer", self.answer_ms),
            ("ice", self.ice_ms),
            ("dtls", self.dtls_ms),
            ("ready", self.ready_ms),
            ("first_frame", self.first_frame_ms),
        ]
        .into_iter()
        .filter_map(|(k, v)| Some((k, v?)))
        .collect();
        marks.sort_by_key(|&(_, ms)| ms);
        let mut prev = 0;
        for (_, ms) in &mut marks {
            (*ms, prev) = (*ms - prev, *ms);
        }
        marks
    }
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
    /// Sent to a client when the robot of its session disconnects. The session is
    /// already closed coordinator-side; the client should end its loop, not linger.
    RobotDisconnected { robot: RobotId },
    /// ICE servers for either peer.
    IceServers { servers: Vec<IceServer> },
    /// Relayed signalling. `from` is the peer session for the robot side.
    Signal {
        from: Option<SessionId>,
        signal: Signal,
    },
    /// Coordinator-side error string.
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_round_trips_each_variant() {
        for s in [
            Signal::Offer {
                sdp: "v=0...".into(),
            },
            Signal::Answer {
                sdp: "v=0...".into(),
            },
            Signal::Ice {
                candidate: "candidate:1 1 udp ...".into(),
                sdp_mline_index: 0,
            },
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
        let stun = IceServer {
            urls: vec!["stun:stun.l.google.com:19302".into()],
            username: None,
            credential: None,
        };
        let json = serde_json::to_string(&stun).unwrap();
        assert!(
            !json.contains("username"),
            "STUN entry must not serialize null creds: {json}"
        );
    }

    #[test]
    fn dial_report_round_trips() {
        let msg = ClientMsg::DialReport(DialReport {
            stage: DialStage::Ready,
            duration_ms: 812,
            local_candidate: Some("host".into()),
            remote_candidate: Some("relay".into()),
            error: None,
            timings: DialTimings {
                offer_ms: Some(300),
                ready_ms: Some(812),
                ..Default::default()
            },
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(msg, serde_json::from_str::<ClientMsg>(&json).unwrap());
        assert!(
            !json.contains("error"),
            "absent error must be omitted: {json}"
        );
        assert!(DialStage::OfferReceived < DialStage::Ready);
        // Reports from clients without timings still decode.
        let old = r#"{"type":"dial_report","stage":"ready","duration_ms":5}"#;
        assert!(serde_json::from_str::<ClientMsg>(old).is_ok());
    }

    #[test]
    fn dial_segments_are_deltas_in_time_order() {
        let t = DialTimings {
            offer_ms: Some(100),
            answer_ms: Some(120),
            ice_ms: Some(300),
            dtls_ms: Some(350),
            ready_ms: Some(400),
            first_frame_ms: Some(380),
        };
        assert_eq!(
            t.segments(),
            [
                ("offer", 100),
                ("answer", 20),
                ("ice", 180),
                ("dtls", 50),
                ("first_frame", 30),
                ("ready", 20),
            ]
        );
        let partial = DialTimings {
            offer_ms: Some(100),
            ice_ms: Some(250),
            ..Default::default()
        };
        assert_eq!(partial.segments(), [("offer", 100), ("ice", 150)]);
    }

    #[test]
    fn robot_disconnected_round_trips() {
        let msg = ServerMsg::RobotDisconnected {
            robot: RobotId("robot-1".into()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(msg, serde_json::from_str::<ServerMsg>(&json).unwrap());
        assert!(
            json.contains("robot_disconnected"),
            "snake_case tag expected: {json}"
        );
    }
}
