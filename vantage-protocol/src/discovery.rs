//! Shared mDNS/DNS-SD constants and TXT-record schema for the LAN fast path.
//! Defined once here so the robot's advertisement and the client's browse agree.

use std::collections::HashMap;

use crate::signalling::RobotInfo;

/// DNS-SD service type advertised by robots on the LAN.
pub const MDNS_SERVICE_TYPE: &str = "_vantage._tcp.local.";

/// TXT record keys.
pub const TXT_ID: &str = "id";
pub const TXT_NAME: &str = "name";

/// Build the TXT properties a robot advertises (its identity; the direct
/// signalling port travels in the SRV record, not TXT).
pub fn advert_txt(info: &RobotInfo) -> HashMap<String, String> {
    HashMap::from([
        (TXT_ID.to_string(), info.id.0.clone()),
        (TXT_NAME.to_string(), info.name.clone()),
    ])
}

/// Recover a robot's identity from resolved TXT properties. Returns None if the
/// required keys are absent (a foreign or malformed service).
pub fn robot_from_txt(props: &HashMap<String, String>) -> Option<(String, String)> {
    let id = props.get(TXT_ID)?.clone();
    let name = props.get(TXT_NAME).cloned().unwrap_or_else(|| id.clone());
    Some((id, name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RobotId;

    #[test]
    fn txt_round_trips_robot_identity() {
        let info = RobotInfo {
            id: RobotId("robot-1".into()),
            name: "Atlas".into(),
            capabilities: vec!["telemetry".into()],
        };
        let txt = advert_txt(&info);
        let (id, name) = robot_from_txt(&txt).expect("required keys present");
        assert_eq!(id, "robot-1");
        assert_eq!(name, "Atlas");
    }

    #[test]
    fn foreign_service_without_id_is_rejected() {
        let props = HashMap::from([("other".to_string(), "x".to_string())]);
        assert!(robot_from_txt(&props).is_none());
    }
}
