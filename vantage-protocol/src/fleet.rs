use serde::{Deserialize, Serialize};

/// Fleet statistics derived from coordinator session lifecycle, served at `/stats`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FleetStats {
    /// Live stream providers (registered robots whose heartbeat has not expired).
    pub providers_online: usize,
    /// Active viewer sessions (clients currently connected to a robot).
    pub consumers_connected: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_codec() {
        let stats = FleetStats { providers_online: 1, consumers_connected: 2 };
        let bytes = crate::codec::encode(&stats).unwrap();
        assert_eq!(crate::codec::decode::<FleetStats>(&bytes).unwrap(), stats);
    }
}
